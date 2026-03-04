# python
import asyncio
import logging
import os

from bson import ObjectId
from dotenv import load_dotenv
from playwright.async_api import Page, async_playwright

import uuid

from presentations_module.core.presentation_document import PresentationDocument
from presentations_module.core.presentation_task import PresentationTask
from presentations_module.database.db import MongoStorage
from presentations_module.files import S3FileStorage
from presentations_module.sources.sokratic_source import DownloadFormat, SokraticSource

load_dotenv()

logger = logging.getLogger("sokratic_source")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    logger.addHandler(console_handler)
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "2"))

tasks = [
    PresentationTask(
        topic="Влияние климата на экосистемы",
        language="ru",
        slides_amount=10,
        grade="5",
        subject="Экология",
        author="Вася Пупкин",
    ),
    # PresentationTask(
    #     topic="Влияние климата на домашних животных",
    #     language="kz",
    #     slides_amount=30,
    #     grade="5",
    #     subject="Экология",       
    #     author="Кривощеков Артем",
    # ),
    # PresentationTask(
    #     topic="Популяция китов",
    #     language="kz",
    #     slides_amount=30,
    #     grade="5",
    #     subject="Биология",
    #     author="Кривощеков Артем",
    # ),
    # PresentationTask(
    #     topic="Проблемы концерна автоваз",
    #     language="kz",
    #     slides_amount=30,
    #     grade="5",
    #     subject="Экономика",
    #     author="Кривощеков Артем",
    # ),
]

def _create_s3_storage() -> S3FileStorage:
    return S3FileStorage(
        bucket=os.environ["S3_BUCKET"],
        prefix=os.getenv("S3_PREFIX", ""),
        region_name=os.getenv("S3_REGION"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        verify_ssl=os.getenv("S3_VERIFY_SSL", "true").lower() == "true",
    )


async def run_presentation_task(
    task_id: ObjectId, task: PresentationTask
) -> tuple[PresentationTask, list[str]]:

    db = MongoStorage()
    file_paths: list[str] = []

    try:
        apw = await async_playwright().start()

        source = SokraticSource(
            apw,
            logger=logger,
            assets_dir=os.getenv("ASSETS_DIR", "./assets/presentations"),
            playwright_default_timeout=int(os.environ["PLAYWRIGHT_DEFAULT_TIMEOUT_MS"]),
            save_screenshots=os.environ.get("SAVE_SCREENSHOTS", "true").lower() == "true",
            save_logs=os.environ.get("SAVE_LOGS", "false").lower() == "true",
            storage=_create_s3_storage(),
        )

        await source.init_async(
            headless=os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true"
        )

        generation_id = uuid.uuid4().hex
        generation_dir = await source._ensure_generation_dir(generation_id)

        await source.authenticate(
            login=os.environ["SOKRATIC_USERNAME"],
            password=os.environ["SOKRATIC_PASSWORD"],
            generation_dir=generation_dir,
        )

        file_paths: list[str] = []
        async for update in source.generate_presentation(
            topic=task.topic,
            language=task.language,
            grade=task.grade,
            subject=task.subject,
            slides_amount=task.slides_amount,
            author=task.author,
            formats_to_download=[DownloadFormat.POWERPOINT, DownloadFormat.TEXT],
            generation_id=generation_id,
        ):
            if update.get("stage") == "done":
                file_paths = list(update.get("files", []))

        logger.info(f"Generated presentation for topic: {task.topic}")

        db.save_result(task_id, file_paths)

    except Exception as e:
        db.save_error(task_id, str(e))
        logger.error(f"Error processing task {task.topic}: {e}")
    finally:
        await source.dispose_async()
        await apw.stop()

    return (task, file_paths)


def set_tasks() -> list[tuple[ObjectId, PresentationTask]]:
    db = MongoStorage()
    res = []

    for task in tasks:
        logger.info(f"Saving task for topic: {task.topic}")

        document = PresentationDocument(
            topic=task.topic,
            language=task.language,
            slides_amount=task.slides_amount,
            grade=task.grade,
            subject=task.subject,
            author=task.author,
        )

        objectId = db.save_presentation(document=document)

        res.append((objectId, task))

    logger.info("All tasks saved.")

    return res


async def main():
    try:
        db_tasks = set_tasks()

        semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

        async def bounded_run(task_id: ObjectId, task: PresentationTask):
            async with semaphore:
                return await run_presentation_task(task_id, task)

        results = await asyncio.gather(
            *(bounded_run(task_id, task) for task_id, task in db_tasks)
        )

        for task, file_names in results:
            logger.info(f"Presentation files for topic {task.topic}: {file_names}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return


if __name__ == "__main__":
    # main()
    asyncio.run(main())
