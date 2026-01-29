# python
import asyncio
import logging
import os

from bson import ObjectId
from dotenv import load_dotenv
from playwright.async_api import Page, async_playwright

from presentations.core.presentation_task import PresentationTask
from presentations.database.db import MongoStorage
from presentations.sources.sokratic_source import SokraticSource

load_dotenv()

logger = logging.getLogger("sokratic_source")
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "2"))

tasks = [
    PresentationTask(
        topic="Влияние климата на экосистемы",
        language="kz",
        slides_amount=30,
        audience="Средняя школа",
        author="Кривощеков Артем",
    ),
    PresentationTask(
        topic="Влияние климата на домашних животных",
        language="kz",
        slides_amount=30,
        audience="Средняя школа",
        author="Кривощеков Артем",
    ),
    PresentationTask(
        topic="Популяция китов",
        language="kz",
        slides_amount=30,
        audience="Средняя школа",
        author="Кривощеков Артем",
    ),
    PresentationTask(
        topic="Проблемы концерна автоваз",
        language="kz",
        slides_amount=30,
        audience="Средняя школа",
        author="Кривощеков Артем",
    ),
]


async def run_presentation_task(
    task_id: ObjectId, task: PresentationTask
) -> tuple[PresentationTask, list[str]]:

    db = MongoStorage()
    file_paths: list[str] = []

    try:
        apw = await async_playwright().start()

        source = SokraticSource(apw, logger=logger)

        await source.init_async()

        await source.authenticate(
            login=os.environ["SOKRATIC_USERNAME"],
            password=os.environ["SOKRATIC_PASSWORD"],
        )

        file_paths: list[str] = []
        async for update in source.generate_presentation(
            topic=task.topic,
            language=task.language,
            slides_amount=task.slides_amount,
            audience=task.audience,
            author=task.author,
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

        objectId = db.save_task(
            topic=task.topic,
            language=task.language,
            slides_amount=task.slides_amount,
            audience=task.audience,
            author=task.author,
        )

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
