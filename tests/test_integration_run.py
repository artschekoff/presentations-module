"""
Integration test that mirrors `make run` / `python main.py`.

Runs a single presentation task end-to-end (Playwright + Sokratic) and verifies
that all three expected files (PowerPoint, PDF, text) are returned in the final
"done" progress update.

Prerequisites:
  - A .env file with all variables required by main.py
    (SOKRATIC_USERNAME, SOKRATIC_PASSWORD, PLAYWRIGHT_DEFAULT_TIMEOUT_MS, etc.)
  - Playwright browsers installed: `playwright install chromium`

Run with:
    pytest tests/test_integration_run.py -v -s
"""

import asyncio
import os
import uuid
import logging

import pytest
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

from presentations_module.core.presentation_task import PresentationTask
from presentations_module.files.s3_file_storage import S3FileStorage
from presentations_module.files.local_file_storage import LocalFileStorage
from presentations_module.sources.sokratic_source import SokraticSource
from presentations_module.sources.download_format import DownloadFormat

logger = logging.getLogger("test_integration_run")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# ---------------------------------------------------------------------------
# Single test task (lightweight — 10 slides, ru language)
# ---------------------------------------------------------------------------

TEST_TASK = PresentationTask(
    topic="Climate change and ecosystems",
    language="ru",
    slides_amount=10,
    grade="5",
    subject="Ecology",
    author="Test User",
)

REQUIRED_FORMATS = [DownloadFormat.POWERPOINT, DownloadFormat.PDF, DownloadFormat.TEXT]

REQUIRED_EXTENSIONS = {
    DownloadFormat.POWERPOINT: (".pptx",),
    DownloadFormat.PDF: (".pdf",),
    DownloadFormat.TEXT: (".txt",),
}


def _create_storage():
    """Return S3FileStorage if S3_BUCKET is configured, otherwise LocalFileStorage."""
    bucket = os.getenv("S3_BUCKET")
    if bucket:
        return S3FileStorage(
            bucket=bucket,
            prefix=os.getenv("S3_PREFIX", ""),
            region_name=os.getenv("S3_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            endpoint_url=os.getenv("S3_ENDPOINT_URL"),
            verify_ssl=os.getenv("S3_VERIFY_SSL", "true").lower() == "true",
        )
    return LocalFileStorage()


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_generate_presentation_saves_all_required_files():
    """
    End-to-end test: authenticate → generate → assert all required files present.

    Fails with a descriptive error if:
      - any exception is raised during generation, or
      - one or more expected file types are missing in the final result.
    """
    playwright_default_timeout = int(
        os.environ.get("PLAYWRIGHT_DEFAULT_TIMEOUT_MS", "30000")
    )
    generation_timeout = int(
        os.environ.get("PRESENTATIONS_GENERATION_TIMEOUT_MS", "600000")
    )
    headless = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true"
    generation_dir = os.getenv("PRESENTATIONS_DIR", "./assets/presentations")

    apw = await async_playwright().start()
    source = SokraticSource(
        apw,
        logger=logger,
        generation_dir=generation_dir,
        generation_timeout=generation_timeout,
        playwright_default_timeout=playwright_default_timeout,
        site_throttle_delay_ms=float(os.getenv("SITE_THROTTLE_DELAY_MS", "5000")),
        save_screenshots=os.getenv("SAVE_SCREENSHOTS", "true").lower() == "true",
        save_logs=os.getenv("SAVE_LOGS", "false").lower() == "true",
        storage=_create_storage(),
    )

    try:
        await source.init_async(headless=headless)

        auth_generation_id = uuid.uuid4().hex
        await source.authenticate(
            login=os.environ["SOKRATIC_USERNAME"],
            password=os.environ["SOKRATIC_PASSWORD"],
            generation_id=auth_generation_id,
        )

        generation_id = uuid.uuid4().hex
        final_files: list[str] = []
        error: Exception | None = None

        try:
            async for update in source.generate_presentation(
                generation_id=generation_id,
                topic=TEST_TASK.topic,
                language=TEST_TASK.language,
                grade=TEST_TASK.grade,
                subject=TEST_TASK.subject,
                slides_amount=TEST_TASK.slides_amount,
                author=TEST_TASK.author,
                formats_to_download=REQUIRED_FORMATS,
            ):
                stage = update.get("stage")
                logger.info("Progress: stage=%s step=%s/%s", stage, update.get("step"), update.get("total_steps"))
                if stage == "done":
                    final_files = list(update.get("files", []))
        except Exception as exc:
            error = exc

        # --- assert no error ---
        assert error is None, f"Presentation generation raised an error: {error}"

        # --- assert all required file types are present ---
        missing: list[str] = []
        for fmt in REQUIRED_FORMATS:
            expected_exts = REQUIRED_EXTENSIONS[fmt]
            found = any(
                str(f).lower().endswith(ext)
                for f in final_files
                for ext in expected_exts
            )
            if not found:
                missing.append(fmt.value)

        assert not missing, (
            f"Generation completed but the following file types are missing from the result: {missing}.\n"
            f"Files returned: {final_files}"
        )

        logger.info("All required files saved: %s", final_files)

    finally:
        await source.dispose_async()
        await apw.stop()
