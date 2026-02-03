import logging
import os
import random
from typing import AsyncIterator

from playwright.async_api import Playwright, Browser, Page

from .presentation_source import PresentationSource
from ..core.progress_payload import ProgressPayload

GENERATION_TIMEOUT = 1000 * 60 * 10  # 10 minutes


class SokraticSource(PresentationSource):
    browser: Browser | None
    page: Page | None

    def __init__(
        self,
        playwright: Playwright,
        logger: logging.Logger,
        assets_dir: str = "./assets/presentations",
        generation_timeout: int = GENERATION_TIMEOUT,
    ) -> None:
        self.chrome = playwright.chromium
        self.url = "https://sokratic.ru"
        self.is_init = False
        self.page = None
        self.logger = logger
        self.assets_dir = assets_dir
        self.generation_timeout = generation_timeout

    def _ensure_assets_dir(self) -> None:
        os.makedirs(self.assets_dir, exist_ok=True)

    def _ensure_screenshots_dir(self) -> str:
        base_dir = os.path.dirname(os.path.abspath(self.assets_dir))
        screenshots_dir = os.path.join(base_dir, "screenshots")
        os.makedirs(screenshots_dir, exist_ok=True)
        return screenshots_dir

    async def init_async(self):
        if not self.is_init:
            self.browser = await self.chrome.launch(headless=False)
            self.is_init = True
            self.page = await self.browser.new_page()

    def _check_init(self):
        if not self.is_init:
            raise Exception("Browser is not initialized. Call 'init_async' first.")

    def _get_page(self) -> Page:
        self._check_init()
        assert self.page is not None
        return self.page

    async def dispose_async(self):
        if self.page:
            await self.page.close()
            self.page = None
        if self.browser:
            await self.browser.close()
            self.is_init = False

    async def generate_presentation(
        self,
        topic: str,
        language: str,
        slides_amount: int,
        audience: str,
        author: str | None = None,
        style_id: str | None = None,
    ) -> AsyncIterator[ProgressPayload]:
        self._check_init()
        self._ensure_assets_dir()

        steps = [
            "start",
            "form_saved",
            "style_selected",
            "generation_started",
            "downloaded_powerpoint",
            "downloaded_pdf",
            "done",
        ]
        total_steps = len(steps)

        def report_progress(
            step_index: int, stage: str, files: list[str] | None = None
        ) -> ProgressPayload:
            payload: ProgressPayload = {
                "stage": stage,
                "step": step_index + 1,
                "total_steps": total_steps,
                "percent": int(((step_index + 1) / total_steps) * 100),
            }
            if files is not None:
                payload["files"] = files
            return payload

        yield report_progress(0, "start")

        page = self._get_page()

        await page.locator(
            '//button[contains(normalize-space(), "Создать с AI")]'
        ).click()

        await page.locator(
            '//h2[contains(normalize-space(), "Создать презентацию")]'
        ).wait_for(timeout=5000)

        await page.locator('//textarea[@name="topic"]').type(topic)
        await page.locator(
            '//form//select[.//option[contains(normalize-space(), "20")]]'
        ).select_option(str(slides_amount))

        await page.locator(
            '//form//select[.//option[contains(normalize-space(), "Русский")]]'
        ).select_option(language)

        await page.locator(
            '//form//button[contains(normalize-space(), "Дополнительные настройки")]'
        ).click()

        await page.locator(
            '//button[contains(normalize-space(), "Выберите аудиторию")]'
        ).click()

        await page.locator(
            f'//div[@role="option"][contains(normalize-space(), "{audience}")]'
        ).click()

        await page.locator('//input[@name="author"]').type(author or "")
        await page.locator('//button[contains(normalize-space(), "Сохранить")]').click()

        yield report_progress(1, "form_saved")

        await page.locator(
            '//button[contains(normalize-space(), "Смотреть все дизайны")]'
        ).click()

        styles_selector = "//div[@role='dialog']//h2[normalize-space()='Дизайны']//..//..//div[contains(@class, 'group/item')]"

        styles_count = await page.locator(styles_selector).count()

        final_style_id = (
            random.randint(0, styles_count - 1) if style_id is None else style_id
        )

        await page.locator(styles_selector).nth(int(final_style_id)).click()

        yield report_progress(2, "style_selected")

        await page.locator(
            '//form//button[contains(normalize-space(), "Создать с AI")]'
        ).click()

        yield report_progress(3, "generation_started")

        await page.wait_for_url(f"{self.url}/ru/orders/*")

        pres_button = "//button[normalize-space(.)='Презентация'][not(contains(@class,'text-transparent'))]"

        await page.locator(pres_button).wait_for(timeout=self.generation_timeout)

        await page.locator(pres_button).click()
        files = []

        files.append(
            await self._download_presentation(
                doc_format="PowerPoint",
                save_path=self.assets_dir,
            )
        )

        yield report_progress(4, "downloaded_powerpoint", files=list(files))

        files.append(
            await self._download_presentation(
                doc_format="PDF",
                save_path=self.assets_dir,
            )
        )

        yield report_progress(5, "downloaded_pdf", files=list(files))
        yield report_progress(6, "done", files=list(files))

        return

    async def authenticate(self, login: str, password: str) -> None:
        self._check_init()
        page = self._get_page()

        await page.goto(url=f"{self.url}/ru?auth-modal-open=true")
        screenshots_dir = self._ensure_screenshots_dir()
        await page.screenshot(path=os.path.join(screenshots_dir, "sokratic_auth_1.png"))

        email_input = await page.query_selector("input[id='email']")

        if email_input is None:
            raise Exception("Email input not found on Sokratic login page")

        await email_input.type(login)

        password_input = await page.query_selector("input[id='password']")

        if password_input is None:
            raise Exception("Password input not found on Sokratic login page")
        await password_input.type(password)

        submit_button = (await page.query_selector_all("button[type='submit']"))[1]
        await submit_button.click()

        await page.wait_for_url(f"{self.url}/ru?auth-success=true", timeout=10000)
        await page.screenshot(path=os.path.join(screenshots_dir, "sokratic_auth_2.png"))

    async def _download_presentation(self, doc_format: str, save_path: str) -> str:
        if not os.path.exists(save_path):
            os.makedirs(save_path)

        self.logger.debug("Check ref window")
        page = self._get_page()

        if (
            await page.locator(
                "//div[@role='dialog']//h2[normalize-space(.)='Реферальная программа']"
            ).count()
            > 0
        ):
            self.logger.debug("Popup window detected, closing")
            await page.locator(
                "//div[@role='dialog']//button[contains(@class, 'top-4')]"
            ).click()
        else:
            self.logger.info("Popup window not detected, continue")

        self.logger.debug("Click download button")

        await page.locator("//button[normalize-space(.)='Скачать']").click(
            timeout=self.generation_timeout
        )

        async with page.expect_download() as download_info:
            self.logger.debug("Click download format button")
            await page.locator(
                "//div[@role='menuitem'][normalize-space(.)='{doc_format}']".format(
                    doc_format=doc_format
                )
            ).click()

        download = await download_info.value
        filepath = os.path.join(save_path, download.suggested_filename)

        await download.save_as(filepath)
        self.logger.debug("File saved to {filepath}".format(filepath=filepath))

        return filepath


async def generate_presentation(
    playwright: Playwright,
    *,
    topic: str,
    language: str,
    slides_amount: int,
    audience: str,
    author: str | None = None,
    style_id: str | None = None,
    generation_timeout: int = GENERATION_TIMEOUT,
    logger: logging.Logger | None = None,
) -> AsyncIterator[ProgressPayload]:
    logger = logger or logging.getLogger("sokratic_source")
    source = SokraticSource(
        playwright,
        logger=logger,
        generation_timeout=generation_timeout,
    )

    try:
        await source.init_async()

        async for update in source.generate_presentation(
            topic=topic,
            language=language,
            slides_amount=slides_amount,
            audience=audience,
            author=author,
            style_id=style_id,
        ):
            yield update
    finally:
        await source.dispose_async()
