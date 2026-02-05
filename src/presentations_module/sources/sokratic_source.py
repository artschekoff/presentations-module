import logging
import os
import random
from typing import AsyncIterator
import uuid

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
        self.browser = None
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

    def _ensure_generation_dir(self, generation_id: str) -> str:
        generation_dir = os.path.join(self.assets_dir, generation_id)
        os.makedirs(generation_dir, exist_ok=True)
        return generation_dir

    async def _save_generation_screenshot(
        self, page: Page, screenshots_dir: str, step_index: int, stage: str
    ) -> str:
        filename = f"{step_index + 1:02d}_{stage}.png"
        path = os.path.join(screenshots_dir, filename)
        await page.screenshot(path=path)
        return path

    async def init_async(self, headless: bool = False):
        if not self.is_init:
            self.browser = await self.chrome.launch(headless=headless)
            self.is_init = True
            self.page = await self.browser.new_page(
                viewport={"width": 1280, "height": 720},
                locale="ru-RU",
                timezone_id="Europe/Moscow",
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            )

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
        self.logger.info("Start presentation generation")

        # generate random uuid for this generation
        generation_id = uuid.uuid4().hex
        generation_dir = self._ensure_generation_dir(generation_id)
        screenshots_dir = os.path.join(generation_dir, "screenshots")
        os.makedirs(screenshots_dir, exist_ok=True)

        steps = [
            "start",
            "form_saved",
            "style_selected",
            "generation_started",
            "downloaded_powerpoint",
            "downloaded_pdf",
            "downloaded_text",
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

        page = self._get_page()

        files: list[str] = []

        files.append(
            await self._save_generation_screenshot(page, screenshots_dir, 0, "start")
        )
        yield report_progress(0, "start", files=list(files))

        self.logger.debug("Click 'Create with AI' on landing page")
        await page.locator(
            '//button[contains(normalize-space(), "Создать с AI")]'
        ).click()

        self.logger.debug("Wait for creation modal")
        await page.locator(
            '//h2[contains(normalize-space(), "Создать презентацию")]'
        ).wait_for(timeout=5000)

        self.logger.debug("Fill topic")
        await page.locator('//textarea[@name="topic"]').type(topic)
        self.logger.debug("Select slides amount: %s", slides_amount)
        await page.locator(
            '//form//select[.//option[contains(normalize-space(), "20")]]'
        ).select_option(str(slides_amount))

        self.logger.debug("Select language: %s", language)

        await page.locator("(//form//select)[2]").select_option(str(language))

        # await page.locator(
        #     '//form//select[.//option[contains(normalize-space(), "Русский")]]'
        # ).select_option(str(language))

        self.logger.debug("Open advanced settings")
        await page.locator(
            '//form//button[contains(normalize-space(), "Дополнительные настройки")]'
        ).click()

        self.logger.debug("Open audience selector")
        await page.locator(
            '//button[contains(normalize-space(), "Выберите аудиторию")]'
        ).click()

        self.logger.debug("Select audience: %s", audience)
        await page.locator(
            f'//div[@role="option"][contains(normalize-space(), "{audience}")]'
        ).click()

        self.logger.debug("Fill author")
        await page.locator('//input[@name="author"]').type(author or "")
        self.logger.debug("Save form")
        await page.locator('//button[contains(normalize-space(), "Сохранить")]').click()

        files.append(
            await self._save_generation_screenshot(
                page, screenshots_dir, 1, "form_saved"
            )
        )
        yield report_progress(1, "form_saved", files=list(files))

        self.logger.debug("Open design gallery")
        await page.locator(
            '//button[contains(normalize-space(), "Смотреть все дизайны")]'
        ).click()

        styles_selector = "//div[@role='dialog']//h2[normalize-space()='Дизайны']//..//..//div[contains(@class, 'group/item')]"

        styles_count = await page.locator(styles_selector).count()
        self.logger.debug("Found %s styles", styles_count)

        if styles_count <= 0:
            raise Exception("No styles found in design gallery")

        if style_id is None:
            final_style_id = random.randint(0, styles_count - 1)
        else:
            try:
                final_style_id = int(style_id)
            except (TypeError, ValueError) as exc:
                raise ValueError("style_id must be a numeric index") from exc

            if final_style_id < 0 or final_style_id >= styles_count:
                raise ValueError(
                    f"style_id index out of range: {final_style_id} (styles_count={styles_count})"
                )

        self.logger.debug("Select style: %s", final_style_id)
        await page.locator(styles_selector).nth(int(final_style_id)).click()

        files.append(
            await self._save_generation_screenshot(
                page, screenshots_dir, 2, "style_selected"
            )
        )
        yield report_progress(2, "style_selected", files=list(files))

        self.logger.debug("Start generation")
        await page.locator(
            '//form//button[contains(normalize-space(), "Создать с AI")]'
        ).click()

        files.append(
            await self._save_generation_screenshot(
                page, screenshots_dir, 3, "generation_started"
            )
        )
        yield report_progress(3, "generation_started", files=list(files))

        self.logger.debug("Wait for order page")
        await page.wait_for_url(f"{self.url}/ru/orders/*")

        pres_button = "//button[normalize-space(.)='Презентация'][not(contains(@class,'text-transparent'))]"

        self.logger.debug("Wait for presentation download button")
        await page.locator(pres_button).wait_for(timeout=self.generation_timeout)

        self.logger.debug("Open presentation download menu")
        await page.locator(pres_button).click()
        self.logger.info("Download PowerPoint")

        files.append(
            await self._download_presentation(
                doc_format="PowerPoint",
                save_path=generation_dir,
            )
        )

        files.append(
            await self._save_generation_screenshot(
                page, screenshots_dir, 4, "downloaded_powerpoint"
            )
        )
        yield report_progress(4, "downloaded_powerpoint", files=list(files))

        self.logger.info("Download PDF")
        files.append(
            await self._download_presentation(
                doc_format="PDF",
                save_path=generation_dir,
            )
        )

        files.append(
            await self._save_generation_screenshot(
                page, screenshots_dir, 5, "downloaded_pdf"
            )
        )
        yield report_progress(5, "downloaded_pdf", files=list(files))

        files.append(await self._download_text(save_path=generation_dir))

        files.append(
            await self._save_generation_screenshot(
                page, screenshots_dir, 6, "downloaded_text"
            )
        )
        yield report_progress(6, "downloaded_text", files=list(files))

        files.append(
            await self._save_generation_screenshot(page, screenshots_dir, 7, "done")
        )
        yield report_progress(7, "done", files=list(files))

    async def authenticate(self, login: str, password: str) -> None:
        self._check_init()
        page = self._get_page()

        self.logger.info("Open auth modal")

        await page.goto(url=f"{self.url}/ru?auth-modal-open=true")

        await page.locator("//div[@role='dialog']").wait_for(timeout=10000)

        screenshots_dir = self._ensure_screenshots_dir()

        # save screenshot here
        await self._save_generation_screenshot(
            page, screenshots_dir, 0, "sokratic_auth_1"
        )

        self.logger.debug("Locate email input")
        email_input = await page.query_selector("input[id='email']")

        if email_input is None:
            raise Exception("Email input not found on Sokratic login page")

        self.logger.debug("Type email")
        await email_input.type(login)

        self.logger.debug("Locate password input")
        password_input = await page.query_selector("input[id='password']")

        if password_input is None:
            raise Exception("Password input not found on Sokratic login page")

        self.logger.debug("Type password")
        await password_input.type(password)

        form = (
            page.locator("form")
            .filter(has=page.locator("input#email"))
            .filter(has=page.locator("input#password"))
        )

        self.logger.debug("Submit auth form")
        submit_button = form.locator("button[type='submit']")
        await submit_button.first.click()

        await self._save_generation_screenshot(
            page, screenshots_dir, 1, "sokratic_auth_2"
        )

        self.logger.debug("Wait for auth success")
        await page.wait_for_url(
            f"{self.url}/ru?auth-success=true",
            timeout=float(os.environ["SITE_THROTTLE_DELAY_MS"]),
        )

        # await page.screenshot(path=os.path.join(screenshots_dir, "sokratic_auth_2.png"))

    async def _download_text(self, save_path: str) -> str:
        if not os.path.exists(save_path):
            os.makedirs(save_path)

        self.logger.debug("Downloading text")

        page = self._get_page()

        await page.locator("//button[normalize-space(.)='Текст выступления']").click(
            timeout=self.generation_timeout
        )

        await page.locator("//button[normalize-space(.)='Сгенерировать текст']").click(
            timeout=self.generation_timeout
        )

        # wait for 5 seconds
        await page.wait_for_timeout(float(os.environ["SITE_THROTTLE_DELAY_MS"]))

        await page.locator("//button[normalize-space(.)='Текст выступления']").click(
            timeout=self.generation_timeout
        )

        markdown_content_path = "//div[contains(@class, 'markdown-body')]"

        await page.locator(markdown_content_path).wait_for()

        # get text content

        text_content = await page.locator(markdown_content_path).inner_text()

        if not text_content:
            raise Exception("Failed to download text content")

        file_path = os.path.join(save_path, "presentation_text.txt")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(text_content)

        return file_path

    async def _download_presentation(self, doc_format: str, save_path: str) -> str:
        if not os.path.exists(save_path):
            os.makedirs(save_path)

        self.logger.debug("Check ref window")
        page = self._get_page()

        if (
            await page.locator(
                "//div[@role='dialog'][.//h2[normalize-space()='Пользователь']]"
            ).count()
            > 0
        ):
            self.logger.debug("Popup window detected, closing")
            await page.locator(
                "//div[@role='dialog']//button[contains(@class, '-top-2')]"
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
