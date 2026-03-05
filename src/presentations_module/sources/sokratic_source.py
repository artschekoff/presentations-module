import dataclasses
import logging
import os
import random
import tempfile
from datetime import datetime, timezone
from typing import AsyncIterator
from urllib.parse import urlparse

from playwright.async_api import (
    Playwright,
    Browser,
    BrowserContext,
    Page,
    Route,
    TimeoutError as PlaywrightTimeoutError,
)

from .download_format import DownloadFormat
from .presentation_source import PresentationSource
from ..files import FileStorage, LocalFileStorage
from ..core.progress_payload import ProgressPayload

GRADE_MAPPING = {
    "1": "Младшая школа",
    "2": "Младшая школа",
    "3": "Младшая школа",
    "4": "Младшая школа",
    "5": "Средняя школа",
    "6": "Средняя школа",
    "7": "Средняя школа",
    "8": "Средняя школа",
    "9": "Средняя школа",
    "10": "Старшая школа",
    "11": "Старшая школа",
}


@dataclasses.dataclass
class _GenCtx:
    """Per-generation context. Holds all state that differs between concurrent generations."""
    page: Page
    generation_dir: str
    log_lines: list[str] = dataclasses.field(default_factory=list)


class GenerationLoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger: logging.Logger) -> None:
        super().__init__(logger, {})
        self._generation_id: str | None = None

    def set_generation_id(self, generation_id: str) -> None:
        self._generation_id = generation_id

    def process(self, msg, kwargs):
        if self._generation_id:
            return f"[generation_id={self._generation_id}] {msg}", kwargs
        return msg, kwargs

class SokraticSource(PresentationSource):
    browser: Browser | None
    context: BrowserContext | None
    page: Page | None

    def __init__(
        self,
        playwright: Playwright,
        logger: logging.Logger,
        generation_dir: str,
        generation_timeout: int,
        details_prompt: str | None = None,
        playwright_default_timeout: int | None = None,
        save_screenshots: bool = True,
        save_logs: bool = False,
        site_throttle_delay_ms: float = 5000,
        storage: FileStorage | None = None,
    ) -> None:
        self.chrome = playwright.chromium
        self.browser = None
        self.context = None
        self.url = "https://sokratic.ru"
        super().__init__(generation_dir=generation_dir)
        self.details_prompt = details_prompt or \
            "презентация на школьный урок по предмету {0} для {1} класса"
        self.is_init = False
        self.page = None
        self.logger = GenerationLoggerAdapter(logger)
        self.generation_timeout = generation_timeout
        self.playwright_default_timeout = playwright_default_timeout
        self.save_screenshots = save_screenshots
        self.save_logs = save_logs
        self.site_throttle_delay_ms = site_throttle_delay_ms
        self.storage = storage or LocalFileStorage()

    async def _ensure_generation_dir(self, generation_id: str) -> str:
        generation_dir = self.storage.build_path(self.generation_dir, generation_id)
        await self.storage.makedirs(generation_dir)
        return generation_dir

    async def _save_generation_screenshot(
        self, ctx: _GenCtx, step_index: int, stage: str
    ) -> str | None:
        await self._flush_browser_logs(ctx)
        if not self.save_screenshots:
            return None
        filename = f"{step_index + 1:02d}_{stage}.png"
        key = self.storage.build_path(ctx.generation_dir, filename)
        try:
            data = await ctx.page.screenshot()
        except PlaywrightTimeoutError:
            logging.warning("Screenshot timed out for step %d (%s), skipping", step_index + 1, stage)
            return None
        return await self.storage.save_bytes(key, data)

    def _append_browser_log(self, ctx: _GenCtx, level: str, message: str) -> None:
        if not self.save_logs:
            return
        timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        for line in message.splitlines() or [""]:
            ctx.log_lines.append(f"{timestamp} [{level}] {line}")

    async def _log_download_diag(self, ctx: _GenCtx, message: str, *, flush: bool = False) -> None:
        self._append_browser_log(ctx, "download-diag", message)
        if flush:
            await self._flush_browser_logs(ctx)

    async def _log_preloader_state(self, ctx: _GenCtx, label: str) -> None:
        try:
            state = await ctx.page.evaluate(
                """() => {
                    const selectors = [
                        '[aria-busy="true"]',
                        '[class*="preloader"]',
                        '[class*="loader"]',
                        '[class*="loading"]',
                        '[data-testid*="loader"]',
                        'div[data-state="open"][aria-hidden="true"][data-aria-hidden="true"][class*="inset-0"]',
                        '[class*="bg-black/80"]'
                    ];
                    const vw = window.innerWidth || document.documentElement.clientWidth;
                    const vh = window.innerHeight || document.documentElement.clientHeight;
                    const isVisible = (el) => {
                        const style = window.getComputedStyle(el);
                        if (style.display === 'none' || style.visibility === 'hidden') return false;
                        if (parseFloat(style.opacity || '1') === 0) return false;
                        const rect = el.getBoundingClientRect();
                        return rect.width > 0 && rect.height > 0;
                    };
                    const out = [];
                    const candidates = document.querySelectorAll(selectors.join(','));
                    for (const el of candidates) {
                        if (!isVisible(el)) continue;
                        const style = window.getComputedStyle(el);
                        const rect = el.getBoundingClientRect();
                        const fullCover = rect.width >= vw * 0.8 && rect.height >= vh * 0.6;
                        const fixedLike = style.position === 'fixed' || style.position === 'absolute';
                        out.push({
                            tag: el.tagName.toLowerCase(),
                            id: el.id || '',
                            className: (el.className || '').toString().slice(0, 200),
                            width: Math.round(rect.width),
                            height: Math.round(rect.height),
                            position: style.position,
                            zIndex: style.zIndex || '',
                            fullCover,
                            blocking: fullCover && fixedLike
                        });
                    }
                    return {
                        url: window.location.href,
                        viewport: `${vw}x${vh}`,
                        visibleCandidates: out.length,
                        blockingCandidates: out.filter((x) => x.blocking).length,
                        topCandidates: out.slice(0, 5),
                    };
                }"""
            )
            self._append_browser_log(ctx, "preloader-state", f"{label}: {state}")
        except Exception as exc:  # pylint: disable=broad-except
            self._append_browser_log(ctx, "preloader-state", f"{label}: failed to evaluate ({exc})")

    async def _flush_browser_logs(self, ctx: _GenCtx) -> str | None:
        if not self.save_logs:
            return None
        log_key = self.storage.build_path(ctx.generation_dir, "log.txt")
        content = "\n".join(ctx.log_lines)
        if content:
            content += "\n"
        return await self.storage.save_text(log_key, content)

    async def init_async(self, headless: bool = False):
        if not self.is_init:
            self.browser = await self.chrome.launch(
                headless=headless,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            self.is_init = True
            self.context = await self.browser.new_context(
                accept_downloads=True,
                viewport={"width": 1280, "height": 720},
                locale="ru-RU",
                timezone_id="Europe/Moscow",
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            )
            self.page = await self.context.new_page()
            if self.playwright_default_timeout is not None:
                self.page.set_default_timeout(self.playwright_default_timeout)

    def _check_init(self):
        if not self.is_init:
            raise RuntimeError("Browser is not initialized. Call 'init_async' first.")

    async def dispose_async(self):
        if self.page:
            await self.page.close()
            self.page = None
        if self.context:
            await self.context.close()
            self.context = None
        if self.browser:
            await self.browser.close()
            self.is_init = False

    async def _new_tab(self) -> Page:
        """Open a new tab in the existing browser context with routing configured."""
        assert self.context is not None
        page = await self.context.new_page()
        if self.playwright_default_timeout is not None:
            page.set_default_timeout(self.playwright_default_timeout)

        async def _block_heavy_resources(route: Route) -> None:
            allowed_hosts = {"sokratic.ru", "storage.yandexcloud.net"}
            host = urlparse(route.request.url).hostname or ""
            is_allowed = host in allowed_hosts or host.endswith(".sokratic.ru")
            if not is_allowed or route.request.resource_type in {"image", "media", "font"}:
                await route.abort()
            else:
                await route.continue_()

        await page.route("**/*", _block_heavy_resources)
        return page

    async def generate_presentation(
        self,
        generation_id: str,
        topic: str,
        language: str,
        slides_amount: int,
        grade: str,
        subject: str,
        author: str | None = None,
        style_id: str | None = None,
        formats_to_download: list[DownloadFormat] | None = None,
    ) -> AsyncIterator[ProgressPayload]:
        self._check_init()
        self.logger.set_generation_id(generation_id)
        generation_dir = await self._ensure_generation_dir(generation_id)
        self.logger.info("Start presentation generation")

        tab = await self._new_tab()
        await tab.goto(self.url)
        self.logger.debug("Opened new tab for generation %s", generation_id)
        ctx = _GenCtx(page=tab, generation_dir=generation_dir)

        ctx.page.on("console", lambda msg: self._append_browser_log(ctx, f"console:{msg.type}", msg.text))
        ctx.page.on("pageerror", lambda exc: self._append_browser_log(ctx, "pageerror", str(exc)))
        ctx.page.on("requestfailed", lambda req: self._append_browser_log(ctx, "requestfailed", f"{req.method} {req.url} - {req.failure}"))

        await self._flush_browser_logs(ctx)

        _formats = (
            set(formats_to_download) if formats_to_download is not None else set(DownloadFormat)
        )

        steps = [
            "start",
            "form_saved",
            "style_selected",
            "generation_started",
            *(["downloaded_powerpoint"] if DownloadFormat.POWERPOINT in _formats else []),
            *(["downloaded_pdf"] if DownloadFormat.PDF in _formats else []),
            *(["downloaded_text"] if DownloadFormat.TEXT in _formats else []),
            "done",
        ]
        total_steps = len(steps)

        def report_progress(
            stage: str, files: list[str] | None = None
        ) -> ProgressPayload:
            step_index = steps.index(stage)
            payload: ProgressPayload = {
                "stage": stage,
                "step": step_index + 1,
                "total_steps": total_steps,
                "percent": int(((step_index + 1) / total_steps) * 100),
            }
            if files is not None:
                payload["files"] = files
            return payload

        try:
            files: list[str] = []

            if path := await self._save_generation_screenshot(
                ctx, steps.index("start"), "start"
            ):
                files.append(path)
            yield report_progress("start", files=list(files))

            self.logger.debug("Click 'Create with AI' on landing page")
            await ctx.page.locator(
                '//button[contains(normalize-space(), "Создать с AI")]'
            ).click()

            self.logger.debug("Wait for creation modal")
            await ctx.page.locator(
                '//h2[contains(normalize-space(), "Создать презентацию")]'
            ).wait_for(timeout=self.playwright_default_timeout)

            self.logger.debug("Fill topic")
            await ctx.page.locator('//textarea[@name="topic"]').type(topic)
            self.logger.debug("Select slides amount: %s", slides_amount)
            await ctx.page.locator(
                '//form//select[.//option[contains(normalize-space(), "20")]]'
            ).select_option(str(slides_amount))

            self.logger.debug("Select language: %s", language)

            await ctx.page.locator("(//form//select)[2]").select_option(str(language))

            self.logger.debug("Open advanced settings")
            await ctx.page.locator(
                '//form//button[contains(normalize-space(), "Дополнительные настройки")]'
            ).click()

            self.logger.debug("Open audience selector")
            await ctx.page.locator(
                '//button[contains(normalize-space(), "Выберите аудиторию")]'
            ).click()

            if grade not in GRADE_MAPPING:
                raise ValueError(
                    f"Invalid grade: {grade}. Must be one of: {list(GRADE_MAPPING.keys())}"
                )

            self.logger.debug("Select audience: %s", grade)
            audience_option = GRADE_MAPPING[grade]
            await ctx.page.locator(
                f'//div[@role="option" and normalize-space()="{audience_option}"]'
            ).click()

            self.logger.debug("Fill author")
            await ctx.page.locator('//input[@name="author"]').type(author or "")
            self.logger.debug("Save form")
            await ctx.page.locator('//button[contains(normalize-space(), "Сохранить")]').click()

            if path := await self._save_generation_screenshot(
                ctx, steps.index("form_saved"), "form_saved"
            ):
                files.append(path)
            yield report_progress("form_saved", files=list(files))

            self.logger.debug("Open design gallery")
            await ctx.page.locator(
                '//button[contains(normalize-space(), "Смотреть все дизайны")]'
            ).click()

            styles_selector = (
                "//div[@role='dialog']//h2[normalize-space()='Дизайны']"
                "//..//..//div[contains(@class, 'group/item')]"
            )

            styles_count = await ctx.page.locator(styles_selector).count()
            self.logger.debug("Found %s styles", styles_count)

            if styles_count <= 0:
                raise RuntimeError("No styles found in design gallery")

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
            await ctx.page.locator(styles_selector).nth(int(final_style_id)).click()

            if path := await self._save_generation_screenshot(
                ctx, steps.index("style_selected"), "style_selected"
            ):
                files.append(path)
            yield report_progress("style_selected", files=list(files))

            self.logger.debug("Start generation")
            await ctx.page.locator(
                '//form//button[contains(normalize-space(), "Создать с AI")]'
            ).click()

            if path := await self._save_generation_screenshot(
                ctx, steps.index("generation_started"), "generation_started"
            ):
                files.append(path)
            yield report_progress("generation_started", files=list(files))

            self.logger.debug("Wait for order page")
            await ctx.page.wait_for_url(f"{self.url}/ru/orders/*", timeout=self.generation_timeout)

            self.logger.debug("Specifying details for generation")
            details_prompt_filled = self.details_prompt.format(subject, grade)
            await ctx.page.locator('//form//textarea').type(details_prompt_filled)
            await ctx.page.locator('//form//button[@type="submit"]').click()

            pres_button = (
                "//button[normalize-space(.)='Презентация']"
                "[not(contains(@class,'text-transparent'))]"
            )

            self.logger.debug("Wait for presentation download button")
            await ctx.page.locator(pres_button).wait_for(timeout=self.generation_timeout)

            self.logger.debug("Open presentation download menu")
            await ctx.page.locator(pres_button).click()

            if DownloadFormat.POWERPOINT in _formats:
                self.logger.info("Download PowerPoint")
                files.append(
                    await self._download_presentation(
                        ctx=ctx,
                        doc_format="PowerPoint",
                        file_stem=generation_id,
                    )
                )
                if path := await self._save_generation_screenshot(
                    ctx, steps.index("downloaded_powerpoint"), "downloaded_powerpoint"
                ):
                    files.append(path)
                yield report_progress("downloaded_powerpoint", files=list(files))

            if DownloadFormat.PDF in _formats:
                self.logger.info("Download PDF")
                files.append(
                    await self._download_presentation(
                        ctx=ctx,
                        doc_format="PDF",
                        file_stem=generation_id,
                    )
                )
                if path := await self._save_generation_screenshot(
                    ctx, steps.index("downloaded_pdf"), "downloaded_pdf"
                ):
                    files.append(path)
                yield report_progress("downloaded_pdf", files=list(files))

            if DownloadFormat.TEXT in _formats:
                files.append(await self._download_text(ctx=ctx, file_stem=generation_id))
                if path := await self._save_generation_screenshot(
                    ctx, steps.index("downloaded_text"), "downloaded_text"
                ):
                    files.append(path)
                yield report_progress("downloaded_text", files=list(files))

            if path := await self._save_generation_screenshot(
                ctx, steps.index("done"), "done"
            ):
                files.append(path)
            yield report_progress("done", files=list(files))
            await self._flush_browser_logs(ctx)
            self.logger.info("Presentation generation completed successfully")
        finally:
            await tab.close()
            self.logger.debug("Closed tab for generation %s", generation_id)

    async def authenticate(self, login: str, password: str, generation_id: str) -> None:
        self._check_init()
        assert self.page is not None
        self.logger.set_generation_id(generation_id)
        generation_dir = await self._ensure_generation_dir(generation_id)
        auth_ctx = _GenCtx(page=self.page, generation_dir=generation_dir)

        self.logger.info("Open auth modal")

        await self.page.goto(url=f"{self.url}/ru?auth-modal-open=true")

        await self.page.locator("//div[@role='dialog']").wait_for(timeout=self.playwright_default_timeout)

        # save screenshot here
        await self._save_generation_screenshot(
            auth_ctx, 0, "sokratic_auth_1"
        )

        self.logger.debug("Locate email input")
        email_input = await self.page.query_selector("input[id='email']")

        if email_input is None:
            raise RuntimeError("Email input not found on Sokratic login page")

        self.logger.debug("Type email")
        await email_input.type(login)

        self.logger.debug("Locate password input")
        password_input = await self.page.query_selector("input[id='password']")

        if password_input is None:
            raise RuntimeError("Password input not found on Sokratic login page")

        self.logger.debug("Type password")
        await password_input.type(password)

        form = (
            self.page.locator("form")
            .filter(has=self.page.locator("input#email"))
            .filter(has=self.page.locator("input#password"))
        )

        self.logger.debug("Submit auth form")
        submit_button = form.locator("button[type='submit']")
        await submit_button.first.click()

        await self._save_generation_screenshot(
            auth_ctx, 1, "sokratic_auth_2"
        )

        self.logger.debug("Wait for auth success")
        await self.page.wait_for_url(
            f"{self.url}/ru?auth-success=true",
            timeout=self.site_throttle_delay_ms,
        )

        # await page.screenshot(path=os.path.join(generation_dir, "sokratic_auth_2.png"))

    async def _download_text(self, ctx: _GenCtx, file_stem: str) -> str:
        await self.storage.makedirs(ctx.generation_dir)

        self.logger.debug("Downloading text")

        page = ctx.page

        await page.locator("//button[normalize-space(.)='Текст выступления']").click(
            timeout=self.generation_timeout
        )

        await page.locator("//button[normalize-space(.)='Сгенерировать текст']").click(
            timeout=self.generation_timeout
        )

        await page.wait_for_timeout(self.site_throttle_delay_ms)

        await page.locator("//button[normalize-space(.)='Текст выступления']").click(
            timeout=self.generation_timeout
        )

        markdown_content_path = "//div[contains(@class, 'markdown-body')]"

        await page.locator(markdown_content_path).wait_for()

        text_content = await page.locator(markdown_content_path).inner_text()

        if not text_content:
            raise RuntimeError("Failed to download text content")

        key = self.storage.build_path(ctx.generation_dir, f"{file_stem}.txt")
        return await self.storage.save_text(key, text_content)

    async def _close_popup_if_visible(self, ctx: _GenCtx, popup_locator, timeout: int = 5000) -> bool:
        try:
            await popup_locator.wait_for(state="visible", timeout=1000)
        except PlaywrightTimeoutError:
            self.logger.info("Popup window not detected, continue")
            return False
        try:
            self.logger.debug("Popup window detected, closing")
            await ctx.page.locator(
                "//div[@role='dialog']//button[contains(@class, '-top-2')]"
            ).click()
            await popup_locator.wait_for(state="hidden", timeout=5000)
            return True
        except PlaywrightTimeoutError:
            self.logger.warning("Popup window detected but failed to close")
            return False

    async def _wait_for_blocking_preloader_to_disappear(
        self, ctx: _GenCtx, timeout: int | None = None
    ) -> None:
        wait_timeout = timeout or self.playwright_default_timeout or 10000
        await self._log_preloader_state(ctx, f"before_wait timeout={wait_timeout}")
        try:
            await ctx.page.wait_for_function(
                """() => {
                    const selectors = [
                        '[aria-busy="true"]',
                        '[class*="preloader"]',
                        '[class*="loader"]',
                        '[class*="loading"]',
                        '[data-testid*="loader"]',
                        'div[data-state="open"][aria-hidden="true"][data-aria-hidden="true"][class*="inset-0"]',
                        '[class*="bg-black/80"]'
                    ];
                    const vw = window.innerWidth || document.documentElement.clientWidth;
                    const vh = window.innerHeight || document.documentElement.clientHeight;
                    const isVisible = (el) => {
                        const style = window.getComputedStyle(el);
                        if (style.display === 'none' || style.visibility === 'hidden') return false;
                        if (parseFloat(style.opacity || '1') === 0) return false;
                        const rect = el.getBoundingClientRect();
                        return rect.width > 0 && rect.height > 0;
                    };
                    const isBlocking = (el) => {
                        if (!isVisible(el)) return false;
                        const style = window.getComputedStyle(el);
                        const rect = el.getBoundingClientRect();
                        const fullCover = rect.width >= vw * 0.8 && rect.height >= vh * 0.6;
                        const fixedLike = style.position === 'fixed' || style.position === 'absolute';
                        return fullCover && fixedLike;
                    };
                    const candidates = document.querySelectorAll(selectors.join(','));
                    for (const el of candidates) {
                        if (isBlocking(el)) return false;
                    }
                    return true;
                }""",
                timeout=wait_timeout,
            )
            await self._log_preloader_state(ctx, "after_wait success")
        except PlaywrightTimeoutError:
            self.logger.warning("Blocking preloader is still visible after %s ms", wait_timeout)
            await self._log_preloader_state(ctx, "after_wait timeout")

    async def _download_presentation(self, ctx: _GenCtx, doc_format: str, file_stem: str) -> str:
        await self.storage.makedirs(ctx.generation_dir)

        self.logger.debug("Check ref window")
        page = ctx.page

        popup_locator = page.locator(
            "//div[@role='dialog'][.//h2[normalize-space()='Пользователь']]"
        )

        download_button = page.locator("//button[normalize-space(.)='Скачать']")
        format_locator = page.locator(
            f"//div[@role='menuitem'][normalize-space(.)='{doc_format}']"
        )
        menu_locator = page.locator(
            f"//div[@role='menu'][.//div[@role='menuitem'][normalize-space(.)='{doc_format}']]"
        )

        max_attempts = 3
        last_error: Exception | None = None
        download_info = None
        menu_timeout = self.playwright_default_timeout or 10000

        for attempt in range(1, max_attempts + 1):
            await self._log_download_diag(
                ctx,
                f"{doc_format} attempt {attempt}/{max_attempts}: start url={page.url}",
            )
            await self._wait_for_blocking_preloader_to_disappear(ctx, timeout=menu_timeout)
            await self._save_generation_screenshot(
                ctx, 0, f"before_download_{doc_format}_attempt_{attempt}"
            )
            popup_closed = await self._close_popup_if_visible(ctx, popup_locator)
            if popup_closed:
                self.logger.debug("Closed popup before clicking download button (attempt %s/%s)", attempt, max_attempts)
                await self._log_download_diag(
                    ctx, f"{doc_format} attempt {attempt}/{max_attempts}: popup closed before download button click"
                )
                await self._save_generation_screenshot(
                    ctx, 0, f"popup_closed_before_download_{doc_format}_attempt_{attempt}"
                )
            self.logger.debug(
                "Click download button (attempt %s/%s)", attempt, max_attempts
            )
            await self._log_download_diag(
                ctx, f"{doc_format} attempt {attempt}/{max_attempts}: before click download button"
            )
            await self._log_preloader_state(ctx, f"attempt {attempt} before_click_download_button")
            await self._save_generation_screenshot(
                ctx, 0, f"before_click_download_{doc_format}_attempt_{attempt}"
            )
            try:
                await download_button.click(
                    timeout=menu_timeout,
                    force=True,
                )
            except PlaywrightTimeoutError as exc:
                last_error = exc
                await self._log_download_diag(
                    ctx,
                    f"{doc_format} attempt {attempt}/{max_attempts}: download button click timeout",
                    flush=True,
                )
                self.logger.warning(
                    "Failed to click download button for format '%s' on attempt %s/%s. URL: %s",
                    doc_format,
                    attempt,
                    max_attempts,
                    page.url,
                )
                await self._save_generation_screenshot(
                    ctx,
                    0,
                    f"download_button_click_timeout_{doc_format}_attempt_{attempt}",
                )
                continue
            await self._log_download_diag(
                ctx, f"{doc_format} attempt {attempt}/{max_attempts}: after click download button"
            )
            await self._log_preloader_state(ctx, f"attempt {attempt} after_click_download_button")
            await self._save_generation_screenshot(
                ctx, 0, f"after_click_download_{doc_format}_attempt_{attempt}"
            )

            popup_closed = await self._close_popup_if_visible(ctx, popup_locator)
            if popup_closed:
                self.logger.debug("Re-open download menu after closing popup")
                await self._log_download_diag(
                    ctx, f"{doc_format} attempt {attempt}/{max_attempts}: popup closed, reopening menu"
                )
                await self._save_generation_screenshot(
                    ctx,
                    0,
                    f"before_reopen_download_menu_{doc_format}_attempt_{attempt}",
                )
                await download_button.click(timeout=menu_timeout, force=True)
                await self._save_generation_screenshot(
                    ctx,
                    0,
                    f"after_reopen_download_menu_{doc_format}_attempt_{attempt}",
                )

            await menu_locator.wait_for(state="visible", timeout=menu_timeout)
            await format_locator.wait_for(state="visible", timeout=menu_timeout)
            await self._save_generation_screenshot(
                ctx, 0, f"menu_open_{doc_format}_attempt_{attempt}"
            )

            try:
                async with page.expect_download(timeout=self.generation_timeout) as download_info:
                    self.logger.debug(
                        "Click download format '%s' (attempt %s/%s)",
                        doc_format,
                        attempt,
                        max_attempts,
                    )
                    await self._log_download_diag(
                        ctx,
                        f"{doc_format} attempt {attempt}/{max_attempts}: before click format",
                    )
                    await self._log_preloader_state(ctx, f"attempt {attempt} before_click_format")
                    await self._save_generation_screenshot(
                        ctx,
                        0,
                        f"before_click_download_format_{doc_format}_attempt_{attempt}",
                    )
                    if attempt == max_attempts:
                        await format_locator.click(no_wait_after=True, force=True)
                    else:
                        await format_locator.click(no_wait_after=True)
                    await self._log_download_diag(
                        ctx,
                        f"{doc_format} attempt {attempt}/{max_attempts}: after click format",
                    )
                    await self._log_preloader_state(ctx, f"attempt {attempt} after_click_format")
                break
            except PlaywrightTimeoutError as exc:
                last_error = exc
                await self._log_download_diag(
                    ctx,
                    f"{doc_format} attempt {attempt}/{max_attempts}: expect_download timeout",
                    flush=True,
                )
                self.logger.warning(
                    "Download event not received for format '%s' on attempt %s/%s. URL: %s",
                    doc_format,
                    attempt,
                    max_attempts,
                    page.url,
                )
                await self._save_generation_screenshot(
                    ctx, 0, f"download_timeout_{doc_format}_attempt_{attempt}"
                )

        if download_info is None:
            self.logger.error(
                "Failed to download format '%s' after %s attempts",
                doc_format,
                max_attempts,
            )
            await self._log_download_diag(
                ctx, f"{doc_format}: failed after {max_attempts} attempts", flush=True
            )
            raise RuntimeError(
                f"Download event not received for format '{doc_format}' after {max_attempts} attempts"
            ) from last_error

        await self._close_popup_if_visible(ctx, popup_locator)

        download = await download_info.value
        ext = os.path.splitext(download.suggested_filename)[1]
        dest_key = self.storage.build_path(ctx.generation_dir, f"{file_stem}{ext}")

        fd, tmp_path = tempfile.mkstemp(suffix=ext)
        os.close(fd)

        try:
            await download.save_as(tmp_path)
            filepath = await self.storage.save_from_local_path(dest_key, tmp_path)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

        self.logger.debug("File saved to %s", filepath)
        return filepath


async def generate_presentation(
    playwright: Playwright,
    *,
    topic: str,
    language: str,
    slides_amount: int,
    grade: str,
    subject: str,
    author: str | None = None,
    style_id: str | None = None,
    formats_to_download: list[DownloadFormat] | None = None,
    generation_id: str,
    logger: logging.Logger | None = None,
) -> AsyncIterator[ProgressPayload]:
    logger = logger or logging.getLogger("sokratic_source")
    source = SokraticSource(
        playwright,
        logger=logger,
        generation_dir=os.getenv("PRESENTATIONS_DIR", "./assets/presentations"),
        generation_timeout=int(os.getenv("PRESENTATIONS_GENERATION_TIMEOUT_MS", "600000")),
        save_logs=os.environ.get("SAVE_LOGS", "false").lower() == "true",
    )

    try:
        await source.init_async()

        async for update in source.generate_presentation(
            generation_id=generation_id,
            topic=topic,
            language=language,
            slides_amount=slides_amount,
            grade=grade,
            subject=subject,
            author=author,
            style_id=style_id,
            formats_to_download=formats_to_download,
        ):
            yield update
    finally:
        await source.dispose_async()
