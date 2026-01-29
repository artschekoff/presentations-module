import os
import shutil

from pyppeteer import launch
from pyppeteer.browser import Browser
from pyppeteer.errors import BrowserError


class Chrome:
    @staticmethod
    def _find_chrome() -> str | None:
        candidates = [
            '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
            '/Applications/Chromium.app/Contents/MacOS/Chromium',
            shutil.which('google-chrome'),
            shutil.which('chromium'),
            shutil.which('chromium-browser'),
        ]
        for c in candidates:
            if c and os.path.exists(c):
                return c
        return None

    async def load_chrome(self) -> Browser | None:
        chrome_path = self._find_chrome()

        if not chrome_path:
            print('Chrome executable not found.')
            return None

        launch_kwargs = {
            'headless': False,
            'args': ['--no-sandbox', '--disable-setuid-sandbox', '--disable-gpu'],
            'executablePath': chrome_path
        }

        try:
            browser = await launch(**launch_kwargs)
            return browser
        except BrowserError as e:
            print('Browser failed to launch:', e)
            return None
