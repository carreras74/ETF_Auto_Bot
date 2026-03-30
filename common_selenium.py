from __future__ import annotations

import logging
import os
import shutil
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

from selenium import webdriver
from selenium.common.exceptions import JavascriptException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

LOGGER = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def get_target_trading_date(now: Optional[datetime] = None) -> datetime:
    now = now or datetime.now()
    if now.weekday() == 5:
        return now - timedelta(days=1)
    if now.weekday() == 6:
        return now - timedelta(days=2)
    return now


def create_download_dir(base_dir: str | Path) -> str:
    base_path = Path(base_dir)
    base_path.mkdir(parents=True, exist_ok=True)
    return tempfile.mkdtemp(prefix="downloads_", dir=str(base_path))


def cleanup_download_dir(download_dir: str | Path) -> None:
    try:
        shutil.rmtree(download_dir, ignore_errors=True)
    except Exception as exc:
        LOGGER.warning("임시 다운로드 폴더 삭제 실패: %s", exc)


def build_driver(download_dir: str | Path, headless: bool = True) -> webdriver.Chrome:
    download_dir = str(Path(download_dir).resolve())

    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_setting_values.automatic_downloads": 1,
        },
    )

    chrome_bin = os.getenv("CHROME_BIN")
    if chrome_bin:
        chrome_options.binary_location = chrome_bin

    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {
            "source": """
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = window.chrome || { runtime: {} };
                Object.defineProperty(navigator, 'languages', { get: () => ['ko-KR', 'ko', 'en-US', 'en'] });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            """
        },
    )
    driver.execute_cdp_cmd(
        "Page.setDownloadBehavior",
        {"behavior": "allow", "downloadPath": download_dir},
    )
    return driver


def wait_for_page_ready(driver: webdriver.Chrome, timeout: int = 20) -> None:
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )


def dismiss_popups(driver: webdriver.Chrome) -> None:
    script = """
        const selectors = [
            '[class*="popup"]', '[class*="layer"]', '[class*="modal"]',
            '[id*="popup"]', '[id*="layer"]', '[role="dialog"]'
        ];
        document.querySelectorAll(selectors.join(',')).forEach(el => {
            if (el && el.style) {
                el.style.display = 'none';
                el.style.visibility = 'hidden';
            }
        });
        document.body.style.overflow = 'auto';
    """
    try:
        driver.execute_script(script)
    except JavascriptException:
        pass


def progressive_scroll(driver: webdriver.Chrome, steps: int = 6, pause: float = 0.8) -> None:
    for step in range(1, steps + 1):
        driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * ({step}/{steps}));")
        time.sleep(pause)
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(0.3)


def find_clickable_candidates(
    driver: webdriver.Chrome,
    xpaths: Iterable[str],
    timeout: int = 15,
):
    deadline = time.time() + timeout
    while time.time() < deadline:
        for xpath in xpaths:
            try:
                elements = driver.find_elements(By.XPATH, xpath)
            except Exception:
                continue

            visible = [el for el in elements if el.is_displayed()]
            if visible:
                return visible

        dismiss_popups(driver)
        time.sleep(0.5)

    raise TimeoutException("다운로드 버튼을 찾지 못했습니다.")


def safe_click(driver: webdriver.Chrome, element) -> None:
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
    time.sleep(0.5)
    try:
        element.click()
    except Exception:
        driver.execute_script("arguments[0].click();", element)


def wait_for_new_download(
    download_dir: str | Path,
    before_snapshot: set[str],
    timeout: int = 60,
) -> Optional[str]:
    download_path = Path(download_dir)
    deadline = time.time() + timeout

    while time.time() < deadline:
        current_files = {str(p) for p in download_path.glob("*") if p.is_file()}
        new_files = current_files - before_snapshot

        partial_exists = any(
            str(p).endswith((".crdownload", ".tmp", ".part"))
            for p in download_path.glob("*")
        )

        completed = [
            path for path in new_files
            if path.lower().endswith((".xlsx", ".xls", ".csv", ".xlsm", ".html"))
            and not path.lower().endswith((".crdownload", ".tmp", ".part"))
        ]

        if completed and not partial_exists:
            completed.sort(key=lambda p: Path(p).stat().st_mtime, reverse=True)
            return completed[0]

        time.sleep(1)

    return None


def move_file(src: str | Path, dst: str | Path) -> str:
    src_path = Path(src)
    dst_path = Path(dst)
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    if dst_path.exists():
        dst_path.unlink()

    shutil.move(str(src_path), str(dst_path))
    return str(dst_path)
