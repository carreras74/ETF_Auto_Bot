from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict

from selenium.common.exceptions import NoAlertPresentException, TimeoutException

from common_selenium import (
    build_driver,
    cleanup_download_dir,
    configure_logging,
    create_download_dir,
    dismiss_popups,
    find_clickable_candidates,
    get_target_trading_date,
    move_file,
    progressive_scroll,
    safe_click,
    wait_for_new_download,
    wait_for_page_ready,
)

LOGGER = logging.getLogger(__name__)

TIME_ROOMS: Dict[str, str] = {
    "코스닥액티브": "https://timeetf.co.kr/m11_view.php?idx=24&cate=002",
    "플러스배당액티브": "https://timeetf.co.kr/m11_view.php?idx=12&cate=002",
    "코스피액티브": "https://timeetf.co.kr/m11_view.php?idx=11&cate=002",
    "밸류업액티브": "https://timeetf.co.kr/m11_view.php?idx=15&cate=002",
    "신재생에너지액티브": "https://timeetf.co.kr/m11_view.php?idx=16&cate=002",
    "바이오액티브": "https://timeetf.co.kr/m11_view.php?idx=13&cate=002",
    "이노베이션액티브": "https://timeetf.co.kr/m11_view.php?idx=17&cate=002",
    "컬처액티브": "https://timeetf.co.kr/m11_view.php?idx=1&cate=002",
}

KOACT_ROOMS: Dict[str, str] = {
    "배당성장액티브": "https://www.samsungactive.co.kr/etf/view.do?id=2ETFM2",
    "수소전력ESS인프라액티브": "https://www.samsungactive.co.kr/etf/view.do?id=2ETFT9",
    "바이오헬스케어액티브": "https://www.samsungactive.co.kr/etf/view.do?id=2ETFJ9",
    "코리아밸류업액티브": "https://www.samsungactive.co.kr/etf/view.do?id=2ETFP3",
    "K수출핵심기업TOP30액티브": "https://www.samsungactive.co.kr/etf/view.do?id=2ETFR6",
    "AI인프라액티브": "https://www.samsungactive.co.kr/etf/view.do?id=2ETFN8",
    "반도체2차전지핵심소재액티브": "https://www.samsungactive.co.kr/etf/view.do?id=2ETFM8",
    "코스닥액티브": "https://www.samsungactive.co.kr/etf/view.do?id=2ETFU6",
}

DOWNLOAD_XPATHS = [
    "//a[contains(@class, 'excel') or contains(@class, 'xls') or contains(@href, 'excel') or contains(@href, 'xls') or contains(translate(normalize-space(.), 'EXCEL', 'excel'), 'excel') or contains(normalize-space(.), '엑셀')]",
    "//button[contains(@class, 'excel') or contains(@class, 'xls') or contains(translate(normalize-space(.), 'EXCEL', 'excel'), 'excel') or contains(normalize-space(.), '엑셀')]",
    "//img[contains(@alt, '엑셀') or contains(translate(@alt, 'EXCEL', 'excel'), 'excel')]/ancestor::a[1]",
    "//*[contains(@title, '엑셀') or contains(translate(@title, 'EXCEL', 'excel'), 'excel')]/self::a | //*[contains(@title, '엑셀') or contains(translate(@title, 'EXCEL', 'excel'), 'excel')]/self::button",
]

TASKS = [
    ("TIME", TIME_ROOMS),
    ("KoAct", KOACT_ROOMS),
]


def build_final_name(brand: str, etf_name: str, source_file: str, date_time: str, date_koact: str) -> str:
    ext = Path(source_file).suffix.lower()
    if ext not in {".xlsx", ".xls", ".csv", ".xlsm", ".html"}:
        ext = ".xlsx"

    if brand == "TIME":
        return f"구성종목(PDF){brand}{etf_name}_{date_time}{ext}"
    return f"{brand} {etf_name}_{date_koact}{ext}"


def collect_brand(driver, brand: str, rooms: Dict[str, str], base_dir: Path, download_dir: str, date_time: str, date_koact: str) -> None:
    LOGGER.info("[%s] 수집 시작 (%d개 ETF)", brand, len(rooms))

    for etf_name, room_url in rooms.items():
        LOGGER.info("[%s] %s 페이지 접속", brand, etf_name)
        try:
            driver.get(room_url)
            wait_for_page_ready(driver, timeout=20)
            dismiss_popups(driver)
            progressive_scroll(driver, steps=6, pause=0.6)
            dismiss_popups(driver)

            candidates = find_clickable_candidates(driver, DOWNLOAD_XPATHS, timeout=15)
            target_button = candidates[-1]

            before_snapshot = {str(p) for p in Path(download_dir).glob('*') if p.is_file()}
            safe_click(driver, target_button)

            try:
                alert = driver.switch_to.alert
                alert_text = alert.text.strip()
                alert.accept()
                LOGGER.warning("[%s] %s 경고창 확인 후 스킵: %s", brand, etf_name, alert_text)
                continue
            except NoAlertPresentException:
                pass

            new_file = wait_for_new_download(download_dir, before_snapshot, timeout=60)
            if not new_file:
                LOGGER.warning("[%s] %s 다운로드 파일 감지 실패", brand, etf_name)
                continue

            final_name = build_final_name(brand, etf_name, new_file, date_time, date_koact)
            final_path = base_dir / final_name
            move_file(new_file, final_path)
            LOGGER.info("[%s] %s 저장 완료 -> %s", brand, etf_name, final_path.name)

        except TimeoutException:
            LOGGER.exception("[%s] %s 다운로드 버튼 탐색 타임아웃", brand, etf_name)
        except Exception:
            LOGGER.exception("[%s] %s 처리 중 예외 발생", brand, etf_name)


def main() -> int:
    configure_logging()

    base_dir = Path(__file__).resolve().parent
    trading_date = get_target_trading_date()
    date_time = trading_date.strftime("%Y-%m-%d")
    date_koact = trading_date.strftime("%Y%m%d")

    LOGGER.info("작업 폴더: %s", base_dir)
    LOGGER.info("거래일 기준: TIME=%s / KoAct=%s", date_time, date_koact)

    download_dir = create_download_dir(base_dir)
    driver = None

    try:
        driver = build_driver(download_dir)
        for brand, rooms in TASKS:
            collect_brand(driver, brand, rooms, base_dir, download_dir, date_time, date_koact)
        LOGGER.info("ETF_collector 작업 종료")
        return 0
    finally:
        if driver is not None:
            driver.quit()
        cleanup_download_dir(download_dir)


if __name__ == "__main__":
    raise SystemExit(main())
