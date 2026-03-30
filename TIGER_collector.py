from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from selenium.common.exceptions import NoAlertPresentException, TimeoutException

from common_selenium import (
    build_driver,
    cleanup_download_dir,
    configure_logging,
    create_download_dir,
    dismiss_popups,
    find_clickable_candidates,
    get_target_trading_date,
    progressive_scroll,
    safe_click,
    wait_for_new_download,
    wait_for_page_ready,
)
from etf_data_utils import (
    ensure_worksheet,
    extract_previous_qty_map_korean,
    normalize_holdings_dataframe,
    open_spreadsheet,
    read_download_table,
)

LOGGER = logging.getLogger(__name__)
SPREADSHEET_ID = "1ZxIYeERuOWOWZudyjpMWpEWA0eljOct_uO9gXg6_2JA"

TIGER_ROOMS = {
    "TIGER 기술이전바이오액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7387280001",
    "TIGER 코리아테크액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7365040005",
    "TIGER 퓨처모빌리티액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7471780007",
}

DOWNLOAD_XPATHS = [
    "//a[contains(@class, 'excel') or contains(@class, 'xls') or contains(@href, 'excel') or contains(@href, 'xls') or contains(translate(normalize-space(.), 'EXCEL', 'excel'), 'excel') or contains(normalize-space(.), '엑셀')]",
    "//button[contains(@class, 'excel') or contains(@class, 'xls') or contains(translate(normalize-space(.), 'EXCEL', 'excel'), 'excel') or contains(normalize-space(.), '엑셀')]",
    "//span[contains(normalize-space(.), '엑셀')]/ancestor::a[1]",
    "//img[contains(@alt, '엑셀') or contains(translate(@alt, 'EXCEL', 'excel'), 'excel')]/ancestor::a[1]",
    "//*[@title='엑셀 다운로드' or contains(@title, 'excel')]/self::a | //*[@title='엑셀 다운로드' or contains(@title, 'excel')]/self::button",
]


def format_change(diff: int, price: int, qty: int) -> str:
    price_str = f"₩{int(price):,}"
    if diff > 0:
        return f"🔴▲{diff:,} | {price_str} | Q{int(qty)}"
    if diff < 0:
        return f"🔵▼{abs(diff):,} | {price_str} | Q{int(qty)}"
    return f"0 | {price_str} | Q{int(qty)}"


def calculate_price(df: pd.DataFrame, qty_col: str | None, value_col: str | None) -> pd.Series:
    if not qty_col or not value_col:
        return pd.Series([0] * len(df), index=df.index)

    qty = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)
    value = pd.to_numeric(df[value_col], errors="coerce").fillna(0)

    prices = pd.Series([0] * len(df), index=df.index)
    valid = qty > 0
    prices.loc[valid] = (value.loc[valid] / qty.loc[valid]).fillna(0).astype(int)
    return prices


def update_sheet(spreadsheet, etf_name: str, formatted_date: str, today_dict: dict[str, dict]) -> None:
    ws = ensure_worksheet(spreadsheet, etf_name, rows=2000, cols=200)
    raw_values = ws.get_all_values()
    existing_values = [row for row in raw_values if any(str(cell).strip() for cell in row)]

    if not existing_values:
        headers = ["일자"]
        row_data = [formatted_date]

        for stock, data in today_dict.items():
            headers.extend([stock, f"{stock}_증감"])
            row_data.extend([data["비중"], format_change(0, data["주가"], data["수량"])])

        ws.update(range_name="A1", values=[headers, row_data])
        LOGGER.info("[%s] 첫 데이터 업로드 완료", etf_name)
        return

    headers = existing_values[0]
    last_row = existing_values[-1]

    if last_row and last_row[0] == formatted_date:
        LOGGER.info("[%s] %s 데이터가 이미 있어 스킵", etf_name, formatted_date)
        return

    new_stocks = [stock for stock in today_dict if stock not in headers]
    if new_stocks:
        for stock in new_stocks:
            headers.extend([stock, f"{stock}_증감"])
        ws.update(range_name="A1", values=[headers])

    previous_qty = extract_previous_qty_map_korean(existing_values)
    new_row = [formatted_date] + [""] * (len(headers) - 1)

    for stock, data in today_dict.items():
        idx = headers.index(stock)
        curr_qty = int(data["수량"])
        prev_qty = previous_qty.get(stock)
        diff = 0 if prev_qty is None else curr_qty - prev_qty

        new_row[idx] = data["비중"]
        new_row[idx + 1] = format_change(diff, data["주가"], curr_qty)

    ws.append_row(new_row)
    LOGGER.info("[%s] 구글 시트 업데이트 완료", etf_name)


def main() -> int:
    configure_logging()

    base_dir = Path(__file__).resolve().parent
    download_dir = create_download_dir(base_dir)
    trading_date = get_target_trading_date()
    formatted_date = trading_date.strftime("%Y-%m-%d")

    LOGGER.info("TIGER 자동 수집기 시작 | 기준일=%s", formatted_date)
    spreadsheet = open_spreadsheet(SPREADSHEET_ID, base_dir=base_dir)

    driver = None
    try:
        driver = build_driver(download_dir)
        before_global = set()

        for etf_name, room_url in TIGER_ROOMS.items():
            LOGGER.info("[%s] 수집 시작", etf_name)
            try:
                driver.get(room_url)
                wait_for_page_ready(driver, timeout=20)
                dismiss_popups(driver)
                progressive_scroll(driver, steps=7, pause=0.7)
                dismiss_popups(driver)

                buttons = find_clickable_candidates(driver, DOWNLOAD_XPATHS, timeout=20)
                target_button = buttons[-1]

                before_snapshot = {str(p) for p in Path(download_dir).glob('*') if p.is_file()} | before_global
                safe_click(driver, target_button)

                try:
                    alert = driver.switch_to.alert
                    alert_text = alert.text.strip()
                    alert.accept()
                    LOGGER.warning("[%s] 경고창 확인 후 스킵: %s", etf_name, alert_text)
                    continue
                except NoAlertPresentException:
                    pass

                downloaded_file = wait_for_new_download(download_dir, before_snapshot, timeout=60)
                if not downloaded_file:
                    LOGGER.warning("[%s] 다운로드 파일 감지 실패", etf_name)
                    continue

                before_global.add(downloaded_file)

                raw_df = read_download_table(downloaded_file)
                df, name_col, weight_col, qty_col, value_col = normalize_holdings_dataframe(raw_df)

                if not qty_col:
                    LOGGER.warning("[%s] 수량 컬럼이 없어 스킵", etf_name)
                    continue

                df["주가"] = calculate_price(df, qty_col, value_col)
                df = df.sort_values(by=weight_col, ascending=False).reset_index(drop=True)

                today_dict = {
                    row[name_col]: {
                        "비중": float(row[weight_col]),
                        "수량": int(row[qty_col]),
                        "주가": int(row["주가"]),
                    }
                    for _, row in df.iterrows()
                }

                update_sheet(spreadsheet, etf_name, formatted_date, today_dict)

            except TimeoutException:
                LOGGER.exception("[%s] 버튼 탐색 타임아웃", etf_name)
            except Exception:
                LOGGER.exception("[%s] 처리 실패", etf_name)

        LOGGER.info("TIGER 작업 종료")
        return 0

    finally:
        if driver is not None:
            driver.quit()
        cleanup_download_dir(download_dir)


if __name__ == "__main__":
    raise SystemExit(main())

