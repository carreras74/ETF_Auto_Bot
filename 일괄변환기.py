from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Dict, List

import FinanceDataReader as fdr
import pandas as pd

from common_selenium import configure_logging
from etf_data_utils import (
    ensure_worksheet,
    extract_previous_qty_map,
    load_existing_sheet_frame,
    normalize_holdings_dataframe,
    open_spreadsheet,
    read_download_table,
)

LOGGER = logging.getLogger(__name__)
SPREADSHEET_ID = "1ZxIYeERuOWOWZudyjpMWpEWA0eljOct_uO9gXg6_2JA"


def extract_date_from_filename(filename: str) -> str | None:
    match = re.search(r"(\d{4}-\d{2}-\d{2}|\d{8})", filename)
    if not match:
        return None

    raw = match.group(1)
    if len(raw) == 8:
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    return raw


def is_weekday(date_str: str) -> bool:
    dt = pd.to_datetime(date_str, format="%Y-%m-%d", errors="coerce")
    return pd.notna(dt) and dt.weekday() < 5


def extract_etf_name(filename: str) -> str:
    stem = Path(filename).stem
    stem = re.sub(r"\d{4}-\d{2}-\d{2}|\d{8}", "", stem)
    stem = stem.replace("구성종목(PDF)TIME", "")
    stem = stem.replace("구성종목(PDF)", "")
    stem = stem.replace("KoAct ", "")
    stem = stem.replace("TIME", "")
    stem = stem.replace("PDF", "")
    stem = re.sub(r"[()_\-]", " ", stem)
    stem = re.sub(r"\s+", " ", stem).strip()
    return stem


def list_source_files(base_dir: Path) -> Dict[str, List[dict]]:
    groups: Dict[str, List[dict]] = {}

    for path in base_dir.iterdir():
        if not path.is_file():
            continue
        if path.suffix.lower() not in {".csv", ".xlsx", ".xls", ".xlsm", ".html"}:
            continue
        if not ("TIME" in path.name or "KoAct" in path.name):
            continue
        if any(token in path.name for token in ["30일추적", "변환완료", "통합완료"]):
            continue

        file_date = extract_date_from_filename(path.name)
        if not file_date or not is_weekday(file_date):
            LOGGER.info("주말/날짜불명 파일 스킵: %s", path.name)
            continue

        etf_name = extract_etf_name(path.name)
        groups.setdefault(etf_name, []).append({"file": path, "date": file_date})

    return groups


def load_market_reference() -> tuple[dict, dict]:
    krx_df = fdr.StockListing("KRX")
    latest_price_map = {}
    name_to_code = {}

    for _, row in krx_df.iterrows():
        name = str(row["Name"]).strip()
        latest_price_map[name] = {
            "Close": row.get("Close", 0),
            "ChagesRatio": row.get("ChagesRatio", 0),
        }
        name_to_code[name] = str(row["Code"])

    return latest_price_map, name_to_code


def build_history_cache(stock_names: set[str], start_date: str, name_to_code: dict) -> dict:
    cache = {}

    for stock_name in stock_names:
        code = name_to_code.get(stock_name)
        if not code:
            cache[stock_name] = {}
            continue

        try:
            hist = fdr.DataReader(code, start_date)
            hist.index = hist.index.strftime("%Y-%m-%d")
            cache[stock_name] = hist[["Close", "Change"]].to_dict("index")
        except Exception:
            cache[stock_name] = {}

    return cache


def format_change(diff: int | None, price_text: str, qty: int) -> str:
    if diff is None:
        return f"0{price_text} | Q{qty:,}"
    if diff > 0:
        return f"🔴▲ {diff:,}{price_text} | Q{qty:,}"
    if diff < 0:
        return f"🔵▼ {abs(diff):,}{price_text} | Q{qty:,}"
    return f"0{price_text} | Q{qty:,}"


def build_price_suffix(
    stock_name: str,
    file_date: str,
    is_last_day: bool,
    history_cache: dict,
    latest_price_map: dict,
) -> str:
    if stock_name in history_cache and file_date in history_cache[stock_name]:
        close_price = history_cache[stock_name][file_date].get("Close", 0)
        change_pct = history_cache[stock_name][file_date].get("Change", 0) * 100
        return f" | ₩{int(close_price):,} ({change_pct:+.2f}%)"

    if is_last_day and stock_name in latest_price_map:
        close_price = latest_price_map[stock_name].get("Close", 0)
        change_pct = latest_price_map[stock_name].get("ChagesRatio", 0)
        return f" | ₩{int(close_price):,} ({change_pct:+.2f}%)"

    return ""


def main() -> int:
    configure_logging()
    base_dir = Path(__file__).resolve().parent
    LOGGER.info("작업 폴더: %s", base_dir)

    spreadsheet = None
    try:
        spreadsheet = open_spreadsheet(SPREADSHEET_ID, base_dir=base_dir)
        LOGGER.info("구글 시트 연결 성공")
    except Exception as exc:
        LOGGER.warning("구글 시트 연결 실패. 로컬 CSV만 생성합니다: %s", exc)

    latest_price_map, name_to_code = load_market_reference()
    LOGGER.info("KRX 종목 정보 로딩 완료: %d건", len(name_to_code))

    etf_groups = list_source_files(base_dir)
    if not etf_groups:
        LOGGER.error("대상 원본 파일이 없습니다.")
        return 1

    for etf_name, files_info in sorted(etf_groups.items()):
        LOGGER.info("[%s] 분석 시작", etf_name)
        files_info.sort(key=lambda item: item["date"])

        try:
            worksheet = None
            existing_df = pd.DataFrame()

            if spreadsheet is not None:
                worksheet, existing_df = load_existing_sheet_frame(spreadsheet, etf_name)

            last_sheet_date = (
                existing_df["Date"].max()
                if (not existing_df.empty and "Date" in existing_df.columns)
                else "1900-01-01"
            )

            target_files = [item for item in files_info if item["date"] > last_sheet_date]
            if not target_files:
                LOGGER.info("[%s] 이미 최신 상태 (마지막 날짜=%s)", etf_name, last_sheet_date)
                continue

            stock_names = set()
            valid_target_files = []

            for item in target_files:
                try:
                    raw_df = read_download_table(item["file"])
                    df, name_col, weight_col, _, _ = normalize_holdings_dataframe(raw_df)
                    top20 = df.sort_values(by=weight_col, ascending=False).head(20)
                    stock_names.update(top20[name_col].astype(str).tolist())
                    valid_target_files.append(item)
                except Exception as exc:
                    LOGGER.warning("[%s] 사전 스캔 실패: %s / %s", etf_name, item["file"].name, exc)

            if not valid_target_files:
                LOGGER.warning("[%s] 처리 가능한 신규 파일이 없어 스킵", etf_name)
                continue

            history_cache = build_history_cache(stock_names, valid_target_files[0]["date"], name_to_code)
            previous_qty_map = extract_previous_qty_map(existing_df)
            ordered_stocks = (
                [col for col in existing_df.columns if col != "Date" and not col.endswith("_증감")]
                if not existing_df.empty
                else []
            )

            rows_to_append = []

            for idx, item in enumerate(valid_target_files):
                file_path = item["file"]
                file_date = item["date"]
                is_last_day = idx == len(valid_target_files) - 1

                try:
                    raw_df = read_download_table(file_path)
                    df, name_col, weight_col, qty_col, _ = normalize_holdings_dataframe(raw_df)

                    if not qty_col:
                        LOGGER.warning("[%s] 수량 컬럼이 없어 파일 스킵: %s", etf_name, file_path.name)
                        continue

                except Exception as exc:
                    LOGGER.warning("[%s] 본처리 실패로 파일 스킵: %s / %s", etf_name, file_path.name, exc)
                    continue

                df = df.sort_values(by=weight_col, ascending=False).head(20).reset_index(drop=True)

                today_map = {
                    row[name_col]: {
                        "weight": float(row[weight_col]),
                        "qty": int(row[qty_col]),
                    }
                    for _, row in df.iterrows()
                }

                for stock in today_map:
                    if stock not in ordered_stocks:
                        ordered_stocks.append(stock)

                row_dict = {"Date": file_date}
                next_previous_qty = previous_qty_map.copy()

                for stock in ordered_stocks:
                    current = today_map.get(stock)
                    if current:
                        weight = current["weight"]
                        qty = current["qty"]
                    else:
                        weight = 0
                        qty = 0

                    prev_qty = previous_qty_map.get(stock)
                    diff = 0 if prev_qty is None else qty - prev_qty
                    price_suffix = build_price_suffix(
                        stock,
                        file_date,
                        is_last_day,
                        history_cache,
                        latest_price_map,
                    )

                    row_dict[stock] = weight
                    row_dict[f"{stock}_증감"] = format_change(diff, price_suffix, qty)
                    next_previous_qty[stock] = qty

                previous_qty_map = next_previous_qty
                rows_to_append.append(row_dict)

            if not rows_to_append:
                LOGGER.warning("[%s] 최종 반영할 행이 없어 스킵", etf_name)
                continue

            final_columns = ["Date"]
            for stock in ordered_stocks:
                final_columns.extend([stock, f"{stock}_증감"])

            new_df = pd.DataFrame(rows_to_append, columns=final_columns)
            final_df = pd.concat([existing_df, new_df], ignore_index=True) if not existing_df.empty else new_df
            final_df = final_df.reindex(columns=final_columns)

            out_path = base_dir / f"통합완료_{etf_name}.csv"
            final_df.to_csv(out_path, index=False, encoding="utf-8-sig")
            LOGGER.info("[%s] 로컬 저장 완료 -> %s", etf_name, out_path.name)

            if spreadsheet is not None:
                worksheet = worksheet or ensure_worksheet(spreadsheet, etf_name, rows=2000, cols=300)
                worksheet.clear()
                worksheet.update(
                    range_name="A1",
                    values=[final_df.fillna("").columns.tolist()] + final_df.fillna("").values.tolist(),
                )
                LOGGER.info("[%s] 구글 시트 업로드 완료", etf_name)

        except Exception as exc:
            LOGGER.exception("[%s] ETF 단위 처리 실패: %s", etf_name, exc)
            continue

    LOGGER.info("일괄변환기 종료")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

