from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Dict, Optional, Tuple

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

LOGGER = logging.getLogger(__name__)

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

UNWANTED_NAME_PATTERN = re.compile(r"원화예금|현금|설정|해지", re.IGNORECASE)


def get_gspread_client(base_dir: str | Path | None = None) -> gspread.Client:
    raw = os.getenv("GOOGLE_KEY", "").strip()
    if raw:
        credentials = Credentials.from_service_account_info(json.loads(raw), scopes=GOOGLE_SCOPES)
        return gspread.authorize(credentials)

    candidate_dirs = []
    if base_dir:
        candidate_dirs.append(Path(base_dir))
    candidate_dirs.append(Path.cwd())

    for directory in candidate_dirs:
        json_path = directory / "google_key.json"
        if json_path.exists():
            return gspread.service_account(filename=str(json_path))

    raise RuntimeError("GOOGLE_KEY 환경변수 또는 google_key.json 파일이 필요합니다.")


def open_spreadsheet(spreadsheet_id: str, base_dir: str | Path | None = None):
    client = get_gspread_client(base_dir=base_dir)
    return client.open_by_key(spreadsheet_id)


def read_download_table(file_path: str | Path) -> pd.DataFrame:
    file_path = Path(file_path)
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        for encoding in ("utf-8-sig", "cp949", "utf-8"):
            try:
                return pd.read_csv(file_path, encoding=encoding, header=None)
            except Exception:
                continue
        raise ValueError(f"CSV 파일을 읽을 수 없습니다: {file_path.name}")

    if suffix in {".xlsx", ".xls", ".xlsm"}:
        try:
            return pd.read_excel(file_path, header=None)
        except Exception:
            pass

    try:
        html_tables = pd.read_html(file_path)
        if not html_tables:
            raise ValueError("표를 찾을 수 없습니다.")
        merged = pd.concat(html_tables, ignore_index=True)
        return merged
    except Exception as exc:
        raise ValueError(f"파일 파싱 실패: {file_path.name} / {exc}") from exc


def _clean_token(value) -> str:
    text = str(value)
    text = text.replace("\n", "")
    text = text.replace("\r", "")
    text = text.replace("\t", "")
    text = text.replace(" ", "")
    return text.strip()


def locate_header_row(df: pd.DataFrame) -> int:
    """
    ETF 파일마다 헤더 표기가 조금씩 달라서
    '종목명 + 비중' 완전일치가 아니라 점수 기반으로 헤더 행을 찾습니다.
    """
    name_keywords = ["종목명", "구성종목", "자산명", "자산", "종목", "종목코드", "자산코드"]
    weight_keywords = ["비중", "비중(%)", "구성비", "편입비", "평가비중", "투자비중"]
    qty_keywords = ["수량", "주식수", "계약수", "보유수량"]
    value_keywords = ["평가금액", "시가평가액", "금액", "평가액"]

    best_idx = None
    best_score = -1

    max_rows = min(len(df), 30)

    for idx in range(max_rows):
        row = df.iloc[idx]
        tokens = [_clean_token(v) for v in row.values if str(v).strip() and str(v).strip().lower() != "nan"]

        if not tokens:
            continue

        score = 0

        if any(any(k in token for k in name_keywords) for token in tokens):
            score += 3
        if any(any(k in token for k in weight_keywords) for token in tokens):
            score += 3
        if any(any(k in token for k in qty_keywords) for token in tokens):
            score += 1
        if any(any(k in token for k in value_keywords) for token in tokens):
            score += 1

        # 너무 짧은 행은 헤더일 가능성이 낮음
        if len(tokens) >= 3:
            score += 1

        if score > best_score:
            best_score = score
            best_idx = idx

    if best_idx is not None and best_score >= 6:
        return best_idx

    raise ValueError("헤더 행을 찾을 수 없습니다.")


def normalize_holdings_dataframe(raw_df: pd.DataFrame) -> Tuple[pd.DataFrame, str, str, Optional[str], Optional[str]]:
    header_idx = locate_header_row(raw_df)
    df = raw_df.copy()
    df.columns = df.iloc[header_idx]
    df = df.iloc[header_idx + 1 :].reset_index(drop=True)
    df.columns = [_clean_token(col) for col in df.columns]

    name_col = next(
        (c for c in df.columns if any(k in c for k in ["종목명", "구성종목", "자산명", "자산", "종목"])),
        None,
    )
    weight_col = next(
        (c for c in df.columns if any(k in c for k in ["비중", "비중(%)", "구성비", "편입비", "평가비중", "투자비중"])),
        None,
    )
    qty_col = next(
        (c for c in df.columns if any(k in c for k in ["수량", "주식수", "계약수", "보유수량"])),
        None,
    )
    value_col = next(
        (c for c in df.columns if any(k in c for k in ["평가금액", "시가평가액", "금액", "평가액"])),
        None,
    )

    if not name_col or not weight_col:
        raise ValueError("종목명/비중 컬럼을 찾지 못했습니다.")

    df = df[df[name_col].notna()].copy()
    df[name_col] = df[name_col].astype(str).str.strip()
    df = df[df[name_col] != ""]
    df = df[~df[name_col].str.contains(UNWANTED_NAME_PATTERN, na=False)]

    df[weight_col] = pd.to_numeric(
        df[weight_col].astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False),
        errors="coerce",
    ).fillna(0)

    if df[weight_col].sum() <= 2:
        df[weight_col] = df[weight_col] * 100

    df[weight_col] = df[weight_col].round(4)

    if qty_col:
        df[qty_col] = pd.to_numeric(
            df[qty_col].astype(str).str.replace(",", "", regex=False),
            errors="coerce",
        ).fillna(0)

    if value_col:
        df[value_col] = pd.to_numeric(
            df[value_col].astype(str).str.replace(",", "", regex=False),
            errors="coerce",
        ).fillna(0)

    return df.reset_index(drop=True), name_col, weight_col, qty_col, value_col


def load_existing_sheet_frame(spreadsheet, worksheet_name: str) -> tuple[Optional[gspread.Worksheet], pd.DataFrame]:
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        return None, pd.DataFrame()

    values = worksheet.get_all_values()
    if len(values) <= 1:
        return worksheet, pd.DataFrame()

    return worksheet, pd.DataFrame(values[1:], columns=values[0])


def parse_qty_from_change_cell(cell_value: str) -> Optional[int]:
    if not cell_value:
        return None
    match = re.search(r"\|\s*Q([\d,]+)", str(cell_value))
    if not match:
        return None
    return int(match.group(1).replace(",", ""))


def extract_previous_qty_map(existing_df: pd.DataFrame) -> Dict[str, Optional[int]]:
    if existing_df.empty:
        return {}

    last_row = existing_df.iloc[-1].to_dict()
    qty_map: Dict[str, Optional[int]] = {}
    for column in existing_df.columns:
        if column == "Date" or column.endswith("_증감"):
            continue
        qty_map[column] = parse_qty_from_change_cell(last_row.get(f"{column}_증감", ""))
    return qty_map


def extract_previous_qty_map_korean(existing_values: list[list[str]]) -> Dict[str, Optional[int]]:
    if len(existing_values) <= 1:
        return {}

    headers = existing_values[0]
    last_row = existing_values[-1]
    qty_map: Dict[str, Optional[int]] = {}
    for idx in range(1, len(headers), 2):
        stock = headers[idx]
        cell = last_row[idx + 1] if idx + 1 < len(last_row) else ""
        qty_map[stock] = parse_qty_from_change_cell(cell)
    return qty_map


def ensure_worksheet(spreadsheet, title: str, rows: int = 2000, cols: int = 200):
    try:
        return spreadsheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        LOGGER.info("시트가 없어 새로 생성합니다: %s", title)
        return spreadsheet.add_worksheet(title=title, rows=str(rows), cols=str(cols))
