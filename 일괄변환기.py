import os
import sys
import subprocess
import time
from datetime import datetime, timedelta, timezone

def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try: import xlrd
except ImportError: install_package("xlrd")

try: import gspread
except ImportError:
    install_package("gspread")
    import gspread

try: import FinanceDataReader as fdr
except ImportError:
    install_package("finance-datareader")
    import FinanceDataReader as fdr

import pandas as pd
import glob
import re

try: current_folder = os.path.dirname(os.path.abspath(__file__))
except: current_folder = os.getcwd()

KST = timezone(timedelta(hours=9))
now_kst = datetime.now(KST)

print(f"📂 작업 폴더: {current_folder}")
print("🚀 [TIME & KoAct 전용 스마트 엔진] 가동 중...\n")

print("=========================================")
print("🌐 구글 시트 접속을 시도합니다...")
try:
    gc = gspread.service_account(filename=os.path.join(current_folder, 'google_key.json'))
    SHEET_URL = 'https://docs.google.com/spreadsheets/d/1ZxIYeERuOWOWZudyjpMWpEWA0eljOct_uO9gXg6_2JA/edit?gid=1831966955#gid=1831966955' 
    sh = gc.open_by_url(SHEET_URL)
    google_connected = True
    print(f"✅ 구글 시트 접속 완료! 문이 열렸습니다.")
except Exception as e:
    print(f"⚠️ 구글 접속 실패: {e}")
    google_connected = False
print("=========================================\n")

global_qty_backup = {}
backup_ws = None
if google_connected:
    try:
        all_ws = sh.worksheets()
        backup_ws = next((ws for ws in all_ws if ws.title.replace(' ', '') == "수량백업(봇전용)"), None)
        if backup_ws:
            data = backup_ws.get_all_values()
            if len(data) > 1:
                for row in data[1:]:
                    if len(row) >= 3:
                        etf, stock, qty = row[0], row[1], row[2]
                        if etf not in global_qty_backup: global_qty_backup[etf] = {}
                        try: global_qty_backup[etf][stock] = int(qty)
                        except: pass
    except: pass

print("📈 한국거래소(KRX) 전체 종목코드 매핑 중...")
try:
    krx_df = fdr.StockListing('KRX')
    krx_dict = {}
    name_to_code = {} 
    for _, row in krx_df.iterrows():
        name = str(row['Name']).replace(' ', '').strip()
        krx_dict[name] = {'Close': row['Close'], 'ChagesRatio': row['ChagesRatio']}
        name_to_code[name] = str(row['Code'])
    print(f"✅ 총 {len(krx_dict):,}개 종목 코드 장전 완료!\n")
except Exception as e:
    krx_dict = {}; name_to_code = {}

# 💡 [필터링 고도화] 오직 TIME과 KoAct 파일만 수집 대상으로 한정합니다.
all_files = [f for f in glob.glob(os.path.join(current_folder, "*.*"))
             if f.endswith(('.csv', '.xlsx', '.xls')) 
             and ("TIME" in f or "KoAct" in f) 
             and "30일추적" not in f and "변환완료" not in f and "통합완료" not in f]

if not all_files:
    print("❌ 폴더에 분석할 TIME/KoAct 파일이 없습니다.")
    exit()

etf_groups = {}
for f in all_files:
    fname = os.path.basename(f)
    date_match = re.search(r'(\d{4}-\d{2}-\d{2}|\d{8})', fname)
    if not date_match: continue
    raw_date = date_match.group()
    file_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}" if len(raw_date) == 8 else raw_date
    etf_name = re.sub(r'구성종목|PDF|기준\s*가격|\d{4}-\d{2}-\d{2}|\d{8}|\.xlsx|\.csv|\.xls|[()_\-\s]', '', fname).strip()
    if etf_name not in etf_groups: etf_groups[etf_name] = []
    etf_groups[etf_name].append({'file': f, 'date': file_date})

def read_etf_data(filepath):
    if filepath.endswith('.csv'):
        try: df = pd.read_csv(filepath, encoding='utf-8-sig', header=None)
        except: df = pd.read_csv(filepath, encoding='cp949', header=None)
    else:
        df = pd.read_excel(filepath, header=None)
    
    header_idx = 0
    for i, row in df.iterrows():
        row_strs = [str(x).replace(' ', '') for x in row.values]
        if any('종목' in s or '자산' in s for s in row_strs) and any('비중' in s for s in row_strs):
            header_idx = i; break
    df.columns = df.iloc[header_idx]
    df = df.iloc[header_idx+1:].reset_index(drop=True)
    df.columns = [str(c).replace(' ', '').replace('\n', '').strip() for c in df.columns]
    n_col = next((c for c in df.columns if '종목명' in c or '자산명' in c), None)
    w_col = next((c for c in df.columns if '비중' in c), None)
    q_col = next((c for c in df.columns if any(k in c for k in ['수량', '주식수', '계약수'])), None)
    
    if n_col and w_col:
        df[w_col] = pd.to_numeric(df[w_col].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce').fillna(0)
        if df[w_col].sum() <= 2.0: df[w_col] = df[w_col] * 100
        df[w_col] = df[w_col].round(1)
        if q_col: df[q_col] = pd.to_numeric(df[q_col].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce').fillna(0)
        return df, n_col, w_col, q_col
    return None, None, None, None

for etf_name, files_info in etf_groups.items():
    print(f"▶️ [{etf_name}] 분석 시작...")
    files_info.sort(key=lambda x: x['date'])
    try:
        existing_df = pd.DataFrame(); worksheet = None
        if google_connected:
            all_ws = sh.worksheets()
            worksheet = next((ws for ws in all_ws if ws.title.replace(' ', '') == etf_name), None)
            if worksheet:
                data = worksheet.get_all_values()
                if len(data) > 1: existing_df = pd.DataFrame(data[1:], columns=data[0])
        
        last_gs_date = existing_df['Date'].max() if not existing_df.empty else "1900-01-01"
        historical_cols = [c for c in existing_df.columns if c != 'Date' and not c.endswith('_증감')] if not existing_df.empty else []
        target_files = [f for f in files_info if f['date'] > last_gs_date]
        if not target_files:
            print(f"   ✅ 최신 상태입니다. 스킵!"); continue

        prev_qty = global_qty_backup.get(etf_name, {})
        new_dates = [f['date'] for f in target_files]
        all_stocks_in_new_files = set()
        for info in target_files:
            r_df, r_n_col, r_w_col, _ = read_etf_data(info['file'])
            if r_df is not None:
                target_names = r_df[r_df[r_w_col] >= 1.0].dropna(subset=[r_n_col])[r_n_col].tolist()
                all_stocks_in_new_files.update(target_names)
        
        global_stock_hist_cache = {}
        for st_name in all_stocks_in_new_files:
            code = name_to_code.get(st_name.replace(' ', '').strip())
            if code:
                try:
                    temp_df = fdr.DataReader(code, min(new_dates))
                    temp_df.index = temp_df.index.strftime('%Y-%m-%d')
                    global_stock_hist_cache[st_name] = temp_df[['Close', 'Change']].to_dict('index')
                except: global_stock_hist_cache[st_name] = {}

        all_rows = []; historical_new_cols = list(historical_cols) 
        for i, info in enumerate(target_files):
            r_df, r_n_col, r_w_col, r_q_col = read_etf_data(info['file'])
            if r_df is None: continue
            today_target = r_df[r_df[r_w_col] >= 1.0].sort_values(by=r_w_col, ascending=False)
            for st_name in today_target[r_n_col]:
                if st_name not in historical_new_cols: historical_new_cols.append(st_name)
            row_dict = {'Date': info['date']}; today_data = r_df.set_index(r_n_col).to_dict('index')
            
            for st_name in historical_new_cols:
                if st_name in today_data:
                    w, q = today_data[st_name][r_w_col], today_data[st_name][r_q_col] if r_q_col else 0
                else: w, q = 0, 0
                p_val, r_val = 0, 0.0
                clean_st = st_name.replace(' ', '').strip() 
                if st_name in global_stock_hist_cache and info['date'] in global_stock_hist_cache[st_name]:
                    p_val = global_stock_hist_cache[st_name][info['date']]['Close']
                    r_val = global_stock_hist_cache[st_name][info['date']]['Change'] * 100
                elif clean_st in krx_dict:
                    p_val, r_val = krx_dict[clean_st]['Close'], krx_dict[clean_st]['ChagesRatio']
                
                try: p_int = int(float(str(p_val).replace(',', '')))
                except: p_int = 0
                try: r_float = float(str(r_val).replace(',', '').replace('%', ''))
                except: r_float = 0.0
                price_str = f" | ₩{p_int:,} ({r_float:+.2f}%)" if p_int > 0 else ""
                diff = int(float(str(q).replace(',', ''))) - int(float(str(prev_qty.get(st_name, 0)).replace(',', '')))
                if i == 0 and existing_df.empty: diff_str = f"0{price_str}"
                else:
                    if diff > 0: diff_str = f"🔴▲ {diff:,}{price_str}"
                    elif diff < 0: diff_str = f"🔵▼ {abs(diff):,}{price_str}"
                    else: diff_str = f"0{price_str}"
                row_dict[st_name], row_dict[f"{st_name}_증감"] = w, diff_str
                
            if r_q_col:
                if etf_name not in global_qty_backup: global_qty_backup[etf_name] = {}
                for st_name, row_data in today_data.items(): global_qty_backup[etf_name][st_name] = row_data[r_q_col]
            all_rows.append(row_dict)
            
        final_cols = ['Date']
        for col in historical_new_cols: final_cols.extend([col, f"{col}_증감"])
        new_df = pd.DataFrame(all_rows, columns=final_cols)
        final_df = pd.concat([existing_df, new_df], axis=0, ignore_index=True).reindex(columns=final_cols) if not existing_df.empty else new_df
        
        if google_connected:
            if not worksheet: worksheet = sh.add_worksheet(title=etf_name, rows="1000", cols="100")
            final_df_gs = final_df.fillna("")
            worksheet.clear()
            worksheet.update(values=[final_df_gs.columns.values.tolist()] + final_df_gs.values.tolist(), range_name="A1")
            print(f"   => 🌐 구글 시트 업데이트 성공!\n")
    except Exception as e: print(f"❌ 실패 [{etf_name}]: {e}\n")

if google_connected and backup_ws:
    backup_rows = [["ETF", "종목명", "수량"]]
    for etf, stocks in global_qty_backup.items():
        for st, q in stocks.items(): backup_rows.append([etf, st, q])
    backup_ws.clear(); backup_ws.update(values=backup_rows, range_name="A1")

print("🎉 불필요한 노이즈 제거 완료! TIME과 KoAct만으로 정갈하게 통합되었습니다.")
