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
print("🚀 [초강력 데이터 파싱 + 순정 포맷 복원 모터] 실행 중...\n")
print(f"🇰🇷 한국 표준 시간: {now_kst.strftime('%Y-%m-%d %H:%M:%S')}\n")

print("🌐 구글 시트 접속 중...")
try:
    gc = gspread.service_account(filename=os.path.join(current_folder, 'google_key.json'))
    SHEET_URL = 'https://docs.google.com/spreadsheets/d/1ZxIYeERuOWOWZudyjpMWpEWA0eljOct_uO9gXg6_2JA/edit?gid=1831966955#gid=1831966955' 
    sh = gc.open_by_url(SHEET_URL)
    google_connected = True
    print("✅ 구글 시트 접속 완료!")
except:
    google_connected = False

print("📈 주식 시장 데이터 매핑 중...")
try:
    krx_df = fdr.StockListing('KRX')
    krx_dict = {}
    for _, row in krx_df.iterrows():
        clean_name = str(row['Name']).replace(' ', '').strip()
        krx_dict[clean_name] = {'Close': row['Close'], 'Ratio': row['ChagesRatio']}
    print(f"✅ {len(krx_dict):,}개 종목 매핑 완료!\n")
except:
    krx_dict = {}

all_files = [f for f in glob.glob(os.path.join(current_folder, "*.*"))
             if f.endswith(('.csv', '.xlsx', '.xls')) 
             and ("TIME" in f or "KoAct" in f) 
             and "30일추적" not in f and "변환완료" not in f and "통합완료" not in f]

if not all_files:
    print("❌ 변환할 원본 파일이 없습니다.")
    exit()

etf_groups = {}
for f in all_files:
    fname = os.path.basename(f)
    date_match = re.search(r'(\d{4}-\d{2}-\d{2}|\d{8})', fname)
    if not date_match: continue
    raw_date = date_match.group()
    file_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}" if len(raw_date) == 8 else raw_date
    if datetime.strptime(file_date, "%Y-%m-%d").weekday() >= 5: continue
    
    etf_name = re.sub(r'구성종목|PDF|기준\s*가격|\d{4}-\d{2}-\d{2}|\d{8}|\.xlsx|\.csv|\.xls|[()_\-\s]', '', fname).strip()
    if etf_name not in etf_groups: etf_groups[etf_name] = []
    etf_groups[etf_name].append({'file': f, 'date': file_date})

def read_etf_data(filepath):
    df = pd.read_csv(filepath, header=None) if filepath.endswith('.csv') else pd.read_excel(filepath, header=None)
    header_idx = 0
    for i, row in df.iterrows():
        row_strs = [str(x).replace(' ', '') for x in row.values]
        if any('종목' in s or '자산' in s for s in row_strs) and any('비중' in s for s in row_strs):
            header_idx = i
            break
    df.columns = df.iloc[header_idx]
    df = df.iloc[header_idx+1:].reset_index(drop=True)
    
    # 💡 [핵심 패치 1] 컬럼명에서 모든 공백 무시 ('주식 수' -> '주식수')
    df.columns = [str(c).replace(' ', '').strip() for c in df.columns]
    
    n_col = next((c for c in df.columns if '종목' in c or '자산' in c), None)
    w_col = next((c for c in df.columns if '비중' in c), None)
    q_col = next((c for c in df.columns if '수량' in c or '주식수' in c or '계약수' in c), None)
    
    if n_col and w_col:
        # 💡 [핵심 패치 2] 종목명에서 모든 공백 무시 ('삼성전자 ' -> '삼성전자')
        df[n_col] = df[n_col].astype(str).str.replace(' ', '').str.strip()
        
        # 💡 [핵심 패치 3] 비중과 수량에서 %나 주 같은 글자 다 부수고 순수 숫자만 추출!
        df[w_col] = pd.to_numeric(df[w_col].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce').fillna(0)
        if df[w_col].sum() <= 2.0: df[w_col] = df[w_col] * 100
        
        if q_col:
            df[q_col] = pd.to_numeric(df[q_col].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce').fillna(0)
        return df, n_col, w_col, q_col
    raise ValueError("필수 컬럼 부족")

for etf_name, files_info in etf_groups.items():
    print(f"▶️ [{etf_name}] 작업 시작...")
    files_info.sort(key=lambda x: x['date'])
    
    try:
        existing_df = pd.DataFrame()
        worksheet = None
        if google_connected:
            try:
                worksheet = sh.worksheet(etf_name)
                data = worksheet.get_all_values()
                if len(data) > 1: existing_df = pd.DataFrame(data[1:], columns=data[0])
            except: pass
        
        last_gs_date = existing_df['Date'].max() if not existing_df.empty else "1900-01-01"
        historical_cols = [c for c in existing_df.columns if c != 'Date' and not c.endswith('_증감')] if not existing_df.empty else []
        
        target_files = [f for f in files_info if f['date'] > last_gs_date]
        if not target_files: continue

        prev_qty = {}
        past_files = [f for f in files_info if f['date'] <= last_gs_date]
        if past_files:
            last_past_file = past_files[-1]
            try:
                p_df, p_n_col, _, p_q_col = read_etf_data(last_past_file['file'])
                if p_q_col:
                    past_data = p_df.set_index(p_n_col).to_dict('index')
                    for st_name, row_data in past_data.items():
                        clean_st = st_name.replace(' ', '').strip()
                        prev_qty[clean_st] = row_data[p_q_col]
            except: pass

        for info in target_files:
            r_df, r_n_col, r_w_col, r_q_col = read_etf_data(info['file'])
            r_df = r_df[r_df[r_n_col].astype(str).str.strip() != '']
            today_top20 = r_df.dropna(subset=[r_n_col]).sort_values(by=r_w_col, ascending=False).head(20)
            today_data = r_df.set_index(r_n_col).to_dict('index')
            
            for st in today_top20[r_n_col]:
                clean_st = st.replace(' ', '').strip()
                # 💡 구글 시트에 없는 종목만 새로 추가
                if not any(clean_st == existing_st.replace(' ', '').strip() for existing_st in historical_cols):
                    historical_cols.append(clean_st)
            
            row_dict = {'Date': info['date']}
            for st in historical_cols:
                clean_st = st.replace(' ', '').strip()
                w, q = 0, 0
                if clean_st in today_data:
                    w = today_data[clean_st][r_w_col]
                    q = today_data[clean_st][r_q_col] if r_q_col else 0
                
                price_str = ""
                if clean_st in krx_dict:
                    price = krx_dict[clean_st]['Close']
                    ratio = krx_dict[clean_st]['Ratio']
                    price_str = f" | ₩{int(price):,} ({ratio:+.2f}%)"
                
                diff = q - prev_qty.get(clean_st, 0)
                
                if diff > 0: diff_str = f"🔴▲ {int(diff):,}{price_str}"
                elif diff < 0: diff_str = f"🔵▼ {abs(int(diff)):,}{price_str}"
                else: diff_str = f"0{price_str}"
                
                row_dict[st] = w
                row_dict[f"{st}_증감"] = diff_str
                
                if q > 0: prev_qty[clean_st] = q
            
            new_row_df = pd.DataFrame([row_dict])
            existing_df = pd.concat([existing_df, new_row_df], ignore_index=True)

        if google_connected:
            final_cols = ['Date']
            for c in historical_cols:
                final_cols.extend([c, f"{c}_증감"])
            final_df = existing_df.reindex(columns=final_cols).fillna("")
            
            if worksheet is None: worksheet = sh.add_worksheet(title=etf_name, rows="1000", cols="100")
            worksheet.clear()
            worksheet.update(values=[final_df.columns.values.tolist()] + final_df.values.tolist(), range_name="A1")
            print(f"✅ [{etf_name}] 구글 시트 업데이트 성공!\n")
            
    except Exception as e:
        print(f"❌ [{etf_name}] 실패: {e}")

print("🎉 모든 공정이 완료되었습니다.")
