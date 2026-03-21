import os
import sys
import subprocess
import time

def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try: import xlrd
except ImportError: install_package("xlrd")

try: import gspread
except ImportError:
    install_package("gspread")
    import gspread

# 💡 [시즌 2 핵심 엔진] 파이낸스 데이터 리더 장착
try: import FinanceDataReader as fdr
except ImportError:
    install_package("finance-datareader")
    import FinanceDataReader as fdr

import pandas as pd
import glob
import re

try: current_folder = os.path.dirname(os.path.abspath(__file__))
except: current_folder = os.getcwd()

print(f"📂 작업 폴더: {current_folder}")
print("🚀 [비중 + 수량증감 + 주가 추적 모터] 시즌2 변환기 실행 중...\n")

print("=========================================")
print("🌐 구글 시트 접속을 시도합니다...")
try:
    gc = gspread.service_account(filename=os.path.join(current_folder, 'google_key.json'))
    
    # 💡 [주의] 구글 시트 주소를 반드시 다시 적어주세요!
    SHEET_URL = 'https://docs.google.com/spreadsheets/d/1ZxIYeERuOWOWZudyjpMWpEWA0eljOct_uO9gXg6_2JA/edit?gid=1831966955#gid=1831966955' 
    sh = gc.open_by_url(SHEET_URL)
    
    google_connected = True
    print(f"✅ 구글 시트 접속 완료! 문이 열렸습니다.")
except Exception as e:
    print(f"⚠️ 구글 접속 실패: {e}")
    google_connected = False
print("=========================================\n")

# =====================================================================
# 💡 [시즌 2 핵심 패치] KRX 전 종목 데이터 단 1초만에 캐싱 (사전 베이킹)
# =====================================================================
print("=========================================")
print("📈 한국거래소(KRX) 전체 주가/등락률 초고속 스캔 중...")
try:
    krx_df = fdr.StockListing('KRX')
    krx_dict = {}
    for _, row in krx_df.iterrows():
        krx_dict[str(row['Name']).strip()] = {
            'Close': row['Close'],
            'ChagesRatio': row['ChagesRatio']
        }
    print(f"✅ 총 {len(krx_dict):,}개 종목 주가 데이터 장전 완료!")
except Exception as e:
    print(f"⚠️ 주가 스캔 실패 (기존 방식으로 진행): {e}")
    krx_dict = {}
print("=========================================\n")


all_files = [f for f in glob.glob(os.path.join(current_folder, "*.*"))
             if f.endswith(('.csv', '.xlsx', '.xls')) 
             and ("TIME" in f or "KoAct" in f) 
             and "30일추적" not in f 
             and "변환완료" not in f
             and "통합완료" not in f]

if not all_files:
    print("❌ 폴더에 원본 파일이 없습니다.")
    try: input("엔터를 누르면 종료됩니다...")
    except: pass
    exit()

etf_groups = {}
for f in all_files:
    fname = os.path.basename(f)
    date_match = re.search(r'(\d{4}-\d{2}-\d{2}|\d{8})', fname)
    if not date_match: continue
    raw_date = date_match.group()
    
    if len(raw_date) == 8: file_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
    else: file_date = raw_date
        
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
            header_idx = i
            break
            
    df.columns = df.iloc[header_idx]
    df = df.iloc[header_idx+1:].reset_index(drop=True)
    df.columns = [str(c).strip() for c in df.columns]
    
    n_col = next((c for c in df.columns if '종목명' in c or '자산명' in c), None)
    w_col = next((c for c in df.columns if '비중' in c), None)
    q_col = next((c for c in df.columns if any(k in c for k in ['수량', '주식수', '계약수'])), None)
    
    if n_col and w_col:
        df[w_col] = pd.to_numeric(df[w_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        if df[w_col].sum() <= 2.0: df[w_col] = df[w_col] * 100
        df[w_col] = df[w_col].round(2)
        
        if q_col:
            df[q_col] = pd.to_numeric(df[q_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
            
        return df, n_col, w_col, q_col
    else:
        raise ValueError("종목명이나 비중 컬럼을 찾을 수 없습니다.")

for etf_name, files_info in etf_groups.items():
    print(f"▶️ [{etf_name}] 수량/주가 추적 및 업로드 중...")
    
    files_info.sort(key=lambda x: x['date'])
    base_file = files_info[0]['file']
    
    try:
        b_df, n_col, w_col, q_col = read_etf_data(base_file)
        
        b_df = b_df[b_df[n_col].astype(str).str.strip() != '']
        b_df = b_df[b_df[n_col].astype(str).str.lower() != 'nan']
        b_df = b_df.dropna(subset=[n_col])
        
        first_day_top20 = b_df.sort_values(by=w_col, ascending=False).head(20)
        standard_cols = first_day_top20[n_col].tolist()
        
        all_rows = []
        historical_new_cols = []
        prev_qty = {} 
        is_first_day = True 
        
        # 💡 반복문을 돌 때 '오늘이 가장 최신 파일(마지막 날)인지' 확인하기 위해 enumerate 사용!
        for i, info in enumerate(files_info):
            is_last_day = (i == len(files_info) - 1)
            
            fpath = info['file']
            fdate = info['date']
            
            r_df, r_n_col, r_w_col, r_q_col = read_etf_data(fpath)
            r_df = r_df[r_df[r_n_col].astype(str).str.strip() != '']
            r_df = r_df.dropna(subset=[r_n_col])
            
            today_top20 = r_df.sort_values(by=r_w_col, ascending=False).head(20)
            
            for st_name in today_top20[r_n_col]:
                if st_name not in standard_cols and st_name not in historical_new_cols:
                    historical_new_cols.append(st_name)
                    
            row_dict = {'Date': fdate}
            today_data = r_df.set_index(r_n_col).to_dict('index')
            
            for st_name in standard_cols + historical_new_cols:
                if st_name in today_data:
                    w = today_data[st_name][r_w_col]
                    q = today_data[st_name][r_q_col] if r_q_col else 0
                else:
                    w = 0; q = 0
                    
                # 💡 [시즌 2 마법 패치] 최신 날짜(오늘)에만 주가/등락률 텍스트를 장착합니다!
                price_str = ""
                if is_last_day and krx_dict:
                    p_info = krx_dict.get(st_name)
                    if p_info:
                        p = p_info.get('Close', 0)
                        r = p_info.get('ChagesRatio', 0.0)
                        price_str = f" | ₩{int(p):,} ({r:+.2f}%)"
                        
                if is_first_day:
                    diff_str = f"-{price_str}" 
                else:
                    diff = q - prev_qty.get(st_name, 0)
                    if diff > 0: diff_str = f"🔴▲ {int(diff):,}{price_str}"
                    elif diff < 0: diff_str = f"🔵▼ {abs(int(diff)):,}{price_str}"
                    else: diff_str = f"0{price_str}"
                
                row_dict[st_name] = w
                row_dict[f"{st_name}_증감"] = diff_str
                
            if r_q_col:
                for st_name, row_data in today_data.items():
                    prev_qty[st_name] = row_data[r_q_col]
                    
            all_rows.append(row_dict)
            is_first_day = False
            
        final_cols = ['Date']
        for col in standard_cols + historical_new_cols:
            final_cols.append(col)
            final_cols.append(f"{col}_증감")
            
        final_df = pd.DataFrame(all_rows, columns=final_cols)
        
        out_name = f"통합완료_{etf_name}.csv"
        final_df.to_csv(os.path.join(current_folder, out_name), index=False, encoding='utf-8-sig')
        print(f"   => 💾 PC 저장 (수량/주가 데이터 포함): {out_name}")
        
        if google_connected:
            try:
                existing_sheets = [ws.title for ws in sh.worksheets()]
                if etf_name in existing_sheets:
                    worksheet = sh.worksheet(etf_name)
                else:
                    worksheet = sh.add_worksheet(title=etf_name, rows="1000", cols="100")
                
                final_df_gs = final_df.fillna("")
                worksheet.clear()
                worksheet.update(values=[final_df_gs.columns.values.tolist()] + final_df_gs.values.tolist(), range_name="A1")
                print(f"   => 🌐 구글 시트 탭 업로드 성공!")
                time.sleep(2) 
            except Exception as e:
                print(f"   => ❌ 구글 시트 업로드 실패: {e}")
        
    except Exception as e:
        print(f"❌ 실패 [{etf_name}]: {e}")

print("\n🎉 모든 수량/주가 추적 공정이 완벽하게 완료되었습니다!")
try:
    input("엔터(Enter)를 누르면 창이 닫힙니다...")
except EOFError:
    pass
