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
print("🚀 [스마트 어펜드 + 첫줄 절대 매칭 모터] 시즌4 변환기 실행 중...\n")

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

print("📈 한국거래소(KRX) 전체 종목코드 매핑 중...")
try:
    krx_df = fdr.StockListing('KRX')
    krx_dict = {}
    name_to_code = {} 
    for _, row in krx_df.iterrows():
        name = str(row['Name']).strip()
        krx_dict[name] = {
            'Close': row['Close'],
            'ChagesRatio': row['ChagesRatio']
        }
        name_to_code[name] = str(row['Code'])
    print(f"✅ 총 {len(krx_dict):,}개 종목 코드 장전 완료!\n")
except Exception as e:
    print(f"⚠️ 코드 스캔 실패: {e}\n")
    krx_dict = {}
    name_to_code = {}

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
    print(f"▶️ [{etf_name}] 분석 및 업데이트 시작...")
    files_info.sort(key=lambda x: x['date'])
    
    try:
        existing_df = pd.DataFrame()
        worksheet = None
        if google_connected:
            try:
                existing_sheets = [ws.title for ws in sh.worksheets()]
                if etf_name in existing_sheets:
                    worksheet = sh.worksheet(etf_name)
                    data = worksheet.get_all_values()
                    if len(data) > 1:
                        existing_df = pd.DataFrame(data[1:], columns=data[0])
            except Exception as e:
                pass
        
        # =====================================================================
        # 💡 [핵심 검증 1] 첫 줄(헤더)에 있는 기존 종목들의 이름표와 순서를 완벽히 기억합니다.
        # =====================================================================
        if not existing_df.empty and 'Date' in existing_df.columns:
            last_gs_date = existing_df['Date'].max()
            historical_cols = [c for c in existing_df.columns if c != 'Date' and not c.endswith('_증감')]
        else:
            last_gs_date = "1900-01-01"
            historical_cols = []
            
        target_files = [f for f in files_info if f['date'] >= last_gs_date]
        
        if len(target_files) <= 1 and target_files and target_files[0]['date'] <= last_gs_date:
            print(f"   ✅ 이미 최신 상태입니다. (마지막 업데이트: {last_gs_date}) 스킵!\n")
            continue

        print(f"   => 🔄 새로운 날짜 {len(target_files)-1}일치 데이터를 추가합니다.")
        
        new_dates = [f['date'] for f in target_files if f['date'] > last_gs_date]
        all_stocks_in_new_files = set()
        for info in target_files:
            try:
                r_df, r_n_col, r_w_col, _ = read_etf_data(info['file'])
                top20_names = r_df.dropna(subset=[r_n_col]).sort_values(by=r_w_col, ascending=False).head(20)[r_n_col].tolist()
                all_stocks_in_new_files.update(top20_names)
            except: pass
        
        global_stock_hist_cache = {}
        for st_name in all_stocks_in_new_files:
            code = name_to_code.get(st_name)
            if code and new_dates:
                try:
                    temp_df = fdr.DataReader(code, min(new_dates))
                    temp_df.index = temp_df.index.strftime('%Y-%m-%d')
                    global_stock_hist_cache[st_name] = temp_df[['Close', 'Change']].to_dict('index')
                except:
                    global_stock_hist_cache[st_name] = {}

        all_rows = []
        # historical_new_cols 변수에 기존 종목 순서를 먼저 박아놓습니다. (열 섞임 원천 차단)
        historical_new_cols = list(historical_cols) 
        prev_qty = {} 
        
        for i, info in enumerate(target_files):
            is_last_day = (i == len(target_files) - 1)
            fpath = info['file']
            fdate = info['date']
            
            r_df, r_n_col, r_w_col, r_q_col = read_etf_data(fpath)
            r_df = r_df[r_df[r_n_col].astype(str).str.strip() != '']
            r_df = r_df.dropna(subset=[r_n_col])
            
            today_top20 = r_df.sort_values(by=r_w_col, ascending=False).head(20)
            
            # =====================================================================
            # 💡 [핵심 검증 2] 새로운 종목은 무조건 기존 종목을 건드리지 않고 '맨 뒤'에 추가
            # =====================================================================
            for st_name in today_top20[r_n_col]:
                if st_name not in historical_new_cols:
                    historical_new_cols.append(st_name)
                    
            row_dict = {'Date': fdate}
            today_data = r_df.set_index(r_n_col).to_dict('index')
            
            # =====================================================================
            # 💡 [핵심 검증 3] 데이터를 집어넣을 때 '종목명(이름표)'을 보고 그 자리에 꽂아 넣음
            # =====================================================================
            for st_name in historical_new_cols:
                if st_name in today_data:
                    w = today_data[st_name][r_w_col]
                    q = today_data[st_name][r_q_col] if r_q_col else 0
                else:
                    w = 0; q = 0
                    
                price_str = ""
                if fdate > last_gs_date:
                    if st_name in global_stock_hist_cache and fdate in global_stock_hist_cache[st_name]:
                        p = global_stock_hist_cache[st_name][fdate]['Close']
                        r = global_stock_hist_cache[st_name][fdate]['Change'] * 100
                        price_str = f" | ₩{int(p):,} ({r:+.2f}%)"
                    elif is_last_day and krx_dict and st_name in krx_dict:
                        p = krx_dict[st_name]['Close']
                        r = krx_dict[st_name]['ChagesRatio']
                        price_str = f" | ₩{int(p):,} ({r:+.2f}%)"
                        
                if i == 0 and existing_df.empty:
                    diff_str = f"-{price_str}" 
                elif i == 0 and not existing_df.empty:
                    diff_str = ""
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
            
            if fdate > last_gs_date or existing_df.empty:
                all_rows.append(row_dict)
            
        final_cols = ['Date']
        for col in historical_new_cols:
            final_cols.append(col)
            final_cols.append(f"{col}_증감")
            
        new_df = pd.DataFrame(all_rows, columns=final_cols)
        
        # =====================================================================
        # 💡 [핵심 검증 4] 기존 데이터와 병합할 때 컬럼명(이름표) 기준으로 완벽하게 정렬하여 합침
        # =====================================================================
        if not existing_df.empty:
            final_df = pd.concat([existing_df, new_df], axis=0, ignore_index=True)
            final_df = final_df.reindex(columns=final_cols)
        else:
            final_df = new_df
            
        out_name = f"통합완료_{etf_name}.csv"
        final_df.to_csv(os.path.join(current_folder, out_name), index=False, encoding='utf-8-sig')
        print(f"   => 💾 PC 저장 (병합 완료): {out_name}")
        
        if google_connected:
            try:
                if worksheet is None:
                    worksheet = sh.add_worksheet(title=etf_name, rows="1000", cols="100")
                
                final_df_gs = final_df.fillna("")
                worksheet.clear()
                worksheet.update(values=[final_df_gs.columns.values.tolist()] + final_df_gs.values.tolist(), range_name="A1")
                print(f"   => 🌐 구글 시트 탭 스마트 업로드 성공!\n")
                time.sleep(2) 
            except Exception as e:
                print(f"   => ❌ 구글 시트 업로드 실패: {e}\n")
        
    except Exception as e:
        print(f"❌ 실패 [{etf_name}]: {e}\n")

print("🎉 모든 스마트 어펜드 공정이 완벽하게 완료되었습니다!")
try:
    input("엔터(Enter)를 누르면 창이 닫힙니다...")
except EOFError:
    pass
