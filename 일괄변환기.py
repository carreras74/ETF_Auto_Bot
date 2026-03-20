import os
import sys
import subprocess

def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try: import xlrd
except ImportError: install_package("xlrd")

try: import gspread
except ImportError:
    install_package("gspread")
    import gspread

import pandas as pd
import glob
import re

try: current_folder = os.path.dirname(os.path.abspath(__file__))
except: current_folder = os.getcwd()

print(f"📂 작업 폴더: {current_folder}")
print("🚀 [비중 + 수량증감 추적 모터] 통합 변환기 실행 중...\n")

print("=========================================")
print("🌐 구글 시트 접속을 시도합니다...")
try:
    gc = gspread.service_account(filename=os.path.join(current_folder, 'google_key.json'))
    
    # 💡 [수정 필수] 대표님의 구글 시트 인터넷 주소를 꼭 다시 넣어주세요!
    SHEET_URL = 'https://docs.google.com/spreadsheets/d/1ZxIYeERuOWOWZudyjpMWpEWA0eljOct_uO9gXg6_2JA/edit?gid=1831966955#gid=1831966955' 
    sh = gc.open_by_url(SHEET_URL)
    
    google_connected = True
    print(f"✅ 구글 시트 접속 완료! 문이 열렸습니다.")
except Exception as e:
    print(f"⚠️ 구글 접속 실패: {e}")
    google_connected = False
print("=========================================\n")

all_files = [f for f in glob.glob(os.path.join(current_folder, "*.*"))
             if f.endswith(('.csv', '.xlsx', '.xls')) 
             and "30일추적" not in f 
             and "변환완료" not in f
             and "통합완료" not in f]

if not all_files:
    print("❌ 폴더에 원본 파일이 없습니다.")
    input("엔터를 누르면 종료됩니다...")
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
    print(f"▶️ [{etf_name}] 수량 추적 및 업로드 중...")
    
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
        
        for info in files_info:
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
                    w = 0
                    q = 0
                    
                # 💡 [핵심 패치] 상승은 빨강(🔴▲), 하락은 파랑(🔵▼)으로 확실하게 색상 지정!
                if is_first_day:
                    diff_str = "-" 
                else:
                    diff = q - prev_qty.get(st_name, 0)
                    if diff > 0: diff_str = f"🔴▲ {int(diff):,}"
                    elif diff < 0: diff_str = f"🔵▼ {abs(int(diff)):,}"
                    else: diff_str = "0"
                
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
        print(f"   => 💾 PC 저장 (수량데이터 포함): {out_name}")
        
        if google_connected:
            try:
                existing_sheets = [ws.title for ws in sh.worksheets()]
                if etf_name in existing_sheets:
                    worksheet = sh.worksheet(etf_name)
                else:
                    worksheet = sh.add_worksheet(title=etf_name, rows="100", cols="30")
                
                final_df_gs = final_df.fillna("")
                worksheet.clear()
                worksheet.update(values=[final_df_gs.columns.values.tolist()] + final_df_gs.values.tolist(), range_name="A1")
                print(f"   => 🌐 구글 시트 탭 업로드 성공!")
            except Exception as e:
                print(f"   => ❌ 구글 시트 업로드 실패: {e}")
        
    except Exception as e:
        print(f"❌ 실패 [{etf_name}]: {e}")

print("\n🎉 모든 수량 추적 공정이 완벽하게 완료되었습니다!")
print("\n🎉 모든 수량 추적 공정이 완벽하게 완료되었습니다!")
try:
    input("엔터(Enter)를 누르면 창이 닫힙니다...")
except EOFError:
    pass # 💡 클라우드(키보드 없는 환경)에서는 에러 내지 말고 그냥 조용히 퇴근해!
