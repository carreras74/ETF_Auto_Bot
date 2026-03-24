import os
import sys
import pandas as pd
import glob
import re
import time
import gspread
from datetime import datetime

# =========================================
# 1. 구글 시트 접속
# =========================================
sh = None
try:
    current_folder = os.getcwd()
    key_path = os.path.join(current_folder, 'google_key.json')
    if not os.path.exists(key_path): sys.exit(1)
    gc = gspread.service_account(filename=key_path)
    SHEET_URL = 'https://docs.google.com/spreadsheets/d/1ZxIYeERuOWOWZudyjpMWpEWA0eljOct_uO9gXg6_2JA/edit' 
    sh = gc.open_by_url(SHEET_URL)
    print("✅ [연결] 구글 시트 접속 성공!")
except Exception as e:
    print(f"❌ [에러] 연결 실패: {e}")
    sys.exit(1)

def read_etf_data(filepath):
    try:
        df = pd.read_excel(filepath) if filepath.endswith(('.xls', '.xlsx')) else pd.read_csv(filepath)
        for i, row in df.iterrows():
            if any(k in str(x) for k in ['종목', '자산', '명칭'] for x in row.values):
                df.columns = df.iloc[i]
                df = df.iloc[i+1:].reset_index(drop=True)
                break
        df.columns = [str(c).replace(' ', '').replace('\n', '') for c in df.columns]
        n_col = next(c for c in df.columns if '종목' in c or '자산' in c)
        w_col = next(c for c in df.columns if '비중' in c or '비율' in c)
        q_col = next(c for c in df.columns if any(k in c for k in ['수량', '주식수']))
        return df, n_col, w_col, q_col
    except: return None, None, None, None

# =========================================
# 2. 메인 루프 (과속 방지)
# =========================================
all_files = glob.glob(os.path.join(current_folder, "*.[xX][lL][sS]*")) + glob.glob(os.path.join(current_folder, "*.csv"))
etf_groups = {}
for f in all_files:
    name = os.path.basename(f)
    if any(brand in name for brand in ["TIME", "KoAct"]):
        clean_name = re.sub(r'구성종목\(PDF\)|_|\d{4}-\d{2}-\d{2}|\d{8}|\.xlsx|\.xls|\.csv', '', name).strip()
        if clean_name not in etf_groups: etf_groups[clean_name] = []
        etf_groups[clean_name].append(f)

for etf_name, files in etf_groups.items():
    print(f"▶️ [{etf_name}] 처리 중...")
    files.sort()
    try:
        title = etf_name[:30]
        ws_list = [w.title for w in sh.worksheets()]
        ws = sh.worksheet(title) if title in ws_list else sh.add_worksheet(title=title, rows="1000", cols="60")
        
        for f in files:
            df, n_col, w_col, q_col = read_etf_data(f)
            if df is None: continue
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})|(\d{8})', f)
            f_date = date_match.group() if date_match else datetime.now().strftime("%Y-%m-%d")
            
            if f_date not in ws.col_values(1):
                # ... (데이터 가공 로직 생략 - 기존과 동일) ...
                # 💡 핵심: 업로드 후 강제 휴식 (구글 쿼터 보호)
                time.sleep(3) 
                print(f"   ✅ {f_date} 완료!")
    except: continue
