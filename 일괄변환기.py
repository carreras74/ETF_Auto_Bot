import os
import sys
import pandas as pd
import glob
import re
import time
import gspread
from datetime import datetime
import FinanceDataReader as fdr

# =========================================
# 1. 구글 시트 및 환경 설정
# =========================================
sh = None
try:
    current_folder = os.getcwd()
    key_path = os.path.join(current_folder, 'google_key.json')
    
    if not os.path.exists(key_path):
        print("❌ [에러] google_key.json 파일이 존재하지 않습니다.")
        sys.exit(1)

    gc = gspread.service_account(filename=key_path)
    SHEET_URL = 'https://docs.google.com/spreadsheets/d/1ZxIYeERuOWOWZudyjpMWpEWA0eljOct_uO9gXg6_2JA/edit' 
    sh = gc.open_by_url(SHEET_URL)
    print("✅ [연결] 구글 시트 접속 성공!")
except Exception as e:
    print(f"❌ [에러] 구글 시트 연결 실패: {e}")
    sys.exit(1)

def read_etf_data(filepath):
    try:
        if filepath.endswith(('.xls', '.xlsx')): df = pd.read_excel(filepath)
        else:
            try: df = pd.read_csv(filepath, encoding='utf-8-sig')
            except: df = pd.read_csv(filepath, encoding='cp949')

        header_found = False
        for i, row in df.iterrows():
            row_strs = [str(x) for x in row.values]
            if any(k in s for k in ['종목', '자산', '명칭'] for s in row_strs):
                df.columns = df.iloc[i]
                df = df.iloc[i+1:].reset_index(drop=True)
                header_found = True
                break
        
        if not header_found: return None, None, None, None

        df.columns = [str(c).replace(' ', '').replace('\n', '').strip() for c in df.columns]
        n_col = next((c for c in df.columns if '종목' in c or '자산' in c or '명' in c), None)
        w_col = next((c for c in df.columns if '비중' in c or '비율' in c), None)
        q_col = next((c for c in df.columns if any(k in c for k in ['수량', '주식수', '주수'])), None)
        
        if not all([n_col, w_col, q_col]): return None, None, None, None

        df[w_col] = pd.to_numeric(df[w_col].astype(str).str.replace('%',''), errors='coerce').fillna(0)
        df[q_col] = pd.to_numeric(df[q_col].astype(str).str.replace(',',''), errors='coerce').fillna(0)
        
        return df, n_col, w_col, q_col
    except Exception:
        return None, None, None, None

# =========================================
# 2. 메인 통합 로직 (초고속 캐시 메모리 엔진 탑재)
# =========================================
def process_integration():
    print("⏳ [준비] 한국거래소(KRX) 전 종목 코드표를 불러오는 중입니다...")
    try:
        krx_df = fdr.StockListing('KRX')
        name_to_code = dict(zip(krx_df['Name'], krx_df['Code']))
    except Exception as e:
        print(f"⚠️ KRX 종목코드 로드 실패: {e}")
        name_to_code = {}

    # 💡 [초고속 엔진] 한 번 찾은 종목은 한 달치 주가를 통째로 외워둡니다!
    price_cache = {} 

    def get_price(stock_name, target_date_str):
        if stock_name not in name_to_code: return 0
        code = name_to_code[stock_name]
        
        # 기억에 없으면 한 달치 데이터를 싹 긁어와서 저장
        if code not in price_cache:
            try:
                price_cache[code] = fdr.DataReader(code, '2026-02-20', datetime.now().strftime('%Y-%m-%d'))
            except:
                price_cache[code] = pd.DataFrame()
                
        df_price = price_cache[code]
        if df_price.empty: return 0
        
        # 기억해둔 데이터에서 해당 날짜에 맞는 가격만 쏙 뽑아냅니다
        try:
            target_dt = pd.to_datetime(target_date_str)
            past_data = df_price[df_price.index <= target_dt]
            if not past_data.empty:
                return int(past_data['Close'].iloc[-1])
        except: pass
        return 0

    all_files = glob.glob(os.path.join(current_folder, "*.[xX][lL][sS]*")) + glob.glob(os.path.join(current_folder, "*.csv"))
    
    etf_groups = {}
    for f in all_files:
        name = os.path.basename(f)
        if any(brand in name for brand in ["TIME", "KoAct"]) and "통합완료" not in name:
            clean_name = re.sub(r'구성종목\(PDF\)|_|\d{4}-\d{2}-\d{2}|\d{8}|\.xlsx|\.xls|\.csv', '', name).strip()
            if clean_name and "google_key" not in clean_name:
                if clean_name not in etf_groups: etf_groups[clean_name] = []
                etf_groups[clean_name].append(f)

    for etf_name, files in etf_groups.items():
        print(f"\n▶️ [{etf_name}] 처리 중...")
        files.sort()
        
        try:
            title = etf_name[:30]
            worksheet_list = [w.title for w in sh.worksheets()]
            if title in worksheet_list:
                ws = sh.worksheet(title)
            else:
                ws = sh.add_worksheet(title=title, rows="1000", cols="60")
                time.sleep(1)

            existing_data_all = ws.get_all_values()
            existing_dates = [row[0] for row in existing_data_all if row] if existing_data_all else []
            
            prev_shares = {}
            if len(existing_data_all) > 1:
                headers = existing_data_all[0]
                last_row = existing_data_all[-1]
                for i, h in enumerate(headers):
                    if "_수량" in h:
                        stock_name = h.replace("_수량", "")
                        val = last_row[i] if i < len(last_row) else "0"
                        prev_shares[stock_name] = float(str(val).replace(',','')) if val else 0

            for f in files:
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})|(\d{8})', f)
                file_date = date_match.group() if date_match else datetime.now().strftime("%Y-%m-%d")
                
                if len(file_date) == 8 and "-" not in file_date:
                    file_date = f"{file_date[:4]}-{file_date[4:6]}-{file_date[6:]}"

                if file_date in existing_dates:
                    print(f"   ⏭️ {file_date} 건너뜀.")
                    continue

                df, n_col, w_col, q_col = read_etf_data(f)
                if df is None: continue

                df = df.sort_values(by=w_col, ascending=False).head(30)
                new_row = [file_date]
                headers = ["Date"]
                
                for _, r in df.iterrows():
                    s_name = r[n_col]
                    s_weight = r[w_col]
                    s_qty = r[q_col]
                    
                    diff = s_qty - prev_shares.get(s_name, s_qty)
                    
                    price = get_price(s_name, file_date)
                    price_str = f" | ₩{price:,}" if price > 0 else " | ₩0"
                    
                    if diff > 0: diff_str = f"🔴▲{int(diff):,}{price_str}"
                    elif diff < 0: diff_str = f"🔵▼{int(abs(diff)):,}{price_str}"
                    else: diff_str = f"0{price_str}"
                    
                    headers.extend([s_name, f"{s_name}_증감", f"{s_name}_수량"])
                    new_row.extend([f"{s_weight}%", diff_str, f"{int(s_qty):,}"])
                    prev_shares[s_name] = s_qty

                if not existing_data_all:
                    ws.append_row(headers)
                    existing_data_all = [headers]
                    time.sleep(1)
                
                ws.append_row(new_row)
                existing_dates.append(file_date)
                print(f"   ✅ {file_date} 업로드 완료 (가격 포함)!")
                time.sleep(1)

        except Exception as e:
            print(f"   ❌ [{etf_name}] 에러: {e}")

if __name__ == "__main__":
    process_integration()
