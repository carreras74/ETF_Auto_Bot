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
        print("❌ [에러] google_key.json 파일이 없습니다.")
        sys.exit(1)

    gc = gspread.service_account(filename=key_path)
    sh = gc.open_by_key("1ZxIYeERuOWOWZudyjpMWpEWA0eljOct_uO9gXg6_2JA")
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

        # 💡 [핵심 패치 1] 파일 첫 줄이 이미 제목인지 바로 검사! (TIME 그룹 호환용)
        col_strs = [str(c).replace(' ', '') for c in df.columns]
        has_valid_header = any('종목' in c or '자산' in c or '명칭' in c for c in col_strs)
        
        if not has_valid_header:
            header_found = False
            for i, row in df.iterrows():
                row_strs = [str(x).replace(' ', '') for x in row.values]
                if any('종목' in s or '자산' in s or '명칭' in s for s in row_strs):
                    df.columns = df.iloc[i]
                    df = df.iloc[i+1:].reset_index(drop=True)
                    header_found = True
                    break
            if not header_found: return None, None, None, None

        df.columns = [str(c).replace(' ', '').replace('\n', '').strip() for c in df.columns]
        
        # 💡 [핵심 패치 2] '종목코드'는 함정이므로 무조건 거르고, 진짜 '종목명'만 찾습니다!
        n_col = next((c for c in df.columns if ('종목' in c or '자산' in c or '명' in c) and '코드' not in c), None)
        w_col = next((c for c in df.columns if '비중' in c or '비율' in c), None)
        q_col = next((c for c in df.columns if any(k in c for k in ['수량', '주식수', '주수'])), None)
        
        if not all([n_col, w_col, q_col]): return None, None, None, None

        df[w_col] = pd.to_numeric(df[w_col].astype(str).str.replace('%','').str.replace(',',''), errors='coerce').fillna(0)
        df[q_col] = pd.to_numeric(df[q_col].astype(str).str.replace(',',''), errors='coerce').fillna(0)
        return df, n_col, w_col, q_col
    except: return None, None, None, None

# =========================================
# 2. 메인 통합 로직 (초고속 엔진 + 자동 정렬)
# =========================================
def process_integration():
    print("⏳ [준비] 한국거래소(KRX) 코드표 로드 중...")
    try:
        krx_df = fdr.StockListing('KRX')
        name_to_code = dict(zip(krx_df['Name'], krx_df['Code']))
    except: name_to_code = {}

    price_cache = {}
    def get_price(stock_name, target_date_str):
        if stock_name not in name_to_code: return 0
        code = name_to_code[stock_name]
        if code not in price_cache:
            try: price_cache[code] = fdr.DataReader(code, '2026-02-20', datetime.now().strftime('%Y-%m-%d'))
            except: price_cache[code] = pd.DataFrame()
        
        df_price = price_cache[code]
        if df_price.empty: return 0
        try:
            target_dt = pd.to_datetime(target_date_str)
            past_data = df_price[df_price.index <= target_dt]
            if not past_data.empty: return int(past_data['Close'].iloc[-1])
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
            ws_list = [w.title for w in sh.worksheets()]
            if title in ws_list:
                ws = sh.worksheet(title)
                existing_data = ws.get_all_values()
                if len(existing_data) > 0:
                    df_sheet = pd.DataFrame(existing_data[1:], columns=existing_data[0])
                else: df_sheet = pd.DataFrame(columns=['Date'])
            else:
                ws = sh.add_worksheet(title=title, rows="1000", cols="60")
                df_sheet = pd.DataFrame(columns=['Date'])
                time.sleep(1)

            existing_dates = df_sheet['Date'].tolist() if 'Date' in df_sheet.columns else []

            prev_shares = {}
            if not df_sheet.empty and len(df_sheet) > 0:
                last_row = df_sheet.iloc[-1]
                for col in df_sheet.columns:
                    if "_수량" in col:
                        s_name = col.replace("_수량", "")
                        val = str(last_row[col]).replace(',', '')
                        prev_shares[s_name] = float(val) if val.replace('.', '', 1).isdigit() else 0

            new_rows = []
            is_fresh_sheet = len(existing_dates) == 0

            for f in files:
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})|(\d{8})', f)
                file_date = date_match.group() if date_match else datetime.now().strftime("%Y-%m-%d")
                if len(file_date) == 8 and "-" not in file_date:
                    file_date = f"{file_date[:4]}-{file_date[4:6]}-{file_date[6:]}"

                if file_date in existing_dates:
                    print(f"   ⏭️ {file_date} 건너뜀.")
                    continue

                df, n_col, w_col, q_col = read_etf_data(f)
                if df is None:
                    print(f"   ⚠️ {file_date} 파일 파싱 실패 (건너뜀)")
                    continue

                # 💡 [비중 뻥튀기 방지] 소수점인지 %인지 판단해서 예쁘게 만듭니다
                sum_w = df[w_col].sum()
                weight_multiplier = 100 if sum_w <= 1.5 else 1

                df = df.sort_values(by=w_col, ascending=False).head(30)
                row_dict = {'Date': file_date}
                
                is_first_file = is_fresh_sheet and (len(new_rows) == 0)

                for _, r in df.iterrows():
                    s_name = r[n_col]
                    s_weight = r[w_col] * weight_multiplier
                    s_qty = r[q_col]
                    
                    if is_first_file: diff = 0
                    else: diff = s_qty - prev_shares.get(s_name, 0)
                    
                    price = get_price(s_name, file_date)
                    price_str = f" | ₩{price:,}" if price > 0 else " | ₩0"
                    
                    if diff > 0: diff_str = f"🔴▲{int(diff):,}{price_str}"
                    elif diff < 0: diff_str = f"🔵▼{int(abs(diff)):,}{price_str}"
                    else: diff_str = f"0{price_str}"
                    
                    row_dict[s_name] = f"{s_weight:.2f}%"
                    row_dict[f"{s_name}_증감"] = diff_str
                    row_dict[f"{s_name}_수량"] = f"{int(s_qty):,}"
                    
                    prev_shares[s_name] = s_qty
                    
                new_rows.append(row_dict)
                existing_dates.append(file_date)
                print(f"   ✅ {file_date} 처리 완료!")

            if new_rows:
                df_new = pd.DataFrame(new_rows)
                df_sheet = pd.concat([df_sheet, df_new], ignore_index=True)
                df_sheet = df_sheet.fillna("")
                
                ws.clear()
                data_to_upload = [df_sheet.columns.values.tolist()] + df_sheet.astype(str).values.tolist()
                ws.update(data_to_upload)
                print(f"   🚀 [{etf_name}] 구글 시트 업데이트 완료!")
                time.sleep(2)

        except Exception as e:
            print(f"   ❌ [{etf_name}] 에러: {e}")

if __name__ == "__main__":
    process_integration()
