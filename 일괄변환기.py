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
    # 대표님 구글 시트 고유 ID
    sh = gc.open_by_key("1ZxIYeERuOWOWZudyjpMWpEWA0eljOct_uO9gXg6_2JA")
    print("✅ [연결] 구글 시트 접속 성공!")
except Exception as e:
    print(f"❌ [에러] 구글 시트 연결 실패: {e}")
    sys.exit(1)

def read_etf_data(filepath):
    """TIME(1행 시작)과 KoAct(3행 시작) 양식을 모두 읽어내는 양손잡이 파서"""
    try:
        if filepath.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(filepath)
        else:
            try: df = pd.read_csv(filepath, encoding='utf-8-sig')
            except: df = pd.read_csv(filepath, encoding='cp949')

        def is_real_header(row_vals):
            strs = [str(x).replace(' ', '') for x in row_vals]
            has_name = any(('종목' in s or '자산' in s or '명' in s) and '코드' not in s for s in strs)
            has_weight = any('비중' in s or '비율' in s for s in strs)
            has_qty = any('수량' in s or '주식수' in s or '주수' in s for s in strs)
            return has_name and has_weight and has_qty

        header_found = False
        if is_real_header(df.columns):
            header_found = True
        else:
            for i, row in df.iterrows():
                if is_real_header(row.values):
                    df.columns = df.iloc[i]
                    df = df.iloc[i+1:].reset_index(drop=True)
                    header_found = True
                    break
                    
        if not header_found: return None, None, None, None

        df.columns = [str(c).replace(' ', '').replace('\n', '').strip() for c in df.columns]
        n_col = next((c for c in df.columns if ('종목' in c or '자산' in c or '명' in c) and '코드' not in c), None)
        w_col = next((c for c in df.columns if '비중' in c or '비율' in c), None)
        q_col = next((c for c in df.columns if any(k in c for k in ['수량', '주식수', '주수'])), None)
        
        if not all([n_col, w_col, q_col]): return None, None, None, None

        df[w_col] = pd.to_numeric(df[w_col].astype(str).str.replace('%','').str.replace(',',''), errors='coerce').fillna(0)
        df[q_col] = pd.to_numeric(df[q_col].astype(str).str.replace(',','').replace('-','0'), errors='coerce').fillna(0)
        return df, n_col, w_col, q_col
    except:
        return None, None, None, None

# =========================================
# 2. 메인 통합 로직 (초고속 엔진 + 거북이 방어선)
# =========================================
def process_integration():
    print("⏳ [준비] 한국거래소(KRX) 코드표 로드 중...")
    try:
        krx_df = fdr.StockListing('KRX')
        name_to_code = dict(zip(krx_df['Name'], krx_df['Code']))
    except:
        name_to_code = {}

    price_cache = {}
    def get_price(stock_name, target_date_str):
        if stock_name not in name_to_code: return 0
        code = name_to_code[stock_name]
        if code not in price_cache:
            try:
                # 최근 한 달 치 가격을 한 번에 가져와서 메모리에 저장(캐싱)
                price_cache[code] = fdr.DataReader(code, '2026-02-20', datetime.now().strftime('%Y-%m-%d'))
            except: price_cache[code] = pd.DataFrame()
        
        df_p = price_cache[code]
        if df_p.empty: return 0
        try:
            target_dt = pd.to_datetime(target_date_str)
            past_data = df_p[df_p.index <= target_dt]
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
        print(f"\n▶️ [{etf_name}] 처리 시작...")
        files.sort()
        try:
            title = etf_name[:30]
            ws_list = [w.title for w in sh.worksheets()]
            if title in ws_list:
                ws = sh.worksheet(title)
                existing_raw = ws.get_all_values()
                if len(existing_raw) > 0:
                    df_sheet = pd.DataFrame(existing_raw[1:], columns=existing_raw[0])
                else: df_sheet = pd.DataFrame(columns=['Date'])
            else:
                ws = sh.add_worksheet(title=title, rows="1000", cols="100")
                df_sheet = pd.DataFrame(columns=['Date'])
                time.sleep(1)

            existing_dates = df_sheet['Date'].tolist() if 'Date' in df_sheet.columns else []

            # 직전 날짜의 수량 데이터 로드
            prev_shares = {}
            if not df_sheet.empty:
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
                    print(f"   ⏭️ {file_date} 건너뜀 (이미 존재)")
                    continue

                df, n_col, w_col, q_col = read_etf_data(f)
                if df is None: continue

                # 비중 단위 자동 변환 (0.15 -> 15.00%)
                sum_w = df[w_col].sum()
                weight_multiplier = 100 if sum_w <= 1.5 else 1

                df = df.sort_values(by=w_col, ascending=False).head(30)
                row_dict = {'Date': file_date}
                
                is_first_file = is_fresh_sheet and (len(new_rows) == 0)

                for _, r in df.iterrows():
                    s_name = r[n_col]
                    s_weight = r[w_col] * weight_multiplier
                    s_qty = r[q_col]
                    
                    # 증감 계산 (최초 행은 0으로 표시)
                    if is_first_file: diff = 0
                    else: diff = s_qty - prev_shares.get(s_name, 0)
                    
                    price = get_price(s_name, file_date)
                    price_str = f" | ₩{price:,}" if price > 0 else " | ₩0"
                    
                    if diff > 0: diff_str = f"🔴▲{int(diff):,}{price_str}"
                    elif diff < 0: diff_str = f"🔵▼{int(abs(diff)):,}{price_str}"
                    else: diff_str = f"0{price_str}"
                    
                    # 스마트 열 정렬
                    row_dict[s_name] = f"{s_weight:.2f}%"
                    row_dict[f"{s_name}_증감"] = diff_str
                    row_dict[f"{s_name}_수량"] = f"{int(s_qty):,}"
                    prev_shares[s_name] = s_qty
                    
                new_rows.append(row_dict)
                existing_dates.append(file_date)
                print(f"   ✅ {file_date} 가공 완료")

            if new_rows:
                # 데이터프레임 합치기 및 빈칸 처리
                df_new = pd.DataFrame(new_rows)
                df_sheet = pd.concat([df_sheet, df_new], ignore_index=True)
                df_sheet = df_sheet.fillna("")
                
                # 💡 구글 시트 업데이트 (거북이 엔진 작동)
                ws.clear()
                final_data = [df_sheet.columns.values.tolist()] + df_sheet.astype(str).values.tolist()
                ws.update(final_data)
                
                print(f"   🚀 [{etf_name}] 시트 업데이트 성공!")
                print("   ⏳ 구글 API 과속 단속 방지를 위해 5초간 대기...")
                time.sleep(5)

        except Exception as e:
            if "429" in str(e):
                print("\n🚨 [비상] 구글 단속 발생! 1분간 동면 후 다시 시작합니다...")
                time.sleep(65)
            else:
                print(f"   ❌ [{etf_name}] 에러 발생: {e}")

if __name__ == "__main__":
    process_integration()
    print("\n✨ 모든 ETF 데이터 통합 공정이 완료되었습니다!")
