import os
import sys
import pandas as pd
import glob
import re
import time
import gspread  # 💡 [핵심] 구글 시트 라이브러리 소환!
from datetime import datetime

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

    # 💡 구글 시트 라이브러리 연결
    gc = gspread.service_account(filename=key_path)
    # 대표님 시트 URL
    SHEET_URL = 'https://docs.google.com/spreadsheets/d/1ZxIYeERuOWOWZudyjpMWpEWA0eljOct_uO9gXg6_2JA/edit' 
    sh = gc.open_by_url(SHEET_URL)
    print("✅ [연결] 구글 시트 접속 성공!")
except Exception as e:
    print(f"❌ [에러] 구글 시트 연결 실패: {e}")
    sys.exit(1)

def read_etf_data(filepath):
    try:
        # 엑셀/CSV 구분해서 읽기
        if filepath.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(filepath)
        else:
            try: df = pd.read_csv(filepath, encoding='utf-8-sig')
            except: df = pd.read_csv(filepath, encoding='cp949')

        # 데이터 시작 지점(헤더) 찾기
        for i, row in df.iterrows():
            row_strs = [str(x) for x in row.values]
            if any(k in s for k in ['종목', '자산', '명칭'] for s in row_strs):
                df.columns = df.iloc[i]
                df = df.iloc[i+1:].reset_index(drop=True)
                break
        
        # 컬럼명 특수문자 제거
        df.columns = [str(c).replace(' ', '').replace('\n', '') for c in df.columns]
        
        # 핵심 기둥 탐지
        n_col = next(c for c in df.columns if '종목' in c or '자산' in c or '명' in c)
        w_col = next(c for c in df.columns if '비중' in c or '비율' in c)
        q_col = next(c for c in df.columns if any(k in c for k in ['수량', '주식수', '주수']))
        
        # 숫자 데이터 정제
        df[w_col] = pd.to_numeric(df[w_col].astype(str).str.replace('%',''), errors='coerce').fillna(0)
        df[q_col] = pd.to_numeric(df[q_col].astype(str).str.replace(',',''), errors='coerce').fillna(0)
        
        return df, n_col, w_col, q_col
    except Exception as e:
        print(f"   ⚠️ 파일 읽기 실패 ({os.path.basename(filepath)}): {e}")
        return None, None, None, None

# =========================================
# 2. 메인 통합 로직 (과속 방지 턱 설치)
# =========================================
def process_integration():
    all_files = glob.glob(os.path.join(current_folder, "*.[xX][lL][sS]*")) + glob.glob(os.path.join(current_folder, "*.csv"))
    
    etf_groups = {}
    for f in all_files:
        name = os.path.basename(f)
        # 💡 KoAct와 TIME만 필터링
        if any(brand in name for brand in ["TIME", "KoAct"]):
            clean_name = re.sub(r'구성종목\(PDF\)|_|\d{4}-\d{2}-\d{2}|\d{8}|\.xlsx|\.xls|\.csv', '', name).strip()
            if clean_name and "google_key" not in clean_name:
                if clean_name not in etf_groups: etf_groups[clean_name] = []
                etf_groups[clean_name].append(f)

    for etf_name, files in etf_groups.items():
        print(f"\n▶️ [{etf_name}] 처리 중...")
        files.sort()
        time.sleep(1)
        
        try:
            title = etf_name[:30]
            worksheet_list = [w.title for w in sh.worksheets()]
            if title in worksheet_list:
                ws = sh.worksheet(title)
            else:
                ws = sh.add_worksheet(title=title, rows="1000", cols="60")
                time.sleep(2)
            
            # 수량 증감용 이전 데이터 수집
            existing_data = ws.get_all_values()
            prev_shares = {}
            if len(existing_data) > 1:
                headers = existing_data[0]
                last_row = existing_data[-1]
                for i, h in enumerate(headers):
                    if "_수량" in h:
                        stock_name = h.replace("_수량", "")
                        prev_shares[stock_name] = float(str(last_row[i]).replace(',','')) if i < len(last_row) and last_row[i] else 0

            for f in files:
                df, n_col, w_col, q_col = read_etf_data(f)
                if df is None: continue

                date_match = re.search(r'(\d{4}-\d{2}-\d{2})|(\d{8})', f)
                file_date = date_match.group() if date_match else datetime.now().strftime("%Y-%m-%d")

                # 중복 날짜 체크
                dates_in_sheet = ws.col_values(1) if len(existing_data) > 0 else []
                if file_date in dates_in_sheet:
                    print(f"   ⏭️ {file_date} 건너뜀.")
                    continue

                # 상위 30개 가공
                df = df.sort_values(by=w_col, ascending=False).head(30)
                new_row = [file_date]
                headers = ["Date"]
                
                for _, r in df.iterrows():
                    s_name = r[n_col]
                    s_weight = r[w_col]
                    s_qty = r[q_col]
                    
                    diff = s_qty - prev_shares.get(s_name, s_qty)
                    diff_str = f"🔴▲{int(diff):,}" if diff > 0 else (f"🔵▼{int(abs(diff)):,}" if diff < 0 else "0")
                    
                    headers.extend([s_name, f"{s_name}_증감", f"{s_name}_수량"])
                    new_row.extend([f"{s_weight}%", diff_str, f"{int(s_qty):,}"])
                    prev_shares[s_name] = s_qty

                if not existing_data:
                    ws.append_row(headers)
                    existing_data = [headers]
                    time.sleep(1)
                
                ws.append_row(new_row)
                print(f"   ✅ {file_date} 업로드 완료!")
                time.sleep(2) # 구글 쿼터 보호

        except Exception as e:
            print(f"   ❌ [{etf_name}] 에러: {e}")
            time.sleep(5)

if __name__ == "__main__":
    process_integration()
