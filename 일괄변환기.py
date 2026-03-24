import os
import sys
import pandas as pd
import glob
import re
import time
import gspread
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

    gc = gspread.service_account(filename=key_path)
    SHEET_URL = 'https://docs.google.com/spreadsheets/d/1ZxIYeERuOWOWZudyjpMWpEWA0eljOct_uO9gXg6_2JA/edit' 
    sh = gc.open_by_url(SHEET_URL)
    print("✅ [연결] 구글 시트 접속 성공!")
except Exception as e:
    print(f"❌ [에러] 구글 시트 연결 실패: {e}")
    sys.exit(1)

def read_etf_data(filepath):
    try:
        if filepath.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(filepath)
        else:
            try: df = pd.read_csv(filepath, encoding='utf-8-sig')
            except: df = pd.read_csv(filepath, encoding='cp949')

        for i, row in df.iterrows():
            row_strs = [str(x) for x in row.values]
            if any(k in s for k in ['종목', '자산', '명칭'] for s in row_strs):
                df.columns = df.iloc[i]
                df = df.iloc[i+1:].reset_index(drop=True)
                break
        
        df.columns = [str(c).replace(' ', '').replace('\n', '') for c in df.columns]
        n_col = next(c for c in df.columns if '종목' in c or '자산' in c or '명' in c)
        w_col = next(c for c in df.columns if '비중' in c or '비율' in c)
        q_col = next(c for c in df.columns if any(k in c for k in ['수량', '주식수', '주수']))
        
        df[w_col] = pd.to_numeric(df[w_col].astype(str).str.replace('%',''), errors='coerce').fillna(0)
        df[q_col] = pd.to_numeric(df[q_col].astype(str).str.replace(',',''), errors='coerce').fillna(0)
        
        return df, n_col, w_col, q_col
    except Exception as e:
        print(f"   ⚠️ 파일 읽기 실패 ({os.path.basename(filepath)}): {e}")
        return None, None, None, None

# =========================================
# 2. 메인 통합 로직 (스마트 체크 모드)
# =========================================
def process_integration():
    all_files = glob.glob(os.path.join(current_folder, "*.[xX][lL][sS]*")) + glob.glob(os.path.join(current_folder, "*.csv"))
    
    etf_groups = {}
    for f in all_files:
        name = os.path.basename(f)
        # 💡 KoAct/TIME만 필터링하고 '통합완료' 파일은 제외
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
                time.sleep(2)
            
            # 💡 [슈퍼 최적화] 시트의 전체 데이터를 한 번만 읽어서 날짜 리스트를 외웁니다.
            existing_data_all = ws.get_all_values()
            existing_dates = [row[0] for row in existing_data_all] if existing_data_all else []
            
            prev_shares = {}
            if len(existing_data_all) > 1:
                headers = existing_data_all[0]
                last_row = existing_data_all[-1]
                for i, h in enumerate(headers):
                    if "_수량" in h:
                        stock_name = h.replace("_수량", "")
                        prev_shares[stock_name] = float(str(last_row[i]).replace(',','')) if i < len(last_row) and last_row[i] else 0

            for f in files:
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})|(\d{8})', f)
                file_date = date_match.group() if date_match else datetime.now().strftime("%Y-%m-%d")

                # 💡 [스마트 패스] 구글한테 안 물어보고, 아까 외운 리스트에서 바로 확인!
                if file_date in existing_dates:
                    print(f"   ⏭️ {file_date} 건너뜀 (이미 시트에 있음)")
                    continue

                df, n_col, w_col, q_col = read_etf_data(f)
                if df is None: continue

                df = df.sort_values(by=w_col, ascending=False).head(30)
                new_row = [file_date]
                headers = ["Date"]
                
                for _, r in df.iterrows():
                    s_name = r[n_col]
                    headers.extend([s_name, f"{s_name}_증감", f"{s_name}_수량"])
                    
                    s_weight = r[w_col]
                    s_qty = r[q_col]
                    diff = s_qty - prev_shares.get(s_name, s_qty)
                    diff_str = f"🔴▲{int(diff):,}" if diff > 0 else (f"🔵▼{int(abs(diff)):,}" if diff < 0 else "0")
                    new_row.extend([f"{s_weight}%", diff_str, f"{int(s_qty):,}"])
                    prev_shares[s_name] = s_qty

                if not existing_data_all:
                    ws.append_row(headers)
                    existing_data_all = [headers]
                    time.sleep(1)
                
                ws.append_row(new_row)
                existing_dates.append(file_date) # 💡 방금 올린 날짜도 외워둡니다.
                print(f"   ✅ {file_date} 업로드 완료!")
                time.sleep(2) 

        except Exception as e:
            print(f"   ❌ [{etf_name}] 에러: {e}")
            time.sleep(5)

if __name__ == "__main__":
    process_integration()
