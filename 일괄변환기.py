import os
import sys  # 💡 추가
import pandas as pd
# ... (기존 import들) ...

# =========================================
# 1. 구글 시트 및 환경 설정
# =========================================
sh = None  # 💡 초기값 설정
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
    sys.exit(1)  # 💡 연결 실패 시 프로그램을 여기서 즉시 종료합니다!

# =========================================
# 2. 데이터 읽기 전용 엔진
# =========================================
def read_etf_data(filepath):
    df_list = []
    if filepath.endswith('.csv'):
        try: df_list = [pd.read_csv(filepath, encoding='utf-8-sig')]
        except: df_list = [pd.read_csv(filepath, encoding='cp949')]
    else:
        try: df_list = pd.read_html(filepath)
        except:
            try: df_list = [pd.read_excel(filepath)]
            except: df_list = []

    if not df_list: raise ValueError("표 데이터를 찾을 수 없습니다.")

    target_df = None
    for df in df_list:
        content = df.astype(str).to_string()
        if ('종목' in content or '자산' in content) and ('비중' in content or '비율' in content):
            target_df = df
            break
    if target_df is None: target_df = df_list[0]

    for i, row in target_df.iterrows():
        if any(k in str(x) for k in ['종목', '자산', '명칭'] for x in row.values):
            target_df.columns = target_df.iloc[i]
            target_df = target_df.iloc[i+1:].reset_index(drop=True)
            break
    
    target_df.columns = [str(c).replace(' ', '').replace('\n', '') for c in target_df.columns]
    
    n_col = next(c for c in target_df.columns if '종목' in c or '자산' in c or '명' in c)
    w_col = next(c for c in target_df.columns if '비중' in c or '비율' in c)
    q_col = next(c for c in target_df.columns if any(k in c for k in ['수량', '주식수', '주수']))
    
    target_df[w_col] = pd.to_numeric(target_df[w_col].astype(str).str.replace('%',''), errors='coerce').fillna(0)
    target_df[q_col] = pd.to_numeric(target_df[q_col].astype(str).str.replace(',',''), errors='coerce').fillna(0)
    
    return target_df, n_col, w_col, q_col

# =========================================
# 3. 메인 통합 로직 (과속 방지 턱 설치)
# =========================================
def process_integration():
    all_files = glob.glob(os.path.join(current_folder, "*.[xX][lL][sS]*")) + glob.glob(os.path.join(current_folder, "*.csv"))
    
    etf_groups = {}
    for f in all_files:
        name = os.path.basename(f)
        clean_name = re.sub(r'구성종목\(PDF\)|_|\d{4}-\d{2}-\d{2}|\d{8}|\.xlsx|\.xls|\.csv', '', name).strip()
        if clean_name and "google_key" not in clean_name and "통합완료" not in clean_name:
            if clean_name not in etf_groups: etf_groups[clean_name] = []
            etf_groups[clean_name].append(f)

    for etf_name, files in etf_groups.items():
        print(f"\n▶️ [{etf_name}] 처리 중...")
        files.sort()
        time.sleep(2) # 💡 구글 API 과속 방지 (탭 전환 시 휴식)
        
        try:
            title = etf_name[:30]
            # 💡 탭 존재 여부 확인 로직 보강
            worksheet_list = [w.title for w in sh.worksheets()]
            if title in worksheet_list:
                ws = sh.worksheet(title)
            else:
                ws = sh.add_worksheet(title=title, rows="1000", cols="60")
                time.sleep(2)
            
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
                try:
                    df, n_col, w_col, q_col = read_etf_data(f)
                    date_match = re.search(r'(\d{4}-\d{2}-\d{2})|(\d{8})', f)
                    file_date = date_match.group() if date_match else datetime.now().strftime("%Y-%m-%d")

                    # 기존 데이터 날짜 체크 (중복 방지)
                    dates_in_sheet = ws.col_values(1) if len(existing_data) > 0 else []
                    if file_date in dates_in_sheet:
                        print(f"   ⏭️ {file_date} 건너뜀.")
                        continue

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
                        time.sleep(2)
                    
                    ws.append_row(new_row)
                    print(f"   ✅ {file_date} 업로드 완료!")
                    time.sleep(3) # 💡 구글 API 과속 방지 (행 추가 후 충분한 휴식)

                except Exception as inner_e:
                    print(f"   ⚠️ {f} 처리 중 소소한 에러: {inner_e}")
                    time.sleep(5) # 에러 나면 조금 더 쉽니다.

        except Exception as e:
            if "429" in str(e):
                print("🚨 구글 쿼터 초과! 60초간 강제 휴식 후 다음 ETF로 이동합니다...")
                time.sleep(60)
            else:
                print(f"   ❌ 에러: {e}")

if __name__ == "__main__":
    process_integration()
