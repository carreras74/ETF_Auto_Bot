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
# 1. 구글 시트 연결 (보안 유지)
# =========================================
try:
    current_folder = os.getcwd()
    key_path = os.path.join(current_folder, 'google_key.json')
    gc = gspread.service_account(filename=key_path)
    sh = gc.open_by_key("1ZxIYeERuOWOWZudyjpMWpEWA0eljOct_uO9gXg6_2JA")
    print("✅ [연결] 구글 시트 접속 성공!")
except Exception as e:
    print(f"❌ [에러] 구글 시트 연결 실패: {e}")
    sys.exit(1)

def read_etf_data(filepath):
    try:
        # 파일 읽기
        if filepath.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(filepath)
        else:
            try: df = pd.read_csv(filepath, encoding='utf-8-sig')
            except: df = pd.read_csv(filepath, encoding='cp949')

        # 제목줄 찾기 (TIME/KoAct 공통)
        def is_real_header(row):
            strs = [str(x).replace(' ', '') for x in row]
            return any('종목' in s and '코드' not in s for s in strs) and any('비중' in s or '비율' in s for s in strs)
        
        header_found = False
        if is_real_header(df.columns): header_found = True
        else:
            for i, row in df.iterrows():
                if is_real_header(row.values):
                    df.columns = df.iloc[i]; df = df.iloc[i+1:].reset_index(drop=True)
                    header_found = True; break
        if not header_found: return None, None, None, None

        df.columns = [str(c).replace(' ', '').strip() for c in df.columns]
        n_col = next(c for c in df.columns if '종목' in c and '코드' not in c)
        w_col = next(c for c in df.columns if '비중' in c or '비율' in c)
        q_col = next(c for c in df.columns if any(k in c for k in ['수량', '주식수', '주수']))

        # 💡 [긴급수리] 빈 칸('')이나 기호가 있어도 에러 없이 0으로 변환 (errors='coerce')
        df[w_col] = pd.to_numeric(df[w_col].astype(str).str.replace('%','').str.replace(',',''), errors='coerce').fillna(0)
        df[q_col] = pd.to_numeric(df[q_col].astype(str).str.replace(',','').replace('-','0'), errors='coerce').fillna(0)
        
        return df, n_col, w_col, q_col
    except: return None, None, None, None

# =========================================
# 2. 통합 엔진 (거북이 엔진 + 날짜 예외 처리)
# =========================================
def process_integration():
    print("⏳ [준비] KRX 종목 정보 로드 중...")
    try:
        krx = fdr.StockListing('KRX')
        name_to_code = dict(zip(krx['Name'], krx['Code']))
    except: name_to_code = {}

    price_cache = {}
    def get_price(name, date_str):
        code = name_to_code.get(name)
        if not code: return 0
        if code not in price_cache:
            try: price_cache[code] = fdr.DataReader(code, '2026-02-01', datetime.now().strftime('%Y-%m-%d'))
            except: price_cache[code] = pd.DataFrame()
        try:
            target_dt = pd.to_datetime(date_str)
            p_df = price_cache[code]
            if p_df.empty: return 0
            return int(p_df[p_df.index <= target_dt]['Close'].iloc[-1])
        except: return 0

    all_files = glob.glob("*.xls*") + glob.glob("*.csv")
    groups = {}
    for f in all_files:
        if ("TIME" in f or "KoAct" in f) and "google_key" not in f:
            # 💡 [긴급수리] 파일 이름에서 날짜가 아예 없는 '통합완료' 파일은 분석 대상에서 제외
            if "통합완료" in f: continue
            
            clean = re.sub(r'구성종목\(PDF\)|_|\d{4}-\d{2}-\d{2}|\d{8}|\.xls.*|\.csv', '', f).strip()
            if clean:
                if clean not in groups: groups[clean] = []
                groups[clean].append(f)

    for etf_name, f_list in groups.items():
        f_list.sort()
        print(f"\n▶️ [{etf_name}] 정제 공정 시작...")
        try:
            title = etf_name[:30]
            ws_list = [w.title for w in sh.worksheets()]
            if title in ws_list:
                ws = sh.worksheet(title)
                existing = ws.get_all_values()
                df_sheet = pd.DataFrame(existing[1:], columns=existing[0]) if existing else pd.DataFrame(columns=['Date'])
            else:
                ws = sh.add_worksheet(title=title, rows="1000", cols="100")
                df_sheet = pd.DataFrame(columns=['Date'])
            
            prev_shares = {}
            if not df_sheet.empty:
                last = df_sheet.iloc[-1]
                for c in df_sheet.columns:
                    if "_수량" in c:
                        s_name = c.replace("_수량","")
                        val = str(last[c]).replace(',','')
                        prev_shares[s_name] = float(val) if val.replace('.','',1).isdigit() else 0

            new_rows = []
            for f in f_list:
                # 💡 [긴급수리] 날짜 추출 실패 시 에러 내지 않고 다음 파일로 넘어가기
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})|(\d{8})', f)
                if not date_match: continue
                
                d = date_match.group()
                if len(d) == 8: d = f"{d[:4]}-{d[4:6]}-{d[6:]}"
                if d in df_sheet['Date'].values: continue
                
                df, n, w, q = read_etf_data(f)
                if df is None: continue
                
                mult = 100 if df[w].sum() <= 1.5 else 1
                df = df.sort_values(by=w, ascending=False).head(30)
                row = {'Date': d}
                for _, r in df.iterrows():
                    s_name, s_w, s_q = r[n], r[w]*mult, r[q]
                    price = get_price(s_name, d)
                    diff = s_q - prev_shares.get(s_name, s_q)
                    p_str = f" | ₩{price:,}" if price > 0 else " | ₩0"
                    if diff > 0: d_str = f"🔴▲{int(diff):,}{p_str}"
                    elif diff < 0: d_str = f"🔵▼{int(abs(diff)):,}{p_str}"
                    else: d_str = f"0{p_str}"
                    
                    row[s_name], row[f"{s_name}_증감"], row[f"{s_name}_수량"] = f"{s_w:.2f}%", d_str, f"{int(s_q):,}"
                    prev_shares[s_name] = s_q
                new_rows.append(row)
                print(f"   ✅ {d} 가공 완료")

            if new_rows:
                df_sheet = pd.concat([df_sheet, pd.DataFrame(new_rows)], ignore_index=True).fillna("")
                ws.clear()
                ws.update([df_sheet.columns.values.tolist()] + df_sheet.astype(str).values.tolist())
                print(f"   🚀 시트 업데이트 완료! (단속 방지 5초 대기...)")
                time.sleep(5)
        except Exception as e:
            if "429" in str(e):
                print("🚨 구글 단속 발생! 65초간 정지...")
                time.sleep(65)
            else: print(f"   ❌ 에러: {e}")

if __name__ == "__main__":
    process_integration()
    print("\n✨ 모든 데이터 정제 및 통합 완료!")
