import os
import sys
import pandas as pd
import glob
import re
import time
import gspread
from datetime import datetime
import FinanceDataReader as fdr

# 구글 시트 연결
try:
    gc = gspread.service_account(filename='google_key.json')
    sh = gc.open_by_key("1ZxIYeERuOWOWZudyjpMWpEWA0eljOct_uO9gXg6_2JA")
except:
    sys.exit(1)

def read_etf_data(filepath):
    try:
        df = pd.read_excel(filepath) if filepath.endswith(('.xls', '.xlsx')) else pd.read_csv(filepath, encoding='utf-8-sig')
        def is_real_header(row):
            strs = [str(x).replace(' ', '') for x in row]
            return any('종목' in s and '코드' not in s for s in strs) and any('비중' in s for s in strs)
        
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
        w_col = next(c for c in df.columns if '비중' in c)
        q_col = next(c for c in df.columns if '수량' in c)
        df[w_col] = pd.to_numeric(df[w_col].astype(str).str.replace('%',''), errors='coerce').fillna(0)
        df[q_col] = pd.to_numeric(df[q_col].astype(str).str.replace(',',''), errors='coerce').fillna(0)
        return df, n_col, w_col, q_col
    except: return None, None, None, None

def process_integration():
    krx = fdr.StockListing('KRX')
    name_to_code = dict(zip(krx['Name'], krx['Code']))
    price_cache = {}

    def get_price(name, date_str):
        code = name_to_code.get(name)
        if not code: return 0
        if code not in price_cache:
            price_cache[code] = fdr.DataReader(code, '2026-02-01', datetime.now().strftime('%Y-%m-%d'))
        try:
            target_dt = pd.to_datetime(date_str)
            p_df = price_cache[code]
            return int(p_df[p_df.index <= target_dt]['Close'].iloc[-1])
        except: return 0

    files = glob.glob("*.xls*") + glob.glob("*.csv")
    groups = {}
    for f in files:
        if "TIME" in f or "KoAct" in f:
            clean = re.sub(r'구성종목\(PDF\)|_|\d{4}-\d{2}-\d{2}|\d{8}|\.xls.*|\.csv', '', f).strip()
            if clean not in groups: groups[clean] = []
            groups[clean].append(f)

    for etf_name, f_list in groups.items():
        f_list.sort()
        try:
            ws = sh.worksheet(etf_name[:30]) if etf_name[:30] in [w.title for w in sh.worksheets()] else sh.add_worksheet(title=etf_name[:30], rows="1000", cols="60")
            existing = ws.get_all_values()
            df_sheet = pd.DataFrame(existing[1:], columns=existing[0]) if existing else pd.DataFrame(columns=['Date'])
            
            prev_shares = {}
            if not df_sheet.empty:
                last = df_sheet.iloc[-1]
                for c in df_sheet.columns:
                    if "_수량" in c: prev_shares[c.replace("_수량","")] = float(str(last[c]).replace(',',''))

            new_rows = []
            for f in f_list:
                d = re.search(r'(\d{4}-\d{2}-\d{2})|(\d{8})', f).group()
                if len(d)==8: d = f"{d[:4]}-{d[4:6]}-{d[6:]}"
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
                    p_str = f" | ₩{price:,}" if price>0 else " | ₩0"
                    if diff > 0: d_str = f"🔴▲{int(diff):,}{p_str}"
                    elif diff < 0: d_str = f"🔵▼{int(abs(diff)):,}{p_str}"
                    else: d_str = f"0{p_str}"
                    row[s_name], row[f"{s_name}_증감"], row[f"{s_name}_수량"] = f"{s_w:.2f}%", d_str, f"{int(s_q):,}"
                    prev_shares[s_name] = s_q
                new_rows.append(row)

            if new_rows:
                df_sheet = pd.concat([df_sheet, pd.DataFrame(new_rows)], ignore_index=True).fillna("")
                ws.clear()
                ws.update([df_sheet.columns.values.tolist()] + df_sheet.astype(str).values.tolist())
                print(f"✅ {etf_name} 완료 (5초 대기...)")
                time.sleep(5) # 💡 과속 방지
        except Exception as e:
            if "429" in str(e): time.sleep(65)
            else: print(f"❌ {etf_name} 에러: {e}")

if __name__ == "__main__": process_integration()
