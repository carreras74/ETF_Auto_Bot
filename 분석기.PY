import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import FinanceDataReader as fdr
import koreanize_matplotlib # 한글 폰트 깨짐 완벽 방지 마법사
import re
from datetime import datetime

current_folder = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()

print("📊 [입체 분석기] 가동 시작...")

# 1. 매입장부 읽기
ledger_path = os.path.join(current_folder, "매입장부.xlsx")
if not os.path.exists(ledger_path):
    print("❌ 매입장부.xlsx 파일이 없습니다.")
    exit()
ledger = pd.read_excel(ledger_path)

# 2. 한국 주식 종목코드 매핑 (로봇 스스로 검색)
print("🔍 한국거래소(KRX) 종목코드 자동 탐색 중...")
krx = fdr.StockListing('KRX')
name_to_code = dict(zip(krx['Name'], krx['Code']))

# 3. 과거 원본 엑셀 파일들 추적 준비
all_files = [f for f in glob.glob(os.path.join(current_folder, "*.*"))
             if f.endswith(('.xlsx', '.xls')) and ("TIME" in f or "KoAct" in f)]

etf_groups = {}
for f in all_files:
    fname = os.path.basename(f)
    date_match = re.search(r'(\d{4}-\d{2}-\d{2}|\d{8})', fname)
    if not date_match: continue
    raw_date = date_match.group()
    file_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}" if len(raw_date) == 8 else raw_date
    etf_name = re.sub(r'구성종목|PDF|기준\s*가격|\d{4}-\d{2}-\d{2}|\d{8}|\.xlsx|\.csv|\.xls|[()_\-\s]', '', fname).strip()

    if etf_name not in etf_groups: etf_groups[etf_name] = []
    etf_groups[etf_name].append({'file': f, 'date': file_date})

def get_weight(filepath, target_stock):
    try:
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

        if n_col and w_col:
            df = df[df[n_col].astype(str).str.strip() != '']
            df[w_col] = pd.to_numeric(df[w_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
            if df[w_col].sum() <= 2.0: df[w_col] = df[w_col] * 100

            target_row = df[df[n_col].astype(str).str.contains(target_stock)]
            if not target_row.empty:
                return target_row.iloc[0][w_col]
    except: pass
    return 0.0

# 4. 분석 및 시각화
for idx, row in ledger.iterrows():
    stock_name = str(row['종목명']).strip()
    buy_date = pd.to_datetime(row['매수일자'])
    sell_date = pd.to_datetime(row['매도일자']) if pd.notna(row['매도일자']) and str(row['매도일자']).strip() != '' else None
    buy_price = row['매수단가']

    stock_code = name_to_code.get(stock_name)
    if not stock_code:
        print(f"⚠️ {stock_name} 종목코드를 찾을 수 없습니다.")
        continue

    print(f"\n▶️ [{stock_name}] 입체 분석 시작...")

    # 4-1. 가장 많이 담고 있는 주력 ETF 자동 탐색
    best_etf, max_weight = None, -1
    for etf_name, files in etf_groups.items():
        latest_file = sorted(files, key=lambda x: x['date'])[-1]['file']
        w = get_weight(latest_file, stock_name)
        if w > max_weight:
            max_weight = w; best_etf = etf_name

    if max_weight <= 0:
        print(f"   => ⚠️ 현재 ETF들이 {stock_name}을(를) 보유하고 있지 않습니다.")
        continue

    print(f"   => 🎯 추적 ETF 자동 선정: {best_etf} (최대비중: {max_weight}%)")

    # 4-2. 과거 비중 히스토리 추출
    weight_history = []
    target_files = sorted(etf_groups[best_etf], key=lambda x: x['date'])
    for f in target_files:
        w = get_weight(f['file'], stock_name)
        weight_history.append({'Date': f['date'], 'Weight': w})

    df_weight = pd.DataFrame(weight_history)
    df_weight['Date'] = pd.to_datetime(df_weight['Date'])
    df_weight.set_index('Date', inplace=True)

    # 4-3. 주가 데이터 수집 (매수 한 달 전부터 뷰 제공)
    start_date = buy_date - pd.Timedelta(days=30)
    end_date = sell_date + pd.Timedelta(days=5) if sell_date else datetime.now()
    
    df_price = fdr.DataReader(stock_code, start_date, end_date)
    if df_price.empty: continue

    # 4-4. 이중 축 그래프 그리기 📈📊
    fig, ax1 = plt.subplots(figsize=(12, 6))
    fig.patch.set_facecolor('#f8f9fa')
    ax1.set_facecolor('#f8f9fa')

    # 주가 (파란 꺾은선)
    ax1.plot(df_price.index, df_price['Close'], color='#1f77b4', linewidth=2.5, label='주가(원)')
    ax1.set_ylabel('주가 (원)', color='#1f77b4', fontweight='bold', fontsize=12)
    ax1.tick_params(axis='y', labelcolor='#1f77b4')

    # 매수/매도 타점 표시
    ax1.axvline(buy_date, color='red', linestyle='--', linewidth=2, label='매수일')
    ax1.plot(buy_date, buy_price, 'ro', markersize=8) 
    if sell_date: ax1.axvline(sell_date, color='blue', linestyle='--', linewidth=2, label='매도일')

    # ETF 비중 (초록 막대)
    ax2 = ax1.twinx()
    merged_df = pd.merge(df_price, df_weight, left_index=True, right_index=True, how='left')
    ax2.bar(merged_df.index, merged_df['Weight'], color='#2ca02c', alpha=0.3, width=1.5, label=f'ETF비중({best_etf})')
    ax2.set_ylabel('ETF 내 비중 (%)', color='#2ca02c', fontweight='bold', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='#2ca02c')

    plt.title(f"📊 {stock_name} ({stock_code}) - 주가 vs {best_etf} 비중", fontsize=16, fontweight='bold', pad=15)
    fig.legend(loc='upper left', bbox_to_anchor=(0.05, 0.95), bbox_transform=ax1.transAxes)

    save_name = f"{stock_name}_분석.png"
    plt.savefig(os.path.join(current_folder, save_name), dpi=300, bbox_inches='tight')
    plt.close()
    print(f"   => 💾 그래프 저장 완료: {save_name}")

print("\n🎉 모든 종목의 입체 분석 그래프 생성이 완료되었습니다!")