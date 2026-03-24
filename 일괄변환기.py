import os
import pandas as pd
import glob
import re
import gspread

# --- 구글 시트 연결 ---
try:
    gc = gspread.service_account(filename='google_key.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1ZxIYeERuOWOWZudyjpMWpEWA0eljOct_uO9gXg6_2JA/edit')
    print("✅ 시트 연결 성공")
except Exception as e:
    print(f"❌ 시트 연결 에러: {e}")

def read_etf_data(filepath):
    # 💡 모든 표를 다 읽어서 '종목'과 '비중'이 있는 표만 필터링
    df_list = []
    try: df_list = pd.read_html(filepath)
    except:
        try: df_list = [pd.read_excel(filepath)]
        except: df_list = []
    
    target_df = None
    for df in df_list:
        content = df.astype(str).to_string()
        if ('종목' in content or '자산' in content) and ('비중' in content or '비율' in content):
            target_df = df
            break
    
    if target_df is None: target_df = df_list[0]
    
    # 헤더 찾기 및 정리
    for i, row in target_df.iterrows():
        if any('종목' in str(x) or '자산' in str(x) for x in row.values):
            target_df.columns = target_df.iloc[i]
            target_df = target_df.iloc[i+1:].reset_index(drop=True)
            break
    
    target_df.columns = [str(c).replace(' ', '').replace('\n', '') for c in target_df.columns]
    n_col = next(c for c in target_df.columns if '종목' in c or '자산' in c)
    w_col = next(c for c in target_df.columns if '비중' in c or '비율' in c)
    q_col = next(c for c in target_df.columns if any(k in c for k in ['수량', '주식수']))
    
    return target_df, n_col, w_col, q_col

# (메인 실행 루프 및 증감 계산 로직 - 대표님 기존 코드 유지)
# ...
print("🚀 변환 및 업로드 엔진 가동!")
