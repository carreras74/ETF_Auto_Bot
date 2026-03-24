import os
import pandas as pd
import glob
import re
import time
import gspread

# 💡 [핵심 패치] TIGER의 여러 표 중 '진짜 종목 데이터'가 있는 표만 골라내는 엔진
def read_etf_data(filepath):
    df_list = []
    if filepath.endswith('.csv'):
        try: df_list = [pd.read_csv(filepath, encoding='utf-8-sig', header=None)]
        except: df_list = [pd.read_csv(filepath, encoding='cp949', header=None)]
    else:
        try:
            # TIGER 엑셀(HTML 형식) 내의 모든 표를 일단 다 읽어옵니다.
            df_list = pd.read_html(filepath)
        except:
            try: df_list = [pd.read_excel(filepath, header=None)]
            except: df_list = []

    if not df_list:
        raise ValueError("파일 내에서 표를 추출할 수 없습니다.")

    target_df = None
    # 💡 섞여 있는 표들 중 '종목'과 '비중'이라는 단어가 동시에 발견되는 표가 진짜입니다!
    for temp_df in df_list:
        content_str = temp_df.astype(str).to_string()
        if ('종목' in content_str or '자산' in content_str) and ('비중' in content_str or '비율' in content_str):
            target_df = temp_df
            break
            
    if target_df is None:
        raise ValueError("진짜 종목 데이터가 담긴 표를 찾지 못했습니다.")

    # 표 머리글(Header) 위치 찾기
    header_idx = 0
    for i, row in target_df.iterrows():
        row_strs = [str(x) for x in row.values]
        if any('종목' in s or '자산' in s for s in row_strs) and any('비중' in s or '비율' in s for s in row_strs):
            header_idx = i
            break
            
    target_df.columns = target_df.iloc[header_idx]
    target_df = target_df.iloc[header_idx+1:].reset_index(drop=True)
    target_df.columns = [str(c).replace(' ', '').replace('\n', '') for c in target_df.columns]
    
    n_col = next((c for c in target_df.columns if '종목' in c or '자산' in c or '명' in c), None)
    w_col = next((c for c in target_df.columns if '비중' in c or '비율' in c), None)
    q_col = next((c for c in target_df.columns if any(k in c for k in ['수량', '주식수', '주수'])), None)
    
    return target_df, n_col, w_col, q_col

# ... 이후 변환 및 구글 업로드 로직 ...
