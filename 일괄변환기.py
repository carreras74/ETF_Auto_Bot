import os
import pandas as pd
import glob
import re
import time
import gspread

# 💡 [시즌 3] TIGER의 멀티 테이블 중 '진짜 종목 데이터'만 솎아내는 엔진
def read_etf_data(filepath):
    df_list = []
    if filepath.endswith('.csv'):
        try: df_list = [pd.read_csv(filepath, encoding='utf-8-sig', header=None)]
        except: df_list = [pd.read_csv(filepath, encoding='cp949', header=None)]
    else:
        try:
            # TIGER 엑셀(사실은 HTML) 내의 모든 표를 싹 다 읽습니다.
            df_list = pd.read_html(filepath)
        except Exception:
            try: df_list = [pd.read_excel(filepath, header=None)]
            except: df_list = []

    if not df_list:
        raise ValueError("표 데이터를 찾을 수 없습니다.")

    target_df = None
    # 💡 여러 표 중 '종목'과 '비중'이 동시에 들어있는 표가 우리가 찾는 진짜입니다!
    for temp_df in df_list:
        temp_str = temp_df.astype(str).to_string()
        if ('종목' in temp_str or '자산' in temp_str) and ('비중' in temp_str or '비율' in temp_str):
            target_df = temp_df
            break
            
    if target_df is None:
        target_df = df_list[0] # 못 찾으면 첫 번째 거라도 시도

    # 실제 기둥(Header) 위치 찾기
    header_idx = 0
    for i, row in target_df.iterrows():
        row_strs = [str(x) for x in row.values]
        if any('종목' in s or '자산' in s for s in row_strs) and any('비중' in s or '비율' in s for s in row_strs):
            header_idx = i
            break
            
    target_df.columns = target_df.iloc[header_idx]
    target_df = target_df.iloc[header_idx+1:].reset_index(drop=True)
    
    # 컬럼명 정리 (공백/줄바꿈 제거)
    target_df.columns = [str(c).replace(' ', '').replace('\n', '').replace('\r', '') for c in target_df.columns]
    
    n_col = next((c for c in target_df.columns if '종목' in c or '자산' in c or '명' in c), None)
    w_col = next((c for c in target_df.columns if '비중' in c or '비율' in c), None)
    q_col = next((c for c in target_df.columns if any(k in c for k in ['수량', '주식수', '주수'])), None)
    
    return target_df, n_col, w_col, q_col

# --- [이하 변환 및 구글 업로드 로직 시작] ---
# (대표님, 기존에 잘 돌아가던 나머지 코드들을 이 아래에 붙여넣으시면 됩니다.)
# 만약 전체 코드가 필요하시면 말씀해 주세요!
