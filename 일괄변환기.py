import os
import pandas as pd
import glob
import re
import time
import gspread
from datetime import datetime

# 💡 [핵심 패치] TIGER의 멀티 테이블 중 '진짜 종목 데이터'만 솎아내는 함수
def read_etf_data(filepath):
    df_list = []
    if filepath.endswith('.csv'):
        try: df_list = [pd.read_csv(filepath, encoding='utf-8-sig', header=None)]
        except: df_list = [pd.read_csv(filepath, encoding='cp949', header=None)]
    else:
        try:
            # TIGER .xls(실제는 HTML) 파일 내의 모든 표를 싹 다 읽습니다.
            df_list = pd.read_html(filepath)
        except Exception as e:
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
        # 못 찾으면 첫 번째 표라도 반환해서 시도합니다.
        target_df = df_list[0]

    # 실제 기둥(Header) 위치 찾기
    header_idx = 0
    for i, row in target_df.iterrows():
        row_strs = [str(x) for x in row.values]
        if any('종목' in s or '자산' in s for s in row_strs) and any('비중' in s or '비율' in s for s in row_strs):
            header_idx = i
            break
            
    target_df.columns = target_df.iloc[header_idx]
    target_df = target_df.iloc[header_idx+1:].reset_index(drop=True)
    
    # 컬럼명 특수문자 및 공백 제거
    target_df.columns = [str(c).replace(' ', '').replace('\n', '').replace('\r', '') for c in target_df.columns]
    
    # 핵심 컬럼(종목명, 비중, 수량) 자동 탐지
    n_col = next((c for c in target_df.columns if '종목' in c or '자산' in c or '명' in c), None)
    w_col = next((c for c in target_df.columns if '비중' in c or '비율' in c), None)
    q_col = next((c for c in target_df.columns if any(k in c for k in ['수량', '주식수', '주수'])), None)
    
    return target_df, n_col, w_col, q_col

# [참고] 이후 구글 시트 업로드 및 변환 로직은 기존과 동일하게 유지하시면 됩니다.
print("✅ TIGER 멀티테이블 대응 엔진 장착 완료!")
