import os
import sys
import pandas as pd
import glob
import re
import time
import gspread

# [시즌 3] TIGER 멀티 테이블 대응 강화판
def read_etf_data(filepath):
    df_list = []
    if filepath.endswith('.csv'):
        try: df_list = [pd.read_csv(filepath, encoding='utf-8-sig', header=None)]
        except: df_list = [pd.read_csv(filepath, encoding='cp949', header=None)]
    else:
        try: # 💡 가짜 엑셀(HTML) 내의 모든 표를 샅샅이 뒤집니다.
            df_list = pd.read_html(filepath)
        except:
            try: df_list = [pd.read_excel(filepath, header=None)]
            except: df_list = []

    if not df_list:
        raise ValueError("파일 내에서 표를 추출할 수 없습니다.")

    # 💡 [핵심 패치] 여러 표 중 '종목'과 '비중/수량'이 모두 있는 진짜 표만 필터링
    target_df = None
    for temp_df in df_list:
        # 모든 데이터를 문자열로 변환 후 검사
        content = temp_df.astype(str).values.flatten()
        content_str = "".join(content)
        
        if ('종목' in content_str or '자산' in content_str) and ('비중' in content_str or '비율' in content_str):
            target_df = temp_df
            break
            
    if target_df is None:
        # 끝까지 못찾으면 첫 번째 표라도 반환 (디버깅용)
        target_df = df_list[0]

    # 헤더(기둥 이름) 찾기
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

# ... (나머지 변환 및 구글 업로드 로직은 동일) ...
# 기존 일괄변환기.py의 나머지 부분을 아래에 유지하세요.
