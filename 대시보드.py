import streamlit as st
import pandas as pd
import glob
import os
import re
import plotly.express as px

# ⚙️ 스트림릿 화면 넓게 쓰기 설정
st.set_page_config(layout="wide", page_title="ETF 수급 로테이션 추적기")

st.title("🚀 TIME & KoAct 수급 로테이션 범프 차트 (최근 20일)")
st.markdown("매일 생성되는 **'최근 5일 누적 찐 주도주 Top 20'** 명단에 한 번이라도 올랐던 종목들의 20일간 순위 쟁탈전을 추적합니다.")

@st.cache_data(ttl=600)
def load_and_process_data():
    try:
        current_folder = os.path.dirname(os.path.abspath(__file__))
    except:
        current_folder = os.getcwd()

    all_files = glob.glob(os.path.join(current_folder, "통합완료_*.csv"))
    
    if not all_files:
        return pd.DataFrame(), pd.DataFrame()

    all_data = []
    
    # 1. '🔴▲ 150 | ₩50,000' 형태에서 순매수 금액 해독
    def parse_net_buy(val):
        if pd.isna(val) or str(val).strip() == '': return 0
        val_str = str(val)
        if '|' not in val_str: return 0
        try:
            qty_part, price_part = val_str.split('|')
            sign = 1 if '🔴' in qty_part else (-1 if '🔵' in qty_part else 0)
            qty_clean = re.sub(r'[^\d]', '', qty_part)
            qty = int(qty_clean) * sign if qty_clean else 0
            price_clean = re.sub(r'[^\d]', '', price_part.split('(')[0])
            price = int(price_clean) if price_clean else 0
            return qty * price
        except:
            return 0

    # 파일 읽기 및 병합
    for f in all_files:
        brand = "TIME" if "TIME" in f else ("KoAct" if "KoAct" in f else "Other")
        if brand == "Other": continue
            
        df = pd.read_csv(f)
        if 'Date' not in df.columns: continue
            
        diff_cols = [c for c in df.columns if c.endswith('_증감')]
        melted = df.melt(id_vars=['Date'], value_vars=diff_cols, var_name='Stock', value_name='Diff_String')
        melted['Stock'] = melted['Stock'].str.replace('_증감', '')
        melted['Brand'] = brand
        melted['Net_Buy_Amount'] = melted['Diff_String'].apply(parse_net_buy)
        all_data.append(melted)

    if not all_data: return pd.DataFrame(), pd.DataFrame()

    master_df = pd.concat(all_data, ignore_index=True)
    master_df['Date'] = pd.to_datetime(master_df['Date'])
    
    # 운용사, 날짜, 종목별 순매수 합산
    daily_grouped = master_df.groupby(['Brand', 'Date', 'Stock'])['Net_Buy_Amount'].sum().reset_index()

    # 2. 선생님의 핵심 전략: "20번의 Top 20 그래프 명단 추출 및 추적"
    def get_bump_data(brand_df):
        if brand_df.empty: return pd.DataFrame()
        
        # 가로축이 날짜, 세로축이 종목인 피벗 테이블 생성 (빈칸은 0)
        pivot_df = brand_df.pivot(index='Date', columns='Stock', values='Net_Buy_Amount').fillna(0)
        
        # 💡 매일매일 '최근 5일치 합산' 계산 (Rolling Window)
        rolling_5d = pivot_df.rolling(window=5, min_periods=1).sum()
        
        # 전체 날짜 중 최근 20영업일만 추출
        dates = sorted(rolling_5d.index.unique())
        last_20_dates = dates[-20:]
        rolling_5d_20d = rolling_5d.loc[last_20_dates]
        
        # 💡 명예의 전당: 20일 동안 '하루라도' 당일 5일 합산 기준 Top 20에 들었던 종목 모으기 (중복 하나로)
        target_universe = set()
        for date in last_20_dates:
            daily_top20 = rolling_5d_20d.loc[date].nlargest(20).index.tolist()
            target_universe.update(daily_top20)
            
        target_universe = list(target_universe)
        
        # 명예의 전당에 오른 종목들만의 20일치 데이터 필터링
        universe_df = rolling_5d_20d[target_universe]
        
        # 다시 길게 풀어서(Melt) 그래프용 데이터로 변환
        long_df = universe_df.reset_index().melt(id_vars='Date', var_name='Stock', value_name='Rolling_5d_Amount')
        
        # 매일매일의 5일 합산액 기준으로 순위 매기기 (금액 높은 순)
        long_df['Rank'] = long_df.groupby('Date')['Rolling_5d_Amount'].rank(method='min', ascending=False)
        
        return long_df.sort_values(['Date', 'Rank'])

    time_bump = get_bump_data(daily_grouped[daily_grouped['Brand'] == 'TIME'])
    koact_bump = get_bump_data(daily_grouped[daily_grouped['Brand'] == 'KoAct'])

    return time_bump, koact_bump

with st.spinner("최근 20일 동안 생성된 20번의 Top 20 명단을 모아 궤적을 계산 중입니다..."):
    time_df, koact_df = load_and_process_data()

# 3. 범프 차트 그리기
def draw_bump_chart(df, brand_name):
    if df.empty:
        st.warning(f"{brand_name} 데이터가 아직 충분하지 않거나 파일이 없습니다.")
        return
        
    fig = px.line(
        df, 
        x='Date', 
        y='Rank', 
        color='Stock', 
        markers=True,
        hover_data={'Rolling_5d_Amount': ':,.0f', 'Rank': True, 'Date': '|%Y-%m-%d'},
        title=f"🏆 {brand_name} 주도주 순위 쟁탈전 (한 번이라도 Top 20에 들었던 종목들)"
    )
    
    # 1등이 제일 위에 보이도록 Y축 뒤집기
    fig.update_yaxes(autorange="reversed", tickmode='linear', tick0=1, dtick=1, title="순위 (최근 5일 합산 순매수액 기준)")
    fig.update_xaxes(title="날짜", tickformat="%m-%d", type='category') # 주말/공백 제외하고 딱 있는 날짜만 표시
    
    fig.update_traces(line=dict(width=4), marker=dict(size=10))
    fig.update_layout(
        height=700, 
        hovermode="x unified",
        legend_title_text=f'주도주 라인업 ({len(df["Stock"].unique())}종목)'
    )
    
    st.plotly_chart(fig, use_container_width=True)

tab1, tab2 = st.tabs(["🔥 TIME 주도주 로테이션", "🔥 KoAct 주도주 로테이션"])

with tab1:
    draw_bump_chart(time_df, "TIME")

with tab2:
    draw_bump_chart(koact_df, "KoAct")

st.info("💡 **차트 팁:** 20일 동안 한 번이라도 '당일 Top 20'에 뽑혔던 종목들 전체의 궤적입니다. 선이 끊기지 않고 이어지며, 마우스를 올리면 그날 기준 '최근 5일 합산 순매수액'을 볼 수 있습니다.")