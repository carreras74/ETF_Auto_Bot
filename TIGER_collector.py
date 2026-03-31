import os
import time
import glob
import shutil
from datetime import datetime, timedelta, timezone 
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC 
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import json
import re
import warnings

warnings.filterwarnings('ignore')

print("🚀 [TIGER 자동 수집기] 투명 망토 + 스마트 레이더 모드 가동!")

# 💡 [날짜 완벽 패치] 평일에는 무조건 '오늘' 한국 시간 날짜를 씁니다!
KST = timezone(timedelta(hours=9))
now = datetime.now(KST)

if now.weekday() == 5: # 토요일(5) -> 금요일(-1)
    target_date = now - timedelta(days=1)
elif now.weekday() == 6: # 일요일(6) -> 금요일(-2)
    target_date = now - timedelta(days=2)
else: # 월, 화, 수, 목, 금 -> 무조건 오늘!
    target_date = now

formatted_date = target_date.strftime("%Y-%m-%d")
print(f"📅 데이터 기록 기준일: {formatted_date}")

# 1. 구글 시트 연결
try:
    google_key_json = os.environ.get('GOOGLE_KEY')
    creds_dict = json.loads(google_key_json)
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(credentials)
    spreadsheet_id = "1ZxIYeERuOWOWZudyjpMWpEWA0eljOct_uO9gXg6_2JA"
    sh = gc.open_by_key(spreadsheet_id)
    print("✅ 구글 시트 연결 성공")
except Exception as e:
    print(f"❌ 구글 시트 연결 실패: {e}")
    exit(1)

target_dir = os.getcwd()
download_dir = target_dir

tiger_rooms = {
    "TIGER 기술이전바이오액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7387280001",
    "TIGER 코리아테크액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7365040005",
    "TIGER 퓨처모빌리티액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7471780007"
}

chrome_options = Options()
chrome_options.add_argument('--headless=new')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu') 
chrome_options.add_argument('--disable-software-rasterizer') 
chrome_options.add_argument('--window-size=1920,1080')
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
chrome_options.add_argument("--disable-blink-features=AutomationControlled") 
chrome_options.add_experimental_option('excludeSwitches', ['enable-automation', 'enable-logging']) 
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
})

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
    "source": """ Object.defineProperty(navigator, 'webdriver', { get: () => undefined }) """
})

wait = WebDriverWait(driver, 20)

for etf_name, room_url in tiger_rooms.items():
    print(f"\n▶️ [{etf_name}] 수집 시작...")
    driver.get(room_url)
    
    time.sleep(3)
    
    try:
        driver.execute_script("""
            var popups = document.querySelectorAll('[class*="popup"], [class*="layer"], [class*="modal"], [id*="popup"]');
            popups.forEach(function(el) { el.remove(); });
        """)
    except: pass
    
    for step in range(1, 6):
        driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * ({step}/5));")
        time.sleep(1.5)
    
    xpath_excel = (
        "//a[contains(@class, 'excel') or contains(@class, 'xls') or contains(text(), '엑셀') or contains(translate(text(), 'EXCEL', 'excel'), 'excel')] | "
        "//button[contains(@class, 'excel') or contains(@class, 'xls') or contains(text(), '엑셀')] | "
        "//span[contains(text(), '엑셀')]/parent::a | "
        "//img[contains(@alt, '엑셀')]/parent::a | "
        "//a[@title='엑셀 다운로드']"
    )
    
    try:
        excel_buttons = wait.until(EC.presence_of_all_elements_located((By.XPATH, xpath_excel)))
    except:
        print("❌ 20초를 기다렸지만 엑셀 버튼을 찾지 못했습니다. (페이지가 로봇을 차단했거나 구조가 다름)")
        continue
        
    if not excel_buttons:
        print("❌ 엑셀 버튼 없음")
        continue
        
    target_button = excel_buttons[-1]
    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", target_button)
    time.sleep(1.5)
    
    before_files = set(glob.glob(os.path.join(download_dir, "*.*")))
    driver.execute_script("arguments[0].click();", target_button)
    
    new_file_path = None
    for _ in range(20):
        time.sleep(1)
        after_files = set(glob.glob(os.path.join(download_dir, "*.*")))
        new_files = after_files - before_files
        excel_files = [f for f in new_files if (f.endswith('.xlsx') or f.endswith('.xls') or f.endswith('.csv')) and not f.endswith('.crdownload') and not f.endswith('.tmp')]
        if excel_files:
            new_file_path = list(excel_files)[0]
            break
            
    if not new_file_path:
        print("❌ 다운로드 실패")
        continue
        
    print("✅ 다운로드 성공. 데이터 변환 중...")
    
    try:
        dfs = pd.read_html(new_file_path, encoding='utf-8')
    except:
        try:
            dfs = pd.read_html(new_file_path, encoding='cp949') 
        except Exception as e:
            print(f"❌ 파일 해독 실패: {e}")
            continue

    df = None
    for temp_df in dfs:
        if '종목명' in temp_df.columns or temp_df.isin(['종목명']).any().any():
            df = temp_df.copy()
            break
            
    if df is None: continue

    if '종목명' not in df.columns:
        for i in range(len(df)):
            if '종목명' in str(df.iloc[i].values):
                df.columns = df.iloc[i]
                df = df.iloc[i+1:]
                break

    df = df.dropna(subset=['종목명', '수량(주)', '비중(%)'])
    df = df[~df['종목명'].astype(str).str.contains('원화예금|현금|설정|해지', na=False)]
    df['비중(%)'] = pd.to_numeric(df['비중(%)'].astype(str).str.replace(',', ''), errors='coerce')
    df['수량(주)'] = pd.to_numeric(df['수량(주)'].astype(str).str.replace(',', ''), errors='coerce')
    df['평가금액(원)'] = pd.to_numeric(df['평가금액(원)'].astype(str).str.replace(',', ''), errors='coerce')
    df['주가'] = 0
    valid_qty_idx = df['수량(주)'] > 0
    df.loc[valid_qty_idx, '주가'] = (df.loc[valid_qty_idx, '평가금액(원)'] / df.loc[valid_qty_idx, '수량(주)']).astype(int)
    df = df.sort_values(by='비중(%)', ascending=False).reset_index(drop=True)
    today_dict = {row['종목명']: {'비중': row['비중(%)'], '수량': row['수량(주)'], '주가': row['주가']} for _, row in df.iterrows()}

    try:
        ws = sh.worksheet(etf_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=etf_name, rows="1000", cols="100")
        
    raw_data = ws.get_all_values()
    existing_data = [row for row in raw_data if any(str(cell).strip() for cell in row)]
    
    if not existing_data:
        headers = ["일자"]
        for stock in today_dict.keys():
            headers.extend([stock, f"{stock}_증감"])
        row_data = [formatted_date]
        for stock in today_dict.keys():
            v = today_dict[stock]
            price_str = f"₩{int(v['주가']):,}"
            row_data.extend([v['비중'], f"0 | {price_str} | Q{int(v['수량'])}"])
        ws.update(range_name='A1', values=[headers, row_data])
        print("✅ 첫 데이터 업로드 완료")
        continue
        
    headers = existing_data[0]
    last_row = existing_data[-1]
    
    if last_row[0] == formatted_date:
        print(f"⏩ 이미 {formatted_date} 데이터가 있습니다 (스킵)")
        continue
        
    new_stocks = [s for s in today_dict.keys() if s not in headers]
    if new_stocks:
        for ns in new_stocks:
            headers.extend([ns, f"{ns}_증감"])
        ws.update(range_name='A1', values=[headers])

    yesterday_qty = {}
    for i in range(1, len(headers), 2):
        stock_name = headers[i]
        if stock_name in today_dict:
            change_str = last_row[i+1] if i+1 < len(last_row) else ""
            if " | Q" in change_str:
                try:
                    yesterday_qty[stock_name] = int(change_str.split(" | Q")[1].replace(',', ''))
                except:
                    yesterday_qty[stock_name] = None
            else:
                yesterday_qty[stock_name] = None 
        else:
            yesterday_qty[stock_name] = 0

    new_row = [formatted_date] + [""] * (len(headers) - 1)
    for stock_name, current_data in today_dict.items():
        idx = headers.index(stock_name)
        curr_qty = current_data['수량']
        
        prev_qty = yesterday_qty.get(stock_name)
        if prev_qty is None:
            diff = 0 
        else:
            diff = curr_qty - prev_qty
            
        price_str = f"₩{int(current_data['주가']):,}"
        
        if diff > 0: diff_str = f"🔴▲{diff:,} | {price_str} | Q{int(curr_qty)}"
        elif diff < 0: diff_str = f"🔵▼{abs(diff):,} | {price_str} | Q{int(curr_qty)}"
        else: diff_str = f"0 | {price_str} | Q{int(curr_qty)}"
        
        new_row[idx] = current_data['비중']
        new_row[idx+1] = diff_str
        
    ws.append_row(new_row)
    print(f"✅ 구글 시트 {formatted_date} 데이터 업데이트 완료")

driver.quit()
print("\n✨ 모든 작업 완료!")

