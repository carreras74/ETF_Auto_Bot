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

print("🚀 [TIGER 자동 수집기] T-1일 날짜 매칭 + 불도저 재시도 + 순정 포맷 가동!")

# 💡 [날짜 패치] TIGER는 무조건 하루 전(T-1) 영업일 데이터를 가져오도록 세팅!
KST = timezone(timedelta(hours=9))
now = datetime.now(KST)

if now.weekday() == 0: # 월요일 아침 -> 금요일(-3)
    target_date = now - timedelta(days=3)
elif now.weekday() == 6: # 일요일 -> 금요일(-2)
    target_date = now - timedelta(days=2)
else: # 화, 수, 목, 금, 토 아침 -> 하루 전(-1)
    target_date = now - timedelta(days=1)

formatted_date = target_date.strftime("%Y-%m-%d")
print(f"📅 데이터 기록 기준일: {formatted_date}\n")

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

def read_tiger_excel(filepath):
    try: dfs = pd.read_html(filepath, encoding='utf-8')
    except:
        try: dfs = pd.read_html(filepath, encoding='cp949') 
        except: return pd.DataFrame()

    all_tables = []
    for temp_df in dfs:
        header_idx = -1
        if '종목명' in temp_df.columns:
            all_tables.append(temp_df.copy())
        else:
            for i in range(len(temp_df)):
                if '종목명' in str(temp_df.iloc[i].values):
                    header_idx = i
                    break
            if header_idx != -1:
                temp_df.columns = temp_df.iloc[header_idx]
                all_tables.append(temp_df.iloc[header_idx+1:].copy())
                
    if not all_tables: return pd.DataFrame()
    
    df = pd.concat(all_tables, ignore_index=True)
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df['종목명'] != '종목명']
    df = df.dropna(subset=['종목명', '수량(주)', '비중(%)'])
    df = df[~df['종목명'].astype(str).str.contains('원화예금|현금|설정|해지', na=False)]
    
    df['비중(%)'] = pd.to_numeric(df['비중(%)'].astype(str).str.replace(',', ''), errors='coerce')
    df['수량(주)'] = pd.to_numeric(df['수량(주)'].astype(str).str.replace(',', ''), errors='coerce')
    df['평가금액(원)'] = pd.to_numeric(df['평가금액(원)'].astype(str).str.replace(',', ''), errors='coerce')
    df['주가'] = 0
    valid_qty_idx = df['수량(주)'] > 0
    df.loc[valid_qty_idx, '주가'] = (df.loc[valid_qty_idx, '평가금액(원)'] / df.loc[valid_qty_idx, '수량(주)']).astype(int)
    
    return df.sort_values(by='비중(%)', ascending=False).head(20).reset_index(drop=True)

for etf_name, room_url in tiger_rooms.items():
    print(f"\n▶️ [{etf_name}] 수집 시작...")
    
    excel_buttons = []
    target_button = None
    
    for attempt in range(2):
        driver.get(room_url)
        time.sleep(5) 
        
        try:
            driver.execute_script("""
                var popups = document.querySelectorAll('[class*="popup"], [class*="layer"], [class*="modal"], [id*="popup"]');
                popups.forEach(function(el) { el.remove(); });
            """)
        except: pass
        
        for step in range(1, 7):
            driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * ({step}/6));")
            time.sleep(1)
        
        xpath_excel = (
            "//a[contains(@class, 'excel') or contains(@class, 'xls') or contains(text(), '엑셀') or contains(translate(text(), 'EXCEL', 'excel'), 'excel')] | "
            "//button[contains(@class, 'excel') or contains(@class, 'xls') or contains(text(), '엑셀')] | "
            "//span[contains(text(), '엑셀')]/parent::a | "
            "//img[contains(@alt, '엑셀')]/parent::a | "
            "//a[@title='엑셀 다운로드'] | "
            "//a[contains(@href, 'excel')]"
        )
        
        try:
            excel_buttons = wait.until(EC.presence_of_all_elements_located((By.XPATH, xpath_excel)))
            if excel_buttons:
                target_button = excel_buttons[-1]
                break 
        except:
            if attempt == 0:
                print("   ⚠️ 사이트 로딩 지연. 새로고침 후 재시도합니다...")
            else:
                print("❌ 20초 대기 초과 (엑셀 버튼 없음)")
                
    if not target_button:
        continue 
        
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
    
    ext = os.path.splitext(new_file_path)[1]
    final_name = f"TIGER_{etf_name}_{formatted_date}{ext}"
    final_path = os.path.join(target_dir, final_name)
    if os.path.abspath(new_file_path) != os.path.abspath(final_path):
        if os.path.exists(final_path): os.remove(final_path)
        shutil.move(new_file_path, final_path)
    
    df = read_tiger_excel(final_path)
    if df.empty: 
        print("❌ 변환 실패")
        continue

    today_dict = {row['종목명']: {'비중': row['비중(%)'], '수량': row['수량(주)'], '주가': row['주가']} for _, row in df.iterrows()}

    prev_qty = {}
    past_files = [f for f in glob.glob(os.path.join(target_dir, f"TIGER_{etf_name}_*.*")) if formatted_date not in f]
    past_files.sort()
    if past_files:
        last_file = past_files[-1]
        prev_df = read_tiger_excel(last_file)
        if not prev_df.empty:
            for _, row in prev_df.iterrows():
                prev_qty[row['종목명']] = row['수량(주)']

    try: ws = sh.worksheet(etf_name)
    except gspread.exceptions.WorksheetNotFound: ws = sh.add_worksheet(title=etf_name, rows="1000", cols="100")
        
    raw_data = ws.get_all_values()
    existing_data = [row for row in raw_data if any(str(cell).strip() for cell in row)]
    
    if not existing_data:
        headers = ["일자"]
        for stock in today_dict.keys(): headers.extend([stock, f"{stock}_증감"])
        row_data = [formatted_date]
        for stock in today_dict.keys():
            v = today_dict[stock]
            price_str = f" | ₩{int(v['주가']):,}"
            row_data.extend([v['비중'], f"0{price_str}"]) 
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
        for ns in new_stocks: headers.extend([ns, f"{ns}_증감"])
        ws.update(range_name='A1', values=[headers])

    new_row = [formatted_date] + [""] * (len(headers) - 1)
    for stock_name, current_data in today_dict.items():
        idx = headers.index(stock_name)
        curr_qty = current_data['수량']
        p_qty = prev_qty.get(stock_name, 0)
        
        diff = curr_qty - p_qty
        price_str = f" | ₩{int(current_data['주가']):,}"
        
        if diff > 0: diff_str = f"🔴▲ {int(diff):,}{price_str}"
        elif diff < 0: diff_str = f"🔵▼ {abs(int(diff)):,}{price_str}"
        else: diff_str = f"0{price_str}"
        
        new_row[idx] = current_data['비중']
        new_row[idx+1] = diff_str
        
    ws.append_row(new_row)
    print(f"✅ 구글 시트 {formatted_date} 데이터 업데이트 완료")

driver.quit()

print("\n🧹 찌꺼기 파일 청소 중...")
for f in glob.glob(os.path.join(target_dir, "*.xlsx")) + glob.glob(os.path.join(target_dir, "*.xls")):
    fname = os.path.basename(f)
    if "매입장부" not in fname and "TIME" not in fname and "KoAct" not in fname and "TIGER" not in fname:
        try: os.remove(f)
        except: pass

print("\n✨ 모든 작업 완료!")

