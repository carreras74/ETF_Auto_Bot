import os
import sys
import time
import glob
import shutil
import json
import gspread
from datetime import datetime, timedelta, timezone
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# =========================================
# 1. 시간 설정 (한국 시간 KST 기준)
# =========================================
kst = timezone(timedelta(hours=9))
now_kst = datetime.now(kst)
date_time = now_kst.strftime("%Y-%m-%d") 
date_koact = now_kst.strftime("%Y%m%d")
current_hour = now_kst.hour

target_dir = os.path.dirname(os.path.abspath(__file__))
download_dir = target_dir

print(f"📍 작업 위치: {target_dir}")
print(f"📅 현재 시간(KST): {now_kst.strftime('%Y-%m-%d %H:%M')}")

# =====================================================================
# 💡 [하이브리드 엔진] 아침 6시~7시는 무조건 수집, 나머지 시간은 시트 체크
# =====================================================================
def should_collect_now():
    # 1. 아침 6시~7시 사이(KST) 실행인 경우: 무조건 수집 (아침 6시 50분 포함)
    if current_hour == 6 or current_hour == 7:
        print("🌅 [아침 정기 수집] 아침 첫 데이터는 무조건 수집을 강행합니다! (시트 체크 생략)")
        return True

    # 2. 그 외의 시간: 구글 시트 확인 후 중복이면 퇴근
    print("🔍 [스마트 체크] 아침 외 시간대이므로 구글 시트를 확인합니다...")
    try:
        key_path = os.path.join(target_dir, 'google_key.json')
        if not os.path.exists(key_path):
            print("⚠️ google_key.json 파일이 없어 수집을 시작합니다.")
            return True

        gc = gspread.service_account(filename=key_path)
        sh = gc.open_by_key("1ZxIYeERuOWOWZudyjpMWpEWA0eljOct_uO9gXg6_2JA")
        
        # 429 단속 대비 최대 2번 확인
        for attempt in range(2):
            try:
                ws = sh.worksheet("TIME코스닥액티브")
                all_values = ws.col_values(1)
                existing_dates = all_values[-5:] if all_values else []
                
                if date_time in existing_dates:
                    print(f"🎯 [알림] 오늘({date_time}) 데이터가 이미 존재합니다. 안전하게 퇴근합니다.")
                    return False # 수집하지 않음
                return True # 데이터 없으면 수집
            except Exception as e:
                if "429" in str(e):
                    print(f"🚨 구글 단속 중... 10초 뒤 재시도... ({attempt+1}/2)")
                    time.sleep(10)
                else: raise e
        
        print("⚠️ 단속으로 확인 불가. 안전을 위해 수집을 강행합니다.")
        return True
    except Exception as e:
        print(f"⚠️ 체크 중 에러 발생: {e}. 수집을 강행합니다.")
        return True

# 수집 여부 판단
if not should_collect_now():
    print("🛑 수집 생략 조건 충족. 프로그램을 종료합니다. 🚀")
    sys.exit(0)

print("🚀 본격적인 웹 수집 공정을 시작합니다!")
print("=====================================================================\n")


# =====================================================================
# 2. 메인 수집 로직 (Selenium)
# =====================================================================
time_rooms = {
    "코스닥액티브": "https://timeetf.co.kr/m11_view.php?idx=24&cate=002",
    "플러스배당액티브": "https://timeetf.co.kr/m11_view.php?idx=12&cate=002",
    "코스피액티브": "https://timeetf.co.kr/m11_view.php?idx=11&cate=002",
    "밸류업액티브": "https://timeetf.co.kr/m11_view.php?idx=15&cate=002",
    "신재생에너지액티브": "https://timeetf.co.kr/m11_view.php?idx=16&cate=002",
    "바이오액티브": "https://timeetf.co.kr/m11_view.php?idx=13&cate=002",
    "이노베이션액티브": "https://timeetf.co.kr/m11_view.php?idx=17&cate=002",
    "컬처액티브": "https://timeetf.co.kr/m11_view.php?idx=1&cate=002"
}

koact_rooms = {
    "배당성장액티브": "https://www.samsungactive.co.kr/etf/view.do?id=2ETFM2",
    "수소전력ESS인프라액티브": "https://www.samsungactive.co.kr/etf/view.do?id=2ETFT9",
    "바이오헬스케어액티브": "https://www.samsungactive.co.kr/etf/view.do?id=2ETFJ9",
    "코리아밸류업액티브": "https://www.samsungactive.co.kr/etf/view.do?id=2ETFP3",
    "K수출핵심기업TOP30액티브": "https://www.samsungactive.co.kr/etf/view.do?id=2ETFR6",
    "AI인프라액티브": "https://www.samsungactive.co.kr/etf/view.do?id=2ETFN8",
    "반도체2차전지핵심소재액티브": "https://www.samsungactive.co.kr/etf/view.do?id=2ETFM8",
    "코스닥액티브": "https://www.samsungactive.co.kr/etf/view.do?id=2ETFU6"
}

task_list = [{"brand": "TIME", "etfs": time_rooms}, {"brand": "KoAct", "etfs": koact_rooms}]

chrome_options = Options()
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--window-size=1920x1080')
chrome_options.add_argument('--log-level=3')
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True, 
    "profile.default_content_setting_values.automatic_downloads": 1 
})

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

try:
    for task in task_list:
        brand = task["brand"]
        rooms = task["etfs"]
        print(f"\n🏢 [{brand}] 추출 시작...")
        
        for etf_name, room_url in rooms.items():
            try:
                driver.get(room_url)
                time.sleep(5)
                xpath_excel = "//a[contains(@class, 'excel') or contains(text(), '엑셀')] | //button[contains(@class, 'excel') or contains(text(), '엑셀')] | //img[contains(@alt, '엑셀')]/parent::a"
                excel_buttons = driver.find_elements(By.XPATH, xpath_excel)
                if excel_buttons:
                    target_button = excel_buttons[-1]
                    before_files = set(glob.glob(os.path.join(download_dir, "*.*")))
                    driver.execute_script("arguments[0].click();", target_button)
                    
                    new_file_path = None
                    for _ in range(15):
                        time.sleep(1)
                        after_files = set(glob.glob(os.path.join(download_dir, "*.*")))
                        new_files = after_files - before_files
                        excel_files = [f for f in new_files if f.endswith(('.xlsx', '.xls', '.csv')) and '.crdownload' not in f]
                        if excel_files:
                            new_file_path = list(excel_files)[0]
                            break
                    
                    if new_file_path:
                        ext = os.path.splitext(new_file_path)[1]
                        # 파일명 규칙 유지
                        if brand == "TIME": final_name = f"구성종목(PDF){brand}{etf_name}_{date_time}{ext}"
                        else: final_name = f"{brand} {etf_name}_{date_koact}{ext}"
                        
                        final_path = os.path.join(target_dir, final_name)
                        if new_file_path != final_path:
                            if os.path.exists(final_path): os.remove(final_path)
                            shutil.move(new_file_path, final_path)
                        print(f"✅ [{brand}] {etf_name} 성공!")
                else: print(f"❌ [{brand}] {etf_name} 버튼 없음.")
            except Exception as e: print(f"❌ 에러: {e}")

finally:
    driver.quit()
    print("\n🧹 불필요한 찌꺼기 파일 정리 중...")
    # 오늘 자 데이터(TIME/KoAct)가 아닌 엑셀 파일은 삭제
    for f in glob.glob(os.path.join(target_dir, "*.xlsx")) + glob.glob(os.path.join(target_dir, "*.xls")):
        fname = os.path.basename(f)
        if "TIME" not in fname and "KoAct" not in fname:
            try: os.remove(f)
            except: pass
    print("✨ 모든 공정 완료!")
