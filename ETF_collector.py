import os
import sys
import time
import glob
import shutil
from datetime import datetime, timedelta, timezone
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# ==========================================================
# 🛠️ [선생님의 디버깅 모드 설정] 
# 깃허브 서버(GitHub Actions) 환경이므로 반드시 False로 설정합니다!
DEBUG_MODE = False 
# ==========================================================

KST = timezone(timedelta(hours=9))
now = datetime.now(KST)

# 💡 [강제 17일(금요일) 타겟팅 로직]
if now.weekday() >= 5:  
    days_to_subtract = now.weekday() - 4 
    target_date = now - timedelta(days=days_to_subtract)
    print(f"⚠️ [주말 모드] 누락된 금요일 데이터를 복구하기 위해 기준일을 강제로 {target_date.strftime('%Y-%m-%d')}로 설정합니다.")
else:
    target_date = now
    print(f"✅ [평일 정상 가동] 오늘({target_date.strftime('%Y-%m-%d')}) 데이터를 수집합니다.")

target_dir = os.path.dirname(os.path.abspath(__file__))
download_dir = target_dir

date_time = target_date.strftime("%Y-%m-%d") # TIME용
date_koact = target_date.strftime("%Y%m%d")   # KoAct용

print(f"📍 작업 위치: {target_dir}")
print(f"📅 [최종 파일 기준일] TIME: {date_time} / KoAct: {date_koact}\n")

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

task_list = [
    {"brand": "TIME", "etfs": time_rooms},
    {"brand": "KoAct", "etfs": koact_rooms}
]

chrome_options = Options()

if not DEBUG_MODE:
    chrome_options.add_argument('--headless=new') # 깃허브 서버용 필수 옵션

chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--window-size=1920,1080')
chrome_options.add_argument('--log-level=3')
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True, 
    "profile.default_content_setting_values.automatic_downloads": 1 
})

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

try:
    print("🚀 [수집기 가동] 1군(TIME, KoAct) 릴레이 시작!")

    for task in task_list:
        brand = task["brand"]
        rooms = task["etfs"]
        print(f"\n=========================================")
        print(f"🏢 [{brand}] 운용사 포트폴리오 추출 시작...")
        
        for etf_name, room_url in rooms.items():
            try:
                print(f"⏳ [{brand}] {etf_name} 접속 중...")
                driver.get(room_url)
                
                time.sleep(5) 
                
                # ==========================================================
                # 🔥 [수정됨] TIME 브랜드일 경우 '첫 번째[0]' 검색 버튼 클릭
                # ==========================================================
                if brand == "TIME":
                    print(f"   🔍 [{brand}] 정확한 데이터 갱신을 위해 '검색' 버튼 선행 클릭 시도...")
                    xpath_search = "//button[contains(text(), '검색')] | //a[contains(text(), '검색')] | //*[contains(@class, 'btn') and contains(text(), '검색')]"
                    search_buttons = driver.find_elements(By.XPATH, xpath_search)
                    
                    if search_buttons:
                        visible_search = [btn for btn in search_buttons if btn.is_displayed()]
                        if visible_search:
                            target_search = visible_search[0]
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", target_search)
                            time.sleep(1)
                            driver.execute_script("arguments[0].click();", target_search)
                            print(f"   ✔️ 첫 번째 '검색' 버튼 클릭 완료! 데이터 테이블 로딩 대기 중 (4초)...")
                            time.sleep(4)
                        else:
                            print(f"   ⚠️ 화면에 표시된 '검색' 버튼이 없습니다.")
                    else:
                        print(f"   ⚠️ '검색' 태그를 찾지 못했습니다.")
                # ==========================================================

                xpath_excel = (
                    "//*[contains(@class, 'excel') or contains(translate(@class, 'EXCEL', 'excel'), 'excel')] | "
                    "//a[contains(translate(text(), 'EXCEL', 'excel'), 'excel') or contains(text(), '엑셀') or contains(@href, 'excel')] | "
                    "//button[contains(translate(text(), 'EXCEL', 'excel'), 'excel') or contains(text(), '엑셀')] | "
                    "//img[contains(@alt, '엑셀') or contains(translate(@alt, 'EXCEL', 'excel'), 'excel')]/parent::* | "
                    "//a[contains(@onclick, 'excel') or contains(@onclick, 'Excel')] | "
                    "//*[contains(text(), 'PDF 다운로드') or contains(text(), 'PDF다운로드')]"
                )
                
                excel_buttons = driver.find_elements(By.XPATH, xpath_excel)
                
                if not excel_buttons:
                    excel_buttons = driver.find_elements(By.XPATH, "//*[contains(translate(text(), 'EXCEL', 'excel'), 'excel') or contains(text(), '엑셀')]")

                if excel_buttons:
                    visible_buttons = [btn for btn in excel_buttons if btn.is_displayed()]
                    
                    if brand == "TIME":
                        target_button = visible_buttons[0] if visible_buttons else excel_buttons[0]
                    else:
                        target_button = visible_buttons[-1] if visible_buttons else excel_buttons[-1]
                    
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", target_button)
                    time.sleep(2)
                    
                    before_files = set(glob.glob(os.path.join(download_dir, "*.*")))
                    
                    print(f"👆 [{brand}] {etf_name} 엑셀 다운로드 버튼 클릭 시도!")
                    driver.execute_script("arguments[0].click();", target_button)
                    
                    time.sleep(2)
                    try:
                        alert = driver.switch_to.alert
                        alert.accept() 
                    except:
                        pass 
                    
                    new_file_path = None
                    print(f"📥 다운로드 대기 중 (최대 15초)...", end="")
                    for i in range(15):
                        time.sleep(1)
                        after_files = set(glob.glob(os.path.join(download_dir, "*.*")))
                        new_files = after_files - before_files
                        excel_files = [f for f in new_files if (f.endswith('.xlsx') or f.endswith('.xls') or f.endswith('.csv')) and not f.endswith('.crdownload') and not f.endswith('.tmp')]
                        
                        if excel_files:
                            new_file_path = list(excel_files)[0]
                            print(" ⭕ 완료!")
                            break
                        print(".", end="", flush=True)
                    
                    if new_file_path:
                        ext = os.path.splitext(new_file_path)[1]
                        if brand == "TIME": final_name = f"구성종목(PDF){brand}{etf_name}_{date_time}{ext}"
                        else: final_name = f"{brand} {etf_name}_{date_koact}{ext}"
                            
                        final_path = os.path.join(target_dir, final_name)
                        
                        if os.path.abspath(new_file_path) != os.path.abspath(final_path):
                            if os.path.exists(final_path): os.remove(final_path)
                            shutil.move(new_file_path, final_path)
                            
                        print(f"✅ [{brand}] {etf_name} 저장 완료 -> {final_name}")
                    else: 
                        print(f"\n⚠️ [{brand}] {etf_name} 다운로드가 안 되었습니다. 폴더를 확인해주세요.")
                else: 
                    print(f"\n❌ [{brand}] {etf_name} 엑셀 버튼 자체를 찾지 못했습니다!")
            except Exception as e: 
                print(f"\n❌ [{brand}] {etf_name} 에러 발생: {e}")
            time.sleep(3)

finally:
    print("\n🛑 모든 작업이 완료되었습니다. 브라우저를 닫습니다.")
    time.sleep(3)
    driver.quit()

safe_files = ["매입장부.xlsx"] 

print("\n🧹 찌꺼기 파일 청소 중...")
for f in glob.glob(os.path.join(target_dir, "*.xlsx")) + glob.glob(os.path.join(target_dir, "*.xls")):
    fname = os.path.basename(f)
    if fname not in safe_files and "TIME" not in fname and "KoAct" not in fname:
        try: 
            os.remove(f)
            print(f"   🗑️ 쓰레기 파일 삭제 완료: {fname}")
        except: pass

print("\n✨ ETF 수집 공정 완벽 종료!")
