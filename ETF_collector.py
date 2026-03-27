import os
import time
import glob
import shutil
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

target_dir = os.path.dirname(os.path.abspath(__file__))
download_dir = target_dir

date_time = datetime.now().strftime("%Y-%m-%d") 
date_koact = datetime.now().strftime("%Y%m%d")  

print(f"📍 작업 위치: {target_dir}")
print(f"📅 TIME 날짜: {date_time} / KoAct 날짜: {date_koact}\n")

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

# 💡 [핵심 패치] 깃허브 리눅스 서버에서 절대 죽지 않도록 방어 옵션 추가!
chrome_options = Options()
chrome_options.add_argument('--headless=new') # 화면 없는 투명 모드 강제 실행
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu') # 그래픽 가속 충돌 방지
chrome_options.add_argument('--disable-software-rasterizer') # 렌더링 충돌 방지
chrome_options.add_argument('--window-size=1920,1080')
chrome_options.add_argument('--log-level=3')
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True, 
    "profile.default_content_setting_values.automatic_downloads": 1 
})

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
    "source": """ Object.defineProperty(navigator, 'webdriver', { get: () => undefined }) """
})

try:
    print("🚀 [수집기 가동] 시각화 모드 (16개 완전체) 시작!")

    for task in task_list:
        brand = task["brand"]
        rooms = task["etfs"]
        print(f"\n=========================================")
        print(f"🏢 [{brand}] 운용사 포트폴리오 추출 시작...")
        
        for etf_name, room_url in rooms.items():
            try:
                driver.get(room_url)
                time.sleep(5) 
                
                xpath_excel = (
                    "//a[contains(@class, 'excel') or contains(translate(text(), 'EXCEL', 'excel'), 'excel') or contains(text(), '엑셀') or contains(@href, 'excel')] | "
                    "//button[contains(@class, 'excel') or contains(translate(text(), 'EXCEL', 'excel'), 'excel') or contains(text(), '엑셀')] | "
                    "//img[contains(@alt, '엑셀') or contains(translate(@alt, 'EXCEL', 'excel'), 'excel')]/parent::a"
                )
                
                excel_buttons = driver.find_elements(By.XPATH, xpath_excel)
                if excel_buttons:
                    target_button = excel_buttons[-1] 
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", target_button)
                    time.sleep(1.5)
                    
                    before_files = set(glob.glob(os.path.join(download_dir, "*.*")))
                    driver.execute_script("arguments[0].click();", target_button)
                    print(f"📥 [{brand}] {etf_name} 버튼 클릭 완료!", end="\r")
                    
                    time.sleep(1)
                    try:
                        alert = driver.switch_to.alert
                        alert.accept() 
                        print(f"⚠️ [{brand}] {etf_name} 다운로드 스킵 (경고창 무시).")
                        continue 
                    except:
                        pass 
                    
                    new_file_path = None
                    for _ in range(15):
                        time.sleep(1)
                        after_files = set(glob.glob(os.path.join(download_dir, "*.*")))
                        new_files = after_files - before_files
                        excel_files = [f for f in new_files if (f.endswith('.xlsx') or f.endswith('.xls') or f.endswith('.csv')) and not f.endswith('.crdownload') and not f.endswith('.tmp')]
                        
                        if excel_files:
                            new_file_path = list(excel_files)[0]
                            break
                    
                    if new_file_path:
                        ext = os.path.splitext(new_file_path)[1]
                        if brand == "TIME": final_name = f"구성종목(PDF){brand}{etf_name}_{date_time}{ext}"
                        else: final_name = f"{brand} {etf_name}_{date_koact}{ext}"
                            
                        final_path = os.path.join(target_dir, final_name)
                        
                        if os.path.abspath(new_file_path) != os.path.abspath(final_path):
                            if os.path.exists(final_path): os.remove(final_path)
                            shutil.move(new_file_path, final_path)
                            
                        print(f"\n✅ [{brand}] {etf_name} 수집 성공!      ")
                    else: print(f"\n⚠️ [{brand}] {etf_name} 다운로드 지연.")
                else: print(f"\n❌ [{brand}] {etf_name} 엑셀 버튼을 찾을 수 없습니다.")
            except Exception as e: print(f"\n❌ [{brand}] {etf_name} 에러 발생: {e}")
            time.sleep(2)

finally:
    time.sleep(2)
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

print("\n✨ 16개 ETF 수집 및 청소 공정 완벽 종료!")
