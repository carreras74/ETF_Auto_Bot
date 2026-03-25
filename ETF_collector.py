import os
import sys
import time
import glob
import shutil
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# 1. 경로 및 날짜 설정
target_dir = os.getcwd()
download_dir = target_dir

date_time = datetime.now().strftime("%Y-%m-%d") 
date_koact = datetime.now().strftime("%Y%m%d")  

print(f"📍 작업 위치: {target_dir}")
print(f"📅 수집 기준 날짜: {date_time}\n")

# 2. 수집 대상 리스트 (16개 완전체)
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

# 3. 브라우저 설정
chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--window-size=1920x1080')
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
    print("🚀 [수집 공정 가동] 불필요한 체크 없이 즉시 수집을 시작합니다!")

    for brand, rooms in [("TIME", time_rooms), ("KoAct", koact_rooms)]:
        print(f"\n🏢 [{brand}] 운용사 접속 중...")
        for etf_name, url in rooms.items():
            try:
                driver.get(url)
                time.sleep(6) # 페이지 로딩을 위한 충분한 대기
                
                # 가장 검증된 XPath 방식
                xpath_excel = (
                    "//a[contains(@class, 'excel') or contains(text(), '엑셀') or contains(@href, 'excel')] | "
                    "//button[contains(@class, 'excel') or contains(text(), '엑셀')] | "
                    "//img[contains(@alt, '엑셀')]/parent::a"
                )
                
                excel_btns = driver.find_elements(By.XPATH, xpath_excel)
                if excel_btns:
                    target_btn = excel_btns[-1] 
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", target_btn)
                    time.sleep(1)
                    
                    before_files = set(glob.glob(os.path.join(download_dir, "*.*")))
                    driver.execute_script("arguments[0].click();", target_btn)
                    
                    # 다운로드 확인 (최대 15초)
                    new_file_path = None
                    for _ in range(15):
                        time.sleep(1)
                        diff = set(glob.glob(os.path.join(download_dir, "*.*"))) - before_files
                        excels = [f for f in diff if f.endswith(('.xlsx', '.xls', '.csv')) and '.crdownload' not in f]
                        if excels:
                            new_file_path = list(excels)[0]
                            break
                    
                    if new_file_path:
                        ext = os.path.splitext(new_file_path)[1]
                        if brand == "TIME": final_name = f"구성종목(PDF){brand}{etf_name}_{date_time}{ext}"
                        else: final_name = f"{brand} {etf_name}_{date_koact}{ext}"
                            
                        final_path = os.path.join(target_dir, final_name)
                        if os.path.exists(final_path): os.remove(final_path)
                        shutil.move(new_file_path, final_path)
                        print(f"✅ {etf_name} 수집 성공!")
                    else: print(f"⚠️ {etf_name} 다운로드 지연")
                else: print(f"❌ {etf_name} 버튼을 찾을 수 없음")
            except Exception as e:
                print(f"❌ {etf_name} 에러 발생")
            time.sleep(2)

finally:
    driver.quit()

print("\n✨ 16개 ETF 수집 공정 완료!")
