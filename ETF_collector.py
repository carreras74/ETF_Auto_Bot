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

target_dir = os.getcwd()
download_dir = target_dir
date_time = datetime.now().strftime("%Y-%m-%d") 
date_koact = datetime.now().strftime("%Y%m%d")  

print(f"📍 작업 위치: {target_dir}")
print(f"📅 수집 기준 날짜: {date_time}")

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

chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--window-size=1920x1080')
chrome_options.add_experimental_option("prefs", {"download.default_directory": download_dir})

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

try:
    for brand, rooms in [("TIME", time_rooms), ("KoAct", koact_rooms)]:
        print(f"\n🏢 [{brand}] 수집 시작...")
        for etf_name, url in rooms.items():
            try:
                driver.get(url)
                time.sleep(6)
                xpath_excel = "//a[contains(text(), '엑셀')] | //button[contains(text(), '엑셀')] | //a[contains(@href, 'excel')] | //img[contains(@alt, '엑셀')]/parent::a"
                btns = driver.find_elements(By.XPATH, xpath_excel)
                if btns:
                    btn = btns[-1]
                    before = set(glob.glob(os.path.join(download_dir, "*.*")))
                    driver.execute_script("arguments[0].click();", btn)
                    for _ in range(15):
                        time.sleep(1)
                        diff = set(glob.glob(os.path.join(download_dir, "*.*"))) - before
                        excels = [f for f in diff if f.endswith(('.xlsx', '.xls', '.csv'))]
                        if excels:
                            new_file = list(excels)[0]
                            ext = os.path.splitext(new_file)[1]
                            f_name = f"구성종목(PDF){brand}{etf_name}_{date_time}{ext}" if brand=="TIME" else f"{brand} {etf_name}_{date_koact}{ext}"
                            f_path = os.path.join(target_dir, f_name)
                            if os.path.exists(f_path): os.remove(f_path)
                            shutil.move(new_file, f_path)
                            print(f"✅ {etf_name} 성공!")
                            break
                else: print(f"❌ {etf_name} 버튼 없음")
            except: print(f"❌ {etf_name} 에러")
finally:
    driver.quit()
