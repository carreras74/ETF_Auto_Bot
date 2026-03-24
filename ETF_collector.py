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

# --- 안정적인 16개 종목 리스트 ---
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
chrome_options.add_argument('--headless=new')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--window-size=1920x1080')
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "profile.default_content_setting_values.automatic_downloads": 1 
})

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

try:
    print(f"🚀 [TIME/KoAct 전용] 안정 수집 시작!", flush=True)
    for task in task_list:
        brand = task["brand"]
        for etf_name, room_url in task["etfs"].items():
            try:
                driver.get(room_url)
                time.sleep(4)
                before_files = set(glob.glob(os.path.join(download_dir, "*.*")))
                
                # 💡 검증된 스크롤 및 클릭 로직
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                time.sleep(2)
                xpath = "//a[contains(., '엑셀')] | //button[contains(., '엑셀')] | //a[contains(@class, 'excel')]"
                btns = driver.find_elements(By.XPATH, xpath)
                
                if btns:
                    driver.execute_script("arguments[0].click();", btns[-1])
                    time.sleep(5)
                    after_files = set(glob.glob(os.path.join(download_dir, "*.*")))
                    new_files = list(after_files - before_files)
                    if new_files:
                        old_path = new_files[0]
                        ext = os.path.splitext(old_path)[1]
                        final_name = f"구성종목(PDF){brand}{etf_name}_{date_time}{ext}" if brand == "TIME" else f"{brand} {etf_name}_{date_koact}{ext}"
                        final_path = os.path.join(target_dir, final_name)
                        if os.path.exists(final_path): os.remove(final_path)
                        shutil.move(old_path, final_path)
                        print(f"  ✅ {brand} {etf_name} 수집 성공!", flush=True)
            except Exception as e:
                print(f"  ⚠️ {etf_name} 에러: {e}", flush=True)
finally:
    driver.quit()
    print("\n✨ 수집 공정 완료!", flush=True)
