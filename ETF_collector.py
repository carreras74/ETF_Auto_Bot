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

print(f"📍 작업 위치: {target_dir}", flush=True)

# --- 종목 리스트 (기존과 동일) ---
tiger_rooms = {
    "코리아테크액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7471780007",
    "AI코리아그로스액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7365040005",
    "퓨처모빌리티액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7387280001",
    "기술이전바이오액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR70168K0008"
}
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
chrome_options.add_argument('--headless=new')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--window-size=1920x1080')
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
chrome_options.add_argument("--lang=ko_KR")

chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "profile.default_content_setting_values.automatic_downloads": 1 
})

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

try:
    print("🚀 [전 종목 완전 수집] 불도저 모드 가동!", flush=True)

    # 1. TIGER 특수 타격
    print("\n🏢 [TIGER] 집중 수집 공정...", flush=True)
    for etf_name, room_url in tiger_rooms.items():
        try:
            driver.get(room_url)
            time.sleep(7) # 충분한 로딩 시간
            before_files = set(glob.glob(os.path.join(download_dir, "*.*")))
            
            # 💡 [투망 로직] 화면 내 모든 버튼/링크 중 '엑셀' 단어가 포함된 것을 무조건 클릭
            found = driver.execute_script("""
                var targets = Array.from(document.querySelectorAll('a, button, span'));
                var excelBtn = targets.find(el => el.innerText.includes('엑셀다운로드') && el.offsetParent !== null);
                if(!excelBtn) excelBtn = targets.find(el => el.innerText.includes('엑셀다운로드')); // 안보여도 일단 클릭
                
                if(excelBtn) {
                    excelBtn.scrollIntoView({block: 'center'});
                    excelBtn.click();
                    return true;
                }
                return false;
            """)
            
            if found:
                time.sleep(8) # 다운로드 대기
                after_files = set(glob.glob(os.path.join(download_dir, "*.*")))
                new_files = list(after_files - before_files)
                if new_files:
                    old_path = new_files[0]
                    final_path = os.path.join(target_dir, f"TIGER {etf_name}_{date_koact}{os.path.splitext(old_path)[1]}")
                    if os.path.exists(final_path): os.remove(final_path)
                    shutil.move(old_path, final_path)
                    print(f"  ✅ {etf_name} 성공!", flush=True)
                else: print(f"  ⚠️ {etf_name} 다운로드 지연", flush=True)
            else: print(f"  ❌ {etf_name} 버튼 실종", flush=True)
        except Exception as e: print(f"  ⚠️ {etf_name} 에러: {e}", flush=True)

    # 2. TIME & KoAct (검증된 로직)
    for brand, rooms in [("TIME", time_rooms), ("KoAct", koact_rooms)]:
        print(f"\n🏢 [{brand}] 안정 수집 공정...", flush=True)
        for etf_name, room_url in rooms.items():
            try:
                driver.get(room_url)
                time.sleep(4)
                before_files = set(glob.glob(os.path.join(download_dir, "*.*")))
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                time.sleep(1)
                
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
                        print(f"  ✅ {etf_name} 성공!", flush=True)
            except: continue

finally:
    driver.quit()
    print("\n✨ 전 공정 종료!", flush=True)
