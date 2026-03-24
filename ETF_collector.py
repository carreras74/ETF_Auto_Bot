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
print(f"📅 TIME 날짜: {date_time} / KoAct, TIGER 날짜: {date_koact}\n", flush=True)

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

tiger_rooms = {
    "코리아테크액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7471780007",
    "AI코리아그로스액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7365040005",
    "퓨처모빌리티액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7387280001",
    "기술이전바이오액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR70168K0008"
}

# 테스트 속도를 위해 TIGER 선봉 유지!
task_list = [
    {"brand": "TIGER", "etfs": tiger_rooms},
    {"brand": "TIME", "etfs": time_rooms},
    {"brand": "KoAct", "etfs": koact_rooms}
]

chrome_options = Options()
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
# 💡 [핵심 패치 1] 가상 모니터 세로 길이를 5000픽셀로 대폭 늘려버립니다!
chrome_options.add_argument('--window-size=1920,5000') 
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

driver.set_page_load_timeout(45)

try:
    print("🚀 [수집기 가동] 시각화 모드 (총 20개 완전체) 시작!", flush=True)

    for task in task_list:
        brand = task["brand"]
        rooms = task["etfs"]
        print(f"\n=========================================", flush=True)
        print(f"🏢 [{brand}] 운용사 포트폴리오 추출 시작...", flush=True)
        
        for etf_name, room_url in rooms.items():
            try:
                try:
                    driver.get(room_url)
                except Exception as e:
                    print(f"⚠️ [{brand}] {etf_name} 로딩 지연! 강제 스크롤 시도...", flush=True)
                    driver.execute_script("window.stop();")
                
                before_files = set(glob.glob(os.path.join(download_dir, "*.*")))
                found_and_clicked = False
                
                if brand == "TIGER":
                    # 💡 [핵심 패치 2] 픽셀 단위(500px씩)로 무식하게 20번 찍어 누르며 스크롤을 강제합니다!
                    for _ in range(20):
                        driver.execute_script("window.scrollBy(0, 500);")
                        time.sleep(0.5)
                    time.sleep(3)
                    
                    # 💡 [핵심 패치 3] 아까 성공했던 '자산구성' 구역을 먼저 찾고, 없으면 3번째/마지막 버튼을 강제 클릭!
                    for _ in range(20): 
                        clicked = driver.execute_script("""
                            // 1순위: 아까 성공했던 '자산구성' 블록을 샅샅이 뒤집니다.
                            var allDivs = Array.from(document.querySelectorAll('div, section, article'));
                            var targetSection = null;
                            for (var i = 0; i < allDivs.length; i++) {
                                var txt = allDivs[i].innerText || allDivs[i].textContent || "";
                                if (txt.replace(/\\s+/g, '').includes('자산구성(구성종목')) {
                                    targetSection = allDivs[i];
                                    break;
                                }
                            }
                            
                            if (targetSection) {
                                var btns = Array.from(targetSection.querySelectorAll('a, button, span'));
                                var excelBtn = btns.find(function(b) {
                                    var bTxt = b.innerText || b.textContent || "";
                                    return bTxt.replace(/\\s+/g, '').includes('엑셀다운로드');
                                });
                                if (excelBtn) {
                                    excelBtn.click();
                                    return true;
                                }
                            }
                            
                            // 2순위: '자산구성' 구역을 못 찾으면 화면 전체의 엑셀 버튼 중 3번째나 마지막 놈을 쏩니다!
                            var fallbackBtns = Array.from(document.querySelectorAll('a, button, span')).filter(function(el) {
                                var txt = el.innerText || el.textContent || "";
                                return txt.replace(/\\s+/g, '').includes('엑셀다운로드');
                            });
                            
                            if (fallbackBtns.length >= 3) {
                                fallbackBtns[2].click();
                                return true;
                            } else if (fallbackBtns.length > 0) {
                                fallbackBtns[fallbackBtns.length - 1].click();
                                return true;
                            }
                            
                            return false;
                        """)
                        if clicked:
                            found_and_clicked = True
                            print(f"📥 [{brand}] {etf_name} 자산구성 엑셀 강제 클릭 완료!", end="\r", flush=True)
                            break
                        time.sleep(1)
                        
                else: 
                    time.sleep(3)
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                    time.sleep(2)
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(3)
                    
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
                        driver.execute_script("arguments[0].click();", target_button)
                        found_and_clicked = True
                        print(f"📥 [{brand}] {etf_name} 버튼 클릭 완료!", end="\r", flush=True)

                if found_and_clicked:
                    time.sleep(1)
                    try:
                        alert = driver.switch_to.alert
                        alert.accept() 
                        print(f"⚠️ [{brand}] {etf_name} 다운로드 스킵 (경고창 무시).", flush=True)
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
                        elif brand == "KoAct": final_name = f"{brand} {etf_name}_{date_koact}{ext}"
                        elif brand == "TIGER": final_name = f"{brand} {etf_name}_{date_koact}{ext}"
                            
                        final_path = os.path.join(target_dir, final_name)
                        
                        if new_file_path != final_path:
                            if os.path.exists(final_path): os.remove(final_path)
                            shutil.move(new_file_path, final_path)
                            
                        print(f"\n✅ [{brand}] {etf_name} 수집 성공!      ", flush=True)
                    else: print(f"\n⚠️ [{brand}] {etf_name} 다운로드 지연.", flush=True)
                else: print(f"\n❌ [{brand}] {etf_name} 엑셀 버튼을 찾을 수 없습니다.", flush=True)
            except Exception as e: print(f"\n❌ [{brand}] {etf_name} 에러 발생: {e}", flush=True)
            time.sleep(2)

finally:
    time.sleep(2)
    driver.quit()

print("\n🧹 찌꺼기 파일 청소 중...", flush=True)
for f in glob.glob(os.path.join(target_dir, "*.xlsx")) + glob.glob(os.path.join(target_dir, "*.xls")) + glob.glob(os.path.join(target_dir, "*.csv")):
    fname = os.path.basename(f)
    if "TIME" not in fname and "KoAct" not in fname and "TIGER" not in fname:
        try:
            os.remove(f)
            print(f"   🗑️ 쓰레기 파일 삭제 완료: {fname}", flush=True)
        except Exception:
            pass

print("\n✨ 총 20개 ETF 수집 및 청소 공정 완벽 종료!", flush=True)
