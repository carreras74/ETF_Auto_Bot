import os
import time
import glob
import shutil
from datetime import datetime, timedelta, timezone
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

target_dir = os.path.dirname(os.path.abspath(__file__))
download_dir = target_dir

KST = timezone(timedelta(hours=9))
date_koact = datetime.now(KST).strftime("%Y%m%d")  

print(f"📍 서버 작업 위치: {target_dir}")
print(f"🚀 [TIGER 3종목 집중 수집] 스텔스 우회 모드 가동!\n")

tiger_rooms = {
    "코리아테크액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7471780007",
    "퓨처모빌리티액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7387280001",
    "기술이전바이오액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR70168K0008"
}

chrome_options = Options()
chrome_options.add_argument('--headless=new')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--window-size=1920,1080')
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

# 💡 [핵심 무기 1] 로봇이 아닌 '사람'인 척 완벽하게 위장하는 스텔스 옵션 
chrome_options.add_argument("--disable-blink-features=AutomationControlled") 
chrome_options.add_experimental_option('excludeSwitches', ['enable-automation', 'enable-logging'])
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True, 
    "profile.default_content_setting_values.automatic_downloads": 1 
})

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# 💡 [핵심 무기 2] 크롬 브라우저의 '나는 봇이다'라는 꼬리표를 강제로 떼어버립니다.
driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
    "source": """ Object.defineProperty(navigator, 'webdriver', { get: () => undefined }) """
})
driver.set_page_load_timeout(30)

try:
    for etf_name, room_url in tiger_rooms.items():
        print(f"🏢 [TIGER] {etf_name} 사이트 진입 중...")
        
        found_and_clicked = False
        
        # 💡 [핵심 무기 3] 봇 차단으로 화면이 안 뜨면, 새로고침(F5) 후 한 번 더 뚫어버립니다!
        for attempt in range(2):
            driver.get(room_url)
            time.sleep(8) # 스텔스 잠입 대기
            
            # 화면을 가리는 가짜 팝업창 찢기
            try:
                driver.execute_script("""
                    document.querySelectorAll('[class*="popup"], [class*="layer"], [class*="modal"], [id*="popup"]').forEach(e => e.remove());
                """)
            except: pass
            
            # 스크롤 내리기
            for step in range(1, 11):
                driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * ({step}/10));")
                time.sleep(1)
            
            time.sleep(3)
            before_files = set(glob.glob(os.path.join(download_dir, "*.*")))
            
            # 선생님의 필살기: 바닥에서 첫 번째 버튼 타격!
            for _ in range(15): 
                clicked = driver.execute_script("""
                    var allElements = document.querySelectorAll('a, button, span, img');
                    var excelBtns = [];
                    for(var i = 0; i < allElements.length; i++) {
                        var el = allElements[i];
                        var txt = (el.innerText || el.textContent || el.alt || "").replace(/\\s+/g, '').toUpperCase();
                        var href = (el.href || "").toUpperCase();
                        var className = (el.className || "").toUpperCase();

                        if (txt.includes('엑셀') || txt.includes('EXCEL') || href.includes('EXCEL') || className.includes('EXCEL')) {
                            if (el.tagName === 'A' || el.tagName === 'BUTTON') {
                                excelBtns.push(el);
                            } else if (el.parentElement && (el.parentElement.tagName === 'A' || el.parentElement.tagName === 'BUTTON')) {
                                excelBtns.push(el.parentElement);
                            }
                        }
                    }
                    if (excelBtns.length > 0) {
                        var targetBtn = excelBtns[excelBtns.length - 1];
                        targetBtn.scrollIntoView({block: 'center', behavior: 'smooth'});
                        targetBtn.click();
                        return true;
                    }
                    return false;
                """)
                
                if clicked:
                    found_and_clicked = True
                    break
                time.sleep(1)
                
            if found_and_clicked:
                print(f"📥 [{etf_name}] 화면 바닥의 엑셀 버튼 클릭 완료!", flush=True)
                break # 성공했으니 재시도 루프 탈출!
            else:
                if attempt == 0:
                    print(f"   ⚠️ 서버 지연/차단 감지! 새로고침(F5) 후 재돌파합니다...")

        if found_and_clicked:
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
                final_name = f"TIGER {etf_name}_{date_koact}{ext}"
                final_path = os.path.join(target_dir, final_name)
                
                if new_file_path != final_path:
                    if os.path.exists(final_path): os.remove(final_path)
                    shutil.move(new_file_path, final_path)
                    
                print(f"✅ [{etf_name}] 수집 성공! 파일명: {final_name}\n")
            else: 
                print(f"⚠️ [{etf_name}] 다운로드 지연.\n")
        else: 
            print(f"❌ [{etf_name}] 화면에서 엑셀 버튼을 찾을 수 없습니다.\n")

finally:
    time.sleep(3)
    driver.quit()
    print("✨ 3종목 스텔스 수집 및 서버 테스트 완료!")

