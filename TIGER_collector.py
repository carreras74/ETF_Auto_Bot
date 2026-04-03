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

# 작업 위치 세팅
target_dir = os.path.dirname(os.path.abspath(__file__))
download_dir = target_dir

# 💡 한국 시간(KST) 기준으로 날짜 생성
KST = timezone(timedelta(hours=9))
date_koact = datetime.now(KST).strftime("%Y%m%d")  

print(f"📍 서버 작업 위치: {target_dir}")
print(f"🚀 [TIGER 3종목 집중 수집] 엑셀 다운로드 시작!\n")

# 💡 'AI코리아그로스액티브'가 제외된 최종 리스트입니다.
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
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True, 
    "profile.default_content_setting_values.automatic_downloads": 1 
})

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
driver.set_page_load_timeout(30)

try:
    for etf_name, room_url in tiger_rooms.items():
        print(f"🏢 [TIGER] {etf_name} 사이트 진입 중...")
        driver.get(room_url)
        
        time.sleep(8)
        
        before_files = set(glob.glob(os.path.join(download_dir, "*.*")))
        found_and_clicked = False
        
        # 1. 스크롤을 바닥까지 천천히 내립니다.
        for step in range(1, 11):
            driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * ({step}/10));")
            time.sleep(1)
        
        time.sleep(3)
        
        # 2. 선생님의 통찰력이 담긴 "바닥에서 첫 번째 엑셀 버튼 찾기" 로직
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
                print(f"📥 [{etf_name}] 화면 바닥의 엑셀 버튼 클릭 완료!", flush=True)
                break
            time.sleep(1)
            
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
    print("✨ 3종목 수집 및 서버 테스트 완료!")

