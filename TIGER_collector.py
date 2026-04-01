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
print(f"🚀 [깃허브 서버 적용 완료] TIGER 4종목 엑셀 다운로드 시작!\n")

tiger_rooms = {
    "코리아테크액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7471780007",
    "AI코리아그로스액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7365040005",
    "퓨처모빌리티액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7387280001",
    "기술이전바이오액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR70168K0008"
}

chrome_options = Options()
# 💡 [핵심 패치 1] 서버 전용 헤드리스 옵션 추가 (이게 없으면 튕깁니다!)
chrome_options.add_argument('--headless=new')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu')

# 💡 [핵심 패치 2] 창 크기는 반드시 쉼표(,)를 써야 서버가 모바일로 오해하지 않습니다!
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
        
        # 💡 [핵심 패치 3] 화면이 다 뜨기도 전에 엑셀을 찾는 걸 방지하기 위해 5초 대기
        time.sleep(5)
        
        # 💡 [핵심 패치 4] 혹시 화면을 덮은 팝업창이 있다면 다 찢어버립니다.
        try:
            driver.execute_script("""
                document.querySelectorAll('[class*="popup"], [class*="layer"], [class*="modal"], [id*="popup"]').forEach(e => e.remove());
            """)
        except: pass
        
        before_files = set(glob.glob(os.path.join(download_dir, "*.*")))
        found_and_clicked = False
        
        for step in range(1, 11):
            driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * ({step}/10));")
            time.sleep(1)
        
        time.sleep(3) # 데이터가 불러와질 여유 시간 추가
        
        for _ in range(15): 
            clicked = driver.execute_script("""
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
                        excelBtn.scrollIntoView({block: 'center', behavior: 'smooth'});
                        excelBtn.click();
                        return true;
                    }
                }
                
                var fallbackBtns = Array.from(document.querySelectorAll('a, button, span')).filter(function(el) {
                    var txt = el.innerText || el.textContent || "";
                    return txt.replace(/\\s+/g, '').includes('엑셀다운로드');
                });
                
                if (fallbackBtns.length >= 3) {
                    fallbackBtns[2].scrollIntoView({block: 'center'});
                    fallbackBtns[2].click();
                    return true;
                } else if (fallbackBtns.length > 0) {
                    fallbackBtns[fallbackBtns.length - 1].scrollIntoView({block: 'center'});
                    fallbackBtns[fallbackBtns.length - 1].click();
                    return true;
                }
                
                return false;
            """)
            if clicked:
                found_and_clicked = True
                print(f"📥 [{etf_name}] 자산구성 엑셀 클릭 완료!", flush=True)
                break
            time.sleep(1)
            
        if found_and_clicked:
            new_file_path = None
            for _ in range(15): # 다운로드 대기 시간도 조금 늘렸습니다
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
            print(f"❌ [{etf_name}] 엑셀 버튼을 찾을 수 없습니다.\n")

finally:
    time.sleep(3)
    driver.quit()
    print("✨ 깃허브 서버 다운로드 완료!")

