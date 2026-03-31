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

# 내 컴퓨터에서 실행한 폴더에 엑셀 파일이 다운로드 됩니다.
target_dir = os.path.dirname(os.path.abspath(__file__))
download_dir = target_dir

date_koact = datetime.now().strftime("%Y%m%d")  

print(f"📍 내 컴퓨터 작업 위치: {target_dir}")
print(f"🚀 [로컬 테스트] TIGER 4종목 엑셀 다운로드 훈련 시작!\n")

tiger_rooms = {
    "코리아테크액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7471780007",
    "AI코리아그로스액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7365040005",
    "퓨처모빌리티액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR7387280001",
    "기술이전바이오액티브": "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/index.do?ksdFund=KR70168K0008"
}

# 💡 내 컴퓨터에서는 크롬 창이 직접 뜨도록 '--headless' 등의 옵션을 제외했습니다.
chrome_options = Options()
chrome_options.add_argument('--window-size=1920x1080')
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
        
        before_files = set(glob.glob(os.path.join(download_dir, "*.*")))
        found_and_clicked = False
        
        # 1. 스크롤을 천천히 내리면서 표를 로딩시킵니다. (눈으로 확인해 보세요!)
        for step in range(1, 11):
            driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * ({step}/10));")
            time.sleep(1)
        
        time.sleep(2)
        
        # 2. '엑셀다운로드' 버튼을 찾아 클릭합니다.
        for _ in range(15): 
            clicked = driver.execute_script("""
                // 대표님이 말씀하신 '자산구성' 구역의 엑셀 버튼을 정확히 노립니다.
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
                
                // 혹시 못 찾으면 화면 전체에서 3번째 혹은 마지막 엑셀 버튼 타격!
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
            for _ in range(10):
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
    print("✨ 내 컴퓨터 테스트 완료! 바탕화면에 엑셀이 받아졌는지 확인해 보세요!")

