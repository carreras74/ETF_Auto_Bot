import pandas as pd
import os

# 1. 초간단 매입장부 (코드와 ETF 칸 삭제!)
data = {
    "종목명": ["삼성전자", "알테오젠", "에코프로비엠"],
    "매수일자": ["2026-03-10", "2026-03-15", "2026-03-18"],
    "매수단가": [74500, 180000, 250000],
    "매도일자": ["", "2026-03-20", ""] # 빈칸이면 현재 보유 중!
}

df = pd.DataFrame(data)

# 2. 파일 저장
current_folder = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
file_path = os.path.join(current_folder, "매입장부.xlsx")
df.to_excel(file_path, index=False)

print(f"🎉 성공! 초간단 매입장부가 생성되었습니다: {file_path}")
print("대표님의 실제 매입 내역(종목명, 날짜, 단가)만 쓱 적어주시면 나머지는 로봇이 알아서 합니다!")