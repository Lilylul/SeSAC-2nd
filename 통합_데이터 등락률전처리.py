import pandas as pd

# CSV 파일 읽어오기 (데이터 타입 명시)
try:
    df = pd.read_csv("C:/Users/r2com/Downloads/통합_데이터_1년치_origin.csv",
                     encoding='utf-8',
                     dtype={'날짜': str, '시가': float, '고가': float, '저가': float, '종가': float, '거래량': float, '등락률': float, '종목코드': str, '종목명': str, '업종명': str})
except pd.errors.ParserError:
    print("데이터 파일을 읽는 중 오류가 발생했습니다. 데이터 형식을 확인해주세요.")
    df = pd.read_csv("C:/Users/r2com/Downloads/통합_데이터_1년치_origin.csv", encoding='utf-8', low_memory=False)

# 날짜 컬럼을 datetime 형식으로 변환 (날짜 형식 예시: '%Y-%m-%d')
df['날짜'] = pd.to_datetime(df['날짜'], format='%Y-%m-%d', errors='coerce')

# 등락률 소수점 4자리까지 반올림
df['등락률'] = df['등락률'].round(4)

print(df)
# 처리된 데이터를 CSV 파일로 저장 (인덱스 제외)
df.to_csv('processed_data.csv', index=False)