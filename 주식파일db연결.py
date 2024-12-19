import pymysql
import pandas as pd

# 데이터베이스 연결 정보
host = "localhost"  # MySQL 서버 주소
user = "root"  # MySQL 사용자 이름
password = "0122"  # MySQL 사용자 비밀번호
database = "samsung"  # 데이터베이스 이름

# CSV 파일 경로
file_path = "C:/Users/r2com/Documents/카카오톡 받은 파일/Downloads/005930_trading_data.csv"

try:
    # 데이터베이스 연결
    conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    print("MySQL 데이터베이스에 연결되었습니다.")

    # CSV 파일 읽기
    df = pd.read_csv(file_path, encoding='utf-8')
    
    # 날짜 컬럼 형식 변환
    df['날짜'] = pd.to_datetime(df['날짜'])
    
    # 데이터 삽입
    with conn.cursor() as cursor:  # 커서를 한 번만 생성
        for index, row in df.iterrows():
            sql = """
            INSERT INTO samsung_chart 
            (col_1, col_2, col_3, col_4, col_5, col_6, col_7, col_8, 
            col_9, col_10, col_11, col_12, col_13, col_14)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            val = (
                row['날짜'], row['종목코드'], row['금융투자'], row['보험'], row['투신'], 
                row['사모'], row['은행'], row['기타금융'], row['연기금 등'], 
                row['기관합계'], row['기타법인'], row['개인'], row['외국인'], 
                row['기타외국인']
            )
            try:
                cursor.execute(sql, val)
            except pymysql.Error as e:
                print(f"Error inserting data for row {index}: {e}")

    # 데이터베이스에 변경 사항 반영
    conn.commit()
    print("데이터 삽입 완료 및 커밋되었습니다.")

except pymysql.Error as e:
    print(f"Error: {e}")

finally:
    # 연결 종료
    if conn.open:
        conn.close()
        print("MySQL 데이터베이스 연결이 종료되었습니다.")
    