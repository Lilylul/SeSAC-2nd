import mysql.connector
import csv
import decimal
import math
import re
# MySQL 연결 정보 
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="0122",
    database="money"
)

mycursor = mydb.cursor()
# 테이블 생성 (만약 존재하지 않으면)

mycursor.execute("""
    CREATE TABLE IF NOT EXISTS money_chart (
        date DATE,
        Open_ INT,
        High_ INT,
        Low_ INT,
        Close_ INT,
        Volume_ INT,
        Change_ DECIMAL(5, 4), 
        Ticker_ VARCHAR(10),
        Name_ VARCHAR(50),
        Industry_ VARCHAR(50)
    )
""")
# CSV 파일 읽기 (인코딩 지정)
with open("C:/Users/r2com/Downloads/통합_데이터_1년치_origin.csv", 'r', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # 헤더 건너뛰기

    for row_index, row in enumerate(reader, 1):  # 행 번호를 사용하여 오류 발생 시 정확한 위치 파악
        try:
            # 데이터 전처리 (정규 표현식 활용, 소수점 자릿수 조정)
            change_str = re.sub(r"[^\d.-]", "", row[6])  # 숫자, 마침표, 마이너스만 남기고 제거
            change_value = decimal.Decimal(change_str)
            rounded_value = change_value.quantize(decimal.Decimal('0.0001'))  # 소수점 아래 4자리까지 반올림

            # 값의 유효성 검사 (필요한 경우)
            if rounded_value > 1 or rounded_value < -1:
                print(f"행 {row_index}: 값이 비정상적입니다: {row[6]}")
                continue

            # 데이터베이스에 삽입
            sql = "INSERT INTO money_chart (date, Open_, High_, Low_, Close_, Volume_, Change_, Ticker_, Name_, Industry_) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (row[0], int(row[1]), int(row[2]), int(row[3]), int(row[4]), int(row[5]), rounded_value, int(row[7]), row[8], row[9])
            mycursor.execute(sql, val)

        except decimal.InvalidOperation:
            print(f"행 {row_index}: 데이터 변환 오류: {row[6]}, 전체 행: {row}")
        except ValueError as e:
            print(f"행 {row_index}: 데이터 변환 오류: {e}, row: {row}")

mydb.commit()