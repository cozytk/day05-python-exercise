"""
Day 05 실습: Python-SQL 연동 및 데이터 파이프라인

이 파일의 TODO 부분을 완성하세요.
"""
import sqlite3
import pandas as pd
import json
from typing import List, Dict, Any, Tuple, Optional
from contextlib import contextmanager
from datetime import datetime


# ============================================================
# Part 1: DB 연결 및 기본 CRUD
# ============================================================

@contextmanager
def get_connection(db_path: str = ':memory:'):
    """데이터베이스 연결 컨텍스트 매니저

    Args:
        db_path: 데이터베이스 경로

    Yields:
        sqlite3.Connection: DB 연결 객체
    """
    # TODO: 연결 생성, yield, 예외 시 rollback, 정상 시 commit, finally에서 close
    # 힌트: conn = sqlite3.connect(db_path)
    # 힌트: conn.row_factory = sqlite3.Row 설정 권장
    pass


def create_tables(conn: sqlite3.Connection) -> None:
    """테이블 생성

    테이블 구조:
    - users: id, name, email, created_at
    - orders: id, user_id, product, amount, order_date
    """
    # TODO: CREATE TABLE IF NOT EXISTS 사용
    pass


def insert_user(conn: sqlite3.Connection, name: str, email: str) -> int:
    """사용자 추가

    Returns:
        int: 삽입된 사용자 ID
    """
    # TODO: INSERT 후 lastrowid 반환
    # 힌트: 파라미터 바인딩 사용 (?, ?)
    pass


def insert_order(conn: sqlite3.Connection, user_id: int, product: str, amount: int) -> int:
    """주문 추가

    Returns:
        int: 삽입된 주문 ID
    """
    # TODO: INSERT 후 lastrowid 반환
    pass


def get_user_orders(conn: sqlite3.Connection, user_id: int) -> List[Dict]:
    """특정 사용자의 주문 조회

    Returns:
        List[Dict]: 주문 정보 딕셔너리 리스트
    """
    # TODO: JOIN을 사용하여 사용자와 주문 정보 조회
    # 힌트: users JOIN orders ON users.id = orders.user_id
    pass


def get_order_summary(conn: sqlite3.Connection) -> List[Dict]:
    """사용자별 주문 요약

    Returns:
        List[Dict]: 사용자별 주문 수, 총 금액
    """
    # TODO: GROUP BY를 사용하여 사용자별 집계
    # 컬럼: user_id, user_name, order_count, total_amount
    pass


# ============================================================
# Part 2: Pandas와 SQL 통합
# ============================================================

def query_to_dataframe(conn: sqlite3.Connection, query: str) -> pd.DataFrame:
    """SQL 쿼리 결과를 DataFrame으로 변환

    Args:
        conn: DB 연결
        query: SQL 쿼리

    Returns:
        pd.DataFrame: 쿼리 결과
    """
    # TODO: pd.read_sql_query 사용
    pass


def dataframe_to_table(conn: sqlite3.Connection, df: pd.DataFrame,
                       table_name: str, if_exists: str = 'replace') -> None:
    """DataFrame을 DB 테이블로 저장

    Args:
        conn: DB 연결
        df: 저장할 DataFrame
        table_name: 테이블 이름
        if_exists: 'fail', 'replace', 'append'
    """
    # TODO: df.to_sql 사용
    # 힌트: index=False 권장
    pass


def analyze_orders(conn: sqlite3.Connection) -> pd.DataFrame:
    """주문 분석: 월별 매출 집계

    Returns:
        pd.DataFrame: 월별 주문 수, 총 매출
    """
    # TODO: SQL로 기본 데이터 추출 후 Pandas로 월별 집계
    # 힌트: strftime('%Y-%m', order_date) 사용 (SQL)
    # 또는 pd.to_datetime 후 dt.to_period('M') 사용 (Pandas)
    pass


# ============================================================
# Part 3: ETL 파이프라인
# ============================================================

def extract_from_json(file_path: str) -> List[Dict]:
    """JSON 파일에서 데이터 추출

    Args:
        file_path: JSON 파일 경로

    Returns:
        List[Dict]: 추출된 데이터
    """
    # TODO: json.load 사용
    pass


def transform_data(raw_data: List[Dict]) -> pd.DataFrame:
    """데이터 변환

    Args:
        raw_data: 원본 데이터

    Returns:
        pd.DataFrame: 변환된 데이터
    """
    # TODO: DataFrame 변환 및 정제
    # - 날짜 컬럼 변환 (있다면)
    # - 필요시 컬럼명 변경
    # - NULL 처리
    pass


def validate_data(df: pd.DataFrame, required_columns: List[str]) -> Tuple[bool, List[str]]:
    """데이터 품질 검증

    Args:
        df: 검증할 DataFrame
        required_columns: 필수 컬럼 리스트

    Returns:
        Tuple[bool, List[str]]: (검증 통과 여부, 오류 메시지 리스트)
    """
    # TODO: 다음 검증 수행
    # 1. 필수 컬럼 존재 여부
    # 2. NULL 값 검사
    # 3. 중복 검사 (첫 번째 컬럼 기준)
    pass


def load_to_database(conn: sqlite3.Connection, df: pd.DataFrame,
                     table_name: str) -> int:
    """데이터를 DB에 적재

    Args:
        conn: DB 연결
        df: 적재할 DataFrame
        table_name: 테이블명

    Returns:
        int: 적재된 행 수
    """
    # TODO: df.to_sql로 저장 후 행 수 반환
    pass


class ETLPipeline:
    """간단한 ETL 파이프라인"""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def run(self, source_file: str, table_name: str,
            required_columns: List[str]) -> Dict[str, Any]:
        """파이프라인 실행

        Args:
            source_file: 소스 파일 경로
            table_name: 저장할 테이블명
            required_columns: 필수 컬럼

        Returns:
            Dict: 실행 결과 (success, rows_loaded, errors)
        """
        # TODO: Extract → Transform → Validate → Load 구현
        # 힌트: 각 단계 함수 호출
        # 검증 실패 시 {'success': False, 'errors': [...]} 반환
        pass


# ============================================================
# Part 4: 배치 처리
# ============================================================

def batch_insert(conn: sqlite3.Connection, data: List[Tuple],
                 table_name: str, columns: List[str],
                 batch_size: int = 100) -> int:
    """배치 삽입

    Args:
        conn: DB 연결
        data: 삽입할 데이터 리스트
        table_name: 테이블명
        columns: 컬럼 리스트
        batch_size: 배치 크기

    Returns:
        int: 삽입된 행 수
    """
    # TODO: executemany를 사용하여 배치 단위로 삽입
    # 힌트: 배치마다 commit
    pass


# ============================================================
# 테스트용 헬퍼
# ============================================================

def setup_test_data(conn: sqlite3.Connection) -> None:
    """테스트 데이터 설정"""
    create_tables(conn)

    # 사용자 추가
    users = [
        ('김철수', 'kim@test.com'),
        ('이영희', 'lee@test.com'),
        ('박민수', 'park@test.com'),
    ]
    for name, email in users:
        insert_user(conn, name, email)

    # 주문 추가
    orders = [
        (1, '노트북', 1500000, '2024-01-15'),
        (1, '마우스', 30000, '2024-01-16'),
        (2, '키보드', 80000, '2024-01-17'),
        (2, '모니터', 350000, '2024-01-18'),
        (3, '헤드셋', 120000, '2024-01-19'),
        (1, '모니터', 350000, '2024-02-01'),
        (2, '노트북', 1500000, '2024-02-05'),
    ]
    cursor = conn.cursor()
    for user_id, product, amount, date in orders:
        cursor.execute(
            "INSERT INTO orders (user_id, product, amount, order_date) VALUES (?, ?, ?, ?)",
            (user_id, product, amount, date)
        )
    conn.commit()


if __name__ == '__main__':
    # 테스트
    with get_connection() as conn:
        if conn:
            setup_test_data(conn)

            print("=== 사용자별 주문 ===")
            orders = get_user_orders(conn, 1)
            if orders:
                for o in orders:
                    print(o)

            print("\n=== 주문 요약 ===")
            summary = get_order_summary(conn)
            if summary:
                for s in summary:
                    print(s)
