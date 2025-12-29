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
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()


def create_tables(conn: sqlite3.Connection) -> None:
    """테이블 생성

    테이블 구조:
    - users: id, name, email, created_at
    - orders: id, user_id, product, amount, order_date
    """
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            product TEXT NOT NULL,
            amount INTEGER NOT NULL,
            order_date TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.commit()


def insert_user(conn: sqlite3.Connection, name: str, email: str) -> int:
    """사용자 추가

    Returns:
        int: 삽입된 사용자 ID
    """
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO users (name, email) VALUES (?, ?)",
        (name, email)
    )
    conn.commit()
    return cursor.lastrowid


def insert_order(conn: sqlite3.Connection, user_id: int, product: str, amount: int) -> int:
    """주문 추가

    Returns:
        int: 삽입된 주문 ID
    """
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO orders (user_id, product, amount) VALUES (?, ?, ?)",
        (user_id, product, amount)
    )
    conn.commit()
    return cursor.lastrowid


def get_user_orders(conn: sqlite3.Connection, user_id: int) -> List[Dict]:
    """특정 사용자의 주문 조회

    Returns:
        List[Dict]: 주문 정보 딕셔너리 리스트
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT u.name, o.product, o.amount, o.order_date
        FROM orders o
        JOIN users u ON o.user_id = u.id
        WHERE u.id = ?
        ORDER BY o.order_date
    """, (user_id,))
    return [dict(row) for row in cursor.fetchall()]


def get_order_summary(conn: sqlite3.Connection) -> List[Dict]:
    """사용자별 주문 요약

    Returns:
        List[Dict]: 사용자별 주문 수, 총 금액
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            u.id AS user_id,
            u.name AS user_name,
            COUNT(o.id) AS order_count,
            SUM(o.amount) AS total_amount
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        GROUP BY u.id, u.name
        ORDER BY total_amount DESC
    """)
    return [dict(row) for row in cursor.fetchall()]


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
    return pd.read_sql_query(query, conn)


def dataframe_to_table(conn: sqlite3.Connection, df: pd.DataFrame,
                       table_name: str, if_exists: str = 'replace') -> None:
    """DataFrame을 DB 테이블로 저장

    Args:
        conn: DB 연결
        df: 저장할 DataFrame
        table_name: 테이블 이름
        if_exists: 'fail', 'replace', 'append'
    """
    df.to_sql(table_name, conn, if_exists=if_exists, index=False)


def analyze_orders(conn: sqlite3.Connection) -> pd.DataFrame:
    """주문 분석: 월별 매출 집계

    Returns:
        pd.DataFrame: 월별 주문 수, 총 매출
    """
    query = """
        SELECT
            strftime('%Y-%m', order_date) AS month,
            COUNT(*) AS order_count,
            SUM(amount) AS total_revenue
        FROM orders
        GROUP BY strftime('%Y-%m', order_date)
        ORDER BY month
    """
    return pd.read_sql_query(query, conn)


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
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def transform_data(raw_data: List[Dict]) -> pd.DataFrame:
    """데이터 변환

    Args:
        raw_data: 원본 데이터

    Returns:
        pd.DataFrame: 변환된 데이터
    """
    df = pd.DataFrame(raw_data)
    # NULL 처리
    df = df.fillna('')
    return df


def validate_data(df: pd.DataFrame, required_columns: List[str]) -> Tuple[bool, List[str]]:
    """데이터 품질 검증

    Args:
        df: 검증할 DataFrame
        required_columns: 필수 컬럼 리스트

    Returns:
        Tuple[bool, List[str]]: (검증 통과 여부, 오류 메시지 리스트)
    """
    errors = []

    # 필수 컬럼 검사
    missing = set(required_columns) - set(df.columns)
    if missing:
        errors.append(f"누락된 컬럼: {missing}")

    # NULL 검사
    if df.isnull().any().any():
        null_cols = df.columns[df.isnull().any()].tolist()
        errors.append(f"NULL 포함 컬럼: {null_cols}")

    # 중복 검사
    if len(df.columns) > 0:
        first_col = df.columns[0]
        if df[first_col].duplicated().any():
            errors.append(f"중복 값 발견: {first_col}")

    return len(errors) == 0, errors


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
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    return len(df)


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
        try:
            # Extract
            raw_data = extract_from_json(source_file)

            # Transform
            df = transform_data(raw_data)

            # Validate
            valid, errors = validate_data(df, required_columns)
            if not valid:
                return {'success': False, 'errors': errors, 'rows_loaded': 0}

            # Load
            with get_connection(self.db_path) as conn:
                rows = load_to_database(conn, df, table_name)

            return {'success': True, 'errors': [], 'rows_loaded': rows}

        except Exception as e:
            return {'success': False, 'errors': [str(e)], 'rows_loaded': 0}


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
    cursor = conn.cursor()
    cols = ', '.join(columns)
    placeholders = ', '.join(['?' for _ in columns])
    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

    total = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        cursor.executemany(sql, batch)
        conn.commit()
        total += len(batch)

    return total


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
