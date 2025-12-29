"""
Day 05 실습 테스트
"""
import pytest
import sqlite3
import pandas as pd
import json
import os
import tempfile
from exercise import (
    get_connection,
    create_tables,
    insert_user,
    insert_order,
    get_user_orders,
    get_order_summary,
    query_to_dataframe,
    dataframe_to_table,
    analyze_orders,
    extract_from_json,
    transform_data,
    validate_data,
    load_to_database,
    ETLPipeline,
    batch_insert,
)


@pytest.fixture
def db_conn():
    """테스트용 DB 연결"""
    with get_connection(':memory:') as conn:
        create_tables(conn)
        yield conn


@pytest.fixture
def populated_db(db_conn):
    """데이터가 있는 DB"""
    # 사용자 추가
    insert_user(db_conn, '김철수', 'kim@test.com')
    insert_user(db_conn, '이영희', 'lee@test.com')

    # 주문 추가
    cursor = db_conn.cursor()
    orders = [
        (1, '노트북', 1500000, '2024-01-15'),
        (1, '마우스', 30000, '2024-01-16'),
        (2, '키보드', 80000, '2024-01-17'),
        (2, '모니터', 350000, '2024-02-01'),
    ]
    for user_id, product, amount, date in orders:
        cursor.execute(
            "INSERT INTO orders (user_id, product, amount, order_date) VALUES (?, ?, ?, ?)",
            (user_id, product, amount, date)
        )
    db_conn.commit()
    return db_conn


# ============================================================
# Part 1: DB 연결 및 기본 CRUD
# ============================================================

class TestGetConnection:
    def test_returns_connection(self):
        """연결 반환 확인"""
        with get_connection(':memory:') as conn:
            assert conn is not None
            assert isinstance(conn, sqlite3.Connection)

    def test_auto_commit_on_success(self):
        """성공 시 자동 커밋"""
        with get_connection(':memory:') as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE test (id INTEGER)")
            cursor.execute("INSERT INTO test VALUES (1)")

        # 새 연결로 확인 (메모리 DB라 불가, 개념적 테스트)


class TestCreateTables:
    def test_creates_users_table(self, db_conn):
        """users 테이블 생성"""
        cursor = db_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
        assert cursor.fetchone() is not None

    def test_creates_orders_table(self, db_conn):
        """orders 테이블 생성"""
        cursor = db_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'")
        assert cursor.fetchone() is not None


class TestInsertUser:
    def test_returns_id(self, db_conn):
        """ID 반환 확인"""
        user_id = insert_user(db_conn, '테스트', 'test@test.com')
        assert user_id is not None
        assert user_id > 0

    def test_data_inserted(self, db_conn):
        """데이터 삽입 확인"""
        insert_user(db_conn, '테스트', 'test@test.com')
        cursor = db_conn.cursor()
        cursor.execute("SELECT name FROM users WHERE email = 'test@test.com'")
        result = cursor.fetchone()
        assert result is not None
        assert result[0] == '테스트'


class TestInsertOrder:
    def test_returns_id(self, db_conn):
        """ID 반환 확인"""
        user_id = insert_user(db_conn, '테스트', 'test@test.com')
        order_id = insert_order(db_conn, user_id, '노트북', 1500000)
        assert order_id is not None
        assert order_id > 0


class TestGetUserOrders:
    def test_returns_orders(self, populated_db):
        """주문 조회"""
        orders = get_user_orders(populated_db, 1)
        assert orders is not None
        assert len(orders) == 2  # 김철수의 주문 2건


class TestGetOrderSummary:
    def test_returns_summary(self, populated_db):
        """요약 반환"""
        summary = get_order_summary(populated_db)
        assert summary is not None
        assert len(summary) == 2  # 2명의 사용자


# ============================================================
# Part 2: Pandas와 SQL 통합
# ============================================================

class TestQueryToDataframe:
    def test_returns_dataframe(self, populated_db):
        """DataFrame 반환"""
        df = query_to_dataframe(populated_db, "SELECT * FROM users")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2


class TestDataframeToTable:
    def test_saves_dataframe(self, db_conn):
        """DataFrame 저장"""
        df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
        dataframe_to_table(db_conn, df, 'test_table')

        cursor = db_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        assert cursor.fetchone()[0] == 2


class TestAnalyzeOrders:
    def test_returns_monthly_analysis(self, populated_db):
        """월별 분석"""
        df = analyze_orders(populated_db)
        assert df is not None
        # 1월, 2월 데이터


# ============================================================
# Part 3: ETL 파이프라인
# ============================================================

class TestExtractFromJson:
    def test_extracts_data(self):
        """JSON 추출"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([{'id': 1, 'name': 'test'}], f)
            f.flush()
            result = extract_from_json(f.name)
            assert len(result) == 1
            os.unlink(f.name)


class TestTransformData:
    def test_returns_dataframe(self):
        """DataFrame 반환"""
        data = [{'id': 1, 'name': 'test'}]
        df = transform_data(data)
        assert isinstance(df, pd.DataFrame)


class TestValidateData:
    def test_passes_valid_data(self):
        """유효한 데이터 통과"""
        df = pd.DataFrame({'id': [1, 2], 'name': ['a', 'b']})
        valid, errors = validate_data(df, ['id', 'name'])
        assert valid is True
        assert len(errors) == 0

    def test_fails_missing_column(self):
        """누락 컬럼 실패"""
        df = pd.DataFrame({'id': [1, 2]})
        valid, errors = validate_data(df, ['id', 'name'])
        assert valid is False


class TestLoadToDatabase:
    def test_loads_data(self, db_conn):
        """데이터 적재"""
        df = pd.DataFrame({'id': [1, 2], 'value': [10, 20]})
        rows = load_to_database(db_conn, df, 'test_load')
        assert rows == 2


class TestETLPipeline:
    def test_pipeline_runs(self):
        """파이프라인 실행"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([{'id': 1, 'value': 100}], f)
            f.flush()

            pipeline = ETLPipeline(':memory:')
            result = pipeline.run(f.name, 'test', ['id', 'value'])

            assert result is not None
            os.unlink(f.name)


# ============================================================
# Part 4: 배치 처리
# ============================================================

class TestBatchInsert:
    def test_batch_insert(self, db_conn):
        """배치 삽입"""
        cursor = db_conn.cursor()
        cursor.execute("CREATE TABLE batch_test (id INTEGER, name TEXT)")
        db_conn.commit()

        data = [(i, f'name_{i}') for i in range(50)]
        rows = batch_insert(db_conn, data, 'batch_test', ['id', 'name'], batch_size=10)
        assert rows == 50


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
