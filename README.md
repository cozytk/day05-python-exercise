# Day 05 실습: Python-SQL 연동 및 데이터 파이프라인

## 🎯 이 실습을 완료하면?

이 실습을 통해 다음 역량을 갖추게 됩니다:

| 배우는 것 | 실무 활용 |
|----------|----------|
| Context Manager (`with`) | 안전한 DB 연결 관리 |
| CRUD 작업 | 데이터 생성/조회/수정/삭제 |
| Pandas + SQL 통합 | 데이터 분석 파이프라인 |
| ETL 파이프라인 | Extract → Transform → Load |
| 배치 처리 | 대량 데이터 효율적 처리 |

> 💡 **Python-SQL 연동이란?** Python 코드로 데이터베이스를 제어하고, 데이터 파이프라인을 자동화하는 기술!

---

## 📚 사전 준비

Day01~Day04 실습을 완료했다면 Git과 Docker가 이미 설치되어 있습니다.

```bash
# 확인
git --version
docker --version
```

> ⚠️ **중요**: Docker Desktop이 **실행 중**이어야 합니다!

---

## 🚀 Step by Step 실습 가이드

### Step 1: 저장소 Fork & Clone

```bash
# YOUR_USERNAME을 본인의 GitHub 사용자명으로 변경
git clone https://github.com/YOUR_USERNAME/day05-python-exercise.git
cd day05-python-exercise
```

### Step 2: 현재 상태 확인

```bash
docker compose run --rm test
```

18개 테스트가 모두 **FAILED**로 나오는 것이 정상입니다!

### Step 3: 단계별 구현하기

| 순서 | 함수명 | 테스트 명령어 |
|------|--------|-------------|
| **Part 1: DB 연결 및 기본 CRUD** | | |
| 1 | `get_connection` | `pytest test_exercise.py::TestGetConnection -v` |
| 2 | `create_tables` | `pytest test_exercise.py::TestCreateTables -v` |
| 3 | `insert_user` | `pytest test_exercise.py::TestInsertUser -v` |
| 4 | `insert_order` | `pytest test_exercise.py::TestInsertOrder -v` |
| 5 | `get_user_orders` | `pytest test_exercise.py::TestGetUserOrders -v` |
| 6 | `get_order_summary` | `pytest test_exercise.py::TestGetOrderSummary -v` |
| **Part 2: Pandas와 SQL 통합** | | |
| 7 | `query_to_dataframe` | `pytest test_exercise.py::TestQueryToDataframe -v` |
| 8 | `dataframe_to_table` | `pytest test_exercise.py::TestDataframeToTable -v` |
| 9 | `analyze_orders` | `pytest test_exercise.py::TestAnalyzeOrders -v` |
| **Part 3: ETL 파이프라인** | | |
| 10 | `extract_from_json` | `pytest test_exercise.py::TestExtractFromJson -v` |
| 11 | `transform_data` | `pytest test_exercise.py::TestTransformData -v` |
| 12 | `validate_data` | `pytest test_exercise.py::TestValidateData -v` |
| 13 | `load_to_database` | `pytest test_exercise.py::TestLoadToDatabase -v` |
| 14 | `ETLPipeline` | `pytest test_exercise.py::TestETLPipeline -v` |
| **Part 4: 배치 처리** | | |
| 15 | `batch_insert` | `pytest test_exercise.py::TestBatchInsert -v` |

> 💡 테스트 명령어 앞에 `docker compose run --rm test` 를 붙여서 실행하세요!

### Step 4: 전체 테스트 통과 확인

```bash
docker compose run --rm test
```

**18 passed**가 나오면 성공!

### Step 5: GitHub에 Push

```bash
git add .
git commit -m "feat: 모든 함수 구현 완료"
git push origin main
```

---

## 💡 막혔을 때는?

각 단계별로 정답이 포함된 브랜치가 준비되어 있습니다:

| 브랜치 | 포함된 함수 |
|--------|-----------|
| `base` | 빈칸 상태 (시작점) |
| `step-1` | Part 1: DB 연결 및 CRUD (6개) |
| `step-2` | + Part 2: Pandas-SQL 통합 (3개) |
| `step-3` | + Part 3: ETL 파이프라인 (5개) |
| `step-4` | + Part 4: 배치 처리 (1개) |
| `main` | 모든 함수 완성 |

### 정답 확인 방법

```bash
# step-1에서 추가된 코드 확인
git diff base step-1 -- exercise.py
```

---

## 📝 Python-SQL 연동 힌트

### Context Manager (with문)
```python
@contextmanager
def get_connection(db_path):
    conn = sqlite3.connect(db_path)
    try:
        yield conn
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        conn.close()
```

### CRUD 기본
```python
# INSERT
cursor.execute("INSERT INTO users (name, email) VALUES (?, ?)", (name, email))
user_id = cursor.lastrowid

# SELECT
cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
user = cursor.fetchone()
```

### Pandas-SQL 통합
```python
# SQL → DataFrame
df = pd.read_sql_query("SELECT * FROM users", conn)

# DataFrame → SQL
df.to_sql('users', conn, if_exists='replace', index=False)
```

### ETL 패턴
```python
# Extract
data = json.load(open('data.json'))

# Transform
df = pd.DataFrame(data).fillna('')

# Load
df.to_sql('table', conn, if_exists='replace', index=False)
```

---

## 🐳 Docker 명령어 모음

| 명령어 | 설명 |
|--------|------|
| `docker compose run --rm test` | 전체 테스트 실행 |
| `docker compose run --rm test pytest test_exercise.py::TestXXX -v` | 특정 테스트만 실행 |
| `docker compose run --rm shell` | Python 대화형 셸 (디버깅용) |

---

## ⚠️ 자주 발생하는 오류

### "Generator didn't yield"

**원인**: `@contextmanager`에서 `yield`가 없음

**해결**: `yield conn` 추가

### "NoneType object has no attribute"

**원인**: 함수에서 `return`이 없음

**해결**: `cursor.lastrowid` 또는 `cursor.fetchall()` 반환 확인

### "no such table"

**원인**: `create_tables` 함수가 먼저 호출되지 않음

**해결**: 테이블 생성 순서 확인

---

## 📁 파일 구조

```
day05-python-exercise/
├── README.md              # 이 파일 (실습 가이드)
├── exercise.py            # 🎯 빈칸 채우기 대상
├── test_exercise.py       # 테스트 코드 (수정 금지)
├── requirements.txt       # Python 패키지 목록
├── Dockerfile             # Docker 이미지 설정
├── docker-compose.yml     # Docker 서비스 설정
└── .github/workflows/test.yml
```

---

## 🎉 실습 완료 체크리스트

- [ ] 모든 18개 테스트 통과
- [ ] GitHub에 Push 완료
- [ ] GitHub Actions에서 ✅ 확인

**Day 05 완료! 데이터 파이프라인 기초를 마스터했습니다!** 🚀
