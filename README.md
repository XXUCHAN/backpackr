# Activity ETL & WAU

Kaggle Ecommerce Activity 로그를 Spark로 처리하여 KST 기준 parquet dataset, Hive external table, WAU 결과를 생성하는 Spark Application 과제다.

현재 구현 범위:

- UTC 원천 이벤트 정규화
- validation / DLQ 분리
- exact deduplication
- 5분 inactivity 기반 sessionization
- `D-1` seed + `end-date` snapshot 저장
- staging -> final promote
- promote rollback / retry
- Hive external table 생성 및 partition 등록
- WAU / weekly active sessions 실행 및 결과 저장
- preflight validation / quality gate / batch run log 기록

## 요구사항 대응

- **구현 언어**: Scala 2.12
- **Scala 선택 사유**: Spark DataFrame API, Window Function, SQL DSL을 가장 간결하게 표현할 수 있고, 세션화와 partition 처리 구현이 Java 대비 짧고 읽기 쉽다.
- **입력 데이터**: `2019-Oct.csv`, `2019-Nov.csv`
- **KST daily partition**: `event_date_kst`
- **세션 규칙**: 동일 `user_id` 내 `event_time` 간격이 5분 이상이면 새 `session_id` 생성
- **저장 포맷**: parquet + snappy
- **재처리 대응**: `start-date`, `end-date` 기반 날짜 범위 재실행 지원
- **External Table 방식**: final parquet를 Hive external table `activity_events`로 등록
- **배치 장애 복구 장치**: preflight validation, quality gate, staging -> final promote, rollback / retry, batch run log
- **WAU 계산**:
  - `user_id` 기준 WAU
  - `session_id` 기준 Weekly Active Sessions

## 실행 환경

- JDK 17
- sbt 1.10+
- Spark 3.5

macOS + Homebrew 예시:

```bash
brew install sbt
brew install openjdk@17
```

실행 전:

```bash
export PROJECT_ROOT="$(pwd)"
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH=/opt/homebrew/opt/openjdk@17/bin:$PATH
```

## 입력 데이터

프로젝트 루트 아래 `.data` 디렉터리에 배치한다.

```text
.data/
├── 2019-Oct.csv
└── 2019-Nov.csv
```

원본: [Kaggle Ecommerce Behavior Data from Multi Category Store](https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store)

## 0. 사전 준비 (Prerequisites)

본 프로젝트는 Scala 2.12와 Spark 3.x 기반으로 작성되었으므로 **JDK 11 이상** (권장: JDK 17)과 **SBT(Scala Build Tool)** 가 설치되어 있어야 합니다.

### 🔹 Java (JDK) 설치 및 경로 설정
* **Mac (Homebrew):** `brew install openjdk@17`
  - 실행 전 터미널에 환경 변수를 등록하세요: `export JAVA_HOME=$(/usr/libexec/java_home -v 17)`
* **Ubuntu / Debian:** `sudo apt-get install openjdk-17-jdk`
  - 실행 전 터미널에 환경 변수를 등록하세요: `export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`

### 🔹 SBT 설치
* **Mac (Homebrew):** `brew install sbt`
* **Ubuntu / Debian:**
  ```bash
  sudo apt-get update
  sudo apt-get install apt-transport-https curl gnupg -yqq
  echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
  echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
  curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo -H gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import
  sudo chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg
  sudo apt-get update
  sudo apt-get install sbt
  ```

(또는 프로젝트 루트에서 로컬 sbt 래퍼 스크립트를 다운로드하여 `sbt` 대신 `./sbt` 명령어를 사용할 수도 있습니다.)

### 🔹 Docker (초간편 실행 - 권장)
Java나 SBT 설치 과정 없이 Docker만 있다면 곧바로 실행 가능합니다.
* **설치:** [Docker Desktop](https://www.docker.com/products/docker-desktop/) 설치

## 실행 방법

기본 경로는 코드에 설정되어 있으므로 필수 파라미터만 주면 된다.

### 1. 전체 데이터셋 실행

`.data` 아래의 `2019-Oct.csv`, `2019-Nov.csv`를 함께 읽어 전체 기간을 처리하는 기본 예시다.

**Docker Compose 환경 (권장)**
```bash
docker compose run spark-etl sbt "run --start-date 2019-10-01 \
  --end-date 2019-11-30 \
  --input-path .data \
  --run-id full_dataset_run \
  --execute-wau"
```

**로컬 환경 (SBT가 설치된 경우)**
```bash
# 로컬 sbt 래퍼를 사용할 경우 sbt 대신 ./sbt 사용
sbt "run --start-date 2019-10-01 \
  --end-date 2019-11-30 \
  --input-path .data \
  --run-id full_dataset_run \
  --execute-wau"
```
### 2. 특정 기간만 부분 실행

검증이나 재처리를 위해 특정 날짜 범위만 실행할 수도 있다.

예: `2019-10-01 ~ 2019-10-15`

**Docker Compose 환경 (권장)**
```bash
docker compose run spark-etl sbt "run --start-date 2019-10-01 \
  --end-date 2019-10-15 \
  --input-path .data/2019-Oct.csv \
  --run-id oct_1_15_run \
  --execute-wau"
```

**로컬 환경 (SBT가 설치된 경우)**
```bash
sbt "run --start-date 2019-10-01 \
  --end-date 2019-10-15 \
  --input-path .data/2019-Oct.csv \
  --run-id oct_1_15_run \
  --execute-wau"
```

기본값:

- `mode = daily`
- `staging-base-path = output/staging`
- `dlq-base-path = output/dlq`
- `session-state-base-path = output/session-state`
- `run-log-base-path = output/run-log`
- `output-base-path = output/final-output`
- `wau-output-base-path = output/wau-results`
- `hive-table-name = activity_events`

`--execute-wau`를 주면 Hive external table 생성, partition 등록, WAU 실행까지 함께 수행한다.

## 테스트

전체 테스트:

**Docker Compose 환경 (권장):**
```bash
docker compose run spark-etl sbt test
```

**로컬 환경:**
```bash
sbt test
```

실데이터 스모크:

**Docker Compose 환경 (권장):**
```bash
docker compose run \
  -e SMOKE_SAMPLE_LIMIT=100000 \
  -e SMOKE_OUTPUT_PATH="output/smoke-output/oct-limit-100000" \
  -e HIVE_SMOKE_OUTPUT_PATH="output/smoke-output/hive-oct-limit-100000" \
  spark-etl sbt "testOnly smoke.ActivityBatchAppE2ESmokeSpec"
```

**로컬 환경:**
```bash
SMOKE_SAMPLE_LIMIT=100000 \
SMOKE_OUTPUT_PATH="output/smoke-output/oct-limit-100000" \
HIVE_SMOKE_OUTPUT_PATH="output/smoke-output/hive-oct-limit-100000" \
sbt "testOnly smoke.ActivityBatchAppE2ESmokeSpec"
```

## 주요 산출물

- staging output: `output/staging/run_id=<run_id>/valid/`
- DLQ output: `output/dlq/run_id=<run_id>/invalid/`
- final output: `output/final-output/event_date_kst=...`
- session snapshot: `output/session-state/snapshot_date_kst=<end-date>/`
- batch run log: `output/run-log/run_id=<run_id>/batch-run-log.json`
- WAU output:
  - `output/wau-results/run_id=<run_id>/wau-users/`
  - `output/wau-results/run_id=<run_id>/weekly-active-sessions/`

## 실제 검증 결과

실데이터 전체 기간 `2019-10-01 ~ 2019-11-30` 실행 결과:

- `input_row_count = 109,950,743`
- `validated_row_count = 109,362,687`
- `sessionized_row_count = 109,232,472`
- `unique_session_count = 22,885,344`
- `duplicate_group_count = 75,317`
- `duplicate_rows_count = 205,532`
- `dropped_duplicate_row_count = 130,215`
- `invalid_row_count = 0`
- `registered_hive_partitions_count = 61`
- output partitions: `2019-10-01 ~ 2019-11-30`

WAU 결과:

- `2019-09-30`: `818,388`
- `2019-10-07`: `1,057,958`
- `2019-10-14`: `1,090,898`
- `2019-10-21`: `1,093,146`
- `2019-10-28`: `1,054,722`
- `2019-11-04`: `1,321,141`
- `2019-11-11`: `1,543,309`
- `2019-11-18`: `1,376,755`
- `2019-11-25`: `1,133,949`

Weekly Active Sessions 결과:

- `2019-09-30`: `1,570,536`
- `2019-10-07`: `2,153,262`
- `2019-10-14`: `2,256,082`
- `2019-10-21`: `2,152,730`
- `2019-10-28`: `2,114,204`
- `2019-11-04`: `2,750,735`
- `2019-11-11`: `4,752,893`
- `2019-11-18`: `2,870,609`
- `2019-11-25`: `2,264,293`

## WAU 계산 쿼리

### 6-a. `user_id` 기준 WAU

```sql
SELECT
  date_sub(next_day(event_date_kst, 'MON'), 7) AS week_start_kst,
  COUNT(DISTINCT user_id) AS wau_users
FROM activity_events
GROUP BY date_sub(next_day(event_date_kst, 'MON'), 7)
ORDER BY week_start_kst;
```

### 6-b. `session_id` 기준 Weekly Active Sessions

```sql
WITH sessions AS (
  SELECT DISTINCT
    session_id,
    to_date(session_start_time_kst) AS session_start_date_kst
  FROM activity_events
)
SELECT
  date_sub(next_day(session_start_date_kst, 'MON'), 7) AS week_start_kst,
  COUNT(*) AS weekly_active_sessions
FROM sessions
GROUP BY date_sub(next_day(session_start_date_kst, 'MON'), 7)
ORDER BY week_start_kst;
```

## 운영 포인트

- `PreflightValidator`: input path, 날짜 범위, run path 충돌을 사전 검증
- `QualityGate`: `DLQ ratio > 1%` warning, `> 5%` fail
- `ActivityWriter`: staging 우선 쓰기, final promote, rollback / retry 지원
- `BatchRunLogger`: 상태 이력을 append JSON 배열로 기록
- `SessionStateStore`: `D-1` snapshot seed 사용, 처리 마지막 날 기준 snapshot 저장

## 참고

- 제출용 요약: [docs/Submission_Summary.md](docs/Submission_Summary.md:1)
