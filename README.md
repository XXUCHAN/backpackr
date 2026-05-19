# Activity ETL & WAU

Kaggle Ecommerce Activity 로그를 Spark로 처리하여 KST 기준 parquet dataset, Hive external table, WAU 결과를 생성하는 Spark Application 과제다.

## 🏗 아키텍처 파이프라인 (Architecture Pipeline)

```mermaid
flowchart TD
    %% Input Layer
    subgraph Data Sources
        R1[(2019-Oct.csv)]
        R2[(2019-Nov.csv)]
    end

    %% Operations
    subgraph Ops [Operations & Logging]
        Preflight{Preflight<br>Validation}
        Logger[(Batch Run Log)]
    end

    %% Processing Layer
    subgraph Spark ETL Pipeline
        Read[CsvReader]
        Norm[Normalizer<br>UTC to KST]
        Val[Validator<br>Quality Gate]
        Dedup[Deduplicator]
        Sess[Sessionizer<br>5min Inactivity]

        Read --> Norm --> Val --> Dedup --> Sess
    end

    %% Storage Layer
    subgraph Storage & Output
        DLQ[(DLQ<br>Invalid Data)]
        Stage[/Staging Area/]
        Final[(Final Parquet<br>Partitioned by KST)]
        Snap[(Session Snapshot)]
    end

    %% Hive & Analytics
    subgraph Analytics
        Hive[(Hive MetaStore<br>External Table)]
        WAU[WAU & Weekly Sessions]
    end

    %% Flow Connections
    R1 & R2 --> Preflight
    Preflight -->|Pass| Read
    Preflight -.->|Fail| Logger
    
    Val -->|Reject| DLQ
    Sess -->|Save State| Snap
    Snap -.->|Load Previous State| Sess
    Sess -->|Write Valid| Stage
    Stage -->|Atomic Promote<br>Rollback/Retry| Final

    Final -->|Register Partitions| Hive
    Hive -->|Execute Query| WAU
    
    Final -.->|Log Status| Logger
    WAU -.->|Log Status| Logger

    %% Style
    classDef default fill:#ffffff,stroke:#333,stroke-width:1px,color:#333;
    classDef primary fill:#2196f3,stroke:#1565c0,stroke-width:2px,color:#fff;
    classDef error fill:#f44336,stroke:#c62828,stroke-width:2px,color:#fff;
    classDef ops fill:#ff9800,stroke:#e65100,stroke-width:2px,color:#fff;
    classDef spark fill:#4caf50,stroke:#2e7d32,stroke-width:2px,color:#fff;

    class Read,Norm,Val,Dedup,Sess spark;
    class Final,Hive,WAU primary;
    class DLQ error;
    class Preflight,Logger ops;
```

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
- **Scala 선택 사유**:
  - 평소 TypeScript를 다루며 함수형 프로그래밍(FP)에 익숙한데, Scala 역시 메서드 체이닝(Method Chaining) 등 강력한 FP 패러다임을 지원하여 데이터 변환 과정을 매우 간결하고 직관적으로 작성할 수 있었다.
  - Java를 사용할 때 발생하는 불필요한 보일러플레이트 코드가 없어 전체 코드 길이가 짧아지고 유지보수를 위한 가독성이 크게 향상되었다.
  - 추가로 Spark의 네이티브 언어인 만큼 DataFrame API, Window Function, SQL DSL 등을 가장 매끄럽고 강력하게 활용할 수 있다는 점이 큰 장점으로 작용했다.
- **입력 데이터**: `2019-Oct.csv`, `2019-Nov.csv`
- **KST daily partition**: `event_date_kst`
- **세션 규칙**: 동일 `user_id` 내 `event_time` 간격이 5분 이상이면 새 `session_id` 생성
- **저장 포맷**: parquet + snappy
- **재처리 대응**: `start-date`, `end-date` 파라미터 기반 특정 날짜 범위 부분 재실행 지원
- **추가 기간 데이터 처리 (Incremental Processing)**: 이전 배치의 마지막 세션 상태(Snapshot)를 로드하여 새로운 기간의 데이터와 병합함으로써, 배치 경계(자정 등)에 걸친 세션도 연속적으로 분리 및 추적
- **External Table 방식**: final parquet를 Hive external table `activity_events`로 등록
- **배치 장애 복구 장치**: preflight validation, quality gate, staging -> final promote, rollback / retry, batch run log
- **WAU 계산**:
  - `user_id` 기준 WAU
  - `session_id` 기준 Weekly Active Sessions

## 실행 환경 (Tech Stack)

- **Scala**: 2.12
- **Spark**: 3.5.x (3.5.1)
- **JDK**: 17
- **SBT**: 1.10+
- **Infrastructure**: Docker Compose 기반

## 입력 데이터

프로젝트 루트 아래 `.data` 디렉터리에 배치한다.

```text
.data/
├── 2019-Oct.csv
└── 2019-Nov.csv
```

원본: [Kaggle Ecommerce Behavior Data from Multi Category Store](https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store)

## 0. 사전 준비 (Prerequisites)

- **설치:** [Docker Desktop](https://www.docker.com/products/docker-desktop/) 설치

## 실행 방법

기본 경로는 코드에 설정되어 있으므로 필수 파라미터만 주면 된다.

### 1. 전체 데이터셋 실행

`.data` 아래의 `2019-Oct.csv`, `2019-Nov.csv`를 함께 읽어 전체 기간을 처리하는 기본 예시다.

**Docker Compose 환경 (권장)**

```bash
docker compose up
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

- 제출용 요약: [docs/Submission_Summary.md](docs/Submission_Summary.md)
