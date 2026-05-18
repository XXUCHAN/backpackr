# Activity ETL & WAU

Kaggle Ecommerce Activity 로그를 Spark로 처리해 KST 기준 partitioned parquet dataset과 Hive external table을 생성하는 과제 프로젝트다.  
현재 구현 범위는 정규화, validation/DLQ, exact dedup, sessionization, session snapshot, staging 기반 publish, Hive external table 등록까지다.

## 구현 범위

- UTC 원천 이벤트를 KST 기준 일 partition으로 변환
- Validation 및 DLQ 분리
- exact deduplication
- `user_id` 기준 5분 inactivity rule 기반 sessionization
- `D-1` snapshot seed 기반 세션 이어붙이기
- Parquet + Snappy 적재
- staging -> final partition promote
- Hive external table 생성 및 partition 등록
- preflight validation / quality gate / batch run log 기록

## 기술 스택

- Scala 2.12
- Apache Spark 3.5
- Hive External Table
- Parquet + Snappy
- sbt

Scala를 선택한 이유는 Spark의 DataFrame, Window Function, SQL DSL을 가장 자연스럽게 사용할 수 있고, 본 과제의 핵심인 dedup, sessionization, partition 처리 구현이 Java 대비 간결하기 때문이다.

## 데이터셋

- `2019-Oct.csv`
- `2019-Nov.csv`

원본: [Kaggle Ecommerce Behavior Data from Multi Category Store](https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store)

프로젝트 루트 아래 `.data` 디렉터리에 배치한다.

```text
.data/
├── 2019-Oct.csv
└── 2019-Nov.csv
```

## 설계 문서

- [docs/Activity_ETL_WAU_Design_V4.md](docs/Activity_ETL_WAU_Design_V4.md)

## 실행 환경

- JDK 17
- sbt 1.10+
- 충분한 디스크 공간

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

## 실행 방법

### 1. 배치 실행

두 CSV를 함께 읽어 staging, DLQ, session snapshot, batch run log, final output까지 생성하는 예시다.

```bash
/usr/bin/env \
COURSIER_CACHE="$PROJECT_ROOT/.coursier" \
sbt \
  -Dsbt.global.base="$PROJECT_ROOT/.sbt" \
  -Dsbt.boot.directory="$PROJECT_ROOT/.sbt/boot" \
  -Dsbt.ivy.home="$PROJECT_ROOT/.ivy2" \
  -Dsbt.coursier.home="$PROJECT_ROOT/.coursier" \
  "run --mode daily \
    --start-date 2019-10-01 \
    --end-date 2019-11-30 \
    --input-path $PROJECT_ROOT/.data \
    --staging-base-path $PROJECT_ROOT/.tmp/staging \
    --dlq-base-path $PROJECT_ROOT/.tmp/dlq \
    --session-state-base-path $PROJECT_ROOT/.tmp/session-state \
    --run-log-base-path $PROJECT_ROOT/.tmp/batch-run-log \
    --output-base-path $PROJECT_ROOT/.tmp/final-output \
    --run-id local_run_001"
```

Hive partition 등록까지 함께 보려면 아래 옵션을 추가한다.

```bash
--register-hive-partitions --hive-table-name activity_events
```

### 2. 검증

전체 테스트:

```bash
/usr/bin/env \
COURSIER_CACHE="$PROJECT_ROOT/.coursier" \
sbt \
  -Dsbt.global.base="$PROJECT_ROOT/.sbt" \
  -Dsbt.boot.directory="$PROJECT_ROOT/.sbt/boot" \
  -Dsbt.ivy.home="$PROJECT_ROOT/.ivy2" \
  -Dsbt.coursier.home="$PROJECT_ROOT/.coursier" \
  test
```

실데이터 E2E 스모크:

```bash
/usr/bin/env \
COURSIER_CACHE="$PROJECT_ROOT/.coursier" \
SMOKE_SAMPLE_LIMIT=50000 \
SMOKE_OUTPUT_PATH="$PROJECT_ROOT/.tmp/smoke-output/oct-limit-50000" \
sbt \
  -Dsbt.global.base="$PROJECT_ROOT/.sbt" \
  -Dsbt.boot.directory="$PROJECT_ROOT/.sbt/boot" \
  -Dsbt.ivy.home="$PROJECT_ROOT/.ivy2" \
  -Dsbt.coursier.home="$PROJECT_ROOT/.coursier" \
  "testOnly smoke.ActivityBatchAppE2ESmokeSpec"
```

## 산출물

배치 실행 시 주요 산출물은 아래 경로에 생성된다.

- staging output: `.tmp/staging/run_id=<run_id>/valid/`
- DLQ output: `.tmp/dlq/run_id=<run_id>/invalid/`
- session snapshot: `.tmp/session-state/snapshot_date_kst=<target_date>/`
- batch run log: `.tmp/batch-run-log/run_id=<run_id>/batch-run-log.json`
- final output: `.tmp/final-output/event_date_kst=...`

스모크 테스트 실행 시에는 아래 추가 산출물을 확인할 수 있다.

- duplicates parquet: `.tmp/smoke-output/<run>/duplicates/parquet/`
- duplicate groups JSON: `.tmp/smoke-output/<run>/duplicates/duplicate-groups.json`
- session snapshot JSON: `.tmp/smoke-output/<run>/session-state/session-snapshot.json`

## 운영 / 장애복구 포인트

- `PreflightValidator`
  - input path 존재 여부
  - 날짜 범위 검증
  - 동일 `run_id` 경로 충돌 방지
- `QualityGate`
  - `input_row_count > 0`
  - `output_row_count > 0`
  - `DLQ ratio > 1%` warning
  - `DLQ ratio > 5%` fail
- `ActivityWriter`
  - final 경로 직접 write 금지
  - staging에 먼저 write 후 검증 통과 시 partition promote
- `BatchRunLogger`
  - `RUNNING`
  - `VALIDATED`
  - `PROMOTED`
  - `SUCCESS`
  - `FAILED`
- `SessionStateStore`
  - `D-1` snapshot을 seed로 읽고 당일 snapshot 저장

## 현재 한계

- WAU 실행 및 결과 저장은 아직 연결 전이다.
- session snapshot은 과제 범위상 `D-1` seed 기반까지만 구현했다.
- 다중 일자 backfill 전체에 대한 snapshot 재계산 자동화는 포함하지 않았다.
- promote 실패 후 자동 재시도는 아직 미구현이다.
