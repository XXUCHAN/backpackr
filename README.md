# Activity ETL & WAU

Kaggle Ecommerce Activity 로그를 Spark로 처리해 KST 기준 partitioned parquet dataset과 Hive external table을 생성하는 과제 프로젝트다.  
현재 구현 범위는 정규화, validation/DLQ, exact dedup, sessionization, session snapshot, staging 기반 publish, Hive external table 등록, WAU 실행 및 결과 저장까지다.

## 구현 범위

- UTC 원천 이벤트를 KST 기준 일 partition으로 변환
- Validation 및 DLQ 분리
- exact deduplication
- `user_id` 기준 5분 inactivity rule 기반 sessionization
- `D-1` snapshot seed 기반 세션 이어붙이기
- Parquet + Snappy 적재
- staging -> final partition promote
- Hive external table 생성 및 partition 등록
- Hive external table 기준 WAU 실행 및 결과 저장
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
  "run --start-date 2019-10-01 \
    --end-date 2019-11-30 \
    --input-path $PROJECT_ROOT/.data \
    --run-id local_run_001"
```

WAU까지 함께 실행하려면 아래 옵션을 추가한다.

```bash
--execute-wau
```

기본값으로 사용되는 경로와 설정은 아래와 같다.

- `mode = daily`
- `staging-base-path = .tmp/staging`
- `dlq-base-path = .tmp/dlq`
- `session-state-base-path = .tmp/session-state`
- `run-log-base-path = .tmp/batch-run-log`
- `output-base-path = .tmp/final-output`
- `wau-output-base-path = .tmp/wau-results`
- `hive-table-name = activity_events`

`--execute-wau`를 주면 Hive external table 생성과 partition 등록도 함께 수행한다.

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
  - `RUNNING -> VALIDATED -> PROMOTED -> SUCCESS/FAILED` 상태 이력이 append 방식으로 저장됨
- final output: `.tmp/final-output/event_date_kst=...`
- WAU output
  - `.tmp/wau-results/run_id=<run_id>/wau-users/`
  - `.tmp/wau-results/run_id=<run_id>/weekly-active-sessions/`

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
- `ActivityWriter` (원자적 배포 및 장애 복구)
  - Final 경로 직접 쓰기 방지 및 Staging 우선 쓰기 적용
  - 배포(Promote) 과정 중 에러 발생 시 기존 데이터 보존을 위한 원상 복구(Rollback) 기능
  - 대기실(Work 폴더) 복사 기반의 안전한 원자적 이동(Atomic Move) 및 자동 재시도(Retry) 메커니즘 구현
- `BatchRunLogger`
  - 상태 이력을 append 방식의 JSON 배열로 기록
- `SessionStateStore`
  - `D-1` snapshot을 seed로 읽고 당일 snapshot 저장

## 한계 및 향후 개선 과제 (Future Works)

**1. 지연 도착 데이터 (Late Arrival Data) 병합**
현재는 파라미터로 입력받은 일자 범위(start-date ~ end-date) 외의 과거 이벤트가 유입될 경우 제외됩니다. 실무 환경에서는 이를 버리지 않고 기존 파티션에 안전하게 끼워 넣는(Merge/Upsert) 로직이 추가로 고려되어야 합니다.

