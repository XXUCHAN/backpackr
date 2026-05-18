# Activity ETL & WAU

Kaggle Ecommerce Activity 로그를 Spark로 처리해 KST 기준 partitioned parquet dataset과 Hive external table을 만들고, 이를 기반으로 WAU를 계산하는 과제용 프로젝트다.

## Scope

- UTC 원천 이벤트를 KST 기준 일 partition으로 적재
- Validation 및 DLQ 분리
- Exact Deduplication 수행
- `user_id` 기준 5분 inactivity rule로 세션 재생성
- Hive external table 제공
- `user_id` 기준 WAU 계산
- `session_id` 기준 주간 활성 세션 수 계산
- 재처리 및 장애 복구가 가능한 배치 구조 설계

## Tech Stack

- Scala 2.12
- Apache Spark 3.5
- Hive External Table
- Parquet + Snappy
- sbt

## Project Structure

```text
.
├── .gitignore
├── build.sbt
├── docs/
│   └── Activity_ETL_WAU_Design_V4.md
├── project/
│   └── build.properties
├── README.md
└── src/
    ├── main/
    │   ├── resources/
    │   │   ├── log4j2.properties
    │   │   └── sql/
    │   │       ├── activity_events.ddl.sql
    │   │       ├── wau_users.sql
    │   │       └── weekly_active_sessions.sql
    │   └── scala/
    │       ├── ActivityBatchApp.scala
    │       ├── config/
    │       ├── logging/
    │       ├── model/
    │       ├── query/
    │       ├── reader/
    │       ├── schema/
    │       ├── sessionization/
    │       ├── support/
    │       ├── transform/
    │       └── writer/
    └── test/
        ├── resources/
        └── scala/
            ├── smoke/
            ├── support/
            └── transform/
```

## Planned Modules

- `ActivityBatchApp`: 배치 엔트리포인트
- `config`: 실행 파라미터 파싱
- `model`: 배치 모드 및 도메인 모델
- `reader`: CSV 입력 로딩
- `transform`: validation, deduplication
- `sessionization`: 세션 생성 및 session snapshot 관리
- `writer`: parquet write, staging publish, partition registration
- `query`: WAU 계산 쿼리와 DDL 리소스 로딩
- `logging`: batch run log 기록
- `support`: Spark session, time, path 공통 유틸

## Dataset

대상 데이터:

- `2019-Oct.csv`
- `2019-Nov.csv`

원본:

- [Kaggle Ecommerce Behavior Data from Multi Category Store](https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store)

## Design Document

- V4 설계 문서: [docs/Activity_ETL_WAU_Design_V4.md](docs/Activity_ETL_WAU_Design_V4.md)

## Current Status

현재 코드로 실제 동작하는 범위는 아래와 같다.

- CSV read
- 원천 이벤트 정규화
- validation / DLQ 분리
- exact deduplication
- 기본 sessionization
- 전일 상태 기반 `session_state_snapshot` 저장
- preflight validation
- batch run log 파일 기록
- quality gate
- staging parquet write
- optional final output write
- Hive external table 생성 및 partition 등록

아직 미구현 상태인 항목은 아래와 같다.

- 다중 일자 backfill 기준 `session_state_snapshot` seed 재계산
- Hive partition 등록
- WAU 실제 집계 연결
- 원자적 staging -> final promote 정식 흐름

## Operational Considerations

운영성과 복구 가능성을 고려해 현재 배치에는 아래 장치를 포함했다.

- `PreflightValidator`
  - input path 존재 여부
  - `start-date <= end-date`
  - 미래 날짜 실행 방지
  - 동일 `run_id` 경로 충돌 방지
- `QualityGate`
  - `input_row_count > 0`
  - `output_row_count > 0`
  - 전체 `event_time` 파싱 실패 방지
  - `DLQ ratio > 1%` warning
  - `DLQ ratio > 5%` fail
- `batch-run-log`
  - `RUNNING`
  - `VALIDATED`
  - `PROMOTED`
  - `SUCCESS`
  - `FAILED`
- exact dedup
  - 진짜 중복만 제거하기 위해 `user_id`, `event_time_utc`, `event_type`, `product_id`, `category_id`, `category_code`, `brand`, `normalized_price`, `raw_user_session`이 모두 같은 경우만 제거

즉 현재 구현은 단순 변환 배치가 아니라, 입력 오류를 조기에 차단하고 실행 상태와 품질 결과를 남기는 운영형 배치 골격까지 포함한다.

## Prerequisites

로컬 실행 기준으로 아래 의존성이 필요하다.

- JDK 17
- sbt 1.10 이상
- 여유 디스크 공간

실데이터 CSV(`2019-Oct.csv`, `2019-Nov.csv`)는 각각 5GB 이상이므로, 로컬에서 parquet까지 기록하려면 충분한 디스크 공간이 필요하다.

macOS + Homebrew 기준 설치 예시는 아래와 같다.

```bash
brew install sbt
brew install openjdk@17
```

JDK 17은 keg-only이므로, 실행 시 아래처럼 `JAVA_HOME`을 명시해서 사용하는 방식을 권장한다.

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH=/opt/homebrew/opt/openjdk@17/bin:$PATH
```

프로젝트 루트에서 아래 변수를 먼저 잡아두면 이후 명령을 그대로 사용할 수 있다.

```bash
export PROJECT_ROOT="$(pwd)"
```

## Local Setup

프로젝트 루트 아래 `.data` 디렉터리에 원본 CSV를 위치시킨다.

```text
.data/
├── 2019-Oct.csv
└── 2019-Nov.csv
```

테스트/실행 캐시는 아래 경로를 사용한다.

- `.coursier/`
- `.ivy2/`
- `.sbt/`
- `.tmp/`

## Verification

README에는 핵심 검증 명령만 남기고, 세부 테스트 조합은 필요 시 `src/test/scala` 기준으로 선택 실행한다.

### 1. 전체 테스트 실행

```bash
/usr/bin/env \
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
PATH=/opt/homebrew/opt/openjdk@17/bin:/opt/homebrew/bin:/usr/bin:/bin \
COURSIER_CACHE="$PROJECT_ROOT/.coursier" \
sbt \
  -Dsbt.global.base="$PROJECT_ROOT/.sbt" \
  -Dsbt.boot.directory="$PROJECT_ROOT/.sbt/boot" \
  -Dsbt.ivy.home="$PROJECT_ROOT/.ivy2" \
  -Dsbt.coursier.home="$PROJECT_ROOT/.coursier" \
  test
```

### 2. 실데이터 기반 E2E 스모크 테스트

`2019-Oct.csv`를 직접 읽되, 샘플 크기는 `SMOKE_SAMPLE_LIMIT`로 제한한다.

```bash
/usr/bin/env \
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
PATH=/opt/homebrew/opt/openjdk@17/bin:/opt/homebrew/bin:/usr/bin:/bin \
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

스모크 테스트는 아래 정보를 로그로 출력한다.

- preflight 통과 여부
- quality gate 통과 여부
- batch run status
- raw row 수
- validation 통과/실패 건수
- dedup 후 row 수
- unique session 수
- 중복 그룹 수
- 제거된 duplicate row 수
- `session snapshot` 경로
- `batch-run-log.json` 경로
- `duplicate-groups.json` 경로

### 3. Hive external table E2E 스모크 테스트

같은 스모크 파일 안에서 `2019-Oct.csv` sample로 final parquet를 생성한 뒤 external table 생성, partition 등록, query까지 확인한다.

```bash
/usr/bin/env \
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
PATH=/opt/homebrew/opt/openjdk@17/bin:/opt/homebrew/bin:/usr/bin:/bin \
COURSIER_CACHE="$PROJECT_ROOT/.coursier" \
HIVE_SMOKE_SAMPLE_LIMIT=1000 \
sbt \
  -Dsbt.global.base="$PROJECT_ROOT/.sbt" \
  -Dsbt.boot.directory="$PROJECT_ROOT/.sbt/boot" \
  -Dsbt.ivy.home="$PROJECT_ROOT/.ivy2" \
  -Dsbt.coursier.home="$PROJECT_ROOT/.coursier" \
  "testOnly smoke.ActivityBatchAppE2ESmokeSpec -- -z \"Hive external table\""
```

## Running Locally

현재 구현 기준에서 실제로 실행 가능한 경로는 `sbt run` 기반 로컬 Spark 검증이다.

### 1. 단일 파일 스모크 실행

`2019-Oct.csv` 하나만 대상으로 실행:

```bash
/usr/bin/env \
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
PATH=/opt/homebrew/opt/openjdk@17/bin:/opt/homebrew/bin:/usr/bin:/bin \
COURSIER_CACHE="$PROJECT_ROOT/.coursier" \
sbt \
  -Dsbt.global.base="$PROJECT_ROOT/.sbt" \
  -Dsbt.boot.directory="$PROJECT_ROOT/.sbt/boot" \
  -Dsbt.ivy.home="$PROJECT_ROOT/.ivy2" \
  -Dsbt.coursier.home="$PROJECT_ROOT/.coursier" \
  "run --mode daily \
    --start-date 2019-10-01 \
    --end-date 2019-10-31 \
    --input-path $PROJECT_ROOT/.data/2019-Oct.csv \
    --staging-base-path $PROJECT_ROOT/.tmp/staging \
    --dlq-base-path $PROJECT_ROOT/.tmp/dlq \
    --session-state-base-path $PROJECT_ROOT/.tmp/session-state \
    --run-log-base-path $PROJECT_ROOT/.tmp/batch-run-log \
    --run-id local_validation_oct"
```

### 2. 두 파일을 함께 실행

`.data` 폴더 전체를 입력으로 주면 내부의 CSV를 함께 읽는다.

```bash
/usr/bin/env \
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
PATH=/opt/homebrew/opt/openjdk@17/bin:/opt/homebrew/bin:/usr/bin:/bin \
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
    --run-id local_validation_001"
```

final output path까지 같이 쓰고 싶으면 `--output-base-path`를 추가한다.

```bash
--output-base-path $PROJECT_ROOT/.tmp/final-output
```

## Output Paths

현재 구현에서는 아래 산출물이 생성된다.

- valid output: `.tmp/staging/run_id=<run_id>/valid/`
- invalid output: `.tmp/dlq/run_id=<run_id>/invalid/`
- session snapshot: `.tmp/session-state/snapshot_date_kst=<target_date>/`
- batch run log: `.tmp/batch-run-log/run_id=<run_id>/batch-run-log.json`
- optional final output: `<output-base-path>/event_date_kst=...`

콘솔에는 아래 지표가 출력된다.

- `input_row_count`
- `validated_row_count`
- `sessionized_row_count`
- `unique_session_count`
- `duplicate_group_count`
- `duplicate_rows_count`
- `dropped_duplicate_row_count`
- `invalid_row_count`

invalid row가 존재하면 `reject_reason` 집계도 함께 출력한다.

실데이터 기반 스모크 테스트는 아래 산출물을 함께 남길 수 있다.

- valid parquet: `.tmp/smoke-output/<run-name>/valid/`
- duplicates parquet: `.tmp/smoke-output/<run-name>/duplicates/parquet/`
- duplicate groups pretty JSON: `.tmp/smoke-output/<run-name>/duplicates/duplicate-groups.json`
- session snapshot pretty JSON: `.tmp/smoke-output/<run-name>/session-state/session-snapshot.json`

`duplicate-groups.json`은 사람이 바로 확인할 수 있도록 pretty JSON 형태로 저장되며, 각 중복 그룹에 대해 아래 구조를 가진다.

```json
[
  {
    "dedup_key": "...",
    "duplicate_group_size": 2,
    "dropped_duplicate_row_count": 1,
    "retained_row": { "...": "..." },
    "dropped_rows": [{ "...": "..." }]
  }
]
```

즉 어떤 row를 남기고 어떤 row를 중복으로 제거했는지 바로 비교할 수 있다.

## Notes

- `2019-Oct.csv` 단일 파일도 로컬에서 매우 크기 때문에, 전체 parquet write까지 수행하면 디스크 부족이 발생할 수 있다.
- 실제로 `2019-Oct.csv` 스모크 실행 시 코드 경로는 정상적으로 통과했지만, write 단계에서 `No space left on device`가 발생했다.
- 따라서 과제 검증 단계에서는 먼저 단위 테스트와 작은 범위 스모크를 수행하고, 전체 데이터 실행은 충분한 디스크 공간이 있을 때 수행하는 것이 안전하다.
- 현재 `build.sbt`는 로컬 `sbt run` 검증을 위해 Spark dependency를 runtime classpath에 포함하도록 설정되어 있다.
- 추후 실제 클러스터 배포형 `spark-submit`을 사용할 경우에는 packaging 전략을 별도로 정리할 필요가 있다.
- 운영형 `sbt run` 실행 시 `batch-run-log.json`에 상태와 주요 메트릭이 기록된다.
- 현재 세션 스냅샷은 `start-date` 기준 전일 snapshot을 seed로 읽고, 처리 후 당일 snapshot을 다시 저장하는 1차 버전까지 구현되어 있다.
- 상세 설계와 SPOF 대응 시나리오는 V4 문서에 정리했다.
