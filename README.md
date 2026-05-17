# Activity ETL & WAU

Kaggle Ecommerce Activity 로그를 Spark로 처리해 KST 기준 partitioned parquet dataset과 Hive external table을 만들고, 이를 기반으로 WAU를 계산하는 과제용 프로젝트다.

## Scope

- UTC 원천 이벤트를 KST 기준 일 partition으로 적재
- Validation 및 DLQ 분리
- Deduplication 수행
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
    │       ├── state/
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
- `transform`: validation, deduplication, sessionization
- `state`: session snapshot 관리
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

## Prerequisites

로컬 실행 기준으로 아래 의존성이 필요하다.

- JDK 17
- sbt 1.10 이상
- 여유 디스크 공간

실데이터 CSV(`2019-Oct.csv`, `2019-Nov.csv`)는 각각 5GB 이상이므로, 로컬에서 parquet까지 기록하려면 충분한 디스크 공간이 필요하다. 현재 구현 범위는 `read -> normalize -> validate -> dedup -> parquet write`까지이며, 전체 데이터를 한 번에 로컬로 처리하면 저장 공간 부족이 발생할 수 있다.

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

## Running Tests

전체 단위 테스트 실행:

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

현재 구현 범위만 빠르게 확인하는 단위 테스트 실행:

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
  "testOnly transform.ActivityNormalizerSpec transform.ValidatorSpec transform.DeduplicatorSpec"
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
    --run-id local_validation_001"
```

## Output Paths

현재 구현에서는 아래 경로에 parquet가 생성된다.

- valid output: `.tmp/staging/run_id=<run_id>/valid/`
- invalid output: `.tmp/dlq/run_id=<run_id>/invalid/`

콘솔에는 아래 지표가 출력된다.

- `input_row_count`
- `validated_row_count`
- `deduplicated_row_count`
- `duplicate_row_count`
- `invalid_row_count`

invalid row가 존재하면 `reject_reason` 집계도 함께 출력한다.

## Current Executable Scope

현재 로컬 실행에서 실제로 동작하는 범위는 아래와 같다.

- CSV read
- 원천 이벤트 정규화
- validation / DLQ 분리
- deduplication
- parquet write

아직 미구현 상태인 항목:

- sessionization
- session_state_snapshot
- Hive partition 등록
- WAU 실제 집계 연결
- staging -> final promote 정식 흐름

## Notes

- `2019-Oct.csv` 단일 파일도 로컬에서 매우 크기 때문에, 전체 parquet write까지 수행하면 디스크 부족이 발생할 수 있다.
- 실제로 `2019-Oct.csv` 스모크 실행 시 코드 경로는 정상적으로 통과했지만, write 단계에서 `No space left on device`가 발생했다.
- 따라서 과제 검증 단계에서는 먼저 단위 테스트와 작은 범위 스모크를 수행하고, 전체 데이터 실행은 충분한 디스크 공간이 있을 때 수행하는 것이 안전하다.
- 현재 `build.sbt`는 로컬 `sbt run` 검증을 위해 Spark dependency를 runtime classpath에 포함하도록 설정되어 있다.
- 추후 실제 클러스터 배포형 `spark-submit`을 사용할 경우에는 packaging 전략을 별도로 정리할 필요가 있다.

## Design Summary

- 내부 계산은 UTC 기준으로 수행한다.
- 분석 및 partition 기준은 KST를 사용한다.
- 원본 `user_session`은 보존하고 분석용 `session_id`는 새로 생성한다.
- 세션 ID는 `user_id + session_start_time_utc` 기반으로 결정한다.
- 결과는 staging 경로에 먼저 쓰고 검증 통과 후 final partition 경로에 반영한다.

## Current Status

- [x] sbt 프로젝트 스캐폴드 생성
- [x] 기본 엔트리포인트 추가
- [x] README 초안 작성
- [x] 패키지 구조 및 SQL 리소스 골격 추가
- [x] 원천 이벤트 정규화 및 validation/DLQ 분리 기본 구현
- [x] Deduplication 기본 구현
- [ ] Sessionization 구현
- [ ] Hive external table DDL 작성
- [ ] WAU 쿼리 및 검증 결과 정리
