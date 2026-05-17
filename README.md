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
    │       └── com/
    │           └── ecommerce/
    │               └── activity/
    │                   ├── ActivityBatchApp.scala
    │                   ├── config/
    │                   ├── logging/
    │                   ├── model/
    │                   ├── query/
    │                   ├── reader/
    │                   ├── schema/
    │                   ├── state/
    │                   ├── support/
    │                   ├── transform/
    │                   └── writer/
    └── test/
        ├── resources/
        └── scala/
            └── com/
                └── ecommerce/
                    └── activity/
                        ├── smoke/
                        └── support/
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

## Execution Plan

애플리케이션은 최종적으로 아래와 같은 파라미터 기반 배치를 목표로 한다.

```bash
sbt package

spark-submit \
  --class com.ecommerce.activity.ActivityBatchApp \
  target/scala-2.12/activity-etl-wau_2.12-0.1.0-SNAPSHOT.jar \
  --mode daily \
  --start-date 2019-10-01 \
  --end-date 2019-10-02 \
  --input-path /data/raw \
  --output-base-path /warehouse/activity_events \
  --staging-base-path /warehouse/activity_events_staging \
  --dlq-base-path /warehouse/activity_events_dlq
```

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
- [ ] Spark ETL 구현
- [ ] Hive external table DDL 작성
- [ ] WAU 쿼리 및 검증 결과 정리
