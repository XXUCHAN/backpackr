# 제출용 1페이지 요약

## 1. 과제 목표

Kaggle Ecommerce Activity 로그를 Spark 기반 ETL로 처리하여 다음 산출물을 생성한다.

- KST 기준 partitioned parquet dataset
- Hive external table
- 5분 inactivity 기준 세션 재생성 결과
- WAU 및 Weekly Active Sessions 집계 결과

## 2. 핵심 설계

- 내부 시간 계산은 UTC, 분석과 partition은 KST 기준으로 분리
- `user_session`은 원본 보존용으로 남기고, 분석용 `session_id`는 Spark에서 재생성
- validation / DLQ, exact dedup, sessionization을 순차 적용
- `D-1` snapshot seed를 사용해 날짜 경계 세션을 이어붙임
- 결과는 staging에 먼저 쓰고, 검증 후 final partition으로 promote
- Hive external table은 final parquet를 참조하고 partition 단위로 등록

## 3. 구현 범위

- UTC -> KST 정규화
- validation / DLQ 분리
- exact deduplication
- 5분 inactivity 기반 sessionization
- session snapshot 저장 및 재사용
- staging -> final promote
- promote rollback / retry
- Hive external table 생성 / partition 등록
- WAU / Weekly Active Sessions 실행 및 저장
- preflight validation / quality gate / batch run log

## 4. 운영 고려사항

- `PreflightValidator`로 input path, 날짜 범위, run path 충돌을 사전 차단
- `QualityGate`로 DLQ 비율과 output row count를 검사
- `BatchRunLogger`는 상태 이력을 append JSON 배열로 기록
- promote 실패 시 backup / work 디렉터리 기반 rollback 및 retry 수행
- late arrival과 cross-boundary duplicate는 별도 상태 저장소 대신 `D-1 ~ D` 재처리 정책으로 대응

## 5. 실제 검증 결과

실데이터 `2019-10-01 ~ 2019-10-15` 실행 결과:

- input rows: `42,448,764`
- validated rows: `19,962,845`
- sessionized rows: `19,950,269`
- unique sessions: `4,369,219`
- invalid rows: `0`
- promoted partitions: `15`

WAU:

- `2019-09-30`: `818,388`
- `2019-10-07`: `1,057,958`
- `2019-10-14`: `393,290`

Weekly Active Sessions:

- `2019-09-30`: `1,570,536`
- `2019-10-07`: `2,153,262`
- `2019-10-14`: `645,421`

## 6. 현재 범위와 향후 확장

현재 구현은 과제 범위에 맞춰 `D-1` snapshot seed와 날짜 범위 재실행까지 지원한다.  
다중 일자 backfill orchestration 자동화는 구현하지 않았으며, 운영 환경에서는 Airflow 등에서 cascade recalculation으로 확장할 계획이다.

추가 확장 포인트:

- infinite bot session 방지용 hard session cap
- schema evolution 대응
- late arrival 자동 lookback orchestration
