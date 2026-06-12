# `src/pipeline/` — Spark 전처리 실행 가이드

HDFS 의 raw CSV 를 읽어 **영문→한글 컬럼 표준화 + dedup + wide→long 변환** 후
년/월 파티션 Parquet 으로 저장한다.

| 파일 | 입력 (HDFS) | 출력 (HDFS) |
|---|---|---|
| `preprocess_subway.py` | `/user/maria_dev/seoul/raw/subway/*.csv` | `/user/maria_dev/seoul/processed/subway/` |
| `preprocess_bus.py` | `/user/maria_dev/seoul/raw/bus/*.csv` | `/user/maria_dev/seoul/processed/bus/` |

## 1. 사전 조건

- HDFS 적재 완료 (루트 README §4-1, 명령은 [`data/README.md`](../../data/README.md) 동일)
- 셸 UTF-8 환경 설정 (루트 README §4-0)

## 2. 실행 (HDP Sandbox, `maria_dev`)

repo 루트에서:

```bash
spark-submit --master yarn --deploy-mode client \
             --num-executors 2 --executor-memory 1g \
             --driver-memory 2g \
             src/pipeline/preprocess_subway.py

spark-submit --master yarn --deploy-mode client \
             --num-executors 2 --executor-memory 1g \
             --driver-memory 2g \
             src/pipeline/preprocess_bus.py
```

> `--driver-memory 2g` 는 필수 — 기본값으로 실행하면 YARN listener heap 부족으로
> `OutOfMemoryError` 가 발생할 수 있다.

## 3. 정상 출력 기준 (Sandbox 실측값)

| 항목 | subway | bus |
|---|---|---|
| 소요 시간 | 약 5분 30초 | 약 24분 30초 |
| 입력 행 수 (월 단위 wide) | 11,178 | 약 757,500 |
| dedup 후 | 10,558 (202603 중복 620행 제거) | 변화 거의 없음 |
| 출력 파티션 | `년=YYYY/월=MM/` | 동일 |

스크립트 로그 마지막에 입력/출력 행 수와 Parquet 경로가 출력되고 종료 코드 0 이면 정상.

### 완료 검증

```bash
hdfs dfs -du -h /user/maria_dev/seoul/processed
# subway + bus 합계 약 668.5 MB

hdfs dfs -ls /user/maria_dev/seoul/processed/subway
# 년=2024  년=2025  년=2026  파티션 디렉토리 존재
```

## 4. 처리 내용 (두 스크립트 공통 구조)

1. raw CSV 로드 (UTF-8, header)
2. 메타키 기준 `dropDuplicates` — subway 202603 의 전체 행 2배 중복(서울시 API 이상치) 대응
3. 영문 컬럼명 → 공식 한글 명칭 rename (스키마 매핑은 [`data/README.md`](../../data/README.md) §4)
4. 시간대 48컬럼 wide→long 변환 → `시간`(0~23) + 승/하차 인원
   - bus 는 `HR_1` 만 `_NOPE` 접미사인 inconsistency 가 있어 정규식 `HR_(\d+)_GET_(ON|OFF)_T?NOPE` 로 통합 매칭
5. `년`/`월` 파티션 Parquet 저장

## 5. 트러블슈팅

| 증상 | 원인 | 조치 |
|---|---|---|
| `SyntaxError: Non-ASCII character` | Python 2.7 PEP-0263 | 스크립트에 `# -*- coding: utf-8 -*-` 헤더 **이미 포함** — 수정 불필요 |
| `UnicodeEncodeError: 'ascii' codec` | Python 2 stdout 이 ASCII | UTF-8 stdout writer **이미 포함** — 수정 불필요 |
| `OutOfMemoryError` (driver) | YARN listener heap | §2 명령의 `--driver-memory 2g` 그대로 사용 |
| 터미널 한글 깨짐 (`?` 표시) | PuTTY charset 미설정 | Window → Translation → Remote character set `UTF-8` |
| `Input path does not exist` | HDFS 적재 누락 | 루트 README §4-1 적재 후 재실행 |
