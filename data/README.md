# `data/` — 데이터 출처 및 스키마

대용량 raw 데이터는 `.gitignore` 로 제외한다 (`data/raw/`, `data/processed/`, `*.csv` 등).
샘플 데이터(100~1000줄)만 `data/sample/` 에 커밋을 허용한다.

## 1. 데이터 출처 (서울 열린데이터광장 OpenAPI 4종)

| # | 데이터셋 | 서비스명 | 데이터셋 ID | 용도 |
|---|---|---|---|---|
| 1 | 지하철호선별 역별 시간대별 승하차 인원 | `CardSubwayTime` | [OA-12913](https://data.seoul.go.kr/dataList/OA-12913/S/1/datasetView.do) | 지하철 시간대별 이용량 |
| 2 | 버스노선별 정류장별 시간대별 승하차 인원 | `CardBusTimeNew` | [OA-12252](https://data.seoul.go.kr/dataList/OA-12252/S/1/datasetView.do) | 버스 시간대별 이용량 |
| 3 | 지하철역 마스터 (역 좌표) | `subwayStationMaster` | [OA-21232](https://data.seoul.go.kr/dataList/OA-21232/S/1/datasetView.do) | 역 위경도 (권역 매칭) |
| 4 | 버스정류장 위치 (정류장 좌표) | `busStopLocationXyInfo` | [OA-15067](https://data.seoul.go.kr/dataList/OA-15067/S/1/datasetView.do) | 정류장 위경도 (권역 매칭) |

- ①②는 `src/ingest/fetch_seoul_data.ipynb` 로 월별 분할 수집 (인증키: `.env` 의 `SEOUL_API_KEY_1`, `SEOUL_API_KEY_2`)
- ③④는 OpenAPI 에서 CSV 1회 다운로드 후 HDFS 에 직접 적재 (변동 거의 없는 마스터 데이터)

## 2. 수집 규모

| 항목 | 값 |
|---|---|
| 기간 | 2024.12 ~ 2026.05 (18개월) |
| 파일 수 | 36개 (subway 18 + bus 18) |
| 누적 용량 | 275.8 MB (raw CSV, ≥100MB 요구사항 충족) |
| 전처리 후 | 668.5 MB (Parquet, 년/월 파티션) |
| 인코딩 | UTF-8 |

## 3. HDFS 경로 (Sandbox HDP)

```
/user/maria_dev/seoul/
├── raw/
│   ├── subway/                 # subway_YYYYMM.csv × 18
│   ├── bus/                    # bus_YYYYMM.csv × 18
│   └── coords/
│       ├── subway_stations_xy.csv
│       └── bus_stops_xy.csv
└── processed/
    ├── subway/                 # Parquet, 년=YYYY/월=MM/ 파티션
    └── bus/                    # Parquet, 년=YYYY/월=MM/ 파티션
```

## 4. 스키마

### 4.1 지하철 시간대별 (raw → processed)

raw CSV 는 영문 컬럼명이며, 전처리(`src/pipeline/preprocess_subway.py`)에서
공식 한글 명칭으로 rename + wide→long 변환한다.

| raw (영문) | processed (한글) | 타입 | 예시 |
|---|---|---|---|
| `USE_MM` | `사용월` | STRING | "202412" |
| `SBWY_ROUT_LN_NM` | `호선명` | STRING | "1호선", "경의선" |
| `STTN` | `지하철역` | STRING | "서울역" |
| `JOB_YMD` | `작업일자` | STRING | "20250103" |
| `HR_{n}_GET_ON_NOPE` (48개) | `시간` + `승차인원` | INT + BIGINT | 0~23 |
| `HR_{n}_GET_OFF_NOPE` | `시간` + `하차인원` | INT + BIGINT | |
| - | `년`, `월` (파티션) | INT | 2024, 12 |

### 4.2 버스 시간대별 (raw → processed)

전처리: `src/pipeline/preprocess_bus.py`

| raw (영문) | processed (한글) | 타입 |
|---|---|---|
| `USE_YM` | `사용년월` | STRING |
| `RTE_NO` | `노선번호` | STRING |
| `RTE_NM` | `노선명` | STRING |
| `STOPS_ID` | `표준버스정류장ID` | STRING |
| `STOPS_ARS_NO` | `버스정류장ARS번호` | STRING |
| `SBWY_STNS_NM` | `역명` | STRING |
| `TRFC_MNS_TYPE_CD` | `교통수단타입코드` | STRING |
| `TRFC_MNS_TYPE_NM` | `교통수단타입명` | STRING |
| `REG_YMD` | `등록일자` | STRING |
| `HR_{n}_GET_(ON\|OFF)_T?NOPE` (48개) | `시간` + `승차총승객수`/`하차총승객수` | INT + BIGINT |
| - | `년`, `월` (파티션) | INT |

> ⚠ raw 데이터 inconsistency: `HR_1` 만 `_NOPE` 접미사, 나머지는 `_TNOPE`
> → 정규식 `HR_(\d+)_GET_(ON|OFF)_T?NOPE` 로 통합 매칭.

### 4.3 지하철역 좌표 (`coords/subway_stations_xy.csv`)

| 컬럼 | 설명 |
|---|---|
| `BLDN_ID` | 역 ID |
| `BLDN_NM` | 역명 (괄호 부역명 포함) |
| `ROUTE` | 호선명 |
| `LAT` / `LOT` | 위도 / 경도 |

### 4.4 버스정류장 좌표 (`coords/bus_stops_xy.csv`)

| 컬럼 | 설명 |
|---|---|
| `STOPS_NO` | 표준버스정류장ID (시간대 데이터와 JOIN 키) |
| `NODE_ID` | 노드 ID |
| `STOPS_TYPE` | 정류장 유형 (분석 시 "한강선착장" 제외) |
| `XCRD` / `YCRD` | 경도 / 위도 |

## 5. 데이터 품질 이슈 (전처리에서 해결)

| 이슈 | 처리 |
|---|---|
| subway 202603 전체 행 정확히 2배 중복 | 메타키 `dropDuplicates` |
| bus `HR_1` 접미사 inconsistency | 정규식 통합 매칭 |
| 한글 인코딩 | UTF-8 확인 (CP949 아님) |
