# 서울시 지하철 vs 버스 — 권역 반경 변화에 따른 수단 선호 분석

빅데이터 프로그래밍 기말 프로젝트 · **60221303 김세창**

지하철역 반경(500m/200m) 권역으로 지하철·버스 이용량을 결합해, **"두 수단이 모두 있을 때 시민은 무엇을, 왜, 어떻게 선호하는가"** 를 18개월(2024.12~2026.05) 승하차 데이터로 분석한다.

## 1. 분석 질문과 핵심 결과

| # | 질문 | 결과 |
|---|---|---|
| **Q1** | 두 수단이 모두 있을 때 무엇을 선호하는가? | **가까울수록 지하철 압승** — subway_share 500m 권역 53.5% → 200m 권역 74.4% (+20.9%p) |
| **Q2** | 무엇이 그 선호를 결정하는가? | **시간대 + 호선 유형** — 출퇴근 시간대 80~83%(200m), 공항철도 76%+ vs 경전철 48% |
| **Q3** | 두 수단은 어떻게 소비되는가? | **지하철 = 출퇴근, 버스 = 라스트 마일** — Peak 6h 집중도 지하철 46% > 버스 40%, Top-15 버스 정류장 = 100% 지하철 환승 거점 (보완재 관계) |

## 2. 시스템 아키텍처

```
[1] Python 수집        서울 열린데이터광장 OpenAPI 4종, 월별 분할 수집 (278MB)
        ↓
[2] HDFS 적재          /user/maria_dev/seoul/raw/  (CSV 38개)
        ↓
[3] Spark 전처리       영문→한글 컬럼 표준화, dedup, wide→long, Parquet 668.5MB (년/월 파티션)
        ↓
[4] Spark SQL 분석     Haversine+BBox 권역 매칭 (500m: 394권역 / 200m: 385권역), Q1/Q2/Q3
        ↓
[5] Plotly 시각화      히스토그램·히트맵·라인차트 등 6종
```

| 도구 | 역할 | 강의 스택 |
|---|---|---|
| Python | 데이터 수집 | - |
| **HDFS** | 분산 저장 (raw 278MB + Parquet 668.5MB) | ✅ |
| **Apache Spark** | 전처리 + Spark SQL 분석 | ✅ |
| Plotly | 시각화 | - |

## 3. 데이터 (repo 동봉 — API 키 불필요)

수집 원본 CSV 전체를 repo 에 동봉했다. 출처·스키마·재적재 명령은 **[`data/README.md`](data/README.md)** 참조.

| 경로 | 내용 |
|---|---|
| `data/raw/` | 시간대별 승하차 CSV 36개 (subway 18 + bus 18, 월별) |
| `data/coords/` | 지하철역 좌표 784건 + 버스정류장 좌표 11,253건 |

- 출처: 서울 열린데이터광장 (OA-12913, OA-12252, OA-21232, OA-15067)
- 규모: raw 278MB (≥100MB 요구 충족), 전처리 후 subway 약 26.9만 행 / bus 약 1,680만 행 (시간 단위)

## 4. 실행 방법 (HDP 3.0.1 Sandbox)

### 4-0. 사전 준비
HDP Sandbox 에 `maria_dev` 로 SSH 접속 후 UTF-8 환경 설정:
```bash
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
export HADOOP_CLIENT_OPTS="-Dfile.encoding=UTF-8"
```

### 4-1. HDFS 적재
repo 의 동봉 데이터를 그대로 올린다:
```bash
hdfs dfs -mkdir -p /user/maria_dev/seoul/raw/subway \
                   /user/maria_dev/seoul/raw/bus \
                   /user/maria_dev/seoul/raw/coords
hdfs dfs -put data/raw/subway_*.csv /user/maria_dev/seoul/raw/subway/
hdfs dfs -put data/raw/bus_*.csv    /user/maria_dev/seoul/raw/bus/
hdfs dfs -put data/coords/*.csv     /user/maria_dev/seoul/raw/coords/
```

### 4-2. Spark 전처리 (CSV → Parquet)
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
→ `/user/maria_dev/seoul/processed/{subway,bus}/` 에 년/월 파티션 Parquet 생성

### 4-3. 분석 노트북 실행
Zeppelin(`http://localhost:9995`, 인터프리터 `spark2`) 또는 pyspark 셸에서
**`src/analyze/q1_q2_q3_analysis.ipynb`** 를 셀 순서대로 실행한다.
```bash
# pyspark 셸 사용 시
pyspark --master yarn --deploy-mode client --driver-memory 2g
```
- 데이터 경로는 `DATA_BASE` 환경변수로 변경 가능 (기본값 `/user/maria_dev/seoul`)
- 상세 가이드: [`src/analyze/README.md`](src/analyze/README.md)

### (선택) 데이터 재수집
동봉 데이터 대신 직접 수집하려면 `.env` 에 인증키 설정 후 `src/ingest/fetch_seoul_data.ipynb` 실행 (`.env.example` 참조):
```bash
pip install -r requirements.txt
```

## 5. Repository 구조

```
BP_clone/
├── README.md                            # 본 문서
├── requirements.txt
├── .env.example                         # (선택 재수집용) API 키 템플릿
├── data/
│   ├── README.md                        # 데이터 출처·스키마·재적재 명령
│   ├── raw/                             # 월별 시간대 CSV 36개 (동봉)
│   └── coords/                          # 좌표 마스터 CSV 2개 (동봉)
└── src/
    ├── ingest/fetch_seoul_data.ipynb    # OpenAPI 분할 수집
    ├── pipeline/preprocess_subway.py    # Spark 전처리 (지하철)
    ├── pipeline/preprocess_bus.py       # Spark 전처리 (버스)
    └── analyze/q1_q2_q3_analysis.ipynb  # Spark SQL 분석 + Plotly 시각화
```

## 6. 주요 기술 이슈와 해결

| 이슈 | 해결 |
|---|---|
| Hive metastore charset(latin1)이 한글 컬럼명 미지원 | Hive 테이블 대신 **Spark SQL temp view** 로 분석 (강의 스택 2개 기준은 HDFS+Spark 로 충족) |
| `subway_202603` 전체 행 정확히 2배 중복 (서울시 API 이상치) | 메타키 기준 `dropDuplicates` |
| bus 데이터 `HR_1`만 `_NOPE`, 나머지 `_TNOPE` 접미사 | 정규식 `HR_(\d+)_GET_(ON\|OFF)_T?NOPE` 통합 매칭 |
| Sandbox Python 2.7 한글 출력 오류 | PEP-0263 헤더 + UTF-8 stdout writer |

## 7. AI Tool Usage

- Claude Code: Spark 전처리·분석 코드 디버깅(Python 2 인코딩, YARN OOM), repo 구조 정리 및 README/데이터 문서 작성 보조
- 분석 설계(권역 반경 비교 관점)·결과 해석·보고서 본문은 직접 작성

## 8. 참고 자료

- 서울 열린데이터광장: [지하철 승하차 OA-12913](https://data.seoul.go.kr/dataList/OA-12913/S/1/datasetView.do) · [버스 승하차 OA-12252](https://data.seoul.go.kr/dataList/OA-12252/S/1/datasetView.do) · [역 좌표 OA-21232](https://data.seoul.go.kr/dataList/OA-21232/S/1/datasetView.do) · [정류장 좌표 OA-15067](https://data.seoul.go.kr/dataList/OA-15067/S/1/datasetView.do)
- Apache Spark 2.x 공식 문서: https://spark.apache.org/docs/2.4.0/
- Apache Hadoop (HDFS): https://hadoop.apache.org/
