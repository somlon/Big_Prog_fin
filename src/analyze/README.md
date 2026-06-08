# `src/analyze/` — 분석 노트북

본 디렉토리는 **Q1/Q2/Q3** 분석과 **시각화** 를 담은 노트북·스크립트를 보관한다.

## 파일

| 파일 | 역할 |
|---|---|
| `q1_q2_q3_analysis.ipynb` | **Sandbox 용** — 500m 와 200m 두 권역에서 Q1/Q2/Q3 분석 + Plotly 시각화 (64셀) |
| `_build_notebook.py` | 위 노트북을 생성하는 단일 소스 스크립트 (셀 추가·수정 시 사용) |
| `q1_q2_q3_analysis_local.ipynb` | **로컬 PySpark/Jupyter 용** — Sandbox 노트북 + DATA_BASE 자동 감지 + Folium 지도 시각화 §13 (72셀) |
| `_build_notebook_local.py` | 위 노트북 생성 스크립트 (`_build_notebook.py` 의 cells 를 import 한 뒤 셀 0/1/2/4/6 교체 + §13 folium 셀 append) |
| `README.md` | 본 문서 |

## 실행 환경

### 로컬 PySpark
```bash
pip install pyspark==2.4.x pandas plotly
DATA_BASE=/path/to/local/data jupyter notebook q1_q2_q3_analysis.ipynb
```

### Sandbox HDP 3.0
```bash
# Zeppelin 또는 sandbox pyspark 셸에서 실행 (DATA_BASE 기본값 /user/maria_dev/seoul 사용)
pyspark --master yarn --deploy-mode client --driver-memory 2g
```

### `DATA_BASE` 환경변수
노트북은 `DATA_BASE` 환경변수로 데이터 루트 경로를 받는다.

| 경로 | 기본값 |
|---|---|
| Parquet (subway/bus) | `${DATA_BASE}/processed/{subway,bus}` |
| 좌표 CSV | `${DATA_BASE}/raw/coords/{bus_stops_xy,subway_stations_xy}.csv` |

기본값: `/user/maria_dev/seoul` (Sandbox HDFS).

## 분석 구조

1. 환경 설정 (SparkSession, UTF-8 fix)
2. Parquet 로드 (`subway_hourly`, `bus_hourly`)
3. 좌표 CSV 로드 (`bus_coord`, `sub_coord`)
4. 정규화 + INNER JOIN → `subway_geo`, `bus_geo`
5. 권역 매칭 함수 `match_zones(R_KM)` (Haversine + BBox)
6. 역별/정류장별 총 이용량 (`sub_total`, `bus_total`)
7. `zone_stats` / `zone_typed` 빌더 함수
8. **500m 분석** — Q1 / Q2 (시간대, 방향, 월별) / Q3 (peak, Top-N)
9. **200m 분석** — 동일 구조
10. **500m vs 200m 비교**
11. **시각화** — Plotly 6개 차트
    - Q1 권역 share 분포 히스토그램
    - Q1 호선 유형별 bar chart
    - Q2 시간대 × 호선유형 히트맵
    - Q3 시간대별 절대량 line chart
    - Q3 Peak 집중도 비교
    - Q3 Top-N 지하철역 / 버스정류장 horizontal bar
12. **결론**
13. **Folium 지도 시각화** (로컬 노트북 전용) — 3개 지도
    - 13-1. Top-15 지하철역 마커 지도 (이용량 = 크기, 호선 = 색상)
    - 13-2. 200m 권역 매칭 예시 지도 (대표 역 3곳 + 매칭된 버스정류장)
    - 13-3. 200m 권역별 subway_share 지도 (지하철역 위치에 share 색상 마커)

## 노트북 재생성

### Sandbox 노트북 (`q1_q2_q3_analysis.ipynb`)
```bash
python3 src/analyze/_build_notebook.py
```

### 로컬 노트북 (`q1_q2_q3_analysis_local.ipynb`)
```bash
python3 src/analyze/_build_notebook_local.py
```
(`_build_notebook.py` 를 내부에서 import 하므로 두 노트북이 함께 재생성됨.)
