#!/usr/bin/env python3
"""
Build q1_q2_q3_analysis.ipynb (sibling file) from cell definitions.

Generates a Jupyter Notebook (JSON) that runs all Q1/Q2/Q3 analyses for both
500m and 200m radius zones, plus Plotly visualization.

Usage:
    python3 src/analyze/_build_notebook.py
"""
import json
from pathlib import Path

NB_PATH = Path(__file__).parent / "q1_q2_q3_analysis.ipynb"


def md(*lines):
    """Markdown cell."""
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": [line + "\n" for line in lines[:-1]] + [lines[-1]] if lines else [""],
    }


def code(*lines):
    """Code cell."""
    src = [line + "\n" for line in lines[:-1]] + [lines[-1]] if lines else [""]
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": src,
    }


cells = []

# ============================================================
# 0. Header
# ============================================================
cells.append(md(
    "# 서울시 지하철·버스 이용 패턴 비교 분석 (Q1/Q2/Q3)",
    "",
    "**주제**: 권역(역+호선)을 중심으로 반경 500m 와 200m 두 가지 기준에서 시민의 수단 선호와 이용 패턴을 비교 분석한다.",
    "",
    "## 데이터 소스",
    "- 지하철·버스 시간대별 승하차: 서울 OpenAPI `CardSubwayTime`, `CardBusTimeNew` (18개월, 2024.12 ~ 2026.05)",
    "- 지하철역 좌표: 서울 OpenAPI `subwayStationMaster` (OA-21232)",
    "- 버스정류소 좌표: 서울 OpenAPI `busStopLocationXyInfo` (OA-15067)",
    "",
    "## 분석 질문",
    "| # | 질문 |",
    "|---|---|",
    "| Q1 | 두 교통수단이 모두 존재하는 권역에서 시민은 어떤 수단을 더 선호하는가? |",
    "| Q2 | 무엇에 따라 선호하는 교통수단이 달라지는가? (시간대·호선 유형·계절·방향) |",
    "| Q3 | 두 교통수단은 어떤 방식으로 소비되는가? (피크 패턴·Top-N·집중도) |",
    "",
    "## 실행 환경",
    "- Sandbox HDP 3.0.1 (Spark2 + YARN + HDFS)",
    "- 또는 로컬 PySpark + 로컬 Parquet/CSV 파일 (`DATA_BASE` 환경변수로 경로 변경 가능)",
    "",
    "## 산출물",
    "- 권역 단위 `subway_share`(%) 분포",
    "- 호선 유형별 / 시간대별 / 방향별 / 계절별 share",
    "- 시간대 절대 이용량 + Peak 집중도",
    "- Top-N 지하철역/버스정류장",
    "- Plotly 인터랙티브 시각화 차트",
))

# ============================================================
# 1. Setup
# ============================================================
cells.append(md(
    "## 1. 환경 설정",
    "",
    "SparkSession 생성 + Python 2 한글 출력 fix + 데이터 경로 설정.",
))

cells.append(code(
    "# Python 2 한글 출력 fix (Sandbox HDP 환경)",
    "import sys",
    "if sys.version_info[0] == 2:",
    "    reload(sys)",
    "    sys.setdefaultencoding('utf-8')",
    "",
    "import os",
    "from pyspark.sql import SparkSession",
    "from pyspark.sql.functions import (",
    "    col, regexp_replace, when, sin, cos, asin, sqrt, radians, broadcast,",
    "    lit, sum as F_sum, count as F_count, avg as F_avg, max as F_max, min as F_min,",
    "    percentile_approx, stddev as F_stddev, round as F_round,",
    ")",
    "from pyspark.sql.types import StringType",
    "",
    "# Spark session",
    "spark = (",
    "    SparkSession.builder",
    "    .appName('BP_Q1Q2Q3_500m_200m')",
    "    .config('spark.sql.shuffle.partitions', '200')",
    "    .getOrCreate()",
    ")",
    "spark.sparkContext.setLogLevel('WARN')",
    "",
    "# 데이터 경로 — Sandbox HDFS 또는 로컬 디렉토리",
    "# 로컬 실행 시 DATA_BASE 환경변수로 경로 지정",
    "DATA_BASE = os.environ.get('DATA_BASE', '/user/maria_dev/seoul')",
    "PARQUET_SUBWAY = '{}/processed/subway'.format(DATA_BASE)",
    "PARQUET_BUS    = '{}/processed/bus'.format(DATA_BASE)",
    "CSV_BUS_COORD  = '{}/raw/coords/bus_stops_xy.csv'.format(DATA_BASE)",
    "CSV_SUB_COORD  = '{}/raw/coords/subway_stations_xy.csv'.format(DATA_BASE)",
    "",
    "print('Spark version :', spark.version)",
    "print('DATA_BASE     :', DATA_BASE)",
))

# ============================================================
# 2. Data Load - Parquet
# ============================================================
cells.append(md(
    "## 2. 시간대별 이용량 Parquet 로드 (`subway_hourly`, `bus_hourly`)",
    "",
    "Spark 전처리 단계의 결과인 long-format Parquet 을 불러온다.",
    "",
    "**스키마**",
    "- subway: `사용월, 호선명, 지하철역, 작업일자, 시간, 승차인원, 하차인원, 년, 월`",
    "- bus: `사용년월, 노선번호, 노선명, 표준버스정류장ID, 버스정류장ARS번호, 역명, 교통수단타입코드/명, 등록일자, 시간, 승차총승객수, 하차총승객수, 년, 월`",
))

cells.append(code(
    "subway = spark.read.parquet(PARQUET_SUBWAY)",
    "bus    = spark.read.parquet(PARQUET_BUS)",
    "",
    "subway.createOrReplaceTempView('subway_hourly')",
    "bus.createOrReplaceTempView('bus_hourly')",
    "",
    "print('Subway:', subway.count(), 'rows')",
    "print('Bus   :', bus.count(), 'rows')",
    "subway.printSchema()",
    "bus.printSchema()",
))

# ============================================================
# 3. Coordinate Data Load - CSV
# ============================================================
cells.append(md(
    "## 3. 좌표 데이터 CSV 로드 (`bus_coord`, `sub_coord`)",
    "",
    "- `bus_stops_xy.csv` (서울 OpenAPI OA-15067): 정류장ID·정류장명·X/Y좌표(WGS84)·STOPS_TYPE",
    "- `subway_stations_xy.csv` (OA-21232): 역사ID·역사명·호선·위도/경도",
    "",
    "ID 컬럼은 `inferSchema` 가 integer 로 추론하므로 `StringType()` 으로 cast 한다 (zero-padding 유지·기존 데이터와 JOIN 타입 정합).",
))

cells.append(code(
    "bus_coord = (",
    "    spark.read.csv(CSV_BUS_COORD, header=True, inferSchema=True, encoding='UTF-8')",
    "    .withColumn('STOPS_NO', col('STOPS_NO').cast(StringType()))",
    "    .withColumn('NODE_ID',  col('NODE_ID').cast(StringType()))",
    ")",
    "sub_coord = (",
    "    spark.read.csv(CSV_SUB_COORD, header=True, inferSchema=True, encoding='UTF-8')",
    "    .withColumn('BLDN_ID', col('BLDN_ID').cast(StringType()))",
    ")",
    "",
    "bus_coord.createOrReplaceTempView('bus_coord')",
    "sub_coord.createOrReplaceTempView('sub_coord')",
    "",
    "print('Bus coord  count:', bus_coord.count())",
    "print('Sub coord  count:', sub_coord.count())",
    "bus_coord.printSchema()",
    "sub_coord.printSchema()",
))

cells.append(code(
    "# 버스 STOPS_TYPE 분포 (한강선착장은 분석에서 제외 예정)",
    "bus_coord.groupBy('STOPS_TYPE').count().orderBy(col('count').desc()).show()",
))

cells.append(code(
    "# 지하철 ROUTE 분포 (호선명 정규화 매핑에 활용)",
    "sub_coord.groupBy('ROUTE').count().orderBy(col('count').desc()).show(50, truncate=False)",
))

# ============================================================
# 4. Normalization + INNER JOIN
# ============================================================
cells.append(md(
    "## 4. 좌표 결합: `subway_geo` / `bus_geo`",
    "",
    "**지하철**: 역명을 `(.*)` 괄호 제거 + 호선명 매핑 (`경의선`→`경의중앙선`, `9호선2~3단계`→`9호선(연장)` 등) 후 좌표와 INNER JOIN. 매칭률 약 98.7%.",
    "",
    "**버스**: `STOPS_NO ↔ 표준버스정류장ID` 정확 매칭. 한강선착장(STOPS_TYPE='한강선착장') 제외. 매칭률 약 83%.",
))

cells.append(code(
    "# === 지하철 정규화 ===",
    "subway_normed = (",
    "    spark.table('subway_hourly')",
    "    .withColumn('sta_norm', regexp_replace(col('`지하철역`'), r'\\(.*\\)', ''))",
    "    .withColumn('line_norm',",
    "        when(col('`호선명`') == u'9호선2~3단계',  u'9호선(연장)')",
    "       .when(col('`호선명`') == u'경의선',         u'경의중앙선')",
    "       .when(col('`호선명`') == u'공항철도 1호선', u'공항철도1호선')",
    "       .otherwise(col('`호선명`'))",
    "    )",
    ")",
    "",
    "sub_coord_norm = (",
    "    sub_coord.select(",
    "        regexp_replace(col('BLDN_NM'), r'\\(.*\\)', '').alias('sta_norm'),",
    "        col('ROUTE').alias('line_norm'),",
    "        col('LAT'),",
    "        col('LOT'),",
    "    )",
    "    .dropDuplicates(['sta_norm', 'line_norm'])",
    ")",
    "",
    "subway_geo = (",
    "    subway_normed.join(",
    "        sub_coord_norm.select('sta_norm', 'line_norm', 'LAT', 'LOT'),",
    "        on=['sta_norm', 'line_norm'], how='inner'",
    "    )",
    "    .withColumnRenamed('LAT', 'sta_lat')",
    "    .withColumnRenamed('LOT', 'sta_lon')",
    "    .drop('sta_norm', 'line_norm')",
    ")",
    "subway_geo.cache()",
    "subway_geo.createOrReplaceTempView('subway_geo')",
    "",
    "# === 버스 한강선착장 제외 + JOIN ===",
    "bus_with_xy = (",
    "    bus_coord",
    "    .filter(col('STOPS_TYPE') != u'한강선착장')",
    "    .select(",
    "        col('STOPS_NO').alias('표준버스정류장ID'),",
    "        col('YCRD').alias('stop_lat'),",
    "        col('XCRD').alias('stop_lon'),",
    "        col('STOPS_TYPE'),",
    "    )",
    ")",
    "",
    "bus_geo = (",
    "    spark.table('bus_hourly').join(bus_with_xy, on='표준버스정류장ID', how='inner')",
    ")",
    "bus_geo.cache()",
    "bus_geo.createOrReplaceTempView('bus_geo')",
    "",
    "print('subway_geo rows:', subway_geo.count())",
    "print('bus_geo    rows:', bus_geo.count())",
))

# ============================================================
# 5. 권역 매칭 함수
# ============================================================
cells.append(md(
    "## 5. 권역 매칭 함수 (Haversine + BBox)",
    "",
    "**입력**: 반경 `R_KM`",
    "**출력**: 지하철역(역+호선) ↔ 반경 안 버스정류장 쌍 DataFrame `(station, line, stop_id, sta_lat, sta_lon, stop_lat, stop_lon, dist_km)`",
    "",
    "BBox 사전 필터링으로 cross join 비용을 줄인 뒤 Haversine 정확 거리 계산.",
))

cells.append(code(
    "def match_zones(R_KM):",
    "    \"\"\"반경 R_KM 안 (지하철역, 버스정류장) 쌍을 반환.\"\"\"",
    "    sub_loc = subway_geo.select(",
    "        col('`지하철역`').alias('station'),",
    "        col('`호선명`').alias('line'),",
    "        'sta_lat', 'sta_lon',",
    "    ).distinct()",
    "    ",
    "    bus_loc = bus_geo.select(",
    "        col('`표준버스정류장ID`').alias('stop_id'),",
    "        'stop_lat', 'stop_lon',",
    "    ).distinct()",
    "    ",
    "    # BBox tolerance (한국 위도 37도 기준)",
    "    LAT_TOL = R_KM / 111.0",
    "    LON_TOL = R_KM / 88.7",
    "    ",
    "    candidate = bus_loc.crossJoin(broadcast(sub_loc)).filter(",
    "        (col('stop_lat') >= col('sta_lat') - LAT_TOL) &",
    "        (col('stop_lat') <= col('sta_lat') + LAT_TOL) &",
    "        (col('stop_lon') >= col('sta_lon') - LON_TOL) &",
    "        (col('stop_lon') <= col('sta_lon') + LON_TOL)",
    "    )",
    "    ",
    "    pairs = candidate.withColumn(",
    "        'dist_km',",
    "        2 * 6371 * asin(sqrt(",
    "            sin(radians(col('sta_lat') - col('stop_lat')) / 2) ** 2",
    "            + cos(radians(col('stop_lat'))) * cos(radians(col('sta_lat')))",
    "            * sin(radians(col('sta_lon') - col('stop_lon')) / 2) ** 2",
    "        ))",
    "    ).filter(col('dist_km') <= R_KM)",
    "    ",
    "    return pairs.cache()",
))

# ============================================================
# 6. 공통 집계 (역별 / 정류장별 총 이용량)
# ============================================================
cells.append(md(
    "## 6. 역별·정류장별 총 이용량 집계 (반경 무관)",
    "",
    "권역 share 계산에 필요한 공통 집계. 반경 변경 시에도 한 번만 계산.",
))

cells.append(code(
    "sub_total_df = spark.sql(\"\"\"",
    "    SELECT `지하철역` AS station, `호선명` AS line,",
    "           SUM(`승차인원` + `하차인원`) AS subway_total",
    "    FROM subway_geo GROUP BY `지하철역`, `호선명`",
    "\"\"\")",
    "sub_total_df.createOrReplaceTempView('sub_total')",
    "",
    "bus_total_df = spark.sql(\"\"\"",
    "    SELECT `표준버스정류장ID` AS stop_id,",
    "           SUM(`승차총승객수` + `하차총승객수`) AS bus_total",
    "    FROM bus_geo GROUP BY `표준버스정류장ID`",
    "\"\"\")",
    "bus_total_df.createOrReplaceTempView('bus_total')",
    "",
    "print('sub_total rows:', sub_total_df.count())",
    "print('bus_total rows:', bus_total_df.count())",
))

# ============================================================
# 7. zone_stats / zone_typed 빌더 함수
# ============================================================
cells.append(md(
    "## 7. zone_stats / zone_typed 빌더 함수",
    "",
    "주어진 `station_stop_pairs` DataFrame 으로 권역별 share 및 호선 유형 라벨링.",
))

cells.append(code(
    "LINE_TYPE_CASE = u\"\"\"",
    "    CASE",
    "        WHEN line IN ('1호선','2호선','3호선','4호선','5호선','6호선','7호선','8호선') THEN '서교공_1-8호선'",
    "        WHEN line IN ('9호선', '9호선2~3단계') THEN '9호선'",
    "        WHEN line IN ('경의선','경의중앙선','중앙선') THEN '경의중앙'",
    "        WHEN line IN ('공항철도 1호선','공항철도1호선') THEN '공항철도'",
    "        WHEN line IN ('우이신설선','신림선') THEN '경전철'",
    "        WHEN line IN ('분당선','수인선','신분당선','신분당선(연장)','신분당선(연장2)') THEN '강남광역'",
    "        WHEN line IN ('경부선','경인선','경원선','경춘선','경강선','일산선','과천선','안산선','장항선','서해선','별내선','진접선') THEN '수도권광역'",
    "        ELSE '기타'",
    "    END",
    "\"\"\"",
    "",
    "def build_zone_stats(pairs_df, suffix):",
    "    \"\"\"권역별 subway/bus 누적 + share. suffix='500' 또는 '200' 으로 view 이름 분리.\"\"\"",
    "    pairs_view = 'pairs_' + suffix",
    "    pairs_df.createOrReplaceTempView(pairs_view)",
    "    ",
    "    zone = spark.sql(\"\"\"",
    "        SELECT p.station, p.line, s.subway_total,",
    "               SUM(b.bus_total) AS bus_total,",
    "               COUNT(p.stop_id) AS n_stops_in_zone",
    "        FROM {} p",
    "        JOIN sub_total s ON p.station = s.station AND p.line = s.line",
    "        JOIN bus_total b ON p.stop_id = b.stop_id",
    "        GROUP BY p.station, p.line, s.subway_total",
    "    \"\"\".format(pairs_view))",
    "    ",
    "    zone = (",
    "        zone",
    "        .withColumn('total_in_zone', col('subway_total') + col('bus_total'))",
    "        .withColumn('subway_share_pct',",
    "            100.0 * col('subway_total') / (col('subway_total') + col('bus_total')))",
    "    )",
    "    zone.cache()",
    "    zone.createOrReplaceTempView('zone_stats_' + suffix)",
    "    ",
    "    typed = spark.sql(\"\"\"",
    "        SELECT *, {} AS line_type",
    "        FROM zone_stats_{}",
    "    \"\"\".format(LINE_TYPE_CASE, suffix))",
    "    typed.cache()",
    "    typed.createOrReplaceTempView('zone_typed_' + suffix)",
    "    ",
    "    return zone, typed",
))

# ============================================================
# 8. 500m 분석 실행
# ============================================================
cells.append(md(
    "## 8. 500m 권역 분석",
    "",
    "도시계획 표준 반경 500m (도보 약 7분) — 환승 가능 거리.",
))

cells.append(code(
    "pairs_500 = match_zones(0.5)",
    "print('500m pairs:', pairs_500.count())",
    "zone_stats_500, zone_typed_500 = build_zone_stats(pairs_500, '500')",
    "print('500m zones:', zone_stats_500.count())",
))

cells.append(md("### 8-1. Q1: 권역별 subway_share 분포"))
cells.append(code(
    "spark.sql(\"\"\"",
    "    SELECT COUNT(*) AS n_zones,",
    "           ROUND(AVG(subway_share_pct), 2) AS avg_share,",
    "           ROUND(PERCENTILE_APPROX(subway_share_pct, 0.25), 2) AS p25,",
    "           ROUND(PERCENTILE_APPROX(subway_share_pct, 0.5), 2)  AS median,",
    "           ROUND(PERCENTILE_APPROX(subway_share_pct, 0.75), 2) AS p75,",
    "           ROUND(MIN(subway_share_pct), 2) AS min_share,",
    "           ROUND(MAX(subway_share_pct), 2) AS max_share",
    "    FROM zone_stats_500",
    "\"\"\").show()",
))

cells.append(md("### 8-2. Q1: 호선 유형별 share (500m)"))
cells.append(code(
    "spark.sql(\"\"\"",
    "    SELECT line_type, COUNT(*) AS n_zones,",
    "           ROUND(AVG(subway_share_pct), 2) AS avg_share,",
    "           ROUND(SUM(subway_total) / 1000000.0, 1) AS sum_subway_M,",
    "           ROUND(SUM(bus_total) / 1000000.0, 1)    AS sum_bus_M,",
    "           ROUND(SUM(subway_total) * 100.0 / SUM(subway_total + bus_total), 2) AS overall_share",
    "    FROM zone_typed_500",
    "    GROUP BY line_type ORDER BY overall_share DESC",
    "\"\"\").show(truncate=False)",
))

cells.append(md("### 8-3. Q2: 시간대 × 호선유형 share (500m)"))
cells.append(code(
    "spark.sql(u\"\"\"",
    "    SELECT z.line_type, s.`시간` AS hr,",
    "           SUM(s.`승차인원` + s.`하차인원`) AS subway_h",
    "    FROM subway_geo s",
    "    JOIN (SELECT DISTINCT station, line, line_type FROM zone_typed_500) z",
    "      ON s.`지하철역` = z.station AND s.`호선명` = z.line",
    "    GROUP BY z.line_type, s.`시간`",
    "\"\"\").createOrReplaceTempView('sub_hr_type_500')",
    "",
    "spark.sql(u\"\"\"",
    "    SELECT z.line_type, b.`시간` AS hr,",
    "           SUM(b.`승차총승객수` + b.`하차총승객수`) AS bus_h",
    "    FROM bus_geo b",
    "    JOIN pairs_500 p ON b.`표준버스정류장ID` = p.stop_id",
    "    JOIN (SELECT DISTINCT station, line, line_type FROM zone_typed_500) z",
    "      ON p.station = z.station AND p.line = z.line",
    "    GROUP BY z.line_type, b.`시간`",
    "\"\"\").createOrReplaceTempView('bus_hr_type_500')",
    "",
    "share_500 = spark.sql(\"\"\"",
    "    SELECT s.line_type, s.hr,",
    "           ROUND(s.subway_h * 100.0 / (s.subway_h + b.bus_h), 2) AS share",
    "    FROM sub_hr_type_500 s",
    "    JOIN bus_hr_type_500 b ON s.line_type = b.line_type AND s.hr = b.hr",
    "\"\"\")",
    "share_500.groupBy('hr').pivot('line_type').max('share').orderBy('hr').show(24)",
))

cells.append(md("### 8-4. Q2: 출근 시간(7-9) 방향 라벨링 — 500m"))
cells.append(code(
    "def build_direction_views(pairs_view, suffix):",
    "    \"\"\"INFLOW/BALANCED/OUTFLOW 라벨링 → MATCH/MISMATCH 평가.\"\"\"",
    "    spark.sql(u\"\"\"",
    "        SELECT `지하철역` AS station, `호선명` AS line,",
    "               SUM(`승차인원`) AS board,",
    "               SUM(`승차인원` + `하차인원`) AS total",
    "        FROM subway_geo WHERE `시간` IN (7,8,9)",
    "        GROUP BY `지하철역`, `호선명`",
    "    \"\"\").createOrReplaceTempView('sub_dir_base_' + suffix)",
    "    ",
    "    spark.sql(\"\"\"",
    "        SELECT *, board * 1.0 / total AS board_ratio,",
    "               CASE WHEN board * 1.0 / total < 0.4 THEN 'INFLOW'",
    "                    WHEN board * 1.0 / total > 0.6 THEN 'OUTFLOW'",
    "                    ELSE 'BALANCED' END AS subway_label",
    "        FROM sub_dir_base_{}",
    "    \"\"\".format(suffix)).createOrReplaceTempView('sub_dir_' + suffix)",
    "    ",
    "    spark.sql(u\"\"\"",
    "        SELECT p.station, p.line,",
    "               SUM(bg.`승차총승객수`) AS board,",
    "               SUM(bg.`승차총승객수` + bg.`하차총승객수`) AS total",
    "        FROM bus_geo bg",
    "        JOIN {} p ON bg.`표준버스정류장ID` = p.stop_id",
    "        WHERE bg.`시간` IN (7,8,9)",
    "        GROUP BY p.station, p.line",
    "    \"\"\".format(pairs_view)).createOrReplaceTempView('bus_dir_base_' + suffix)",
    "    ",
    "    spark.sql(\"\"\"",
    "        SELECT *, board * 1.0 / total AS board_ratio,",
    "               CASE WHEN board * 1.0 / total < 0.4 THEN 'INFLOW'",
    "                    WHEN board * 1.0 / total > 0.6 THEN 'OUTFLOW'",
    "                    ELSE 'BALANCED' END AS bus_label",
    "        FROM bus_dir_base_{}",
    "    \"\"\".format(suffix)).createOrReplaceTempView('bus_dir_' + suffix)",
    "    ",
    "    spark.sql(\"\"\"",
    "        SELECT s.station, s.line, s.subway_label, b.bus_label,",
    "               CASE WHEN s.subway_label = b.bus_label THEN 'MATCH' ELSE 'MISMATCH' END AS direction_match",
    "        FROM sub_dir_{0} s JOIN bus_dir_{0} b",
    "          ON s.station = b.station AND s.line = b.line",
    "    \"\"\".format(suffix)).createOrReplaceTempView('direction_join_' + suffix)",
    "",
    "build_direction_views('pairs_500', '500')",
    "",
    "spark.sql(\"\"\"",
    "    SELECT d.direction_match, COUNT(*) AS n,",
    "           ROUND(AVG(z.subway_share_pct), 2) AS avg_share,",
    "           ROUND(PERCENTILE_APPROX(z.subway_share_pct, 0.5), 2) AS median",
    "    FROM direction_join_500 d",
    "    JOIN zone_stats_500 z ON d.station = z.station AND d.line = z.line",
    "    GROUP BY d.direction_match",
    "\"\"\").show()",
))

cells.append(md("### 8-5. Q2: 월별/계절별 share (500m)"))
cells.append(code(
    "spark.sql(u\"\"\"",
    "    WITH s AS (",
    "        SELECT `사용월` AS ym, SUM(`승차인원` + `하차인원`) AS sub_h",
    "        FROM subway_geo GROUP BY `사용월`",
    "    ),",
    "    b AS (",
    "        SELECT bg.`사용년월` AS ym, SUM(bg.`승차총승객수` + bg.`하차총승객수`) AS bus_h",
    "        FROM bus_geo bg JOIN pairs_500 p ON bg.`표준버스정류장ID` = p.stop_id",
    "        GROUP BY bg.`사용년월`",
    "    )",
    "    SELECT s.ym,",
    "           ROUND(s.sub_h / 1000000.0, 1) AS subway_M,",
    "           ROUND(b.bus_h / 1000000.0, 1) AS bus_M,",
    "           ROUND(s.sub_h * 100.0 / (s.sub_h + b.bus_h), 2) AS share",
    "    FROM s JOIN b ON s.ym = b.ym ORDER BY s.ym",
    "\"\"\").show(24)",
))

cells.append(md("### 8-6. Q3: 시간대별 절대 이용량 + Peak 집중도 (500m)"))
cells.append(code(
    "spark.sql(u\"\"\"",
    "    WITH s AS (",
    "        SELECT `시간` AS hr, SUM(`승차인원` + `하차인원`) AS subway_h",
    "        FROM subway_geo GROUP BY `시간`",
    "    ),",
    "    b AS (",
    "        SELECT bg.`시간` AS hr, SUM(bg.`승차총승객수` + bg.`하차총승객수`) AS bus_h",
    "        FROM bus_geo bg JOIN pairs_500 p ON bg.`표준버스정류장ID` = p.stop_id",
    "        GROUP BY bg.`시간`",
    "    )",
    "    SELECT s.hr,",
    "           ROUND(s.subway_h / 1000000.0, 1) AS subway_M,",
    "           ROUND(b.bus_h    / 1000000.0, 1) AS bus_M,",
    "           ROUND((s.subway_h + b.bus_h) / 1000000.0, 1) AS total_M,",
    "           ROUND(s.subway_h * 100.0 / (s.subway_h + b.bus_h), 2) AS share",
    "    FROM s JOIN b ON s.hr = b.hr ORDER BY s.hr",
    "\"\"\").show(24)",
    "",
    "sub_peak_500 = spark.sql(u\"\"\"",
    "    SELECT ROUND(SUM(CASE WHEN `시간` IN (7,8,9,17,18,19)",
    "                          THEN `승차인원` + `하차인원` ELSE 0 END) * 100.0",
    "                 / SUM(`승차인원` + `하차인원`), 2) AS pct",
    "    FROM subway_geo",
    "\"\"\").collect()[0][0]",
    "",
    "bus_peak_500 = spark.sql(u\"\"\"",
    "    SELECT ROUND(SUM(CASE WHEN bg.`시간` IN (7,8,9,17,18,19)",
    "                          THEN bg.`승차총승객수` + bg.`하차총승객수` ELSE 0 END) * 100.0",
    "                 / SUM(bg.`승차총승객수` + bg.`하차총승객수`), 2) AS pct",
    "    FROM bus_geo bg JOIN pairs_500 p ON bg.`표준버스정류장ID` = p.stop_id",
    "\"\"\").collect()[0][0]",
    "",
    "print('[500m peak 6시간 (7-9, 17-19) 집중도]')",
    "print('subway_peak_pct: {} %'.format(sub_peak_500))",
    "print('bus_peak_pct   : {} %'.format(bus_peak_500))",
))

cells.append(md("### 8-7. Q3: Top-N 지하철역 / 버스정류장 (500m)"))
cells.append(code(
    "# Top 20 지하철역 (호선별)",
    "spark.sql(u\"\"\"",
    "    SELECT `지하철역` AS station, `호선명` AS line,",
    "           ROUND(SUM(`승차인원` + `하차인원`) / 1000000.0, 1) AS total_M",
    "    FROM subway_geo GROUP BY `지하철역`, `호선명`",
    "    ORDER BY total_M DESC LIMIT 20",
    "\"\"\").show(truncate=False)",
))

cells.append(code(
    "# Top 20 버스정류장 (500m 권역 안)",
    "spark.sql(u\"\"\"",
    "    SELECT bg.`표준버스정류장ID` AS stop_id,",
    "           MAX(bg.`역명`) AS stop_name,",
    "           ROUND(SUM(bg.`승차총승객수` + bg.`하차총승객수`) / 1000000.0, 2) AS total_M",
    "    FROM bus_geo bg JOIN pairs_500 p ON bg.`표준버스정류장ID` = p.stop_id",
    "    GROUP BY bg.`표준버스정류장ID`",
    "    ORDER BY total_M DESC LIMIT 20",
    "\"\"\").show(truncate=False)",
))

# ============================================================
# 9. 200m 분석
# ============================================================
cells.append(md(
    "## 9. 200m 권역 분석",
    "",
    "도보 3분 거리 — \"바로 옆\" 정의. 시민이 두 수단을 동시에 보고 선택할 때의 진짜 행동을 더 잘 반영.",
))

cells.append(code(
    "pairs_200 = match_zones(0.2)",
    "print('200m pairs:', pairs_200.count())",
    "zone_stats_200, zone_typed_200 = build_zone_stats(pairs_200, '200')",
    "print('200m zones:', zone_stats_200.count())",
))

cells.append(md("### 9-1. Q1: 권역별 subway_share 분포 (200m)"))
cells.append(code(
    "spark.sql(\"\"\"",
    "    SELECT COUNT(*) AS n_zones,",
    "           ROUND(AVG(subway_share_pct), 2) AS avg_share,",
    "           ROUND(PERCENTILE_APPROX(subway_share_pct, 0.25), 2) AS p25,",
    "           ROUND(PERCENTILE_APPROX(subway_share_pct, 0.5), 2)  AS median,",
    "           ROUND(PERCENTILE_APPROX(subway_share_pct, 0.75), 2) AS p75,",
    "           ROUND(MIN(subway_share_pct), 2) AS min_share,",
    "           ROUND(MAX(subway_share_pct), 2) AS max_share",
    "    FROM zone_stats_200",
    "\"\"\").show()",
))

cells.append(md("### 9-2. Q1: 호선 유형별 share (200m)"))
cells.append(code(
    "spark.sql(\"\"\"",
    "    SELECT line_type, COUNT(*) AS n_zones,",
    "           ROUND(AVG(subway_share_pct), 2) AS avg_share,",
    "           ROUND(SUM(subway_total) / 1000000.0, 1) AS sum_subway_M,",
    "           ROUND(SUM(bus_total) / 1000000.0, 1)    AS sum_bus_M,",
    "           ROUND(SUM(subway_total) * 100.0 / SUM(subway_total + bus_total), 2) AS overall_share",
    "    FROM zone_typed_200",
    "    GROUP BY line_type ORDER BY overall_share DESC",
    "\"\"\").show(truncate=False)",
))

cells.append(md("### 9-3. Q2: 시간대 × 호선유형 share (200m)"))
cells.append(code(
    "spark.sql(u\"\"\"",
    "    SELECT z.line_type, s.`시간` AS hr,",
    "           SUM(s.`승차인원` + s.`하차인원`) AS subway_h",
    "    FROM subway_geo s",
    "    JOIN (SELECT DISTINCT station, line, line_type FROM zone_typed_200) z",
    "      ON s.`지하철역` = z.station AND s.`호선명` = z.line",
    "    GROUP BY z.line_type, s.`시간`",
    "\"\"\").createOrReplaceTempView('sub_hr_type_200')",
    "",
    "spark.sql(u\"\"\"",
    "    SELECT z.line_type, b.`시간` AS hr,",
    "           SUM(b.`승차총승객수` + b.`하차총승객수`) AS bus_h",
    "    FROM bus_geo b",
    "    JOIN pairs_200 p ON b.`표준버스정류장ID` = p.stop_id",
    "    JOIN (SELECT DISTINCT station, line, line_type FROM zone_typed_200) z",
    "      ON p.station = z.station AND p.line = z.line",
    "    GROUP BY z.line_type, b.`시간`",
    "\"\"\").createOrReplaceTempView('bus_hr_type_200')",
    "",
    "share_200 = spark.sql(\"\"\"",
    "    SELECT s.line_type, s.hr,",
    "           ROUND(s.subway_h * 100.0 / (s.subway_h + b.bus_h), 2) AS share",
    "    FROM sub_hr_type_200 s",
    "    JOIN bus_hr_type_200 b ON s.line_type = b.line_type AND s.hr = b.hr",
    "\"\"\")",
    "share_200.groupBy('hr').pivot('line_type').max('share').orderBy('hr').show(24)",
))

cells.append(md("### 9-4. Q2: 출근 시간 방향 (200m)"))
cells.append(code(
    "build_direction_views('pairs_200', '200')",
    "",
    "spark.sql(\"\"\"",
    "    SELECT d.direction_match, COUNT(*) AS n,",
    "           ROUND(AVG(z.subway_share_pct), 2) AS avg_share,",
    "           ROUND(PERCENTILE_APPROX(z.subway_share_pct, 0.5), 2) AS median",
    "    FROM direction_join_200 d",
    "    JOIN zone_stats_200 z ON d.station = z.station AND d.line = z.line",
    "    GROUP BY d.direction_match",
    "\"\"\").show()",
))

cells.append(md("### 9-5. Q3: 시간대별 + Peak 집중도 (200m)"))
cells.append(code(
    "spark.sql(u\"\"\"",
    "    WITH s AS (",
    "        SELECT `시간` AS hr, SUM(`승차인원` + `하차인원`) AS subway_h",
    "        FROM subway_geo GROUP BY `시간`",
    "    ),",
    "    b AS (",
    "        SELECT bg.`시간` AS hr, SUM(bg.`승차총승객수` + bg.`하차총승객수`) AS bus_h",
    "        FROM bus_geo bg JOIN pairs_200 p ON bg.`표준버스정류장ID` = p.stop_id",
    "        GROUP BY bg.`시간`",
    "    )",
    "    SELECT s.hr,",
    "           ROUND(s.subway_h / 1000000.0, 1) AS subway_M,",
    "           ROUND(b.bus_h    / 1000000.0, 1) AS bus_M,",
    "           ROUND(s.subway_h * 100.0 / (s.subway_h + b.bus_h), 2) AS share",
    "    FROM s JOIN b ON s.hr = b.hr ORDER BY s.hr",
    "\"\"\").show(24)",
    "",
    "sub_peak_200 = spark.sql(u\"\"\"",
    "    SELECT ROUND(SUM(CASE WHEN `시간` IN (7,8,9,17,18,19)",
    "                          THEN `승차인원` + `하차인원` ELSE 0 END) * 100.0",
    "                 / SUM(`승차인원` + `하차인원`), 2) AS pct",
    "    FROM subway_geo",
    "\"\"\").collect()[0][0]",
    "",
    "bus_peak_200 = spark.sql(u\"\"\"",
    "    SELECT ROUND(SUM(CASE WHEN bg.`시간` IN (7,8,9,17,18,19)",
    "                          THEN bg.`승차총승객수` + bg.`하차총승객수` ELSE 0 END) * 100.0",
    "                 / SUM(bg.`승차총승객수` + bg.`하차총승객수`), 2) AS pct",
    "    FROM bus_geo bg JOIN pairs_200 p ON bg.`표준버스정류장ID` = p.stop_id",
    "\"\"\").collect()[0][0]",
    "",
    "print('[200m peak 6시간 집중도]')",
    "print('subway_peak_pct: {} %'.format(sub_peak_200))",
    "print('bus_peak_pct   : {} %'.format(bus_peak_200))",
))

# ============================================================
# 10. 500m vs 200m 비교
# ============================================================
cells.append(md(
    "## 10. 500m vs 200m 비교",
    "",
    "두 반경의 핵심 지표를 한눈에 비교.",
))

cells.append(code(
    "# 각 반경의 평균 share 비교",
    "for suffix in ['500', '200']:",
    "    row = spark.sql(\"\"\"",
    "        SELECT COUNT(*) AS n,",
    "               ROUND(AVG(subway_share_pct), 2) AS avg,",
    "               ROUND(PERCENTILE_APPROX(subway_share_pct, 0.5), 2) AS median,",
    "               ROUND(STDDEV(subway_share_pct), 2) AS stddev",
    "        FROM zone_stats_{}",
    "    \"\"\".format(suffix)).collect()[0]",
    "    print('{}m: n={}, avg={}, median={}, std={}'.format(",
    "        suffix, row['n'], row['avg'], row['median'], row['stddev']))",
))

# ============================================================
# 11. 시각화 — Plotly
# ============================================================
cells.append(md(
    "## 11. 시각화 (Plotly)",
    "",
    "Spark DataFrame → Pandas 변환 후 Plotly 인터랙티브 차트로 결과를 시각화한다.",
    "",
    "필요한 패키지: `pandas`, `plotly`.",
))

cells.append(code(
    "import pandas as pd",
    "import plotly.express as px",
    "import plotly.graph_objects as go",
    "from plotly.subplots import make_subplots",
))

cells.append(md("### 11-1. Q1: 권역별 subway_share 분포 (500m vs 200m 히스토그램)"))
cells.append(code(
    "pdf_500 = zone_stats_500.select('subway_share_pct').toPandas()",
    "pdf_200 = zone_stats_200.select('subway_share_pct').toPandas()",
    "pdf_500['radius'] = '500m'",
    "pdf_200['radius'] = '200m'",
    "pdf_share = pd.concat([pdf_500, pdf_200], ignore_index=True)",
    "",
    "fig = px.histogram(",
    "    pdf_share, x='subway_share_pct', color='radius',",
    "    nbins=40, opacity=0.7, barmode='overlay',",
    "    title='권역별 subway_share 분포: 500m vs 200m',",
    "    labels={'subway_share_pct': 'Subway share (%)', 'radius': '반경'},",
    ")",
    "fig.update_layout(template='plotly_white', height=480)",
    "fig.show()",
))

cells.append(md("### 11-2. Q1: 호선 유형별 share bar chart"))
cells.append(code(
    "def line_type_overall(suffix):",
    "    df = spark.sql(\"\"\"",
    "        SELECT line_type,",
    "               ROUND(SUM(subway_total) * 100.0 / SUM(subway_total + bus_total), 2) AS overall_share,",
    "               COUNT(*) AS n_zones",
    "        FROM zone_typed_{} GROUP BY line_type",
    "    \"\"\".format(suffix)).toPandas()",
    "    df['radius'] = suffix + 'm'",
    "    return df",
    "",
    "lt_df = pd.concat([line_type_overall('500'), line_type_overall('200')], ignore_index=True)",
    "",
    "fig = px.bar(",
    "    lt_df.sort_values(['radius', 'overall_share'], ascending=[True, False]),",
    "    x='line_type', y='overall_share', color='radius', barmode='group',",
    "    title='호선 유형별 overall subway_share (%)',",
    "    text='overall_share',",
    ")",
    "fig.update_traces(textposition='outside')",
    "fig.update_layout(template='plotly_white', height=520, yaxis_range=[0, 100])",
    "fig.show()",
))

cells.append(md("### 11-3. Q2: 시간대 × 호선유형 share 히트맵 (200m)"))
cells.append(code(
    "heat_pdf = share_200.toPandas().pivot(index='line_type', columns='hr', values='share')",
    "",
    "fig = px.imshow(",
    "    heat_pdf,",
    "    labels=dict(x='시간(hr)', y='호선유형', color='share(%)'),",
    "    text_auto='.0f', aspect='auto',",
    "    color_continuous_scale='RdBu_r', range_color=[0, 100],",
    "    title='시간대 × 호선유형 subway_share (%) — 200m',",
    ")",
    "fig.update_layout(template='plotly_white', height=480)",
    "fig.show()",
))

cells.append(md("### 11-4. Q3: 시간대별 절대 이용량 line chart (200m)"))
cells.append(code(
    "hr_pdf = spark.sql(u\"\"\"",
    "    WITH s AS (",
    "        SELECT `시간` AS hr, SUM(`승차인원` + `하차인원`) AS subway_h",
    "        FROM subway_geo GROUP BY `시간`",
    "    ),",
    "    b AS (",
    "        SELECT bg.`시간` AS hr, SUM(bg.`승차총승객수` + bg.`하차총승객수`) AS bus_h",
    "        FROM bus_geo bg JOIN pairs_200 p ON bg.`표준버스정류장ID` = p.stop_id",
    "        GROUP BY bg.`시간`",
    "    )",
    "    SELECT s.hr,",
    "           s.subway_h / 1000000.0 AS subway_M,",
    "           b.bus_h    / 1000000.0 AS bus_M",
    "    FROM s JOIN b ON s.hr = b.hr ORDER BY s.hr",
    "\"\"\").toPandas()",
    "",
    "fig = go.Figure()",
    "fig.add_trace(go.Scatter(x=hr_pdf['hr'], y=hr_pdf['subway_M'], mode='lines+markers', name='Subway'))",
    "fig.add_trace(go.Scatter(x=hr_pdf['hr'], y=hr_pdf['bus_M'],    mode='lines+markers', name='Bus'))",
    "fig.update_layout(",
    "    template='plotly_white',",
    "    title='시간대별 절대 이용량 (백만 명, 18개월 누적) — 200m',",
    "    xaxis_title='시간(hr)', yaxis_title='이용량(백만)', height=460,",
    ")",
    "fig.show()",
))

cells.append(md("### 11-5. Q3: Peak 집중도 비교 차트"))
cells.append(code(
    "peak_df = pd.DataFrame({",
    "    'mode'  : ['Subway', 'Subway', 'Bus', 'Bus'],",
    "    'radius': ['500m',   '200m',   '500m', '200m'],",
    "    'peak_pct': [sub_peak_500, sub_peak_200, bus_peak_500, bus_peak_200],",
    "})",
    "fig = px.bar(",
    "    peak_df, x='mode', y='peak_pct', color='radius', barmode='group',",
    "    text='peak_pct',",
    "    title='Peak 6시간 집중도 (7-9, 17-19) — Subway vs Bus, 500m vs 200m',",
    ")",
    "fig.update_traces(textposition='outside')",
    "fig.update_layout(template='plotly_white', height=440, yaxis_range=[0, 60])",
    "fig.show()",
))

cells.append(md("### 11-6. Q3: Top-N 지하철역 / 버스정류장 (200m 권역)"))
cells.append(code(
    "top_sub_pdf = spark.sql(u\"\"\"",
    "    SELECT CONCAT(`지하철역`, ' (', `호선명`, ')') AS station_line,",
    "           SUM(`승차인원` + `하차인원`) / 1000000.0 AS total_M",
    "    FROM subway_geo GROUP BY `지하철역`, `호선명`",
    "    ORDER BY total_M DESC LIMIT 15",
    "\"\"\").toPandas()",
    "",
    "fig = px.bar(",
    "    top_sub_pdf.sort_values('total_M'),",
    "    x='total_M', y='station_line', orientation='h',",
    "    title='Top 15 지하철역 (18개월 누적, 백만 명)',",
    "    text='total_M',",
    ")",
    "fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')",
    "fig.update_layout(template='plotly_white', height=520, yaxis_title='역(호선)', xaxis_title='이용량(백만)')",
    "fig.show()",
))

cells.append(code(
    "top_bus_pdf = spark.sql(u\"\"\"",
    "    SELECT MAX(bg.`역명`) AS stop_name,",
    "           SUM(bg.`승차총승객수` + bg.`하차총승객수`) / 1000000.0 AS total_M",
    "    FROM bus_geo bg JOIN pairs_200 p ON bg.`표준버스정류장ID` = p.stop_id",
    "    GROUP BY bg.`표준버스정류장ID`",
    "    ORDER BY total_M DESC LIMIT 15",
    "\"\"\").toPandas()",
    "",
    "fig = px.bar(",
    "    top_bus_pdf.sort_values('total_M'),",
    "    x='total_M', y='stop_name', orientation='h',",
    "    title='Top 15 버스정류장 (200m 권역, 18개월 누적, 백만 명)',",
    "    text='total_M',",
    ")",
    "fig.update_traces(texttemplate='%{text:.2f}', textposition='outside')",
    "fig.update_layout(template='plotly_white', height=520)",
    "fig.show()",
))

# ============================================================
# 12. 결론
# ============================================================
cells.append(md(
    "## 12. 종합 결론",
    "",
    "### Q1 — 권역 내 수단 선호",
    "- **200m (도보 3분)** 기준에서는 시민이 압도적으로 **지하철 (avg ~74%)** 을 선택.",
    "- **500m (도시계획 표준)** 기준에서는 약 **53%** 로 거의 균형 — 멀리 떨어진 정류장도 권역에 포함되어 버스 권역이 과대 측정.",
    "- 즉, **권역 정의가 결과를 크게 좌우**.",
    "",
    "### Q2 — 선호 결정 변수",
    "1. **시간대** (가장 강력): 출퇴근 80%+, 심야는 지하철 운행 종료로 버스 독점",
    "2. **호선 유형**: 서교공 1-8호선 / 9호선 / 공항철도 등은 지하철 우세, 경전철은 균형",
    "3. **계절·월**: 거의 평탄 (1~3%p 변동)",
    "4. **요일 (평일/주말)**: 데이터 한계로 측정 불가",
    "5. **방향 효과**: 미미 (1~2%p MATCH/MISMATCH 차이)",
    "",
    "### Q3 — 수단 용도",
    "- **지하철** = 통근 도구 (peak 집중도 46%)",
    "- **버스** = 종일 평탄 활용 (peak 41%)",
    "- 18시 퇴근이 8시 출근보다 약간 더 많음",
    "- 환승 메가 허브: **서울역 (5개 호선, 122M)**, 잠실 (110M), 홍대입구 (104M)",
    "",
    "### 한계 (보고서 §6)",
    "- 일자 단위 데이터 부재 → 평일/주말 분석 불가",
    "- 노선 진행 방향 정보 부재 → 정확한 방향 매칭 불가",
    "- 매칭 누락 정류장 약 17% (폐쇄·신설·표기 차이)",
))

# ============================================================
# Notebook metadata + write
# ============================================================
notebook = {
    "cells": cells,
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        },
        "language_info": {
            "name": "python",
            "version": "3.x",
        },
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

NB_PATH.parent.mkdir(parents=True, exist_ok=True)
with open(NB_PATH, "w", encoding="utf-8") as f:
    json.dump(notebook, f, ensure_ascii=False, indent=1)

print("Wrote:", NB_PATH)
print("Total cells:", len(cells))
