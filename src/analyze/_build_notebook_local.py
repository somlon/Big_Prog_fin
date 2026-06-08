#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build q1_q2_q3_analysis_local.ipynb — 로컬 PySpark/Jupyter 전용 노트북.

기존 `_build_notebook.py` 의 `cells` 변수를 import 한 뒤 셀 1-3 (환경 설정,
Parquet 로드, CSV 로드) 만 로컬 친화 버전으로 교체한다. 나머지 분석·시각화
셀은 sandbox 노트북과 100% 동일.

로컬 친화 변경 사항 (셀 1-3 만):
  - DATA_BASE 자동 감지 (환경변수 + ./data, ../data, ../../data 탐색)
  - data_path() Windows 백슬래시 호환
  - _safe_read_parquet / _safe_read_csv 래퍼: 친절한 에러 메시지
  - 로컬 경로 미존재 시 즉시 안내

Usage:
    python3 src/analyze/_build_notebook_local.py
"""
import importlib.util
import json
from pathlib import Path

HERE = Path(__file__).parent

# ============================================================
# 1. _build_notebook.py 의 cells 가져오기
#    (import 시 q1_q2_q3_analysis.ipynb 가 재빌드되지만, 동일 내용이라 diff 없음)
# ============================================================
spec = importlib.util.spec_from_file_location(
    "_base_builder", str(HERE / "_build_notebook.py")
)
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)


def md(*lines):
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": [line + "\n" for line in lines[:-1]] + [lines[-1]] if lines else [""],
    }


def code(*lines):
    src = [line + "\n" for line in lines[:-1]] + [lines[-1]] if lines else [""]
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": src,
    }


# ============================================================
# 2. cells 복사 + 로컬 전용 셀로 교체
# ============================================================
cells = list(base.cells)

# 셀 인덱스 (참고):
#   0: 헤더 (md)
#   1: '## 1. 환경 설정' (md)
#   2: setup code
#   3: '## 2. Parquet 로드' (md)
#   4: parquet load code
#   5: '## 3. CSV 로드' (md)
#   6: csv load code
#   7~: 나머지 분석·시각화

# --- 셀 0: 헤더 (로컬 안내 추가) ---
cells[0] = md(
    "# 서울시 지하철·버스 이용 패턴 비교 분석 (Q1/Q2/Q3) — **로컬 PySpark 버전**",
    "",
    "> 본 노트북은 **로컬 PySpark + Jupyter** 환경 전용입니다.",
    "> Sandbox HDP 환경에서는 `q1_q2_q3_analysis.ipynb` 를 사용하세요.",
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
    "## 실행 전 준비",
    "1. 의존성 설치: `pip install -r requirements.txt`",
    "2. 데이터 준비 (3가지 옵션 중 택1):",
    "   - **`DATA_BASE` 환경변수**: `set DATA_BASE=C:\\path\\to\\data` (Windows) / `export DATA_BASE=/path/to/data` (Linux/macOS)",
    "   - **`./data/` 폴더 자동 감지**: 노트북 작업 디렉토리 또는 그 부모/조부모에 `data/` 가 있으면 자동 사용",
    "   - **Sandbox HDFS URI 직접 사용**: `set DATA_BASE=hdfs://<cluster>:8020/user/maria_dev/seoul`",
    "",
    "## 산출물",
    "- 권역 단위 `subway_share`(%) 분포",
    "- 호선 유형별 / 시간대별 / 방향별 / 계절별 share",
    "- 시간대 절대 이용량 + Peak 집중도",
    "- Top-N 지하철역/버스정류장",
    "- Plotly 인터랙티브 시각화 차트",
)

# --- 셀 1: '## 1. 환경 설정' md (로컬 안내 보강) ---
cells[1] = md(
    "## 1. 환경 설정 (로컬 자동 감지)",
    "",
    "SparkSession 생성 + 데이터 경로 자동 감지 + Windows winutils 자동 적용.",
    "",
    "### `DATA_BASE` 우선순위",
    "1. **환경변수 `DATA_BASE`** (명시 — 권장)",
    "2. 작업 디렉토리 기준 자동 탐색 (`./data`, `../data`, `../../data` 중 `processed/subway` 또는 `raw/coords` 가 있는 곳)",
    "3. **Sandbox HDFS** (`/user/maria_dev/seoul`) — 마지막 폴백",
    "",
    "**Windows cmd**: `set DATA_BASE=C:\\path\\to\\data`",
    "**Linux/macOS**: `export DATA_BASE=/path/to/data`",
    "",
    "### Spark 드라이버 메모리 (시스템 RAM 에 맞춰 조정)",
    "기본 `4g` (16GB+ RAM 시스템 권장). 1683만 행 bus 데이터 처리 중 OOM 방지용.",
    "",
    "| 시스템 RAM | 권장 `SPARK_DRIVER_MEMORY` |",
    "|---|---|",
    "| 8 GB | `3g` |",
    "| 16 GB | `4g` (기본) |",
    "| 24 GB+ | `6g` |",
    "",
    "**Windows cmd**: `set SPARK_DRIVER_MEMORY=6g` (Kernel restart 필요)",
    "**Linux/macOS**: `export SPARK_DRIVER_MEMORY=6g`",
    "",
    "### Windows winutils 사전 준비 (필수)",
    "PySpark 가 Windows 에서 로컬 파일을 읽으려면 `winutils.exe` + `hadoop.dll` 이 필요합니다.",
    "",
    "1. [cdarlint/winutils](https://github.com/cdarlint/winutils) 에서 `hadoop-3.3.x/bin/winutils.exe` 와 `hadoop-3.3.x/bin/hadoop.dll` 다운로드 (PySpark 3.5.x 와 호환되는 3.3.4 또는 3.3.6 권장)",
    "2. `C:\\hadoop\\bin\\` 에 배치 (다른 경로 사용 시 `set HADOOP_HOME=...` 으로 override)",
    "3. 노트북 셀 2 가 자동 감지하여 `HADOOP_HOME` / `PATH` / `_JAVA_OPTIONS` 환경변수를 설정",
    "",
    "준비 안 된 경우 셀 2 에 WARNING 출력 + 셀 4 (Parquet 로드) 에서 `UnsatisfiedLinkError` 발생.",
)

# --- 셀 2: setup code (자동 감지 + data_path helper) ---
cells[2] = code(
    "# Python 2 한글 출력 fix (Spark2.4 호환 noop on Python 3)",
    "import sys",
    "if sys.version_info[0] == 2:",
    "    reload(sys)",
    "    sys.setdefaultencoding('utf-8')",
    "",
    "import os",
    "",
    "# === Windows winutils + hadoop.dll 자동 감지 (Linux/macOS no-op) ===",
    "# winutils 가 있으면 HADOOP_HOME / PATH / _JAVA_OPTIONS 자동 설정.",
    "# 사전 준비: cdarlint/winutils 에서 hadoop-3.3.x 의 winutils.exe + hadoop.dll 을 C:\\hadoop\\bin\\ 에 배치.",
    "# 다른 경로 사용 시 HADOOP_HOME 환경변수로 override 가능.",
    "_winutils_home = os.environ.get('HADOOP_HOME', 'C:/hadoop').replace('\\\\', '/').rstrip('/')",
    "if os.path.exists(os.path.join(_winutils_home, 'bin', 'hadoop.dll')):",
    "    os.environ['HADOOP_HOME'] = _winutils_home",
    "    os.environ['PATH']        = _winutils_home + '/bin;' + os.environ.get('PATH', '')",
    "    # _JAVA_OPTIONS 는 JVM 시작 시 무조건 읽음 (spark.driver.extraJavaOptions 의 backslash escape 문제 회피)",
    "    os.environ['_JAVA_OPTIONS'] = '-Djava.library.path=' + _winutils_home + '/bin'",
    "    _WINUTILS_OK = True",
    "else:",
    "    _WINUTILS_OK = False",
    "",
    "from pathlib import Path",
    "from pyspark.sql import SparkSession",
    "from pyspark.sql.functions import (",
    "    col, regexp_replace, when, sin, cos, asin, sqrt, radians, broadcast,",
    "    lit, sum as F_sum, count as F_count, avg as F_avg, max as F_max, min as F_min,",
    "    percentile_approx, stddev as F_stddev, round as F_round,",
    ")",
    "from pyspark.sql.types import StringType",
    "",
    "# === Driver/Executor 메모리 (로컬 16M+ 행 처리용 OOM 방지) ===",
    "# 기본 1g 으로는 bus.count()(1683만 행) 처리 중 JVM crash 발생.",
    "# 시스템 RAM 에 맞춰 환경변수로 override 가능:",
    "#   set SPARK_DRIVER_MEMORY=6g   (Windows, 24GB+ RAM)",
    "#   set SPARK_DRIVER_MEMORY=3g   (Windows, 8GB RAM)",
    "_SPARK_DRIVER_MEM        = os.environ.get('SPARK_DRIVER_MEMORY', '4g')",
    "_SPARK_DRIVER_MAX_RESULT = os.environ.get('SPARK_DRIVER_MAX_RESULT_SIZE', '2g')",
    "",
    "# Spark session",
    "spark = (",
    "    SparkSession.builder",
    "    .appName('BP_Q1Q2Q3_local')",
    "    .config('spark.sql.shuffle.partitions', '200')",
    "    .config('spark.driver.memory',        _SPARK_DRIVER_MEM)",
    "    .config('spark.driver.maxResultSize', _SPARK_DRIVER_MAX_RESULT)",
    "    .getOrCreate()",
    ")",
    "spark.sparkContext.setLogLevel('WARN')",
    "",
    "# === 데이터 경로 자동 감지 ===",
    "def _find_data_base():",
    "    env = os.environ.get('DATA_BASE')",
    "    if env:",
    "        return env, 'env'",
    "    here = Path.cwd()",
    "    for cand in [here / 'data', here.parent / 'data', here.parent.parent / 'data']:",
    "        if (cand / 'processed' / 'subway').exists() or (cand / 'raw' / 'coords').exists():",
    "            return str(cand.resolve()), 'auto'",
    "    return '/user/maria_dev/seoul', 'sandbox-default'",
    "",
    "DATA_BASE, _src = _find_data_base()",
    "",
    "def data_path(*segments):",
    "    \"\"\"DATA_BASE 기준 경로. HDFS/로컬/URI 모두 호환 (Spark는 / 구분자 OK).\"\"\"",
    "    base = DATA_BASE.rstrip('/').replace('\\\\', '/')",
    "    return '/'.join([base] + list(segments))",
    "",
    "PARQUET_SUBWAY = data_path('processed', 'subway')",
    "PARQUET_BUS    = data_path('processed', 'bus')",
    "CSV_BUS_COORD  = data_path('raw', 'coords', 'bus_stops_xy.csv')",
    "CSV_SUB_COORD  = data_path('raw', 'coords', 'subway_stations_xy.csv')",
    "",
    "print('Spark version :', spark.version)",
    "print('Driver memory :', _SPARK_DRIVER_MEM, '(maxResultSize: ' + _SPARK_DRIVER_MAX_RESULT + ')')",
    "print('DATA_BASE     :', DATA_BASE, '(source: {})'.format(_src))",
    "print('  subway      :', PARQUET_SUBWAY)",
    "print('  bus         :', PARQUET_BUS)",
    "print('  bus_coord   :', CSV_BUS_COORD)",
    "print('  sub_coord   :', CSV_SUB_COORD)",
    "",
    "# Windows winutils 적용 결과 안내",
    "if sys.platform == 'win32':",
    "    print()",
    "    print('--- Windows Hadoop native lib ---')",
    "    if _WINUTILS_OK:",
    "        print('winutils + hadoop.dll : ' + os.environ['HADOOP_HOME'] + '/bin (auto-applied)')",
    "        print('  _JAVA_OPTIONS       : ' + os.environ['_JAVA_OPTIONS'])",
    "    else:",
    "        print('WARNING: hadoop.dll 미감지 (' + _winutils_home + '/bin/hadoop.dll)')",
    "        print('  Parquet 로드 시 UnsatisfiedLinkError 발생 가능. 다음 중 하나 진행:')",
    "        print('    1) cdarlint/winutils 의 hadoop-3.3.x/bin/{winutils.exe,hadoop.dll} 을 C:\\\\hadoop\\\\bin\\\\ 에 배치')",
    "        print('    2) 또는 set HADOOP_HOME=<winutils 가 있는 경로> 후 Kernel 재시작')",
    "",
    "# 로컬 경로일 때 존재 여부 사전 안내",
    "_is_local = not (DATA_BASE.startswith('/') or '://' in DATA_BASE)",
    "if _is_local and not os.path.exists(PARQUET_SUBWAY):",
    "    print()",
    "    print('=' * 64)",
    "    print('WARNING: Parquet 경로가 로컬에 존재하지 않습니다.')",
    "    print('=' * 64)",
    "    print('해결 방법:')",
    "    print('  A. DATA_BASE 환경변수 설정:')",
    "    print('       Linux:   export DATA_BASE=/path/to/data')",
    "    print('       Windows: set DATA_BASE=C:\\\\path\\\\to\\\\data')",
    "    print('  B. 노트북 위치 기준 ./data 또는 ../data 폴더에 데이터 배치')",
    "    print('       data/processed/{subway,bus}/  + data/raw/coords/*.csv')",
    "    print('  C. Sandbox HDFS 에서 다운로드 (Sandbox SSH 후):')",
    "    print('       hdfs dfs -get /user/maria_dev/seoul/processed ./processed')",
    "    print('       hdfs dfs -get /user/maria_dev/seoul/raw/coords ./coords')",
)

# --- 셀 4: Parquet 로드 code (_safe_read_parquet) ---
cells[4] = code(
    "def _safe_read_parquet(path, label):",
    "    try:",
    "        return spark.read.parquet(path)",
    "    except Exception as e:",
    "        msg = str(e)",
    "        raise RuntimeError(",
    "            '[' + label + '] Parquet 로드 실패\\n'",
    "            '  path : ' + path + '\\n'",
    "            '  cause: ' + msg[:200] + ('...' if len(msg) > 200 else '') + '\\n'",
    "            '  DATA_BASE 환경변수 또는 데이터 위치를 확인하세요. (셀 1 참고)'",
    "        )",
    "",
    "subway = _safe_read_parquet(PARQUET_SUBWAY, 'subway')",
    "bus    = _safe_read_parquet(PARQUET_BUS,    'bus')",
    "",
    "subway.createOrReplaceTempView('subway_hourly')",
    "bus.createOrReplaceTempView('bus_hourly')",
    "",
    "print('Subway:', subway.count(), 'rows')",
    "print('Bus   :', bus.count(), 'rows')",
    "subway.printSchema()",
    "bus.printSchema()",
)

# --- 셀 6: CSV 로드 code (_safe_read_csv) ---
cells[6] = code(
    "def _safe_read_csv(path, label, **kwargs):",
    "    try:",
    "        return spark.read.csv(path, **kwargs)",
    "    except Exception as e:",
    "        msg = str(e)",
    "        raise RuntimeError(",
    "            '[' + label + '] CSV 로드 실패\\n'",
    "            '  path : ' + path + '\\n'",
    "            '  cause: ' + msg[:200] + ('...' if len(msg) > 200 else '') + '\\n'",
    "            '  DATA_BASE 환경변수 또는 데이터 위치를 확인하세요. (셀 1 참고)'",
    "        )",
    "",
    "bus_coord = (",
    "    _safe_read_csv(CSV_BUS_COORD, 'bus_coord', header=True, inferSchema=True, encoding='UTF-8')",
    "    .withColumn('STOPS_NO', col('STOPS_NO').cast(StringType()))",
    "    .withColumn('NODE_ID',  col('NODE_ID').cast(StringType()))",
    ")",
    "sub_coord = (",
    "    _safe_read_csv(CSV_SUB_COORD, 'sub_coord', header=True, inferSchema=True, encoding='UTF-8')",
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
)

# ============================================================
# 3. Folium 지도 시각화 셀 추가 (로컬 전용, 마지막에 append)
# ============================================================
# 셀 64+: 지도 시각화 3종
#   13-1: Top-15 지하철역 마커
#   13-2: 200m 권역 매칭 예시
#   13-3: 권역별 subway_share 지도

cells.append(md(
    "## 13. Folium 지도 시각화 (로컬 전용)",
    "",
    "> Folium 은 로컬 PySpark + Jupyter 환경에서만 동작합니다. Sandbox Zeppelin 에서는 Plotly 만 사용하세요.",
    "",
    "좌표 기반 분석 결과를 실제 서울 지도 위에 시각화합니다. 앞선 Plotly 차트가 통계적 비교를 보여줬다면, 아래 지도는 **지리적 분포** 를 보여줍니다.",
    "",
    "| # | 지도 | 메시지 |",
    "|---|---|---|",
    "| 13-1 | Top-15 지하철역 마커 | 어디가 가장 붐비는가? |",
    "| 13-2 | 200m 권역 매칭 예시 | \"200m\" 가 지도 위에서 무엇을 의미하는가? |",
    "| 13-3 | 권역별 subway_share | 서울의 어느 지역이 지하철/버스 강세인가? |",
))

cells.append(code(
    "import folium",
    "",
    "# 서울 시청 좌표",
    "SEOUL_CENTER = (37.5663, 126.9779)",
    "",
    "# 호선별 대표 색상 (folium 의 CSS 색상명)",
    "LINE_COLORS = {",
    "    '1호선'         : 'darkblue',",
    "    '2호선'         : 'green',",
    "    '3호선'         : 'orange',",
    "    '4호선'         : 'lightblue',",
    "    '5호선'         : 'purple',",
    "    '6호선'         : 'darkred',",
    "    '7호선'         : 'darkgreen',",
    "    '8호선'         : 'pink',",
    "    '9호선'         : 'gold',",
    "    '경부선'        : 'gray',",
    "    '경의중앙선'    : 'cadetblue',",
    "    '공항철도1호선' : 'lightred',",
    "    '분당선'        : 'darkpurple',",
    "    '수인선'        : 'lightgreen',",
    "    '신분당선'      : 'red',",
    "    '경춘선'        : 'darkblue',",
    "    '경강선'        : 'lightgray',",
    "    '서해선'        : 'black',",
    "    '우이신설선'    : 'gray',",
    "    '신림선'        : 'gray',",
    "}",
    "",
    "def line_color(line):",
    "    return LINE_COLORS.get(line, 'gray')",
    "",
    "print('folium version :', folium.__version__)",
    "print('등록 호선 색상 :', len(LINE_COLORS))",
))

cells.append(md(
    "### 13-1. Top-15 지하철역 마커 지도",
    "",
    "이용량 상위 15개 지하철역을 서울 지도 위에 표시. **마커 크기 = 18개월 누적 이용량**, **색상 = 호선**.",
    "",
    "지하철 Top-15 의 지리적 분포 (2호선 환승 메가 허브 + 강남/송파 라인) 가 한눈에 보입니다.",
))

cells.append(code(
    "# Top-15 지하철역 (이용량 + 좌표)",
    "top15_sub_pdf = spark.sql(u\"\"\"",
    "    SELECT `지하철역` AS station,",
    "           `호선명`   AS line,",
    "           SUM(`승차인원` + `하차인원`) / 1000000.0 AS total_M,",
    "           AVG(sta_lat) AS lat,",
    "           AVG(sta_lon) AS lon",
    "    FROM subway_geo",
    "    GROUP BY `지하철역`, `호선명`",
    "    ORDER BY total_M DESC",
    "    LIMIT 15",
    "\"\"\").toPandas()",
    "",
    "print('Top-15 지하철역 좌표 수집 완료:', len(top15_sub_pdf), '개')",
    "",
    "m1 = folium.Map(location=SEOUL_CENTER, zoom_start=11, tiles='OpenStreetMap')",
    "",
    "max_total = top15_sub_pdf['total_M'].max()",
    "for _, row in top15_sub_pdf.iterrows():",
    "    # 마커 반지름 = 이용량에 비례 (max 30px)",
    "    radius_px = 8 + 22 * (row['total_M'] / max_total)",
    "    color = line_color(row['line'])",
    "    folium.CircleMarker(",
    "        location=(row['lat'], row['lon']),",
    "        radius=radius_px,",
    "        popup=folium.Popup(",
    "            '<b>{}</b><br>호선: {}<br>이용량: {:.1f}M'.format(",
    "                row['station'], row['line'], row['total_M']),",
    "            max_width=200,",
    "        ),",
    "        tooltip='{} ({}) — {:.1f}M'.format(row['station'], row['line'], row['total_M']),",
    "        color=color,",
    "        fill=True,",
    "        fill_color=color,",
    "        fill_opacity=0.6,",
    "        weight=2,",
    "    ).add_to(m1)",
    "",
    "m1",
))

cells.append(md(
    "### 13-2. 200m 권역 매칭 시각화",
    "",
    "대표 지하철역 3곳 (강남, 홍대입구, 잠실) 의 **200m 반경** 권역을 빨간 원으로 그리고, 권역 안에 포함된 버스정류장을 파란 점으로 표시.",
    "",
    "\"200m\" 가 어느 정도 거리인지, 그 안에 얼마나 많은 버스정류장이 들어가는지 직관적으로 확인할 수 있습니다.",
))

cells.append(code(
    "SAMPLE_STATIONS = ['강남', '홍대입구', '잠실(송파구청)']",
    "",
    "# 샘플 역 좌표",
    "sample_sub_pdf = spark.sql(u\"\"\"",
    "    SELECT DISTINCT `지하철역` AS station, `호선명` AS line,",
    "           sta_lat AS lat, sta_lon AS lon",
    "    FROM subway_geo",
    "    WHERE `지하철역` IN ({})",
    "\"\"\".format(','.join([\"'{}'\".format(s) for s in SAMPLE_STATIONS]))).toPandas()",
    "",
    "# 샘플 역의 200m 권역 안 버스정류장 (pairs_200 에서 추출)",
    "sample_pairs_pdf = pairs_200.filter(",
    "    col('station').isin(SAMPLE_STATIONS)",
    ").select('station', 'line', 'sta_lat', 'sta_lon',",
    "         'stop_id', 'stop_lat', 'stop_lon', 'dist_km').toPandas()",
    "",
    "print('샘플 역      :', list(sample_sub_pdf['station']))",
    "print('200m 안 쌍   :', len(sample_pairs_pdf))",
    "",
    "# 샘플 역들 사이 중심에 지도 시작",
    "center_lat = sample_sub_pdf['lat'].mean()",
    "center_lon = sample_sub_pdf['lon'].mean()",
    "",
    "m2 = folium.Map(location=(center_lat, center_lon), zoom_start=12, tiles='OpenStreetMap')",
    "",
    "# 1) 200m 권역 원 (빨강) + 지하철역 마커",
    "for _, srow in sample_sub_pdf.iterrows():",
    "    folium.Circle(",
    "        location=(srow['lat'], srow['lon']),",
    "        radius=200,  # meters",
    "        color='red',",
    "        weight=2,",
    "        fill=True,",
    "        fill_opacity=0.1,",
    "        popup='{} ({}) — 200m 권역'.format(srow['station'], srow['line']),",
    "    ).add_to(m2)",
    "    folium.Marker(",
    "        location=(srow['lat'], srow['lon']),",
    "        popup='<b>{}</b><br>{}'.format(srow['station'], srow['line']),",
    "        tooltip=srow['station'],",
    "        icon=folium.Icon(color='red', icon='train', prefix='fa'),",
    "    ).add_to(m2)",
    "",
    "# 2) 200m 안 버스정류장 (파랑 점)",
    "stops_in_zone = sample_pairs_pdf.drop_duplicates(subset='stop_id')",
    "for _, brow in stops_in_zone.iterrows():",
    "    folium.CircleMarker(",
    "        location=(brow['stop_lat'], brow['stop_lon']),",
    "        radius=4,",
    "        popup='버스정류장 {}<br>거리: {:.0f}m<br>매칭 역: {}'.format(",
    "            brow['stop_id'], brow['dist_km']*1000, brow['station']),",
    "        color='blue',",
    "        fill=True,",
    "        fill_color='blue',",
    "        fill_opacity=0.8,",
    "    ).add_to(m2)",
    "",
    "print('지도 표시 정류장:', len(stops_in_zone), '개')",
    "m2",
))

cells.append(md(
    "### 13-3. 200m 권역별 subway_share 지도",
    "",
    "200m 매칭에 성공한 모든 지하철역에 대해 `subway_share_pct` 를 **색상** 으로 표시.",
    "",
    "- 🔴 빨강: 67%+ (지하철 강세)",
    "- 🟠 주황: 50~67%",
    "- 🔵 하늘: 33~50%",
    "- 🟦 파랑: ~33% (버스 강세)",
    "",
    "서울 지도 위에서 빨간 영역이 지하철 의존도가 높은 곳, 파란 영역이 버스 의존도가 높은 곳을 한눈에 보여줍니다.",
))

cells.append(code(
    "# 200m 권역별 share + 좌표 (zone_stats_200 + subway_geo join)",
    "zone_coord_pdf = spark.sql(u\"\"\"",
    "    SELECT z.station, z.line, z.subway_share_pct,",
    "           AVG(sg.sta_lat) AS lat,",
    "           AVG(sg.sta_lon) AS lon",
    "    FROM zone_stats_200 z",
    "    JOIN subway_geo sg ON z.station = sg.`지하철역` AND z.line = sg.`호선명`",
    "    GROUP BY z.station, z.line, z.subway_share_pct",
    "\"\"\").toPandas()",
    "",
    "print('지도 표시 권역:', len(zone_coord_pdf), '개')",
    "",
    "def share_color(share):",
    "    if share is None: return 'gray'",
    "    if share < 33: return 'blue'",
    "    if share < 50: return 'lightblue'",
    "    if share < 67: return 'orange'",
    "    return 'red'",
    "",
    "m3 = folium.Map(location=SEOUL_CENTER, zoom_start=11, tiles='OpenStreetMap')",
    "",
    "for _, row in zone_coord_pdf.iterrows():",
    "    c = share_color(row['subway_share_pct'])",
    "    folium.CircleMarker(",
    "        location=(row['lat'], row['lon']),",
    "        radius=6,",
    "        popup=folium.Popup(",
    "            '<b>{}</b> ({})<br>subway_share: {:.1f}%'.format(",
    "                row['station'], row['line'], row['subway_share_pct']),",
    "            max_width=220,",
    "        ),",
    "        tooltip='{}: {:.1f}%'.format(row['station'], row['subway_share_pct']),",
    "        color=c,",
    "        fill=True,",
    "        fill_color=c,",
    "        fill_opacity=0.75,",
    "        weight=1,",
    "    ).add_to(m3)",
    "",
    "# 범례",
    "legend_html = '''",
    "<div style=\"position: fixed; bottom: 20px; left: 20px; width: 180px; padding: 10px;",
    "            background-color: white; border: 2px solid grey; z-index: 9999;",
    "            font-size: 13px; font-family: sans-serif;\">",
    "<b>subway_share (200m)</b><br>",
    "<i style=\"background:red;width:14px;height:14px;display:inline-block;border-radius:50%\"></i> 67%+ 지하철 강세<br>",
    "<i style=\"background:orange;width:14px;height:14px;display:inline-block;border-radius:50%\"></i> 50~67%<br>",
    "<i style=\"background:lightblue;width:14px;height:14px;display:inline-block;border-radius:50%\"></i> 33~50%<br>",
    "<i style=\"background:blue;width:14px;height:14px;display:inline-block;border-radius:50%\"></i> ~33% 버스 강세<br>",
    "</div>",
    "'''",
    "m3.get_root().html.add_child(folium.Element(legend_html))",
    "",
    "m3",
))

# ============================================================
# 4. 저장
# ============================================================
notebook = {
    "cells": cells,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.x"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

NB_PATH = HERE / "q1_q2_q3_analysis_local.ipynb"
with open(NB_PATH, "w", encoding="utf-8") as f:
    json.dump(notebook, f, ensure_ascii=False, indent=1)

print("Wrote:", NB_PATH)
print("Total cells:", len(cells))
