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
    "SparkSession 생성 + 데이터 경로 자동 감지.",
    "",
    "### `DATA_BASE` 우선순위",
    "1. **환경변수 `DATA_BASE`** (명시 — 권장)",
    "2. 작업 디렉토리 기준 자동 탐색 (`./data`, `../data`, `../../data` 중 `processed/subway` 또는 `raw/coords` 가 있는 곳)",
    "3. **Sandbox HDFS** (`/user/maria_dev/seoul`) — 마지막 폴백",
    "",
    "**Windows cmd**: `set DATA_BASE=C:\\path\\to\\data`",
    "**Linux/macOS**: `export DATA_BASE=/path/to/data`",
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
    "from pathlib import Path",
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
    "    .appName('BP_Q1Q2Q3_local')",
    "    .config('spark.sql.shuffle.partitions', '200')",
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
    "print('DATA_BASE     :', DATA_BASE, '(source: {})'.format(_src))",
    "print('  subway      :', PARQUET_SUBWAY)",
    "print('  bus         :', PARQUET_BUS)",
    "print('  bus_coord   :', CSV_BUS_COORD)",
    "print('  sub_coord   :', CSV_SUB_COORD)",
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
# 3. 저장
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
