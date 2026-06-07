# -*- coding: utf-8 -*-
"""
Subway raw CSV → long format Parquet 전처리
- UTF-8 인코딩 (서울 열린데이터광장 최신 API 기준)
- 영문 컬럼명을 사이트 공식 한글 명칭으로 rename
- 메타키 기준 중복 제거 (subway_202603 등 2배 row 중복 처리)
- wide → long 변환 (HR_n_GET_ON/OFF_NOPE × 48 → 시간/승차인원/하차인원)
- year/month 파티션으로 Parquet 저장

실행:
    spark-submit --master yarn --deploy-mode client \\
                 --num-executors 2 --executor-memory 1g \\
                 preprocess_subway.py
"""
import functools

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import LongType


SOURCE = "/user/maria_dev/seoul/raw/subway/*.csv"
TARGET = "/user/maria_dev/seoul/processed/subway"

# 영문 → 한글 매핑 (서울 열린데이터광장 공식 명칭)
SUBWAY_META_MAP = {
    "USE_MM":          "사용월",
    "SBWY_ROUT_LN_NM": "호선명",
    "STTN":            "지하철역",
    "JOB_YMD":         "작업일자",
}

# 메타 키 (중복 제거 기준 — 한 (사용월, 호선, 역) 조합당 1행만 유지)
META_KEYS = ["사용월", "호선명", "지하철역"]


def parse_hour_col(en_col):
    """예: HR_4_GET_ON_NOPE -> (4, '승차인원')"""
    parts = en_col.split("_")          # ['HR', '4', 'GET', 'ON', 'NOPE']
    h = int(parts[1])
    kind = "승차인원" if "ON" in parts else "하차인원"
    return h, kind


def main():
    spark = SparkSession.builder.appName("PreprocessSubway").getOrCreate()

    # 1) Wide format 읽기
    df = (spark.read
          .option("header", "true")
          .option("encoding", "UTF-8")
          .option("inferSchema", "false")
          .csv(SOURCE))

    # 2) 메타 컬럼 한글로 rename
    for en, kr in SUBWAY_META_MAP.items():
        df = df.withColumnRenamed(en, kr)

    # 3) 메타키 기준 dedup (202603 등 정확히 2배 중복 사례 처리)
    before = df.count()
    df = df.dropDuplicates(META_KEYS)
    after = df.count()
    print("[Dedup] {} -> {} rows ({} removed)".format(before, after, before - after))

    # 4) 시간대 컬럼 추출
    hour_en_cols = [c for c in df.columns if c.startswith("HR_")]
    print("[Hour cols] {}".format(len(hour_en_cols)))

    # 5) 각 시간대 컬럼을 (사용월, 호선, 역, 작업일자, 시간, kind, count) 형태로 분리 후 union
    parts = []
    for en_col in hour_en_cols:
        h, kind = parse_hour_col(en_col)
        part = df.select(
            col("사용월"),
            col("호선명"),
            col("지하철역"),
            col("작업일자"),
            lit(h).alias("시간"),
            lit(kind).alias("kind"),
            col(en_col).cast(LongType()).alias("count"),
        )
        parts.append(part)
    long_df = functools.reduce(lambda a, b: a.union(b), parts)

    # 6) kind를 컬럼으로 피벗 → 한 row = (사용월, 호선, 역, 시간)
    pivoted = (long_df
               .groupBy("사용월", "호선명", "지하철역", "작업일자", "시간")
               .pivot("kind", ["승차인원", "하차인원"])
               .sum("count"))

    # 7) 년/월 파생 + 결측치 0으로
    final = (pivoted
             .withColumn("년", (col("사용월").cast("int") / lit(100)).cast("int"))
             .withColumn("월", (col("사용월").cast("int") % lit(100)).cast("int"))
             .na.fill({"승차인원": 0, "하차인원": 0}))

    print("[Final] row count: {}".format(final.count()))
    final.show(5, truncate=False)

    # 8) Parquet 저장 (년/월 파티션)
    (final.write
          .mode("overwrite")
          .partitionBy("년", "월")
          .parquet(TARGET))

    print("[Saved] {}".format(TARGET))
    spark.stop()


if __name__ == "__main__":
    main()
