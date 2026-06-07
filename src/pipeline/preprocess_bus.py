# -*- coding: utf-8 -*-
"""
Bus raw CSV → long format Parquet 전처리
- UTF-8 인코딩 (서울 열린데이터광장 최신 API 기준)
- 영문 컬럼명을 사이트 공식 한글 명칭으로 rename
- HR_1만 NOPE, 나머지 시간대는 TNOPE 인 데이터셋 inconsistency 정규식으로 통합 매칭
- 메타키 기준 중복 제거
- wide → long 변환
- year/month 파티션으로 Parquet 저장

실행:
    spark-submit --master yarn --deploy-mode client \\
                 --num-executors 2 --executor-memory 1g \\
                 preprocess_bus.py
"""
import functools
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import LongType


SOURCE = "/user/maria_dev/seoul/raw/bus/*.csv"
TARGET = "/user/maria_dev/seoul/processed/bus"

# 영문 → 한글 매핑 (서울 열린데이터광장 공식 명칭)
BUS_META_MAP = {
    "USE_YM":           "사용년월",
    "RTE_NO":           "노선번호",
    "RTE_NM":           "노선명",
    "STOPS_ID":         "표준버스정류장ID",
    "STOPS_ARS_NO":     "버스정류장ARS번호",
    "SBWY_STNS_NM":     "역명",
    "TRFC_MNS_TYPE_CD": "교통수단타입코드",
    "TRFC_MNS_TYPE_NM": "교통수단타입명",
    "REG_YMD":          "등록일자",
}

# HR_1만 NOPE, 나머지 시간대는 TNOPE → 정규식으로 둘 다 매칭
HOUR_PAT = re.compile(r"HR_(\d+)_GET_(ON|OFF)_T?NOPE")

# 메타 키 (노선 × 정류장 × 사용년월 유일 보장)
META_KEYS = ["사용년월", "노선번호", "표준버스정류장ID"]


def parse_hour_col(en_col):
    """예: HR_0_GET_ON_TNOPE -> (0, '승차총승객수'),
           HR_1_GET_OFF_NOPE -> (1, '하차총승객수')"""
    m = HOUR_PAT.match(en_col)
    if not m:
        return None, None
    h = int(m.group(1))
    kind = "승차총승객수" if m.group(2) == "ON" else "하차총승객수"
    return h, kind


def main():
    spark = SparkSession.builder.appName("PreprocessBus").getOrCreate()

    df = (spark.read
          .option("header", "true")
          .option("encoding", "UTF-8")
          .option("inferSchema", "false")
          .csv(SOURCE))

    for en, kr in BUS_META_MAP.items():
        df = df.withColumnRenamed(en, kr)

    # 중복 제거 (subway 202603 사례처럼 일부 월에 row 중복 가능성 대비)
    before = df.count()
    df = df.dropDuplicates(META_KEYS)
    after = df.count()
    print("[Dedup] {} -> {} rows ({} removed)".format(before, after, before - after))

    # 시간대 컬럼 추출 (HR_1의 _NOPE / 나머지 _TNOPE 모두 매칭)
    hour_en_cols = [c for c in df.columns if HOUR_PAT.match(c)]
    print("[Hour cols] {}".format(len(hour_en_cols)))

    parts = []
    for en_col in hour_en_cols:
        h, kind = parse_hour_col(en_col)
        part = df.select(
            col("사용년월"),
            col("노선번호"),
            col("노선명"),
            col("표준버스정류장ID"),
            col("버스정류장ARS번호"),
            col("역명"),
            col("교통수단타입코드"),
            col("교통수단타입명"),
            col("등록일자"),
            lit(h).alias("시간"),
            lit(kind).alias("kind"),
            col(en_col).cast(LongType()).alias("count"),
        )
        parts.append(part)
    long_df = functools.reduce(lambda a, b: a.union(b), parts)

    pivoted = (long_df
               .groupBy("사용년월", "노선번호", "노선명",
                        "표준버스정류장ID", "버스정류장ARS번호", "역명",
                        "교통수단타입코드", "교통수단타입명", "등록일자",
                        "시간")
               .pivot("kind", ["승차총승객수", "하차총승객수"])
               .sum("count"))

    final = (pivoted
             .withColumn("년", (col("사용년월").cast("int") / lit(100)).cast("int"))
             .withColumn("월", (col("사용년월").cast("int") % lit(100)).cast("int"))
             .na.fill({"승차총승객수": 0, "하차총승객수": 0}))

    print("[Final] row count: {}".format(final.count()))
    final.show(5, truncate=False)

    (final.write
          .mode("overwrite")
          .partitionBy("년", "월")
          .parquet(TARGET))

    print("[Saved] {}".format(TARGET))
    spark.stop()


if __name__ == "__main__":
    main()
