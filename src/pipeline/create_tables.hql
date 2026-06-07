-- ============================================================================
-- Hive External Table 정의 (서울 지하철·버스 시간대별 이용 데이터)
--
-- 입력  : preprocess_subway.py / preprocess_bus.py 가 만든 Parquet
--         /user/maria_dev/seoul/processed/{subway,bus}/년=YYYY/월=MM/
-- 결과  : Hive 데이터베이스 `seoul` 아래 두 External Table 생성
--         - seoul.subway_hourly  (지하철 시간대별, 9컬럼 + 2 파티션)
--         - seoul.bus_hourly     (버스 시간대별, 12컬럼 + 2 파티션)
--
-- 컬럼명은 사이트 공식 한글 명칭을 그대로 사용.
-- Hive에서 한글 컬럼/파티션을 다룰 때는 백틱(`) 으로 감싸야 함.
--
-- 실행:
--     beeline -u jdbc:hive2://localhost:10000 -n maria_dev -f /tmp/create_tables.hql
-- ============================================================================

CREATE DATABASE IF NOT EXISTS seoul
COMMENT '서울시 대중교통 이용 패턴 분석'
LOCATION '/user/maria_dev/seoul/hive';

USE seoul;

-- ----------------------------------------------------------------------------
-- Subway (지하철 시간대별 승하차)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS subway_hourly;

CREATE EXTERNAL TABLE subway_hourly (
  `사용월`     STRING  COMMENT 'YYYYMM',
  `호선명`     STRING  COMMENT '예: 1호선, 경의선',
  `지하철역`   STRING  COMMENT '예: 서울역, 시청',
  `작업일자`   STRING  COMMENT '데이터 작업일자 YYYYMMDD',
  `시간`       INT     COMMENT '0~23',
  `승차인원`   BIGINT  COMMENT '해당 시간대 승차',
  `하차인원`   BIGINT  COMMENT '해당 시간대 하차'
)
PARTITIONED BY (`년` INT, `월` INT)
STORED AS PARQUET
LOCATION '/user/maria_dev/seoul/processed/subway';

-- Parquet 디렉토리의 모든 파티션을 메타스토어에 등록
MSCK REPAIR TABLE subway_hourly;

-- ----------------------------------------------------------------------------
-- Bus (버스 시간대별 승하차)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bus_hourly;

CREATE EXTERNAL TABLE bus_hourly (
  `사용년월`            STRING  COMMENT 'YYYYMM',
  `노선번호`            STRING  COMMENT '예: 100',
  `노선명`              STRING  COMMENT '예: 100번(하계동~용산구청)',
  `표준버스정류장ID`    STRING  COMMENT '예: 110000327',
  `버스정류장ARS번호`   STRING  COMMENT '예: 11428',
  `역명`                STRING  COMMENT '정류장명',
  `교통수단타입코드`    STRING,
  `교통수단타입명`      STRING  COMMENT '예: 서울간선버스',
  `등록일자`            STRING  COMMENT 'YYYYMMDD',
  `시간`                INT     COMMENT '0~23',
  `승차총승객수`        BIGINT,
  `하차총승객수`        BIGINT
)
PARTITIONED BY (`년` INT, `월` INT)
STORED AS PARQUET
LOCATION '/user/maria_dev/seoul/processed/bus';

MSCK REPAIR TABLE bus_hourly;

-- ----------------------------------------------------------------------------
-- 검증 쿼리 (beeline 실행시 결과로 출력됨)
-- ----------------------------------------------------------------------------
SHOW TABLES;

SELECT 'subway_hourly' AS table_name, COUNT(*) AS row_count FROM subway_hourly
UNION ALL
SELECT 'bus_hourly'    AS table_name, COUNT(*) AS row_count FROM bus_hourly;

SHOW PARTITIONS subway_hourly;
SHOW PARTITIONS bus_hourly;
