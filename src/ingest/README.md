# `src/ingest/` — 데이터 수집 가이드

서울 열린데이터광장 OpenAPI 를 **월별 분할 호출**하여 시간대별 승하차 CSV 를 수집한다.

| 파일 | 역할 |
|---|---|
| `fetch_seoul_data.ipynb` | OpenAPI 2종 월별 수집 → `data/raw/{subway,bus}_YYYYMM.csv` |

> ⚠ **수집 결과물은 이미 [`data/raw/`](../../data/raw) 에 동봉**되어 있다 (36개, 약 278MB).
> 재현 시 재수집은 불필요하며, 아래 절차는 직접 재수집할 때만 해당한다.

## 수집 대상 API

| 서비스명 | 데이터셋 | 인증키 변수 |
|---|---|---|
| `CardSubwayTime` | 지하철 시간대별 승하차 (OA-12913) | `SEOUL_API_KEY_1` |
| `CardBusTimeNew` | 버스 시간대별 승하차 (OA-12252) | `SEOUL_API_KEY_2` |

좌표 마스터(`data/coords/`)는 OA-21232·OA-15067 에서 1회 다운로드한 원본 CSV — 별도 수집 코드 없음 ([`data/README.md`](../../data/README.md) §1 참조).

## 실행 방법 (로컬 PC)

1. 의존성 설치: `pip install -r requirements.txt`
2. 서울 열린데이터광장에서 인증키 발급 후 프로젝트 루트 `.env` 에 설정 (`.env.example` 참조)
3. Jupyter 에서 `fetch_seoul_data.ipynb` 를 셀 순서대로 실행
   - 수집 기간(기본 2024.12~2026.05, 18개월)은 노트북 상단 설정 셀에서 변경 가능

## 정상 출력 기준

- `data/raw/` 에 `subway_YYYYMM.csv` 18개 + `bus_YYYYMM.csv` 18개 = **36개**
- 누적 약 278 MB, 인코딩 UTF-8
- 컬럼은 영문명 (`USE_MM`, `STTN`, `HR_n_GET_ON_NOPE` 등) — 한글 변환은 전처리([`src/pipeline/`](../pipeline)) 담당
