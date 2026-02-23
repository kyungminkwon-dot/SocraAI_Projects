권경민 | Data Analyst Portfolio
데이터 수집부터 변환, 적재, 시각화까지 전 생애주기를 설계하고 구축한 경험을 기반으로, 데이터로 의사결정을 돕는 분석가를 지향합니다.
Tech Stack
분류기술LanguageSQL, PythonData PlatformDatabricks, RedashData TransformationdbtWorkflowAirflowVisualizationDatabricks Dashboard, StreamlitCollaborationGit, Notion, Slack

Projects
1. 리얼스피킹 분석 시스템 구축

SocraAI 데이터 분석가 인턴 (2025.08 ~ 2025.12)
분석 환경이 갖춰지지 않은 상태에서, 데이터 파이프라인 설계부터 대시보드 구축까지 전 과정을 수행했습니다.

1-1. 데이터 파이프라인 설계 및 제작
문제

재현·유지보수 가능한 ETL 파이프라인 부재
Raw 테이블의 컬럼명·타입·스키마 비표준화
다수 도메인의 이벤트 로그가 각각 다른 스키마로 산재

해결

dbt 기반 계층형 파이프라인 설계 (Source → Staging → Core → Mart)
4개 도메인의 이질적 스키마를 통합한 이벤트 Fact 테이블 설계 (event_type / subtype 분류 체계 정의)
Intermediate 레이어에서 복잡한 비즈니스 룰 사전 처리 (구매/이용권 기간 병합, 유저 세그먼트, 프로필 피벗)
파생 필터 컬럼 및 GROUP BY CUBE 활용한 다차원 집계 Mart 설계
Incremental 전략 적용 (7일 Lookback Window 기반 증분 적재)

성과

전사 최초 dbt 기반 제품 데이터 파이프라인 구축 및 운영
리얼스피킹 데이터 전처리 기준 및 지표 정의 표준화, 메타데이터 문서화

1-2. 제품 대시보드 제작
문제

기존 Redash 기반 대시보드의 필터·집계 유연성 부족
서비스 핵심 지표를 통합 모니터링할 환경 부재

해결

Databricks Dashboard 기반 전면 재구축 (Global Filter 적용)
6단계 유저 퍼널 체계 설계 (가입 → 프로필 생성 → 활동 시작 → 대화 진행 → 대화 완료 → 반복 완료)
3차 리뷰 사이클을 통한 점진적 고도화: V1(핵심 지표) → V2(매출/환불, UX 개선) → V3(2.0 신규 지표)

성과

리얼실 구성원이 현재까지 직접 사용하는 셀프서비스 대시보드 구축 및 운영
활용 사례: 마케팅팀에서 메이트 순위 차트 기반 배너 노출 순서 조정


2. Meta Kaggle Hackathon — Trends Over Time 트랙 1위

5인 팀 팀장 | 119팀 중 1위
Meta Kaggle 데이터셋(42GB, 41 테이블, ~24M 유저)을 분석하여 Kaggle 플랫폼의 성장 전략을 도출했습니다.

2-1. 데이터 전처리
문제

42GB 원천 데이터, 로컬 환경에서 메모리 초과(OOM) 발생
41개 테이블 간 관계 파악 및 분석 목적별 데이터셋 구축 필요

해결

DuckDB(CPU 기반 조회·병합) + Polars(병렬 처리 전처리) 활용으로 OOM 해결
교차 검증 기반 이상치 탐지, 50+ 파생 변수 생성

2-2. 시계열 회귀 최적화
문제

초기 모델 성능 부족 (R² 0.24, MAE 3,534)

해결

5단계 최적화 수행: Baseline → 파생변수 추가 → PolynomialFeatures → FLAML AutoML → SHAP 기반 변수 선택
최종 성능: R² 0.89, MAE 974 달성

2-3. 분석 인사이트 도출
문제

Kaggle 플랫폼의 유저 이탈 원인 분석 필요

주요 인사이트

인센티브 구조 불균형 발견: 랭킹 보유자 0.04%, 상금 수혜자 0.28%, 티어 보유자 1.07% → 98.93%의 유저가 보상 체계 밖에 위치
AS-IS(경쟁지표 위주 보상) → TO-BE(커뮤니티 기여 기반 보상 체계) 전략 제안


Education
기간내용2019.03 ~ 2025.02세종대학교 경영학과 졸업 (4.42/4.5, 수석, Dean's List)2025.03 ~ 2025.07KDT 실무형 데이터 분석가 양성과정 수료 (리더상)
Certifications

ADsP (데이터분석 준전문가)
SQLD (SQL 개발자)
컴퓨터활용능력 1급
한국사능력검정시험 1급


Contact

Email: qpo134@naver.com
