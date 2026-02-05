{{
    config(
        materialized='view'
    )
}}

{{
    simple_cte([
        ('fct_event',   'fct_usage_event_master'),
        ('dim_user',    'dim_user_profile_master'),
        ('dim_date',    'dim_date_calendar'),
    ])
}}

-- 1) 기준 코호트 생성: 결제 완료 유저 대상 (KST 기준 주 단위 그룹화)
, step1 AS (
  SELECT
      u.user_id,
      from_utc_timestamp(u.recent_conversion_at, 'Asia/Seoul') AS conversion_at_kst,
      dd.week_start_date                                      AS cohort_week_start,
      dd.week_end_date                                        AS cohort_week_end,
      u.item_category,
      u.revenue_model,
      u.payment_status,
      u.new_customer_flag,
      u.is_verified_profile
  FROM dim_user u
  JOIN dim_date dd ON to_date(from_utc_timestamp(u.recent_conversion_at, 'Asia/Seoul')) = dd.date_day
  WHERE 1=1 
    AND u.user_segment = 'PREMIUM_GROUP'
    AND u.payment_status IS NOT NULL
)

-- 2) 코호트 유저의 전체 활동 이벤트 매핑
, step2 AS (
  SELECT
      e.user_id,
      e.event_type,
      e.event_subtype,
      e.event_date_at_kst,
      s1.conversion_at_kst,
      s1.cohort_week_start,
      s1.cohort_week_end,
      s1.item_category,
      s1.revenue_model,
      s1.payment_status,
      s1.new_customer_flag,
      s1.is_verified_profile
  FROM fct_event e
  JOIN step1 s1 ON s1.user_id = e.user_id
)

-- 3) 프로필 인증 완료 유저 필터링 (활성 유저 기준)
, step3 AS (
  SELECT
      *
  FROM step2
  WHERE 1=1 
    AND is_verified_profile IS TRUE
)

-- 4) 핵심 액션 발생 유저 필터링 (단순 수신 이벤트를 제외한 능동적 액션)
, step4 AS (
  SELECT * FROM step3
  WHERE 1=1 
    AND event_type IN ('CORE_ACTION_TYPE_A', 'CORE_ACTION_TYPE_B')
    AND event_subtype <> 'PASSIVE_RECEIVED'
)

-- 5) 목표 액션 완료 유저 (최종 단계 도달 확인)
, step5 AS (
  SELECT * FROM step4
  WHERE 1=1 
    AND event_subtype IN ('STATUS_COMPLETED_V1', 'STATUS_COMPLETED_V2')
)

-- 6) 고착도 검증: 주당 2회 이상 목표 액션 달성 유저 (Retention Driver)
, step6 AS (
  SELECT DISTINCT
      user_id,
      cohort_week_start,
      cohort_week_end,
      item_category,
      revenue_model,
      payment_status,
      new_customer_flag
  FROM (
    SELECT
      s5.user_id,
      s5.cohort_week_start,
      s5.cohort_week_end,
      s5.item_category,
      s5.revenue_model,
      s5.payment_status,
      s5.new_customer_flag,
      dd.week_start_date,
      COUNT(*) AS completed_cnt
    FROM step5 s5
    JOIN dim_date dd ON s5.event_date_at_kst = dd.date_day
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
  ) w
  WHERE completed_cnt >= 2
)

-- 7) 시각화를 위한 단계별 데이터 통합 (Long-form Structure)
, final AS (
  SELECT '01_PAYMENT_COMPLETED' AS step_name, item_category, revenue_model, cohort_week_start, cohort_week_end, COUNT(DISTINCT user_id) AS user_cnt
  FROM step1
  GROUP BY 1, 2, 3, 4, 5

  UNION ALL
  SELECT '02_TOTAL_INTERACTION', item_category, revenue_model, cohort_week_start, cohort_week_end, COUNT(DISTINCT user_id)
  FROM step2
  GROUP BY 1, 2, 3, 4, 5

  UNION ALL
  SELECT '03_PROFILE_VERIFIED', item_category, revenue_model, cohort_week_start, cohort_week_end, COUNT(DISTINCT user_id)
  FROM step3
  GROUP BY 1, 2, 3, 4, 5

  UNION ALL
  SELECT '04_ACTIVE_ENGAGED', item_category, revenue_model, cohort_week_start, cohort_week_end, COUNT(DISTINCT user_id)
  FROM step4
  GROUP BY 1, 2, 3, 4, 5

  UNION ALL
  SELECT '05_GOAL_ACHIEVED', item_category, revenue_model, cohort_week_start, cohort_week_end, COUNT(DISTINCT user_id)
  FROM step5
  GROUP BY 1, 2, 3, 4, 5

  UNION ALL
  SELECT '06_LOYAL_USER_2P', item_category, revenue_model, cohort_week_start, cohort_week_end, COUNT(DISTINCT user_id)
  FROM step6
  GROUP BY 1, 2, 3, 4, 5
)

SELECT * FROM final
