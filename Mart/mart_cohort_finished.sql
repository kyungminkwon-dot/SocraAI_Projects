{{
    config(
        materialized='table'
    )
}}

{{
    simple_cte(
        [
            ('fct_event', 'fct_usage_event_master'),
            ('dim_user',  'dim_user_profile_master')
        ]
    )
}}

-- 1) 기준 코호트 정의 (최초 결제월 기준)
, cohort_base AS (
  SELECT 
    u.user_id,
    DATE_TRUNC('month', FROM_UTC_TIMESTAMP(u.first_payment_at, 'Asia/Seoul')) AS cohort_month,
    u.product_type,
    u.user_gender,
    u.user_age_group
  FROM dim_user u
  WHERE 1=1 
    AND u.user_segment = 'PREMIUM'
    AND u.account_status = 'ACTIVE_PAID'
)

-- 2) 코호트 사이즈 산출 (분모)
, cohort_summary AS (
  SELECT
    cohort_month,
    product_type,
    user_gender,
    user_age_group,
    COUNT(DISTINCT user_id) AS total_cohort_size
  FROM cohort_base
  {{ dbt_utils.group_by(n=4) }}
)

-- 3) 유효 활동 이벤트 집계 (월 단위)
, active_events AS (
  SELECT
      e.user_id,
      DATE_TRUNC('month', e.event_at_kst) AS activity_month,
      CASE 
        WHEN e.event_type = 'CORE_ACTION_A' AND e.event_subtype IN ('COMPLETE_A', 'PROCEED_A') THEN 'ACTION_GROUP_01'
        WHEN e.event_type = 'CORE_ACTION_B' AND e.event_subtype = 'COMPLETE_B' THEN 'ACTION_GROUP_02'
        WHEN e.event_type = 'CORE_ACTION_C' AND e.event_subtype = 'COMPLETE_C' THEN 'ACTION_GROUP_03'
      END AS activity_category
  FROM fct_event e
  JOIN dim_user u
    USING (dim_user_id)
  WHERE 1=1 
    AND u.user_segment = 'PREMIUM'
    AND (
        (e.event_type = 'CORE_ACTION_A' AND e.event_subtype IN ('COMPLETE_A', 'PROCEED_A'))
        OR (e.event_type = 'CORE_ACTION_B' AND e.event_subtype = 'COMPLETE_B')
        OR (e.event_type = 'CORE_ACTION_C' AND e.event_subtype = 'COMPLETE_C')
    )
)

-- 4) 코호트와 활동 데이터 매핑 (Month Offset 계산)
, interaction_mapped AS (
  SELECT
      c.user_id,
      c.cohort_month,
      c.product_type,
      c.user_gender,
      c.user_age_group,
      a.activity_month,
      a.activity_category,
      CAST(MONTHS_BETWEEN(a.activity_month, c.cohort_month) AS INT) AS month_offset
  FROM active_events a
  JOIN cohort_base c
    ON a.user_id = c.user_id
  WHERE a.activity_month >= c.cohort_month
    AND CAST(MONTHS_BETWEEN(a.activity_month, c.cohort_month) AS INT) BETWEEN 0 AND 6
)

-- 5) 분석용 그리드 (Offsets & Categories)
, ref_offsets AS (
  SELECT 0 AS month_offset UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL
  SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6
)

, ref_categories AS (
  SELECT 'ACTION_GROUP_01' AS activity_category
  UNION ALL SELECT 'ACTION_GROUP_02'
  UNION ALL SELECT 'ACTION_GROUP_03'
)

-- 6) 월별 단순 유지 유저수 집계 (분자 1)
, monthly_retention AS (
  SELECT
    cohort_month,
    month_offset,
    activity_category,
    product_type,
    user_gender,
    user_age_group,
    COUNT(DISTINCT user_id) AS active_user_count
  FROM interaction_mapped
  {{ dbt_utils.group_by(n=6) }}
)

-- 7) 연속 유지(Streak) 분석을 위한 유저별 활동 매트릭스 생성
, user_activity_matrix AS (
  SELECT
    c.user_id,
    c.cohort_month,
    cat.activity_category,
    c.product_type,
    c.user_gender,
    c.user_age_group,
    o.month_offset,
    CASE WHEN m.user_id IS NOT NULL THEN 1 ELSE 0 END AS is_active
  FROM (SELECT DISTINCT user_id, cohort_month, product_type, user_gender, user_age_group FROM cohort_base) c
  CROSS JOIN ref_offsets o
  CROSS JOIN ref_categories cat
  LEFT JOIN interaction_mapped m
    ON m.user_id = c.user_id
   AND m.cohort_month = c.cohort_month
   AND m.activity_category = cat.activity_category
   AND m.month_offset = o.month_offset
)

-- 8) Window Function을 활용한 연속 유지 달성 여부 판단
, streak_calculation AS (
  SELECT
    *,
    MIN(is_active) OVER (
      PARTITION BY user_id, cohort_month, activity_category, product_type, user_gender, user_age_group
      ORDER BY month_offset
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS is_streak_maintained
  FROM user_activity_matrix
)

-- 9) 코호트×오프셋×카테고리별 집계 레이어
, aggregation_base AS (
  SELECT
    cohort_month,
    month_offset,
    activity_category,
    product_type,
    SUM(total_cohort_size) AS total_size,
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_users,
    SUM(is_streak_maintained) AS streak_maintained_users
  FROM streak_calculation
  {{ dbt_utils.group_by(n=4) }}
)

-- 10) 최종 계산 및 정리 (Final Layer)
, final AS (
  SELECT 
    cohort_month,
    month_offset,
    activity_category,
    product_type,
    total_size AS cohort_size,
    active_users,
    ROUND(100.0 * active_users / total_size, 2) AS retention_pct,
    streak_maintained_users,
    ROUND(100.0 * streak_maintained_users / total_size, 2) AS streak_retention_pct
  FROM aggregation_base
  WHERE total_size > 0
  ORDER BY cohort_month, month_offset, activity_category, product_type
)

SELECT * FROM final
