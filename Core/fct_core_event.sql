-- simple_cte 테이블 및 각 컬럼명은 비식별화 처리되어있습니다.

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='fct_event__sk',
        on_schema_change='fail'
    )
}}

{{
    simple_cte(
        [
            ('msg_log',        'stg_service_a__message_logs'),
            ('session_info',   'stg_service_a__user_sessions'),
            ('interaction_log','stg_service_a__user_interactions'),
            ('user_master',    'stg_service_common__user_master'),
            ('account_base',   'stg_service_common__account_base'),
            ('activity_base',  'stg_service_b__activity_history'),
            ('activity_type',  'stg_service_b__activity_type_meta'),
            ('access_log',     'stg_service_b__daily_access_records'),
            ('content_session','stg_service_b__content_execution_session')
        ]
    )
}}

-- Incremental 업데이트를 위한 기준점 정의
{% if is_incremental() %}
, incr_base AS (
  SELECT COALESCE(MAX(event_at), TIMESTAMP('1970-01-01')) AS latest_ts
  FROM {{ this }}
)
{% endif %}

------------------------------------------------------------------------------------------------
-- 1) 시스템 접속 이벤트 (System Access)
------------------------------------------------------------------------------------------------
, base_access AS (
    SELECT
        -- 고유 식별자(SK) 생성
        {{ dbt_utils.generate_surrogate_key(['"service_b"','"access_records"','ar.record_id','"SYS_ACCESS_LOGIN"']) }} AS fct_event__sk,
        
        -- 차원 키 해시 처리
        XXHASH64(u.user_id) AS dim_user_id,
        XXHASH64(u.account_key) AS dim_account_id,
        XXHASH64('DUMMY_VAL') AS dim_aux_id,

        -- 비식별화된 식별자
        u.user_id AS user_id,
        u.account_key AS account_key,

        -- 세션 정보 통합
        XXHASH64('access_record', CAST(ar.record_id AS STRING)) AS session_id,
        'access_record' AS session_src,
        CAST(ar.record_id AS STRING) AS original_session_id,

        -- 이벤트 분류 표준화
        'SYSTEM' AS event_category,
        'LOGIN'  AS event_action,

        -- 시간대 표준화 (KST 변환)
        ar.created_at AS event_at,
        from_utc_timestamp(ar.created_at, 'Asia/Seoul')             AS event_at_local,
        to_date(from_utc_timestamp(ar.created_at, 'Asia/Seoul'))    AS event_date_local,

        -- 메타데이터
        CURRENT_TIMESTAMP() AS _dw_processed_at

    FROM access_log ar
    LEFT JOIN account_base ab ON ar.account_id = ab.account_id
    LEFT JOIN user_master u ON ab.master_key = u.master_key

    WHERE 1=1
      AND ar.created_at IS NOT NULL
      {% if is_incremental() %}
      AND ar.created_at > (SELECT latest_ts FROM incr_base) - INTERVAL 7 DAYS
      {% endif %}
)

------------------------------------------------------------------------------------------------
-- 2) 메인 콘텐츠 실행 이벤트 (Content Execution)
------------------------------------------------------------------------------------------------
, content_created AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['"service_b"','"content_session"','cs.session_id','"CONTENT_START"']) }} AS fct_event__sk,
        
        XXHASH64(u.user_id) AS dim_user_id,
        XXHASH64(u.account_key) AS dim_account_id,
        XXHASH64('DUMMY_VAL') AS dim_aux_id,

        u.user_id AS user_id,
        u.account_key AS account_key,

        XXHASH64('content_session', CAST(cs.session_id AS STRING)) AS session_id,
        'content_session' AS session_src,
        CAST(cs.session_id AS STRING) AS original_session_id,

        'CONTENT' AS event_category,
        'START'   AS event_action,

        cs.created_at AS event_at,
        from_utc_timestamp(cs.created_at, 'Asia/Seoul')             AS event_at_local,
        to_date(from_utc_timestamp(cs.created_at, 'Asia/Seoul'))    AS event_date_local,

        CURRENT_TIMESTAMP() AS _dw_processed_at

    FROM content_session cs
    LEFT JOIN account_base ab ON cs.account_id = ab.account_id
    LEFT JOIN user_master u ON ab.master_key = u.master_key

    WHERE 1=1
      AND cs.created_at IS NOT NULL
      AND cs.is_deleted IS FALSE
      {% if is_incremental() %}
      AND cs.created_at > (SELECT latest_ts FROM incr_base) - INTERVAL 7 DAYS
      {% endif %}
)   

------------------------------------------------------------------------------------------------
-- 3) 중단 후 재개 이벤트 (Virtual Event: Resumed)
-- 실제 로그 부재 상황을 비즈니스 로직으로 극복
------------------------------------------------------------------------------------------------
, content_resumed AS ( 
    SELECT
        -- 업데이트 일자를 포함하여 동일 세션 내 재개 이벤트 구분
        {{ dbt_utils.generate_surrogate_key(['"service_b"','"content_session"','cs.session_id','TO_DATE(from_utc_timestamp(cs.updated_at, \'Asia/Seoul\'))','"CONTENT_RESUMED"']) }} AS fct_event__sk,
        
        XXHASH64(u.user_id) AS dim_user_id,
        XXHASH64(u.account_key) AS dim_account_id,
        XXHASH64('DUMMY_VAL') AS dim_aux_id,

        u.user_id AS user_id,
        u.account_key AS account_key,

        XXHASH64('content_session', CAST(cs.session_id AS STRING)) AS session_id,
        'content_session' AS session_src,
        CAST(cs.session_id AS STRING) AS original_session_id,

        'CONTENT' AS event_category,
        'RESUMED' AS event_action,

        -- 재개 시점 집계 로직
        MIN(cs.updated_at) AS event_at,
        from_utc_timestamp(MIN(cs.updated_at), 'Asia/Seoul')             AS event_at_local,
        to_date(from_utc_timestamp(MIN(cs.updated_at), 'Asia/Seoul'))    AS event_date_local,

        CURRENT_TIMESTAMP() AS _dw_processed_at

    FROM content_session cs
    LEFT JOIN account_base ab ON cs.account_id = ab.account_id
    LEFT JOIN user_master u ON ab.master_key = u.master_key

    WHERE 1=1
      AND cs.updated_at IS NOT NULL
      AND cs.completed_at IS NULL  
      AND cs.is_deleted IS FALSE
      -- 비즈니스 로직 적용: 24시간 이상 경과 후 업데이트 시 '재개'로 정의
      AND DATEDIFF(cs.updated_at, cs.created_at) >= 1
      {% if is_incremental() %}
      AND cs.updated_at > (SELECT latest_ts FROM incr_base) - INTERVAL 7 DAYS
      {% endif %}

    GROUP BY cs.session_id, u.user_id, u.account_key, TO_DATE(from_utc_timestamp(cs.updated_at, 'Asia/Seoul'))
)

------------------------------------------------------------------------------------------------
-- 최종 유니온 및 결과 출력
------------------------------------------------------------------------------------------------
, final_union AS (
    SELECT * FROM base_access
    UNION ALL 
    SELECT * FROM content_created
    UNION ALL 
    SELECT * FROM content_resumed
)

SELECT * FROM final_union
