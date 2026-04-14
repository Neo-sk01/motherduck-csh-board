CREATE OR REPLACE VIEW v_logical_calls AS
SELECT
    cdr_id,
    to_id,
    start_time,
    answer_time,
    end_time,
    duration,
    caller_name,
    user_extension,
    CASE WHEN answer_time IS NULL AND duration = 0 THEN true ELSE false END AS is_abandoned,
    EXTRACT(HOUR FROM start_time)    AS call_hour,
    CAST(start_time AS DATE)         AS call_date,
    DAYOFWEEK(start_time)            AS day_of_week,
    DAYNAME(start_time)              AS day_name
FROM raw_cdrs
WHERE to_id IN ('16135949199', '6135949199');

CREATE OR REPLACE VIEW v_kpi_daily AS
WITH
  cdr_totals AS (
    SELECT
      call_date,
      COUNT(*)                                            AS kpi1_total_incoming,
      COUNT(*) FILTER (WHERE is_abandoned)                AS kpi2_dropped_cdr
    FROM v_logical_calls
    GROUP BY call_date
  ),
  queue_daily AS (
    SELECT
      CAST(interval_ts AS DATE) AS call_date,
      queue_label,
      SUM(volume) AS calls_offered
    FROM raw_queue_splits
    WHERE period_type = 'day'
    GROUP BY CAST(interval_ts AS DATE), queue_label
  ),
  queue_pivot AS (
    SELECT
      call_date,
      COALESCE(SUM(CASE WHEN queue_label = 'english' THEN calls_offered END), 0) AS kpi3_english,
      COALESCE(SUM(CASE WHEN queue_label = 'french'  THEN calls_offered END), 0) AS kpi4_french,
      COALESCE(SUM(CASE WHEN queue_label = 'ai'      THEN calls_offered END), 0) AS kpi5_ai
    FROM queue_daily
    GROUP BY call_date
  ),
  queue_stats_daily AS (
    SELECT
      date_start AS call_date,
      queue_label,
      abandoned_calls,
      average_talk_time
    FROM raw_queue_stats
    WHERE date_start = date_end
  ),
  stats_pivot AS (
    SELECT
      call_date,
      SUM(abandoned_calls) AS kpi2_dropped_queue,
      MAX(CASE WHEN queue_label = 'english' THEN average_talk_time END) AS kpi8_talk_english_sec,
      MAX(CASE WHEN queue_label = 'french'  THEN average_talk_time END) AS kpi8_talk_french_sec,
      MAX(CASE WHEN queue_label = 'ai'      THEN average_talk_time END) AS kpi8_talk_ai_sec
    FROM queue_stats_daily
    GROUP BY call_date
  )
SELECT
  c.call_date,
  c.kpi1_total_incoming,
  c.kpi2_dropped_cdr,
  COALESCE(s.kpi2_dropped_queue, 0)                                           AS kpi2_dropped_queue,
  COALESCE(qp.kpi3_english, 0)                                                AS kpi3_english,
  COALESCE(qp.kpi4_french, 0)                                                 AS kpi4_french,
  COALESCE(qp.kpi5_ai, 0)                                                     AS kpi5_ai,
  ROUND(c.kpi2_dropped_cdr * 100.0 / NULLIF(c.kpi1_total_incoming, 0), 2)     AS kpi6_pct_dropped,
  ROUND(COALESCE(qp.kpi3_english,0) * 100.0 / NULLIF(c.kpi1_total_incoming,0), 2) AS kpi7_pct_english,
  ROUND(COALESCE(qp.kpi4_french,0)  * 100.0 / NULLIF(c.kpi1_total_incoming,0), 2) AS kpi7_pct_french,
  ROUND(COALESCE(qp.kpi5_ai,0)      * 100.0 / NULLIF(c.kpi1_total_incoming,0), 2) AS kpi7_pct_ai,
  ROUND(COALESCE(s.kpi8_talk_english_sec,0) / 60.0, 2)                        AS kpi8_avg_min_english,
  ROUND(COALESCE(s.kpi8_talk_french_sec,0)  / 60.0, 2)                        AS kpi8_avg_min_french,
  ROUND(COALESCE(s.kpi8_talk_ai_sec,0)      / 60.0, 2)                        AS kpi8_avg_min_ai,
  COALESCE(qp.kpi3_english,0) + COALESCE(qp.kpi4_french,0) + COALESCE(qp.kpi5_ai,0) AS queue_offered_total,
  ROUND(
    ABS(c.kpi1_total_incoming - (COALESCE(qp.kpi3_english,0) + COALESCE(qp.kpi4_french,0) + COALESCE(qp.kpi5_ai,0)))
    * 100.0 / NULLIF(c.kpi1_total_incoming, 0), 2
  ) AS reconciliation_drift_pct
FROM cdr_totals c
LEFT JOIN queue_pivot qp ON c.call_date = qp.call_date
LEFT JOIN stats_pivot s  ON c.call_date = s.call_date
ORDER BY c.call_date;

CREATE OR REPLACE VIEW v_kpi9_dow_avg AS
WITH daily_totals AS (
  SELECT CAST(interval_ts AS DATE) AS call_date, SUM(volume) AS volume
  FROM raw_queue_splits
  WHERE period_type = 'day'
  GROUP BY CAST(interval_ts AS DATE)
)
SELECT
  DATE_TRUNC('month', call_date)  AS month,
  DAYNAME(call_date)              AS weekday_name,
  DAYOFWEEK(call_date)            AS weekday_num,
  ROUND(SUM(volume) * 1.0 / COUNT(DISTINCT call_date), 1) AS avg_calls
FROM daily_totals
GROUP BY DATE_TRUNC('month', call_date), DAYNAME(call_date), DAYOFWEEK(call_date)
ORDER BY month, weekday_num;

CREATE OR REPLACE VIEW v_kpi10_hourly_duration AS
SELECT
  call_date,
  call_hour,
  COUNT(*)                          AS call_count,
  ROUND(AVG(duration) / 60.0, 2)   AS avg_duration_min
FROM v_logical_calls
WHERE answer_time IS NOT NULL
GROUP BY call_date, call_hour
ORDER BY call_date, call_hour;
