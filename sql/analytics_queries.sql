-- ==========================================
-- StreamPro Analytics Queries (SQL) / Gold Layer
-- ==========================================

-- -------------------------------------------------------------------------------------------------
-- 4.1 What percentage of new users reach at least 30 seconds of watch_time in their first session?
-- -------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE streampro_gold.kpi_new_user_conversion AS
WITH first_sessions AS (
    SELECT 
        user_id,
        total_watch_time
    FROM streampro_silver.fact_sessions
    WHERE session_rank = 1
)
SELECT 
    SUM(CASE WHEN total_watch_time >= 30 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS percentage_new_users_30s
FROM first_sessions;

-- -------------------------------------------------------------------------------------------------
-- 4.2 Which video genres drive the highest second-session retention within 3 days?
-- -------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE streampro_gold.kpi_retention_by_genre AS
WITH first_session_genres AS (
    -- Get all unique genres watched by each user in their first session
    SELECT DISTINCT
        s.user_id,
        s.session_start_time,
        v.genre
    FROM streampro_silver.fact_sessions s
    JOIN streampro_silver.fact_events e ON s.session_id = e.session_id
    JOIN streampro_silver.dim_videos v ON e.video_id = v.video_id
    WHERE s.session_rank = 1
),
second_session_genres AS (
    -- Get the start time and all unique genres watched in the user's second session
    SELECT DISTINCT
        s.user_id,
        s.session_start_time,
        v.genre
    FROM streampro_silver.fact_sessions s
    JOIN streampro_silver.fact_events e ON s.session_id = e.session_id
    JOIN streampro_silver.dim_videos v ON e.video_id = v.video_id
    WHERE s.session_rank = 2
)
SELECT 
    fsg.genre,
    COUNT(DISTINCT fsg.user_id) AS total_first_session_users,
    COUNT(DISTINCT ssg.user_id) AS retained_users_in_same_genre,
    COUNT(DISTINCT ssg.user_id) * 100.0 / NULLIF(COUNT(DISTINCT fsg.user_id), 0) AS genre_retention_rate_pct
FROM first_session_genres fsg
LEFT JOIN second_session_genres ssg 
    ON fsg.user_id = ssg.user_id 
    AND fsg.genre = ssg.genre
    AND TIMESTAMP_DIFF(ssg.session_start_time, fsg.session_start_time, HOUR) <= 72
GROUP BY fsg.genre
ORDER BY genre_retention_rate_pct DESC;
-- -------------------------------------------------------------------------------------------------
-- 4.3 Are there specific device_os or app_version combinations where user drop-off is abnormally high?
-- -------------------------------------------------------------------------------------------------
-- Assuming "abnormal drop-off" is defined as a session where the user watched for less than 1% of total duration of a video
CREATE OR REPLACE TABLE streampro_gold.kpi_device_drop_off AS
WITH session_metrics AS (
    -- Session level outcomes with dynamic threshold
    SELECT 
        s.session_id,
        s.total_watch_time,
        v.duration_seconds,
        (v.duration_seconds * 0.01) AS drop_off_threshold_seconds
    FROM streampro_silver.fact_sessions s
    JOIN streampro_silver.dim_videos v
        ON s.first_video_id = v.video_id
),
session_devices AS (
    -- All unique devices used in each session (e.g. if they switched device mid-session)
    SELECT DISTINCT 
        session_id,
        device_os,
        app_version
    FROM streampro_silver.fact_events
),
device_session_mapping AS (
    -- Map outcomes back to all devices involved
    SELECT 
        device_os,
        app_version,
        m.session_id,
        m.total_watch_time,
        m.drop_off_threshold_seconds
    FROM session_devices sd
    JOIN session_metrics m 
        ON sd.session_id = m.session_id
)
SELECT 
    device_os,
    app_version,
    COUNT(session_id) AS total_sessions,
    COUNT(CASE WHEN total_watch_time < drop_off_threshold_seconds THEN 1 END) AS drop_off_sessions,
    COUNT(CASE WHEN total_watch_time < drop_off_threshold_seconds THEN 1 END) * 100.0 / NULLIF(COUNT(session_id), 0) AS drop_off_rate_pct
FROM device_session_mapping
GROUP BY device_os, app_version
ORDER BY drop_off_rate_pct DESC
;
