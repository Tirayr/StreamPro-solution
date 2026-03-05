-- ==========================================
-- StreamPro Data Transformation (SQL)
-- ==========================================

-- ------------------------------------------
-- 1. Dimensions
-- ------------------------------------------
-- dim_users
CREATE OR REPLACE TABLE streampro_silver.dim_users AS
SELECT
    user_id,
    SAFE.PARSE_DATE('%d/%m/%Y', signup_date) AS signup_date,
    subscription_tier,
    age_group,
    gender,
    email
FROM streampro_bronze.raw_users;

-- dim_videos
CREATE OR REPLACE TABLE streampro_silver.dim_videos AS
SELECT
    video_id,
    title,
    genre,
    CAST(duration_seconds AS INT64) AS duration_seconds,
    patent_id
FROM streampro_bronze.raw_videos;

-- dim_devices
CREATE OR REPLACE TABLE streampro_silver.dim_devices AS
SELECT
    device AS device_type,
    os,
    model,
    os_version,
    TO_HEX(MD5(CONCAT(device, os, model, os_version))) AS device_pk -- Assuming a composite PK based on attributes
FROM streampro_bronze.raw_devices;

-- ------------------------------------------
-- 2. Fact Tables
-- ------------------------------------------

-- fact_events
CREATE OR REPLACE TABLE streampro_silver.fact_events AS
SELECT
    TO_HEX(MD5(CONCAT(session_id, timestamp, event_name))) AS event_id, -- surrogate key
    session_id,
    user_id,
    video_id,
    event_name,
    SAFE_CAST(timestamp AS TIMESTAMP) AS event_timestamp,
    CAST(value AS FLOAT64) AS value,
    device_os,
    app_version
FROM streampro_bronze.raw_events;

-- fact_sessions
CREATE OR REPLACE TABLE streampro_silver.fact_sessions AS
WITH session_agg AS (
    SELECT
        session_id,
        user_id,
        MIN(event_timestamp) AS session_start_time,
        MAX(event_timestamp) AS session_end_time,
        SUM(CASE WHEN event_name = 'watch_time' THEN value ELSE 0 END) AS total_watch_time,
        -- Check if reaction occurred
        LOGICAL_OR(event_name = 'heart') AS has_heart_reaction
    FROM streampro_silver.fact_events
    GROUP BY session_id, user_id
),
first_events AS (
    SELECT DISTINCT
        session_id,
        FIRST_VALUE(video_id) OVER(PARTITION BY session_id ORDER BY event_timestamp ASC) AS first_video_id,
        FIRST_VALUE(device_os) OVER(PARTITION BY session_id ORDER BY event_timestamp ASC) AS first_device_os,
        FIRST_VALUE(app_version) OVER(PARTITION BY session_id ORDER BY event_timestamp ASC) AS first_app_version
    FROM streampro_silver.fact_events
),
ranked_sessions AS (
    SELECT
        s.session_id,
        s.user_id,
        s.session_start_time,
        s.session_end_time,
        s.total_watch_time,
        s.has_heart_reaction,
        f.first_video_id,
        f.first_device_os,
        f.first_app_version,
        ROW_NUMBER() OVER(PARTITION BY s.user_id ORDER BY s.session_start_time ASC) AS session_rank
    FROM session_agg s
    JOIN first_events f ON s.session_id = f.session_id
)
SELECT * FROM ranked_sessions;

-- ------------------------------------------
-- 3. Primary & Foreign Keys (Metadata)
-- ------------------------------------------
-- In BigQuery, PK/FK constraints are NOT ENFORCED during DML, 
-- but they are highly recommended as they enable the query optimizer to perform join elimination.

-- Primary Keys
ALTER TABLE streampro_silver.dim_users ADD PRIMARY KEY (user_id) NOT ENFORCED;
ALTER TABLE streampro_silver.dim_videos ADD PRIMARY KEY (video_id) NOT ENFORCED;
-- dim_devices relies on the derived device_pk, so we apply it to that column
ALTER TABLE streampro_silver.dim_devices ADD PRIMARY KEY (device_pk) NOT ENFORCED;
ALTER TABLE streampro_silver.fact_events ADD PRIMARY KEY (event_id) NOT ENFORCED;
ALTER TABLE streampro_silver.fact_sessions ADD PRIMARY KEY (session_id) NOT ENFORCED;

-- Foreign Keys
ALTER TABLE streampro_silver.fact_events
    ADD FOREIGN KEY (user_id) REFERENCES streampro_silver.dim_users(user_id) NOT ENFORCED,
    ADD FOREIGN KEY (video_id) REFERENCES streampro_silver.dim_videos(video_id) NOT ENFORCED,
    ADD FOREIGN KEY (session_id) REFERENCES streampro_silver.fact_sessions(session_id) NOT ENFORCED;

ALTER TABLE streampro_silver.fact_sessions
    ADD FOREIGN KEY (user_id) REFERENCES streampro_silver.dim_users(user_id) NOT ENFORCED,
    ADD FOREIGN KEY (first_video_id) REFERENCES streampro_silver.dim_videos(video_id) NOT ENFORCED;
