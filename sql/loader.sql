-- ==========================================
-- StreamPro Data Loader Queries (SQL)
-- ==========================================
-- -------------------------------------------------------------------------------------------------
-- 1. Create Raw / Staging Tables
-- -------------------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE streampro_bronze.raw_users (
    user_id STRING,
    signup_date STRING,
    subscription_tier STRING,
    age_group STRING,
    gender STRING,
    email STRING
);

CREATE OR REPLACE TABLE streampro_bronze.raw_videos (
    video_id STRING,
    title STRING,
    genre STRING,
    duration_seconds STRING,
    patent_id STRING
);

CREATE OR REPLACE TABLE streampro_bronze.raw_devices (
    device STRING,
    os STRING,
    model STRING,
    os_version STRING
);

CREATE OR REPLACE TABLE streampro.raw_events (
    timestamp STRING,
    account_id STRING,
    video_id STRING,
    user_id STRING,
    event_name STRING,
    value STRING,
    device STRING,
    app_version STRING,
    device_os STRING,
    network_type STRING,
    ip STRING,
    country STRING,
    session_id STRING
);

-- -------------------------------------------------------------------------------------------------
-- 2. Load Data from Cloud Storage into Raw Tables
-- -------------------------------------------------------------------------------------------------

-- Load Users (CSV)
LOAD DATA INTO streampro_bronze.raw_users (
    user_id STRING,
    signup_date STRING,
    subscription_tier STRING,
    age_group STRING,
    gender STRING,
    email STRING
)
FROM FILES (
  format = 'CSV',
  uris = ['gs://steampro-dev/data/users.csv'],
  skip_leading_rows = 1
);



-- Load Videos (CSV)
LOAD DATA INTO streampro_bronze.raw_videos (
    video_id STRING,
    title STRING,
    genre STRING,
    duration_seconds STRING,
    patent_id STRING
)
FROM FILES (
  format = 'CSV',
  uris = ['gs://steampro-dev/data/videos.csv'],
  skip_leading_rows = 1
);

-- Load Devices (CSV)
LOAD DATA INTO streampro_bronze.raw_devices (
    device STRING,
    os STRING,
    model STRING,
    os_version STRING
)
FROM FILES (
  format = 'CSV',
  uris = ['gs://steampro-dev/data/devices.csv'],
  skip_leading_rows = 1
);

-- Load Events (JSON)
LOAD DATA INTO streampro_bronze.raw_events (
    timestamp STRING,
    account_id STRING,
    video_id STRING,
    user_id STRING,
    event_name STRING,
    `value` STRING,
    device STRING,
    app_version STRING,
    device_os STRING,
    network_type STRING,
    ip STRING,
    country STRING,
    session_id STRING
)FROM FILES (
  format = 'JSON',
  uris = ['gs://steampro-dev/data/events.json']
);
