# StreamPro Data Model

We adopt a **Medallion Architecture (Bronze / Silver / Gold)**, centered around a **Star Schema** in the Silver layer to easily support session-level analytics. 

```mermaid
erDiagram
    dim_users ||--o{ fact_sessions : "has"
    dim_users ||--o{ fact_events : "has"
    dim_videos ||--o{ fact_events : "watched_in"
    dim_videos ||--o{ fact_sessions : "first_video_watched"
    dim_devices ||--o{ fact_events : "origin_of"
    
    fact_sessions ||--o{ fact_events : "contains"
    
    %% Gold Layer Dependencies
    fact_sessions ||--o{ kpi_new_user_conversion : "aggregates into"
    fact_sessions ||--o{ kpi_retention_by_genre : "aggregates into"
    fact_sessions ||--o{ kpi_device_drop_off : "aggregates into"

    dim_users {
        string user_id PK
        timestamp signup_date
        string subscription_tier
        string age_group
        string gender
        string email
    }

    dim_videos {
        string video_id PK
        string title
        string genre
        int duration_seconds
        string patent_id
    }

    dim_devices {
        string device_type
        string os
        string model
        string os_version
        string device_pk PK "Composite or Surrogate"
    }

    fact_events {
        string event_id PK "Surrogate Key"
        string session_id FK
        string user_id FK
        string video_id FK
        string event_name
        timestamp event_timestamp
        float value
        string device_os
        string app_version
    }

    fact_sessions {
        string session_id PK
        string user_id FK
        timestamp session_start_time
        timestamp session_end_time
        int session_rank "1 for first session ever"
        float total_watch_time
        string first_video_id FK
        boolean has_heart_reaction
        string first_device_os
        string first_app_version
    }
    
    kpi_retention_by_genre {
        string genre PK
        int total_first_session_users
        int retained_users_in_same_genre
        float genre_retention_rate_pct
    }
```

**Key Design Decisions:**
1.  **Medallion Architecture:**
    *   **Bronze (`streampro_bronze`):** Raw, unmodified data ingested directly from `.csv` and `.json` files.
    *   **Silver (`streampro_silver`):** Cleaned, conformed dimensions (`dim_users`, `dim_videos`) and facts (`fact_events`, `fact_sessions`) that form the Star Schema.
    *   **Gold (`streampro_gold`):** Highly aggregated, business-level metric tables (e.g., `kpi_retention_by_genre`) ready for direct ingestion by BI dashboards.
2.  **`fact_sessions` Table:** To easily answer questions about sessions (drop-off, watch time per session, first session conversions), we heavily rely on aggregating events into a `fact_sessions` table in the Silver layer. This avoids complex window functions and self-joins in downstream analytical queries.
3.  **Genre Retention Logic:** As captured in `streampro_gold.kpi_retention_by_genre`, a user is only considered "retained" by a genre if they watch that *same exact genre* again during their second session within a 72-hour window.
