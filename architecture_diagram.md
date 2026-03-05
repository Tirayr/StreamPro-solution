# StreamPro Data Flow & Architecture

We utilize a **Medallion Architecture** to process data from raw ingestion to reporting-ready KPIs. The following diagram illustrates this high-level data flow.

```mermaid
graph TD
    subgraph Raw Data Sources
        E(events.json)
        U(users.csv)
        V(videos.csv)
        D(devices.csv)
    end

    subgraph Ingestion / Cloud Storage
        CS[Cloud Storage e.g. Amazon S3 / GCS]
    end

    subgraph streampro_bronze [Bronze Layer]
        StgE[raw_events]
        StgU[raw_users]
        StgV[raw_videos]
        StgD[raw_devices]
    end

    subgraph dbt_transform [Transformation Engine dbt/SQL]
        Clean[Data Cleaning & Deduplication]
        Sessionize[Sessionization Logic]
    end

    subgraph streampro_silver [Silver Layer / Star Schema]
        DimU[dim_users]
        DimV[dim_videos]
        DimD[dim_devices]
        FactE[fact_events]
        FactS[fact_sessions]
    end
    
    subgraph streampro_gold [Gold Layer / KPIs]
        Gold1[kpi_new_user_conversion]
        Gold2[kpi_retention_by_genre]
        Gold3[kpi_device_drop_off]
    end

    E --> CS
    U --> CS
    V --> CS
    D --> CS

    CS --> StgE
    CS --> StgU
    CS --> StgV
    CS --> StgD

    StgE --> Clean
    StgU --> Clean
    StgV --> Clean
    StgD --> Clean

    Clean --> Sessionize

    Sessionize --> DimU
    Sessionize --> DimV
    Sessionize --> DimD
    Sessionize --> FactE
    Sessionize --> FactS
    
    FactS --> Gold1
    FactS --> Gold2
    FactS --> Gold3
```

**Medallion Layers:**
*   **Raw Data Sources:** The raw output files from different application services.
*   **Ingestion / Cloud Storage:** The landing zone (Data Lake) where raw files are stored cheaply and immutably.
*   **Bronze (`streampro_bronze`):** Raw files loaded into the warehouse with a strict 1:1 mapping from source, providing a historical archive inside the database.
*   **Transformation Engine:** SQL or dbt models that clean data, cast data types, handle nulls, and execute business logic like constructing `session_id`.
*   **Silver (`streampro_silver`):** Query-optimized dimensional models (Star Schema) consisting of conformed dimensions and granular fact tables.
*   **Gold (`streampro_gold`):** Highly aggregated, business-level metric tables and KPIs ready to be directly visualized by BI dashboards (Tableau, Looker, etc.) without requiring complex joins.
