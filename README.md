# StreamPro Assignment Solution

The deliverables for this assignment have been organized into the following individual files for better readability:

## 1. Data Flow & Architecture
*   [Architecture Diagram](./architecture_diagram.md) - Contains the Mermaid diagram illustrating the data flow from raw sources to the Data Warehouse.

## 2. Data Modeling
*   [Data Model ERD](./data_model.md) - Contains the Entity Relationship Diagram (ERD) defining the Star Schema, including facts and dimensions.

## 3. Data Transformation
*   [Data Loader Scripts](./sql/loader.sql) - Contains generic Data Warehouse `COPY INTO` syntax to load the raw `.csv` and `.jsonl` files into staging tables.
*   [SQL Transformations](./sql/transformations.sql) - Contains the DDL and logic to transform raw tables into the final dimensional model (`fact_sessions`, `fact_events`, etc.).

## 4. Analytics Queries
*   [Analytics SQL Queries](./sql/analytics_queries.sql) - Contains the SQL queries to answer the specific assignment questions (user watch time conversions, retention by genre, and drop-off anomalies).

## 5. Operational Use Case
*   [Python Script](./src/operational_script.py) - Automates retrieving the last watched video per user and pushes the state to an external endpoint.
*   [Requirements File](./requirements.txt) - Dependency list containing `requests`.
