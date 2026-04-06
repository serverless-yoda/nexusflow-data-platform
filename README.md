# NexusFlow: Enterprise Financial Data Platform

**NexusFlow** is a high-performance, automated data engine designed to replace fragmented legacy financial systems with a unified, governed, and resilient Lakehouse architecture.

## 🔴 Current State & Challenges

The existing infrastructure at **NexusFlow Financial** faces several critical architectural bottlenecks that impede scaling and data reliability.

### Legacy Architecture
* **Systems:** Fragmented on-premises SQL Server instances and siloed CSV-based landing zones.
* **Databases:** Multiple disconnected relational databases (RDS) with no centralized metadata layer.
* **Problems:**
    * **Data Silos:** Inconsistent customer definitions across different business units.
    * **Manual Intervention:** Pipelines lack self-healing capabilities, requiring manual restarts during schema shifts.
    * **Governance Gaps:** No unified Row-Level Security (RLS) or PII masking, complicating NZ compliance audits.
    * **Stale Reporting:** 24-hour latency on executive dashboards due to rigid batch processing.

---

## 🏗️ Target Architecture (The Solution)

The proposed solution migrates the platform to a **Databricks Medallion Architecture** using Unity Catalog for centralized governance.



---

## 📅 Implementation Roadmap

The modernization is divided into 10 high-impact phases.

| Phase | Focus | Estimated Timeframe | Started | End |
| :--- | :--- | :--- |:--- | :--- |
| **Phase 1** | **Foundation:** Storage & Delta Optimization | 2 Days | 06-Apr-26 | TBD |
| **Phase 2** | **Governance:** Unity Catalog & Security | 3 Days | TBD | TBD |
| **Phase 3** | **Compute:** Serverless SQL & FinOps | 2 Days | TBD | TBD |
| **Phase 4** | **Ingestion:** Auto Loader & Schema Evolution | 4 Days | TBD | TBD |
| **Phase 5** | **Transformation:** Medallion & SCD Type 2 | 5 Days | TBD | TBD |
| **Phase 6** | **DevOps:** DABs & GitHub Actions CI/CD | 3 Days | TBD | TBD |
| **Phase 7** | **Simulation:** Power Law Data Generation | 2 Days | TBD | TBD |
| **Phase 8** | **Strategy:** Design Authority & Defense | 1 Day | TBD | TBD |
| **Phase 9** | **Observability:** SLA & Quality Dashboards | 3 Days | TBD | TBD |
| **Phase 10** | **Resiliency:** Disaster Recovery & Deep Clones | 2 Days | TBD | TBD |

---

## 🛠️ Tech Stack
* **Platform:** Databricks (Azure/AWS/GCP)
* **Language:** Python (PySpark), SQL
* **Orchestration:** Databricks Lakeflow / Workflows
* **Governance:** Unity Catalog
* **Deployment:** Databricks Asset Bundles (DABs), GitHub Actions


## 🔍 Technical Deep Dive

### 1. Intelligent Ingestion: Databricks Auto Loader
The **Bronze Layer** utilizes Databricks Auto Loader (`cloudFiles`) to handle high-velocity financial logs with zero manual intervention.
* **Schema Evolution:** Configured with `cloudFiles.schemaEvolutionMode: addNewColumns` to prevent pipeline failures during upstream API shifts.
* **Rescued Data Safety Net:** Implemented a `_rescued_data` column to capture malformed JSON payloads (e.g., type mismatches in currency fields), allowing for asynchronous data repair without blocking the primary stream.
* **Performance:** Leverages File Notification mode (Azure Event Grid/AWS SNS) to achieve constant-time file discovery, bypassing the $O(n)$ latency of directory listing as the file count reaches the millions.

### 2. Storage Optimization: Liquid Clustering
To ensure sub-second query performance for the **Gold Layer**, NexusFlow implements **Liquid Clustering** over traditional hive-style partitioning.
* **Multidimensional Indexing:** Tables are clustered by `customer_id` and `transaction_date` simultaneously. This avoids "hot partitions" and ensures that both "Account History" and "Daily Revenue" queries are equally optimized.
* **Flexibility:** Unlike Z-Order, Liquid Clustering is incremental and does not require a full table rewrite, significantly reducing the DBU overhead for daily `OPTIMIZE` operations.

### 3. Historical Integrity: SCD Type 2 Implementation
For compliance-heavy entities like **Customer Profiles**, the system utilizes **Slowly Changing Dimensions (SCD) Type 2** to maintain a perfect audit trail.
* **Temporal Accuracy:** Using the `dlt.apply_changes` API, the platform tracks changes in sensitive fields (e.g., `risk_level`, `credit_limit`) with `__start_at` and `__end_at` timestamps.
* **Auditability:** This allows the business to reconstruct the exact state of a customer profile at the time of any historical transaction, a core requirement for RBNZ/FMA financial audits.

### 4. Enterprise Security: Unity Catalog & RLS
* **Dynamic Masking:** PII (emails, names) is automatically masked for non-privileged users using Unity Catalog functions.
* **Row-Level Security (RLS):** Data access is restricted based on the user's regional assignment, ensuring an "Auckland" analyst cannot view "Wellington" branch transactions unless authorized.