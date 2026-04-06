# NexusFlow: Enterprise Financial Data Platform

**NexusFlow** is a high-performance, automated data engine designed to replace fragmented legacy financial systems with a unified, governed, and resilient Lakehouse architecture.

## 🔴 Current State & Challenges

The existing infrastructure at **NexusFlow Financial** faces several critical architectural bottlenecks that impede scaling and data reliability.

### Legacy Architecture
* **Systems:** Fragmented on-premises SQL Server instances and siloed CSV-based landing zones.
* **Databases:** Multiple disconnected relational databases (RDS) with no centralized metadata layer.
* **Problems:**
    * **Security Risk:** Hardcoded credentials and lack of centralized secret management for cloud storage.
    * **Data Silos:** Inconsistent customer definitions across different business units.
    * **Manual Intervention:** Pipelines lack self-healing capabilities, requiring manual restarts during schema shifts.
    * **Governance Gaps:** No unified Row-Level Security (RLS) or PII masking, complicating NZ compliance audits.
    * **Stale Reporting:** 24-hour latency on executive dashboards due to rigid batch processing.

---

## 🏗️ Target Architecture (The Solution)

The proposed solution migrates the platform to a **Databricks Medallion Architecture** using Unity Catalog for centralized governance and a "Configuration-First" infrastructure strategy.

---

## 📅 Implementation Roadmap

The modernization is divided into 10 high-impact phases, prioritized by architectural dependency.

| Phase | Focus | Estimated Timeframe |
| :--- | :--- | :--- |
| **Phase 1** | **Infrastructure:** Terraform IaC (Workspace, Storage, Key Vault) | 3 Days |
| **Phase 2** | **Security:** Secret Scopes & AKV Integration | 2 Days |
| **Phase 3** | **Foundation:** Storage & Delta Optimization (Liquid Clustering) | 2 Days |
| **Phase 4** | **Governance:** Unity Catalog, RLS & PII Masking | 3 Days |
| **Phase 5** | **Ingestion:** Auto Loader & Schema Evolution | 4 Days |
| **Phase 6** | **Transformation:** Medallion & SCD Type 2 | 5 Days |
| **Phase 7** | **DevOps:** DABs & GitHub Actions CI/CD | 3 Days |
| **Phase 8** | **Simulation:** Power Law Data Generation & Stress Testing | 2 Days |
| **Phase 9** | **Observability:** SLA, Quality & FinOps Dashboards | 3 Days |
| **Phase 10** | **Resiliency:** Cross-Region DR & Deep Clones | 3 Days |

---

## 🛠️ Tech Stack
* **Cloud Infrastructure:** Azure (ADLS Gen2, Key Vault), Terraform
* **Platform:** Databricks (Lakehouse, Unity Catalog)
* **Language:** Python (PySpark), SQL
* **Orchestration:** Databricks Workflows / Delta Live Tables (DLT)
* **Deployment:** Databricks Asset Bundles (DABs), GitHub Actions

---

## 🔍 Technical Deep Dive

### 1. Infrastructure & Security: Zero-Trust Foundation
NexusFlow treats infrastructure as code. Using **Terraform**, the platform provisions a hardened Azure environment.
* **Secret Management:** Integration with **Azure Key Vault (AKV)** via Databricks Secret Scopes ensures that storage keys and service principal credentials are never exposed in code or notebook outputs.
* **Identity:** Implements a Service Principal-led execution model to maintain a strict audit trail of data access.

### 2. Intelligent Ingestion: Databricks Auto Loader
The **Bronze Layer** utilizes Auto Loader (`cloudFiles`) to handle high-velocity financial logs with zero manual intervention.
* **Schema Evolution:** Configured with `cloudFiles.schemaEvolutionMode: addNewColumns` to prevent pipeline failures during upstream API shifts.
* **Rescued Data Safety Net:** Implemented a `_rescued_data` column to capture malformed JSON payloads, allowing for asynchronous data repair without blocking the primary stream.

### 3. Storage Optimization: Liquid Clustering
To ensure sub-second query performance for the **Gold Layer**, NexusFlow implements **Liquid Clustering** over traditional hive-style partitioning.
* **Multidimensional Indexing:** Tables are clustered by `customer_id` and `transaction_date` simultaneously. This avoids "hot partitions" and ensures that both "Account History" and "Daily Revenue" queries are equally optimized.
* **Efficiency:** Unlike Z-Order, Liquid Clustering is incremental, significantly reducing the DBU overhead for daily maintenance.

### 4. Historical Integrity: SCD Type 2 Implementation
For compliance-heavy entities like **Customer Profiles**, the system utilizes **Slowly Changing Dimensions (SCD) Type 2** to maintain a perfect audit trail.
* **Temporal Accuracy:** Tracks changes in sensitive fields (e.g., `risk_level`, `credit_limit`) with `__start_at` and `__end_at` timestamps.
* **Auditability:** Essential for RBNZ/FMA financial audits, allowing for point-in-time reconstruction of customer states.

### 5. Enterprise Security: Unity Catalog & RLS
* **Dynamic Masking:** PII (emails, names) is automatically masked for non-privileged users using Unity Catalog functions.
* **Row-Level Security (RLS):** Data access is restricted based on the user's regional assignment (e.g., "Auckland" vs "Wellington"), ensuring strict data sovereignty.