# 🚀 Azure End-to-End Data Engineering Project – Medallion Architecture

This repository demonstrates an end-to-end **data engineering pipeline** using Azure services, following the **Medallion Architecture** (Bronze → Silver → Gold).

---

## 🏗️ Architecture
![Architecture Diagram](architecture/architecture-diagram.png)

**Flow:**
1. Data ingestion with **Azure Data Factory** into **ADLS Gen2** (Bronze).
2. Transformation & cleaning with **Databricks (PySpark)** (Silver).
3. Dimensional modeling & fact table creation with Delta Lake (Gold).
4. Loading into **Azure SQL Database** for reporting.
5. Optional **Power BI** visualization.

---

## ⚙️ Services & Tools
- Azure Data Factory (ADF)
- Azure Data Lake Storage Gen2
- Azure Databricks (PySpark, Delta Lake)
- Azure SQL Database
- Power BI (optional)

---

## 📂 Repository Contents
- **/notebooks**: PySpark scripts for Bronze, Silver, and Gold layers.
- **/adf**: Sanitized JSON exports of pipelines & datasets.
- **/sql**: SQL scripts for creating tables & validation queries.
- **/screenshots**: Screenshots of pipeline runs, jobs, SQL outputs.
- **/architecture**: Architecture diagram.

---

## 📝 Key Steps
1. **Bronze Layer (Raw Ingestion)**  
   - Ingest CSV/JSON from source into ADLS using parameterized ADF pipeline.  

2. **Silver Layer (Transformations)**  
   - Clean nulls, cast datatypes, normalize schema using PySpark in Databricks.  

3. **Gold Layer (Business-Ready Data)**  
   - Build dimension tables (SCD Type 1 logic using Delta merge).  
   - Build fact table with joins across dimensions.  

4. **Load into SQL DB**  
   - Curated Gold layer is loaded into Azure SQL Database.  

---

## 📊 Screenshots
- ADF Pipeline: ![ADF Pipeline](screenshots/adf_pipeline.png)
- Databricks Notebook: ![Notebook](screenshots/databricks_notebook.png)
- SQL Table Output: ![SQL Output](screenshots/sql_tables.png)

---

## 🔑 Learnings
- Parameterized pipelines in ADF  
- PySpark transformations with Databricks  
- Medallion architecture implementation (Bronze → Silver → Gold)  
- SCD Type 1 logic with Delta Lake  
- SQL DB integration for downstream analytics  

---

## 👤 Author
Project implemented by **[Your Name]**  
*(Screenshots/files have been sanitized to remove sensitive subscription/workspace details.)*
