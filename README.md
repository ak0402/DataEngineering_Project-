# 🚀 Azure End-to-End Data Engineering Project – Medallion Architecture

This repository demonstrates an end-to-end **data engineering pipeline** using Azure services, following the **Medallion Architecture** (Bronze → Silver → Gold).

---
<h2 align="center">🏗️ Architecture</h2>

<p align="center">
  <img src="DE_Architecture.png" alt="Architecture Diagram" width="600"/>
</p>


 <!-- ## Architecture
![Architecture Diagram](DE_Architecture.png) -->

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
<!--
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

--- -->

## ⚙️ Project Workflow (Detailed Steps)  

The project follows the **Medallion Architecture** consisting of three main layers: **Bronze**, **Silver**, and **Gold**.  

---

### 🥉 Step 1: Bronze Layer – Raw Data Ingestion with Azure Data Factory

- Data is ingested from external sources (CSV files, JSON logs, etc.).  
- The raw data is stored as-is in **Azure Data Lake Storage (ADLS)** under the **Raw Zone**.  
- Purpose: Maintain an immutable copy of the source data for auditing and replay.  

<h3 align="center">Source Pipieline</h3>
<p align="center">
  <img src="screenshots/source_prep_properties.jpg" alt="Source Pipieline" width="700"/>
</p>  

<h3 align="center">Incremental Pipelinee</h3>
<p align="center">
  <img src="screenshots/increm_data_pipeline_properties1.jpg" alt="Incremental Pipeline" width="700"/>
</p>  


### 🥈 Step 2: Silver Layer – Data Cleaning & Transformation  

- Raw data is processed using **Azure Databricks (PySpark)**.  
- Transformations include:  
  - Handling missing values  
  - Standardizing schema  
  - Filtering invalid records  
  - Enforcing data quality checks  
- The cleaned & normalized data is stored in ADLS **Curated Zone**.  
- Refer to [Silver](notebooks/silver_notebook.py) Notebook provided in the Notebooks folder. 

<h3 align="center">Post Transformations Data Written to Silver Layer in ADLS</h3>

<p align="center">
  <img src="screenshots/databrickstransformation_writing_tosilverlayer.jpg" alt="screenshots/databrickstransformation_writing_tosilverlayer.jpg" width="700"/>
</p>  

---

### 🥇 Step 3: Gold Layer – Business Aggregation  

- Business-ready data is prepared by applying:  
  - Aggregations (sums, averages, counts)  
  - Joins across multiple curated datasets  
  - Data model transformations for reporting  
- Final tables are stored in **Azure SQL Database** (serving layer).  
- This data is then available for **Power BI dashboards** or other BI tools.  
- Refer to [Gold fact sales](notebooks/gold_fact_sales.py) Notebook provided in the Notebooks folder.

<h3 align="center">Data Writting to Gold Layer</h3>
<p align="center">
  <img src="screenshots/databricks_writing_togoldlayer.jpg" alt="Gold Layer data writting" width="700"/>
</p>  

<h3 align="center">Data in Gold Layer</h3>
<p align="center">
  <img src="screenshots/databricks_written_togoldlayer.jpg" alt="Gold Layer data" width="700"/>
</p>  


### 🔄 Step 4: Orchestration (ADF) & Compute (Databricks)

- **Azure Data Factory (ADF)** is used **For orchestration and ingestion of Incremental Pipeline**:
  - Triggers/schedules pipelines.
  - Copies **raw data → ADLS (Bronze)**.
- **All compute happens in Databricks**:
  - **Bronze → Silver**: cleansing, standardization, and quality checks in **Databricks (PySpark)**.
  - **Silver → Gold**: business logic, joins, aggregations, and SCD logic **on Databricks compute (clusters/Jobs)**.
- Databricks writes **Silver** and **Gold** datasets back to ADLS (Delta).  
  *(Optional) Final serving to Azure SQL/BI can be triggered by ADF or written directly from Databricks, depending on your setup.*

<h3 align="center">Data Model Pipeline in Databricks</h3>
<p align="center">
  <img src="screenshots/databricks_datamodel_run.jpg" alt="Data Model Pipeline in Databricks" width="700"/>
</p>



<h3 align="center">Gold Schema Ready for Data Analysts</h3>
<p align="center">
  <img src="screenshots/gold_schema_readydata.jpg" alt="Data Model Pipeline in Databricks" width="700"/>
</p>




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
