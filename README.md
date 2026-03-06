# ABC Pharma – Databricks ETL Pipeline (Bronze → Silver → Gold)
![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-red)
![PySpark](https://img.shields.io/badge/PySpark-ETL-orange)
![GitHub](https://img.shields.io/badge/Git-VersionControl-black)
## 📌 Overview
This repository contains an end‑to‑end **Databricks Lakehouse ETL pipeline** built for **ABC Pharma** using **Databricks Pipelines (DLT-style)** and **PySpark**.

The pipeline follows a **Bronze -> Silver -> Gold** architecture and answers key business questions related to:
- Market share
- Sales trends
- Product performance
- Geography performance
- Top physicians

The entire pipeline is **version‑controlled using GitHub** and **executed directly from Databricks Repos**, enabling seamless updates via Git pull without manual file copying.

---

## 🏗️ Architecture
```text
Raw Files
   ↓
Bronze Layer (Base Tables)
   ↓
Silver Layer (Cleaned & Conformed)
   ↓
Gold Layer (Business Answer Tables)
```

---

## 📂 Repository Structure

```text
abc-pharma-databricks-etl/
│
├── transformation/
│   ├── ingestion.py     # Bronze layer – raw ingestion
│   ├── silver.py        # Silver layer – cleaning & conformance
│   └── gold.py          # Gold layer – business answers
│
├── exploration/
│   └── FinalAnalysis.ipynb  # Business question exploration
│
├── README.md
```
---

## 🧱 Layers Description

### 🔹 Bronze Layer (`ingestion.py`)
- Reads raw source files
- Applies schema enforcement
- Performs minimal transformations
- Creates base tables prefixed with `bs_`

Example:
- `bs_sales`
- `bs_product_master`
- `bs_prescriber`

---

### 🔹 Silver Layer (`silver.py`)
- Cleans and standardizes data
- Applies data quality filters
- Normalizes schemas
- Resolves hierarchies and relationships
- Creates analytics‑ready tables prefixed with `sl_`

Example:
- `sl_sales` (month‑wise unpivoted sales)
- `sl_product_master`
- `sl_prescriber`
- `sl_geo_hierarchy_resolved`

---

### 🔹 Gold Layer (`gold.py`)
- Directly answers business questions
- Aggregates and ranks data
- Creates reporting‑ready tables prefixed with `g_`

---

## 📊 Gold Tables & Business Questions

### ✅ Q1: Market Share of Products
**Table:** `g_market_share`  
Calculates % contribution of each product to total market TRx (Units, Volume, Dollar).

---

### ✅ Q2: Sales Trend of Products (Brand Level – Last 4 Quarters)
**Table:** `g_brand_sales_trend_qtr`  
Shows quarterly TRx trends (Q1–Q4) at brand level.

---

### ✅ Q3: Sales of Products by Specialty
**Table:** `g_product_sales_by_specialty`  
Aggregates product sales by prescriber specialty.

---

### ✅ Q4: Top 10 Districts by Sales (Last Year)
**Table:** `g_top_10_districts_by_sales`  
Ranks districts based on total annual TRx.

---

### ✅ Q5: Top 5 Physicians per City by Sales
**Table:** `g_top5_physicians_by_city`  
Identifies top‑performing physicians in each city using TRx Dollar with window functions.

---

## 🚀 Running the Pipeline in Databricks

1. Clone this repository using **Databricks Repos**
2. Create a **Databricks Pipeline**
3. Set the pipeline **Source Path**
4. Run the pipeline

✅ Any GitHub update → Pull in Databricks → Re‑run pipeline  
✅ No manual copying of files required

---

## 🔁 Version Control & CI/CD Flow


```text
Local Edit
   ↓
Git Commit & Push
   ↓
Databricks Repo Pull
   ↓
Databricks Pipeline Run
```

GitHub acts as the **single source of truth** for all pipeline code.

---

## 🧪 Exploration Notebook

The `exploration/` notebook contains:
- All business questions
- General (all manufacturers) analysis
- ABC Pharma‑specific analysis

This notebook is intended for:
- Validation
- Dashboard development
- Stakeholder review

---

## ✅ Key Technologies Used

- Databricks Lakehouse
- PySpark
- Databricks Pipelines
- Delta Lake
- GitHub + Databricks Repos

---

## 👤 Author
Built as a hands‑on Databricks ETL learning project by **Pratham Upadhyay**.

---

## 📌 Notes
- Designed for extensibility (CDC, DQ frameworks can be added)
- Production‑ready structure
- Follows enterprise data engineering best practices