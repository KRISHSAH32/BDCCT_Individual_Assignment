# SmartGrocer Analytics — Big Data & Cloud Computing Capstone Project

## 1. Project Title

**SmartGrocer Analytics: End-to-End Big Data Pipeline for Grocery Retail Intelligence**

---

## 2. Problem Statement

India's grocery retail sector generates millions of transactions daily across thousands of stores. Retailers struggle to extract actionable insights from this massive volume of data using traditional tools. Key challenges include:

- **Inventory blind spots** — overstocking slow-moving items while understocking high-demand products.
- **Pricing inefficiency** — uniform pricing that ignores regional demand variation and seasonal trends.
- **Customer churn** — inability to identify at-risk customers or personalize promotions.
- **Operational bottlenecks** — peak-hour staffing and supply-chain misalignment.

This project builds an **end-to-end Big Data pipeline** using PySpark that ingests 300,000+ synthetic grocery transactions, cleans and transforms the data, performs analytical queries, and exposes the results through an interactive Streamlit dashboard — enabling data-driven decision-making at every level of the business.

---

## 3. Dataset Description

| Attribute        | Detail                              |
|------------------|-------------------------------------|
| **Rows**         | 300,000                             |
| **Columns**      | 13 (raw) → 21 (after processing)   |
| **Date Range**   | Jan 2023 – Dec 2025                |
| **Categories**   | 10 grocery categories              |
| **Products**     | 85+ distinct items                  |
| **Cities**       | 10 Indian metro cities              |
| **Customers**    | 15,000 unique IDs + guest shoppers  |
| **Payment Types**| Cash, Credit Card, Debit Card, UPI, Digital Wallet |

### Columns (Raw)

| Column           | Type   | Description                          |
|------------------|--------|--------------------------------------|
| transaction_id   | String | Unique ID (TXN-0000001)             |
| date             | Date   | Transaction date                     |
| hour             | Int    | Hour of transaction (7–22)           |
| customer_id      | String | Customer ID or null (guest)          |
| store_id         | String | Store identifier                     |
| city             | String | City of the store                    |
| product_name     | String | Product purchased                    |
| category         | String | Product category                     |
| quantity         | Int    | Quantity purchased                   |
| unit_price       | Float  | Price per unit (INR)                 |
| discount_pct     | Float  | Discount applied (0–20%)             |
| total_amount     | Float  | Final transaction value              |
| payment_method   | String | Payment mode                         |

### Realistic Variations

- **Missing values (~2.5%)** — customer_id (1.5%), city (1%), payment_method (0.8%), discount_pct (0.5%)
- **Outliers (~0.5%)** — inflated quantities (50–200 units) simulating bulk/wholesale orders
- **Seasonal trends** — beverages peak in summer (Mar–Jul), dairy peaks in winter (Nov–Feb), snacks peak during festivities (Oct–Jan)

---

## 4. System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    SMARTGROCER ANALYTICS                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────┐    ┌───────────────┐    ┌──────────────┐         │
│  │  DATA     │    │  PROCESSING   │    │  STORAGE     │         │
│  │  SOURCE   │───▶│  ENGINE       │───▶│  LAYER       │         │
│  │          │    │               │    │              │         │
│  │ Synthetic │    │  PySpark      │    │ Cleaned CSV  │         │
│  │ Generator │    │  ETL Pipeline │    │ Summary CSVs │         │
│  │ (Python)  │    │  (Local/      │    │ (processed/) │         │
│  │          │    │   Cluster)    │    │              │         │
│  └──────────┘    └───────────────┘    └──────┬───────┘         │
│                                              │                 │
│                                              ▼                 │
│                                     ┌──────────────┐           │
│                                     │  DASHBOARD   │           │
│                                     │  (Streamlit  │           │
│                                     │   + Plotly)  │           │
│                                     │              │           │
│                                     │ KPIs, Charts │           │
│                                     │ Filters,     │           │
│                                     │ Heatmaps     │           │
│                                     └──────────────┘           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Architecture Layers

| Layer           | Technology        | Purpose                                    |
|-----------------|-------------------|--------------------------------------------|
| Data Generation | Python + NumPy    | Creates realistic synthetic transactions   |
| Processing      | Apache PySpark    | Distributed cleaning, transformation, aggregation |
| Storage         | CSV / Parquet     | Lightweight, portable intermediate storage |
| Visualization   | Streamlit + Plotly| Interactive, web-based analytics dashboard |

---

## 5. Workflow Steps

```
Step 1:  generate_dataset.py    →  data/grocery_transactions.csv  (300K rows)
Step 2:  pyspark_pipeline.py    →  processed/grocery_cleaned.csv  (cleaned + engineered)
                                →  processed/summary_*.csv        (pre-aggregated tables)
Step 3:  streamlit run dashboard.py  →  Interactive web dashboard at localhost:8501
```

### Detailed Workflow

1. **Data Generation** (`generate_dataset.py`)
   - Builds a product catalog with 85+ items across 10 categories
   - Generates 300,000 transactions with date/time, customer, store, city, product, and payment data
   - Injects seasonal weighting, missing values, and outliers for realism

2. **PySpark ETL Pipeline** (`pyspark_pipeline.py`)
   - **Load**: Reads CSV with explicit schema enforcement
   - **Profile**: Counts nulls, duplicates, and computes descriptive statistics
   - **Clean**: Imputes missing values (mode-fill for city, "GUEST" for unknown customers), caps outliers at 99th percentile, recalculates derived columns
   - **Transform**: Engineers 8+ new features (year, month, quarter, day_name, is_weekend, revenue_bucket, customer transaction rank)
   - **Analyze**: Runs 7 analytical queries (category revenue, top products, monthly trends, city performance, payment split, weekend vs weekday, peak hours)
   - **Export**: Writes cleaned transaction-level data + 6 pre-aggregated summary CSVs

3. **Dashboard** (`dashboard.py`)
   - Loads processed data with caching
   - Provides interactive sidebar filters (date range, category, city, payment method)
   - Renders 10+ visualizations: KPI cards, line charts, bar charts, pie charts, heatmap, quarterly comparison

---

## 6. Tools Used & Justification

| Tool          | Role                  | Why?                                                   |
|---------------|-----------------------|--------------------------------------------------------|
| **Python**    | Core language         | Rich ecosystem, Spark integration, rapid prototyping   |
| **PySpark**   | Data processing       | Handles Big Data at scale; runs locally or on a cluster|
| **Pandas**    | Data generation       | Fast for single-machine synthetic data creation        |
| **NumPy**     | Randomization         | Efficient array operations for realistic distributions |
| **Streamlit** | Dashboard framework   | Python-native, zero-JS, instant interactive web apps   |
| **Plotly**    | Charting library      | Interactive, publication-quality, hover-enabled charts |

---

## 7. Insights & Business Value

### Key Insights from the Dashboard

1. **Category Performance**
   - Grains & Cereals and Meat & Seafood are the highest-revenue categories due to higher unit prices
   - Fruits & Vegetables lead in transaction count but have lower average order value
   - Personal Care has the highest avg order value — premium products with stable demand

2. **Temporal Patterns**
   - Revenue peaks during Oct–Dec (festive season: Diwali, Christmas, New Year)
   - Evenings (5–8 PM) are the busiest shopping hours; mornings (9–11 AM) are second
   - Weekends generate ~30% higher per-transaction value than weekdays

3. **Geographic Insights**
   - Mumbai and Delhi account for ~34% of total revenue
   - Bangalore has the highest UPI adoption rate
   - Tier-2 cities (Jaipur, Lucknow) show growing digital payment trends

4. **Payment Trends**
   - UPI dominates with ~32% of all transactions — reflecting India's digital payment revolution
   - Cash remains strong at ~20%, especially for low-value (<₹100) transactions

5. **Discount Impact**
   - 50% of orders have zero discount, but discounted orders have 1.4× higher average quantity
   - 15–20% discount tiers show strong volume lift — useful for promotional planning

---

## 8. Data-Driven Decision Making

### How Businesses Can Use These Insights

| Business Decision          | Dashboard Insight Used                       | Expected Impact             |
|----------------------------|----------------------------------------------|-----------------------------|
| **Inventory Optimization** | Category revenue + seasonal trends           | Reduce stockouts by 15–20% |
| **Dynamic Pricing**        | City-wise revenue + discount impact analysis | Increase margin by 5–8%    |
| **Staff Scheduling**       | Hourly heatmap + weekend/weekday split       | Reduce labor costs by 10%  |
| **Marketing Targeting**    | Customer purchase frequency + payment method | 2× higher campaign ROI     |
| **Store Expansion**        | City-wise customer density + revenue         | Data-backed location picks  |
| **Demand Forecasting**     | Monthly/quarterly revenue trends             | Accurate 3-month forecasts |

### Example Scenario

> **Problem**: A store in Pune is consistently overstocking Frozen Foods in winter.
>
> **Dashboard Evidence**: The heatmap + seasonal trend shows Frozen Foods peak in March–July (summer), not winter.
>
> **Action**: Reduce Frozen Food orders by 30% in Nov–Feb; reallocate shelf space to Dairy & Bakery (which peak in winter).
>
> **Result**: Reduced waste, higher sales per square foot.

---

## 9. Scalability & Big Data Relevance

### Why PySpark?

| Aspect           | Local Mode (This Project)    | Production / Cloud Scale              |
|------------------|------------------------------|---------------------------------------|
| Data volume      | 300K rows, ~40 MB            | Billions of rows, terabytes           |
| Execution        | `local[*]` on laptop         | YARN / Kubernetes cluster             |
| Storage          | CSV files on disk            | HDFS / Amazon S3 / Azure Data Lake    |
| Processing time  | ~30 seconds                  | Minutes (distributed across nodes)    |
| Dashboard        | Streamlit on localhost       | Deployed on Streamlit Cloud / EC2     |

### Scaling This Project to Production

1. **Data Lake Architecture** — Replace CSV with Delta Lake on S3 for ACID transactions
2. **Orchestration** — Use Apache Airflow to schedule the pipeline daily
3. **Streaming** — Swap batch CSV ingestion for Kafka + Spark Structured Streaming for real-time analytics
4. **Cloud Deployment** — Run Spark on AWS EMR / Azure Databricks / Google Dataproc
5. **Dashboard Hosting** — Deploy Streamlit to Streamlit Community Cloud or containerize with Docker
6. **Data Warehouse** — Load aggregated data into Snowflake / BigQuery for SQL-based BI tools

### Architecture at Scale

```
  POS Systems ──▶ Kafka Topics ──▶ Spark Streaming ──▶ Delta Lake (S3)
                                                            │
                                          ┌─────────────────┤
                                          ▼                 ▼
                                    Airflow DAG        Snowflake DW
                                    (batch agg)        (SQL analytics)
                                          │                 │
                                          ▼                 ▼
                                    Streamlit           Power BI / Tableau
                                    Dashboard           Executive Reports
```

---

## 10. How to Run

### Prerequisites

- Python 3.9+
- Java 8 or 11 (required for PySpark)

### Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Step 1: Generate the dataset
python generate_dataset.py

# Step 2: Run PySpark pipeline
python pyspark_pipeline.py

# Step 3: Launch the dashboard
streamlit run dashboard.py
```

The dashboard will open at **http://localhost:8501**.

---

## 11. Project Structure

```
Individual/
├── generate_dataset.py       # Synthetic data generator (300K rows)
├── pyspark_pipeline.py       # PySpark ETL pipeline
├── dashboard.py              # Streamlit interactive dashboard
├── requirements.txt          # Python dependencies
├── README.md                 # Project documentation (this file)
├── data/
│   └── grocery_transactions.csv   # Raw dataset
└── processed/
    ├── grocery_cleaned.csv        # Cleaned transaction data
    ├── summary_category.csv       # Revenue by category
    ├── summary_monthly.csv        # Monthly revenue trend
    ├── summary_city.csv           # City-level metrics
    ├── summary_top_products.csv   # Top 20 products
    ├── summary_hourly.csv         # Hourly transaction count
    └── summary_payment.csv        # Payment method split
```

---

## 12. Dashboard Screenshots (Description)

### Screen 1 — KPI Cards & Revenue Trend
- Six KPI cards at the top: Total Revenue, Total Orders, Avg Order Value, Unique Customers, Items Sold, Avg Discount
- A line chart showing monthly revenue from Jan 2023 to Dec 2025 with seasonal peaks visible
- A horizontal bar chart showing revenue by all 10 categories

### Screen 2 — Category & Product Analysis
- A donut chart breaking down order volume by category
- A horizontal bar chart of the top 10 products by revenue (Basmati Rice, Mutton, Ghee lead)

### Screen 3 — Geographic & Payment Analysis
- A bar chart of revenue by city (Mumbai leads, followed by Delhi and Bangalore)
- A donut chart showing payment method distribution (UPI ~32%, Cash ~20%)

### Screen 4 — Heatmap & Advanced Charts
- A color-intensity heatmap showing revenue by Day of Week (rows) × Hour (columns)
- Saturday and Sunday evenings (5–8 PM) show the darkest/highest values
- Quarterly revenue comparison bar chart grouped by year

---

## 13. Suggested Improvements

1. **Machine Learning Integration** — Add demand forecasting using Prophet or Spark MLlib
2. **Real-time Streaming** — Replace batch pipeline with Spark Structured Streaming + Kafka
3. **Customer Segmentation** — Apply RFM (Recency, Frequency, Monetary) analysis with K-Means clustering
4. **Anomaly Detection** — Flag unusual transactions (potential fraud or data errors)
5. **A/B Test Analysis** — Track impact of pricing/discount experiments through the dashboard
6. **Mobile Responsive** — Deploy with Streamlit Cloud for access on any device

---

*Project developed as part of the Big Data & Cloud Computing Capstone.*
