"""
SmartGrocer Analytics — PySpark Data Pipeline
===============================================
End-to-end ETL pipeline that:
  1. Loads raw grocery transaction CSV
  2. Profiles & cleans the data
  3. Performs transformations and feature engineering
  4. Runs analytical queries (top products, revenue trends, etc.)
  5. Exports processed data for the dashboard
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType
)
from pyspark.sql.window import Window
import os
import shutil

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_CSV = os.path.join(BASE_DIR, "data", "grocery_transactions.csv")
PROCESSED_DIR = os.path.join(BASE_DIR, "processed")

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("SmartGrocer-ETL")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("date", StringType(), True),
    StructField("hour", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
])


def banner(title: str):
    print("\n" + "=" * 65)
    print(f"  {title}")
    print("=" * 65)


# ===================================================================
# STEP 1 — Load Raw Data
# ===================================================================
def load_data():
    banner("STEP 1 — Loading Raw Data")
    df = (
        spark.read
        .option("header", "true")
        .schema(SCHEMA)
        .csv(RAW_CSV)
    )
    print(f"  Loaded {df.count():,} rows, {len(df.columns)} columns")
    df.printSchema()
    return df


# ===================================================================
# STEP 2 — Data Profiling
# ===================================================================
def profile_data(df):
    banner("STEP 2 — Data Profiling")

    print("\n  --- Null counts per column ---")
    null_counts = df.select([
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ])
    null_counts.show(truncate=False)

    print("  --- Duplicate transaction IDs ---")
    dup_count = df.count() - df.dropDuplicates(["transaction_id"]).count()
    print(f"  Duplicates found: {dup_count}")

    print("\n  --- Numeric column statistics ---")
    df.select("quantity", "unit_price", "discount_pct", "total_amount").describe().show()

    return df


# ===================================================================
# STEP 3 — Data Cleaning
# ===================================================================
def clean_data(df):
    banner("STEP 3 — Data Cleaning")

    initial_count = df.count()

    # 3a. Remove duplicate transactions
    df = df.dropDuplicates(["transaction_id"])
    after_dedup = df.count()
    print(f"  Removed {initial_count - after_dedup} duplicate rows")

    # 3b. Cast date column
    df = df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))

    # 3c. Fill missing city with mode per store_id, else "Unknown"
    city_mode = (
        df.filter(F.col("city").isNotNull())
        .groupBy("store_id")
        .agg(F.mode("city").alias("city_mode"))
    )
    df = df.join(city_mode, on="store_id", how="left")
    df = df.withColumn(
        "city",
        F.coalesce(F.col("city"), F.col("city_mode"), F.lit("Unknown"))
    ).drop("city_mode")

    # 3d. Fill missing customer_id with "GUEST"
    df = df.withColumn(
        "customer_id",
        F.coalesce(F.col("customer_id"), F.lit("GUEST"))
    )

    # 3e. Fill missing payment_method with "Unknown"
    df = df.withColumn(
        "payment_method",
        F.coalesce(F.col("payment_method"), F.lit("Unknown"))
    )

    # 3f. Fill missing discount to 0
    df = df.withColumn(
        "discount_pct",
        F.coalesce(F.col("discount_pct"), F.lit(0.0))
    )

    # 3g. Cap outliers in quantity at 99th percentile
    q99 = df.approxQuantile("quantity", [0.99], 0.01)[0]
    outlier_count = df.filter(F.col("quantity") > q99).count()
    df = df.withColumn(
        "quantity_capped",
        F.when(F.col("quantity") > q99, q99).otherwise(F.col("quantity")).cast(IntegerType())
    )
    print(f"  Capped {outlier_count} quantity outliers (> {q99:.0f})")

    # 3h. Recalculate total_amount for capped rows
    df = df.withColumn(
        "total_amount_clean",
        F.round(F.col("unit_price") * F.col("quantity_capped") * (1 - F.col("discount_pct") / 100), 2)
    )

    remaining_nulls = sum(
        df.select(F.sum(F.when(F.col(c).isNull(), 1).otherwise(0))).collect()[0][0] or 0
        for c in df.columns
    )
    print(f"  Remaining nulls after cleaning: {remaining_nulls}")
    print(f"  Final clean row count: {df.count():,}")

    return df


# ===================================================================
# STEP 4 — Feature Engineering & Transformations
# ===================================================================
def transform_data(df):
    banner("STEP 4 — Feature Engineering & Transformations")

    df = (
        df
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("day_name", F.date_format("date", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("is_weekend", F.when(F.dayofweek("date").isin(1, 7), 1).otherwise(0))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("year_month", F.date_format("date", "yyyy-MM"))
    )

    # Revenue bucket
    df = df.withColumn(
        "revenue_bucket",
        F.when(F.col("total_amount_clean") < 100, "Low")
         .when(F.col("total_amount_clean") < 500, "Medium")
         .when(F.col("total_amount_clean") < 2000, "High")
         .otherwise("Premium")
    )

    # Customer purchase frequency (rolling window proxy)
    cust_window = Window.partitionBy("customer_id").orderBy("date")
    df = df.withColumn("cust_txn_rank", F.row_number().over(cust_window))

    print("  Added: year, month, day_of_week, day_name, quarter, is_weekend,")
    print("         year_month, revenue_bucket, cust_txn_rank")
    print(f"  Column count: {len(df.columns)}")

    return df


# ===================================================================
# STEP 5 — Analytical Queries
# ===================================================================
def run_analysis(df):
    banner("STEP 5 — Analytical Queries")

    # 5a. Revenue by Category
    print("\n  --- Revenue by Category ---")
    df.groupBy("category") \
        .agg(
            F.sum("total_amount_clean").alias("total_revenue"),
            F.count("*").alias("num_transactions"),
            F.avg("total_amount_clean").alias("avg_order_value"),
        ) \
        .orderBy(F.desc("total_revenue")) \
        .show(truncate=False)

    # 5b. Top 15 Products by Revenue
    print("  --- Top 15 Products by Revenue ---")
    df.groupBy("product_name", "category") \
        .agg(F.sum("total_amount_clean").alias("revenue")) \
        .orderBy(F.desc("revenue")) \
        .limit(15) \
        .show(truncate=False)

    # 5c. Monthly Revenue Trend
    print("  --- Monthly Revenue Trend (last 12 months) ---")
    df.groupBy("year_month") \
        .agg(F.sum("total_amount_clean").alias("monthly_revenue")) \
        .orderBy("year_month") \
        .show(36, truncate=False)

    # 5d. City-wise Performance
    print("  --- City-wise Revenue ---")
    df.groupBy("city") \
        .agg(
            F.sum("total_amount_clean").alias("revenue"),
            F.countDistinct("customer_id").alias("unique_customers"),
        ) \
        .orderBy(F.desc("revenue")) \
        .show(truncate=False)

    # 5e. Payment Method Distribution
    print("  --- Payment Method Distribution ---")
    df.groupBy("payment_method") \
        .agg(F.count("*").alias("txn_count")) \
        .orderBy(F.desc("txn_count")) \
        .show()

    # 5f. Weekend vs Weekday
    print("  --- Weekend vs Weekday Sales ---")
    df.groupBy("is_weekend") \
        .agg(
            F.sum("total_amount_clean").alias("total_revenue"),
            F.avg("total_amount_clean").alias("avg_order_value"),
            F.count("*").alias("num_orders"),
        ) \
        .show()

    # 5g. Peak Hours
    print("  --- Peak Shopping Hours ---")
    df.groupBy("hour") \
        .agg(F.count("*").alias("txn_count")) \
        .orderBy("hour") \
        .show(24)


# ===================================================================
# STEP 6 — Export Processed Data
# ===================================================================
def export_data(df):
    banner("STEP 6 — Exporting Processed Data")

    select_cols = [
        "transaction_id", "date", "hour", "customer_id", "store_id",
        "city", "product_name", "category", "quantity_capped",
        "unit_price", "discount_pct", "total_amount_clean",
        "payment_method", "year", "month", "day_of_week", "day_name",
        "quarter", "is_weekend", "year_month", "revenue_bucket",
    ]
    out_df = df.select(select_cols)

    # Rename for clarity
    out_df = (
        out_df
        .withColumnRenamed("quantity_capped", "quantity")
        .withColumnRenamed("total_amount_clean", "total_amount")
    )

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    final_path = os.path.join(PROCESSED_DIR, "grocery_cleaned.csv")

    # Convert to Pandas for reliable CSV export on Windows
    pdf = out_df.toPandas()
    pdf.to_csv(final_path, index=False)

    print(f"  Saved processed data -> {final_path}")
    print(f"  Rows: {len(pdf):,}  |  Columns: {len(pdf.columns)}")

    # --- Aggregated summary tables for fast dashboard loading ---

    # Revenue by category
    (
        df.groupBy("category")
        .agg(
            F.sum("total_amount_clean").alias("total_revenue"),
            F.count("*").alias("num_transactions"),
            F.avg("total_amount_clean").alias("avg_order_value"),
            F.sum("quantity_capped").alias("total_quantity"),
        )
        .toPandas()
        .to_csv(os.path.join(PROCESSED_DIR, "summary_category.csv"), index=False)
    )

    # Monthly revenue trend
    (
        df.groupBy("year_month")
        .agg(
            F.sum("total_amount_clean").alias("monthly_revenue"),
            F.count("*").alias("monthly_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .orderBy("year_month")
        .toPandas()
        .to_csv(os.path.join(PROCESSED_DIR, "summary_monthly.csv"), index=False)
    )

    # City-level summary
    (
        df.groupBy("city")
        .agg(
            F.sum("total_amount_clean").alias("revenue"),
            F.count("*").alias("orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .toPandas()
        .to_csv(os.path.join(PROCESSED_DIR, "summary_city.csv"), index=False)
    )

    # Top products
    (
        df.groupBy("product_name", "category")
        .agg(
            F.sum("total_amount_clean").alias("revenue"),
            F.sum("quantity_capped").alias("qty_sold"),
        )
        .orderBy(F.desc("revenue"))
        .limit(20)
        .toPandas()
        .to_csv(os.path.join(PROCESSED_DIR, "summary_top_products.csv"), index=False)
    )

    # Hourly distribution
    (
        df.groupBy("hour")
        .agg(F.count("*").alias("txn_count"))
        .orderBy("hour")
        .toPandas()
        .to_csv(os.path.join(PROCESSED_DIR, "summary_hourly.csv"), index=False)
    )

    # Payment method distribution
    (
        df.groupBy("payment_method")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("total_amount_clean").alias("revenue"),
        )
        .toPandas()
        .to_csv(os.path.join(PROCESSED_DIR, "summary_payment.csv"), index=False)
    )

    print("  Exported 6 summary CSVs for dashboard")


# ===================================================================
# MAIN
# ===================================================================
def main():
    banner("SmartGrocer Analytics — PySpark ETL Pipeline")

    df = load_data()
    df = profile_data(df)
    df = clean_data(df)
    df = transform_data(df)
    run_analysis(df)
    export_data(df)

    banner("Pipeline Complete")
    spark.stop()


if __name__ == "__main__":
    main()
