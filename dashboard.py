"""
SmartGrocer Analytics — Interactive Streamlit Dashboard
========================================================
Run with:  streamlit run dashboard.py

Features:
  - KPI cards (Revenue, Orders, AOV, Customers)
  - Interactive filters (date range, category, city)
  - Bar / Line / Pie / Heatmap charts
  - Fully responsive layout
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# ---------------------------------------------------------------------------
# Page Config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="SmartGrocer Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS for professional look
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    .main > div { padding-top: 1rem; }
    .stMetric { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 1rem; border-radius: 0.75rem; color: white; }
    .stMetric label { color: rgba(255,255,255,0.85) !important; font-size: 0.9rem !important; }
    .stMetric [data-testid="stMetricValue"] { color: white !important; font-size: 1.8rem !important; }
    .stMetric [data-testid="stMetricDelta"] { color: rgba(255,255,255,0.9) !important; }
    div[data-testid="stSidebar"] { background: linear-gradient(180deg, #1a1a2e 0%, #16213e 100%); }
    div[data-testid="stSidebar"] .stMarkdown { color: white; }
    .block-container { padding-top: 1rem; padding-bottom: 1rem; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "processed")
DATA_DIR = os.path.join(BASE_DIR, "data")


@st.cache_data(ttl=600)
def load_processed_data():
    """Load the cleaned transaction-level dataset."""
    path = os.path.join(PROCESSED_DIR, "grocery_cleaned.csv")
    if not os.path.exists(path):
        st.error(f"Processed data not found at `{path}`. Run the PySpark pipeline first.")
        st.stop()
    df = pd.read_csv(path, parse_dates=["date"])
    return df


@st.cache_data(ttl=600)
def load_summary(filename):
    path = os.path.join(PROCESSED_DIR, filename)
    if os.path.exists(path):
        return pd.read_csv(path)
    return None


df = load_processed_data()

# ---------------------------------------------------------------------------
# Sidebar — Filters
# ---------------------------------------------------------------------------
st.sidebar.markdown("## 🛒 SmartGrocer Analytics")
st.sidebar.markdown("---")

min_date = df["date"].min().date()
max_date = df["date"].max().date()

st.sidebar.markdown("### 📅 Date Range")
date_range = st.sidebar.date_input(
    "Select dates",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

st.sidebar.markdown("### 📦 Category")
all_categories = sorted(df["category"].unique())
selected_categories = st.sidebar.multiselect(
    "Select categories",
    options=all_categories,
    default=all_categories,
)

st.sidebar.markdown("### 🏙️ City")
all_cities = sorted(df["city"].unique())
selected_cities = st.sidebar.multiselect(
    "Select cities",
    options=all_cities,
    default=all_cities,
)

st.sidebar.markdown("### 💳 Payment Method")
all_payments = sorted(df["payment_method"].unique())
selected_payments = st.sidebar.multiselect(
    "Select payment methods",
    options=all_payments,
    default=all_payments,
)

# Apply filters
if len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date

mask = (
    (df["date"].dt.date >= start_date)
    & (df["date"].dt.date <= end_date)
    & (df["category"].isin(selected_categories))
    & (df["city"].isin(selected_cities))
    & (df["payment_method"].isin(selected_payments))
)
filtered = df[mask].copy()

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown("# 🛒 SmartGrocer Analytics Dashboard")
st.markdown(
    f"**Showing {len(filtered):,} transactions** out of {len(df):,} "
    f"| {start_date} to {end_date}"
)
st.markdown("---")

# ---------------------------------------------------------------------------
# KPI Row
# ---------------------------------------------------------------------------
if filtered.empty:
    st.warning("No data matches the selected filters. Adjust the sidebar filters.")
    st.stop()

total_revenue = filtered["total_amount"].sum()
total_orders = len(filtered)
avg_order_value = filtered["total_amount"].mean()
unique_customers = filtered["customer_id"].nunique()
total_quantity = filtered["quantity"].sum()
avg_discount = filtered["discount_pct"].mean()

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Total Revenue", f"₹{total_revenue:,.0f}")
k2.metric("Total Orders", f"{total_orders:,}")
k3.metric("Avg Order Value", f"₹{avg_order_value:,.2f}")
k4.metric("Unique Customers", f"{unique_customers:,}")
k5.metric("Items Sold", f"{total_quantity:,}")
k6.metric("Avg Discount", f"{avg_discount:.1f}%")

st.markdown("---")

# ---------------------------------------------------------------------------
# Row 1 — Revenue Trend + Category Revenue
# ---------------------------------------------------------------------------
col1, col2 = st.columns([3, 2])

with col1:
    st.subheader("📈 Monthly Revenue Trend")
    monthly = (
        filtered.groupby("year_month")
        .agg(revenue=("total_amount", "sum"), orders=("total_amount", "count"))
        .reset_index()
        .sort_values("year_month")
    )
    fig_line = px.line(
        monthly, x="year_month", y="revenue",
        markers=True,
        labels={"year_month": "Month", "revenue": "Revenue (₹)"},
        color_discrete_sequence=["#667eea"],
    )
    fig_line.update_layout(
        hovermode="x unified",
        yaxis_tickprefix="₹",
        margin=dict(l=20, r=20, t=10, b=20),
        height=370,
    )
    st.plotly_chart(fig_line, use_container_width=True)

with col2:
    st.subheader("📦 Revenue by Category")
    cat_rev = (
        filtered.groupby("category")["total_amount"]
        .sum()
        .reset_index()
        .sort_values("total_amount", ascending=True)
    )
    fig_bar = px.bar(
        cat_rev, x="total_amount", y="category", orientation="h",
        labels={"total_amount": "Revenue (₹)", "category": ""},
        color="total_amount",
        color_continuous_scale="Viridis",
    )
    fig_bar.update_layout(
        showlegend=False,
        coloraxis_showscale=False,
        margin=dict(l=20, r=20, t=10, b=20),
        height=370,
    )
    st.plotly_chart(fig_bar, use_container_width=True)

# ---------------------------------------------------------------------------
# Row 2 — Category Pie + Top 10 Products
# ---------------------------------------------------------------------------
col3, col4 = st.columns(2)

with col3:
    st.subheader("🥧 Category Distribution (Orders)")
    cat_count = filtered["category"].value_counts().reset_index()
    cat_count.columns = ["category", "count"]
    fig_pie = px.pie(
        cat_count, names="category", values="count",
        color_discrete_sequence=px.colors.qualitative.Set3,
        hole=0.4,
    )
    fig_pie.update_traces(textposition="inside", textinfo="percent+label")
    fig_pie.update_layout(
        showlegend=False,
        margin=dict(l=20, r=20, t=10, b=20),
        height=400,
    )
    st.plotly_chart(fig_pie, use_container_width=True)

with col4:
    st.subheader("🏆 Top 10 Products by Revenue")
    top_prod = (
        filtered.groupby("product_name")["total_amount"]
        .sum()
        .nlargest(10)
        .reset_index()
        .sort_values("total_amount", ascending=True)
    )
    fig_top = px.bar(
        top_prod, x="total_amount", y="product_name", orientation="h",
        labels={"total_amount": "Revenue (₹)", "product_name": ""},
        color="total_amount",
        color_continuous_scale="Plasma",
    )
    fig_top.update_layout(
        showlegend=False,
        coloraxis_showscale=False,
        margin=dict(l=20, r=20, t=10, b=20),
        height=400,
    )
    st.plotly_chart(fig_top, use_container_width=True)

# ---------------------------------------------------------------------------
# Row 3 — City Revenue Map + Payment Method
# ---------------------------------------------------------------------------
col5, col6 = st.columns(2)

with col5:
    st.subheader("🏙️ Revenue by City")
    city_rev = (
        filtered.groupby("city")
        .agg(revenue=("total_amount", "sum"), orders=("total_amount", "count"))
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    fig_city = px.bar(
        city_rev, x="city", y="revenue",
        color="orders",
        labels={"city": "City", "revenue": "Revenue (₹)", "orders": "Orders"},
        color_continuous_scale="Teal",
    )
    fig_city.update_layout(
        margin=dict(l=20, r=20, t=10, b=20),
        height=380,
    )
    st.plotly_chart(fig_city, use_container_width=True)

with col6:
    st.subheader("💳 Payment Method Split")
    pay_dist = filtered["payment_method"].value_counts().reset_index()
    pay_dist.columns = ["method", "count"]
    fig_pay = px.pie(
        pay_dist, names="method", values="count",
        color_discrete_sequence=px.colors.qualitative.Pastel,
        hole=0.45,
    )
    fig_pay.update_traces(textposition="inside", textinfo="percent+label")
    fig_pay.update_layout(
        margin=dict(l=20, r=20, t=10, b=20),
        height=380,
    )
    st.plotly_chart(fig_pay, use_container_width=True)

# ---------------------------------------------------------------------------
# Row 4 — Heatmap (Day of Week × Hour)
# ---------------------------------------------------------------------------
st.subheader("🔥 Sales Heatmap — Day of Week × Hour")

day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
heat_data = (
    filtered.groupby(["day_name", "hour"])["total_amount"]
    .sum()
    .reset_index()
    .pivot(index="day_name", columns="hour", values="total_amount")
    .reindex(day_order)
    .fillna(0)
)

fig_heat = px.imshow(
    heat_data,
    labels=dict(x="Hour of Day", y="Day of Week", color="Revenue (₹)"),
    color_continuous_scale="YlOrRd",
    aspect="auto",
)
fig_heat.update_layout(
    margin=dict(l=20, r=20, t=10, b=20),
    height=350,
)
st.plotly_chart(fig_heat, use_container_width=True)

# ---------------------------------------------------------------------------
# Row 5 — Quarterly Trend + Discount Analysis
# ---------------------------------------------------------------------------
col7, col8 = st.columns(2)

with col7:
    st.subheader("📊 Quarterly Revenue Comparison")
    qtr_data = (
        filtered.groupby(["year", "quarter"])["total_amount"]
        .sum()
        .reset_index()
    )
    qtr_data["period"] = qtr_data["year"].astype(str) + "-Q" + qtr_data["quarter"].astype(str)
    fig_qtr = px.bar(
        qtr_data, x="period", y="total_amount",
        color="year", barmode="group",
        labels={"total_amount": "Revenue (₹)", "period": "Quarter"},
        color_discrete_sequence=px.colors.qualitative.Bold,
    )
    fig_qtr.update_layout(
        margin=dict(l=20, r=20, t=10, b=20),
        height=370,
    )
    st.plotly_chart(fig_qtr, use_container_width=True)

with col8:
    st.subheader("🏷️ Discount Impact on Revenue")
    disc_data = (
        filtered.groupby("discount_pct")
        .agg(revenue=("total_amount", "sum"), orders=("total_amount", "count"))
        .reset_index()
    )
    fig_disc = px.bar(
        disc_data, x="discount_pct", y="revenue",
        text="orders",
        labels={"discount_pct": "Discount (%)", "revenue": "Revenue (₹)", "orders": "Orders"},
        color_discrete_sequence=["#e74c3c"],
    )
    fig_disc.update_layout(
        margin=dict(l=20, r=20, t=10, b=20),
        height=370,
    )
    st.plotly_chart(fig_disc, use_container_width=True)

# ---------------------------------------------------------------------------
# Row 6 — Weekend vs Weekday
# ---------------------------------------------------------------------------
st.subheader("📅 Weekend vs Weekday Performance")
wk_data = (
    filtered.groupby("is_weekend")
    .agg(revenue=("total_amount", "sum"), orders=("total_amount", "count"),
         avg_val=("total_amount", "mean"))
    .reset_index()
)
wk_data["type"] = wk_data["is_weekend"].map({0: "Weekday", 1: "Weekend"})

w1, w2, w3 = st.columns(3)
for i, row in wk_data.iterrows():
    col = [w1, w2][i] if i < 2 else w3
    col.metric(f"{row['type']} Revenue", f"₹{row['revenue']:,.0f}")
    col.metric(f"{row['type']} Orders", f"{row['orders']:,}")

# ---------------------------------------------------------------------------
# Data Table Preview
# ---------------------------------------------------------------------------
st.markdown("---")
st.subheader("📋 Raw Data Preview")
with st.expander("Click to expand data table", expanded=False):
    st.dataframe(
        filtered.head(500).style.format({
            "total_amount": "₹{:,.2f}",
            "unit_price": "₹{:,.2f}",
            "discount_pct": "{:.0f}%",
        }),
        use_container_width=True,
        height=400,
    )

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown(
    "<div style='text-align:center; color:#888; font-size:0.85rem;'>"
    "SmartGrocer Analytics Dashboard | Big Data & Cloud Computing Capstone Project"
    "</div>",
    unsafe_allow_html=True,
)
