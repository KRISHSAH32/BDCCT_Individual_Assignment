"""
SmartGrocer Analytics — Synthetic Dataset Generator
====================================================
Generates 300,000+ realistic grocery-store transaction records with:
  - Seasonal purchasing patterns
  - Intentional missing values (~2-3%)
  - Outliers in quantity and price
  - Multiple cities, payment methods, and customer segments
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import os
import warnings

warnings.filterwarnings("ignore")

np.random.seed(42)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
NUM_ROWS = 300_000
DATE_START = datetime(2023, 1, 1)
DATE_END = datetime(2025, 12, 31)
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "grocery_transactions.csv")

CITIES = [
    "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai",
    "Kolkata", "Pune", "Ahmedabad", "Jaipur", "Lucknow"
]
CITY_WEIGHTS = [0.18, 0.16, 0.14, 0.12, 0.10, 0.08, 0.07, 0.06, 0.05, 0.04]

PAYMENT_METHODS = ["Cash", "Credit Card", "Debit Card", "UPI", "Digital Wallet"]
PAYMENT_WEIGHTS = [0.20, 0.18, 0.15, 0.32, 0.15]

CUSTOMER_COUNT = 15_000
STORE_COUNT = 50

# ---------------------------------------------------------------------------
# Product Catalog — realistic grocery items with base prices (INR)
# ---------------------------------------------------------------------------
PRODUCTS = {
    "Fruits & Vegetables": {
        "items": [
            ("Bananas (1 kg)", 40), ("Apples (1 kg)", 180), ("Tomatoes (1 kg)", 30),
            ("Onions (1 kg)", 35), ("Potatoes (1 kg)", 28), ("Mangoes (1 kg)", 120),
            ("Spinach (bunch)", 25), ("Carrots (1 kg)", 45), ("Grapes (500 g)", 90),
            ("Watermelon (1 pc)", 60), ("Cucumber (500 g)", 20), ("Capsicum (250 g)", 35),
        ],
        "seasonal_peak": [4, 5, 6, 7],  # summer months
    },
    "Dairy & Eggs": {
        "items": [
            ("Milk (1 L)", 56), ("Curd (400 g)", 35), ("Paneer (200 g)", 80),
            ("Butter (100 g)", 52), ("Cheese Slice (10 pc)", 120), ("Eggs (12 pc)", 78),
            ("Ghee (500 ml)", 280), ("Cream (200 ml)", 65), ("Lassi (200 ml)", 30),
        ],
        "seasonal_peak": [11, 12, 1, 2],  # winter months
    },
    "Bakery": {
        "items": [
            ("White Bread", 40), ("Brown Bread", 50), ("Pav (8 pc)", 30),
            ("Cake (500 g)", 250), ("Cookies Pack", 60), ("Croissant (2 pc)", 90),
            ("Bun (4 pc)", 35), ("Rusk (200 g)", 40),
        ],
        "seasonal_peak": [10, 11, 12],  # festive season
    },
    "Meat & Seafood": {
        "items": [
            ("Chicken (1 kg)", 220), ("Mutton (1 kg)", 650), ("Fish - Rohu (1 kg)", 280),
            ("Prawns (500 g)", 350), ("Eggs (30 pc)", 190), ("Chicken Sausage (250 g)", 150),
            ("Fish Fillet (500 g)", 320),
        ],
        "seasonal_peak": [11, 12, 1],
    },
    "Beverages": {
        "items": [
            ("Coca-Cola (1 L)", 45), ("Pepsi (1 L)", 45), ("Mango Juice (1 L)", 90),
            ("Green Tea (25 bags)", 150), ("Coffee Powder (100 g)", 180),
            ("Mineral Water (1 L)", 20), ("Buttermilk (200 ml)", 15),
            ("Energy Drink (250 ml)", 125), ("Orange Juice (1 L)", 100),
        ],
        "seasonal_peak": [3, 4, 5, 6, 7],  # hot months
    },
    "Snacks & Confectionery": {
        "items": [
            ("Lays Chips (100 g)", 30), ("Kurkure (90 g)", 20), ("Dark Chocolate (100 g)", 120),
            ("Biscuit Pack", 30), ("Namkeen Mix (200 g)", 55), ("Popcorn (100 g)", 40),
            ("Candy Pack", 10), ("Peanut Bar", 15), ("Protein Bar", 100),
        ],
        "seasonal_peak": [10, 11, 12, 1],  # festive + new year
    },
    "Grains & Cereals": {
        "items": [
            ("Basmati Rice (5 kg)", 450), ("Wheat Flour (5 kg)", 220), ("Oats (500 g)", 130),
            ("Moong Dal (1 kg)", 140), ("Toor Dal (1 kg)", 160), ("Cornflakes (500 g)", 180),
            ("Quinoa (500 g)", 250), ("Poha (500 g)", 40), ("Semolina (1 kg)", 50),
        ],
        "seasonal_peak": [9, 10, 11],  # harvest / festive stocking
    },
    "Frozen Foods": {
        "items": [
            ("Frozen Peas (500 g)", 70), ("Ice Cream (500 ml)", 150),
            ("Frozen Momos (10 pc)", 120), ("Frozen Parathas (5 pc)", 90),
            ("Frozen Fish Fingers", 180), ("Frozen Pizza", 200),
            ("Ice Cream Cone (4 pc)", 100),
        ],
        "seasonal_peak": [3, 4, 5, 6, 7],
    },
    "Personal Care": {
        "items": [
            ("Shampoo (200 ml)", 180), ("Soap (4 pc)", 120), ("Toothpaste (100 g)", 90),
            ("Face Wash (100 ml)", 200), ("Hand Sanitizer (200 ml)", 100),
            ("Deodorant (150 ml)", 220), ("Body Lotion (200 ml)", 250),
        ],
        "seasonal_peak": [],  # stable year-round
    },
    "Household Items": {
        "items": [
            ("Detergent (1 kg)", 190), ("Dish Soap (500 ml)", 70), ("Floor Cleaner (1 L)", 120),
            ("Tissue Box (100 pc)", 80), ("Garbage Bags (30 pc)", 90),
            ("Aluminium Foil (9 m)", 100), ("Kitchen Towel (2 rolls)", 110),
        ],
        "seasonal_peak": [],
    },
}

# Flatten product catalog
product_list = []
for category, info in PRODUCTS.items():
    for item_name, base_price in info["items"]:
        product_list.append({
            "product_name": item_name,
            "category": category,
            "base_price": base_price,
            "seasonal_peak": info["seasonal_peak"],
        })


def generate_dates(n: int) -> np.ndarray:
    """Generate transaction dates with weekend and festive-season bias."""
    total_days = (DATE_END - DATE_START).days
    dates = []
    while len(dates) < n:
        day_offset = np.random.randint(0, total_days)
        dt = DATE_START + timedelta(days=int(day_offset))
        weekday = dt.weekday()
        month = dt.month
        # Higher chance on weekends
        accept_prob = 0.85 if weekday < 5 else 1.0
        # Boost October–December (festive) and January (New Year)
        if month in (10, 11, 12, 1):
            accept_prob *= 1.15
        if np.random.random() < accept_prob:
            dates.append(dt)
    return np.array(dates[:n])


def generate_hours(n: int) -> np.ndarray:
    """Transaction hours follow a bimodal distribution (morning + evening rush)."""
    morning = np.random.normal(10, 1.5, n // 2).clip(7, 14).astype(int)
    evening = np.random.normal(18, 1.5, n - n // 2).clip(14, 22).astype(int)
    hours = np.concatenate([morning, evening])
    np.random.shuffle(hours)
    return hours


def seasonal_multiplier(month: int, peak_months: list) -> float:
    if not peak_months:
        return 1.0
    return 1.35 if month in peak_months else 0.85


def main():
    print("=" * 60)
    print("  SmartGrocer Analytics — Dataset Generator")
    print("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"\n[1/6] Generating {NUM_ROWS:,} transaction dates ...")
    dates = generate_dates(NUM_ROWS)
    hours = generate_hours(NUM_ROWS)

    print("[2/6] Assigning customers, stores, and locations ...")
    customer_ids = np.random.randint(1000, 1000 + CUSTOMER_COUNT, size=NUM_ROWS)
    store_ids = np.random.randint(1, STORE_COUNT + 1, size=NUM_ROWS)
    cities = np.random.choice(CITIES, size=NUM_ROWS, p=CITY_WEIGHTS)
    payment_methods = np.random.choice(PAYMENT_METHODS, size=NUM_ROWS, p=PAYMENT_WEIGHTS)

    print("[3/6] Selecting products with seasonal weighting ...")
    records = []
    for i in range(NUM_ROWS):
        month = dates[i].month
        # Weight products by seasonal relevance
        weights = np.array([
            seasonal_multiplier(month, p["seasonal_peak"]) for p in product_list
        ])
        weights /= weights.sum()
        idx = np.random.choice(len(product_list), p=weights)
        product = product_list[idx]

        quantity = int(np.random.exponential(2) + 1)
        price_noise = np.random.uniform(0.90, 1.10)
        unit_price = round(product["base_price"] * price_noise, 2)
        discount = np.random.choice([0, 5, 10, 15, 20], p=[0.50, 0.20, 0.15, 0.10, 0.05])
        total_amount = round(unit_price * quantity * (1 - discount / 100), 2)

        records.append({
            "transaction_id": f"TXN-{i+1:07d}",
            "date": dates[i].strftime("%Y-%m-%d"),
            "hour": int(hours[i]),
            "customer_id": f"CUST-{customer_ids[i]:05d}",
            "store_id": f"STORE-{store_ids[i]:03d}",
            "city": cities[i],
            "product_name": product["product_name"],
            "category": product["category"],
            "quantity": quantity,
            "unit_price": unit_price,
            "discount_pct": discount,
            "total_amount": total_amount,
            "payment_method": payment_methods[i],
        })

    df = pd.DataFrame(records)

    print("[4/6] Injecting outliers (~0.5%) ...")
    outlier_idx = np.random.choice(NUM_ROWS, size=int(NUM_ROWS * 0.005), replace=False)
    df.loc[outlier_idx, "quantity"] = np.random.randint(50, 200, size=len(outlier_idx))
    df.loc[outlier_idx, "total_amount"] = (
        df.loc[outlier_idx, "unit_price"]
        * df.loc[outlier_idx, "quantity"]
        * (1 - df.loc[outlier_idx, "discount_pct"] / 100)
    ).round(2)

    print("[5/6] Injecting missing values (~2.5%) ...")
    for col, frac in [("customer_id", 0.015), ("city", 0.01),
                      ("payment_method", 0.008), ("discount_pct", 0.005)]:
        mask = np.random.random(NUM_ROWS) < frac
        df.loc[mask, col] = np.nan

    print(f"[6/6] Saving to {OUTPUT_FILE} ...")
    df.to_csv(OUTPUT_FILE, index=False)

    print("\n" + "=" * 60)
    print("  Dataset Summary")
    print("=" * 60)
    print(f"  Rows          : {len(df):,}")
    print(f"  Columns       : {len(df.columns)}")
    print(f"  Date range    : {df['date'].min()} to {df['date'].max()}")
    print(f"  Categories    : {df['category'].nunique()}")
    print(f"  Products      : {df['product_name'].nunique()}")
    print(f"  Cities        : {df['city'].dropna().nunique()}")
    print(f"  Customers     : {df['customer_id'].dropna().nunique()}")
    print(f"  Null values   : {df.isnull().sum().sum():,}")
    print(f"  File size     : {os.path.getsize(OUTPUT_FILE) / (1024**2):.1f} MB")
    print("=" * 60)


if __name__ == "__main__":
    main()
