"""
Synthetic E-commerce Data Generator
=====================================
Generates realistic e-commerce data and seeds a Postgres database.

Usage:
    python scripts/generate_data.py
    python scripts/generate_data.py --customers 500 --orders 2000
    python scripts/generate_data.py --reset  # drop and recreate tables first

Tables created:
    customers, products, suppliers, inventory, orders, order_items, returns
"""

import argparse
import random
import uuid
from datetime import datetime, timedelta

import psycopg2
from faker import Faker

# ── Config ────────────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "ecommerce",
    "user": "ecommerce",
    "password": "ecommerce",
}

fake = Faker()
random.seed(42)
Faker.seed(42)

# ── Product catalogue ─────────────────────────────────────────────────────────

CATEGORIES = {
    "Electronics": [
        ("Wireless Headphones", 49.99, 199.99),
        ("USB-C Hub", 19.99, 59.99),
        ("Mechanical Keyboard", 59.99, 249.99),
        ("Webcam 1080p", 39.99, 129.99),
        ("Portable Charger", 14.99, 49.99),
        ("Smart Watch", 89.99, 399.99),
        ("Bluetooth Speaker", 29.99, 149.99),
        ("Monitor Stand", 24.99, 89.99),
    ],
    "Home & Kitchen": [
        ("Coffee Maker", 29.99, 149.99),
        ("Air Fryer", 49.99, 199.99),
        ("Blender", 24.99, 89.99),
        ("Knife Set", 19.99, 99.99),
        ("Cutting Board", 9.99, 39.99),
        ("Toaster", 19.99, 79.99),
        ("Food Storage Set", 14.99, 49.99),
        ("Electric Kettle", 19.99, 69.99),
    ],
    "Sports & Outdoors": [
        ("Yoga Mat", 14.99, 59.99),
        ("Resistance Bands", 9.99, 29.99),
        ("Water Bottle", 9.99, 39.99),
        ("Foam Roller", 14.99, 44.99),
        ("Jump Rope", 7.99, 24.99),
        ("Hiking Backpack", 49.99, 199.99),
        ("Running Shoes", 59.99, 179.99),
        ("Gym Gloves", 9.99, 29.99),
    ],
    "Books": [
        ("The Pragmatic Programmer", 29.99, 49.99),
        ("Designing Data-Intensive Applications", 39.99, 59.99),
        ("Clean Code", 24.99, 44.99),
        ("The Phoenix Project", 14.99, 29.99),
        ("Fundamentals of Data Engineering", 34.99, 54.99),
        ("Python Crash Course", 19.99, 39.99),
        ("System Design Interview", 24.99, 44.99),
        ("AWS Certified Solutions Architect", 29.99, 49.99),
    ],
    "Clothing": [
        ("Men's T-Shirt", 9.99, 39.99),
        ("Women's Hoodie", 19.99, 79.99),
        ("Running Shorts", 14.99, 49.99),
        ("Baseball Cap", 9.99, 34.99),
        ("Winter Jacket", 49.99, 199.99),
        ("Compression Socks", 7.99, 24.99),
        ("Polo Shirt", 14.99, 59.99),
        ("Denim Jeans", 29.99, 99.99),
    ],
}

WAREHOUSES = ["KHI-01", "LHE-02", "ISB-03", "KHI-04"]

ORDER_STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
RETURN_REASONS = [
    "defective_product",
    "wrong_item",
    "not_as_described",
    "changed_mind",
    "damaged_in_shipping",
]

# ── DDL ───────────────────────────────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id     UUID PRIMARY KEY,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(255) UNIQUE NOT NULL,
    phone           VARCHAR(30),
    city            VARCHAR(100),
    country         VARCHAR(100),
    segment         VARCHAR(20) CHECK (segment IN ('retail', 'wholesale', 'vip')),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id     UUID PRIMARY KEY,
    name            VARCHAR(200) NOT NULL,
    contact_email   VARCHAR(255),
    country         VARCHAR(100),
    lead_time_days  INT,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    product_id      UUID PRIMARY KEY,
    sku             VARCHAR(50) UNIQUE NOT NULL,
    name            VARCHAR(200) NOT NULL,
    category        VARCHAR(100),
    price           NUMERIC(10,2) NOT NULL,
    cost            NUMERIC(10,2) NOT NULL,
    supplier_id     UUID REFERENCES suppliers(supplier_id),
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inventory (
    inventory_id    UUID PRIMARY KEY,
    product_id      UUID REFERENCES products(product_id),
    warehouse       VARCHAR(20) NOT NULL,
    quantity        INT NOT NULL DEFAULT 0,
    reorder_level   INT NOT NULL DEFAULT 10,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id        UUID PRIMARY KEY,
    customer_id     UUID REFERENCES customers(customer_id),
    status          VARCHAR(20) NOT NULL,
    shipping_city   VARCHAR(100),
    shipping_country VARCHAR(100),
    discount_pct    NUMERIC(5,2) DEFAULT 0,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id   UUID PRIMARY KEY,
    order_id        UUID REFERENCES orders(order_id),
    product_id      UUID REFERENCES products(product_id),
    quantity        INT NOT NULL,
    unit_price      NUMERIC(10,2) NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS returns (
    return_id       UUID PRIMARY KEY,
    order_item_id   UUID REFERENCES order_items(order_item_id),
    reason          VARCHAR(50),
    refund_amount   NUMERIC(10,2),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);
"""

DROP_DDL = """
DROP TABLE IF EXISTS returns CASCADE;
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS inventory CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS suppliers CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
"""

# ── Generators ────────────────────────────────────────────────────────────────

def random_date(start_days_ago=365, end_days_ago=0):
    start = datetime.now() - timedelta(days=start_days_ago)
    end = datetime.now() - timedelta(days=end_days_ago)
    return start + (end - start) * random.random()


def generate_customers(n=200):
    segments = ["retail"] * 70 + ["wholesale"] * 20 + ["vip"] * 10
    customers = []
    for _ in range(n):
        created = random_date(730, 30)
        customers.append((
            str(uuid.uuid4()),
            fake.first_name(),
            fake.last_name(),
            fake.unique.email(),
            fake.phone_number()[:20],
            fake.city(),
            fake.country(),
            random.choice(segments),
            created,
            created,
        ))
    return customers


def generate_suppliers(n=10):
    suppliers = []
    for _ in range(n):
        suppliers.append((
            str(uuid.uuid4()),
            fake.company(),
            fake.company_email(),
            fake.country(),
            random.randint(3, 30),
            random_date(1000, 365),
        ))
    return suppliers


def generate_products(supplier_ids):
    products = []
    for category, items in CATEGORIES.items():
        for name, min_price, max_price in items:
            price = round(random.uniform(min_price, max_price), 2)
            cost = round(price * random.uniform(0.35, 0.65), 2)
            sku = f"{category[:3].upper()}-{fake.bothify('????-####').upper()}"
            products.append((
                str(uuid.uuid4()),
                sku,
                name,
                category,
                price,
                cost,
                random.choice(supplier_ids),
                True,
                random_date(500, 30),
                random_date(30, 0),
            ))
    return products


def generate_inventory(product_ids):
    inventory = []
    for product_id in product_ids:
        for warehouse in random.sample(WAREHOUSES, k=random.randint(1, 3)):
            inventory.append((
                str(uuid.uuid4()),
                product_id,
                warehouse,
                random.randint(0, 500),
                random.randint(5, 30),
                random_date(30, 0),
            ))
    return inventory


def generate_orders_and_items(customer_ids, product_prices, n_orders=1000):
    orders = []
    order_items = []

    for _ in range(n_orders):
        order_id = str(uuid.uuid4())
        customer_id = random.choice(customer_ids)
        status = random.choices(
            ORDER_STATUSES,
            weights=[5, 10, 15, 65, 5],
            k=1
        )[0]
        created = random_date(365, 0)
        discount = random.choices([0, 5, 10, 15, 20], weights=[60, 15, 12, 8, 5], k=1)[0]

        orders.append((
            order_id,
            customer_id,
            status,
            fake.city(),
            fake.country(),
            discount,
            created,
            created,
        ))

        # 1–5 items per order
        n_items = random.randint(1, 5)
        for _ in range(n_items):
            product_id = random.choice(list(product_prices.keys()))
            qty = random.randint(1, 4)
            unit_price = product_prices[product_id]
            order_items.append((
                str(uuid.uuid4()),
                order_id,
                product_id,
                qty,
                unit_price,
                created,
            ))

    return orders, order_items


def generate_returns(order_items, return_rate=0.05):
    returns = []
    for item in order_items:
        if random.random() < return_rate:
            order_item_id, _, _, qty, unit_price, created_at = item
            refund = round(unit_price * qty * random.uniform(0.8, 1.0), 2)
            return_date = created_at + timedelta(days=random.randint(1, 30))
            returns.append((
                str(uuid.uuid4()),
                order_item_id,
                random.choice(RETURN_REASONS),
                refund,
                return_date,
            ))
    return returns


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def bulk_insert(cur, table, columns, rows, batch_size=500):
    col_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    for i in range(0, len(rows), batch_size):
        cur.executemany(sql, rows[i:i + batch_size])
    return len(rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def main(n_customers=200, n_orders=1000, reset=False):
    print("🔌 Connecting to Postgres...")
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    if reset:
        print("🗑️  Dropping existing tables...")
        cur.execute(DROP_DDL)
        conn.commit()

    print("🏗️  Creating tables...")
    cur.execute(DDL)
    conn.commit()

    print(f"👥 Generating {n_customers} customers...")
    customers = generate_customers(n_customers)
    n = bulk_insert(cur, "customers",
        ["customer_id","first_name","last_name","email","phone","city","country","segment","created_at","updated_at"],
        customers)
    print(f"   ✅ {n} customers inserted")

    print("🏭 Generating suppliers...")
    suppliers = generate_suppliers(10)
    supplier_ids = [s[0] for s in suppliers]
    bulk_insert(cur, "suppliers",
        ["supplier_id","name","contact_email","country","lead_time_days","created_at"],
        suppliers)
    print(f"   ✅ {len(suppliers)} suppliers inserted")

    print("📦 Generating products...")
    products = generate_products(supplier_ids)
    product_ids = [p[0] for p in products]
    product_prices = {p[0]: p[4] for p in products}
    bulk_insert(cur, "products",
        ["product_id","sku","name","category","price","cost","supplier_id","is_active","created_at","updated_at"],
        products)
    print(f"   ✅ {len(products)} products inserted")

    print("🏪 Generating inventory...")
    inventory = generate_inventory(product_ids)
    bulk_insert(cur, "inventory",
        ["inventory_id","product_id","warehouse","quantity","reorder_level","updated_at"],
        inventory)
    print(f"   ✅ {len(inventory)} inventory records inserted")

    print(f"🛒 Generating {n_orders} orders + items...")
    customer_ids = [c[0] for c in customers]
    orders, order_items = generate_orders_and_items(customer_ids, product_prices, n_orders)
    bulk_insert(cur, "orders",
        ["order_id","customer_id","status","shipping_city","shipping_country","discount_pct","created_at","updated_at"],
        orders)
    bulk_insert(cur, "order_items",
        ["order_item_id","order_id","product_id","quantity","unit_price","created_at"],
        order_items)
    print(f"   ✅ {len(orders)} orders, {len(order_items)} order items inserted")

    print("↩️  Generating returns (~5% of items)...")
    returns = generate_returns(order_items)
    bulk_insert(cur, "returns",
        ["return_id","order_item_id","reason","refund_amount","created_at"],
        returns)
    print(f"   ✅ {len(returns)} returns inserted")

    conn.commit()
    cur.close()
    conn.close()

    print("\n🎉 Done! Summary:")
    print(f"   Customers  : {len(customers)}")
    print(f"   Suppliers  : {len(suppliers)}")
    print(f"   Products   : {len(products)}")
    print(f"   Inventory  : {len(inventory)}")
    print(f"   Orders     : {len(orders)}")
    print(f"   Order items: {len(order_items)}")
    print(f"   Returns    : {len(returns)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed ecommerce Postgres DB with synthetic data")
    parser.add_argument("--customers", type=int, default=200, help="Number of customers (default: 200)")
    parser.add_argument("--orders", type=int, default=1000, help="Number of orders (default: 1000)")
    parser.add_argument("--reset", action="store_true", help="Drop and recreate all tables first")
    args = parser.parse_args()

    main(n_customers=args.customers, n_orders=args.orders, reset=args.reset)