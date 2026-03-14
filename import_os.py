import os
import sqlite3
import random
import datetime
import pandas as pd
import matplotlib.pyplot as plt

# --------------------------------------------------
# Project folders
# --------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUT_DIR = os.path.join(BASE_DIR, "output")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "housing.db")

# --------------------------------------------------
# Database
# --------------------------------------------------

def get_connection():
    return sqlite3.connect(DB_PATH)


def create_table(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS housing (
        id INTEGER,
        city TEXT,
        property_type TEXT,
        bedrooms INTEGER,
        bathrooms INTEGER,
        area_sqft INTEGER,
        price REAL,
        year_built INTEGER,
        listing_date TEXT
    )
    """
    conn.execute(sql)
    conn.commit()


# --------------------------------------------------
# Data collection (simulation)
# --------------------------------------------------

def generate_data(n=500):
    cities = ["Delhi","Mumbai","Bangalore","Chennai","Hyderabad","Pune","Kolkata"]
    types = ["Apartment","Villa","Independent House","Studio"]

    rows = []

    for i in range(1, n + 1):
        city = random.choice(cities)
        t = random.choice(types)
        bedrooms = random.randint(1, 5)
        bathrooms = random.randint(1, 4)
        area = random.randint(400, 3000)
        price = area * random.randint(3500, 12000)
        year = random.randint(1998, 2024)

        rows.append(
            (i, city, t, bedrooms, bathrooms, area,
             price, year, str(datetime.date.today()))
        )

    return rows


def insert_data(conn, rows):
    conn.execute("DELETE FROM housing")
    conn.executemany(
        "INSERT INTO housing VALUES (?,?,?,?,?,?,?,?,?)", rows
    )
    conn.commit()


# --------------------------------------------------
# Data extraction
# --------------------------------------------------

def extract_data(conn):
    return pd.read_sql_query("SELECT * FROM housing", conn)


# --------------------------------------------------
# Data preparation
# --------------------------------------------------

def clean_data(df):

    df = df.drop_duplicates()

    df["city"] = df["city"].str.strip().str.title()
    df["property_type"] = df["property_type"].str.strip().str.title()

    df = df[df["area_sqft"] > 0]
    df = df[df["price"] > 0]

    df["price_per_sqft"] = df["price"] / df["area_sqft"]
    df["property_age"] = datetime.datetime.now().year - df["year_built"]

    return df


# --------------------------------------------------
# Analytics
# --------------------------------------------------

def city_summary(df):
    return (
        df.groupby("city")
          .agg(
              avg_price=("price","mean"),
              avg_area=("area_sqft","mean"),
              avg_pps=("price_per_sqft","mean"),
              total_listings=("id","count")
          )
          .reset_index()
    )


def type_summary(df):
    return (
        df.groupby("property_type")
          .agg(
              avg_price=("price","mean"),
              listings=("id","count")
          )
          .reset_index()
    )


# --------------------------------------------------
# Visualization
# --------------------------------------------------

def plot_price_distribution(df):
    plt.figure(figsize=(7,5))
    plt.hist(df["price"], bins=40)
    plt.title("Price Distribution")
    plt.xlabel("Price")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR,"price_distribution.png"))
    plt.close()


def plot_city_average(city_df):
    plt.figure(figsize=(7,5))
    plt.bar(city_df["city"], city_df["avg_price"])
    plt.xticks(rotation=45)
    plt.title("Average Price by City")
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR,"avg_price_city.png"))
    plt.close()


# --------------------------------------------------
# Export for Tableau
# --------------------------------------------------

def export_for_tableau(df, city_df, type_df):
    df.to_csv(os.path.join(OUT_DIR,"housing_clean.csv"), index=False)
    city_df.to_csv(os.path.join(OUT_DIR,"city_summary.csv"), index=False)
    type_df.to_csv(os.path.join(OUT_DIR,"type_summary.csv"), index=False)


# --------------------------------------------------
# Main pipeline
# --------------------------------------------------

def main():

    print("Connecting database...")
    conn = get_connection()

    print("Creating table...")
    create_table(conn)

    print("Collecting dataset...")
    rows = generate_data(800)

    print("Loading dataset...")
    insert_data(conn, rows)

    print("Extracting data...")
    raw_df = extract_data(conn)

    print("Preparing data...")
    clean_df = clean_data(raw_df)

    print("Creating analytics tables...")
    city_df = city_summary(clean_df)
    type_df = type_summary(clean_df)

    print("Creating visualizations...")
    plot_price_distribution(clean_df)
    plot_city_average(city_df)

    print("Exporting for Tableau...")
    export_for_tableau(clean_df, city_df, type_df)

    conn.close()

    print("Project completed successfully")


# --------------------------------------------------
# Run
# --------------------------------------------------

if __name__ == "__main__":
    main()