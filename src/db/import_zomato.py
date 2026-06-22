#!/usr/bin/env python3
import os
import csv
import psycopg2

# Database Connection Details
conn_params = {
    "host": "localhost",
    "database": "ashpaz_db",
    "user": "ashpaz_user",
    "password": "password123"
}

csv_file_path = os.path.join(os.path.dirname(__file__), "..", "..", "zomato.csv")

def run_pipeline():
    try:
        # 1. Connect to PostgreSQL
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        print("Successfully connected to ashpaz_db.")

        # 2. Reset and build the staging table cleanly to match the 14 columns exactly
        cursor.execute("DROP TABLE IF EXISTS raw_zomato_staging CASCADE;")
        cursor.execute("""
            CREATE TABLE raw_zomato_staging (
                address TEXT,
                name VARCHAR(255),
                online_order VARCHAR(10),
                book_table VARCHAR(10),
                rate VARCHAR(50),
                votes VARCHAR(50),
                phone TEXT,
                location TEXT,
                rest_type TEXT,
                cuisines TEXT,
                "approx_cost(for two people)" TEXT,
                menu_item TEXT,
                "listed_in(type)" TEXT,
                "listed_in(city)" TEXT
            );
        """)
        conn.commit()
        print("Staging table initialized to match CSV structure.")

        # 3. Read and stream CSV rows directly using Python's engine
        print("Parsing 'zomato.csv' and streaming records into staging...")
        with open(csv_file_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader) # Skip CSV header line

            insert_staging_query = """
                INSERT INTO raw_zomato_staging (
                    address, name, online_order, book_table, rate, votes, phone,
                    location, rest_type, cuisines, "approx_cost(for two people)", 
                    menu_item, "listed_in(type)", "listed_in(city)"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            batch = []
            row_count = 0
            for row in reader:
                # Ensure row has exactly 14 elements
                if len(row) < 14:
                    row.extend([None] * (14 - len(row)))
                elif len(row) > 14:
                    row = row[:14]
                batch.append(tuple(row))
                row_count += 1

                if len(batch) == 1000:
                    cursor.executemany(insert_staging_query, batch)
                    batch = []

            if batch:
                cursor.executemany(insert_staging_query, batch)
        
        print(f"Staging complete: Loaded {row_count} raw rows.")

        # 4. Execute Normalization Step 1: Locations
        print("Normalizing Step 1: Populating 'locations' table...")
        cursor.execute("""
            INSERT INTO locations (location_name, listed_in_city)
            SELECT DISTINCT location, "listed_in(city)"
            FROM raw_zomato_staging
            WHERE location IS NOT NULL AND "listed_in(city)" IS NOT NULL
            ON CONFLICT (location_name, listed_in_city) DO NOTHING;
        """)
        print(f"Locations populated: {cursor.rowcount} distinct areas saved.")

        # 5. Execute Normalization Step 2: Restaurants
        print("Normalizing Step 2: Populating 'restaurants' table...")
        cursor.execute("""
            INSERT INTO restaurants (name, address, phone, location_id)
            SELECT DISTINCT r.name, r.address, r.phone, l.location_id
            FROM raw_zomato_staging r
            JOIN locations l ON r.location = l.location_name AND r."listed_in(city)" = l.listed_in_city;
        """)
        print(f"Restaurants populated: {cursor.rowcount} unique venues saved.")

        # 6. Execute Normalization Step 3: Metrics and Services
        print("Normalizing Step 3: Populating 'restaurant_metrics_services' table...")
        cursor.execute("""
            INSERT INTO restaurant_metrics_services (
                restaurant_id, online_order, book_table, rate, votes, 
                rest_type, cuisines, approx_cost_two, menu_item, listed_in_type
            )
            SELECT 
                res.restaurant_id,
                CASE WHEN raw.online_order = 'Yes' THEN TRUE ELSE FALSE END,
                CASE WHEN raw.book_table = 'Yes' THEN TRUE ELSE FALSE END,
                CAST(
                    NULLIF(
                        NULLIF(
                            NULLIF(
                                TRIM(SPLIT_PART(raw.rate, '/', 1)), 
                                'NEW'
                            ), 
                            '-'
                        ),
                        ''
                    ) AS NUMERIC
                ),
                CAST(NULLIF(REGEXP_REPLACE(raw.votes, '\\D', '', 'g'), '') AS INTEGER),
                raw.rest_type,
                raw.cuisines,
                CAST(NULLIF(REGEXP_REPLACE(raw."approx_cost(for two people)", '\\D', '', 'g'), '') AS INTEGER),
                raw.menu_item,
                raw."listed_in(type)"
            FROM raw_zomato_staging raw
            JOIN locations loc ON raw.location = loc.location_name AND raw."listed_in(city)" = loc.listed_in_city
            JOIN restaurants res ON raw.name = res.name AND raw.address = res.address AND res.location_id = loc.location_id;
        """)
        print(f"Metrics and services populated: {cursor.rowcount} entries saved.")

        # --- THIS IS THE CRITICAL LINE THAT WAS OMITTED OR BYPASSED ---
        conn.commit() 
        print("\n🎉 PIPELINE SUCCESSFUL! All relational tables are permanently populated.")

    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        if 'conn' in locals():
            conn.rollback()
            print("Transaction rolled back safely.")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    run_pipeline()