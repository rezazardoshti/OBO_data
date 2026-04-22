import duckdb


def main():
    con = duckdb.connect("serving/analytics.duckdb")

    print("\n=== TABLES / VIEWS ===")
    print(con.execute("SHOW TABLES FROM analytics").fetchdf())

    print("\n=== SALES DASHBOARD ===")
    print(con.execute("SELECT * FROM analytics.vw_sales_dashboard LIMIT 10").fetchdf())

    print("\n=== PRODUCT DASHBOARD ===")
    print(con.execute("SELECT * FROM analytics.vw_product_dashboard LIMIT 10").fetchdf())

    print("\n=== CUSTOMER DASHBOARD ===")
    print(con.execute("SELECT * FROM analytics.vw_customer_dashboard LIMIT 10").fetchdf())


if __name__ == "__main__":
    main()