import duckdb
con = duckdb.connect("api/warehouse.duckdb")
con.execute("DELETE FROM ads_spend;")
con.close()
