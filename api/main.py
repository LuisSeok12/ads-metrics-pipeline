from fastapi import FastAPI, UploadFile, File, Query, HTTPException
from fastapi.responses import JSONResponse
from datetime import date, timedelta
import os
import io
import duckdb
import pandas as pd

app = FastAPI(title="Metrics API", version="1.0.0")

DB_PATH = os.getenv("DB_PATH", os.path.abspath("warehouse.duckdb"))

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS ads_spend (
  date DATE,
  platform TEXT,
  account TEXT,
  campaign TEXT,
  country TEXT,
  device TEXT,
  spend DOUBLE,
  clicks BIGINT,
  impressions BIGINT,
  conversions BIGINT,
  load_date DATE,
  source_file_name TEXT
);
"""

def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(CREATE_SQL)

def table_exists(con: duckdb.DuckDBPyConnection, name: str = "ads_spend") -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ? COLLATE NOCASE",
        [name],
    ).fetchone()
    return row is not None

# -------------------------
# Ingestion and Metrics Endpoints
# -------------------------
@app.post("/ingest")
async def ingest(file: UploadFile = File(...)):
    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(400, f"CSV inválido: {e}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    for col in ["spend", "clicks", "impressions", "conversions"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["account"] = df["account"].astype(str)

  
    df["load_date"] = date.today()
    df["source_file_name"] = file.filename or "upload.csv"

    con = duckdb.connect(DB_PATH)  
    ensure_schema(con)
    con.register("tmp_df", df)


    con.execute("INSERT INTO ads_spend SELECT * FROM tmp_df")
    total = con.execute("SELECT COUNT(*) FROM ads_spend").fetchone()[0]
    con.close()
    return {"status": "ok", "inserted_rows": len(df), "total_rows": total, "db_path": DB_PATH}


@app.get("/metrics")
def metrics(start: date = Query(...), end: date = Query(...)):
    con = duckdb.connect(DB_PATH, read_only=True)  # leitura
    if not table_exists(con):
        con.close()
        raise HTTPException(404, "Tabela 'ads_spend' não encontrada. Faça uma ingestão primeiro.")
    q = """
    WITH period AS (
      SELECT *
      FROM ads_spend
      WHERE date BETWEEN $start AND $end
    ),
    agg AS (
      SELECT
        SUM(spend) AS spend,
        SUM(conversions) AS conversions
      FROM period
    )
    SELECT
      spend,
      conversions,
      conversions * 100.0 AS revenue,
      CASE WHEN conversions > 0 THEN spend / conversions ELSE NULL END AS CAC,
      CASE WHEN spend > 0 THEN (conversions * 100.0) / spend ELSE NULL END AS ROAS
    FROM agg;
    """
    row = con.execute(q, {"start": start, "end": end}).fetchdf().iloc[0].to_dict()
    con.close()
    return JSONResponse(row)

@app.get("/bounds")
def bounds():
    con = duckdb.connect(DB_PATH, read_only=True)
    if not table_exists(con):
        con.close()
        return {"max_date": None, "last_30d": None, "prev_30d": None}
    row = con.execute("SELECT MAX(date) FROM ads_spend").fetchone()
    if not row or not row[0]:
        con.close()
        return {"max_date": None, "last_30d": None, "prev_30d": None}
    max_date = row[0]
    last30_start = max_date - timedelta(days=29)
    prior30_end = last30_start - timedelta(days=1)
    prior30_start = prior30_end - timedelta(days=29)
    con.close()
    return {
        "max_date": str(max_date),
        "last_30d": {"start": str(last30_start), "end": str(max_date)},
        "prev_30d": {"start": str(prior30_start), "end": str(prior30_end)},
    }

@app.get("/compare_30d")
def compare_30d():
    con = duckdb.connect(DB_PATH, read_only=True)
    if not table_exists(con):
        con.close()
        raise HTTPException(404, "Tabela 'ads_spend' não encontrada. Faça uma ingestão primeiro.")
    q = """
    WITH bounds AS (
      SELECT
        MAX(date) AS max_date,
        (MAX(date) - INTERVAL 29 DAY) AS last30_start,
        (MAX(date) - INTERVAL 30 DAY) AS prior30_end,
        (MAX(date) - INTERVAL 59 DAY) AS prior30_start
      FROM ads_spend
    ),
    last30 AS (
      SELECT SUM(spend) AS spend, SUM(conversions) AS conversions,
             SUM(conversions) * 100.0 AS revenue
      FROM ads_spend, bounds
      WHERE date BETWEEN bounds.last30_start AND bounds.max_date
    ),
    prior30 AS (
      SELECT SUM(spend) AS spend, SUM(conversions) AS conversions,
             SUM(conversions) * 100.0 AS revenue
      FROM ads_spend, bounds
      WHERE date BETWEEN bounds.prior30_start AND bounds.prior30_end
    )
    SELECT * FROM (
      SELECT 'Spend' AS metric, l.spend AS last_30d, p.spend AS prev_30d
        FROM last30 l, prior30 p
      UNION ALL
      SELECT 'Conversions', l.conversions, p.conversions FROM last30 l, prior30 p
      UNION ALL
      SELECT 'Revenue', l.revenue, p.revenue FROM last30 l, prior30 p
      UNION ALL
      SELECT 'CAC',
        CASE WHEN l.conversions > 0 THEN l.spend / l.conversions ELSE NULL END,
        CASE WHEN p.conversions > 0 THEN p.spend / p.conversions ELSE NULL END
        FROM last30 l, prior30 p
      UNION ALL
      SELECT 'ROAS',
        CASE WHEN l.spend > 0 THEN l.revenue / l.spend ELSE NULL END,
        CASE WHEN p.spend > 0 THEN p.revenue / p.spend ELSE NULL END
        FROM last30 l, prior30 p
    ) t
    LEFT JOIN (
      SELECT
        'Spend' AS metric,
        (l.spend - p.spend) AS delta_abs,
        CASE WHEN p.spend <> 0 THEN (l.spend - p.spend) / p.spend * 100 ELSE NULL END AS delta_pct
      FROM last30 l, prior30 p
      UNION ALL
      SELECT 'Conversions', (l.conversions - p.conversions),
        CASE WHEN p.conversions <> 0 THEN (l.conversions - p.conversions) / p.conversions * 100 ELSE NULL END
      FROM last30 l, prior30 p
      UNION ALL
      SELECT 'Revenue', (l.revenue - p.revenue),
        CASE WHEN p.revenue <> 0 THEN (l.revenue - p.revenue) / p.revenue * 100 ELSE NULL END
      FROM last30 l, prior30 p
      UNION ALL
      SELECT 'CAC',
        CASE WHEN p.conversions > 0 AND l.conversions > 0 THEN (l.spend / l.conversions) - (p.spend / p.conversions) ELSE NULL END,
        CASE
          WHEN p.conversions > 0 AND l.conversions > 0 AND (p.spend / p.conversions) <> 0
          THEN ((l.spend / l.conversions) - (p.spend / p.conversions)) / (p.spend / p.conversions) * 100
          ELSE NULL
        END
      FROM last30 l, prior30 p
      UNION ALL
      SELECT 'ROAS',
        CASE WHEN p.spend > 0 AND l.spend > 0 THEN (l.revenue / l.spend) - (p.revenue / p.spend) ELSE NULL END,
        CASE
          WHEN p.spend > 0 AND (p.revenue / p.spend) <> 0 AND l.spend > 0
          THEN ((l.revenue / l.spend) - (p.revenue / p.spend)) / (p.revenue / p.spend) * 100
          ELSE NULL
        END
      FROM last30 l, prior30 p
    ) d USING (metric);
    """
    df = con.execute(q).fetchdf()
    con.close()
    return JSONResponse(df.to_dict(orient="records"))
