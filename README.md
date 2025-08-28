# AI Data Engineer Test

This project implements the Data Engineer challenge focusing on data ingestion, KPI modeling, and exposing metrics via API.

## Architecture

Project workflow:

1. **Ingestion (n8n + FastAPI + DuckDB)**  
   - Dataset `ads_spend.csv` is downloaded and sent via **n8n workflow** to the API (`/ingest`).  
   - The API inserts the data into **DuckDB**, with metadata (`load_date`, `source_file_name`).  
   - Data persists after refresh.

2. **KPI Modeling (SQL)**  
   - Metrics calculated:
     - **CAC** = `spend / conversions`
     - **ROAS** = `(conversions * 100) / spend` (revenue = conversions × 100)
   - Queries comparing last 30 days vs. prior 30 days.

3. **Analyst Access (FastAPI)**  
   - Endpoints expose metrics in JSON:
     - `POST /ingest` → Data ingestion.
     - `GET /metrics?start=YYYY-MM-DD&end=YYYY-MM-DD` → KPIs for a given interval.
     - `GET /bounds` → Time window discovery (last 30d, prior 30d).
     - `GET /compare_30d` → KPIs comparison between last 30 days and prior 30 days.

4. **Agent Demo (Optional)**  
   - Simple script maps natural language question to the corresponding query.  
   - Example:  
     **Input:** `"Compare CAC and ROAS for last 30 days vs prior 30 days"`  
     **Output:** JSON with CAC and ROAS metrics.

## How to Run

### 1. Clone repository
```bash
git clone https://github.com/<your-username>/ai-data-engineer-test.git
cd ai-data-engineer-test
````

### 2. Create virtual environment

```bash
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
.venv\Scripts\activate      # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run API

```bash
cd api
uvicorn main:app --host 0.0.0.0 --port 8000
```

API docs: [http://localhost:8000/docs](http://localhost:8000/docs)

---

## Main Endpoints

* **POST /ingest** → Upload CSV.
  Example:

  ```bash
  curl -F "file=@ads_spend.csv" http://localhost:8000/ingest
  ```

* **GET /metrics**

  ```bash
  http://localhost:8000/metrics?start=2025-07-01&end=2025-07-31
  ```

* **GET /compare\_30d**

  ```bash
  http://localhost:8000/compare_30d
  ```

---

## 🔄 n8n Workflow

* Exported workflow available at `workflows/ingest_n8n.json`.
* It performs:

  1. Download of `ads_spend.csv` (Google Drive).
  2. POST request to `http://host.docker.internal:8000/ingest`.


---

## Agent Demo

Run the agent:

```bash
python scripts/agent_demo.py
```

Example output:

```json
[
  {
    "metric": "CAC",
    "last_30d": 29.80,
    "prev_30d": 32.27,
    "delta_abs": -2.46,
    "delta_pct": -7.62
  },
  {
    "metric": "ROAS",
    "last_30d": 3.35,
    "prev_30d": 3.09,
    "delta_abs": 0.25,
    "delta_pct": 8.26
  }
]


