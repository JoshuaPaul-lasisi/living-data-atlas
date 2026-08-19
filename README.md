# Living Data Atlas

A continuously updated data platform for Nigeria: economic, weather, and
air-quality data pulled from public APIs, stored in one long-format fact
table, and served through a live dashboard. Runs entirely on free-tier cloud
infrastructure, so it doesn't depend on Nigeria's power or internet — the
pipeline runs on GitHub's servers, not yours.

See `docs/PROCESS.md` for the full architecture writeup and the reasoning
behind each decision. `process_log.txt` is the original day-by-day journal
from when this started as a local Docker project.

## Architecture

```
World Bank API ─┐
Open-Meteo API  ─┼─► etl/*_loader.py ─► core.observations (Supabase Postgres)
OpenAQ API      ─┘         ▲                      │
                            │                      ▼
              GitHub Actions (daily cron)    app.py (Streamlit Cloud)
```

- **Database**: [Supabase](https://supabase.com) Postgres. Schema in `sql/schema.sql`.
  Everything lands in a single table, `core.observations` — one row per
  `(date, indicator, region, source)`. Adding a new data source never needs a
  migration, just a new `indicator` name.
- **Ingestion**: `.github/workflows/etl.yml` runs the three loaders in
  `etl/` daily via GitHub Actions cron. No server of yours needs to be running.
- **Dashboard**: `app.py`, a Streamlit app reading straight from Supabase,
  deployed on Streamlit Community Cloud.
- **Monitoring**: every loader run writes a row to `ops.ingestion_log`,
  visible in the dashboard's "Pipeline Health" tab.

## Setup

### 1. Supabase

1. Create a project at supabase.com.
2. In the SQL Editor, run `sql/schema.sql`.
3. Project Settings → Database → Connection string → URI. This is your `DATABASE_URL`.

### 2. Local development

```bash
cp .env.example .env   # fill in DATABASE_URL and OPENAQ_API_KEY
pip install -r requirements.txt

python etl/worldbank_loader.py
python etl/weather_loader.py
python etl/airquality_loader.py

streamlit run app.py
```

`OPENAQ_API_KEY` comes from an account at openaq.org (API Keys section).

### 3. GitHub Actions (scheduled ingestion)

In the repo's Settings → Secrets and variables → Actions, add:

- `DATABASE_URL`
- `OPENAQ_API_KEY`

The `ETL` workflow then runs daily at 03:00 UTC, or on demand via
Actions → ETL → Run workflow.

### 4. Streamlit Community Cloud (dashboard)

Deploy `app.py` from this repo at share.streamlit.io. In the app's Settings →
Secrets, add:

```toml
DATABASE_URL = "postgresql://..."
```

## Adding a new data source

1. Add a `etl/<source>_loader.py` with a `normalize()` function that returns
   a DataFrame with `date`, `indicator`, `region`, `value`, and optional `meta`.
2. Call `load_observations(df, source="...")` and `log_ingestion(...)` from
   `etl_utils.py`, same as the existing loaders.
3. Add a step to `.github/workflows/etl.yml`.
4. If it's economic/weather/air-quality-shaped, it shows up in the dashboard
   automatically once you add its indicator name to `app.py`'s indicator lists.

## Repo layout

```
etl/                  loaders + shared config/db utilities
sql/schema.sql         Supabase schema (core.observations, core.alerts, ops.ingestion_log)
.github/workflows/     scheduled ETL runs
app.py                 Streamlit dashboard
docs/PROCESS.md         architecture decisions and reasoning
```
