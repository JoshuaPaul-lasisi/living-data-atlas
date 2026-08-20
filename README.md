# Living Data Atlas

A continuously updated data platform for Nigeria: economic, weather,
air-quality, and financial-market data pulled from public sources, stored in
one long-format fact table, and served through a live dashboard. Runs
entirely on free-tier cloud infrastructure, so it doesn't depend on
Nigeria's power or internet — the pipeline runs on GitHub's servers, not
yours.

See `docs/PROCESS.md` for the full architecture writeup and the reasoning
behind each decision. `process_log.txt` is the original day-by-day journal
from when this started as a local Docker project.

## Architecture

```
World Bank API ─┐
Open-Meteo API  ─┤
OpenAQ API      ─┼─► etl/*_loader.py ─► core.observations (Neon Postgres)
NGX Pulse API   ─┤         ▲                      │
CBN (scraped)   ─┘         │                      ▼
              GitHub Actions (daily cron)    app.py (Streamlit Cloud)
```

- **Database**: [Neon](https://neon.tech) Postgres — serverless, scales to
  zero, and auto-resumes on the next connection (no manual "unpause" step,
  unlike some free-tier providers). Schema in `sql/schema.sql`.
  Everything lands in a single table, `core.observations` — one row per
  `(date, indicator, region, source)`. Adding a new data source never needs a
  migration, just a new `indicator` name.
- **Ingestion**: `.github/workflows/etl.yml` runs the three loaders in
  `etl/` daily via GitHub Actions cron. No server of yours needs to be running.
- **Dashboard**: `app.py`, a Streamlit app reading straight from Neon,
  deployed on Streamlit Community Cloud.
- **Monitoring**: every loader run writes a row to `ops.ingestion_log`,
  visible in the dashboard's "Pipeline Health" tab.
- **Reliability**: HTTP calls retry with backoff on rate limits/5xx
  (`etl_utils.http_session`); each loader runs independently in CI so one
  broken API doesn't block the others, but the workflow still fails overall
  if any loader fails, so GitHub's normal failed-run notifications fire —
  make sure email notifications for failed workflows are on in your GitHub
  notification settings. Implausible values (e.g. a negative population, a
  55°C+ reading) get flagged into `core.alerts`, visible in the dashboard's
  "Alerts" tab.
- **Data sources**: World Bank (economic indicators), Open-Meteo (weather),
  OpenAQ (air quality), [NGX Pulse](https://ngxpulse.ng/api) (stock market
  index — public API, no key), and CBN (official USD/NGN rate). CBN has no
  public API, so `cbn_loader.py` scrapes their official rate page — the most
  likely loader to need a fix if CBN changes their page structure; check
  `ops.ingestion_log` / the "Pipeline Health" tab if it starts failing.

## Setup

### 1. Neon

1. Create a project at neon.tech.
2. In the SQL editor (or via `psql`), run `sql/schema.sql`.
3. Dashboard → Connection string (make sure it has `?sslmode=require`). This is your `DATABASE_URL`.

### 2. Local development

```bash
cp .env.example .env   # fill in DATABASE_URL and OPENAQ_API_KEY
pip install -r requirements.txt

python etl/worldbank_loader.py
python etl/weather_loader.py
python etl/airquality_loader.py
python etl/ngx_loader.py
python etl/cbn_loader.py

streamlit run app.py
```

`OPENAQ_API_KEY` comes from an account at openaq.org (API Keys section).

To backfill historical weather instead of the usual rolling window:

```bash
python etl/weather_loader.py --start 2023-01-01 --end 2023-12-31
```

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
sql/schema.sql         Neon/Postgres schema (core.observations, core.alerts, ops.ingestion_log)
.github/workflows/     scheduled ETL runs
app.py                 Streamlit dashboard
docs/PROCESS.md         architecture decisions and reasoning
```
