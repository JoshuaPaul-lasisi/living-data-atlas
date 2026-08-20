# Process & architecture notes

This documents what changed in the 2026-08-19 rebuild, and why. Read it
alongside `process_log.txt`, which is the original journal from when this
was a local-only Docker project.

## Starting point

Before this pass, the project had:

- A `docker-compose.yml` running Postgres + pgAdmin locally.
- A schema (`sql/schema.sql`) with four domain-specific `core` tables:
  `econ_daily`, `weather_daily`, `air_quality_daily`, `alerts`, plus
  `ops.ingestion_log`.
- Three loader scripts (World Bank, Open-Meteo, OpenAQ) sharing a
  `load_to_db()`/`log_ingestion()` helper in `etl_utils.py`.

Two problems made this not actually runnable end to end:

1. **A committed secret.** `.env` — containing a real Postgres password and
   a live OpenAQ API key — was tracked in git and pushed to GitHub across
   several commits. That key has to be treated as compromised regardless of
   what we do to the repo going forward.
2. **A schema/loader mismatch.** `weather_loader.py` and
   `airquality_loader.py` both called `load_to_db()` writing long-format
   rows (`date, indicator, region, value, source, meta`), but the actual
   `core.weather_daily` and `core.air_quality_daily` tables in
   `schema.sql` were wide-format (`temp_c, precip_mm, humidity...` /
   `pm25, pm10, aqi...`). Running those loaders against that schema would
   have failed on an `INSERT` column mismatch. Only `econ_daily` — which
   happened to already be long-format — actually matched what the code wrote.

## Why the infrastructure changed

The stated goal for this pass was to stop depending on a machine at home
(unreliable power/internet) and to stop hand-operating the pipeline. That
rules out "run Postgres and a cron job on my laptop" outright. The
alternative needed to be: free or near-free, needs no server you manage, and
usable with accounts you already have (Streamlit, Vercel) or can create in
minutes (Neon).

- **Neon over self-hosted Postgres**: same Postgres underneath, so the
  schema and SQLAlchemy code don't change in kind — only the connection
  string. Free tier is serverless (scales to zero, auto-resumes on the next
  connection). We tried Supabase first, but its free tier auto-pauses a
  project after a week of inactivity and requires a manual "unpause" click
  in its dashboard — a bad fit for a pipeline meant to run unattended, since
  a paused DB fails silently until someone notices. Neon's auto-resume needs
  no manual step. Supabase also bundles auth/storage/a table UI that this
  project doesn't use, so nothing was lost in the switch.
- **GitHub Actions over a hosted scheduler (Airflow, a VPS cron, etc.)**:
  the code already lives in a GitHub repo; Actions' free `schedule:` trigger
  needs no separate account, no server, and gives real CI/CD exposure (the
  cloud/pipeline experience noted as a gap in `process_log.txt`).
- **Streamlit Community Cloud over Vercel for the dashboard**: the dashboard
  needs to run Python, hold a live DB connection, and render Plotly charts —
  Streamlit Cloud does this natively from a GitHub repo with zero glue code.
  Vercel is a better fit for a future static landing page than for a
  stateful Python+Postgres dashboard, so it's left out of the pipeline for now.

## Why the schema was consolidated

Rather than fix each wide table to match its loader, all three domains now
write into one long/tidy table: `core.observations(date, indicator, region,
value, source, meta, updated_at)`, primary-keyed on
`(date, indicator, region, source)`.

This directly answers a question already asked in the process log ("Can't I
generalize the loading and ingestion process?"): with one shape, `load_observations()`
in `etl_utils.py` is a single generic upsert every loader calls, and adding a
new source (CBN, NGX, a public-sentiment feed) never requires a migration —
just a new `indicator` string. The cost is that querying "give me
temperature and precipitation side by side" needs a pivot rather than a
plain `SELECT *`; the dashboard's query layer absorbs that.

## What changed in each loader

- **`worldbank_loader.py`**: now loops over a set of indicators
  (`etl/config.py`) — GDP, inflation, unemployment, population, poverty,
  FX rate — instead of just GDP, and moved to `https` for the API call.
- **`weather_loader.py`**: was hardcoded to fetch January 2024 for Abuja
  only, once. It now pulls a rolling 10-day window for six cities on every
  run; re-running the same window is safe because of the upsert, which also
  covers Open-Meteo's short reporting lag.
- **`airquality_loader.py`**: moved to OpenAQ v3's `/locations` +
  `/locations/{id}/latest` pattern across the same six cities, with each
  city/location wrapped in its own try/except so one bad station doesn't
  drop the whole run. This is a "latest reading per run" model, not a true
  historical daily aggregate — OpenAQ coverage in Nigeria is sparse enough
  that a daily-aggregate endpoint would return empty for most stations most
  days. Revisit if/when OpenAQ station density in Nigeria improves.

All three `normalize()` functions were unit-tested offline against
hand-built sample payloads shaped like each API's real response (see the
assertions run during this build) before being wired to the database.

## Known limitations / next steps

- GitHub Actions free-tier cron can run a few minutes late; not a problem
  for daily granularity.

## Robustness pass (first "make it robust" iteration)

Once the pipeline was live, three gaps became the priority: a broken loader
was invisible (green CI, silent failure), there was no way to backfill
history, and nothing checked whether a value was even plausible before it
landed in the database.

- **Retries**: `etl_utils.http_session()` wraps every loader's HTTP calls in
  a `requests.Session` with backoff on 429/5xx, so a transient rate limit or
  a dropped connection doesn't kill an entire run.
- **Visible CI failures**: each loader step in `etl.yml` still runs
  independently (`continue-on-error: true`, so one broken API doesn't block
  the other two), but a final step now checks all three outcomes and fails
  the job if any loader failed. Before this change, `continue-on-error`
  meant the workflow stayed green even when every loader failed — the only
  way to notice was opening the dashboard and seeing stale data. Now GitHub's
  own failed-workflow notification does that job.
- **Backfill**: `weather_loader.py` takes `--start`/`--end` for a one-off
  historical pull, chunked into ≤31-day requests (the Open-Meteo range limit
  noted in `process_log.txt`) via `fetch_weather_range()`. World Bank already
  pulls full history by nature of its API; OpenAQ's "latest reading" model
  doesn't have a meaningful backfill equivalent yet.

## Broader data coverage: NGX and CBN

The original plan called for CBN and NGX as market/finance sources. Both
needed research this session couldn't fully verify live: this environment's
network egress is locked to a small allowlist (GitHub, PyPI, npm, Anthropic),
so even `WebFetch` couldn't reach `ngxgroup.com`, `ngxpulse.ng`, or
`cbn.gov.ng` to inspect them directly — everything below came from web
search results, not a live fetch.

- **NGX** (`ngx_loader.py`): NGX Pulse (`ngxpulse.ng/api`) documents a public,
  keyless endpoint for index history — `/api/ngxdata/indices/{code}/history`,
  returning `{success, code, name, history: [{date, value}]}`. This is
  reasonably solid ground: it's documented, and the shape is simple. Starts
  with just the All-Share Index (`ngx_asi`); add more codes to
  `INDEX_CODES` in `ngx_loader.py` as needed (e.g. sector indices).
- **CBN** (`cbn_loader.py`): CBN has no documented public API. The only
  option is scraping their official rate page
  (`cbn.gov.ng/rates/ExchRateByCurrency.html`), a legacy ASP-era page whose
  exact table structure couldn't be confirmed from this session. The loader
  is written defensively — it searches all tables on the page for one with
  currency + rate columns (by substring match, not an exact schema) rather
  than hardcoding column names, and logs a clear failure to
  `ops.ingestion_log` ("could not locate a recognizable USD rate row")
  instead of silently loading garbage if the page doesn't match what was
  expected.

### First live run results

Once actually run on GitHub Actions (real network, unlike this dev session),
World Bank, weather, air quality, and NGX all succeeded on the first try.
Two real bugs turned up, both fixed same-day:

- **CBN**: failed with `FileNotFoundError: No such file or directory:
  <!DOCTYPE html>...`. Not a page-structure problem at all — a pandas API
  contract issue: recent pandas versions require a literal HTML string passed
  to `pd.read_html()` to be wrapped in `io.StringIO()`, otherwise it's
  treated as a file path. Fixed by wrapping `r.text` in `io.StringIO()`
  before parsing. The table-detection heuristics themselves were never
  actually exercised against the real page yet — that's still open.
- **NGX**: failed with `401 Unauthorized` on the index-history endpoint,
  contradicting what the web-search summary said about it being public/keyless.
  Confirms the limit of researching an API via search snippets instead of
  its real docs page — this session couldn't fetch `ngxpulse.ng/api` directly
  to check. Needs a human to check the actual docs page for the real auth
  requirement (API key header? signup flow?) before this loader can work.
- **Data-quality checks**: `etl/quality.py` checks every loaded value against
  a plausible `(low, high)` range per indicator (`config.QUALITY_BOUNDS`) and
  writes violations into `core.alerts` — the table that existed in the schema
  from the start but nothing wrote to. This catches unit errors and API
  glitches (e.g. a stray 999°C reading), not genuine extremes; it's
  intentionally generous rather than trying to be a real anomaly detector.
  A day-over-day jump detector (compare against the last known value per
  indicator/region) would be the natural next layer here.
