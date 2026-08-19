import os

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

st.set_page_config(page_title="Living Data Atlas", layout="wide")

# Validated categorical order (dataviz skill) -- fixed order, never cycled per-filter.
CATEGORICAL = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
STATUS = {"success": "#0ca30c", "partial": "#fab219", "fail": "#d03b3b"}

ECON_INDICATORS = {
    "gdp_usd": "GDP (current US$)",
    "inflation_cpi_pct": "Inflation, CPI (%)",
    "unemployment_pct": "Unemployment (%)",
    "population_total": "Population",
    "poverty_headcount_pct": "Poverty headcount (%)",
    "fx_rate_usd_ngn": "FX rate (NGN per USD)",
}
WEATHER_INDICATORS = ["temp_max_c", "temp_min_c"]


@st.cache_resource
def get_engine():
    db_url = st.secrets.get("DATABASE_URL", os.getenv("DATABASE_URL"))
    if not db_url:
        return None
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return create_engine(db_url, pool_pre_ping=True)


@st.cache_data(ttl=600)
def query_observations(indicators: tuple) -> pd.DataFrame:
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()
    with engine.connect() as conn:
        return pd.read_sql(
            text("""
                SELECT date, indicator, region, value, source
                FROM core.observations
                WHERE indicator = ANY(:indicators)
                ORDER BY date
            """),
            conn,
            params={"indicators": list(indicators)},
        )


@st.cache_data(ttl=300)
def query_ingestion_log() -> pd.DataFrame:
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()
    with engine.connect() as conn:
        return pd.read_sql(
            text("""
                SELECT DISTINCT ON (source) source, run_ts, status, records, message
                FROM ops.ingestion_log
                ORDER BY source, run_ts DESC
            """),
            conn,
        )


def table_view(df: pd.DataFrame, key: str):
    with st.expander("View as table"):
        st.dataframe(df, use_container_width=True, hide_index=True)


st.title("Living Data Atlas")
st.caption("A continuously updated view of Nigeria's economy, weather, and air quality.")

if get_engine() is None:
    st.warning(
        "No DATABASE_URL configured. Set it in `.streamlit/secrets.toml` locally, "
        "or in the app's Secrets on Streamlit Community Cloud, pointing at your Supabase project."
    )
    st.stop()

tab_overview, tab_econ, tab_weather, tab_air, tab_health = st.tabs(
    ["Overview", "Economy", "Weather", "Air Quality", "Pipeline Health"]
)

with tab_overview:
    df_econ_all = query_observations(tuple(ECON_INDICATORS.keys()))
    if df_econ_all.empty:
        st.info("No data yet — run the ETL loaders (see README) to populate the Atlas.")
    else:
        latest = df_econ_all.sort_values("date").groupby(["indicator", "region"]).tail(1)
        ng_latest = latest[latest["region"] == "NG"]
        cols = st.columns(len(ECON_INDICATORS))
        for col, (code, label) in zip(cols, ECON_INDICATORS.items()):
            row = ng_latest[ng_latest["indicator"] == code]
            if row.empty:
                col.metric(label, "—")
            else:
                col.metric(label, f"{row.iloc[0]['value']:,.2f}", help=f"as of {row.iloc[0]['date']}")

with tab_econ:
    choice = st.selectbox("Indicator", options=list(ECON_INDICATORS.keys()), format_func=lambda c: ECON_INDICATORS[c])
    df = query_observations((choice,))
    if df.empty:
        st.info("No data for this indicator yet.")
    else:
        fig = px.line(
            df, x="date", y="value", color="region",
            color_discrete_sequence=CATEGORICAL,
            title=ECON_INDICATORS[choice], markers=True,
        )
        fig.update_layout(yaxis_title=ECON_INDICATORS[choice], xaxis_title=None, hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
        table_view(df, "econ")

with tab_weather:
    df_w = query_observations(tuple(WEATHER_INDICATORS))
    if df_w.empty:
        st.info("No weather data yet.")
    else:
        regions = sorted(df_w["region"].unique())
        picked = st.multiselect("Cities", regions, default=regions[:3])
        df_w = df_w[df_w["region"].isin(picked)] if picked else df_w
        df_w["series"] = df_w["region"] + " · " + df_w["indicator"]
        fig = px.line(
            df_w, x="date", y="value", color="series",
            color_discrete_sequence=CATEGORICAL,
            title="Temperature (°C)", markers=True,
        )
        fig.update_layout(yaxis_title="°C", xaxis_title=None, hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

        df_p = query_observations(("precip_mm",))
        df_p = df_p[df_p["region"].isin(picked)] if picked else df_p
        if not df_p.empty:
            fig2 = px.bar(
                df_p, x="date", y="value", color="region",
                color_discrete_sequence=CATEGORICAL,
                title="Precipitation (mm)",
            )
            fig2.update_layout(yaxis_title="mm", xaxis_title=None)
            st.plotly_chart(fig2, use_container_width=True)
        table_view(df_w, "weather")

with tab_air:
    df_a = query_observations(("pm25", "pm10"))
    if df_a.empty:
        st.info("No air quality data yet — OpenAQ coverage in Nigeria is sparse in places.")
    else:
        latest_a = df_a.sort_values("date").groupby(["indicator", "region"]).tail(1)
        fig = px.bar(
            latest_a, x="region", y="value", color="indicator",
            barmode="group", color_discrete_sequence=CATEGORICAL,
            title="Latest PM2.5 / PM10 by city (µg/m³)",
        )
        fig.update_layout(yaxis_title="µg/m³", xaxis_title=None)
        st.plotly_chart(fig, use_container_width=True)
        table_view(df_a, "air")

with tab_health:
    log = query_ingestion_log()
    if log.empty:
        st.info("No ingestion runs logged yet.")
    else:
        for _, row in log.iterrows():
            color = STATUS.get(row["status"], "#898781")
            st.markdown(
                f"**{row['source']}** &nbsp; "
                f"<span style='color:{color}'>●</span> {row['status']} "
                f"&nbsp;·&nbsp; {row['records']} records &nbsp;·&nbsp; {row['run_ts']}",
                unsafe_allow_html=True,
            )
            if row["message"]:
                st.caption(row["message"])
