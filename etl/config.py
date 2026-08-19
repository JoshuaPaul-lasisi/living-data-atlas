"""Shared reference data for loaders: cities and indicators to pull each run."""

# lat/lon are city centroids, used for both weather and air-quality lookups.
CITIES = [
    {"region": "NG-LAG", "state": "Lagos", "city": "Lagos", "lat": 6.5244, "lon": 3.3792},
    {"region": "NG-FCT", "state": "FCT", "city": "Abuja", "lat": 9.0579, "lon": 7.4951},
    {"region": "NG-KAN", "state": "Kano", "city": "Kano", "lat": 12.0022, "lon": 8.5920},
    {"region": "NG-RIV", "state": "Rivers", "city": "Port Harcourt", "lat": 4.8156, "lon": 7.0498},
    {"region": "NG-OYO", "state": "Oyo", "city": "Ibadan", "lat": 7.3775, "lon": 3.9470},
    {"region": "NG-ENU", "state": "Enugu", "city": "Enugu", "lat": 6.5244, "lon": 7.5086},
]

# World Bank indicator code -> short name we store in core.observations.
WORLDBANK_INDICATORS = {
    "NY.GDP.MKTP.CD": "gdp_usd",
    "FP.CPI.TOTL.ZG": "inflation_cpi_pct",
    "SL.UEM.TOTL.ZS": "unemployment_pct",
    "SP.POP.TOTL": "population_total",
    "SI.POV.NAHC": "poverty_headcount_pct",
    "PA.NUS.FCRF": "fx_rate_usd_ngn",
}

OPENAQ_RADIUS_M = 25000
