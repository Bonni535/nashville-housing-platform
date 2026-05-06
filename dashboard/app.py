"""
Nashville Housing Platform — Streamlit in Snowflake Dashboard
TICKET-033: App scaffold

Setup:
  - Database : HOUSING_PIPELINE
  - Schema   : PUBLIC
  - Warehouse: HOUSING_PIPELINE_WH
  - Runtime  : Run on warehouse
  - Packages : pydeck (add via Packages panel — not pre-installed)

Run from within Streamlit in Snowflake. All Snowflake access uses
get_active_session() — no explicit credentials needed.
"""

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# ---------------------------------------------------------------------------
# Page config — must be the first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Nashville Housing Platform",
    page_icon="🏠",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Snowflake session
# ---------------------------------------------------------------------------
session = get_active_session()

# ---------------------------------------------------------------------------
# Cached data loaders
# All queries wrapped in @st.cache_data(ttl=3600) — prevents re-query on
# every widget interaction. fct_monthly_zip is 10,650 rows; fetch once.
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def load_opportunity_scores() -> pd.DataFrame:
    """
    One row per zip (76 total). Used by Map, Affordability, Income,
    Crime, and Transactions sections, and the weight-slider recomputation.
    """
    df = session.sql("""
        SELECT
            zip_code,
            nashville_region,
            county_name,
            county_fips,
            market_as_of,
            crime_as_of,
            permit_year,
            median_sale_price,
            median_dom,
            median_homes_sold,
            median_household_income,
            poverty_rate,
            incidents_per_1k,
            permit_count,
            total_construction_cost,
            affordability_score,
            market_speed_score,
            activity_score,
            income_score,
            poverty_score,
            safety_score,
            permit_score,
            opportunity_score,
            data_confidence
        FROM HOUSING_PIPELINE.MARTS.FCT_OPPORTUNITY_SCORE
        ORDER BY opportunity_score DESC
    """).to_pandas()
    df.columns = df.columns.str.lower()
    return df


@st.cache_data(ttl=3600)
def load_monthly_zip() -> pd.DataFrame:
    """
    10,650 rows (71 zips × 169 months, 2012–2026).
    Used by Affordability trend, Inventory, Momentum, and Transactions.
    avg_mortgage_rate lives here — not in fct_opportunity_score.
    """
    df = session.sql("""
        SELECT
            zip_code,
            period_month,
            median_sale_price,
            median_dom,
            median_inventory,
            median_avg_sale_to_list,
            median_months_of_supply,
            median_homes_sold,
            median_new_listings,
            zhvi,
            zori,
            avg_mortgage_rate
        FROM HOUSING_PIPELINE.MARTS.FCT_MONTHLY_ZIP
        ORDER BY zip_code, period_month
    """).to_pandas()
    df.columns = df.columns.str.lower()
    return df


@st.cache_data(ttl=3600)
def load_pipeline_audit() -> pd.DataFrame:
    """Pipeline run history. Used by Pipeline Health section."""
    df = session.sql("""
        SELECT
            run_id,
            run_timestamp,
            dag_id,
            status,
            dbt_tests_run,
            dbt_tests_passed,
            freshness_redfin,
            freshness_zillow,
            freshness_property,
            freshness_crime,
            freshness_census,
            notes
        FROM HOUSING_PIPELINE.RAW.PIPELINE_AUDIT
        ORDER BY run_timestamp DESC
        LIMIT 30
    """).to_pandas()
    df.columns = df.columns.str.lower()
    return df


# ---------------------------------------------------------------------------
# Sidebar — navigation + data load
# ---------------------------------------------------------------------------
st.sidebar.title("🏠 Nashville Housing")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigate",
    [
        "🗺️ Map",
        "💰 Affordability",
        "📦 Inventory",
        "🚨 Crime",
        "📈 Momentum",
        "🏠 Transactions",
        "⚙️ Pipeline Health",
    ],
)

st.sidebar.markdown("---")
st.sidebar.caption(
    "Data sources: Zillow · Redfin · Census ACS5 · MNPD · FRED · Metro Codes"
)

# Load shared datasets — cached, so subsequent page switches are instant
scores_df = load_opportunity_scores()
monthly_df = load_monthly_zip()


# ---------------------------------------------------------------------------
# Helper: weighted score recomputation (used by Map section sliders)
# Called client-side in Python — no Snowflake round-trip on slider change.
# ---------------------------------------------------------------------------
def recompute_opportunity_score(row: pd.Series, weights: dict) -> float:
    """
    Recompute a single zip's opportunity score from individual sub-scores
    and caller-supplied weights. Mirrors the equal-weight formula in
    fct_opportunity_score but allows dynamic reweighting.

    Args:
        row    : one row of scores_df
        weights: dict mapping signal name → float weight

    Returns:
        Weighted average score clamped to [0, 100].
    """
    signal_cols = {
        "affordability": "affordability_score",
        "market_speed":  "market_speed_score",
        "activity":      "activity_score",
        "income":        "income_score",
        "poverty":       "poverty_score",
        "safety":        "safety_score",
        "permits":       "permit_score",
    }
    total_weight = sum(weights.values())
    if total_weight == 0:
        return 50.0
    weighted_sum = sum(
        row[col] * weights[signal]
        for signal, col in signal_cols.items()
    )
    return round(weighted_sum / total_weight, 2)


# ---------------------------------------------------------------------------
# Confidence badge helper
# ---------------------------------------------------------------------------
CONFIDENCE_COLOR = {
    "High":    "🟢",
    "Partial": "🟡",
    "Low":     "🔴",
}


def confidence_badge(level: str) -> str:
    return f"{CONFIDENCE_COLOR.get(level, '⚪')} {level}"


# ---------------------------------------------------------------------------
# Section: Map
# Built in TICKET-034 — pydeck choropleth + weight sliders
# ---------------------------------------------------------------------------
if page == "🗺️ Map":
    st.title("🗺️ Opportunity Score Map")
    st.info(
        "Interactive choropleth map coming in **TICKET-034**. "
        "It will show zip-level opportunity scores with 7 weight sliders "
        "in the sidebar so you can reweight the signals in real time.",
        icon="🚧",
    )

    # Scorecard table as a useful interim deliverable while map is built
    st.subheader("Opportunity Scores — All 76 Zips")
    display_cols = [
        "zip_code", "nashville_region", "county_name",
        "opportunity_score", "data_confidence",
        "median_sale_price", "median_household_income",
        "incidents_per_1k", "permit_count",
    ]
    display_df = scores_df[display_cols].copy()
    display_df["data_confidence"] = display_df["data_confidence"].map(confidence_badge)
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
    )


# ---------------------------------------------------------------------------
# Section: Affordability
# ---------------------------------------------------------------------------
elif page == "💰 Affordability":
    st.title("💰 Affordability")

    col1, col2, col3 = st.columns(3)
    col1.metric(
        "Median Sale Price (MSA)",
        f"${scores_df['median_sale_price'].median():,.0f}",
    )
    col2.metric(
        "Median Household Income (MSA)",
        f"${scores_df['median_household_income'].median():,.0f}",
    )
    col3.metric(
        "Avg Mortgage Rate (latest)",
        f"{monthly_df['avg_mortgage_rate'].dropna().iloc[-1]:.2f}%"
        if not monthly_df["avg_mortgage_rate"].dropna().empty else "N/A",
    )

    st.markdown("---")
    st.info("📊 Affordability trend charts and zip selector coming in a future ticket.", icon="🚧")


# ---------------------------------------------------------------------------
# Section: Inventory
# ---------------------------------------------------------------------------
elif page == "📦 Inventory":
    st.title("📦 Inventory")

    col1, col2 = st.columns(2)
    latest = monthly_df.sort_values("period_month").groupby("zip_code").last().reset_index()
    col1.metric(
        "MSA Median Inventory (latest month)",
        f"{latest['median_inventory'].median():,.0f}",
    )
    col2.metric(
        "MSA Median Months of Supply",
        f"{latest['median_months_of_supply'].median():.1f}",
    )

    st.markdown("---")
    st.info("📊 Inventory trend charts and zip selector coming in a future ticket.", icon="🚧")


# ---------------------------------------------------------------------------
# Section: Crime
# ---------------------------------------------------------------------------
elif page == "🚨 Crime":
    st.title("🚨 Crime & Safety")

    col1, col2 = st.columns(2)
    col1.metric(
        "MSA Median Crime Rate (per 1k residents)",
        f"{scores_df['incidents_per_1k'].median():.1f}",
    )
    col2.metric(
        "Zips with Full Crime Data",
        f"{(scores_df['incidents_per_1k'].notna()).sum()} / {len(scores_df)}",
    )

    st.caption(
        "⚠️ MNPD jurisdiction covers Davidson County only. "
        "23 suburban zips (Williamson, Rutherford, Wilson, Sumner) "
        "are imputed with the MSA average in the opportunity score."
    )

    st.markdown("---")
    st.info("📊 Crime breakdown by zip and region coming in a future ticket.", icon="🚧")


# ---------------------------------------------------------------------------
# Section: Momentum
# ---------------------------------------------------------------------------
elif page == "📈 Momentum":
    st.title("📈 Market Momentum")

    col1, col2 = st.columns(2)
    latest = monthly_df.sort_values("period_month").groupby("zip_code").last().reset_index()
    col1.metric(
        "MSA Median Days on Market",
        f"{scores_df['median_dom'].median():.0f} days",
    )
    col2.metric(
        "MSA Avg Sale-to-List Ratio",
        f"{latest['median_avg_sale_to_list'].median():.1%}"
        if not latest["median_avg_sale_to_list"].dropna().empty else "N/A",
    )

    st.markdown("---")
    st.info("📊 DOM and sale-to-list trend charts coming in a future ticket.", icon="🚧")


# ---------------------------------------------------------------------------
# Section: Transactions
# ---------------------------------------------------------------------------
elif page == "🏠 Transactions":
    st.title("🏠 Transactions & Permits")

    col1, col2, col3 = st.columns(3)
    col1.metric(
        "MSA Median Homes Sold / Month",
        f"{scores_df['median_homes_sold'].median():.0f}",
    )
    col2.metric(
        "MSA Median Permit Count (current window)",
        f"{scores_df['permit_count'].median():.0f}",
    )
    col3.metric(
        "Zips with Permit Data",
        f"{(scores_df['permit_count'].notna() & (scores_df['permit_count'] > 0)).sum()} / {len(scores_df)}",
    )

    st.caption(
        "⚠️ Metro Codes covers Davidson County only. "
        "Suburban zips are imputed with the MSA average permit count."
    )

    st.markdown("---")
    st.info("📊 Transactions and permit breakdown by zip coming in a future ticket.", icon="🚧")


# ---------------------------------------------------------------------------
# Section: Pipeline Health
# ---------------------------------------------------------------------------
elif page == "⚙️ Pipeline Health":
    st.title("⚙️ Pipeline Health")

    audit_df = load_pipeline_audit()

    if audit_df.empty:
        st.warning("No audit records found in RAW.PIPELINE_AUDIT.")
    else:
        # Summary metrics from most recent run
        latest_run = audit_df.iloc[0]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Last Run", str(latest_run["run_timestamp"])[:16])
        col2.metric("DAG", latest_run["dag_id"])
        col3.metric("Status", latest_run["status"].upper())
        tests_run = latest_run["dbt_tests_run"]
        tests_passed = latest_run["dbt_tests_passed"]
        col4.metric(
            "dbt Tests",
            f"{int(tests_passed)}/{int(tests_run)}" if pd.notna(tests_run) else "N/A",
        )

        st.markdown("---")
        st.subheader("Run History (last 30 runs)")
        st.dataframe(audit_df, use_container_width=True, hide_index=True)

        # Freshness status from most recent daily run
        st.markdown("---")
        st.subheader("Source Freshness (last daily run)")
        freshness_cols = {
            "freshness_redfin":   "Redfin",
            "freshness_zillow":   "Zillow",
            "freshness_crime":    "Crime",
            "freshness_census":   "Census",
            "freshness_property": "Property",
        }
        cols = st.columns(len(freshness_cols))
        for col, (field, label) in zip(cols, freshness_cols.items()):
            val = latest_run.get(field, "unknown")
            icon = "✅" if val == "pass" else ("⚠️" if val == "warn" else "❓")
            col.metric(label, f"{icon} {val}" if pd.notna(val) else "N/A")