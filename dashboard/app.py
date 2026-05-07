"""
Nashville Housing Platform — Streamlit in Snowflake Dashboard
TICKET-033: App scaffold
TICKET-034: Choropleth map + weight sliders
TICKET-035: Dashboard sections — Affordability, Inventory, Crime, Momentum, Transactions

Setup:
  - Database : HOUSING_PIPELINE
  - Schema   : PUBLIC
  - Warehouse: HOUSING_PIPELINE_WH
  - Runtime  : Run on warehouse
  - Packages : pydeck (add via Packages panel — not pre-installed)

Run from within Streamlit in Snowflake. All Snowflake access uses
get_active_session() — no explicit credentials needed.
"""

import json
import os
import tempfile
import altair as alt
import pydeck as pdk
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
            computed_months_of_supply,
            median_homes_sold,
            median_new_listings,
            zhvi,
            zori,
            avg_mortgage_rate,
            computed_months_of_supply
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



@st.cache_data(ttl=3600)
def load_crime_index() -> pd.DataFrame:
    """Annual crime rate by zip from INT_CRIME_INDEX. Used by Crime section."""
    df = session.sql("""
        SELECT zip_code, incident_year, incident_count, incidents_per_1k
        FROM HOUSING_PIPELINE.INTERMEDIATE.INT_CRIME_INDEX
        ORDER BY zip_code, incident_year
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
monthly_df["period_month"] = pd.to_datetime(monthly_df["period_month"])


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
# GeoJSON loader — ZCTA boundaries from Snowflake internal stage
# Boundaries are static so ttl=None (cache for lifetime of the app session).
# File uploaded once via scripts/fetch_nashville_geojson.py.
# ---------------------------------------------------------------------------
@st.cache_data(ttl=None)
def load_zip_geojson() -> dict:
    """
    Loads Nashville MSA ZCTA boundary GeoJSON from the Snowflake internal
    stage. No external network call at runtime — file was uploaded once by
    scripts/fetch_nashville_geojson.py.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        session.file.get(
            "@HOUSING_PIPELINE.PUBLIC.DASHBOARD_ASSETS/nashville_zips.geojson",
            tmpdir,
        )
        filepath = os.path.join(tmpdir, "nashville_zips.geojson")
        with open(filepath) as f:
            return json.load(f)


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
# Color scale: score 0–100 → [R, G, B, A]
# Low (0)  = red  [214, 39,  40]
# High (100) = teal [0,  168, 132]
# ---------------------------------------------------------------------------
def score_to_color(score: float, alpha: int = 190) -> list:
    t = max(0.0, min(100.0, float(score) if score == score else 50.0)) / 100.0
    return [
        int(214 * (1 - t)),
        int(39  * (1 - t) + 168 * t),
        int(40  * (1 - t) + 132 * t),
        alpha,
    ]


# ---------------------------------------------------------------------------
# Section: Map
# Built in TICKET-034 — pydeck choropleth + weight sliders
# ---------------------------------------------------------------------------
if page == "🗺️ Map":
    st.title("🗺️ Opportunity Score Map")

    # --- Weight sliders (only shown on Map page) ---
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ⚖️ Signal Weights")
    weights = {
        "affordability": st.sidebar.slider("💵 Affordability",      0, 10, 5),
        "market_speed":  st.sidebar.slider("⚡ Market Speed (DOM)", 0, 10, 5),
        "activity":      st.sidebar.slider("📊 Activity",            0, 10, 5),
        "income":        st.sidebar.slider("💰 Income",              0, 10, 5),
        "poverty":       st.sidebar.slider("📉 Low Poverty",         0, 10, 5),
        "safety":        st.sidebar.slider("🛡️ Safety",              0, 10, 5),
        "permits":       st.sidebar.slider("🏗️ Permits",             0, 10, 5),
    }

    # --- Recompute scores from sliders (no Snowflake round-trip) ---
    map_df = scores_df.copy()
    map_df["display_score"] = map_df.apply(
        lambda row: recompute_opportunity_score(row, weights), axis=1
    )
    map_df = map_df.sort_values("display_score", ascending=False)

    # --- Summary metrics ---
    top_zip = map_df.iloc[0]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("🏆 Top Zip", top_zip["zip_code"])
    col2.metric("Top Score", f"{top_zip['display_score']:.1f}")
    col3.metric("MSA Average", f"{map_df['display_score'].mean():.1f}")
    col4.metric(
        "Score Range",
        f"{map_df['display_score'].min():.1f} – {map_df['display_score'].max():.1f}",
    )

    # --- Choropleth map ---
    try:
        geojson = load_zip_geojson()

        # Build lookup: zip → score + metadata
        score_lookup = map_df.set_index("zip_code")[
            ["display_score", "nashville_region", "data_confidence",
             "median_sale_price", "median_household_income"]
        ].to_dict("index")

        # Merge scores into GeoJSON feature properties
        for feature in geojson["features"]:
            zcta = feature["properties"].get("ZCTA5", "")
            info = score_lookup.get(zcta, {})
            score = info.get("display_score", 50.0)
            feature["properties"]["display_score"]           = round(score, 1)
            feature["properties"]["nashville_region"]        = info.get("nashville_region", "—")
            feature["properties"]["data_confidence"]         = info.get("data_confidence", "—")
            feature["properties"]["median_sale_price"]       = (
                f"${info['median_sale_price']:,.0f}"
                if info.get("median_sale_price") else "N/A"
            )
            feature["properties"]["median_household_income"] = (
                f"${info['median_household_income']:,.0f}"
                if info.get("median_household_income") else "N/A"
            )
            feature["properties"]["fill_color"] = score_to_color(score)

        layer = pdk.Layer(
            "GeoJsonLayer",
            data=geojson,
            pickable=True,
            stroked=True,
            filled=True,
            get_fill_color="properties.fill_color",
            get_line_color=[255, 255, 255, 60],
            line_width_min_pixels=1,
        )

        view_state = pdk.ViewState(
            latitude=36.17,
            longitude=-86.78,
            zoom=9,
            pitch=0,
        )

        tooltip = {
            "html": (
                "<b>{ZCTA5}</b> · {nashville_region}<br/>"
                "Score: <b>{display_score}</b> · {data_confidence}<br/>"
                "Median Sale Price: {median_sale_price}<br/>"
                "Median Income: {median_household_income}"
            ),
            "style": {
                "backgroundColor": "#0d1b2a",
                "color": "#e0f2f1",
                "fontSize": "13px",
                "padding": "8px",
                "borderRadius": "4px",
            },
        }

        st.pydeck_chart(
            pdk.Deck(
                layers=[layer],
                initial_view_state=view_state,
                tooltip=tooltip,
                map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
            ),
            use_container_width=True,
        )

        # Color scale legend
        st.caption("🔴 Lower opportunity → 🟢 Higher opportunity (teal)")

    except Exception as e:
        st.error(f"Could not load map boundaries: {e}")
        st.info(
            "TIGERweb may be unreachable from this Snowflake environment. "
            "The scorecard table below shows the same data.",
            icon="ℹ️",
        )

    # Scorecard table — always shown beneath the map
    st.markdown("---")
    st.subheader("Opportunity Scores — All 76 Zips")
    display_cols = [
        "zip_code", "nashville_region", "county_name",
        "display_score", "data_confidence",
        "median_sale_price", "median_household_income",
        "incidents_per_1k", "permit_count",
    ]
    display_df = map_df[display_cols].copy()
    display_df["data_confidence"] = display_df["data_confidence"].map(confidence_badge)
    st.dataframe(display_df, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Section: Affordability
# ---------------------------------------------------------------------------
elif page == "💰 Affordability":
    st.title("💰 Affordability")

    # MSA-level monthly aggregate
    msa_monthly = (
        monthly_df.groupby("period_month")
        .agg(
            zhvi=("zhvi", "median"),
            median_sale_price=("median_sale_price", "median"),
            avg_mortgage_rate=("avg_mortgage_rate", "first"),
        )
        .reset_index()
        .sort_values("period_month")
    )

    latest_zhvi  = msa_monthly["zhvi"].dropna().iloc[-1]
    latest_rate  = monthly_df.loc[monthly_df["avg_mortgage_rate"].notna(), "avg_mortgage_rate"].iloc[-1]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("MSA Median ZHVI",          f"${latest_zhvi:,.0f}")
    col2.metric("MSA Median Sale Price",    f"${scores_df['median_sale_price'].median():,.0f}")
    col3.metric("30-Yr Mortgage Rate",      f"{latest_rate:.2f}%")
    col4.metric("Median Household Income",  f"${scores_df['median_household_income'].median():,.0f}")

    st.markdown("---")

    # ZHVI trend — full width
    st.subheader("Nashville MSA Home Value Index (ZHVI)")
    zhvi_data = msa_monthly.dropna(subset=["zhvi"])
    zhvi_chart = (
        alt.Chart(zhvi_data)
        .mark_line(color="#00a884", strokeWidth=2)
        .encode(
            x=alt.X("period_month:T", title="Month"),
            y=alt.Y("zhvi:Q", title="Median ZHVI ($)", scale=alt.Scale(zero=False)),
            tooltip=[
                alt.Tooltip("period_month:T", title="Month", format="%b %Y"),
                alt.Tooltip("zhvi:Q",         title="ZHVI",  format="$,.0f"),
            ],
        )
        .properties(height=260)
    )
    st.altair_chart(zhvi_chart, use_container_width=True)

    # Sale price by region | Mortgage rate — two columns
    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Median Sale Price by Region")
        region_prices = (
            scores_df.dropna(subset=["median_sale_price"])
            .groupby("nashville_region")["median_sale_price"]
            .median()
            .reset_index()
            .sort_values("median_sale_price", ascending=False)
        )
        price_bar = (
            alt.Chart(region_prices)
            .mark_bar(color="#00a884")
            .encode(
                x=alt.X("median_sale_price:Q", title="Median Sale Price ($)"),
                y=alt.Y("nashville_region:N", sort="-x", title=None),
                tooltip=[
                    alt.Tooltip("nashville_region:N",  title="Region"),
                    alt.Tooltip("median_sale_price:Q", title="Median Sale Price", format="$,.0f"),
                ],
            )
            .properties(height=260)
        )
        st.altair_chart(price_bar, use_container_width=True)

    with col_r:
        st.subheader("30-Year Fixed Mortgage Rate")
        rate_data = msa_monthly.dropna(subset=["avg_mortgage_rate"])
        rate_chart = (
            alt.Chart(rate_data)
            .mark_line(color="#e67e22", strokeWidth=2)
            .encode(
                x=alt.X("period_month:T", title="Month"),
                y=alt.Y("avg_mortgage_rate:Q", title="Rate (%)", scale=alt.Scale(zero=False)),
                tooltip=[
                    alt.Tooltip("period_month:T",      title="Month", format="%b %Y"),
                    alt.Tooltip("avg_mortgage_rate:Q", title="Rate",  format=".2f"),
                ],
            )
            .properties(height=260)
        )
        st.altair_chart(rate_chart, use_container_width=True)

    # Zip drill-down
    st.markdown("---")
    st.subheader("Zip-Level ZHVI vs MSA Average")
    zip_options = sorted(monthly_df["zip_code"].dropna().unique().tolist())
    default_idx = zip_options.index("37203") if "37203" in zip_options else 0
    selected_zip = st.selectbox("Select zip code", zip_options, index=default_idx)

    msa_zhvi_df = msa_monthly[["period_month", "zhvi"]].rename(columns={"zhvi": "msa_zhvi"})
    zip_df = (
        monthly_df[monthly_df["zip_code"] == selected_zip][["period_month", "zhvi"]]
        .merge(msa_zhvi_df, on="period_month", how="inner")
        .dropna(subset=["zhvi", "msa_zhvi"])
    )
    if not zip_df.empty:
        base = alt.Chart(zip_df)
        zip_line = base.mark_line(color="#00a884", strokeWidth=2).encode(
            x=alt.X("period_month:T", title="Month"),
            y=alt.Y("zhvi:Q", scale=alt.Scale(zero=False), title="ZHVI ($)"),
            tooltip=[
                alt.Tooltip("period_month:T", title="Month", format="%b %Y"),
                alt.Tooltip("zhvi:Q", title=f"{selected_zip} ZHVI", format="$,.0f"),
            ],
        )
        msa_line = base.mark_line(color="#95a5a6", strokeWidth=1.5, strokeDash=[4, 2]).encode(
            x="period_month:T",
            y=alt.Y("msa_zhvi:Q", scale=alt.Scale(zero=False)),
            tooltip=[alt.Tooltip("msa_zhvi:Q", title="MSA Median", format="$,.0f")],
        )
        st.altair_chart((zip_line + msa_line).properties(height=240), use_container_width=True)
        st.caption(f"Solid teal = {selected_zip}  ·  Dashed gray = MSA median")
    else:
        st.info(f"No ZHVI data available for {selected_zip}.")


# ---------------------------------------------------------------------------
# Section: Inventory
# ---------------------------------------------------------------------------
elif page == "📦 Inventory":
    st.title("📦 Inventory")

    latest_month = monthly_df.sort_values("period_month").groupby("zip_code").last().reset_index()

    col1, col2, col3 = st.columns(3)
    col1.metric("MSA Median Inventory",       f"{latest_month['median_inventory'].median():,.0f} homes")
    col2.metric("Months of Supply",           f"{latest_month['computed_months_of_supply'].dropna().median():.1f} mo" if latest_month['computed_months_of_supply'].notna().any() else "N/A")
    col3.metric("MSA Median New Listings",    f"{latest_month['median_new_listings'].median():,.0f} / mo")

    st.markdown("---")

    # MSA aggregates over time
    inv_monthly = (
        monthly_df.groupby("period_month")
        .agg(
            median_inventory=("median_inventory",       "median"),
            months_of_supply=("computed_months_of_supply","median"),
            new_listings=    ("median_new_listings",    "median"),
        )
        .reset_index()
        .sort_values("period_month")
    )

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Active Inventory — MSA Median")
        inv_chart = (
            alt.Chart(inv_monthly.dropna(subset=["median_inventory"]))
            .mark_area(color="#00a884", opacity=0.35, line={"color": "#00a884", "strokeWidth": 2})
            .encode(
                x=alt.X("period_month:T", title="Month"),
                y=alt.Y("median_inventory:Q", title="Median Active Listings", scale=alt.Scale(zero=True)),
                tooltip=[
                    alt.Tooltip("period_month:T",     title="Month",     format="%b %Y"),
                    alt.Tooltip("median_inventory:Q", title="Inventory", format=",.0f"),
                ],
            )
            .properties(height=250)
        )
        st.altair_chart(inv_chart, use_container_width=True)

    with col_r:
        st.subheader("Months of Supply — MSA Median")
        mos_data = inv_monthly.dropna(subset=["months_of_supply"])
        if not mos_data.empty:
            mos_chart = (
                alt.Chart(mos_data)
                .mark_line(color="#1a3a5c", strokeWidth=2)
                .encode(
                    x=alt.X("period_month:T", title="Month"),
                    y=alt.Y("months_of_supply:Q", title="Months of Supply", scale=alt.Scale(zero=False)),
                    tooltip=[
                        alt.Tooltip("period_month:T",     title="Month",      format="%b %Y"),
                        alt.Tooltip("months_of_supply:Q", title="Mos Supply", format=".1f"),
                    ],
                )
                .properties(height=250)
            )
            rule = alt.Chart(pd.DataFrame({"y": [6]})).mark_rule(
                color="#e74c3c", strokeDash=[4, 2], strokeWidth=1.5
            ).encode(y="y:Q")
            st.altair_chart(mos_chart + rule, use_container_width=True)
            st.caption("Red dashed line = 6 months (balanced market threshold)")
        else:
            st.info("Months of supply not reported by Redfin for Nashville MSA zip codes.")
            st.caption("This is a Redfin data coverage limitation, not a pipeline issue.")

    # New listings trend — full width
    st.markdown("---")
    st.subheader("New Listings — MSA Median")
    nl_chart = (
        alt.Chart(inv_monthly.dropna(subset=["new_listings"]))
        .mark_bar(color="#00a884", opacity=0.7)
        .encode(
            x=alt.X("period_month:T", title="Month"),
            y=alt.Y("new_listings:Q", title="Median New Listings"),
            tooltip=[
                alt.Tooltip("period_month:T",  title="Month",        format="%b %Y"),
                alt.Tooltip("new_listings:Q",  title="New Listings", format=",.0f"),
            ],
        )
        .properties(height=220)
    )
    st.altair_chart(nl_chart, use_container_width=True)

    # Zip drill-down
    st.markdown("---")
    st.subheader("Zip-Level Inventory & Months of Supply")
    zip_options_i = sorted(monthly_df["zip_code"].dropna().unique().tolist())
    default_i = zip_options_i.index("37203") if "37203" in zip_options_i else 0
    selected_zip_i = st.selectbox("Select zip code", zip_options_i, index=default_i, key="inv_zip")

    zip_inv = (
        monthly_df[monthly_df["zip_code"] == selected_zip_i]
        [["period_month", "median_inventory", "computed_months_of_supply"]]
        .sort_values("period_month")
    )
    msa_inv = inv_monthly[["period_month", "median_inventory", "months_of_supply"]].rename(
        columns={"median_inventory": "msa_inventory", "months_of_supply": "msa_mos"}
    )
    zip_inv_merged = zip_inv.merge(msa_inv, on="period_month", how="inner")

    col_zi, col_zm = st.columns(2)

    with col_zi:
        inv_compare = zip_inv_merged.dropna(subset=["median_inventory", "msa_inventory"])
        if not inv_compare.empty:
            base_i = alt.Chart(inv_compare)
            zip_inv_line = base_i.mark_line(color="#00a884", strokeWidth=2).encode(
                x=alt.X("period_month:T", title="Month"),
                y=alt.Y("median_inventory:Q", title="Active Listings", scale=alt.Scale(zero=False)),
                tooltip=[
                    alt.Tooltip("period_month:T",      title="Month",    format="%b %Y"),
                    alt.Tooltip("median_inventory:Q",  title=f"{selected_zip_i}", format=",.0f"),
                ],
            )
            msa_inv_line = base_i.mark_line(color="#95a5a6", strokeWidth=1.5, strokeDash=[4, 2]).encode(
                x="period_month:T",
                y=alt.Y("msa_inventory:Q", scale=alt.Scale(zero=False)),
                tooltip=[alt.Tooltip("msa_inventory:Q", title="MSA Median", format=",.0f")],
            )
            st.altair_chart((zip_inv_line + msa_inv_line).properties(height=220), use_container_width=True)
            st.caption(f"Solid teal = {selected_zip_i}  ·  Dashed gray = MSA median")

    with col_zm:
        mos_compare = zip_inv_merged.dropna(subset=["computed_months_of_supply", "msa_mos"])
        if not mos_compare.empty:
            base_m = alt.Chart(mos_compare)
            zip_mos_line = base_m.mark_line(color="#1a3a5c", strokeWidth=2).encode(
                x=alt.X("period_month:T", title="Month"),
                y=alt.Y("computed_months_of_supply:Q", title="Months of Supply", scale=alt.Scale(zero=False)),
                tooltip=[
                    alt.Tooltip("period_month:T",              title="Month", format="%b %Y"),
                    alt.Tooltip("computed_months_of_supply:Q",   title=f"{selected_zip_i} MoS", format=".1f"),
                ],
            )
            msa_mos_line = base_m.mark_line(color="#95a5a6", strokeWidth=1.5, strokeDash=[4, 2]).encode(
                x="period_month:T",
                y=alt.Y("msa_mos:Q", scale=alt.Scale(zero=False)),
                tooltip=[alt.Tooltip("msa_mos:Q", title="MSA Median MoS", format=".1f")],
            )
            rule_i = alt.Chart(pd.DataFrame({"y": [6]})).mark_rule(
                color="#e74c3c", strokeDash=[4, 2], strokeWidth=1.5
            ).encode(y="y:Q")
            st.altair_chart(
                (zip_mos_line + msa_mos_line + rule_i).properties(height=220),
                use_container_width=True,
            )
            st.caption(f"Solid navy = {selected_zip_i}  ·  Dashed gray = MSA  ·  Red = 6 mo threshold")


# ---------------------------------------------------------------------------
# Section: Crime
# ---------------------------------------------------------------------------
elif page == "🚨 Crime":
    st.title("🚨 Crime & Safety")

    davidson_crime  = scores_df[scores_df["incidents_per_1k"].notna()]
    msa_median_rate = davidson_crime["incidents_per_1k"].median()
    safest_zip      = davidson_crime.loc[davidson_crime["incidents_per_1k"].idxmin(), "zip_code"]
    highest_zip     = davidson_crime.loc[davidson_crime["incidents_per_1k"].idxmax(), "zip_code"]

    col1, col2, col3 = st.columns(3)
    col1.metric("Davidson Median Crime Rate", f"{msa_median_rate:.1f} per 1k")
    col2.metric("Safest Zip",                 safest_zip)
    col3.metric("Highest Crime Zip",          highest_zip)

    st.caption(
        "⚠️ MNPD jurisdiction covers Davidson County only (53 zips). "
        "23 suburban zips are imputed with the MSA average in the opportunity score."
    )
    st.markdown("---")

    crime_df = load_crime_index()

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Crime Rate by Zip — Current Year")
        rate_by_zip = (
            davidson_crime[["zip_code", "nashville_region", "incidents_per_1k"]]
            .sort_values("incidents_per_1k", ascending=False)
            .head(30)
        )
        zip_bar = (
            alt.Chart(rate_by_zip)
            .mark_bar()
            .encode(
                x=alt.X("incidents_per_1k:Q", title="Incidents per 1k Residents"),
                y=alt.Y("zip_code:N", sort="-x", title=None),
                color=alt.Color(
                    "incidents_per_1k:Q",
                    scale=alt.Scale(scheme="reds"),
                    legend=None,
                ),
                tooltip=[
                    alt.Tooltip("zip_code:N",        title="Zip"),
                    alt.Tooltip("nashville_region:N", title="Region"),
                    alt.Tooltip("incidents_per_1k:Q", title="Rate", format=".1f"),
                ],
            )
            .properties(height=500)
        )
        st.altair_chart(zip_bar, use_container_width=True)

    with col_r:
        st.subheader("MSA Crime Rate Trend (2019–2026)")
        trend = (
            crime_df.groupby("incident_year")["incidents_per_1k"]
            .median()
            .reset_index()
            .rename(columns={"incidents_per_1k": "msa_median"})
        )
        trend_chart = (
            alt.Chart(trend)
            .mark_line(color="#e74c3c", strokeWidth=2, point=True)
            .encode(
                x=alt.X("incident_year:O", title="Year"),
                y=alt.Y("msa_median:Q", title="Median Crime Rate (per 1k)", scale=alt.Scale(zero=False)),
                tooltip=[
                    alt.Tooltip("incident_year:O", title="Year"),
                    alt.Tooltip("msa_median:Q",    title="Median Rate", format=".1f"),
                ],
            )
            .properties(height=260)
        )
        st.altair_chart(trend_chart, use_container_width=True)

        # Regional breakdown
        st.subheader("Crime Rate by Region")
        region_crime = (
            davidson_crime.dropna(subset=["incidents_per_1k"])
            .groupby("nashville_region")["incidents_per_1k"]
            .median()
            .reset_index()
            .sort_values("incidents_per_1k", ascending=False)
        )
        reg_bar = (
            alt.Chart(region_crime)
            .mark_bar(color="#e74c3c")
            .encode(
                x=alt.X("incidents_per_1k:Q", title="Median Rate (per 1k)"),
                y=alt.Y("nashville_region:N", sort="-x", title=None),
                tooltip=[
                    alt.Tooltip("nashville_region:N",  title="Region"),
                    alt.Tooltip("incidents_per_1k:Q",  title="Median Rate", format=".1f"),
                ],
            )
            .properties(height=220)
        )
        st.altair_chart(reg_bar, use_container_width=True)


# ---------------------------------------------------------------------------
# Section: Momentum
# ---------------------------------------------------------------------------
elif page == "📈 Momentum":
    st.title("📈 Market Momentum")

    latest_month = monthly_df.sort_values("period_month").groupby("zip_code").last().reset_index()
    msa_dom       = latest_month["median_dom"].median()
    msa_stl       = latest_month["median_avg_sale_to_list"].median()
    msa_sal_price = latest_month["median_sale_price"].median()

    col1, col2, col3 = st.columns(3)
    col1.metric("MSA Median Days on Market",  f"{msa_dom:.0f} days")
    col2.metric("MSA Sale-to-List Ratio",     f"{msa_stl:.1%}" if pd.notna(msa_stl) else "N/A")
    col3.metric("MSA Median Sale Price",      f"${msa_sal_price:,.0f}")

    st.markdown("---")

    mom_monthly = (
        monthly_df.groupby("period_month")
        .agg(
            median_dom=           ("median_dom",            "median"),
            median_avg_sale_to_list=("median_avg_sale_to_list","median"),
        )
        .reset_index()
        .sort_values("period_month")
    )

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Median Days on Market — MSA")
        dom_chart = (
            alt.Chart(mom_monthly.dropna(subset=["median_dom"]))
            .mark_line(color="#1a3a5c", strokeWidth=2)
            .encode(
                x=alt.X("period_month:T", title="Month"),
                y=alt.Y("median_dom:Q", title="Median DOM", scale=alt.Scale(zero=False)),
                tooltip=[
                    alt.Tooltip("period_month:T",  title="Month", format="%b %Y"),
                    alt.Tooltip("median_dom:Q",    title="DOM",   format=".0f"),
                ],
            )
            .properties(height=260)
        )
        st.altair_chart(dom_chart, use_container_width=True)

    with col_r:
        st.subheader("Sale-to-List Ratio — MSA")
        stl_chart = (
            alt.Chart(mom_monthly.dropna(subset=["median_avg_sale_to_list"]))
            .mark_line(color="#00a884", strokeWidth=2)
            .encode(
                x=alt.X("period_month:T", title="Month"),
                y=alt.Y("median_avg_sale_to_list:Q", title="Sale-to-List Ratio",
                        axis=alt.Axis(format=".0%"), scale=alt.Scale(zero=False)),
                tooltip=[
                    alt.Tooltip("period_month:T",             title="Month",  format="%b %Y"),
                    alt.Tooltip("median_avg_sale_to_list:Q",  title="Ratio",  format=".1%"),
                ],
            )
            .properties(height=260)
        )
        # Reference line at 100%
        rule = alt.Chart(pd.DataFrame({"y": [1.0]})).mark_rule(
            color="#e74c3c", strokeDash=[4, 2], strokeWidth=1.5
        ).encode(y="y:Q")
        st.altair_chart(stl_chart + rule, use_container_width=True)
        st.caption("Red dashed line = 100% (list price)")

    # Zip drill-down
    st.markdown("---")
    st.subheader("Zip-Level Days on Market")
    zip_options_m = sorted(monthly_df["zip_code"].dropna().unique().tolist())
    default_m = zip_options_m.index("37203") if "37203" in zip_options_m else 0
    selected_zip_m = st.selectbox("Select zip code", zip_options_m, index=default_m, key="mom_zip")

    msa_dom_df = mom_monthly[["period_month", "median_dom"]].rename(columns={"median_dom": "msa_dom"})
    zip_dom_df = (
        monthly_df[monthly_df["zip_code"] == selected_zip_m][["period_month", "median_dom"]]
        .merge(msa_dom_df, on="period_month", how="inner")
        .dropna(subset=["median_dom", "msa_dom"])
    )
    if not zip_dom_df.empty:
        base_m = alt.Chart(zip_dom_df)
        zip_dom = base_m.mark_line(color="#1a3a5c", strokeWidth=2).encode(
            x=alt.X("period_month:T", title="Month"),
            y=alt.Y("median_dom:Q", title="Median DOM", scale=alt.Scale(zero=False)),
            tooltip=[
                alt.Tooltip("period_month:T", title="Month", format="%b %Y"),
                alt.Tooltip("median_dom:Q",   title=f"{selected_zip_m} DOM", format=".0f"),
            ],
        )
        msa_dom_line = base_m.mark_line(color="#95a5a6", strokeWidth=1.5, strokeDash=[4, 2]).encode(
            x="period_month:T",
            y=alt.Y("msa_dom:Q", scale=alt.Scale(zero=False)),
            tooltip=[alt.Tooltip("msa_dom:Q", title="MSA Median DOM", format=".0f")],
        )
        st.altair_chart((zip_dom + msa_dom_line).properties(height=240), use_container_width=True)
        st.caption(f"Solid navy = {selected_zip_m}  ·  Dashed gray = MSA median")
    else:
        st.info(f"No DOM data available for {selected_zip_m}.")


# ---------------------------------------------------------------------------
# Section: Transactions
# ---------------------------------------------------------------------------
elif page == "🏠 Transactions":
    st.title("🏠 Transactions & Permits")

    top_permit_zip = scores_df.loc[scores_df["permit_count"].idxmax(), "zip_code"] if scores_df["permit_count"].notna().any() else "N/A"

    col1, col2, col3 = st.columns(3)
    col1.metric("MSA Median Homes Sold / Month",   f"{scores_df['median_homes_sold'].median():.0f}")
    col2.metric("MSA Median Permit Count",          f"{scores_df['permit_count'].median():.0f}")
    col3.metric("Top Permit Zip",                   top_permit_zip)

    st.caption(
        "⚠️ Metro Codes covers Davidson County only. "
        "23 suburban zips are imputed with the MSA average permit count."
    )
    st.markdown("---")

    # Homes sold trend
    trans_monthly = (
        monthly_df.groupby("period_month")
        .agg(median_homes_sold=("median_homes_sold", "median"))
        .reset_index()
        .sort_values("period_month")
    )

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Homes Sold — MSA Median")
        sold_chart = (
            alt.Chart(trans_monthly.dropna(subset=["median_homes_sold"]))
            .mark_area(color="#1a3a5c", opacity=0.3, line={"color": "#1a3a5c", "strokeWidth": 2})
            .encode(
                x=alt.X("period_month:T", title="Month"),
                y=alt.Y("median_homes_sold:Q", title="Median Homes Sold", scale=alt.Scale(zero=True)),
                tooltip=[
                    alt.Tooltip("period_month:T",      title="Month",       format="%b %Y"),
                    alt.Tooltip("median_homes_sold:Q", title="Homes Sold",  format=",.0f"),
                ],
            )
            .properties(height=280)
        )
        st.altair_chart(sold_chart, use_container_width=True)

        # Zip drill-down
        st.subheader("Zip-Level Homes Sold")
        zip_options_t = sorted(monthly_df["zip_code"].dropna().unique().tolist())
        default_t = zip_options_t.index("37203") if "37203" in zip_options_t else 0
        selected_zip_t = st.selectbox("Select zip code", zip_options_t, index=default_t, key="trans_zip")
        zip_sold = (
            monthly_df[monthly_df["zip_code"] == selected_zip_t]
            [["period_month", "median_homes_sold"]]
            .sort_values("period_month")
            .dropna(subset=["median_homes_sold"])
        )
        if not zip_sold.empty:
            zip_sold_chart = (
                alt.Chart(zip_sold)
                .mark_bar(color="#1a3a5c", opacity=0.8)
                .encode(
                    x=alt.X("period_month:T", title="Month"),
                    y=alt.Y("median_homes_sold:Q", title="Homes Sold"),
                    tooltip=[
                        alt.Tooltip("period_month:T",      title="Month",      format="%b %Y"),
                        alt.Tooltip("median_homes_sold:Q", title="Homes Sold", format=",.0f"),
                    ],
                )
                .properties(height=220)
            )
            st.altair_chart(zip_sold_chart, use_container_width=True)
        else:
            st.info(f"No homes sold data for {selected_zip_t}.")

    with col_r:
        st.subheader("Building Permits by Zip (Top 20)")
        permit_by_zip = (
            scores_df.dropna(subset=["permit_count"])
            .query("permit_count > 0")
            [["zip_code", "nashville_region", "permit_count"]]
            .sort_values("permit_count", ascending=False)
            .head(20)
        )
        permit_bar = (
            alt.Chart(permit_by_zip)
            .mark_bar(color="#00a884")
            .encode(
                x=alt.X("permit_count:Q", title="Permit Count (~3 yr window)"),
                y=alt.Y("zip_code:N", sort="-x", title=None),
                tooltip=[
                    alt.Tooltip("zip_code:N",        title="Zip"),
                    alt.Tooltip("nashville_region:N", title="Region"),
                    alt.Tooltip("permit_count:Q",     title="Permits", format=",.0f"),
                ],
            )
            .properties(height=520)
        )
        st.altair_chart(permit_bar, use_container_width=True)
        st.caption("Metro Codes rolling ~3-year window · Davidson County only")


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