#!/usr/bin/env python3
"""
Supply Chain Control Tower
A narrative-driven live view over supply_chain.db, structured as a Control
Tower story: what it costs (Hero KPIs), how the solver avoids SLA failure
(the chart), why that's mathematically inevitable (the analytical insight),
and the raw receipts to verify it (the data audit).

Run with: streamlit run dashboard.py
"""

import os
from datetime import datetime, timezone

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

DB_PATH = "supply_chain.db"
REFRESH_INTERVAL_MS = 3000
STALE_AFTER_SECONDS = 300  # no write in 5 min -> System Status tile flips to STALE

# Mirrors the fixed cost/capacity assumptions inventory_optimizer.py uses to solve the
# LP. The DuckDB schema stores quantities and the already-computed sla_penalty_cost,
# not the production/holding cost - the dashboard derives those two from quantities
# using the same constants, and reads the SLA cost straight from its stored column.
PRODUCTION_COST_PER_UNIT = 10.00
HOLDING_COST_PER_UNIT_PER_DAY = 1.50
MAX_DAILY_PRODUCTION = 150.0

# Categorical slots 1 (blue) & 2 (aqua) for the two "neutral" series (Production,
# Inventory), the fixed status palette's "critical" red for Stockout (a failure
# signal, not just "series 3"), and "good"/"warning" for the System Status tile.
# Light/dark variants swap on the active Streamlit theme.
PALETTE = {
    "light": {
        "surface": "#fcfcfb",
        "text_primary": "#0b0b0b",
        "text_secondary": "#52514e",
        "muted": "#898781",
        "gridline": "#e1e0d9",
        "border": "rgba(11,11,11,0.10)",
        "production": "#2a78d6",
        "inventory": "#1baf7a",
        "critical": "#d03b3b",
        "good": "#0ca30c",
        "warning": "#fab219",
    },
    "dark": {
        "surface": "#1a1a19",
        "text_primary": "#ffffff",
        "text_secondary": "#c3c2b7",
        "muted": "#898781",
        "gridline": "#2c2c2a",
        "border": "rgba(255,255,255,0.10)",
        "production": "#3987e5",
        "inventory": "#199e70",
        "critical": "#d03b3b",
        "good": "#0ca30c",
        "warning": "#fab219",
    },
}


def get_theme_colors() -> dict:
    theme_type = "light"
    try:
        theme_type = st.context.theme.type or "light"
    except Exception:
        pass
    return PALETTE.get(theme_type, PALETTE["light"])


def load_schedule(db_path: str) -> pd.DataFrame:
    """
    Open a FRESH read-only connection on every call rather than reusing one across
    Streamlit reruns. Verified empirically: a DuckDB read-only connection snapshots
    at open time and does not see commits made by a separate writer process
    afterward - caching the *connection* (e.g. via st.cache_resource) would freeze
    this dashboard on whatever data existed at first connect, silently defeating
    the autorefresh. Reconnecting each call is what makes "every 3 seconds" real.
    """
    if not os.path.exists(db_path):
        return pd.DataFrame()
    try:
        con = duckdb.connect(db_path, read_only=True)
    except duckdb.IOException:
        return pd.DataFrame()
    try:
        return con.execute("SELECT * FROM inventory_schedule ORDER BY product_id, date").df()
    except duckdb.CatalogException:
        return pd.DataFrame()
    finally:
        con.close()


def render_tile(label: str, value: str, detail: str, theme: dict,
                 value_color: str = None, glow: bool = False) -> str:
    """Render one Hero KPI tile as self-contained HTML so all three share a single
    visual contract (border, padding, typography) regardless of which one carries
    conditional color or the SLA-breach glow animation."""
    color = value_color or theme["text_primary"]
    glow_class = " kpi-glow" if glow else ""
    return f"""
    <div class="kpi-tile{glow_class}" style="border:1px solid {theme['border']};
                border-radius:0.5rem; padding:1rem 1.25rem; background:{theme['surface']};
                height:100%;">
        <div style="font-size:0.78rem; font-weight:600; color:{theme['text_secondary']};
                    text-transform:uppercase; letter-spacing:0.04em;">{label}</div>
        <div style="font-size:2rem; font-weight:700; color:{color}; line-height:1.25;
                    margin:0.2rem 0;">{value}</div>
        <div style="font-size:0.82rem; color:{theme['text_secondary']};">{detail}</div>
    </div>
    """


st.set_page_config(page_title="Supply Chain Control Tower", layout="wide")
st_autorefresh(interval=REFRESH_INTERVAL_MS, key="autorefresh")

theme = get_theme_colors()
schedule_df = load_schedule(DB_PATH)

st.title("Supply Chain Control Tower")
st.caption("An event-driven forecast → optimize → monitor loop for production cost and SLA risk")

if schedule_df.empty:
    st.info(
        "No optimization data yet. Start `docker compose up`, then run "
        "`pos_simulator.py`, `demand_forecaster.py`, and `inventory_optimizer.py` - "
        "this view populates as soon as the first 7-day schedule is solved."
    )
    st.stop()

products = sorted(schedule_df["product_id"].unique())

# ======================================================================
# SECTION 1 - THE HERO KPIS ("What")
# ======================================================================
total_production_units = schedule_df["optimal_production"].sum()
total_inventory_unit_days = schedule_df["projected_inventory"].sum()
total_stockout_units = schedule_df["stockout"].sum()
total_sla_penalty = schedule_df["sla_penalty_cost"].sum()
total_cost = (
    total_production_units * PRODUCTION_COST_PER_UNIT
    + total_inventory_unit_days * HOLDING_COST_PER_UNIT_PER_DAY
    + total_sla_penalty
)

last_write = pd.to_datetime(schedule_df["updated_at"]).max()
now_utc_naive = datetime.now(timezone.utc).replace(tzinfo=None)
age_seconds = (now_utc_naive - last_write).total_seconds()
is_live = age_seconds < STALE_AFTER_SECONDS

st.markdown("""
<style>
@keyframes kpi-pulse {
  0%, 100% { box-shadow: 0 0 0 0 rgba(208, 59, 59, 0.0); }
  50%      { box-shadow: 0 0 18px 4px rgba(208, 59, 59, 0.55); }
}
.kpi-glow { animation: kpi-pulse 1.6s ease-in-out infinite; border-color: #d03b3b !important; }
</style>
""", unsafe_allow_html=True)

hero1, hero2, hero3 = st.columns(3)
with hero1:
    st.markdown(
        render_tile(
            "Total Operating Cost",
            f"${total_cost:,.2f}",
            "Production + holding + SLA penalties, next 7 days, all products",
            theme,
        ),
        unsafe_allow_html=True,
    )
with hero2:
    sla_breach = total_sla_penalty > 0
    sla_detail = (
        f"BREACH — {total_stockout_units:,.0f} unit(s) of demand unmet"
        if sla_breach else "No violations — all demand covered"
    )
    st.markdown(
        render_tile(
            "SLA Penalties Incurred",
            f"${total_sla_penalty:,.2f}",
            sla_detail,
            theme,
            value_color=theme["critical"] if sla_breach else theme["text_primary"],
            glow=sla_breach,  # flashes red only once the tile breaches $0, per spec
        ),
        unsafe_allow_html=True,
    )
with hero3:
    status_value = "LIVE" if is_live else "STALE"
    status_detail = (
        f"Schedule updated {int(age_seconds)}s ago" if is_live
        else f"No update in {int(age_seconds // 60)}m — check the pipeline"
    )
    st.markdown(
        render_tile(
            "System Status",
            status_value,
            status_detail,
            theme,
            value_color=theme["good"] if is_live else theme["warning"],
        ),
        unsafe_allow_html=True,
    )

st.divider()

# ======================================================================
# SECTION 2 - THE VISUAL PROOF ("How")
# ======================================================================
st.subheader("How the solver avoids SLA failure")
selected_product = st.selectbox("Select product", products)

product_df = schedule_df.loc[schedule_df["product_id"] == selected_product].sort_values("date")

# Production and Stockout are stacked bars: stacking Stockout on top of Production
# shows the full demand-coverage attempt in one bar - a red cap appearing means the
# factory hit its ceiling before demand was satisfied that day. Inventory rides as
# a deliberately thick, contrasting overlaid line (not part of the stack, since it's
# a running level rather than a daily flow) so a viewer's eye is drawn straight to
# it climbing in the days before a spike - the visual signature of the solver
# pre-building stock ahead of a shortfall it can already see coming.
fig = go.Figure()
fig.add_trace(go.Bar(
    x=product_df["date"],
    y=product_df["optimal_production"],
    name="Optimal Production",
    marker_color=theme["production"],
    hovertemplate="%{y:,.1f} units<extra>Optimal Production</extra>",
))
fig.add_trace(go.Bar(
    x=product_df["date"],
    y=product_df["stockout"],
    name="Stockout (SLA Violation)",
    marker_color=theme["critical"],
    hovertemplate="%{y:,.1f} units<extra>Stockout</extra>",
))
fig.add_trace(go.Scatter(
    x=product_df["date"],
    y=product_df["projected_inventory"],
    name="Projected Inventory",
    mode="lines+markers",
    line=dict(color=theme["inventory"], width=4),
    marker=dict(size=9, color=theme["inventory"]),
    hovertemplate="%{y:,.1f} units<extra>Projected Inventory</extra>",
))
fig.add_hline(
    y=MAX_DAILY_PRODUCTION,
    line=dict(color=theme["muted"], width=1.5, dash="dash"),
    annotation_text=f"Max Factory Capacity ({MAX_DAILY_PRODUCTION:.0f} units)",
    annotation_position="top left",
    annotation_font=dict(color=theme["muted"], size=11),
)
fig.update_layout(
    barmode="stack",
    paper_bgcolor=theme["surface"],
    plot_bgcolor=theme["surface"],
    font=dict(color=theme["text_secondary"], family="system-ui, -apple-system, 'Segoe UI', sans-serif"),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0, font=dict(color=theme["text_primary"])),
    margin=dict(l=10, r=10, t=10, b=10),
    hovermode="x unified",
    xaxis=dict(title=None, gridcolor=theme["gridline"], linecolor=theme["gridline"], tickfont=dict(color=theme["muted"])),
    yaxis=dict(title="Units", rangemode="tozero", gridcolor=theme["gridline"], linecolor=theme["gridline"], tickfont=dict(color=theme["muted"]), zeroline=False),
)
st.plotly_chart(fig, config={"displaylogo": False})

if product_df["stockout"].sum() > 0:
    st.warning(
        f"{selected_product} could not fully meet forecasted demand on "
        f"{(product_df['stockout'] > 0).sum()} of the next 7 days — note the inventory "
        f"line climbing beforehand as the solver pre-builds ahead of the shortfall."
    )

# ======================================================================
# SECTION 3 - THE ANALYTICAL CONTEXT ("Why")
# ======================================================================
st.info(
    "**Analytical Insight: The Mathematical Floor**\n\n"
    "When a 300-unit demand spike hits against a 150-unit daily production cap and "
    "zero starting inventory, a 10-unit residual stockout is mathematically "
    "unavoidable. The solver successfully finds this absolute floor, minimizing the "
    "SLA penalty rather than failing."
)

st.divider()

# ======================================================================
# SECTION 4 - THE DATA AUDIT ("Receipts")
# ======================================================================
st.subheader("The Data Audit")
st.caption("Full daily detail behind the chart above — verify the exact production, inventory, and stockout math for any product.")

display_df = schedule_df.rename(columns={
    "product_id": "Product ID",
    "date": "Date",
    "forecasted_demand": "Forecasted Demand",
    "optimal_production": "Optimal Production",
    "projected_inventory": "Projected Inventory",
    "stockout": "Stockout",
    "sla_penalty_cost": "SLA Penalty Cost",
    "updated_at": "Updated At (UTC)",
})
st.dataframe(
    display_df,
    hide_index=True,
    column_config={
        "Forecasted Demand": st.column_config.NumberColumn(format="%.1f"),
        "Optimal Production": st.column_config.NumberColumn(format="%.1f"),
        "Projected Inventory": st.column_config.NumberColumn(format="%.1f"),
        "Stockout": st.column_config.NumberColumn(format="%.1f"),
        "SLA Penalty Cost": st.column_config.NumberColumn(format="$%.2f"),
        "Updated At (UTC)": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm:ss"),
    },
)
