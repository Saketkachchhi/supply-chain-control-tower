#!/usr/bin/env python3
"""
Supply Chain Control Tower
Live view over supply_chain.db: the optimized production/inventory schedules
written by inventory_optimizer.py, polling for fresh data every few seconds.

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
STALE_AFTER_SECONDS = 300  # no write in 5 min -> flag the pipeline as stale

# Mirrors the fixed cost/capacity assumptions inventory_optimizer.py uses to solve the LP.
# The DuckDB schema stores quantities and the already-computed sla_penalty_cost (Task 1's
# spec), not the production/holding cost - the dashboard derives those two from quantities
# using the same constants, and reads the SLA cost straight from the stored column instead
# of re-deriving it, so SLA_PENALTY_COST itself doesn't need to be duplicated here too.
PRODUCTION_COST_PER_UNIT = 10.00
HOLDING_COST_PER_UNIT_PER_DAY = 1.50
MAX_DAILY_PRODUCTION = 150.0  # factory capacity ceiling, drawn as a reference line on the chart

# Categorical slots 1 (blue) & 2 (aqua) from the validated palette for the two "neutral"
# series (Production, Inventory), plus the fixed status palette's "critical" red for
# Stockout - stockout means failure, not just "series 3", so it wears a status color
# rather than the next categorical hue (see dataviz skill: status colors are reserved
# and never impersonate a series). Critical is the same hex in both modes per the palette.
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


st.set_page_config(page_title="Supply Chain Control Tower", layout="wide")
st_autorefresh(interval=REFRESH_INTERVAL_MS, key="autorefresh")

theme = get_theme_colors()
schedule_df = load_schedule(DB_PATH)

title_col, status_col = st.columns([3, 1])
with title_col:
    st.title("Supply Chain Control Tower")
    st.caption("Optimized production & inventory schedule, per product - next 7 days")
with status_col:
    st.caption(f"Dashboard polled: {datetime.now().strftime('%H:%M:%S')} (every {REFRESH_INTERVAL_MS // 1000}s)")
    if not schedule_df.empty:
        last_write = pd.to_datetime(schedule_df["updated_at"]).max()
        now_utc_naive = datetime.now(timezone.utc).replace(tzinfo=None)
        age_seconds = (now_utc_naive - last_write).total_seconds()
        if age_seconds < STALE_AFTER_SECONDS:
            st.success(f"Live - schedule updated {int(age_seconds)}s ago")
        else:
            st.warning(f"Stale - no update in {int(age_seconds // 60)}m")

if schedule_df.empty:
    st.info(
        "No optimization data yet. Start `docker compose up`, then run "
        "`pos_simulator.py`, `demand_forecaster.py`, and `inventory_optimizer.py` - "
        "this view populates as soon as the first 7-day schedule is solved."
    )
    st.stop()

products = sorted(schedule_df["product_id"].unique())

# ---- Critical SLA KPI (own row, at the top - this is the one number that means
# "the supply chain is failing," so it gets outsized visual priority over the
# routine KPIs below it) ----
total_stockout_units = schedule_df["stockout"].sum()
total_sla_penalty = schedule_df["sla_penalty_cost"].sum()

# st.metric has no way to color the headline value itself (only the small delta text),
# and the task requires the number to turn red on a violation - so this one tile is
# hand-built to match st.metric's look (border=True) while allowing that. Colored text
# is never the only signal: the status word next to it carries the same meaning, so the
# alert still reads correctly without relying on color perception alone.
if total_sla_penalty > 0:
    value_color = theme["critical"]
    status_text = f"SLA BREACH — {total_stockout_units:,.0f} unit(s) of demand went unmet"
else:
    value_color = theme["text_primary"]
    status_text = "No SLA violations — all forecasted demand is covered"

st.markdown(f"""
<div style="border:1px solid {theme['border']}; border-radius:0.5rem; padding:1rem 1.25rem;
            background:{theme['surface']}; margin-bottom:1rem;">
    <div style="font-size:0.875rem; color:{theme['text_secondary']};">SLA Penalties Incurred ($)</div>
    <div style="font-size:2.25rem; font-weight:600; color:{value_color}; line-height:1.3;">
        ${total_sla_penalty:,.2f}
    </div>
    <div style="font-size:0.85rem; font-weight:600; color:{value_color};">{status_text}</div>
</div>
""", unsafe_allow_html=True)

# ---- Supporting KPI row ----
total_production_units = schedule_df["optimal_production"].sum()
total_inventory_unit_days = schedule_df["projected_inventory"].sum()
total_demand_units = schedule_df["forecasted_demand"].sum()
total_cost = (
    total_production_units * PRODUCTION_COST_PER_UNIT
    + total_inventory_unit_days * HOLDING_COST_PER_UNIT_PER_DAY
    + total_sla_penalty
)

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
with kpi1:
    st.metric("Total optimized cost (7d, all products)", f"${total_cost:,.2f}", border=True)
with kpi2:
    st.metric("Total forecasted demand (7d)", f"{total_demand_units:,.0f} units", border=True)
with kpi3:
    st.metric("Total planned production (7d)", f"{total_production_units:,.0f} units", border=True)
with kpi4:
    st.metric("Products tracked", f"{len(products)}", border=True)

st.divider()

# ---- Production / inventory / stockout chart ----
st.subheader("Production capacity vs. stockouts")
selected_product = st.selectbox("Select product", products)

product_df = schedule_df.loc[schedule_df["product_id"] == selected_product].sort_values("date")

# Production and Stockout are stacked bars: Stockout is the demand that production
# couldn't reach that day, so stacking it on top of Production shows the full picture
# in one bar - a full-height bar with no red cap means the day was fully covered; red
# appearing on top is the day capacity failed to keep up. Inventory is a running level.
# rather than a per-day flow, so it rides as its own line rather than joining the stack.
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
    name="Stockouts (SLA Violation)",
    marker_color=theme["critical"],
    hovertemplate="%{y:,.1f} units<extra>Stockout</extra>",
))
fig.add_trace(go.Scatter(
    x=product_df["date"],
    y=product_df["projected_inventory"],
    name="Projected Inventory",
    mode="lines+markers",
    line=dict(color=theme["inventory"], width=2),
    marker=dict(size=8, color=theme["inventory"]),
    hovertemplate="%{y:,.1f} units<extra>Projected Inventory</extra>",
))
fig.add_hline(
    y=MAX_DAILY_PRODUCTION,
    line=dict(color=theme["muted"], width=1, dash="dash"),
    annotation_text=f"Max daily capacity ({MAX_DAILY_PRODUCTION:.0f})",
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
        f"{(product_df['stockout'] > 0).sum()} of the next 7 days - production hit the "
        f"{MAX_DAILY_PRODUCTION:.0f}-unit daily cap before demand was satisfied."
    )

st.divider()

# ---- Raw data ----
st.subheader("Raw schedule data")
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
