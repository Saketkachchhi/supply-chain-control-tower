# Assets

Drop screenshots here with these exact filenames to fill in the image placeholders in `PRODUCT_AND_INTERVIEW_REPORT.md`:

- `dashboard_overview.png` — a full view of the running Streamlit Control Tower (all three Hero KPI tiles plus the chart).
- `pos_simulator_terminal.png` — a terminal window running `pos_simulator.py`, showing live simulated sales printing as they're generated.
- `architecture_flow.png` — a diagram of the pipeline: POS Simulator → Redpanda → Prophet Forecaster → OR-Tools Optimizer → DuckDB → Streamlit Dashboard.
- `dashboard_prebuild_chart.png` — the dashboard's production/inventory/stockout chart for a product with an active SLA breach, ideally showing the inventory line climbing on the day before the stockout (select a product with a demand spike, such as one seeded with `[10, 300, ...]` and zero starting inventory, to reproduce this).
