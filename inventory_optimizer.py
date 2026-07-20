#!/usr/bin/env python3
"""
Inventory Optimizer
Consumes 7-day demand forecasts from the `demand_forecasts` topic and solves
a linear program for the cost-minimizing daily production/inventory schedule,
subject to a finite daily production capacity. Demand that capacity can't cover
is absorbed by a stockout slack variable at a fixed SLA penalty cost per unit,
rather than making the model infeasible.
"""

import json
import time
from datetime import datetime, timezone

import duckdb
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from kafka.serializer import DefaultSerializer, Deserializer
from ortools.linear_solver import pywraplp

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "demand_forecasts"
CONSUMER_GROUP_ID = "inventory-optimizer"

DB_PATH = "supply_chain.db"

WAREHOUSE_CAPACITY = 5000.0        # units, I_t <= this every day
HOLDING_COST_PER_UNIT_PER_DAY = 1.50   # $, cost of carrying one unit of I_t overnight
PRODUCTION_COST_PER_UNIT = 10.00       # $, cost of one unit of P_t
MAX_DAILY_PRODUCTION = 150.0           # units, P_t <= this every day - factory capacity ceiling
SLA_PENALTY_COST = 100.00              # $, cost per unit of S_t (unmet demand / stockout)
STARTING_INVENTORY = 100.0             # units on hand before day 1 (I_0), fixed per the spec -
                                        # a production system would carry forward each product's
                                        # actual current stock between runs instead of resetting to
                                        # this constant every time a forecast arrives.


class JSONDeserializer(Deserializer):
    """kafka-python's own JsonSerializer is broken in 3.0.x (deserialize() returns None
    because its super().deserialize() call resolves wrong); this replaces it."""

    def deserialize(self, topic, headers, data):
        if data is None:
            return None
        return json.loads(data)


def create_consumer(broker: str, topic: str, retries: int = 10, retry_delay: float = 3.0) -> KafkaConsumer:
    """Connect to the Kafka/Redpanda broker, retrying while it warms up."""
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=broker,
                value_deserializer=JSONDeserializer(),
                key_deserializer=DefaultSerializer(),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id=CONSUMER_GROUP_ID,
                bootstrap_timeout_ms=5000,
            )
            print(f"[OK] Connected to broker at {broker}, subscribed to '{topic}'\n")
            return consumer
        except KafkaError:
            print(
                f"[WAIT] Broker not reachable at {broker} "
                f"(attempt {attempt}/{retries}). Is Redpanda running? Retrying in {retry_delay:.0f}s..."
            )
            time.sleep(retry_delay)
    raise ConnectionError(f"Could not reach Kafka broker at {broker} after {retries} attempts.")


def solve_production_schedule(daily_demand: list, starting_inventory: float = STARTING_INVENTORY) -> dict:
    """
    Linear program (GLOP): for each day t = 0..horizon-1, choose production P_t in
    [0, MAX_DAILY_PRODUCTION], inventory I_t in [0, WAREHOUSE_CAPACITY], and stockout
    (unmet demand) S_t >= 0 to minimize
        sum_t ( PRODUCTION_COST_PER_UNIT * P_t
                + HOLDING_COST_PER_UNIT_PER_DAY * I_t
                + SLA_PENALTY_COST * S_t )
    subject to the inventory balance
        I_t - S_t = I_{t-1} + P_t - Demand_t      (I_{-1} := starting_inventory)
    The I_t >= 0, I_t <= WAREHOUSE_CAPACITY, P_t <= MAX_DAILY_PRODUCTION, and S_t >= 0
    bounds are encoded directly as each variable's [lower, upper] bounds rather than
    separate constraint rows.

    S_t is what keeps the model feasible now that production is capped: without it,
    a day whose demand exceeds I_{t-1} + MAX_DAILY_PRODUCTION would force I_t negative,
    which the I_t >= 0 bound forbids outright (INFEASIBLE, no solution at all). S_t
    absorbs exactly that shortfall instead, so the LP always has a solution - it just
    prices the shortfall at SLA_PENALTY_COST/unit rather than pretending it can't happen.
    Because SLA_PENALTY_COST ($100) is far above PRODUCTION_COST_PER_UNIT ($10), the
    solver only ever lets S_t go positive once P_t is already pinned at its ceiling -
    it is always cheaper to make one more unit than to eat the penalty for it.
    """
    solver = pywraplp.Solver.CreateSolver("GLOP")
    if solver is None:
        raise RuntimeError("Could not create GLOP solver")

    horizon = len(daily_demand)
    production = [solver.NumVar(0, MAX_DAILY_PRODUCTION, f"P_{t}") for t in range(horizon)]
    inventory = [solver.NumVar(0, WAREHOUSE_CAPACITY, f"I_{t}") for t in range(horizon)]
    stockout = [solver.NumVar(0, solver.infinity(), f"S_{t}") for t in range(horizon)]

    for t in range(horizon):
        previous_inventory = starting_inventory if t == 0 else inventory[t - 1]
        solver.Add(inventory[t] - stockout[t] == previous_inventory + production[t] - daily_demand[t])

    objective = solver.Objective()
    for t in range(horizon):
        objective.SetCoefficient(production[t], PRODUCTION_COST_PER_UNIT)
        objective.SetCoefficient(inventory[t], HOLDING_COST_PER_UNIT_PER_DAY)
        objective.SetCoefficient(stockout[t], SLA_PENALTY_COST)
    objective.SetMinimization()

    status = solver.Solve()
    status_name = {
        pywraplp.Solver.OPTIMAL: "OPTIMAL",
        pywraplp.Solver.FEASIBLE: "FEASIBLE",
        pywraplp.Solver.INFEASIBLE: "INFEASIBLE",
        pywraplp.Solver.UNBOUNDED: "UNBOUNDED",
    }.get(status, "ERROR")

    return {
        "status": status_name,
        "production": [p.solution_value() for p in production],
        "inventory": [i.solution_value() for i in inventory],
        "stockout": [s.solution_value() for s in stockout],
        "total_cost": objective.Value() if status_name in ("OPTIMAL", "FEASIBLE") else None,
    }


def write_schedule_to_duckdb(db_path: str, product_id: str, dates: list, daily_demand: list, result: dict) -> None:
    """
    Persist the latest 7-day schedule for one product to DuckDB, including per-day
    stockout volume and its dollar SLA penalty (SLA_PENALTY_COST * stockout).

    Every write is a transactional delete-then-insert scoped to product_id, not an
    ON CONFLICT upsert: each new forecast's 7-day window can shift (yesterday's
    dates roll off, a new date appears at the end), and a keyed upsert would leave
    the rolled-off date behind as a stale row. Deleting the product's existing rows
    before inserting the new ones is what actually guarantees the table holds only
    the current outlook per product, which is the stated requirement.

    The ALTER TABLE calls migrate a database created before this SLA upgrade (which
    only had the original 5 data columns) - CREATE TABLE IF NOT EXISTS alone would
    leave an old table's schema untouched, and the INSERT below would then fail with
    a column-count mismatch the first time this runs against pre-upgrade data.
    """
    con = duckdb.connect(db_path)
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS inventory_schedule (
                product_id VARCHAR NOT NULL,
                date DATE NOT NULL,
                forecasted_demand DOUBLE,
                optimal_production DOUBLE,
                projected_inventory DOUBLE,
                stockout DOUBLE,
                sla_penalty_cost DOUBLE,
                updated_at TIMESTAMP,
                PRIMARY KEY (product_id, date)
            )
        """)
        con.execute("ALTER TABLE inventory_schedule ADD COLUMN IF NOT EXISTS stockout DOUBLE DEFAULT 0.0")
        con.execute("ALTER TABLE inventory_schedule ADD COLUMN IF NOT EXISTS sla_penalty_cost DOUBLE DEFAULT 0.0")

        now = datetime.now(timezone.utc)
        rows = [
            (product_id, date, demand, production, inventory, stockout, stockout * SLA_PENALTY_COST, now)
            for date, demand, production, inventory, stockout in zip(
                dates, daily_demand, result["production"], result["inventory"], result["stockout"]
            )
        ]

        con.execute("BEGIN TRANSACTION")
        con.execute("DELETE FROM inventory_schedule WHERE product_id = ?", [product_id])
        con.executemany("INSERT INTO inventory_schedule VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
        con.execute("COMMIT")

        total_stockout = sum(result["stockout"])
        if total_stockout > 0:
            print(f"    -> wrote {len(rows)}-day schedule to {db_path} "
                  f"[!! SLA BREACH: {total_stockout:.1f} units unmet]")
        else:
            print(f"    -> wrote {len(rows)}-day schedule to {db_path}")
    finally:
        con.close()


def print_schedule(product_id: str, dates: list, daily_demand: list, result: dict) -> None:
    print("\n" + "=" * 60)
    print(f" INVENTORY OPTIMIZATION - {product_id}")
    print("=" * 60)

    if result["status"] != "OPTIMAL":
        print(f" Solver status: {result['status']} - no schedule produced.")
        print("=" * 60 + "\n")
        return

    production = result["production"]
    inventory = result["inventory"]
    stockout = result["stockout"]

    print(f" {'Date':<12}{'Demand':>10}{'Production':>12}{'Inventory':>12}{'Stockout':>12}")
    print(" " + "-" * 58)
    for date, demand, p, i, s in zip(dates, daily_demand, production, inventory, stockout):
        flag = "  <-- SLA BREACH" if s > 0 else ""
        print(f" {date:<12}{demand:>10.1f}{p:>12.1f}{i:>12.1f}{s:>12.1f}{flag}")

    total_production = sum(production)
    total_stockout = sum(stockout)
    total_production_cost = total_production * PRODUCTION_COST_PER_UNIT
    total_holding_cost = sum(inventory) * HOLDING_COST_PER_UNIT_PER_DAY
    total_sla_penalty_cost = total_stockout * SLA_PENALTY_COST

    print(" " + "-" * 58)
    print(f" Total production:        {total_production:>10.1f} units")
    print(f" Total production cost:   ${total_production_cost:>10,.2f}")
    print(f" Total holding cost:      ${total_holding_cost:>10,.2f}")
    print(f" Total stockout:          {total_stockout:>10.1f} units")
    print(f" Total SLA penalty cost:  ${total_sla_penalty_cost:>10,.2f}")
    print(f" MINIMIZED TOTAL COST:    ${result['total_cost']:>10,.2f}")
    if total_stockout > 0:
        print(f" !! SLA VIOLATION: demand exceeded the {MAX_DAILY_PRODUCTION:.0f}/day production "
              f"cap on {sum(1 for s in stockout if s > 0)} of {len(stockout)} day(s).")
    print("=" * 60 + "\n")


def main():
    print("=" * 60)
    print(" INVENTORY OPTIMIZER")
    print(f" Topic:              {TOPIC_NAME}")
    print(f" Broker:             {KAFKA_BROKER}")
    print(f" Warehouse capacity: {WAREHOUSE_CAPACITY:,.0f} units")
    print(f" Holding cost:       ${HOLDING_COST_PER_UNIT_PER_DAY:.2f} / unit / day")
    print(f" Production cost:    ${PRODUCTION_COST_PER_UNIT:.2f} / unit")
    print(f" Max daily production: {MAX_DAILY_PRODUCTION:,.0f} units")
    print(f" SLA penalty cost:   ${SLA_PENALTY_COST:.2f} / unit unmet")
    print(f" Starting inventory: {STARTING_INVENTORY:,.0f} units")
    print(f" DuckDB sink:        {DB_PATH}")
    print("=" * 60 + "\n")

    consumer = create_consumer(KAFKA_BROKER, TOPIC_NAME)
    total_forecasts = 0

    try:
        for message in consumer:
            forecast = message.value
            if forecast is None:
                continue

            total_forecasts += 1
            product_id = forecast["product_id"]
            demand_map = forecast["daily_forecasted_demand"]
            dates = list(demand_map.keys())
            daily_demand = [max(0.0, float(v)) for v in demand_map.values()]

            print(f"[{total_forecasts}] Received forecast for {product_id}: "
                  f"{[round(d, 1) for d in daily_demand]}")

            result = solve_production_schedule(daily_demand)
            print_schedule(product_id, dates, daily_demand, result)

            if result["status"] == "OPTIMAL":
                write_schedule_to_duckdb(DB_PATH, product_id, dates, daily_demand, result)

    except KeyboardInterrupt:
        print(f"\n\nShutting down. Total forecasts processed: {total_forecasts}")
    finally:
        consumer.close()
        print("Consumer closed cleanly.")


if __name__ == "__main__":
    main()
