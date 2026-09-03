# ShopData ETL Pipeline

**Data Engineer Technical Assessment** — Digital Storemesh

An ETL pipeline that extracts raw sales data from SQLite, cleans historical inconsistencies, and loads the results into an analytics database to enable Customer Lifetime Value (CLV) reporting.

## Prerequisites

- Python 3.12+
- SQLite 3
- Prefect 3.x
- pytest 7.x+

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

### Run the Pipeline
```bash
python pipeline.py
```
Generates `output/analytics.db` with two tables: `dim_customers` and `fct_orders`.

### Run Unit Tests
```bash
pytest tests/ -v
```
Runs 16 tests (10 for `clean_phone`, 6 for `_clean_orders_logic`).

### Run SQL Queries
```bash
# Data quality exploration
sqlite3 data/shopdata.db < exploration.sql

# CLV report
sqlite3 output/analytics.db < clv_report.sql
```

## Project Structure

## Data Quality Findings (Part 1)

After exploring `vw_raw_customers`, `vw_raw_orders`, and `vw_exchange_rates`, I identified **5 distinct data quality issues**:

### Issue 1: Duplicate Customer Records
- **Finding:** `customer_id` values `1` and `2` each appear twice (4 rows total)
- **Impact:** Inflates customer counts and skews per-customer metrics
- **Resolution:** Deduplicate by keeping the record with the most recent `signup_date`

### Issue 2: Missing Contact Information
- **Finding:** 2 customers missing `email` (Bob Jones, Hannah Abbott) and 2 missing `phone`
- **Impact:** Prevents customer outreach for support or marketing
- **Resolution:** Fill missing emails with `unknown@domain.com` per task specification

### Issue 3: Invalid Order Amounts (System Errors)
- **Finding:** 2 orders with negative `total_amount` (order 103: -50 USD; order 113: -100 EUR)
- **Key insight:** Both negative-amount orders have `status = 'SYSTEM_ERROR'`. This correlation reveals a systematic upstream issue rather than isolated data entry errors — future work should investigate the source system.
- **Impact:** Would misrepresent revenue metrics if included
- **Resolution:** Filter out `total_amount <= 0` during the transform step

### Issue 4: Orphan Orders (Referential Integrity)
- **Finding:** 2 orders (106, 118) reference `customer_id = 99` which does not exist in `vw_raw_customers`
- **Impact:** Revenue cannot be attributed to a customer; broken referential integrity
- **Resolution:** Kept in the pipeline (LEFT JOIN preserves them) but excluded from CLV report (they don't match any customer)

### Issue 5: Missing Exchange Rates
- **Finding:** 6 non-USD orders (EUR, GBP, JPY) lack a matching row in `vw_exchange_rates`
- **Impact:** Currency conversion would fail without a fallback
- **Resolution:** Default `rate_to_usd = 1.0` per task specification ("assume already USD if rate missing")

## Architecture & Design Decisions

### Why Extract `_clean_orders_logic` from `@task`?

The `@task` decorator wraps business logic with orchestration concerns (logging, retries, context management). This makes the function **untestable outside a Prefect flow context** — calling it directly raises `MissingContextError`.

**Solution — split responsibilities:**

- `_clean_orders_logic()` — **pure function**, contains only transformation logic; testable in isolation with dummy DataFrames.
- `@task clean_orders()` — thin wrapper that adds logging, retries, and calls `_clean_orders_logic`.

This follows **Separation of Concerns**: business logic is decoupled from the orchestration framework, so tests don't require Prefect and future migration to Airflow/Dagster would only touch the wrapper.

### Why LEFT JOIN in CLV Report?

Used `LEFT JOIN dim_customers → fct_orders` so that **customers who never placed an order still appear** in the report (with `lifetime_value_usd = NULL`).

An `INNER JOIN` would hide inactive customers, misleading the reader into thinking only active customers exist.

### Why Filter `status = 'COMPLETED'` in CLV?

Business rationale: **CLV represents realized revenue**, not intent to purchase.
- `CANCELLED` — order cancelled, no revenue collected
- `PENDING` — awaiting confirmation, not yet realized
- `SYSTEM_ERROR` — upstream data quality issue
- `COMPLETED` — actual revenue → the only status that should count

### Why Place `status` Filter in the ON Clause?

Placing `AND o.status = 'COMPLETED'` in the **ON clause** (not `WHERE`) preserves LEFT JOIN semantics: customers with zero orders keep `o.status = NULL` after the JOIN and are retained in the result. Placing the filter in `WHERE` would silently convert the LEFT JOIN to an INNER JOIN by discarding those NULL rows.

## Testing Approach

**16 unit tests** across two transformation functions, all running **independently of the database** via `pytest` fixtures containing dummy DataFrames:

### `TestCleanPhone` (10 tests)
Standard formats, Thai number format, and edge cases:
- `None`, `pd.NA`, empty string
- Alphabetic characters, only special characters
- Integer input, float input (drops decimal)

### `TestCleanOrders` (6 tests)
Covers all transformation rules:
- Filters negative and zero amounts
- Currency conversion with matching exchange rate (EUR × 1.1 → USD)
- Fallback: missing `currency` → default to USD
- Fallback: missing `rate_to_usd` → default to 1.0
- Verifies `usd_amount` column is added

Uses `pytest.approx()` for floating-point comparisons to handle IEEE 754 precision issues.

**Run tests:** `pytest tests/ -v`

## CLV Report — Sample Output

Top customer by lifetime value: **Charlie Brown** (id=3, cohort `2023-03`) with **$25,000** from a single order.

- Total customers in report: **10**
- Customers with no orders: **1** (Hannah Abbott — signed up but never purchased)

## Reviewer Notes

- Pipeline uses SQLite for both source (`data/shopdata.db`) and destination (`output/analytics.db`), matching task specification.
- Both output formats supported:
  - **SQLite** (`analytics.db`) — primary output
  - **CSV** (`clean_customers.csv`, `clean_orders.csv`) — fallback as specified in the task

## Author

**Teeradate Phathun**
Data Engineer Applicant — Digital Storemesh
aomsin.4480@gmail.com
0922514359
GitHub: [Aomzaaan](https://github.com/Aomzaaan)

