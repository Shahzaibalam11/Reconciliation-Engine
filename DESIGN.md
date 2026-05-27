# Design document

This document explains the design choices behind the pipeline — what
problem each one solves, what alternatives were considered, and what
trade-offs were taken. The matching code is in `Gold/gold_matching_engine.ipynb`;
the data model is in `Diagrams/erd_reconciliation_model.html`; this is
the prose that connects them.

## 1. Problem and scope

Subscriptions are billed by telco partners on the company's behalf.
Two systems record every transaction:

- The **operator** logs every successful or failed charge. Source of truth for money.
- The **platform** records every entitlement grant — the user's right to access content. Source of truth for service delivery.

These should agree, transaction-for-transaction. They often don't. Webhooks
get lost. Operators retry charges silently. Users cancel directly with the
telco. Refunds happen without notifying the platform. Timezones don't align.

Daily reconciliation must produce two consumable artifacts:

- **`reconciliation_daily`** — a per-(business_date, operator) summary that Finance reads each morning and uses to close the books at month-end.
- **`fact_reconciliation_break`** — a row-level table for drill-down: every individual mismatch, classified into one of six categories.

The brief's six outcome categories:

| Category              | Meaning                                                                                  |
|-----------------------|------------------------------------------------------------------------------------------|
| `matched`             | Both sides agree (within tolerance) on the same transaction.                              |
| `amount_mismatch`     | Same transaction, amounts disagree beyond tolerance.                                     |
| `missing_on_platform` | Operator charged the user; platform has no record.                                       |
| `missing_at_partner`  | Platform granted entitlement; operator has no record.                                    |
| `orphan_churn`        | Platform marked the user churned; operator is still billing successfully.                |
| `late_arrival`        | Transaction shows up at the partner more than one day after its business_date.           |

## 2. Architecture

See `Diagrams/architecture_medallion_full.svg`. Four layers, standard
medallion:

- **RAW** — immutable file landing. Partner SFTP drops land here as CSV/JSON; CDC streams from the internal OLTP also land here.
- **ODS** — Delta tables that mirror RAW with light typing applied. For partner feeds the mirror is trivial (one file → one append). For internal feeds, each operator's OLTP table is mirrored separately: `sub_initial_telco_a`, `sub_initial_telco_b`, etc.

  In this implementation the ODS load is a *bootstrap cell* at the top of `Silver/silver_internal.ipynb`. In production the bootstrap would be replaced by a separate CDC ingestion service (the table count is large enough that bundling ingestion into the silver notebook stops scaling). The bootstrap cell is explicit and idempotent — running it twice is a no-op — which makes the demo work without a CDC stack.

- **SILVER** — one canonical event stream per side:
  - `silver_partner_events` — all 6–7 operator feeds normalized to one schema
  - `silver_internal_events` — all three internal table families (`sub_initial`, `sub_recursion_success`, `sub_recursion_failure`) collapsed to one schema

  SILVER is where operator-specific column names die and where every row gets stamped with both `business_date` (when the transaction happened) and `ingestion_date` (when this pipeline learned about it).

- **GOLD** — matched_pairs, breaks_raw, fact_reconciliation_break, reconciliation_daily. Built deterministically from SILVER; carries no state of its own. Re-running GOLD for a given business_date is safe.

## 3. The four design decisions

### 3.1 Idempotency lives in two places

**RAW is append-only and immutable.** Every operator file ever delivered exists on disk byte-for-byte. We never overwrite. Audit trail comes for free.

**SILVER's MERGE deduplicates.** The merge key is `(operator_code, partner_txn_id, business_date)`. A resent file produces zero new rows; a correction surfaces as an update to existing rows. The original payload is still in RAW for audit.

Why `business_date` in the merge key, not just `partner_txn_id`? Operators have been observed reusing `partner_txn_id` values across long intervals. Without `business_date`, a reused id would silently overwrite the older row. Including `business_date` forces re-use of an id within the same calendar day to collide (which is exactly the resend case we want to absorb) while preserving older rows that share an id from months earlier.

There is one edge: if a single transaction's `business_date` shifts between two ingestion runs (e.g., operator corrects a near-midnight timestamp), the MERGE misses the original row and inserts a new one, leaving a stale row behind. We catch this with a downstream data-quality check: *no two SILVER rows should have the same (operator_code, partner_txn_id) with different business_dates*. If the check fires, the row goes to a quarantine table and operator engineering investigates.

### 3.2 Dynamic discovery handles operator suffixes

The brief specifies the internal OLTP has per-operator tables:
`sub_initial_telco_a`, `sub_initial_telco_b`, `sub_initial_wallet_x`, ...
Same shape for the two recursion tables. The naive approach — hardcode
a list and union — breaks the moment operator #8 onboards.

Solution: `spark.catalog.listTables(ods_database)` returns every table; we
filter by family prefix (`sub_initial_`, `sub_recursion_success_`,
`sub_recursion_failure_`), parse the operator from the suffix, and
`unionByName` the results. Each family has its own normalizer registered
in a plain dict.

**Adding new operator to production:** land their three CDC tables. The
silver build picks them up automatically. No code change to the
matching engine, no code change to the marts.

`unionByName` runs in strict mode — if a new operator's CDC has a
schema that doesn't match the others, the build fails loud. Schema
drift should be a deployment problem, not a silent data problem.

### 3.3 Matching is a uniform fold

Every rule has the same input/output contract:

```
(partner_remaining, internal_remaining) →
    (matched_pairs, partner_remaining', internal_remaining', breaks)
```

Rules apply in priority order. Each rule consumes what it can claim,
the remainder flows to the next rule. Final unmatched rows on each
side become `missing_on_platform` and `missing_at_partner`.

**Rule order** (see `Diagrams/matching_decision_tree.svg`):

1. **`exact_match_on_txn_id`** — highest confidence. Inner join on `(operator_code, partner_txn_id, txn_type)`. Including `txn_type` in the join key routes RECUR_OK to recursion_success rows and SUB_OK to subscription rows — the SILVER normalizers tag every row with its source-derived txn_type, so this implicit routing falls out for free.

2. **`fallback_match_composite`** — for internal rows with NULL `partner_txn_id`. Join on `(operator_code, partner_account_id, txn_type, |timestamp_diff| ≤ 5min, |amount_diff| ≤ tolerance)`. If a single internal row matches 2+ partner candidates, we **refuse the match** — better to flag as `missing_on_platform` and let a human investigate than silently pair the wrong rows.

3. **`flag_late_arrivals`** — annotates matched pairs where `ingestion_date - business_date > 1 day`. Not a break category; an attribute on the row. A matched pair can be both healthy AND late.

4. **`classify_amount_mismatch`** — among matched pairs, flag where `|amount_partner - amount_internal| > tolerance`. Skips rows where either side has NULL amount (recursion_failure has no amount because no money moved).

5. **`detect_orphan_churn`** — among matched pairs, flag where the user has a churn event predating the transaction.

6. **`unmatched_partner_is_missing_on_platform`** — remaining partner rows after rules 1+2.

7. **`unmatched_internal_is_missing_at_partner`** — remaining internal rows after rules 1+2.

**Why this order matters.** Exact match before fallback (high-confidence pairs first, fewer false positives). Annotations and break-classification operate on the matched set, which has to exist first. Missing-on-each-side last, because they're the catch-all for what no rule claimed.

The fold is implemented in PySpark with declarative joins, not row-by-row scans. The fallback rule uses inequality conditions in the join predicate plus a `groupBy().count()` for ambiguity detection. At realistic volume (50k–500k rows per operator per day) the whole engine runs in seconds.

### 3.4 Bitemporal: business_date + ingestion_date on every row

Every row in `silver_partner_events`, `silver_internal_events`,
`fact_reconciliation_break`, and `reconciliation_daily` carries two dates:

- `business_date` — when the transaction actually happened (UTC)
- `ingestion_date` — when this pipeline learned about it

This pair is what makes the monthly close work. See
`Diagrams/bitemporal_restatement.svg` for the picture.

**The frozen close query** answers "what did Finance see at month-end?":

```sql
SELECT business_date, operator_code, SUM(variance)
FROM   reconciliation_daily
WHERE  business_date BETWEEN '2026-05-01' AND '2026-05-31'
  AND  ingestion_date <= '2026-06-05'     -- the close cutoff
GROUP BY business_date, operator_code;
```

**The restated query** answers "what does this period look like today?":

```sql
-- same query, drop the ingestion_date filter
SELECT business_date, operator_code, SUM(variance)
FROM   reconciliation_daily
WHERE  business_date BETWEEN '2026-05-01' AND '2026-05-31'
GROUP BY business_date, operator_code;
```

Both queries hit the same table. Neither mutates anything. Late arrivals
(like the `A-0998` example in the sample data) get inserted with
`business_date=2026-05-18, ingestion_date=2026-05-20`. The frozen close
query filters them out by ingestion date; the restated query includes them.

This is **insert-only on facts** — close cousin of SCD2, but applied to
facts not dimensions. We never UPDATE a `reconciliation_daily` row.
Re-runs overwrite the partition for a given `(business_date, ingestion_date)`
pair; corrections from a late file land as a new row with a later
ingestion_date.

## 4. Data model

See `Diagrams/erd_reconciliation_model.html` for the full ERD with crow's-foot notation.

**Silver tables** (two, both per-row carries `business_date` and `ingestion_date`):

- `silver_partner_events` — PK `(operator_code, partner_txn_id, business_date)`. The merge key from §3.1.
- `silver_internal_events` — PK `(operator_code, internal_id)`. `partner_txn_id` is FK, nullable (NULL drives the fallback matcher).

**Gold tables**:

- `gold_matched_pairs` — partner ↔ internal rows that paired up. Has `match_method` ("exact_txn_id" or "fallback_composite") and `is_late_arrival` annotation.
- `gold_breaks_raw` — uniform envelope for all break categories. Direction-aware amount placement: `missing_on_platform` puts the partner amount in `partner_amount` and leaves `internal_amount` null; `missing_at_partner` does the inverse.
- `fact_reconciliation_break` — built from `gold_breaks_raw` with a deterministic SHA256 `break_id` (so re-runs don't churn downstream dashboards) and the bitemporal date pair on every row.
- `reconciliation_daily` — the brief's §5 spec. One row per `(business_date, operator_code)`. Built as four CTEs joined together: partner totals, internal totals, matched counts (incl. late_arrival_count), break category counts (pivot).

**Dimension table** referenced but not built in this submission:

- `dim_user` — partner_account_id per user, per operator. Used by the fallback matcher's user_id → partner_account_id lookup. The matching notebook builds a placeholder; production replaces with the real SCD2 dim.

## 5. Sample data design

The sample data is engineered so every break category and every code
path fires with the minimum row count. A reviewer can trace each row
to its scenario.

- **8 partner rows** in `telco_a_2026-05-20.csv` — one each for clean match, amount mismatch, RECUR_OK that joins to recursion_success, REFUND missing on platform, timezone-edge UTC conversion, late arrival (2 days old), missing partner_txn_id (drives fallback), tolerance match (rounding).
- **3 partner rows** in `telco_b_2026-05-20.csv` — different schema (`partner_reference` vs `op_txn`), one is the orphan-churn scenario.
- **3 rows** in `telco_b_2026-05-21_correction.csv` — the resend that exercises the MERGE idempotency.
- **5 internal CSV files** under `Sample Data/Internal/`, one per (table_family, operator_code). The filename is the table name; the operator code is implicit in the filename. No `operator_code` column inside any file — it gets injected by the normalizer.

## 6. What's not in this submission

These deferred items are in `README_future_work.md` with full reasoning.
Headline summary:

- Auto Loader / structured ingestion for production-scale file pickup.
- Data-quality contracts at SILVER and operational monitoring at GOLD.
- A real `dim_user` instead of the hand-built placeholder.
