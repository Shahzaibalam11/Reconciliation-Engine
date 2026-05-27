# Subscription reconciliation Engine

A daily reconciliation pipeline that joins partner (telco/wallet) transaction
feeds with internal platform subscription records, identifies breaks across
six categories, and produces stats that Finance can use to close the books
and Product can use to detect silent churn.

Implemented on Microsoft Fabric using PySpark notebooks against a lakehouse.
Runs end-to-end on Fabric's free tier with the sample data shipped here.

![Architecture overview](Diagrams/architecture_medallion_full.svg)

## What's in this repo

```
Reconciliation Engine - Case Study/
├── README.md                           ← you are here
├── DESIGN.md                           ← full design document
├── README_future_work.md               ← extensions deferred from the scope
├── Diagrams/
│   ├── architecture_medallion_full.svg ← end-to-end pipeline
│   ├── erd_reconciliation_model.html   ← data model (Mermaid)
│   ├── matching_decision_tree.svg      ← rule order in the matching engine
│   └── bitemporal_restatement.svg      ← how late arrivals update closed periods
├── Silver/
│   ├── silver_partner.ipynb            ← partner feed normalization
│   └── silver_internal.ipynb           ← internal table normalization
├── Gold/
│   ├── gold_matching_engine.ipynb      ← the seven-rule fold
│   └── gold_reconciliation.ipynb       ← fact_break + reconciliation_daily
├── monthly_close_query.ipynb           ← SQL queries to show monthly close for finance
└── Sample Data/
    ├── Partner/                        ← per-operator per-date CSVs
    └── Internal/                       ← per-operator-table CSVs
```

## How to run on Fabric

1. **Create a lakehouse** in your Fabric workspace by name ODS; attach it to all shared notebooks.
2. **Upload sample data** to `Files/`: copy `Sample Data/Partner/*` and `Sample Data/Internal/*`.
3. **Run notebooks in order**:
   - `Silver/silver_partner.ipynb` — reads partner CSVs from `Files/Sample Data/Partner/`, normalizes, writes `silver_partner_events` - runs this notebook  twice, one time for ingestion date parameter set to '2026-05-25' and one time for '2026-05-26' (to capture late arrival records)
   - `Silver/silver_internal.ipynb` — bootstraps the per-operator ODS tables, normalizes, writes `silver_internal_events`
   
   - `Gold/gold_matching_engine.ipynb` — applies the seven rules, writes `gold_matched_pairs` + `gold_breaks_raw`
   - `Gold/gold_reconciliation.ipynb` — builds `fact_reconciliation_break` and `reconciliation_daily`
   - `monthly_close_query.ipynb` — the four close queries from §6 of the brief

Each notebook is self-contained — markdown cells explain intent, code cells implement

## The four design decisions worth knowing about

Full reasoning is in `DESIGN.md`; one-line summary of each below.

1. **Idempotency lives in two places.** RAW is immutable (every file preserved); SILVER's MERGE on `(operator_code, partner_txn_id, business_date)` makes resends a no-op. Operator can re-deliver the same file every day without double-counting.

2. **Dynamic discovery handles the operator-suffix problem.** The internal OLTP has `sub_initial_telco_a`, `sub_initial_telco_b`, etc. The silver notebook uses `spark.catalog.listTables()` filtered by prefix and unions them. Adding new operator is "land their CDC table" — no code change.

3. **Matching is a uniform fold.** Each rule has the same shape: `(partner, internal) → (matched_pairs, partner_remaining, internal_remaining, breaks)`. Rules apply in priority order, each one consumes what it can claim, the remainder flows to the next rule. Easy to test, easy to reorder, easy to extend.

4. **Every row carries both `business_date` and `ingestion_date`.** This is the bitemporal property that makes the monthly close work. The same `reconciliation_daily` table answers both "what did Finance publish at close" and "what does this period look like today" — see `Diagrams/bitemporal_restatement.svg`.

## Where to look first

- **Got 5 minutes?** Read `DESIGN.md` §1–3 and look at `Diagrams/architecture_medallion_full.svg`.
- **Got 15 minutes?** Add `DESIGN.md` §4 (the matching decision tree) and `Diagrams/matching_decision_tree.svg`.
- **Want to run it?** Follow the steps above; expect ~10 minutes from clone to first mart row.
- **Want to know what I'd build next?** See `README_future_work.md`.

## Tooling note

I used Claude to accelerate parts of this
submission — generating sample data, drafting the diagrams, and writing
the README and design document.
