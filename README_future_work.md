# What I would do with more time

This submission focuses on the four design decisions the brief asks
about most directly: normalization, matching with fallbacks, idempotency,
and bitemporal restatement. The items below extend the pipeline in
directions a production deployment would need but the brief doesn't
require. Each is ordered by impact-per-effort.

## Recursion-aware break categories

The brief lists six matching outcomes in §4 and these are what
`fact_reconciliation_break` records today. The internal schema
gives three OLTP table families (sub_initial, sub_recursion_success,
sub_recursion_failure), and the pipeline routes by `txn_type` so a
partner RECUR_OK joins to recursion_success and a partner RECUR_FAIL
joins to recursion_failure.

The brief stops short of asking for recursion-specific break categories.
With more time I would add two:

- **`silent_recursion_failure`** — partner sent RECUR_FAIL, platform has
  no failure row. The platform still thinks the user is entitled — the
  exact silent-churn signature §1 of the brief mentions. Currently
  surfaces as `missing_on_platform` with `txn_type=RECUR_FAIL`; a
  dedicated category would make Product's silent-churn dashboard a
  single filter rather than a compound one.

- **`phantom_renewal`** — platform recorded a recursion_success that
  the operator has no record of. Revenue leak in the opposite direction.

Both are one rule each in the matching engine plus one column each in
the daily mart's category-count block. The reason to defer rather than
build now: adding categories the description doesn't ask for risks scope
expansion.

A useful metric once these categories exist: **time to recover from
RECUR_FAIL** — median days between a failed renewal attempt and the
next successful one. Operator-side reliability problems often show up
here before they show up anywhere else.

## Configuration-driven matching

The rules are Python functions called in a fixed order with hard-coded
tolerances. In production I would refactor so:

- Rule parameters (amount tolerance, fallback time window, late-arrival
  threshold) live in `dim_operator` rather than in code. Operators with
  known quirks get per-row overrides — telco_c has 15-minute clock skew,
  their fallback window is 15 minutes, not the global 5.
- The rule sequence is itself a config, not a Python list. Changing
  matching order in production is a config PR, not a code review.

Trade-off: indirection makes the code harder to read in isolation. The
win is auditability — when Finance asks "why did matching change last
week?", the answer is a git diff on the config file.

## Data quality: contracts at SILVER, monitoring at GOLD

Two layers, two tools.

**At SILVER** — schema contracts enforced by the build. Today if an
operator adds a column or renames `amt` to `amount_local`, the
normalizer silently produces NULLs in `amount`. I will add data quality checks:

- `amount` is not null and >= 0 for SUB_OK/RECUR_OK rows
- `amount` is not null and < 0 for REFUND rows
- `txn_ts_utc` is within the last 30 days (rejects stale files)
- `partner_account_id` matches the operator's expected format regex

When any assertion fails, the SILVER write aborts and on-call gets a
Teams alert. RAW is unaffected, so reprocessing is a re-run after the
operator fixes their feed.

**At GOLD** — operational monitoring:

- Volume anomalies (telco_b normally sends 200k rows/day; today it sent 5k → page on-call)
- Freshness violations (no new partner file by 6 AM → alert)
- Cross-table invariant drift (`reconciliation_daily.variance` trending up across operators → investigate)

## Auto Loader / structured ingestion

The current design assumes a batch script lands files in RAW. In
production I would replace this with structured ingestion:

- **Late-arriving files are first-class.** A file dropped at 11:55 PM
  for yesterday's date triggers a re-emission flagged with its actual
  arrival time. No need to track "what files have I already processed"
  by hand — the checkpoint state does it.
- **Schema evolution is bookkept per operator.** New columns appear in
  a `_rescued_data` field and a data-quality check surfaces them.

## A full star schema for analytics

The submission ships one fact table (`fact_reconciliation_break`) and
one aggregate (`reconciliation_daily`). Dimensions are referenced
but not built — `dim_operator` is implied by the tunable parameters
discussion above, `dim_user` by the fallback matcher's account lookup,
others not at all. In production I'd build out the full Kimball-style
model.

**Facts.** Beyond `fact_reconciliation_break`, three more are worth
materializing because they answer questions the current tables can't:

- `fact_subscription` — one row per (user, operator, sub_id, valid_from, valid_to). Reads from `sub_initial_*` plus churn events. Lets Product answer "how many active subscriptions did we have on date X" without scanning the recursion tables.
- `fact_recurrence` — one row per renewal attempt, success or fail. Reads from both recursion table families. The base table for time-to-recover-from-RECUR_FAIL and renewal-rate metrics.
- `fact_revenue_daily` — one row per (business_date, operator, plan). Net revenue after refunds. Different grain from `reconciliation_daily` — that's a recon view, this is a revenue view.

**Dimensions.** Five worth building, all SCD2 where the attributes can change:

- `dim_operator` — operator_code, name, country, default_currency, default_timezone, plus the per-operator matching parameters (amount_tolerance, fallback_window_seconds, late_arrival_threshold_days). The config-driven matching idea from earlier reads from here.
- `dim_user` — internal user_id, mapped to partner_account_id per operator. SCD2 because users change phone numbers and wallet accounts.
- `dim_plan` — plan_code per operator, normalized to a canonical plan_id, with price, billing cycle, currency. Lets us answer "monthly vs weekly plan break rate" cleanly.
- `dim_partner_account` — the raw account identifier (msisdn, wallet handle) with its operator and any operator-side status (active, blocked, ported-out). Decouples the matching engine from operator-side state changes.
- `dim_date` — standard calendar dim. Cheap to build, pays for itself the first time Finance asks "by fiscal week".

**How this changes the current build.** The matching engine doesn't change shape — it still produces `gold_matched_pairs` and `gold_breaks_raw`. What changes:

- `fact_reconciliation_break` carries FKs (`operator_key`, `user_key`, `plan_key`, `date_key`) instead of natural keys. Existing queries against `partner_txn_id` and `internal_id` still work; new queries join through the dims.
- The configuration-driven matching refactor (mentioned above) reads tolerances from `dim_operator` rather than module constants.
- `silent_recursion_failure` becomes detectable because the matcher now has a stable user → partner_account_id mapping via `dim_user`, not the hand-built dict the current notebook uses.

## Backfill as a tool, not a manual run

The bitemporal design supports replays, but today triggering one is a
manual notebook run. I would add a backfill CLI:

```
recon backfill --business-date 2026-05-18 --reason "telco_a late file 2026-05-20"
```

It would identify all RAW files for that business_date plus a look-back
window, re-run SILVER and GOLD with `ingestion_date = today`, diff the
new GOLD output against the prior run, and notify Finance via Teams if
any closed month is affected.

Finance doesn't need the *ability* to replay — the schema supports
that. They need the *operational tool* that makes replays safe, audited,
and obvious.

## Performance: cheap wins worth turning on early

At ~3M rows/day across operators the current design is fine on a small
cluster. Two Delta features matter at higher volume and are both
one-liner table properties:

- **filter index on merge join key.** The MERGE join key benefits
  enormously — cuts file-skipping during MERGE dramatically.
- **Z-ORDER on `(operator_code, business_date)`.** The matching engine
  reads slices by these two columns; z-ordering co-locates them physically.

Neither matters for the case study's volume, but both are worth turning
on the first time you scale up.

## Operator scorecard

Beyond Finance's daily mart, Product wants a weekly per-operator view:

- Match rate (matched / partner_txn_count)
- Break rate by category
- Late-arrival rate (% of rows with ingestion_date > business_date)
- Time-to-correction (median lag between business_date and the ingestion that finally produces a match)

One SQL query against `reconciliation_daily` and `fact_break`,
materialized as `gold.operator_scorecard_weekly` and dashboarded.
Gives partner managers something concrete to bring to QBRs.


## A few more ideas that might work if demanded but currently out of scope:

- **Real-time recon.** Daily is what Finance needs. Streaming might add benefit upon a justiied business case else it might be ove-engineering.
- **Cross-operator identity resolution.** A user with both telco_a and
  wallet_x subscriptions appears as two users. Worth doing.
- **ML-based fuzzy matching.** When `partner_txn_id` is missing, the
  composite-key fallback is rule-based and refuses ambiguity. An ML
  matcher would catch more pairs but introduces false positives Finance
  can't audit. Stay rule-based until unmatched volume justifies the
  operational cost.
