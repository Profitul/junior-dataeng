# Junior Data Engineer Challenge

An end-to-end Python + SQL ETL pipeline: Supabase orders ingestion, auditable cleaning, historical RON/EUR rates from Frankfurter, and two analytics marts in SQLite.

## Run it

Requires Python 3.11+. Runtime uses only the standard library.

```bash
python -m src.pipeline
python scripts/inspect_results.py
pip install -r requirements.txt
pytest -q
```

The database is `data/pipeline.db`. `DATABASE_PATH` and `ORDERS_API_URL` can override the defaults.

## Architecture

```text
Supabase REST -> orders_raw -> orders_clean -> customer_spend_eur
                       |             |       -> country_category_revenue
                       -> rejected   -> fx_rates <- Frankfurter
```

`orders_raw` is source-faithful and retains original JSON. Cleaning is idempotent SQL. `orders_rejected` makes every discarded row auditable. FX records both the requested date and the provider's effective date, which matters on weekends and holidays. Marts count only `completed` orders; test/refunded records remain in clean history.

## Issues found and decisions

Profiling 9,268 source rows found:

- 78 exact duplicates: remove only exact copies. Repeated `order_id` values with different SKUs are valid multi-line orders.
- 1,406 Unix timestamps mixed with ISO timestamps: normalize both to UTC datetime text.
- Three malformed SKU forms (`SKUEL001`, `SKU HK 003`, `SKU-FA-O03`): map to canonical SKUs.
- 79 null categories: recover deterministically from canonical SKU family.
- 103 null customer IDs: recover from the consistent `customer<id>@example.com` convention; quarantine anything unrecoverable.
- 167 non-positive quantities, 24 zero prices, and 13 `999999` sentinel prices: quarantine rather than inventing financial facts. In production I would confirm return and pricing rules with the data owner.
- Future FX reference dates: future RON rows remain out of marts until the relevant daily run can fetch a rate. The missing-FX count is a quality metric. EUR rows need no conversion.

Amounts use `qty * unit_price`; RON uses the rate for `fx_reference_date`. Rounding happens after aggregation.

## Monitoring in production

GitHub Actions runs at 05:15 UTC and manually. Production would add an external freshness monitor that alerts when no successful `pipeline_runs` record arrives by SLA; alerts for job failure, rejection rate, row-count drift, null/uniqueness failures, and missing FX; structured logs/metrics in an observability platform; retries followed by Slack/email/paging; atomic table swaps; raw snapshot retention; managed secrets and a runbook. The workflow commits SQLite only for this short exercise—in production it would write to a managed warehouse.

## AI usage

I used OpenAI Codex for scaffolding, profiling queries, SQL review, and test drafts. I retained the layered design, idempotency, quarantine table, audit metrics, and tests. I manually reviewed the consequential choices: preserving order line items, quarantining suspicious financial values, excluding non-completed statuses, and delaying future FX-dependent rows. Generated work was executed against the real data and tested; AI output was treated as a draft, not evidence.

## Repository map

- `src/pipeline.py`: orchestration and API calls
- `sql/`: schema, cleaning, and marts
- `tests/`: cleaning tests
- `.github/workflows/daily_pipeline.yml`: daily refresh

## Submission

1. Push this folder to a GitHub repository.
2. Invite `aqurate-careers` under **Settings > Collaborators**.
3. Set **Actions > General > Workflow permissions** to read/write.
4. Trigger the workflow once, inspect it, then submit the repository URL.

