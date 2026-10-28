# Daily Log

## Day 1 - 26 Oct 2026
**Built:** Azure budget with cost alerts, GitHub repo, folder structure, .gitignore, .env.example, README skeleton.
**Broke:** Git commit email was still a placeholder; a multi-line PowerShell paste did not run.
**Learned:** Set the commit email to the GitHub noreply address; verify every command's output before moving on.
**Evidence:** docs/evidence/day-01/01-azure-budget-alerts.png
**Also done:** Python 3.11 venv, ruff, pytest, pre-commit hooks with gitleaks secret scanning.

## Day 2 - 27 Oct 2026
**Built:** ADR-001 (Event Hubs Standard with 2 throughput units, Debezium topic rerouting, Central India region) and a seeded trip simulator with lifecycle events and 16 unit tests.
**Broke:** The new files had no final newline, so the end-of-file hook blocked the first commit; re-staged and committed again.
**Learned:** Event Hubs Standard limits (10 event hubs per namespace, 25 schema versions); testing rules that must always hold over 1,000 random seeded trips.
**Evidence:** docs/adr/001-azure-stack.md and tests/test_trip_simulator.py (16 passed).


## Day 3 - 27 Oct 2026
**Built:** GPS ping simulator (500 drivers, 5-second pings), payment simulator (one per completed trip, synthetic PII phone numbers) and chaos injection (about 2% duplicates, 3% late events, 1% bad records) with a manifest; 30 new tests, 46 in total.
**Broke:** Downloaded files again had no final newline, so ruff format flagged them; fixed with ruff format.
**Learned:** Fault injection with a manifest to prove pipeline quality later; seeded generators make random rates testable; .gitattributes keeps line endings consistent.
**Evidence:** simulators/chaos.py and tests/test_chaos.py (46 passed).

## Day 4 - 28 Oct 2026
**Built:** Avro data contracts in schemas/ (trips v1 and v2 with a nullable surge_multiplier defaulting to 1.0, gps v1, payments v1), a validation script (scripts/validate_schemas.py) that checks the schemas parse, v2 reads v1 data and simulator output matches the schemas, and a GitHub Actions CI workflow that runs ruff, pytest and the schema checks.
**Broke:** One command failed locally and passed on re-run; repeated the test run to check for a flaky test.
**Learned:** Adding a field with a default keeps old and new data compatible; negative tests prove bad records are rejected; CI verifies every push outside my laptop.
**Evidence:** docs/evidence/day-04/01-github-actions-ci-green.png (CI #1 green, commit f6e4983).

## Day 5 - 28 Oct 2026
**Built:** Bronze parser (ingestion/bronze.py) with Avro schema checks plus value-range rules, ingestion metadata on good records and quarantine with a reason for bad ones; Silver logic (ingestion/silver.py) with dedupe on natural keys, a 300 second watermark and a late_arrivals table; ADR-002 for the late-data policy; 43 new tests, 99 in total.
**Broke:** A new file created in VS Code came with a folder of the same name; checked the layout with Get-ChildItem before committing.
**Learned:** Counting every input record into exactly one output (silver, late or duplicate) proves nothing is lost; schema checks and business-rule checks catch different bad records; a watermark decides what counts as too late.
**Evidence:** docs/adr/002-late-data-policy.md, tests/test_bronze.py and tests/test_silver.py (99 passed), docs/evidence/day-05/01-github-actions-ci-green (2).png (CI green).
