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
**Next:** Avro data contracts (trips v1 and v2, gps, payments) and a local schema validation script.
