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
