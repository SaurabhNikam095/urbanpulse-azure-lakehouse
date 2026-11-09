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

## Day 6 - 31 Oct 2026
**Built:** PII hashing (ingestion/pii.py) using HMAC-SHA256 with a salt read from an environment variable, and SCD Type 2 logic (ingestion/scd2.py) for drivers and riders from Debezium-style CDC events; phone and email are hashed before they enter the dimension rows and row_hash is computed over the hashed values; a 13-event sample CDC file; 30 new tests, 129 in total.
**Broke:** CI failed on the first scd2 push because the sample CDC file had never been committed; the staged copy was stale after a hook edit, so I re-staged the file, committed it and CI went green.
**Learned:** Pseudonymization keeps joins working because the same input and salt give the same hash; updated_at is left out of row_hash so a no-change update is ignored; rebuilding each key's timeline makes duplicate and out-of-order events safe.
**Evidence:** tests/test_pii.py and tests/test_scd2.py (129 passed), CI #8 green (commit 60f429a).

## Day 7 - 31 Oct 2026
**Built:** ADR-003 (Azure Databricks as the Spark engine) and ADR-004 (Azure Data Factory for orchestration), closing the two open items in ADR-001; Terraform modules for the resource group, ADLS Gen2, Key Vault and Event Hubs (Standard, 2 throughput units) with a root configuration, written and validated only, with no apply.
**Broke:** A multi-line PowerShell command pasted without its first line left the terminal waiting at the continuation prompt, and the first ADR file had never been saved, so the rename failed; cancelled with Ctrl+C and created both ADR files again with the right names.
**Learned:** An ADR records a decision and its trade-offs; terraform validate checks a configuration without creating anything in Azure; the lock file pins the provider version for everyone, including CI.
**Evidence:** docs/adr/003-spark-engine.md, docs/adr/004-orchestration.md, infra/terraform/, docs/evidence/day-07/01-github-actions-ci-green.png (CI #11 green, commit 0e7d8e1).

## Day 8 - 31 Oct 2026
**Built:** First cloud day: applied the validated Terraform baseline to Azure (Central India) after pre-flight checks (budget alert, resource providers registered, globally unique names, Key Vault soft-delete retention of 7 days with purge protection off). Added the owner and phase tags to every taggable resource, reviewed the plan (10 to add, 0 to change, 0 to destroy) and applied it: resource group, ADLS Gen2 with bronze, silver and gold containers, Key Vault, and an Event Hubs Standard namespace (2 throughput units) with trips, gps and payments event hubs. No Databricks.
**Broke:** The three Azure resource providers were NotRegistered on the new subscription, so they were registered before planning. After the plan, git status showed tfplan as untracked rather than ignored, so a .gitignore rule was added and pushed before any evidence commit. az resource list showed 3 resources instead of 10 because it lists only top-level resources, not event hubs or storage containers; terraform state list confirmed all 10.
**Learned:** terraform plan -out saves the exact plan that apply runs; plan and state files hold sensitive values and must stay git-ignored; child resources (event hubs, containers) do not appear in az resource list; Event Hubs Standard is billed per throughput unit per hour, so the namespace costs money while it exists.
**Evidence:** docs/evidence/day-08/01-resource-group.png, docs/evidence/day-08/02-state-tags-eventhubs.png, infra/terraform/ (tags in main.tf), commit e6fe87f (.gitignore fix).

## Day 9 - 2 Nov 2026
**Built:** Added a dead-letter event hub and an Event Hubs Schema Registry group (urbanpulse, Avro, Backward compatibility) with Terraform (2 added, 0 changed, 0 destroyed). Granted my login the Schema Registry Contributor role on the namespace and registered the trip v1 and v2, gps and payment Avro schemas with scripts/register_schemas.py. Wrote ingestion/producer.py: it sends the simulator streams through the Event Hubs Kafka API as Avro with the registered schema id in a header, and routes records that fail the Bronze checks to the dead-letter topic; it has a dry-run mode and a max-events guard, and 6 new tests bring the total to 135. A test run delivered 3,255 events (trips 564, gps 2,534, payments 129, dead-letter 28) with 0 server errors. Then destroyed only the Event Hubs resources (6 destroyed) and kept the resource group, ADLS Gen2 and Key Vault.
**Broke:** The role name Schema Registry Contributor (Preview) no longer exists; the current name is Schema Registry Contributor. The first dry run at 250 trips and 20 ticks produced 11,386 events and the guard refused to send, so it was resized to 150 trips and 5 ticks. ruff flagged one line over 100 characters. PowerShell split -target=module.event_hubs at the dot, fixed by quoting the argument. The portal showed 6.51k incoming messages, twice the 3,255 of one run, in two bursts about 13 minutes apart, so the producer ran twice; nothing failed, and duplicates like this are what the Silver dedupe is for.
**Learned:** Event Hubs exposes a Kafka endpoint on Standard tier; Schema Registry data-plane actions need their own role that Owner does not include; a dead-letter topic keeps bad records for inspection instead of losing them; a targeted destroy removes only the messaging layer; at-least-once delivery can create duplicates.
**Evidence:** docs/evidence/day-09/01-schema-registry.png, 02-producer-run.png, 03-namespace-overview.png, 04-incoming-by-hub.png (gps 5.07k, trips 1.13k, payments 258, dead-letter 56 across both runs), commit df106e0 (producer), infra/terraform/modules/event_hubs/.


## Day 10 - 9 Nov 2026
**Built:** Streaming to Bronze. Terraform added an Azure Databricks Premium workspace, an access connector with a system-assigned managed identity and a Storage Blob Data Contributor role on the ADLS account (module infra/terraform/modules/databricks), plus a separate root (infra/terraform/databricks_cluster) with a single-node cluster (Standard_D4ds_v5, auto-terminate after 10 minutes, tagged), a Unity Catalog storage credential and an external location for the bronze container, so no keys are used. Re-applied Event Hubs (6 added), then re-registered the 4 Avro schemas. scripts/set_databricks_secret.py stores the Event Hubs connection string in a Databricks secret scope without printing it, and databricks/bronze_stream.py is a Structured Streaming job that reads the Kafka endpoint for trips, gps and payments and appends raw payload, headers, topic, partition, offset and ingest time to a Bronze Delta table with a fixed checkpoint path. A fresh batch of 750 events (trips 195, gps 502, payments 48, dead-letter 5) gave exactly 745 rows in Bronze, matching topic by topic.
**Broke:** The subscription allows only 4 vCPUs in Central India, so the cluster must be a single 4-core node. The first node type (Standard_D4ads_v7) failed with a restrictive-constraint stockout; Standard_D4ds_v5 worked through a node_type_id override. A transient Azure CLI token failure made register_schemas.py register trip v2 before v1, so the schema group was replaced and all four were registered again in order. The producer guard refused two oversized runs (--ticks scales GPS volume linearly, 500 ticks gave 250,000 events); --trips 50 --ticks 1 gave 750. Saved plans went stale and environment variables were lost in new terminals. Event Hubs retention is 1 day, so events sent earlier had expired and a fresh batch was sent.
**Learned:** Cluster quota and VM zone restrictions decide which node types are usable; Unity Catalog with an access connector gives keyless access to ADLS; a secret scope keeps credentials out of code; Structured Streaming checkpoints record processed offsets; a matching row count between source and Bronze proves the chain end to end; -replace resets a resource that cannot be edited in place.
**Evidence:** docs/evidence/day-10/01-bronze-counts.png (gps 502, trips 195, payments 48), docs/evidence/day-10/02-stream-dashboard.png, databricks/bronze_stream.py, scripts/set_databricks_secret.py, infra/terraform/modules/databricks/, infra/terraform/databricks_cluster/.
