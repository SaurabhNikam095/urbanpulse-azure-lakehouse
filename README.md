# UrbanPulse on Azure
![CI](https://github.com/SaurabhNikam095/urbanpulse-azure-lakehouse/actions/workflows/ci.yml/badge.svg)

Real-time and batch lakehouse platform for ride-hailing analytics, simulated for Pune.

> **Status:** In development, Day 1 of 28. Nothing is deployed yet. This README is updated as each phase is finished and measured.

## What this project is
An end-to-end data engineering project: a streaming and batch Medallion lakehouse (Bronze, Silver, Gold) with data contracts, CDC with SCD Type 2, late-data handling, quarantine, PII masking, backfills, lineage, monitoring, CI/CD and infrastructure as code.

## Planned architecture
Simulators and PostgreSQL CDC -> Event Hubs (Kafka API) -> Bronze (ADLS Gen2, Delta) -> Silver (Databricks Structured Streaming) -> Gold (dbt) -> Power BI.
The diagram will be added to docs/diagrams.

## Planned tech stack
| Area | Tools |
|---|---|
| Streaming | Azure Event Hubs, Schema Registry (Avro), Debezium |
| Storage and processing | ADLS Gen2, Delta Lake, Azure Databricks (PySpark) |
| Modelling | dbt |
| Orchestration | Data Factory, Databricks Workflows |
| Quality and governance | Great Expectations or Soda, dbt tests, lineage |
| Monitoring | Azure Monitor, Log Analytics, Grafana |
| DevOps | Terraform, GitHub Actions (OIDC) |
| BI and ML | Power BI, MLflow |

## Roadmap
| Week | Focus |
|---|---|
| 1 | Repo, simulator, data contracts, CI, local Silver logic, Terraform written |
| 2 | Azure baseline, ingestion to Bronze, CDC |
| 3 | Silver at scale, Gold with dbt, orchestration, backfill |
| 4 | Quality, lineage, monitoring, benchmarks, failure drills, ML and BI, teardown, docs |

## Repository structure
- docs: guide, ADRs, diagrams, evidence, daily log
- infra/terraform: infrastructure as code
- simulators: trip, GPS and payment generators
- schemas: Avro data contracts
- ingestion: Debezium config, batch loaders
- databricks: Bronze and Silver notebooks and jobs
- dbt_project: Gold models and tests
- quality: data-quality suites
- monitoring: dashboards and alerts
- ml: features and surge model
- tests: unit and integration tests
- .github/workflows: CI and deployment

## Results
To be filled with measured numbers only (throughput, latency, cost).

## Author
Saurabh Nikam - GitHub: SaurabhNikam095
