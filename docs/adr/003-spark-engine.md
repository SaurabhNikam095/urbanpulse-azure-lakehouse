# ADR-003: Spark engine – Azure Databricks

- **Status:** Accepted
- **Date:** 2026-10-31
- **Author:** Saurabh Nikam
- **Project:** UrbanPulse
- **Closes:** open item in ADR-001 (Spark engine)

## Context
UrbanPulse needs a Spark engine for Bronze/Silver/Gold processing on Delta Lake.
Options: Azure Databricks, or Microsoft Fabric / Synapse Spark. Constraints: Azure
pay-as-you-go, a 28-day build window, and an 8 GB local laptop (so heavy work must
run in the cloud, not locally).

## Decision
Use **Azure Databricks** with Delta Lake on ADLS Gen2.

## Reasons
- Delta Lake is native to Databricks, matching the lakehouse design.
- The repo already assumes it (`databricks/` folder, dbt on Databricks).
- Job clusters can be small and auto-terminate, which keeps cost controllable.
- It is a widely used industry skill, so it suits a portfolio project.

## Consequences
- Cost control is mandatory: small job clusters, short auto-termination, no idle
  all-purpose clusters, and the existing Azure budget alerts stay on.
- Terraform (Day 7) covers the resource group, ADLS, Key Vault and Event Hubs only;
  the Databricks workspace is added later, not applied today.
- Local code stays plain Python (pytest), so the laptop never runs Spark.

## Alternatives considered
- **Fabric / Synapse Spark:** tighter Microsoft integration, but a different
  ecosystem from the repo's Delta/Databricks/dbt design, so more rework.
