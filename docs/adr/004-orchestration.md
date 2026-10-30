# ADR-004: Orchestration – Azure Data Factory

- **Status:** Accepted
- **Date:** 2026-10-31
- **Author:** Saurabh Nikam
- **Project:** UrbanPulse
- **Closes:** open item in ADR-001 (orchestration)

## Context
The pipeline needs scheduling and dependency control across ingestion, Databricks
jobs and quality checks. Options: Azure Data Factory, or Apache Airflow. The laptop
has 8 GB RAM and the budget is pay-as-you-go.

## Decision
Use **Azure Data Factory** to orchestrate the pipeline, calling Databricks jobs.

## Reasons
- Fully managed: nothing to host, nothing heavy on the laptop.
- Native connectors for ADLS, Event Hubs and Databricks.
- Billing is per activity run, which is low for a project of this size.

## Consequences
- Pipelines are defined as ADF JSON/Terraform, not Python DAGs.
- Day 7 does not create ADF; it is added after the core modules.
- Secrets are read from Key Vault, not stored in pipeline definitions.

## Alternatives considered
- **Airflow:** flexible and Python-based, but needs Docker locally (heavy on
  8 GB) or a paid host in Azure, which is more setup and cost for this scale.
