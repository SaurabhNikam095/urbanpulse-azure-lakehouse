# ADR-001: Azure messaging tier, CDC approach and region

- **Status:** Accepted (messaging, CDC, region, Spark engine and orchestration). Spark engine is decided in ADR-003 and orchestration in ADR-004.
- **Date:** 27 October 2026
- **Author:** Saurabh Nikam
- **Project:** UrbanPulse on Azure

## Context

UrbanPulse needs a Kafka-compatible stream for trips, GPS and payments, schema contracts for those events, and change data capture (CDC) from PostgreSQL. The project runs inside a 28-day Azure window on a pay-as-you-go subscription, so cost and quota limits shape the design as much as features do.

Findings from Microsoft documentation (checked 27 October 2026):

- The Basic tier has no Kafka support. The Kafka endpoint and Schema Registry start at the Standard tier.
- Standard limits: 10 event hubs per namespace, 7 days maximum retention, a 25 MB schema registry with one schema group and 25 schema versions, and compacted event hubs up to 1 GB per partition.
- One throughput unit (TU) allows 1 MB/s or 1,000 events/s of ingress and 2 MB/s or 4,096 events/s of egress, and is billed hourly with a one-hour minimum.
- Microsoft documents Debezium PostgreSQL CDC into Event Hubs through Kafka Connect. Kafka Connect internal topics must be compacted, and Debezium creates one topic per table unless topics are rerouted.
- Microsoft recommends Premium or Dedicated for full Kafka compatibility, and lists Standard for development and testing.
- Central India (Pune) offers three availability zones, and Azure Databricks is available in the region.

## Decision

| # | Decision |
|---|----------|
| 1 | Use the **Event Hubs Standard** tier with **2 throughput units** and auto-inflate **off** |
| 2 | Use **Debezium topic rerouting** so all CDC tables go to a single event hub |
| 3 | Keep the Schema Registry to **about 20 schema versions or fewer** (hard limit is 25) |
| 4 | Treat **Bronze in ADLS Gen2 as the replay source**, not Event Hubs retention (7 days) |
| 5 | Use **Central India (Pune)** as the primary region |

## Event hub budget (limit: 10 per namespace)

| Event hubs | Count |
|------------|-------|
| trips, gps, payments | 3 |
| dead-letter topic | 1 |
| Kafka Connect internal topics (configs, offsets, status) | 3 |
| rerouted CDC topic | 1 |
| **Total used** | **8 of 10** |

Without rerouting, three CDC tables would use 10 of 10 and leave no spare.

## Consequences

- **Good:** lowest-cost tier that supports Kafka and Schema Registry. 2 TUs cover the 500 to 1,000 events/s target with headroom. Replay does not depend on short Event Hubs retention.
- **Trade-offs:** Standard has limits that may affect some Kafka workloads, so unusual Kafka features may not work. The small schema registry limits how many test schema versions we can register. Rerouting means CDC consumers filter by table inside one topic.
- **Risks:** a VM size or vCPU quota may be unavailable in Central India. Prices are not yet verified.

## Alternatives considered

| Option | Why not chosen |
|--------|----------------|
| Basic tier | No Kafka endpoint |
| Premium tier | Fuller Kafka support, but higher cost than needed for a 28-day budget |
| Self-hosted Kafka on a VM | More operations work and cost, and less Azure-native |
| South India as primary region | Central India has availability zones, and South India is kept as a fallback |

## Still open

- Spark engine: decided in ADR-003 (Azure Databricks)
- Orchestration: decided in ADR-004 (Azure Data Factory, calling Databricks jobs)
- Late-data policy: decided in ADR-002 (store late events, 300 seconds allowed lateness)
- Exact prices from the Azure pricing calculator **[TO CONFIRM]**
- VM size and vCPU quota in Central India, checked before the first Terraform apply

## Sources

- Microsoft Learn: Azure Event Hubs tier comparison (learn.microsoft.com/en-us/azure/event-hubs/compare-tiers)
- Microsoft Learn: Azure Event Hubs quotas and limits (learn.microsoft.com/en-us/azure/event-hubs/event-hubs-quotas)
- Microsoft Learn: Apache Kafka migration guide for Event Hubs
- Microsoft Learn: Integrate Apache Kafka Connect support on Azure Event Hubs with Debezium for Change Data Capture
- Microsoft Learn: Azure Databricks supported regions
