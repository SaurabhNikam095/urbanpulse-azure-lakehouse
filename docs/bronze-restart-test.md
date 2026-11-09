# Day 11: Bronze restart test

## Question
If the Bronze streaming job is stopped in the middle of a run and started again with the
same checkpoint, does Bronze end up with every source record exactly once?

## Setup
- Job: `databricks/bronze_stream.py` (Structured Streaming, Kafka endpoint of Event Hubs
  to a Bronze Delta table in ADLS, fixed checkpoint path).
- Throttle: `maxOffsetsPerTrigger = 100` with a 10 second trigger, so the batch takes
  about 2.5 minutes and can be stopped in the middle.
- Compute: single-node cluster `urbanpulse-single-node` (Standard_D4ds_v5).
- Source batch: producer with `--trips 100 --ticks 2 --seed 11`, sent once.

## Known numbers
| Item | trips | gps | payments | total |
|---|---|---|---|---|
| Bronze before the test (Day 10 baseline) | 195 | 502 | 48 | 745 |
| Good events sent in the Day 11 batch | 380 | 1,003 | 83 | 1,466 |
| Expected Bronze after the test | 575 | 1,505 | 131 | 2,211 |

The batch also sent 8 records to the dead-letter hub. The job does not read that hub, so
they are not counted.

## Result
| Step | trips | gps | payments | total |
|---|---|---|---|---|
| Stream stopped about 30 seconds after the start | 414 | 1,089 | 93 | 1,596 |
| Stream restarted with the same checkpoint, caught up | 575 | 1,505 | 131 | 2,211 |

After the restart, rows = 2,211 and distinct (topic, partition, offset) = 2,211.

## Conclusion
- No loss: the final count equals the baseline plus every good event sent.
- No duplicates from the restart: every (topic, partition, offset) appears once.
- The stop really happened in the middle of the run: 1,596 rows, between 745 and 2,211.

## Notes and limits
- The source was sent once. Late and duplicate records created by the chaos injection are
  separate messages with their own offsets, so Bronze keeps them raw; Silver removes them.
- The stream was stopped with `stream.stop()`, which is a clean stop. A hard stop (cluster
  terminated while a batch is running) was not tested.
- Event Hubs keeps messages for 1 day, so the test was done in one sitting.

## Evidence
- docs/evidence/day-11/01-partial-count-after-stop.png
- docs/evidence/day-11/02-final-counts-after-restart.png
- docs/evidence/day-11/03-no-duplicates-check.png
