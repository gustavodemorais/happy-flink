---
title: Window / time join needs watermark
id: require-watermark
impact: HIGH
impactDescription: "Windows never fire, so the job produces no output while pending state keeps growing."
requires: sql
operators: [WindowAggregate, Join, Match]
match: "A window TVF or event-time join uses a column that has no WATERMARK defined."
tags: [watermark, event-time, window, time-attribute]
---

# Window / time join needs watermark

## Problem

A `TUMBLE`, `HOP`, or `SESSION` window, or an event-time join (interval or temporal), uses a column that has no `WATERMARK`. Without a watermark, the column is not a time attribute, so the operator never fires or the query fails.

## Why it hurts

Without a watermark, there is no signal that time is advancing. Windows never close and produce no output. Pending state keeps growing while the operator waits, and the job appears to hang.

## How to fix

Define `WATERMARK FOR ts AS ...` on the source table. Keep the out-of-orderness interval smaller than the window size; otherwise windows never emit.

- Add the watermark in `CREATE TABLE` or with `ALTER TABLE ... ADD WATERMARK`.
- Keep the lateness interval smaller than the window size.
- Idle or sparse source partitions can stall the watermark. Set `table.exec.source.idle-timeout` so idle partitions do not hold it back.

## Example

Before: no watermark, so the window never fires.

```sql
CREATE TABLE clicks (
  user_id BIGINT,
  click_time TIMESTAMP(3)
) WITH ('connector' = 'kafka', ...);
```

After: with a watermark defined, `click_time` is a time attribute.

```sql
CREATE TABLE clicks (
  user_id BIGINT,
  click_time TIMESTAMP(3),
  WATERMARK FOR click_time AS click_time - INTERVAL '5' SECOND
) WITH ('connector' = 'kafka', ...);

SELECT window_start, COUNT(*)
FROM TABLE(TUMBLE(TABLE clicks, DESCRIPTOR(click_time), INTERVAL '1' HOUR))
GROUP BY window_start, window_end;
```
