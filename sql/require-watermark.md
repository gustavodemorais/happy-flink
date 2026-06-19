# Window / time join needs watermark

| Field | Value |
|-------|-------|
| **ID** | `require-watermark` |
| **Severity** | high |
| **Requires** | sql |
| **Operators** | WindowAggregate, Join (interval / temporal), Match |
| **Match** | Window TVF / event-time join on a column with no WATERMARK defined |

## Problem

`TUMBLE` / `HOP` / `SESSION` window or event-time join on a column that has no `WATERMARK`. Without watermark the column is not a time attribute - op never fire, or query fail.

## Why it hurts

No watermark = no signal that time advance. Window never close, no output. Pending state grow while waiting. Look like job hung.

## How to fix

Define `WATERMARK FOR ts AS ...` on the source table. Keep out-of-orderness interval smaller than the window size, else windows never emit.

- add watermark in `CREATE TABLE` or via `ALTER TABLE ... ADD WATERMARK`
- lateness interval < window size
- see [watermark-lag](../runtime/watermark-lag.md) for idle-partition stalls at runtime

## Example

Before - no watermark, window never fire:

```sql
CREATE TABLE clicks (
  user_id BIGINT,
  click_time TIMESTAMP(3)
) WITH ('connector' = 'kafka', ...);
```

After - watermark defined, click_time is a time attribute:

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
