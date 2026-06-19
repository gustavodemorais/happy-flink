# Watermark lag / idle partition

| Field | Value |
|-------|-------|
| **ID** | `watermark-lag` |
| **Severity** | high |
| **Requires** | runtime |
| **Operators** | WindowAggregate, Join (interval / temporal), Match |
| **Match** | Window / time-join never emit; watermark not advancing; one source partition idle |

## Problem

> Stub. Fill later.

Event-time op wait on watermark = min across input partitions. One idle partition hold watermark still. Window never fire.

## Why it hurts

> Stub. Fill later.

## How to fix

> Stub. Fill later.

## Example

> Stub. Fill later.
