# State size growth

| Field | Value |
|-------|-------|
| **ID** | `state-size-growth` |
| **Severity** | high |
| **Requires** | runtime |
| **Operators** | Join, GroupAggregate, Deduplicate, Rank, any stateful operator |
| **Match** | Checkpoint / state size of a stateful op grow without ceiling over time |

## Problem

> Stub. Fill later.

Stateful op state climb every checkpoint, no plateau. Keys keep added, none removed.

## Why it hurts

> Stub. Fill later.

## How to fix

> Stub. Fill later.

## Example

> Stub. Fill later.
