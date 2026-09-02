# ADR 0002: Transactional outbox with a polling relay, not CDC

**Status:** accepted, 2026-10-17

## Context

Every state change must reach Kafka (and then merchants). Writing to Postgres and then to Kafka from the request path is a dual write: a crash between the two leaves them disagreeing forever. The two standard fixes are a transactional outbox with a relay, or change data capture (Debezium reading the WAL).

## Decision

State changes insert an `outbox` row in their own transaction. A separate `relay` process polls unpublished rows with `FOR UPDATE SKIP LOCKED`, produces them to Redpanda with `acks=all`, and marks them published.

## Why

- **No extra infrastructure.** CDC means running Kafka Connect and Debezium, managing replication slots, and handling slot bloat when the connector is down. The relay is about 100 lines of Go.
- **The event is an explicit contract.** The outbox row holds exactly the payload merchants see, versioned per aggregate. CDC would publish raw table changes that need reshaping downstream.
- **Testable failure points.** `relay.after_publish` is a named crashpoint, and the kill test proves the duplicate window is harmless.

## Consequences

- **Latency is set by the poll interval** (50 ms idle sleep), where CDC would be near-instant. That is fine for webhooks.
- **At-least-once.** A crash after the Kafka ack but before `published_at` republishes the batch. Consumers dedupe: the dispatcher with `ON CONFLICT`, merchants by event id.
- **Ordering across relays is per batch.** `SKIP LOCKED` lets several relays work at once, but their batches can interleave. Each envelope carries the aggregate `version` so receivers can order or drop stale events.
- The outbox table grows without bound. A retention job that deletes published rows older than N days is needed before production.
