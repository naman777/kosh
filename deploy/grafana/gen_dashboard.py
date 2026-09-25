"""Generates dashboards/kosh.json. Run: python deploy/grafana/gen_dashboard.py"""
import json
import os

panels = [
    ("API requests / s by route", 'sum by (route) (rate(kosh_http_request_seconds_count[1m]))', "reqps", "{{route}}"),
    ("API p99 latency by route", 'histogram_quantile(0.99, sum by (le, route) (rate(kosh_http_request_seconds_bucket[1m])))', "s", "{{route}}"),
    ("API 5xx / s", 'sum(rate(kosh_http_request_seconds_count{code=~"5.."}[1m]))', "reqps", "5xx"),
    ("Ledger lock wait (p50, p99)", [
        ('histogram_quantile(0.5, sum by (le) (rate(kosh_ledger_lock_wait_seconds_bucket[1m])))', "p50"),
        ('histogram_quantile(0.99, sum by (le) (rate(kosh_ledger_lock_wait_seconds_bucket[1m])))', "p99"),
    ], "s", None),
    ("Ledger post p99", 'histogram_quantile(0.99, sum by (le, job) (rate(kosh_ledger_post_seconds_bucket[1m])))', "s", "{{job}}"),
    ("Bank call p99 by operation", 'histogram_quantile(0.99, sum by (le, op) (rate(kosh_bank_call_seconds_bucket[1m])))', "s", "{{op}}"),
    ("Bank outcomes / s", 'sum by (outcome) (rate(kosh_bank_call_seconds_count[1m]))', "ops", "{{outcome}}"),
    ("Outbox: unpublished rows", 'kosh_outbox_unpublished', "short", "unpublished"),
    ("Outbox: oldest unpublished (s)", 'kosh_outbox_oldest_seconds', "s", "oldest"),
    ("Webhook backlog", 'kosh_webhook_backlog', "short", "undelivered"),
    ("Webhook delivery p99 (event → 2xx)", 'histogram_quantile(0.99, sum by (le) (rate(kosh_webhook_delivery_seconds_bucket[5m])))', "s", "p99"),
    ("Webhook attempts / s", 'sum by (result) (rate(kosh_webhook_attempts_total[1m]))', "ops", "{{result}}"),
    ("Payments by state", 'kosh_payments', "short", "{{state}}"),
    ("Worker resolutions / s", 'sum by (kind) (rate(kosh_poll_resolved_total[1m]))', "ops", "{{kind}}"),
    ("Open recon exceptions", 'kosh_recon_open_exceptions', "short", "open"),
]

out = []
for i, (title, expr, unit, legend) in enumerate(panels):
    exprs = expr if isinstance(expr, list) else [(expr, legend)]
    out.append({
        "id": i + 1,
        "type": "timeseries",
        "title": title,
        "datasource": {"type": "prometheus", "uid": "prometheus"},
        "gridPos": {"x": (i % 3) * 8, "y": (i // 3) * 8, "w": 8, "h": 8},
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}},
        "targets": [{"refId": chr(65 + j), "expr": e, "legendFormat": l or ""} for j, (e, l) in enumerate(exprs)],
    })

dash = {
    "uid": "kosh",
    "title": "Kosh",
    "timezone": "browser",
    "refresh": "5s",
    "time": {"from": "now-15m", "to": "now"},
    "schemaVersion": 39,
    "panels": out,
}
here = os.path.dirname(os.path.abspath(__file__))
os.makedirs(os.path.join(here, "dashboards"), exist_ok=True)
with open(os.path.join(here, "dashboards", "kosh.json"), "w", encoding="utf-8", newline="\n") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print("wrote", len(out), "panels")
