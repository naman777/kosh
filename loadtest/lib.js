// Shared helpers for Kosh's k6 scenarios.
import http from 'k6/http';
import { check, fail } from 'k6';
import { Trend, Counter } from 'k6/metrics';

export const BASE = __ENV.BASE_URL || 'http://host.docker.internal:8090';
export const BANK = __ENV.BANK_URL || 'http://host.docker.internal:8091';
export const ADMIN = __ENV.ADMIN_TOKEN || 'dev-admin';

export const orderTime = new Trend('kosh_order_ms', true);
export const payTime = new Trend('kosh_pay_ms', true);
export const captureTime = new Trend('kosh_capture_ms', true);
export const saleTime = new Trend('kosh_sale_ms', true);
export const sales = new Counter('kosh_sales');

// A fast, healthy bank, so the numbers measure Kosh and not the simulator.
export function quietBank() {
  const r = http.put(`${BANK}/admin/config`, JSON.stringify({ latency_min_ms: 1, latency_max_ms: 3, hang_for_ms: 35000 }),
    { headers: { 'Content-Type': 'application/json' } });
  if (r.status !== 200) fail(`bank config: ${r.status}`);
}

export function createMerchants(n, webhookBase) {
  const keys = [];
  for (let i = 0; i < n; i++) {
    const body = { name: `load-${Date.now()}-${i}` };
    if (webhookBase) body.webhook_url = `${webhookBase}/${i}`;
    const r = http.post(`${BASE}/admin/merchants`, JSON.stringify(body),
      { headers: { Authorization: `Bearer ${ADMIN}`, 'Content-Type': 'application/json' } });
    if (r.status !== 201) fail(`create merchant: ${r.status} ${r.body}`);
    keys.push(r.json('api_key'));
  }
  return keys;
}

function post(key, path, idem, body, tags) {
  return http.post(`${BASE}${path}`, body ? JSON.stringify(body) : null, {
    headers: { Authorization: `Bearer ${key}`, 'Idempotency-Key': idem, 'Content-Type': 'application/json' },
    tags,
  });
}

// sale is one customer journey: order → payment → capture.
export function sale(key, amount) {
  const id = `${__VU}-${__ITER}-${Date.now()}`;
  const start = Date.now();
  const o = post(key, '/v1/orders', `o-${id}`, { amount }, { step: 'order' });
  orderTime.add(o.timings.duration);
  if (!check(o, { 'order 201': (r) => r.status === 201 })) return;
  const p = post(key, '/v1/payments', `p-${id}`, { order_id: o.json('id'), method: 'upi' }, { step: 'pay' });
  payTime.add(p.timings.duration);
  if (!check(p, { 'payment 201': (r) => r.status === 201 })) return;
  if (p.json('status') !== 'authorized') return;
  const c = post(key, `/v1/payments/${p.json('id')}/capture`, `c-${id}`, null, { step: 'capture' });
  captureTime.add(c.timings.duration);
  if (check(c, { 'capture 200': (r) => r.status === 200 })) {
    saleTime.add(Date.now() - start);
    sales.add(1);
  }
}
