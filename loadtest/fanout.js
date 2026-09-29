// Webhook fan-out: MERCHANTS merchants with webhook URLs, each sale producing
// three events (payment created, authorized, captured). Delivery latency is
// read from Prometheus afterwards:
//   histogram_quantile(0.99, sum by (le) (rate(kosh_webhook_delivery_seconds_bucket[2m])))
// Needs the relay, the dispatcher and samplemerchant (its /sink accepts anything).
//   loadtest/run.sh fanout.js -e RATE=50 -e DURATION=60s
import { createMerchants, quietBank, sale } from './lib.js';

const RATE = Number(__ENV.RATE || 50);
const MERCHANTS = Number(__ENV.MERCHANTS || 20);
const SINK = __ENV.SINK_URL || 'http://localhost:8092/sink';

export const options = {
  scenarios: {
    fanout: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: Math.max(50, RATE),
      maxVUs: RATE * 4,
    },
  },
  thresholds: { http_req_failed: ['rate<0.01'] },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  quietBank();
  return { keys: createMerchants(MERCHANTS, SINK) };
}

export default function (data) {
  sale(data.keys[(__VU + __ITER) % data.keys.length], 1000 + Math.floor(Math.random() * 100000));
}
