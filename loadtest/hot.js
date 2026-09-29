// One hot merchant: every sale credits the same merchant_payable row.
//   loadtest/run.sh hot.js -e RATE=100 -e DURATION=60s
import { createMerchants, quietBank, sale } from './lib.js';

const RATE = Number(__ENV.RATE || 100);

export const options = {
  scenarios: {
    hot: {
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
  return { key: createMerchants(1)[0] };
}

export default function (data) {
  sale(data.key, 1000 + Math.floor(Math.random() * 100000));
}
