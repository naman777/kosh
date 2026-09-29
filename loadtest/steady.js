// Steady traffic: RATE sales per second (3 requests each) spread over
// MERCHANTS merchants, for DURATION.
//   loadtest/run.sh steady.js -e RATE=100 -e DURATION=60s
import { createMerchants, quietBank, sale } from './lib.js';

const RATE = Number(__ENV.RATE || 100);
const MERCHANTS = Number(__ENV.MERCHANTS || 50);

export const options = {
  scenarios: {
    steady: {
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
  return { keys: createMerchants(MERCHANTS) };
}

export default function (data) {
  sale(data.keys[(__VU + __ITER) % data.keys.length], 1000 + Math.floor(Math.random() * 100000));
}
