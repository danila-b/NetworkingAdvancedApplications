import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

// Custom metrics
const cacheMissRate = new Rate('cache_miss_rate');
const normalExecutionTime = new Trend('normal_execution_time');
const pipelinedExecutionTime = new Trend('pipelined_execution_time');
const speedupFactor = new Trend('speedup_factor');

const testEndpoint = __ENV.ENDPOINT;

export const options = {
  vus: 10,
  duration: '5s',
};

export default function () {
  if (!testEndpoint) {
    console.error('ENDPOINT environment variable is not set');
    return;
  }

  let randomProductId = Math.floor(Math.random() * 10000) + 1;
  const response = http.get(`http://backend:80/reviews/${testEndpoint}/${randomProductId}`);

  // In this scenario - this is only for checking that test is run correctly. Cache Miss rate should be 0.
  let isCacheMiss = response.headers['X-Cache'] === 'MISS'
  cacheMissRate.add(isCacheMiss === 'MISS' ? 1 : 0);

  // Parse response body
  const payload = JSON.parse(response.body);

  // Add execution times and speedup factor to trends
  normalExecutionTime.add(payload.without_pipeline_execution_time_ms);
  pipelinedExecutionTime.add(payload.pipelined_execution_time_ms);
  speedupFactor.add(payload.speedup_factor);

  // Verify response
  check(response, {
    'is status 200': (r) => r.status === 200,
    'cache status exists': (r) => r.headers['X-Cache'] !== undefined,
    'review counts match': (r) => {
      const body = JSON.parse(r.body);
      return body.normal_review_count === body.pipelined_review_count;
    }
  });

  sleep(0.01);
}

function formatMetricsReport(data) {
  return `VUs: ${data.metrics.vus.values.max}\n` +
    `Cache Miss Rate: ${data.metrics.cache_miss_rate.values.rate}\n` +
    `Normal Execution Time Median: ${data.metrics.normal_execution_time.values.med}\n` +
    `Normal Execution Time P90: ${data.metrics.normal_execution_time.values['p(90)']}\n` +
    `Pipelined Execution Time Median: ${data.metrics.pipelined_execution_time.values.med}\n` +
    `Pipelined Execution Time P90: ${data.metrics.pipelined_execution_time.values['p(90)']}\n` +
    `Speedup Factor Median: ${data.metrics.speedup_factor.values.med}\n` +
    `Speedup Factor P90: ${data.metrics.speedup_factor.values['p(90)']}\n` +
    `\n`;
}

export function handleSummary(data) {
  return {
    stdout: formatMetricsReport(data)
  };
}
