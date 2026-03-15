import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

// Custom metrics
const cacheMissRate = new Rate('cache_miss_rate');
const cacheMissLatency = new Trend('cache_miss_latency', true);
const cacheHitLatency = new Trend('cache_hit_latency', true);

const testEndpoint = __ENV.ENDPOINT;


export const options = {
  vus: 10,
  duration: '120s',
};

export default function () {

  if (!testEndpoint) {
    console.error('ENDPOINT environment variable is not set');
    return;
  }


  let randomProductId = Math.floor(Math.random() * 10000) + 1;
  const response = http.get(`http://backend:80/${testEndpoint}/${randomProductId}`);


  let isCacheMiss = response.headers['X-Cache'] === 'MISS'
  cacheMissRate.add(isCacheMiss? 1 : 0);
  if (isCacheMiss) {
    cacheMissLatency.add(response.timings.duration);
  } else {
    cacheHitLatency.add(response.timings.duration);
  }

  // Verify response
  check(response, {
    'is status 200': (r) => r.status === 200,
    'cache status exists': (r) => r.headers['X-Cache'] !== undefined,
  });

  sleep(0.01);

}

function formatMetricsReportProductsFromDb(data) {
  return `VUs: ${data.metrics.vus.values.max}\n` +
    `Data Received Rate: ${data.metrics.data_received.values.rate}\n` +
    `Data Received Count: ${data.metrics.data_received.values.count}\n` +
    `Request Count: ${data.metrics.http_reqs.values.count}\n` +
    `Request Duration Median: ${data.metrics.http_req_duration.values.med}\n` +
    `Request Duration P90: ${data.metrics.http_req_duration.values['p(90)']}\n` +
    `Request Duration P95: ${data.metrics.http_req_duration.values['p(95)']}\n` +
    `\n`;
}

function formatMetricsReportProductsFromCache(data) {
  return `VUs: ${data.metrics.vus.values.max}\n` +
    `Cache Miss Rate: ${data.metrics.cache_miss_rate.values.rate} \n` +
    `Data Received Rate: ${data.metrics.data_received.values.rate}\n` +
    `Data Received Count: ${data.metrics.data_received.values.count}\n` +
    `Request Count: ${data.metrics.http_reqs.values.count}\n` +
    `Request Duration Median: ${data.metrics.http_req_duration.values.med}\n` +
    `Request Duration P90: ${data.metrics.http_req_duration.values['p(90)']}\n` +
    `Request Duration P95: ${data.metrics.http_req_duration.values['p(95)']}\n` +
    `Cache Miss Duration Median: ${data.metrics.cache_miss_latency.values.med}\n` +
    `Cache Miss Duration P90: ${data.metrics.cache_miss_latency.values['p(90)']}\n` +
    `Cache Miss Duration P95: ${data.metrics.cache_miss_latency.values['p(95)']}\n` +
    `Cache Hit Duration Median: ${data.metrics.cache_hit_latency.values.med}\n` +
    `Cache Hit Duration P90: ${data.metrics.cache_hit_latency.values['p(90)']}\n` +
    `Cache Hit Duration P95: ${data.metrics.cache_hit_latency.values['p(95)']}` +
    `\n`;
}

export function handleSummary(data) {
  //console.log(JSON.stringify(data.metrics, null, 2));
  if (testEndpoint === 'products') {
    return {
      stdout: formatMetricsReportProductsFromCache(data)
    };
  } else {
    return {
      stdout: formatMetricsReportProductsFromDb(data)
    };
  }
}