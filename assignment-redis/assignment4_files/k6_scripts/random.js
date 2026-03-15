import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

// Custom metrics
const cacheMissRate = new Rate('cache_miss_rate');

export const options = {
  vus: 10,
  duration: '120s',
};

export default function () {

    let randomProductId = Math.floor(Math.random() * 10000) + 1;
    const response = http.get(`http://backend:80/products/${randomProductId}`);

    cacheMissRate.add(response.headers['X-Cache'] === 'MISS' ? 1 : 0);

    // Verify response
    check(response, {
      'is status 200': (r) => r.status === 200,
      'cache status exists': (r) => r.headers['X-Cache'] !== undefined,
    });

    sleep(0.01);



}

export function handleSummary(data) {
  //console.log(JSON.stringify(data.metrics, null, 2));
  //console.log(data.metrics)
  return {
    stdout: `VUs: ${data.metrics.vus.values.max}\n` +
           `Cache Miss Rate: ${data.metrics.cache_miss_rate.values.rate} \n` +
           `Data Received Rate: ${data.metrics.data_received.values.rate}\n` +
           `Data Received Count: ${data.metrics.data_received.values.count}\n` +
           `Request Count: ${data.metrics.http_reqs.values.count}\n` +
           `Request Duration Median: ${data.metrics.http_req_duration.values.med}\n` +
           `Request Duration P90: ${data.metrics.http_req_duration.values['p(90)']}\n` +
           `\n`
  };
}