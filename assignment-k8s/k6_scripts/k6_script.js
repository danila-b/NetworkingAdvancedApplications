import http from "k6/http";
import { sleep } from "k6";
import { Counter } from "k6/metrics";

// Create a custom metric to count successful responses
const successfulRequests = new Counter('successful_requests');

export const options = {
  vus: 10,
  duration: '180s',
  noConnectionReuse: true,
};

export default function () {
  let response = http.get("http://localhost:32001/products");

  // Check if status code is 200 and increment counter if successful
  if (response.status === 200) {
    successfulRequests.add(1);
  }

  sleep(0.001);
}

export function handleSummary(data) {
  console.log(JSON.stringify(data.metrics, null, 2));
  //console.log(data.metrics)
  return {
    stdout: `VUs: ${data.metrics.vus.values.max}\n` +
           `Successful Requests: ${data.metrics.successful_requests.values.count}\n` +
           `Request Count: ${data.metrics.http_reqs.values.count}\n` +
           `Request Duration Median: ${data.metrics.http_req_duration.values.med}\n` +
           `Request Duration P90: ${data.metrics.http_req_duration.values['p(90)']}\n` +
           `\n`
  };
}