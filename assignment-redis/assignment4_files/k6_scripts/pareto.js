import http from 'k6/http';
import { sleep } from 'k6';
import { check } from 'k6';
import { Rate } from 'k6/metrics';


const popularProductsRatio = __ENV.POPULAR_PRODUCTS_RATIO ? parseFloat(__ENV.POPULAR_PRODUCTS_RATIO) : 0.8;
const numberOfPopularProducts = __ENV.NUMBER_OF_POPULAR_PRODUCTS ? parseInt(__ENV.NUMBER_OF_POPULAR_PRODUCTS) : 1000;

const successRate = new Rate('success_rate');
const cacheMissRate = new Rate('cache_miss_rate');
const popularProductCacheMissRate = new Rate('popular_product_cache_miss_rate');
const normalProductCacheMissRate = new Rate('normal_product_cache_miss_rate');

export const options = {
  vus: 10,
  duration: '120s',
};

const popularProductRange = {
  min: 1,
  max: numberOfPopularProducts
};

const normalProductsRange = {
  min: numberOfPopularProducts + 1,
  max: 10000
};

export default function () {

  const usePopularProduct = Math.random() < popularProductsRatio;

  let productId;
  if (usePopularProduct) {
    // Select a random product from the high-traffic list
    productId = Math.floor(Math.random() * (popularProductRange.max - popularProductRange.min + 1)) + popularProductRange.min;
  } else {
    // Generate a random product ID within the defined range
    productId = Math.floor(Math.random() * (normalProductsRange.max - normalProductsRange.min + 1)) + normalProductsRange.min;
  }

  // Make the request
  const response = http.get(`http://backend:80/products/${productId}`);

  let isCacheMiss = response.headers['X-Cache'] === 'MISS'

  cacheMissRate.add(isCacheMiss ? 1 : 0);

  if (usePopularProduct) {
    popularProductCacheMissRate.add(isCacheMiss ? 1 : 0);
  } else {
    normalProductCacheMissRate.add(isCacheMiss ? 1 : 0);
  }

  // Record success/failure metrics
  if (response.status === 200) {
    successRate.add(1);
  } else {
    successRate.add(0);
    console.log(`Request failed for product ${productId}: ${response.status}`);
  }

  // Perform checks
  check(response, {
    'is status 200': (r) => r.status === 200,
    'cache status exists': (r) => r.headers['X-Cache'] !== undefined,
  });

  // Add a small sleep to avoid overwhelming the backend
  sleep(0.01);
}

export function handleSummary(data) {
  return {
    stdout: `Popular product traffic ratio: ${popularProductsRatio}\n` +
           `Number of popular products: ${numberOfPopularProducts}\n` +
           `VUs: ${data.metrics.vus.values.max}\n` +
           `Success Rate: ${data.metrics.success_rate.values.rate}\n` +
           `Cache Miss Rate: ${data.metrics.cache_miss_rate.values.rate} \n` +
           `Popular Product Cache Miss Rate: ${data.metrics.popular_product_cache_miss_rate.values.rate}\n` +
           `Normal Product Cache Miss Rate: ${data.metrics.normal_product_cache_miss_rate.values.rate}\n` +
           `Request Count: ${data.metrics.http_reqs.values.count}\n` +
           `Request Duration Median: ${data.metrics.http_req_duration.values.med}\n` +
           `Request Duration P90: ${data.metrics.http_req_duration.values['p(90)']}\n` +
           `Data Received Rate: ${data.metrics.data_received.values.rate}\n` +
           `Data Received Count: ${data.metrics.data_received.values.count}\n` +
           `Data Sent Rate: ${data.metrics.data_sent.values.rate}\n` +
           `Data Sent Count: ${data.metrics.data_sent.values.count}\n` +
           `\n`
  };
}
