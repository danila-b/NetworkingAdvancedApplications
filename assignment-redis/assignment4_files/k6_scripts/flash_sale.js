import http from 'k6/http';
import { sleep } from 'k6';
import { check } from 'k6';
import { SharedArray } from 'k6/data';

import { Rate, Counter, Trend } from 'k6/metrics';

const readSuccessRate = new Rate('read_success_rate');
const writeSuccessRate = new Rate('write_success_rate');
const priceStaleRate = new Rate('price_stale_rate');
const cacheMissRate = new Rate('cache_miss_rate');
// Reader-specific metrics
const priceNotInitializedCount = new Counter("price_not_initialized")
const readerRequests = new Counter('reader_requests');
const readerDuration = new Trend('reader_duration');

const priceUpdateInterval = __ENV.PRICE_UPDATE_INTERVAL ? parseInt(__ENV.PRICE_UPDATE_INTERVAL) : 1;

export let options = {
    scenarios: {
        price_readers: {
            executor: 'constant-vus',
            vus: 10,
            duration: '10s',
            exec: 'readPrices'
        },
        price_updater: {
            executor: 'constant-vus',
            vus: 1,
            duration: '10s',
            exec: 'updatePrices'
        }
    }
};


export function readPrices() {

    const startTime = new Date();

    const productId = '1';
    const response = http.get(`http://backend:80/products/${productId}`);

    readerRequests.add(1);
    readerDuration.add(new Date() - startTime);

    check(response, {
        'status is 200': (r) => r.status === 200,
        'response has price': (r) => r.json().price !== undefined,
    });

    const isSuccess = check(response, {
        'status is 200': (r) => r.status === 200,
    });

    readSuccessRate.add(isSuccess);

    const cacheStatus = response.headers['X-Cache'];
    cacheMissRate.add(cacheStatus === 'MISS');


    const staleStatus = response.headers['X-Stale'];
    if (staleStatus === 'STALE') {
        priceStaleRate.add(true);
    }
    if (staleStatus === 'FRESH') {
        priceStaleRate.add(false);
    }

    sleep(0.1);
}

let currentPrice = 0;

export function updatePrices() {

    const newPrice = currentPrice + 1;
    currentPrice = newPrice;

    const payload = JSON.stringify({
        price: newPrice
    });

    const productId = '1';
    const response = http.put(`http://backend:80/products/${productId}/price`, payload, {
        headers: { 'Content-Type': 'application/json' },
    });

    // Check update success
    const isSuccess = check(response, {
        'status is 200': (r) => r.status === 200,
    });

    writeSuccessRate.add(isSuccess);

    console.log(`Updated price to: ${newPrice}`);
    sleep(priceUpdateInterval);
}

export function handleSummary(data) {
    // Helper function to safely get metric values
    const getMetricValue = (metricPath, subValue) => {
        return data?.metrics?.[metricPath]?.values?.[subValue] ?? 'N/A';
    };

    return {
        stdout: `Update Frequency (Sec): ${priceUpdateInterval}\n` +
               `Read Success Rate: ${getMetricValue('read_success_rate', 'rate')}\n` +
               `Write Success Rate: ${getMetricValue('write_success_rate', 'rate')}\n` +
               `Cache Miss Rate: ${getMetricValue('cache_miss_rate', 'rate')}\n` +
               `Price Staleness Rate: ${getMetricValue('price_stale_rate', 'rate')}\n` +
               `Read Requests Count: ${getMetricValue('reader_requests', 'count')}\n` +
               `Read Request Duration Median: ${getMetricValue('reader_duration', 'med')}\n` +
               `Read Request Duration P90: ${getMetricValue('reader_duration', 'p(90)')}\n` +
               `Read Request Duration P95: ${getMetricValue('reader_duration', 'p(95)')}\n` +
               `Price Not Initialized on Read Count: ${getMetricValue('price_not_initialized', 'count')}\n` +
               `\n`
    };
}
