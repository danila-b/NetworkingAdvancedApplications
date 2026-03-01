import http from 'k6/http';

export const options = {
  scenarios: {
    no_graceful_stop: {
      executor: 'constant-vus',
      vus: 10,
      duration: '10s',
      gracefulStop: '0s',
    },
  },
};

export default function () {
  http.get('http://nginx-proxy:80/health');
}
