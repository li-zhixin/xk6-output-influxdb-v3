import { Gauge } from 'k6/metrics';

const smoke = new Gauge('k6_smoke');

export const options = { vus: 1, iterations: 3 };

export default function () {
  smoke.add(__ITER + 1, { source: 'influxdb3-smoke' });
}
