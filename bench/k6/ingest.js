import http from "k6/http";
import { check } from "k6";

const BASE = __ENV.BASE_URL || "http://localhost:8081";

export const options = {
  scenarios: {
    ingest: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.RATE || 200),
      timeUnit: "1s",
      duration: __ENV.DURATION || "30s",
      preAllocatedVUs: 20,
      maxVUs: 100,
    },
  },
};

export default function () {
  const payload = JSON.stringify({
    points: [
      {
        ts: new Date().toISOString(),
        metric: "cpu.usage",
        value: Math.random() * 100,
        labels: { host: "dev-001", site: "lab" },
      },
    ],
  });
  const res = http.post(`${BASE}/v1/points`, payload, {
    headers: { "Content-Type": "application/json" },
  });
  check(res, { written: (r) => r.status === 200 });
}
