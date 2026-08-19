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
    samples: [
      {
        ts: new Date().toISOString(),
        tag_id: 1,
        value: Math.random() * 100,
        quality: 192,
      },
    ],
  });
  const res = http.post(`${BASE}/v1/write`, payload, {
    headers: { "Content-Type": "application/json" },
  });
  check(res, { written: (r) => r.status === 200 });
}
