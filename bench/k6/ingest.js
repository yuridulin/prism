import http from "k6/http";
import { check } from "k6";
import { Trend } from "k6/metrics";

const BASE = __ENV.BASE_URL || "http://localhost:8081";
const writeLatency = new Trend("prism_write_ms");

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
  const now = new Date().toISOString();
  const host = `dev-${Math.floor(Math.random() * 50)
    .toString()
    .padStart(3, "0")}`;
  const payload = JSON.stringify({
    points: [
      {
        ts: now,
        metric: "cpu.usage",
        value: Math.random() * 100,
        labels: { host, site: "lab" },
      },
    ],
  });
  const res = http.post(`${BASE}/v1/points`, payload, {
    headers: { "Content-Type": "application/json" },
  });
  writeLatency.add(res.timings.duration);
  check(res, { "written": (r) => r.status === 204 });
}
