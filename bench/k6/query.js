import http from "k6/http";
import { check } from "k6";
import { Trend } from "k6/metrics";

const BASE = __ENV.BASE_URL || "http://localhost:8081";
const queryLatency = new Trend("prism_query_ms");

export const options = {
  vus: Number(__ENV.VUS || 10),
  duration: __ENV.DURATION || "30s",
};

export default function () {
  const to = new Date();
  const from = new Date(to.getTime() - 15 * 60 * 1000);
  const url =
    `${BASE}/v1/query?metric=cpu.usage` +
    `&from=${encodeURIComponent(from.toISOString())}` +
    `&to=${encodeURIComponent(to.toISOString())}` +
    `&step=1m&agg=avg`;
  const res = http.get(url);
  queryLatency.add(res.timings.duration);
  check(res, { "queried": (r) => r.status === 200 });
}
