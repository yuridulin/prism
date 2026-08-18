import http from "k6/http";
import { check } from "k6";
import { Trend } from "k6/metrics";

const BASE = __ENV.BASE_URL || "http://localhost:8081";
const profile = JSON.parse(__ENV.PROFILE_JSON || "{}");
const ingest = profile.ingest || {};
const query = profile.query || {};
const writeMs = new Trend("prism_write_ms");
const queryMs = new Trend("prism_query_ms");

function durationOr(seconds, fallback) {
  if (seconds && Number(seconds) > 0) {
    return `${seconds}s`;
  }
  return fallback;
}

const scenarios = {};
if (ingest.enabled && ingest.rate > 0) {
  scenarios.ingest = {
    executor: "constant-arrival-rate",
    rate: Math.max(1, Math.round(Number(ingest.rate) / Math.max(Number(ingest.batch) || 1, 1))),
    timeUnit: "1s",
    duration: durationOr(profile.duration, __ENV.DURATION || "30s"),
    preAllocatedVUs: 20,
    maxVUs: 200,
    exec: "writeBatch",
  };
}
if (query.enabled && query.rate > 0) {
  scenarios.query = {
    executor: "constant-arrival-rate",
    rate: Math.max(1, Math.round(Number(query.rate))),
    timeUnit: "1s",
    duration: durationOr(profile.duration, __ENV.DURATION || "30s"),
    preAllocatedVUs: 10,
    maxVUs: 100,
    exec: "runQuery",
  };
}

export const options = {
  scenarios: Object.keys(scenarios).length ? scenarios : {
    ingest: {
      executor: "constant-arrival-rate",
      rate: 20,
      timeUnit: "1s",
      duration: "30s",
      preAllocatedVUs: 10,
      maxVUs: 50,
      exec: "writeBatch",
    },
  },
};

function pick(values, fallback) {
  if (!values || !values.length) {
    return fallback;
  }
  return values[Math.floor(Math.random() * values.length)];
}

function randomLabels() {
  const labels = {};
  const spec = ingest.labels || {};
  for (const [key, values] of Object.entries(spec)) {
    labels[key] = pick(values, "unknown");
  }
  return labels;
}

function pickMix() {
  const mix = query.mix || [{ op: "query", weight: 1, window: "15m", step: "1m", agg: "avg" }];
  const total = mix.reduce((sum, item) => sum + (item.weight || 1), 0);
  let cursor = Math.random() * total;
  for (const item of mix) {
    cursor -= item.weight || 1;
    if (cursor <= 0) {
      return item;
    }
  }
  return mix[mix.length - 1];
}

function windowMs(raw) {
  const match = /^(\d+(?:\.\d+)?)(ms|s|m|h)$/.exec(raw || "15m");
  if (!match) {
    return 15 * 60 * 1000;
  }
  const amount = Number(match[1]);
  const unit = match[2];
  const mul = { ms: 1, s: 1000, m: 60000, h: 3600000 }[unit];
  return amount * mul;
}

export function writeBatch() {
  const now = new Date().toISOString();
  const batchSize = Math.max(Number(ingest.batch) || 1, 1);
  const metrics = ingest.metrics || ["cpu.usage"];
  const points = [];
  for (let i = 0; i < batchSize; i += 1) {
    points.push({
      ts: now,
      metric: pick(metrics, "cpu.usage"),
      value: Math.random() * 100,
      labels: randomLabels(),
    });
  }
  const res = http.post(`${BASE}/v1/points`, JSON.stringify({ points }), {
    headers: { "Content-Type": "application/json" },
  });
  writeMs.add(res.timings.duration);
  check(res, { written: (r) => r.status === 200 });
}

export function runQuery() {
  const item = pickMix();
  const metric = pick(ingest.metrics || ["cpu.usage"], "cpu.usage");
  const labels = randomLabels();
  if (item.op === "latest") {
    const res = http.post(`${BASE}/v1/latest`, JSON.stringify({ metric, labels }), {
      headers: { "Content-Type": "application/json" },
    });
    queryMs.add(res.timings.duration);
    check(res, { latest: (r) => r.status === 200 || r.status === 404 });
    return;
  }
  const to = new Date();
  const from = new Date(to.getTime() - windowMs(item.window));
  const res = http.post(
    `${BASE}/v1/query`,
    JSON.stringify({
      metric,
      from: from.toISOString(),
      to: to.toISOString(),
      step: item.step || "1m",
      agg: item.agg || "avg",
      labels,
    }),
    { headers: { "Content-Type": "application/json" } },
  );
  queryMs.add(res.timings.duration);
  check(res, { queried: (r) => r.status === 200 });
}
