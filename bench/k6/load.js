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

function pickTag() {
  const start = Number(ingest.tag_start || 1);
  const count = Number(ingest.tag_count || 1);
  return start + Math.floor(Math.random() * count);
}

function pickMix() {
  const mix = query.mix || [{ op: "range", weight: 1, window: "15m", step: "1m" }];
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
  const match = /^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/.exec(raw || "15m");
  if (!match) {
    return 15 * 60 * 1000;
  }
  const amount = Number(match[1]);
  const unit = match[2];
  const mul = { ms: 1, s: 1000, m: 60000, h: 3600000, d: 86400000 }[unit];
  return amount * mul;
}

export function writeBatch() {
  const now = new Date().toISOString();
  const batchSize = Math.max(Number(ingest.batch) || 1, 1);
  const samples = [];
  for (let i = 0; i < batchSize; i += 1) {
    samples.push({
      ts: now,
      tag_id: pickTag(),
      value: Math.random() * 100,
      quality: 192,
    });
  }
  const res = http.post(`${BASE}/v1/write`, JSON.stringify({ samples }), {
    headers: { "Content-Type": "application/json" },
  });
  writeMs.add(res.timings.duration);
  check(res, { written: (r) => r.status === 200 });
}

export function runQuery() {
  const item = pickMix();
  const tagIds = [pickTag()];
  const to = new Date();
  if (item.op === "locf" || item.op === "latest") {
    const res = http.post(
      `${BASE}/v1/read`,
      JSON.stringify({ mode: "locf", tag_ids: tagIds, at: to.toISOString() }),
      { headers: { "Content-Type": "application/json" } },
    );
    queryMs.add(res.timings.duration);
    check(res, { locf: (r) => r.status === 200 });
    return;
  }
  const from = new Date(to.getTime() - windowMs(item.window));
  const payload = {
    mode: item.op === "query" ? "range" : item.op,
    tag_ids: tagIds,
    from: from.toISOString(),
    to: to.toISOString(),
  };
  if (payload.mode === "sample") {
    payload.step = item.step || "1m";
  }
  const res = http.post(`${BASE}/v1/read`, JSON.stringify(payload), {
    headers: { "Content-Type": "application/json" },
  });
  queryMs.add(res.timings.duration);
  check(res, { queried: (r) => r.status === 200 });
}
