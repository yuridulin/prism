import http from "k6/http";
import { check } from "k6";

const BASE = __ENV.BASE_URL || "http://localhost:8081";

export const options = {
  vus: Number(__ENV.VUS || 10),
  duration: __ENV.DURATION || "30s",
};

export default function () {
  const to = new Date();
  const from = new Date(to.getTime() - 15 * 60 * 1000);
  const res = http.post(
    `${BASE}/v1/read`,
    JSON.stringify({
      mode: "range",
      tag_ids: [1],
      from: from.toISOString(),
      to: to.toISOString(),
    }),
    { headers: { "Content-Type": "application/json" } },
  );
  check(res, { queried: (r) => r.status === 200 });
}
