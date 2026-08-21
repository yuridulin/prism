import http from "k6/http";
import { check } from "k6";

const BASE = __ENV.BASE_URL || "http://localhost:8081";

export const options = {
  vus: Number(__ENV.VUS || 10),
  duration: __ENV.DURATION || "30s",
};

export default function () {
  const young = new Date();
  const old = new Date(young.getTime() - 15 * 60 * 1000);
  const res = http.post(
    `${BASE}/api/values`,
    JSON.stringify({
      tagsId: [1],
      old: old.toISOString(),
      young: young.toISOString(),
    }),
    { headers: { "Content-Type": "application/json" } },
  );
  check(res, { queried: (r) => r.status === 200 });
}
