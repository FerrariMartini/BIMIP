export interface HealthResponse {
  status: "ok" | "degraded";
  uptime: number;
}
