import express, { type Express } from "express";
import type { HealthResponse } from "../schema/app_health";

export function createApiServer(): Express {
  const app = express();

  app.get("/health", (_req, res) => {
    const body: HealthResponse = {
      status: "ok",
      uptime: process.uptime(),
    };
    res.json(body);
  });

  return app;
}
