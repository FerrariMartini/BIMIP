import { createApiServer } from "./api";
import { startCollector } from "./collector";

const PORT = Number(process.env.PORT) || 3000;
const HOST = process.env.HOST || "0.0.0.0";

async function main(): Promise<void> {
  const app = createApiServer();
  const collector = await startCollector();

  const server = app.listen(PORT, HOST, () => {
    console.log(`API running on http://${HOST}:${PORT}`);
  });

  async function shutdown(signal: string): Promise<void> {
    console.log(`[main] received ${signal}, shutting down`);
    await collector.stop();
    await collector.producer.disconnect();
    server.close(() => {
      process.exit(0);
    });
  }

  process.on("SIGINT", () => {
    void shutdown("SIGINT");
  });

  process.on("SIGTERM", () => {
    void shutdown("SIGTERM");
  });
}

main().catch((error) => {
  console.error("[main] failed to start", error);
  process.exit(1);
});
