import { createApiServer } from "./api";

const PORT = Number(process.env.PORT) || 3000;
const HOST = process.env.HOST || "0.0.0.0";
const app = createApiServer();

app.listen(PORT, HOST, () => {
  console.log(`API running on http://${HOST}:${PORT}`);
});


