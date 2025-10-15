import express from "express";
import screenDealRouter from "./routes/screen-deal";
import fileUploadRouter from "./routes/file-upload";

const app = express();
app.use(express.json());

app.get("/", (req, res) => {
  console.log("Root check");
  res.send("OK");
});

app.get("/health", (req, res) => {
  console.log("Health check");

  res.send("OK");
});

// Mount route modules
app.use(screenDealRouter);
app.use(fileUploadRouter);

const PORT = Number(process.env.PORT) || 8080;
const HOST = process.env.HOST || "0.0.0.0";
app.listen(PORT, HOST, () => {
  console.log(
    `Worker HTTP server listening for Pub/Sub events on ${HOST}:${PORT}`
  );
});
