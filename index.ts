import express from "express";
import screenDealRouter from "./routes/screen-deal";
import fileUploadRouter from "./routes/file-upload";

const app = express();
app.use(express.json());

app.get("/health", (req, res) => {
  console.log("Health check");

  res.send("OK");
});

// Mount route modules
app.use(screenDealRouter);
app.use(fileUploadRouter);

app.listen(8080, () => {
  console.log("Worker HTTP server listening for Pub/Sub events on :8080");
});
