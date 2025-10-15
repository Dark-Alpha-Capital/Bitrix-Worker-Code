import express from "express";
import screenDealRouter from "./routes/screen-deal";
import fileUploadRouter from "./routes/file-upload";
import redis from "./lib/redis";

const app = express();

// Middleware
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));

// Health check endpoint with Redis status
app.get("/", (req, res) => {
  console.log("Root check");
  res.send("OK");
});

app.get("/health", async (req, res) => {
  console.log("Health check");

  const health = {
    status: "OK",
    timestamp: new Date().toISOString(),
    redis: redis ? "connected" : "not configured",
    uptime: process.uptime(),
  };

  res.json(health);
});

// Mount route modules
app.use(screenDealRouter);
app.use(fileUploadRouter);

// Error handling middleware
app.use(
  (
    err: Error,
    req: express.Request,
    res: express.Response,
    next: express.NextFunction
  ) => {
    console.error("Unhandled error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
);

// 404 handler
app.use((req, res) => {
  res.status(404).json({ error: "Not found" });
});

const PORT = Number(process.env.PORT) || 8080;
const HOST = process.env.HOST || "0.0.0.0";

const server = app.listen(PORT, HOST, () => {
  console.log(`Worker HTTP server listening on ${HOST}:${PORT}`);
});

// Graceful shutdown for Google Cloud Run
const gracefulShutdown = async (signal: string) => {
  console.log(`Received ${signal}, starting graceful shutdown...`);

  server.close(async () => {
    console.log("HTTP server closed");

    // Close Redis connection if it exists
    if (redis) {
      try {
        await redis.quit();
        console.log("Redis connection closed");
      } catch (error) {
        console.error("Error closing Redis connection:", error);
      }
    }

    console.log("Graceful shutdown complete");
    process.exit(0);
  });

  // Force close after 30 seconds
  setTimeout(() => {
    console.error("Forced shutdown after timeout");
    process.exit(1);
  }, 30000);
};

// Handle termination signals
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

// Handle uncaught exceptions
process.on("uncaughtException", (error) => {
  console.error("Uncaught Exception:", error);
  gracefulShutdown("uncaughtException");
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("Unhandled Rejection at:", promise, "reason:", reason);
  gracefulShutdown("unhandledRejection");
});
