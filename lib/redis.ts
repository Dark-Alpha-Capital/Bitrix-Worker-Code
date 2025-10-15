import Redis from "ioredis";

const redisUrl = process.env.REDIS_URL;

let redis: Redis | null = null;

if (!redisUrl) {
  console.warn(
    "REDIS_URL environment variable is not set. Redis-dependent routes will be disabled."
  );
} else {
  redis = new Redis(redisUrl);

  redis.on("error", (err) => {
    console.error("Redis Client Error", err);
  });

  redis.on("connect", () => {
    console.log("Redis client connected");
  });

  redis.on("ready", () => {
    console.log("Redis client ready");
  });

  redis.on("close", () => {
    console.log("Redis client connection closed");
  });

  redis.on("reconnecting", () => {
    console.log("Redis client reconnecting...");
  });

  // Connect to Redis
  redis.connect().catch((err) => {
    console.error("Failed to connect to Redis:", err);
  });
}

export default redis;
