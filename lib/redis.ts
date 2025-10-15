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
}

export default redis;
