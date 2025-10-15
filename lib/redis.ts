import Redis from "ioredis";

const redisUrl = process.env.REDIS_URL;

if (!redisUrl) {
  throw new Error("REDIS_URL environment variable is not set.");
}

const redis = new Redis(redisUrl);

redis.on("error", (err) => {
  console.error("Redis Client Error", err);
});

export default redis;
