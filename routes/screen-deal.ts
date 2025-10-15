import { Router } from "express";
import type { Request, Response } from "express";
import { screenDealPayloadSchema } from "../lib/schemas/screen-deal-payload-schema";
import redis from "../lib/redis";
import { evaluateDealAndSaveResult } from "../lib/actions/evaluate-deal";

const router = Router();

router.post("/screen-deal", async (req: Request, res: Response) => {
  if (!redis) {
    console.error("Redis not configured");
    return res.status(503).json({ error: "Redis not configured" });
  }

  // Check Redis connection status
  if (redis.status !== "ready") {
    console.error("Redis not ready, status:", redis.status);
    return res.status(503).json({ error: "Redis not ready" });
  }

  let jobId: string | null = null;
  try {
    const pubsubMessage = req.body.message;
    if (!pubsubMessage) return res.status(400).send("no message");

    const dataStr = Buffer.from(pubsubMessage.data, "base64").toString();
    const payload = JSON.parse(dataStr);
    const validatedPayload = screenDealPayloadSchema.safeParse(payload);

    if (!validatedPayload.success) {
      console.error("❌ Invalid payload:", validatedPayload.error);
      return res.status(400).json({ error: "Invalid payload" });
    }

    const {
      jobId: parsedJobId,
      dealId,
      screenerId,
      userId,
      jobType,
    } = validatedPayload.data;
    jobId = parsedJobId;

    console.log(`🚀 Starting job processing:`, {
      jobId,
      dealId,
      screenerId,
      userId,
      jobType,
    });

    // Deduplication: prevent reprocessing Pub/Sub retries or duplicate events
    const messageId: string | undefined = req.body?.message?.messageId;
    const dedupKey = messageId ? `pubsub:processed:${messageId}` : null;
    const jobKey = `job:${jobId}:processed`;

    // If either the message or job has already been processed, skip
    const [hasMessage, hasJob] = await Promise.all([
      dedupKey ? redis.exists(dedupKey) : Promise.resolve(0),
      redis.exists(jobKey),
    ]);

    if ((dedupKey && hasMessage === 1) || hasJob === 1) {
      console.log(
        `⚠️ Duplicate detected for job ${jobId}${
          messageId ? ` or message ${messageId}` : ""
        }`
      );
      return res.status(204).send();
    }

    // Atomically set dedup markers with expirations
    const multi = redis.multi();
    if (dedupKey) multi.set(dedupKey, "1", "EX", 3600); // 1 hour for message id
    multi.set(jobKey, "1", "EX", 86400); // 24 hours for job id
    await multi.exec();

    // Update status to processing
    await redis.hset(`job:${jobId}`, "status", "processing");
    console.log(`📝 Updated job ${jobId} status to processing in Redis`);

    // Publish processing status
    const processingUpdate = JSON.stringify({ jobId, status: "processing" });
    await redis.publish("job-updates", processingUpdate);
    console.log(
      `📡 Published processing update for job ${jobId}:`,
      processingUpdate
    );

    const evaluationResult = await evaluateDealAndSaveResult(
      dealId,
      screenerId
    );

    if (!evaluationResult.success) {
      console.error(
        "❌ Failed to evaluate deal and save result:",
        evaluationResult.message
      );
      // Mark job as failed and publish update
      await redis.hset(`job:${jobId}`, "status", "failed");
      await redis.publish(
        "job-updates",
        JSON.stringify({
          jobId,
          status: "failed",
          error: evaluationResult.message,
        })
      );
      console.log(`📡 Published failure update for job ${jobId}`);
      return res
        .status(500)
        .json({ error: evaluationResult.message || "Evaluation failed" });
    }
    console.log(`⏱️ Processing job ${jobId}...`);
    // Update status to done
    await redis.hset(`job:${jobId}`, "status", "done");
    console.log(`📝 Updated job ${jobId} status to done in Redis`);

    // Publish completion status
    const doneUpdate = JSON.stringify({ jobId, status: "done" });
    await redis.publish("job-updates", doneUpdate);
    console.log(`📡 Published completion update for job ${jobId}:`, doneUpdate);

    console.log(`✅ Job ${jobId} completed successfully`);
    res.status(204).send();
  } catch (error) {
    console.error("❌ /screen-deal error:", error);

    // Try to publish error status if we have jobId
    try {
      if (jobId) {
        await redis.hset(`job:${jobId}`, "status", "failed");
        await redis.publish(
          "job-updates",
          JSON.stringify({ jobId, status: "failed" })
        );
        console.log(`📡 Published error update for job ${jobId}`);
      }
    } catch (publishError) {
      console.error("❌ Failed to publish error status:", publishError);
    }

    return res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
