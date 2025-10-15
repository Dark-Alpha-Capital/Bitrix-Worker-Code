import { Router } from "express";
import type { Request, Response } from "express";
import { screenDealPayloadSchema } from "../lib/schemas/screen-deal-payload-schema";
import redis from "../lib/redis";

const router = Router();

router.post("/screen-deal", async (req: Request, res: Response) => {
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

    const { jobId, dealId, screenerId, userId, jobType } =
      validatedPayload.data;

    console.log(`🚀 Starting job processing:`, {
      jobId,
      dealId,
      screenerId,
      userId,
      jobType,
    });

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

    // Simulate processing time
    const delaySeconds = Math.floor(Math.random() * 11) + 5;
    console.log(`⏱️ Processing job ${jobId} for ${delaySeconds} seconds...`);
    await new Promise((resolve) => setTimeout(resolve, delaySeconds * 1000));

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
      const jobId = req.body.message
        ? JSON.parse(Buffer.from(req.body.message.data, "base64").toString())
            .jobId
        : null;

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
