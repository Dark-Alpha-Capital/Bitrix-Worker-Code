import express from "express";
import prismaDB from "./lib/prisma";
import { splitContentIntoChunks } from "./lib/utils";
import { generateObject, generateText } from "ai";
import { openai } from "./lib/ai/available-models";
import { z } from "zod";
import { redisClient } from "./lib/redis";

const app = express();
app.use(express.json());

// --------------------------------------
// 🔹 Zod Schema for Submission Validation
// --------------------------------------
const submissionSchema = z.object({
  id: z.string(),
  brokerage: z.string(),
  firstName: z.string(),
  lastName: z.string(),
  tags: z.array(z.string()),
  email: z.string().email(),
  linkedinUrl: z.string(),
  workPhone: z.string(),
  dealCaption: z.string(),
  revenue: z.number(),
  ebitda: z.number(),
  title: z.string(),
  dealTeaser: z.string().nullable(),
  grossRevenue: z.number().nullable(),
  askingPrice: z.number().nullable(),
  ebitdaMargin: z.number(),
  industry: z.string(),
  dealType: z.string(),
  sourceWebsite: z.string(),
  companyLocation: z.string(),
  createdAt: z.string(),
  updatedAt: z.string(),
  bitrixLink: z.string().nullable(),
  status: z.string(),
  isReviewed: z.boolean(),
  isPublished: z.boolean(),
  seen: z.boolean(),
  bitrixId: z.string().nullable(),
  bitrixCreatedAt: z.string().nullable(),
  userId: z.string(),
  screenerId: z.string(),
  screenerContent: z.string(),
  screenerName: z.string(),
  jobId: z.string(),
});

type Submission = z.infer<typeof submissionSchema>;

interface AIScreeningResult {
  title: string;
  score: number;
  sentiment: "POSITIVE" | "NEGATIVE" | "NEUTRAL";
  explanation: string;
}

// --------------------------------------
// 🔹 Helper: Send notification to WebSocket service with retry logic
// --------------------------------------
async function sendNotification(userId: string, title: string, status: string) {
  const url = `${process.env.WEBSOCKET_URL}/notify`;
  const body = { userId, title, status };

  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      if (!res.ok) throw new Error(`HTTP ${res.status}`);

      console.log(`🔔 Notification sent → ${title} (${status})`);
      return;
    } catch (error) {
      console.error(`❌ Failed to send notification (attempt ${attempt}):`, error);
      if (attempt === 2) {
        console.error("❌ Notification could not be delivered after 2 attempts");
      } else {
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }
  }
}

// --------------------------------------
// 🔹 Generate final summarized AI screening
// --------------------------------------
async function generateFinalSummary(
  combinedSummary: string
): Promise<AIScreeningResult | null> {
  try {
    console.log("Generating final AI screening result...");

    const result = await generateObject({
      model: openai("gpt-4o-mini"),
      prompt: `Combine the following summaries into a single structured summary: ${combinedSummary}`,
      schema: z.object({
        title: z.string(),
        score: z.number(),
        sentiment: z.enum(["POSITIVE", "NEGATIVE", "NEUTRAL"]),
        explanation: z.string(),
      }),
    });

    return result.object;
  } catch (error) {
    console.error("Error generating final summary:", error);
    return null;
  }
}

// --------------------------------------
// 🔹 Save AI screening result in database
// --------------------------------------
async function saveAIScreeningResult(
  submissionId: string,
  result: AIScreeningResult,
  combinedSummary: string
): Promise<void> {
  await prismaDB.aiScreening.create({
    data: {
      dealId: submissionId,
      title: result.title,
      explanation: result.explanation,
      score: result.score,
      sentiment: result.sentiment,
      content: combinedSummary,
    },
  });
}

// --------------------------------------
// 🔹 Process text chunks using OpenAI
// --------------------------------------
async function processContentChunks(
  chunks: string[],
  dealInfo: Submission
): Promise<string[]> {
  const summaries: string[] = [];

  for (let i = 0; i < chunks.length; i++) {
    try {
      const dealContext = {
        title: dealInfo.title,
        brokerage: dealInfo.brokerage,
        dealCaption: dealInfo.dealCaption,
        dealType: dealInfo.dealType,
        ebitda: dealInfo.ebitda,
        ebitdaMargin: dealInfo.ebitdaMargin,
        companyLocation: dealInfo.companyLocation,
        revenue: dealInfo.revenue,
        industry: dealInfo.industry,
      };

      const prompt = `Given this deal context: ${JSON.stringify(
        dealContext
      )}, analyze the following section: ${chunks[i]}`;

      const summary = await generateText({
        system:
          "You are an expert AI Assistant that specializes in deal sourcing, evaluation, and private equity.",
        model: openai("gpt-4o-mini"),
        prompt,
      });

      summaries.push(summary.text);
    } catch (error) {
      console.error(`Error processing chunk ${i + 1}:`, error);
      summaries.push(`[Error processing chunk]`);
    }
  }

  return summaries;
}

// --------------------------------------
// 🔹 Process an incoming submission end-to-end
// --------------------------------------
async function processSubmission(submission: Submission): Promise<void> {
  console.log(`Processing submission: ${submission.jobId}`);

  const redisKey = `deal:${submission.jobId}`;
  await redisClient.hset(redisKey, { status: "Processing" });

  try {
    const chunks = await splitContentIntoChunks(submission.screenerContent);

    if (chunks.length === 0) {
      console.warn("No content chunks generated for submission");
      await redisClient.hset(redisKey, { status: "Done", result: "Failed" });
      await sendNotification(submission.userId, submission.title, "Failed");
      return;
    }

    const summaries = await processContentChunks(chunks, submission);
    const combinedSummary = summaries.join("\n\n=== Next Section ===\n\n");

    const finalResult = await generateFinalSummary(combinedSummary);
    if (!finalResult) {
      console.error("Failed to generate final AI result");
      await redisClient.hset(redisKey, { status: "Done", result: "Failed" });
      await sendNotification(submission.userId, submission.title, "Failed");
      return;
    }

    await saveAIScreeningResult(submission.id, finalResult, combinedSummary);

    await redisClient.hset(redisKey, { status: "Done", result: "Success" });
    await sendNotification(submission.userId, submission.title, "Success");

    console.log(`✅ Submission ${submission.jobId} processed successfully`);
  } catch (err) {
    console.error(`Error processing submission ${submission.jobId}:`, err);
    await redisClient.hset(redisKey, { status: "Done", result: "Failed" });
    await sendNotification(submission.userId, submission.title, "Failed");
  }
}

// --------------------------------------
// 🔹 Pub/Sub handler → Triggered when new job is published
// --------------------------------------
app.post("/", async (req, res) => {
  try {
    const pubsubMessage = req.body.message;
    if (!pubsubMessage) return res.status(400).send("No message provided");

    const dataStr = Buffer.from(pubsubMessage.data, "base64").toString();

    // --------------------------------------
    // 🔹 Parse and validate payload with Zod
    // --------------------------------------
    const parsed = submissionSchema.safeParse(JSON.parse(dataStr));
    if (!parsed.success) {
      console.error("Invalid submission payload", parsed.error);
      return res.status(400).send("Invalid payload");
    }
    const payload: Submission = parsed.data;

    console.log(
      "Received Pub/Sub message:",
      pubsubMessage.attributes?.jobType,
      "→ Job ID:",
      payload.jobId
    );

    await processSubmission(payload);

    res.status(204).send();
  } catch (err) {
    console.error("Error handling Pub/Sub message:", err);
    res.status(500).send("Internal Server Error");
  }
});

// --------------------------------------
// ✅ Test Notification Endpoint
// --------------------------------------
app.post("/notify", async (req, res) => {
  try {
    const { userId, title, status } = req.body;

    if (!userId || !title) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    console.log("📩 Received test /notify request:", req.body);

    // Publish message to Redis so your WebSocket worker receives it
    await redisClient.publish(
      "notifications",
      JSON.stringify({ userId, title, status })
    );

    res.status(200).json({ message: "Notification published to Redis" });
  } catch (err) {
    console.error("Error in /notify:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// --------------------------------------
// 🔹 Health check endpoint
// --------------------------------------
app.get("/health", (_, res) => {
  res.status(200).send("OK");
});

// --------------------------------------
// 🔹 Start Express server
// --------------------------------------
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Worker listening for Pub/Sub events on port ${port}`);
});
