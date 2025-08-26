import { createClient } from "redis";
import prismaDB from "./lib/prisma";
import { splitContentIntoChunks } from "./lib/utils";
import { generateObject, generateText } from "ai";
import { openai } from "./lib/ai/available-models";
import { z } from "zod";

// Constants
const REDIS_QUEUE_NAME = "dealListings";
const REDIS_PUBLISH_CHANNEL = "problem_done";
const WORKER_DELAY_MS = 2000;
const DEFAULT_PORT = 8080;

// Types
interface Submission {
  id: string;
  userId: string;
  name: string;
  screenerContent: string;
  brokerage: string;
  firstName: string;
  lastName: string;
  linkedinUrl: string;
  workPhone: string;
  dealCaption: string;
  dealType: string;
  revenue: number;
  ebitda: number;
  ebitdaMargin: number;
  industry: string;
  sourceWebsite: string;
  companyLocation: string;
}

interface AIScreeningResult {
  title: string;
  score: number;
  sentiment: "POSITIVE" | "NEGATIVE" | "NEUTRAL";
  explanation: string;
}

// Redis client
const redisClient = createClient({
  url: process.env.REDIS_URL,
});

redisClient.on("error", (error) => {
  console.error("Redis error:", error);
});

redisClient.on("connect", () => {
  console.log("Redis connected");
});

// HTTP server for health checks
const port = Number(process.env.PORT) || DEFAULT_PORT;
const server = Bun.serve({
  port,
  fetch(req) {
    const url = new URL(req.url);
    if (url.pathname === "/health" || url.pathname === "/") {
      return new Response("OK", { status: 200 });
    }
    return new Response("Not Found", { status: 404 });
  },
});

console.log(`HTTP server listening on port ${server.port}`);

/**
 * Process content chunks and generate summaries
 */
async function processContentChunks(
  chunks: string[],
  dealInfo: any
): Promise<string[]> {
  const summaries: string[] = [];

  for (const chunk of chunks) {
    try {
      const summary = await generateText({
        model: openai("gpt-4o-mini"),
        prompt: `Evaluate this listing ${JSON.stringify(dealInfo)}: ${chunk}`,
      });
      summaries.push(summary.text);
    } catch (error) {
      console.error("Error processing chunk:", error);
      summaries.push(`[Error processing chunk]`);
    }
  }

  return summaries;
}

/**
 * Generate final AI screening result
 */
async function generateFinalSummary(
  combinedSummary: string
): Promise<AIScreeningResult | null> {
  try {
    const result = await generateObject({
      model: openai("gpt-4o-mini"),
      prompt: `Combine the following summaries into a single summary: ${combinedSummary}`,
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

/**
 * Save AI screening result to database
 */
async function saveAIScreeningResult(
  submissionId: string,
  result: AIScreeningResult,
  combinedSummary: string
): Promise<boolean> {
  try {
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
    return true;
  } catch (error) {
    console.error("Error saving AI screening result:", error);
    return false;
  }
}

/**
 * Publish completion notification
 */
async function publishCompletionNotification(
  submission: Submission
): Promise<void> {
  try {
    await redisClient.publish(
      REDIS_PUBLISH_CHANNEL,
      JSON.stringify({
        userId: submission.userId,
        productId: submission.id,
        status: "done",
        productName: submission.name,
      })
    );
  } catch (error) {
    console.error("Error publishing notification:", error);
  }
}

/**
 * Process a single submission
 */
async function processSubmission(submission: Submission): Promise<boolean> {
  try {
    console.log(`Processing submission: ${submission.id}`);

    // Split content into chunks
    const chunks = await splitContentIntoChunks(submission.screenerContent);
    if (chunks.length === 0) {
      console.warn("No content chunks generated");
      return false;
    }

    // Process chunks
    const summaries = await processContentChunks(chunks, submission);
    const combinedSummary = summaries.join("\n\n=== Next Section ===\n\n");

    // Generate final result
    const finalResult = await generateFinalSummary(combinedSummary);
    if (!finalResult) {
      console.error("Failed to generate final summary");
      return false;
    }

    // Save to database
    const saveSuccess = await saveAIScreeningResult(
      submission.id,
      finalResult,
      combinedSummary
    );

    if (saveSuccess) {
      await publishCompletionNotification(submission);
      console.log(`Submission ${submission.id} processed successfully`);
      return true;
    }

    return false;
  } catch (error) {
    console.error(`Error processing submission ${submission.id}:`, error);
    return false;
  }
}

/**
 * Main worker function
 */
async function startWorker(): Promise<void> {
  console.log("Starting worker...");

  // Connect to Redis
  try {
    await redisClient.connect();
    console.log("Connected to Redis");
  } catch (error) {
    console.error("Failed to connect to Redis:", error);
    return;
  }

  // Main processing loop
  while (true) {
    try {
      // Check if Redis is connected
      if (!redisClient.isOpen) {
        console.log("Redis disconnected, reconnecting...");
        await redisClient.connect();
      }

      // Get submission from queue
      const submission = await redisClient.rPop(REDIS_QUEUE_NAME);

      if (submission) {
        const submissionData: Submission = JSON.parse(submission);
        await processSubmission(submissionData);

        // Small delay between processing
        await new Promise((resolve) => setTimeout(resolve, WORKER_DELAY_MS));
      } else {
        // No submissions, wait a bit before checking again
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    } catch (error) {
      console.error("Error in worker loop:", error);
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
}

// Start the worker
startWorker().catch((error) => {
  console.error("Worker startup error:", error);
});

console.log("Worker started");
