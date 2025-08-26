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
  console.log("Redis connected successfully");
});

redisClient.on("ready", () => {
  console.log("Redis client is ready");
});

redisClient.on("end", () => {
  console.log("Redis connection ended");
});

redisClient.on("reconnecting", () => {
  console.log("Redis reconnecting...");
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
  console.log(
    `Processing ${chunks.length} content chunks for deal: ${dealInfo.id}`
  );
  const summaries: string[] = [];

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    if (!chunk) {
      console.warn(`Chunk ${i + 1} is undefined, skipping...`);
      continue;
    }
    try {
      console.log(
        `Processing chunk ${i + 1}/${chunks.length} (${
          chunk.length
        } characters)`
      );
      const summary = await generateText({
        model: openai("gpt-4o-mini"),
        prompt: `Evaluate this listing ${JSON.stringify(dealInfo)}: ${chunk}`,
      });
      summaries.push(summary.text);
      console.log(`Chunk ${i + 1} processed successfully`);
    } catch (error) {
      console.error(`Error processing chunk ${i + 1}:`, error);
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
    console.log("Generating final AI screening result...");
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
    console.log(
      "Final AI screening result generated successfully:",
      result.object
    );
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
    console.log(
      `Saving AI screening result to database for submission: ${submissionId}`
    );
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
    console.log("AI screening result saved successfully to database");
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
    console.log(
      `Publishing completion notification for submission: ${submission.id}`
    );
    const notification = {
      userId: submission.userId,
      productId: submission.id,
      status: "done",
      productName: submission.name,
    };
    console.log("Notification payload:", notification);

    const result = await redisClient.publish(
      REDIS_PUBLISH_CHANNEL,
      JSON.stringify(notification)
    );
    console.log(`Notification published successfully. Subscribers: ${result}`);
  } catch (error) {
    console.error("Error publishing notification:", error);
  }
}

/**
 * Process a single submission
 */
async function processSubmission(submission: Submission): Promise<boolean> {
  try {
    console.log(`=== Starting to process submission: ${submission.id} ===`);
    console.log(`Submission details:`, {
      id: submission.id,
      name: submission.name,
      userId: submission.userId,
      contentLength: submission.screenerContent?.length || 0,
    });

    // Split content into chunks
    console.log("Splitting content into chunks...");
    const chunks = await splitContentIntoChunks(submission.screenerContent);
    console.log(`Content split into ${chunks.length} chunks`);

    if (chunks.length === 0) {
      console.warn("No content chunks generated - submission will be skipped");
      return false;
    }

    // Process chunks
    console.log("Processing content chunks...");
    const summaries = await processContentChunks(chunks, submission);
    const combinedSummary = summaries.join("\n\n=== Next Section ===\n\n");
    console.log(
      `Combined summary length: ${combinedSummary.length} characters`
    );

    // Generate final result
    console.log("Generating final AI screening result...");
    const finalResult = await generateFinalSummary(combinedSummary);
    if (!finalResult) {
      console.error(
        "Failed to generate final summary - submission processing failed"
      );
      return false;
    }

    // Save to database
    console.log("Saving result to database...");
    const saveSuccess = await saveAIScreeningResult(
      submission.id,
      finalResult,
      combinedSummary
    );

    if (saveSuccess) {
      console.log(
        "Database save successful, publishing completion notification..."
      );
      await publishCompletionNotification(submission);
      console.log(`=== Submission ${submission.id} processed successfully ===`);
      return true;
    } else {
      console.error("Database save failed - submission processing incomplete");
      return false;
    }
  } catch (error) {
    console.error(`=== Error processing submission ${submission.id}:`, error);
    return false;
  }
}

/**
 * Check Redis queue status
 */
async function checkQueueStatus(): Promise<void> {
  try {
    const queueLength = await redisClient.lLen(REDIS_QUEUE_NAME);
    console.log(
      `Current queue length for '${REDIS_QUEUE_NAME}': ${queueLength}`
    );

    if (queueLength > 0) {
      const firstItem = await redisClient.lIndex(REDIS_QUEUE_NAME, 0);
      console.log(
        "First item in queue (preview):",
        firstItem ? firstItem.substring(0, 100) + "..." : "None"
      );
    }
  } catch (error) {
    console.error("Error checking queue status:", error);
  }
}

/**
 * Main worker function
 */
async function startWorker(): Promise<void> {
  console.log("=== Starting worker... ===");
  console.log("Environment variables:", {
    REDIS_URL: process.env.REDIS_URL
      ? `${process.env.REDIS_URL.substring(0, 20)}...`
      : "NOT SET",
    PORT: process.env.PORT || DEFAULT_PORT,
    NODE_ENV: process.env.NODE_ENV || "NOT SET",
  });

  // Connect to Redis
  try {
    console.log("Attempting to connect to Redis...");
    await redisClient.connect();
    console.log("Redis connection established successfully");

    // Test Redis connection
    await redisClient.ping();
    console.log("Redis ping successful");

    // Check initial queue status
    await checkQueueStatus();
  } catch (error) {
    console.error("Failed to connect to Redis:", error);
    console.error("Please check your REDIS_URL environment variable");
    return;
  }

  let processedCount = 0;
  let errorCount = 0;

  console.log("=== Starting main processing loop ===");

  // Main processing loop
  while (true) {
    try {
      // Check if Redis is connected
      if (!redisClient.isOpen) {
        console.log("Redis disconnected, attempting to reconnect...");
        try {
          await redisClient.connect();
          console.log("Redis reconnected successfully");
        } catch (reconnectError) {
          console.error("Failed to reconnect to Redis:", reconnectError);
          await new Promise((resolve) => setTimeout(resolve, 10000)); // Wait 10 seconds before retry
          continue;
        }
      }

      // Check queue status periodically
      if (processedCount % 10 === 0) {
        await checkQueueStatus();
      }

      // Get submission from queue
      console.log("Attempting to pop submission from queue...");
      const submission = await redisClient.rPop(REDIS_QUEUE_NAME);

      if (submission) {
        console.log("Submission found in queue, processing...");
        let submissionData: Submission;

        try {
          submissionData = JSON.parse(submission);
          console.log(`Parsed submission data for ID: ${submissionData.id}`);
        } catch (parseError) {
          console.error("Failed to parse submission JSON:", parseError);
          console.error("Raw submission data:", submission);
          errorCount++;
          continue;
        }

        const success = await processSubmission(submissionData);
        if (success) {
          processedCount++;
          console.log(
            `=== Processing stats: ${processedCount} successful, ${errorCount} errors ===`
          );
        } else {
          errorCount++;
          console.log(
            `=== Processing stats: ${processedCount} successful, ${errorCount} errors ===`
          );
        }

        // Small delay between processing
        await new Promise((resolve) => setTimeout(resolve, WORKER_DELAY_MS));
      } else {
        // No submissions, wait a bit before checking again
        if (processedCount % 20 === 0) {
          // Log every 20th check to avoid spam
          console.log("No submissions in queue, waiting...");
        }
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    } catch (error) {
      console.error("Error in worker loop:", error);
      errorCount++;
      console.log(
        `=== Processing stats: ${processedCount} successful, ${errorCount} errors ===`
      );
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
}

// Graceful shutdown handling
process.on("SIGINT", async () => {
  console.log("Received SIGINT, shutting down gracefully...");
  await redisClient.quit();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  console.log("Received SIGTERM, shutting down gracefully...");
  await redisClient.quit();
  process.exit(0);
});

// Start the worker
startWorker().catch((error) => {
  console.error("Worker startup error:", error);
  process.exit(1);
});

console.log("Worker startup initiated");
