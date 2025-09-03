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
const REDIS_RETRY_DELAY = 5000;
const MAX_RETRIES = 3;

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

// Redis client with better error handling
const redisClient = createClient({
  url: process.env.REDIS_URL,
  socket: {
    reconnectStrategy: (retries) => {
      if (retries > 10) {
        console.error("Max Redis reconnection attempts reached");
        return new Error("Max reconnection attempts reached");
      }
      return Math.min(retries * 1000, 3000);
    },
    connectTimeout: 10000,
  },
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
 * Check if Redis is properly connected and responsive
 */
async function isRedisHealthy(): Promise<boolean> {
  try {
    if (!redisClient.isOpen) {
      return false;
    }

    // Test with a simple ping command
    await redisClient.ping();
    return true;
  } catch (error) {
    console.error("Redis health check failed:", error);
    return false;
  }
}

/**
 * Safely reconnect to Redis
 */
async function reconnectRedis(): Promise<boolean> {
  try {
    console.log("Attempting to reconnect to Redis...");

    if (redisClient.isOpen) {
      await redisClient.disconnect();
    }

    await redisClient.connect();
    await redisClient.ping();
    console.log("Redis reconnected successfully");
    return true;
  } catch (error) {
    console.error("Failed to reconnect to Redis:", error);
    return false;
  }
}

/**
 * Safely pop from Redis queue with retry logic
 */
async function safePopFromQueue(): Promise<string | null> {
  let retries = 0;

  while (retries < MAX_RETRIES) {
    try {
      if (!(await isRedisHealthy())) {
        console.log(`Redis not healthy, attempt ${retries + 1}/${MAX_RETRIES}`);
        if (!(await reconnectRedis())) {
          retries++;
          await new Promise((resolve) =>
            setTimeout(resolve, REDIS_RETRY_DELAY)
          );
          continue;
        }
      }

      const result = await redisClient.lPop(REDIS_QUEUE_NAME);
      return result;
    } catch (error) {
      console.error(
        `Error popping from queue (attempt ${retries + 1}/${MAX_RETRIES}):`,
        error
      );
      retries++;

      if (retries < MAX_RETRIES) {
        await new Promise((resolve) => setTimeout(resolve, REDIS_RETRY_DELAY));
      }
    }
  }

  console.error("Failed to pop from queue after all retries");
  return null;
}

/**
 * Process content chunks and generate summaries
 */
async function processContentChunks(
  chunks: string[],
  dealInfo: Submission
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

      const dealContext = {
        name: dealInfo.name,
        brokerage: dealInfo.brokerage,
        dealCaption: dealInfo.dealCaption,
        dealType: dealInfo.dealType,
        ebitda: dealInfo.ebitda,
        ebitdaMargin: dealInfo.ebitdaMargin,
        companyLocation: dealInfo.companyLocation,
        revenue: dealInfo.revenue,
        caption: dealInfo.dealCaption,
        industry: dealInfo.industry,
      };

      const prompt = `Based on this deal context: ${JSON.stringify(
        dealContext
      )}, evaluate the following text: ${chunk}`;

      const summary = await generateText({
        system:
          "You are an expert AI Assistant that specializes in deal sourcing, evaluation and private equity in general",
        model: openai("gpt-4o-mini"),
        prompt,
      });

      console.log("summar generated by AI", summary);

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
    if (!(await isRedisHealthy())) {
      console.log("Redis not healthy, skipping queue status check");
      return;
    }

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
  let consecutiveErrors = 0;

  console.log("=== Starting main processing loop ===");

  // Main processing loop
  while (true) {
    try {
      // Check if Redis is healthy
      if (!(await isRedisHealthy())) {
        console.log("Redis not healthy, attempting to reconnect...");
        if (!(await reconnectRedis())) {
          console.log("Reconnection failed, waiting before retry...");
          await new Promise((resolve) =>
            setTimeout(resolve, REDIS_RETRY_DELAY)
          );
          continue;
        }
      }

      if (processedCount % 10 === 0) {
        await checkQueueStatus();
      }

      console.log("Attempting to pop submission from queue...");
      const submission = await safePopFromQueue();

      if (submission) {
        console.log("Submission found in queue, processing...");
        consecutiveErrors = 0; // Reset error counter on success

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
      consecutiveErrors++;

      console.log(
        `=== Processing stats: ${processedCount} successful, ${errorCount} errors (${consecutiveErrors} consecutive) ===`
      );

      // If we have too many consecutive errors, wait longer
      const waitTime = consecutiveErrors > 5 ? 10000 : 5000;
      await new Promise((resolve) => setTimeout(resolve, waitTime));

      // Try to reconnect if we have persistent errors
      if (consecutiveErrors > 10) {
        console.log(
          "Too many consecutive errors, attempting Redis reconnection..."
        );
        await reconnectRedis();
        consecutiveErrors = 0;
      }
    }
  }
}

// Graceful shutdown handling
process.on("SIGINT", async () => {
  console.log("Received SIGINT, shutting down gracefully...");
  try {
    await redisClient.quit();
  } catch (error) {
    console.error("Error during shutdown:", error);
  }
  process.exit(0);
});

process.on("SIGTERM", async () => {
  console.log("Received SIGTERM, shutting down gracefully...");
  try {
    await redisClient.quit();
  } catch (error) {
    console.error("Error during shutdown:", error);
  }
  process.exit(0);
});

// Start the worker
startWorker().catch((error) => {
  console.error("Worker startup error:", error);
  process.exit(1);
});

console.log("Worker startup initiated");
