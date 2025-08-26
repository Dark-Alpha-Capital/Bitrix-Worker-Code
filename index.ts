import { createClient } from "redis";
import prismaDB from "./lib/prisma";
import { splitContentIntoChunks } from "./lib/utils";
import { generateObject, generateText } from "ai";
import { openai } from "./lib/ai/available-models";
import { z } from "zod";

// Constants
const REDIS_QUEUE_NAME = "dealListings";
const REDIS_PUBLISH_CHANNEL = "problem_done";
const WORKER_DELAY_MS = 5000;
const DEFAULT_PORT = 8080;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

// Types
interface DealInformation {
  id: string;
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

// Global variables for cleanup
let redisReconnectInterval: NodeJS.Timeout | null = null;
let isShuttingDown = false;

// Redis client configuration
const redisClient = createClient({
  url: process.env.REDIS_URL,
  socket: {
    reconnectStrategy: (retries) => {
      if (retries > 10) {
        console.error("Max Redis reconnection attempts reached");
        return new Error("Max reconnection attempts reached");
      }
      return Math.min(retries * 100, 3000);
    },
  },
});

redisClient.on("error", (error) => {
  console.error("Redis client error:", error);
});

redisClient.on("connect", () => {
  console.log("Redis client connected successfully");
});

redisClient.on("ready", () => {
  console.log("Redis client ready");
});

redisClient.on("end", () => {
  console.log("Redis client connection ended");
});

// Graceful shutdown handler
const gracefulShutdown = async (signal: string) => {
  console.log(`\n🛑 Received ${signal}. Starting graceful shutdown...`);
  isShuttingDown = true;

  try {
    // Clear intervals
    if (redisReconnectInterval) {
      clearInterval(redisReconnectInterval);
      redisReconnectInterval = null;
    }

    // Close Redis connection
    if (redisClient.isOpen) {
      await redisClient.quit();
      console.log("✅ Redis connection closed");
    }

    // Close Prisma connection
    await prismaDB.$disconnect();
    console.log("✅ Database connection closed");

    console.log("✅ Graceful shutdown completed");
    process.exit(0);
  } catch (error) {
    console.error("❌ Error during graceful shutdown:", error);
    process.exit(1);
  }
};

// Handle shutdown signals
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

// HTTP server for Cloud Run
const resolvedPort = Number.parseInt(process.env.PORT || `${DEFAULT_PORT}`, 10);

const server = Bun.serve({
  port: Number.isFinite(resolvedPort) ? resolvedPort : DEFAULT_PORT,
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
 * Extracts deal information from submission
 */
function extractDealInformation(submission: Submission): DealInformation {
  console.log("🔍 Extracting deal information for submission:", submission.id);
  console.log("📊 Deal details:", {
    brokerage: submission.brokerage,
    dealType: submission.dealType,
    revenue: submission.revenue,
    ebitda: submission.ebitda,
    industry: submission.industry,
  });

  const dealInfo = {
    id: submission.id,
    brokerage: submission.brokerage,
    firstName: submission.firstName,
    lastName: submission.lastName,
    linkedinUrl: submission.linkedinUrl,
    workPhone: submission.workPhone,
    dealCaption: submission.dealCaption,
    dealType: submission.dealType,
    revenue: submission.revenue,
    ebitda: submission.ebitda,
    ebitdaMargin: submission.ebitdaMargin,
    industry: submission.industry,
    sourceWebsite: submission.sourceWebsite,
    companyLocation: submission.companyLocation,
  };

  console.log("✅ Deal information extracted successfully");
  return dealInfo;
}

/**
 * Processes individual content chunks and generates summaries
 */
async function processContentChunks(
  chunks: string[],
  dealInformation: DealInformation
): Promise<string[]> {
  console.log("🔄 Starting content chunk processing...");
  console.log("📝 Total chunks to process:", chunks.length);

  const intermediateSummaries: string[] = [];

  for (let i = 0; i < chunks.length; i++) {
    if (isShuttingDown) {
      console.log("🛑 Shutdown requested, stopping chunk processing");
      break;
    }

    const chunk = chunks[i];
    if (!chunk) {
      continue;
    }

    try {
      console.log(`\n📄 Processing chunk ${i + 1}/${chunks.length}`);
      console.log(`📏 Chunk length: ${chunk.length} characters`);
      console.log(`🔤 Chunk preview: ${chunk.substring(0, 100)}...`);

      const summary = await generateText({
        model: openai("gpt-4o-mini"),
        prompt: `Evaluate this listing ${JSON.stringify(
          dealInformation
        )}: ${chunk}`,
      });

      console.log(`✅ Chunk ${i + 1} evaluation completed`);
      console.log(`📝 Summary length: ${summary.text.length} characters`);
      console.log(`📋 Summary preview: ${summary.text.substring(0, 150)}...`);

      intermediateSummaries.push(summary.text);
    } catch (error) {
      console.error(`❌ Error processing chunk ${i + 1}:`, error);
      // Continue with other chunks instead of failing completely
      intermediateSummaries.push(`[Error processing chunk ${i + 1}]`);
    }
  }

  console.log(
    `🎯 Content chunk processing completed. Generated ${intermediateSummaries.length} summaries`
  );
  return intermediateSummaries;
}

/**
 * Generates final AI screening result
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
 * Saves AI screening result to database
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
 * Publishes completion notification to Redis
 */
async function publishCompletionNotification(
  submission: Submission
): Promise<boolean> {
  try {
    if (!redisClient.isOpen) {
      console.warn("Redis not connected, skipping notification");
      return false;
    }

    await redisClient.publish(
      REDIS_PUBLISH_CHANNEL,
      JSON.stringify({
        userId: submission.userId,
        productId: submission.id,
        status: "done",
        productName: submission.name,
      })
    );
    return true;
  } catch (error) {
    console.error("Error publishing completion notification:", error);
    return false;
  }
}

/**
 * Main function to process a submission
 */
async function processSubmission(submission: Submission): Promise<boolean> {
  console.log("Processing submission:", submission.id);

  try {
    // Extract deal information
    const dealInformation = extractDealInformation(submission);

    // Split content into chunks
    const chunks = await splitContentIntoChunks(submission.screenerContent);
    console.log(`Content split into ${chunks.length} chunks`);

    if (chunks.length === 0) {
      console.warn(
        "No content chunks generated for submission:",
        submission.id
      );
      return false;
    }

    // Process content chunks
    const intermediateSummaries = await processContentChunks(
      chunks,
      dealInformation
    );

    // Combine summaries
    const combinedSummary = intermediateSummaries.join(
      "\n\n=== Next Section ===\n\n"
    );

    // Generate final summary
    const finalSummary = await generateFinalSummary(combinedSummary);
    if (!finalSummary) {
      console.error(
        "Failed to generate final summary for submission:",
        submission.id
      );
      return false;
    }

    console.log("Final summary generated:", finalSummary);

    // Save to database
    const saveSuccess = await saveAIScreeningResult(
      submission.id,
      finalSummary,
      combinedSummary
    );

    if (!saveSuccess) {
      console.error(
        "Failed to save AI screening result for submission:",
        submission.id
      );
      return false;
    }

    // Publish completion notification
    const publishSuccess = await publishCompletionNotification(submission);
    if (!publishSuccess) {
      console.error(
        "Failed to publish completion notification for submission:",
        submission.id
      );
      // Don't return false here as the main processing was successful
    }

    console.log("Submission processed successfully:", submission.id);
    return true;
  } catch (error) {
    console.error("Error processing submission:", submission.id, error);
    return false;
  }
}

/**
 * Safely process Redis operations with retries
 */
async function safeRedisOperation<T>(
  operation: () => Promise<T>,
  retries = MAX_RETRIES
): Promise<T | null> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      if (!redisClient.isOpen) {
        await redisClient.connect();
      }
      return await operation();
    } catch (error) {
      console.error(`Redis operation attempt ${attempt} failed:`, error);
      if (attempt === retries) {
        return null;
      }
      await new Promise((resolve) =>
        setTimeout(resolve, RETRY_DELAY_MS * attempt)
      );
    }
  }
  return null;
}

/**
 * Main worker function
 */
async function startWorker(): Promise<void> {
  console.log("🚀 Worker starting...");

  try {
    // Initial Redis connection
    await redisClient.connect();
    console.log("✅ Initial Redis connection established");
  } catch (error) {
    console.error("❌ Failed to establish initial Redis connection:", error);
    // Continue anyway, the reconnection logic will handle it
  }

  // Set up Redis reconnection interval
  redisReconnectInterval = setInterval(async () => {
    if (isShuttingDown) return;

    try {
      if (!redisClient.isOpen) {
        await redisClient.connect();
        console.log("✅ Redis reconnected");
      }
    } catch (error) {
      console.error("❌ Redis reconnection failed:", error);
    }
  }, 10_000);

  console.log("🔄 Starting main processing loop...");

  let consecutiveErrors = 0;
  const maxConsecutiveErrors = 5;

  while (!isShuttingDown) {
    try {
      // Process submissions from Redis queue
      const submission = await safeRedisOperation(() =>
        redisClient.rPop(REDIS_QUEUE_NAME)
      );

      if (submission) {
        const dealListingData: Submission = JSON.parse(submission);
        console.log(`📥 Processing submission: ${dealListingData.id}`);

        // Add delay between processing
        await new Promise((resolve) => setTimeout(resolve, WORKER_DELAY_MS));

        // Process the submission
        const success = await processSubmission(dealListingData);

        if (!success) {
          console.warn("❌ Submission processing failed:", dealListingData.id);
          consecutiveErrors++;
        } else {
          consecutiveErrors = 0; // Reset on success
        }
      }

      // Reset consecutive errors if no submission was found
      if (!submission) {
        consecutiveErrors = 0;
      }

      // Check if we have too many consecutive errors
      if (consecutiveErrors >= maxConsecutiveErrors) {
        console.error(
          `❌ Too many consecutive errors (${consecutiveErrors}), pausing worker`
        );
        await new Promise((resolve) => setTimeout(resolve, 30000)); // Pause for 30 seconds
        consecutiveErrors = 0;
      }

      // Small delay to prevent tight loop
      await new Promise((resolve) => setTimeout(resolve, 100));
    } catch (error) {
      console.error("❌ Error in main worker loop:", error);
      consecutiveErrors++;

      // Add delay before retrying to prevent rapid error loops
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }

  console.log("🛑 Worker loop stopped due to shutdown signal");
}

// Start the worker without exiting the process on failure so Cloud Run stays healthy
startWorker().catch((error) => {
  console.error("❌ Worker startup error (continuing to serve HTTP):", error);
});

// Health check endpoint for Cloud Run
console.log("🏥 Health check endpoint available at /health");
console.log("📊 Worker status: Starting...");
