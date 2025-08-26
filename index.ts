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
const DEFAULT_PORT = 8081;

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

// Redis client configuration
const redisClient = createClient({
  url: process.env.REDIS_URL,
});

redisClient.on("error", (error) => {
  console.error("Redis client error:", error);
});

redisClient.on("connect", () => {
  console.log("Redis client connected successfully");
});

// HTTP server for Cloud Run
const port = parseInt(process.env.PORT || "8081");

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

console.log(`HTTP server listening on port ${port}`);

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
 * Main worker function
 */
async function startWorker(): Promise<void> {
  console.log("Worker started");

  try {
    await redisClient.connect();
    console.log("Redis client connected successfully");
  } catch (error) {
    console.error("Failed to connect to Redis:", error);
    throw error;
  }

  console.log("Starting main processing loop...");

  while (true) {
    try {
      // Process submissions from Redis queue
      const submission = await redisClient.rPop(REDIS_QUEUE_NAME);

      if (submission) {
        const dealListingData: Submission = JSON.parse(submission);

        // Add delay between processing
        await new Promise((resolve) => setTimeout(resolve, WORKER_DELAY_MS));

        // Process the submission
        const success = await processSubmission(dealListingData);

        if (!success) {
          console.warn("Submission processing failed:", dealListingData.id);
        }
      }

      // Small delay to prevent tight loop
      await new Promise((resolve) => setTimeout(resolve, 100));
    } catch (error) {
      console.error("Error in main worker loop:", error);

      // Add delay before retrying to prevent rapid error loops
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
}

// Start the worker
startWorker().catch((error) => {
  console.error("Fatal error in worker:", error);
  process.exit(1);
});
