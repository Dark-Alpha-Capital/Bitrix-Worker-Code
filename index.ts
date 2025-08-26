import { createClient } from "redis";
import prismaDB from "./lib/prisma";
import { doAIDealScreening } from "./lib/ai/tools/ai-screening";

const redisClient = createClient({
  url: process.env.REDIS_URL,
});

redisClient.on("error", (error) => {
  console.log("redis client error", error);
});

// Add HTTP server for Cloud Run
const port = process.env.PORT || 8080;

// Create a simple HTTP server
const server = Bun.serve({
  port: parseInt(port.toString()),
  fetch(req) {
    const url = new URL(req.url);

    // Health check endpoint
    if (url.pathname === "/health" || url.pathname === "/") {
      return new Response("OK", { status: 200 });
    }

    return new Response("Not Found", { status: 404 });
  },
});

console.log(`HTTP server listening on port ${port}`);

async function processSubmission(submission: any) {
  console.log("inside process submissions");

  console.log("processing submission", submission);

  // const dealScreeningResult = await doAIDealScreening(
  //   JSON.stringify(submission)
  // );

  await redisClient.publish(
    "problem_done",
    JSON.stringify({
      userId: submission.userId,
      productId: submission.id,
      status: "done",
      productName: submission.name,
    })
  );
}

async function startWorker() {
  console.log("worker started");

  try {
    await redisClient.connect();
  } catch (error) {
    console.log("redis client error", error);
  }

  while (true) {
    try {
      const submission = await redisClient.rPop("dealListings");

      if (submission) {
        const dealListingData = JSON.parse(submission);
        await new Promise((resolve) => setTimeout(resolve, 3000));
        await processSubmission(dealListingData);
      }
    } catch (error) {
      console.error("Error processing submission", error);
    }
  }
}

startWorker().catch(console.error);
