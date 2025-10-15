import { generateText } from "ai";
import prismaDB from "../prisma";
import { splitContentIntoChunks } from "../utils";
import { openai, openaiClient } from "../ai/available-models";
import { z } from "zod";
import { generateObject } from "ai";
import { Sentiment } from "@prisma/client";

/**
 * Evaluates a deal against a screener
 * @param dealId - The ID of the deal to evaluate
 * @param screenerId - The ID of the screener to use for evaluation
 * @returns The evaluation result
 */
export async function evaluateDealAndSaveResult(
  dealId: string,
  screenerId: string
) {
  const fetchedDealInformation = await prismaDB.deal.findFirst({
    where: {
      id: dealId,
    },
    select: {
      id: true,
      title: true,
      dealCaption: true,
      dealTeaser: true,
      askingPrice: true,
      dealType: true,
      grossRevenue: true,
      tags: true,
      brokerage: true,
      ebitdaMargin: true,
      ebitda: true,
      revenue: true,
    },
  });

  if (!fetchedDealInformation) {
    return {
      success: false,
      error: "Deal not found",
    };
  }

  try {
    const screener = await prismaDB.screener.findFirst({
      where: {
        id: screenerId,
      },
    });

    if (!screener) {
      return {
        success: false,
        error: "Screener not found",
      };
    }

    const chunks = await splitContentIntoChunks(screener.content);
    const totalChunks = chunks.length;
    console.log("total chunks", totalChunks);

    const intermediateSummaries = [];

    for (const chunk of chunks) {
      const summary = await generateText({
        model: openai("gpt-4o-mini"),
        prompt: `Evaluate this listing ${JSON.stringify(
          fetchedDealInformation
        )}: ${chunk}`,
      });
      console.log("pushing chunk evaluation", summary.text);
      intermediateSummaries.push(summary.text);
    }
    const combinedSummary = intermediateSummaries.join(
      "\n\n=== Next Section ===\n\n"
    );

    console.log(combinedSummary);

    let finalSummary;

    try {
      finalSummary = await generateObject({
        model: openai("gpt-4o-mini"),
        prompt: `Combine the following summaries into a single summary: ${combinedSummary}`,
        schema: z.object({
          title: z.string(),
          score: z.number(),
          sentiment: z.enum(["POSITIVE", "NEGATIVE", "NEUTRAL"]),
          explanation: z.string(),
        }),
      });
    } catch (error) {
      console.log(error);
      return {
        success: false,
        message: "Error generating summary",
      };
    }

    let evaluation = finalSummary.object;

    console.log("evaluation", evaluation);

    let sentiment: Sentiment = Sentiment.NEUTRAL;
    if (evaluation.sentiment) {
      switch (evaluation.sentiment) {
        case "POSITIVE":
          sentiment = Sentiment.POSITIVE;
          break;
        case "NEGATIVE":
          sentiment = Sentiment.NEGATIVE;
          break;
        case "NEUTRAL":
        default:
          sentiment = Sentiment.NEUTRAL;
          break;
      }
    }

    // Create the AI screening record
    const savedEvaluation = await prismaDB.aiScreening.create({
      data: {
        dealId,
        title: evaluation.title,
        explanation: evaluation.explanation,
        score: evaluation.score ? Math.round(evaluation.score) : null,
        content: combinedSummary,
        sentiment,
        screenerId,
      },
    });

    return {
      success: true,
      message: "Evaluation saved successfully",
      evaluationId: savedEvaluation.id,
      data: savedEvaluation,
    };
  } catch (error) {
    console.error("Error evaluating deal and saving result:", error);
    return {
      success: false,
      message: error instanceof Error ? error.message : "Unknown error",
    };
  }
}
