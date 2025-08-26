import "dotenv/config";
import OpenAI from "openai";
import { createOpenAI } from "@ai-sdk/openai";

export const openaiClient = new OpenAI({
  apiKey: process.env.AI_API_KEY,
});

export const openai = createOpenAI({
  apiKey: process.env.AI_API_KEY,
});
