import "dotenv/config";
import OpenAI from "openai";

export const openaiClient = new OpenAI({
  apiKey: process.env.AI_API_KEY,
});
