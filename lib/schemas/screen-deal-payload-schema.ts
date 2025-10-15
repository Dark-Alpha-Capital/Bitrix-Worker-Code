import { z } from "zod";

export const screenDealPayloadSchema = z.object({
  jobId: z.string(),
  dealId: z.string(),
  screenerId: z.string(),
  userId: z.string(),
  jobType: z.string(),
});

export type screenDealPayloadType = z.infer<typeof screenDealPayloadSchema>;
