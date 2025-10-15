import { Router } from "express";
import type { Request, Response } from "express";
import multer from "multer";

const router = Router();

// Configure multer to store files in memory by default.
// Swap to disk storage if needed in the future.
const upload = multer({ storage: multer.memoryStorage() });

router.post(
  "/file-upload",
  upload.single("file"),
  async (req: Request, res: Response) => {
    return res.status(200).json({ ok: true });
  }
);

export default router;
