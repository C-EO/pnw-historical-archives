import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import * as fs from "fs";
import * as path from "path";

// Initialize S3 client configured specifically for Cloudflare R2
const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
  // Disable trailing checksum trailers that Cloudflare R2 rejects
  requestChecksumCalculation: "WHEN_REQUIRED",
  responseChecksumValidation: "WHEN_REQUIRED",
});

async function uploadParquetToR2(filePath: string, r2Key: string): Promise<void> {
  const fileBuffer = fs.readFileSync(filePath);
  const fileName = path.basename(filePath);
  const sizeMb = (fileBuffer.length / (1024 * 1024)).toFixed(2);

  console.log(`☁️ Uploading ${fileName} (${sizeMb} MB) to R2 [${r2Key}]...`);

  await s3.send(
    new PutObjectCommand({
      Bucket: R2_BUCKET_NAME,
      Key: r2Key,
      Body: fileBuffer,
      ContentLength: fileBuffer.length,
      ContentType: "application/vnd.apache.parquet",
    })
  );

  console.log(`✅ Successfully uploaded ${fileName} to R2!`);
}

async function saveCheckpointToR2(checkpoint: Checkpoint): Promise<void> {
  const body = Buffer.from(JSON.stringify(checkpoint, null, 2), "utf-8");
  await s3.send(
    new PutObjectCommand({
      Bucket: R2_BUCKET_NAME,
      Key: CHECKPOINT_KEY,
      Body: body,
      ContentLength: body.length,
      ContentType: "application/json",
    })
  );
  console.log(`💾 Checkpoint saved: last attack ID = ${checkpoint.lastAttackId}`);
}
