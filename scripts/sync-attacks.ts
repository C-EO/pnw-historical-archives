import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { DuckDBInstance } from "@duckdb/node-api";
import * as fs from "fs";
import * as path from "path";

// Configuration
const PNW_API_URL = "https://api.politicsandwar.com/graphql";
const API_KEY = process.env.PNW_UNLIMITED_API_KEY || "";
const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID || "";
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID || "";
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY || "";
const R2_BUCKET_NAME = process.env.R2_BUCKET_NAME || "";
const MANUAL_START_ID = process.env.MANUAL_START_ATTACK_ID
  ? parseInt(process.env.MANUAL_START_ATTACK_ID, 10)
  : null;

const CHECKPOINT_KEY = "attacks/checkpoint.json";
const CONCURRENCY = 6;
const CHUNK_SIZE = 25000; // Attack ID span per work chunk

if (!API_KEY || !R2_ACCOUNT_ID || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY || !R2_BUCKET_NAME) {
  console.error("❌ Missing required environment variables.");
  process.exit(1);
}

// S3 Client configured for Cloudflare R2
const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
  requestChecksumCalculation: "WHEN_REQUIRED",
  responseChecksumValidation: "WHEN_REQUIRED",
});

interface Checkpoint {
  lastAttackId: number;
  lastUpdated: string;
}

const ATTACK_FIELDS = `
  id
  date
  war_id
  att_id
  def_id
  type
  victor
  success
  city_id
  infra_destroyed
  infra_destroyed_value
  money_stolen
  money_looted
  money_destroyed
  resistance_lost
  att_soldiers_used
  att_soldiers_lost
  def_soldiers_used
  def_soldiers_lost
  att_tanks_used
  att_tanks_lost
  def_tanks_used
  def_tanks_lost
  att_aircraft_used
  att_aircraft_lost
  def_aircraft_used
  def_aircraft_lost
  att_ships_used
  att_ships_lost
  def_ships_used
  def_ships_lost
  att_missiles_lost
  def_missiles_lost
  att_nukes_lost
  def_nukes_lost
  att_gas_used
  def_gas_used
  att_mun_used
  def_mun_used
  military_salvage_steel
  military_salvage_aluminum
  coal_looted
  oil_looted
  uranium_looted
  iron_looted
  bauxite_looted
  lead_looted
  gasoline_looted
  munitions_looted
  steel_looted
  aluminum_looted
  food_looted
`;

async function fetchGraphQL(query: string, variables: Record<string, any> = {}, retries = 3): Promise<any> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(`${PNW_API_URL}?api_key=${API_KEY}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query, variables }),
      });
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${res.statusText}`);
      }
      const data = await res.json();
      if (data.errors && data.errors.length > 0) {
        throw new Error(data.errors.map((e: any) => e.message).join(", "));
      }
      return data.data;
    } catch (err) {
      if (attempt === retries) throw err;
      await new Promise((r) => setTimeout(r, 1000 * attempt));
    }
  }
}

async function getCheckpointFromR2(): Promise<Checkpoint | null> {
  try {
    const res = await s3.send(
      new GetObjectCommand({
        Bucket: R2_BUCKET_NAME,
        Key: CHECKPOINT_KEY,
      })
    );
    const bodyString = await res.Body?.transformToString();
    if (!bodyString) return null;
    return JSON.parse(bodyString);
  } catch {
    return null;
  }
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

async function run() {
  console.log("🚀 Starting Accelerated PnW Attack Ingestion Pipeline...");

  const checkpoint = await getCheckpointFromR2();
  let startAttackId = 1;

  if (MANUAL_START_ID) {
    startAttackId = MANUAL_START_ID;
    console.log(`⚙️ Manual override start ID: ${startAttackId}`);
  } else if (checkpoint?.lastAttackId) {
    startAttackId = checkpoint.lastAttackId + 1;
    console.log(`📍 Resuming from checkpoint attack ID: ${startAttackId}`);
  } else {
    console.log("ℹ️ No existing checkpoint found in R2. Starting fresh.");
  }

  // 1. Probe for earliest attack ID (ASC) and latest attack ID (DESC)
  console.log("🔍 Probing attack ID range...");
  const ascProbeQuery = `
    query ProbeAsc($min_id: Int) {
      warattacks(min_id: $min_id, first: 1, orderBy: [{ column: ID, order: ASC }]) {
        paginatorInfo { total count }
        data { id }
      }
    }
  `;
  const ascData = await fetchGraphQL(ascProbeQuery, { min_id: startAttackId });
  const firstAttack = ascData?.warattacks?.data?.[0];

  if (!firstAttack) {
    console.log("✨ No new attacks found to ingest. Up to date!");
    return;
  }

  const actualStartId = parseInt(firstAttack.id, 10);
  const totalAttacks = ascData?.warattacks?.paginatorInfo?.total || 670000;

  // Find latest attack ID
  let latestId = actualStartId + totalAttacks + 5000;
  try {
    const descQuery = `query { warattacks(first: 1, orderBy: [{ column: ID, order: DESC }]) { data { id } } }`;
    const descData = await fetchGraphQL(descQuery);
    if (descData?.warattacks?.data?.[0]?.id) {
      latestId = parseInt(descData.warattacks.data[0].id, 10);
    }
  } catch {
    // Keep estimated upper bound
  }

  console.log(`📊 Ingestion Target: ID ${actualStartId} -> ${latestId} (~${totalAttacks.toLocaleString()} attacks)`);

  // 2. Build Work Chunk Queue
  interface Chunk {
    minId: number;
    maxId: number;
  }
  const queue: Chunk[] = [];
  for (let current = actualStartId; current <= latestId; current += CHUNK_SIZE) {
    queue.push({ minId: current, maxId: Math.min(current + CHUNK_SIZE - 1, latestId + 1000) });
  }

  console.log(`⚡ Created ${queue.length} work chunks. Launching ${CONCURRENCY} parallel workers...`);

  const tempDir = path.join(process.cwd(), "temp_attacks");
  if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir, { recursive: true });

  const tempJsonlPath = path.join(tempDir, "attacks_stream.jsonl");
  if (fs.existsSync(tempJsonlPath)) fs.unlinkSync(tempJsonlPath);
  const jsonlStream = fs.createWriteStream(tempJsonlPath, { flags: "a" });

  let totalIngested = 0;
  let maxIngestedId = actualStartId;

  // 3. Worker Function
  async function worker(workerId: number) {
    const batchQuery = `
      query GetChunk($min_id: Int, $max_id: Int, $first: Int) {
        warattacks(min_id: $min_id, max_id: $max_id, first: $first, orderBy: [{ column: ID, order: ASC }]) {
          data { ${ATTACK_FIELDS} }
        }
      }
    `;

    while (queue.length > 0) {
      const chunk = queue.shift();
      if (!chunk) break;

      let chunkMin = chunk.minId;
      while (chunkMin <= chunk.maxId) {
        const res = await fetchGraphQL(batchQuery, {
          min_id: chunkMin,
          max_id: chunk.maxId,
          first: 1000,
        });

        const attacks: any[] = res?.warattacks?.data || [];
        if (attacks.length === 0) break;

        const lines = attacks.map((a) => JSON.stringify(a)).join("\n") + "\n";
        jsonlStream.write(lines);

        totalIngested += attacks.length;
        const batchMax = Math.max(...attacks.map((a) => parseInt(a.id, 10)));
        if (batchMax > maxIngestedId) maxIngestedId = batchMax;

        if (totalIngested % 25000 < 1000) {
          console.log(`⚡ Progress: ${totalIngested.toLocaleString()} attacks ingested (Max ID: ${maxIngestedId})...`);
        }

        chunkMin = batchMax + 1;
        if (attacks.length < 1000) break;
      }
    }
  }

  // Execute concurrent workers
  await Promise.all(Array.from({ length: CONCURRENCY }, (_, i) => worker(i + 1)));

  // Close JSONL stream
  await new Promise((resolve) => jsonlStream.end(resolve));
  console.log(`🎉 Ingested ${totalIngested.toLocaleString()} attacks total (Max ID: ${maxIngestedId})!`);

  // 4. DuckDB: Compile to Sorted, Compressed Parquet
  console.log("📦 Compiling DuckDB JSONL stream into sorted, compressed Parquet...");
  const currentYear = new Date().getFullYear();
  const parquetFileName = `attacks-${currentYear}.parquet`;
  const parquetFilePath = path.join(tempDir, parquetFileName);

  const instance = await DuckDBInstance.create();
  const connection = await instance.connect();

  const formattedJsonlPath = tempJsonlPath.replace(/\\/g, "/");
  const formattedParquetPath = parquetFilePath.replace(/\\/g, "/");

  await connection.run(`
    COPY (
      SELECT * FROM read_json_auto('${formattedJsonlPath}')
      ORDER BY id ASC
    ) TO '${formattedParquetPath}' (FORMAT PARQUET, COMPRESSION ZSTD);
  `);

  // 5. Upload Parquet and Checkpoint to Cloudflare R2
  const r2Key = `attacks/${parquetFileName}`;
  await uploadParquetToR2(parquetFilePath, r2Key);
  await saveCheckpointToR2({
    lastAttackId: maxIngestedId,
    lastUpdated: new Date().toISOString(),
  });

  // 6. Cleanup local temp files
  try {
    fs.rmSync(tempDir, { recursive: true, force: true });
  } catch {}

  console.log("🏁 Pipeline completed successfully!");
}

run().catch((err) => {
  console.error("❌ Pipeline failed:", err);
  process.exit(1);
});
