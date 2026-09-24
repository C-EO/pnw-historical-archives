import { DuckDBInstance } from '@duckdb/node-api';
import { S3Client, PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import fs from 'fs';
import path from 'path';

// Environment Configuration
const PNW_API_KEY = process.env.PNW_UNLIMITED_API_KEY || process.env.PNW_API_KEY;
const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
const R2_BUCKET = process.env.R2_BUCKET_NAME || 'pnw-archives';

if (!PNW_API_KEY) throw new Error('Missing PNW_UNLIMITED_API_KEY environment variable');
if (!R2_ACCOUNT_ID || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY) {
  throw new Error('Missing Cloudflare R2 credentials (R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY)');
}

// S3 Client configured for Cloudflare R2 Private Bucket
const s3 = new S3Client({
  region: 'auto',
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
});

const GRAPHQL_ENDPOINT = `https://api.politicsandwar.com/graphql?api_key=${PNW_API_KEY}`;
const BATCH_SIZE = 1000;
const CHECKPOINT_KEY = 'attacks/checkpoint.json';

const GET_ATTACKS_QUERY = `
  query GetAttacks($min_id: Int, $first: Int) {
    warattacks(min_id: $min_id, first: $first, orderBy: [{ column: ID, order: ASC }]) {
      paginatorInfo {
        count
        hasMorePages
      }
      data {
        id
        date
        war_id
        att_id
        def_id
        type
        victor
        success
        infra_destroyed
        infra_destroyed_value
        money_stolen
        money_looted
        food_looted
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
        military_salvage_steel
        military_salvage_aluminum
        att_soldiers_lost
        def_soldiers_lost
        att_tanks_lost
        def_tanks_lost
        att_aircraft_lost
        def_aircraft_lost
        att_ships_lost
        def_ships_lost
        att_gas_used
        def_gas_used
        att_mun_used
        def_mun_used
      }
    }
  }
`;

async function fetchAttacksBatch(minId: number) {
  const res = await fetch(GRAPHQL_ENDPOINT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      query: GET_ATTACKS_QUERY,
      variables: { min_id: minId, first: BATCH_SIZE },
    }),
  });

  if (!res.ok) {
    throw new Error(`PnW GraphQL request failed with status ${res.status}: ${await res.text()}`);
  }

  const json = await res.json();
  if (json.errors?.length) {
    throw new Error(`GraphQL Error: ${json.errors[0]?.message}`);
  }

  const data = json?.data?.warattacks?.data || [];
  const hasMore = Boolean(json?.data?.warattacks?.paginatorInfo?.hasMorePages);
  return { attacks: data, hasMore };
}

async function getCheckpoint(): Promise<number> {
  const manualStart = process.env.MANUAL_START_ATTACK_ID;
  if (manualStart && !isNaN(Number(manualStart))) {
    return Number(manualStart);
  }

  try {
    const res = await s3.send(new GetObjectCommand({ Bucket: R2_BUCKET, Key: CHECKPOINT_KEY }));
    const text = await res.Body?.transformToString();
    if (text) {
      const parsed = JSON.parse(text);
      if (parsed.last_attack_id) return parsed.last_attack_id;
    }
  } catch {
    console.log('ℹ️ No existing checkpoint found in R2. Starting fresh.');
  }

  return 1;
}

async function saveCheckpoint(lastAttackId: number) {
  await s3.send(
    new PutObjectCommand({
      Bucket: R2_BUCKET,
      Key: CHECKPOINT_KEY,
      ContentType: 'application/json',
      Body: JSON.stringify({
        last_attack_id: lastAttackId,
        updated_at: new Date().toISOString(),
      }),
    })
  );
  console.log(`💾 Checkpoint saved to R2: last_attack_id = ${lastAttackId}`);
}

async function uploadParquetToR2(filePath: string, targetKey: string) {
  const fileStream = fs.createReadStream(filePath);
  const fileSize = fs.statSync(filePath).size;
  console.log(`☁️ Uploading ${path.basename(filePath)} (${(fileSize / (1024 * 1024)).toFixed(2)} MB) to R2 [${targetKey}]...`);

  await s3.send(
    new PutObjectCommand({
      Bucket: R2_BUCKET,
      Key: targetKey,
      ContentType: 'application/vnd.apache.parquet',
      Body: fileStream,
    })
  );

  console.log(`✅ Upload complete: s3://${R2_BUCKET}/${targetKey}`);
}

async function main() {
  console.log('🚀 Starting PnW Attack Ingestion Pipeline to Cloudflare R2...');

  const workDir = path.resolve('./temp_attacks');
  if (!fs.existsSync(workDir)) fs.mkdirSync(workDir, { recursive: true });

  const tempJsonl = path.join(workDir, 'attacks_staging.jsonl');
  if (fs.existsSync(tempJsonl)) fs.unlinkSync(tempJsonl);

  let currentMinId = await getCheckpoint();
  console.log(`📍 Starting ingestion from attack ID: ${currentMinId}`);

  let totalIngested = 0;
  let highestId = currentMinId;
  let keepGoing = true;

  const currentYear = new Date().getUTCFullYear();

  while (keepGoing) {
    console.log(`📥 Fetching 1,000 attacks starting from ID >= ${currentMinId}...`);
    const { attacks, hasMore } = await fetchAttacksBatch(currentMinId);

    if (attacks.length === 0) {
      console.log('🏁 No new attacks returned. Reached the latest live record.');
      break;
    }

    for (const a of attacks) {
      const aid = Number(a.id);
      if (aid > highestId) highestId = aid;
    }

    // Append batch directly to JSON Lines file
    const lines = attacks.map((a: any) => JSON.stringify(a)).join('\n') + '\n';
    fs.appendFileSync(tempJsonl, lines, 'utf-8');

    totalIngested += attacks.length;
    currentMinId = highestId + 1;

    console.log(`⚡ Ingested ${totalIngested.toLocaleString()} attacks total (Current Max ID: ${highestId})...`);

    if (!hasMore || attacks.length < BATCH_SIZE) {
      keepGoing = false;
    }
  }

  if (totalIngested === 0) {
    console.log('✨ All attacks are already up to date. Exiting.');
    return;
  }

  // Compile JSONL to optimized ZSTD Parquet via DuckDB
  const outputParquet = path.join(workDir, `attacks-${currentYear}.parquet`);
  console.log(`📦 Compiling DuckDB JSONL stream into sorted, compressed Parquet (${outputParquet})...`);

  const instance = await DuckDBInstance.create(':memory:');
  const conn = await instance.connect();

  await conn.run(`
    COPY (
      SELECT 
        CAST(id AS BIGINT) AS id,
        CAST(date AS VARCHAR) AS date,
        CAST(war_id AS BIGINT) AS war_id,
        CAST(att_id AS BIGINT) AS att_id,
        CAST(def_id AS BIGINT) AS def_id,
        CAST(type AS VARCHAR) AS type,
        CAST(victor AS BIGINT) AS victor,
        COALESCE(CAST(success AS INTEGER), 0) AS success,
        COALESCE(CAST(infra_destroyed AS DOUBLE), 0.0) AS infra_destroyed,
        COALESCE(CAST(infra_destroyed_value AS DOUBLE), 0.0) AS infra_destroyed_value,
        COALESCE(CAST(money_stolen AS DOUBLE), 0.0) AS money_stolen,
        COALESCE(CAST(money_looted AS DOUBLE), 0.0) AS money_looted,
        COALESCE(CAST(food_looted AS DOUBLE), 0.0) AS food_looted,
        COALESCE(CAST(coal_looted AS DOUBLE), 0.0) AS coal_looted,
        COALESCE(CAST(oil_looted AS DOUBLE), 0.0) AS oil_looted,
        COALESCE(CAST(uranium_looted AS DOUBLE), 0.0) AS uranium_looted,
        COALESCE(CAST(iron_looted AS DOUBLE), 0.0) AS iron_looted,
        COALESCE(CAST(bauxite_looted AS DOUBLE), 0.0) AS bauxite_looted,
        COALESCE(CAST(lead_looted AS DOUBLE), 0.0) AS lead_looted,
        COALESCE(CAST(gasoline_looted AS DOUBLE), 0.0) AS gasoline_looted,
        COALESCE(CAST(munitions_looted AS DOUBLE), 0.0) AS munitions_looted,
        COALESCE(CAST(steel_looted AS DOUBLE), 0.0) AS steel_looted,
        COALESCE(CAST(aluminum_looted AS DOUBLE), 0.0) AS aluminum_looted,
        COALESCE(CAST(military_salvage_steel AS DOUBLE), 0.0) AS military_salvage_steel,
        COALESCE(CAST(military_salvage_aluminum AS DOUBLE), 0.0) AS military_salvage_aluminum,
        COALESCE(CAST(att_soldiers_lost AS INTEGER), 0) AS att_soldiers_lost,
        COALESCE(CAST(def_soldiers_lost AS INTEGER), 0) AS def_soldiers_lost,
        COALESCE(CAST(att_tanks_lost AS INTEGER), 0) AS att_tanks_lost,
        COALESCE(CAST(def_tanks_lost AS INTEGER), 0) AS def_tanks_lost,
        COALESCE(CAST(att_aircraft_lost AS INTEGER), 0) AS att_aircraft_lost,
        COALESCE(CAST(def_aircraft_lost AS INTEGER), 0) AS def_aircraft_lost,
        COALESCE(CAST(att_ships_lost AS INTEGER), 0) AS att_ships_lost,
        COALESCE(CAST(def_ships_lost AS INTEGER), 0) AS def_ships_lost,
        COALESCE(CAST(att_gas_used AS DOUBLE), 0.0) AS att_gas_used,
        COALESCE(CAST(def_gas_used AS DOUBLE), 0.0) AS def_gas_used,
        COALESCE(CAST(att_mun_used AS DOUBLE), 0.0) AS att_mun_used,
        COALESCE(CAST(def_mun_used AS DOUBLE), 0.0) AS def_mun_used
      FROM read_json_auto('${tempJsonl.replace(/\\/g, '/')}')
      ORDER BY war_id ASC, date ASC
    ) TO '${outputParquet.replace(/\\/g, '/')}' 
    (FORMAT PARQUET, COMPRESSION 'ZSTD');
  `);

  conn.disconnectSync();

  // Upload to Cloudflare R2
  await uploadParquetToR2(outputParquet, `attacks/attacks-${currentYear}.parquet`);
  await saveCheckpoint(highestId);

  // Clean local scratch
  fs.rmSync(workDir, { recursive: true, force: true });
  console.log('🎉 Attack sync finished successfully!');
}

main().catch((err) => {
  console.error('❌ Pipeline failed:', err);
  process.exit(1);
});
