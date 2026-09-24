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
    console.log('ℹ️ No existing checkpoint found in R2. Starting fresh or using default minimum.');
  }

  // Fallback baseline: start from 1 (or recent era if preferred)
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

  const instance = await DuckDBInstance.create(':memory:');
  const conn = await instance.connect();

  // Create staging table for attacks
  await conn.run(`
    CREATE TABLE attacks_stage (
      id BIGINT,
      date VARCHAR,
      war_id BIGINT,
      att_id BIGINT,
      def_id BIGINT,
      type VARCHAR,
      victor BIGINT,
      success INTEGER,
      infra_destroyed DOUBLE,
      infra_destroyed_value DOUBLE,
      money_stolen DOUBLE,
      money_looted DOUBLE,
      food_looted DOUBLE,
      coal_looted DOUBLE,
      oil_looted DOUBLE,
      uranium_looted DOUBLE,
      iron_looted DOUBLE,
      bauxite_looted DOUBLE,
      lead_looted DOUBLE,
      gasoline_looted DOUBLE,
      munitions_looted DOUBLE,
      steel_looted DOUBLE,
      aluminum_looted DOUBLE,
      military_salvage_steel DOUBLE,
      military_salvage_aluminum DOUBLE,
      att_soldiers_lost INTEGER,
      def_soldiers_lost INTEGER,
      att_tanks_lost INTEGER,
      def_tanks_lost INTEGER,
      att_aircraft_lost INTEGER,
      def_aircraft_lost INTEGER,
      att_ships_lost INTEGER,
      def_ships_lost INTEGER,
      att_gas_used DOUBLE,
      def_gas_used DOUBLE,
      att_mun_used DOUBLE,
      def_mun_used DOUBLE
    );
  `);

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

    const appender = await conn.createAppender('attacks_stage');
    for (const a of attacks) {
      const aid = Number(a.id);
      if (aid > highestId) highestId = aid;

      appender.appendBigInt(BigInt(aid));
      appender.appendVarchar(String(a.date || ''));
      appender.appendBigInt(BigInt(a.war_id || 0));
      appender.appendBigInt(BigInt(a.att_id || 0));
      appender.appendBigInt(BigInt(a.def_id || 0));
      appender.appendVarchar(String(a.type || ''));
      appender.appendBigInt(BigInt(a.victor || 0));
      appender.appendInteger(Number(a.success) || 0);
      appender.appendDouble(Number(a.infra_destroyed) || 0);
      appender.appendDouble(Number(a.infra_destroyed_value) || 0);
      appender.appendDouble(Number(a.money_stolen) || 0);
      appender.appendDouble(Number(a.money_looted) || 0);
      appender.appendDouble(Number(a.food_looted) || 0);
      appender.appendDouble(Number(a.coal_looted) || 0);
      appender.appendDouble(Number(a.oil_looted) || 0);
      appender.appendDouble(Number(a.uranium_looted) || 0);
      appender.appendDouble(Number(a.iron_looted) || 0);
      appender.appendDouble(Number(a.bauxite_looted) || 0);
      appender.appendDouble(Number(a.lead_looted) || 0);
      appender.appendDouble(Number(a.gasoline_looted) || 0);
      appender.appendDouble(Number(a.munitions_looted) || 0);
      appender.appendDouble(Number(a.steel_looted) || 0);
      appender.appendDouble(Number(a.aluminum_looted) || 0);
      appender.appendDouble(Number(a.military_salvage_steel) || 0);
      appender.appendDouble(Number(a.military_salvage_aluminum) || 0);
      appender.appendInteger(Number(a.att_soldiers_lost) || 0);
      appender.appendInteger(Number(a.def_soldiers_lost) || 0);
      appender.appendInteger(Number(a.att_tanks_lost) || 0);
      appender.appendInteger(Number(a.def_tanks_lost) || 0);
      appender.appendInteger(Number(a.att_aircraft_lost) || 0);
      appender.appendInteger(Number(a.def_aircraft_lost) || 0);
      appender.appendInteger(Number(a.att_ships_lost) || 0);
      appender.appendInteger(Number(a.def_ships_lost) || 0);
      appender.appendDouble(Number(a.att_gas_used) || 0);
      appender.appendDouble(Number(a.def_gas_used) || 0);
      appender.appendDouble(Number(a.att_mun_used) || 0);
      appender.appendDouble(Number(a.def_mun_used) || 0);
      appender.endRow();
    }
    await appender.flush();
    await appender.close();

    totalIngested += attacks.length;
    currentMinId = highestId + 1;

    console.log(`⚡ Ingested ${totalIngested.toLocaleString()} attacks total (Current Max ID: ${highestId})...`);

    if (!hasMore || attacks.length < BATCH_SIZE) {
      keepGoing = false;
    }
  }

  if (totalIngested === 0) {
    console.log('✨ All attacks are already up to date. Exiting.');
    conn.disconnectSync();
    return;
  }

  // Export to optimized, sorted, ZSTD-compressed Parquet file
  const outputParquet = path.join(workDir, `attacks-${currentYear}.parquet`);
  console.log(`📦 Compiling DuckDB table into sorted, compressed Parquet (${outputParquet})...`);

  await conn.run(`
    COPY (
      SELECT * 
      FROM attacks_stage
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
