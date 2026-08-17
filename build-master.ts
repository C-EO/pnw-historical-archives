import { DuckDBInstance } from '@duckdb/node-api';
import { execSync } from 'child_process';
import fs from 'fs';
import path from 'path';

const DATASET_SORT_KEYS: Record<string, string> = {
  nations: 'nation_id',
  alliances: 'alliance_id',
  cities: 'nation_id, city_id',
  wars: 'war_id',
  trades: 'trade_id',
};

const PAST_YEARS = [2020, 2021, 2022, 2023, 2024, 2025];

function ensureRelease(tag: string) {
  try {
    execSync(`gh release view ${tag}`, { stdio: 'ignore' });
  } catch {
    console.log(`✨ Creating release tag: ${tag}...`);
    execSync(
      `gh release create ${tag} --title "PnW Master Historical Archives" --notes "Unified Master Archives (2020-2025)"`,
      { stdio: 'inherit' }
    );
  }
}

async function main() {
  const dataset = process.argv[2] || 'nations';
  const sortKey = DATASET_SORT_KEYS[dataset] || 'id';
  const targetTag = 'v-master';

  console.log(`🚀 Streaming Master Consolidation for ${dataset.toUpperCase()} (2020-2025)...`);

  const outputDir = path.resolve('./master_build');
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  ensureRelease(targetTag);

  // Use persistent on-disk DuckDB database file (Zero RAM overflow)
  const dbPath = path.join(outputDir, `${dataset}_temp.duckdb`);
  if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);

  const instance = await DuckDBInstance.create(dbPath);
  const conn = await instance.connect();

  // DuckDB safety & performance settings
  await conn.run(`SET preserve_insertion_order = false;`);
  await conn.run(`SET memory_limit = '4GB';`);
  await conn.run(`SET threads = 2;`);

  let tableCreated = false;

  for (const year of PAST_YEARS) {
    const filename = `${dataset}-${year}.parquet`;
    const targetPath = path.join(outputDir, filename);
    const releaseTag = `v${year}`;

    console.log(`\n📥 Downloading & Streaming ${filename} from ${releaseTag}...`);
    try {
      execSync(`gh release download ${releaseTag} -p "${filename}" -D "${outputDir}" --clobber`, {
        stdio: 'inherit',
      });
    } catch (err: any) {
      console.warn(`  ⚠️ Could not download ${filename}: ${err.message}`);
      continue;
    }

    if (!fs.existsSync(targetPath)) continue;

    const cleanParquetPath = targetPath.replace(/\\/g, '/');

    if (!tableCreated) {
      console.log(`  🏗️ Initializing master table with ${year}...`);
      await conn.run(`
        CREATE TABLE master_table AS 
        SELECT * FROM read_parquet('${cleanParquetPath}')
        ORDER BY ${sortKey}, snapshot_date;
      `);
      tableCreated = true;
    } else {
      console.log(`  ➕ Appending and sorting ${year} into master table...`);
      await conn.run(`
        INSERT INTO master_table 
        SELECT * FROM read_parquet('${cleanParquetPath}')
        ORDER BY ${sortKey}, snapshot_date;
      `);
    }

    // Delete the single annual file immediately to save runner disk space
    fs.unlinkSync(targetPath);
    console.log(`  🧹 Freed local disk space for ${filename}`);
  }

  if (!tableCreated) {
    conn.disconnectSync();
    throw new Error(`No files were successfully processed for ${dataset}`);
  }

  console.log(`\n⚡ Exporting master table directly to ${dataset}-history.parquet (ZSTD)...`);
  const masterParquetPath = path.join(outputDir, `${dataset}-history.parquet`);
  const cleanMasterPath = masterParquetPath.replace(/\\/g, '/');

  await conn.run(`
    COPY master_table TO '${cleanMasterPath}' (FORMAT PARQUET, COMPRESSION ZSTD);
  `);

  conn.disconnectSync();

  // Remove temporary DuckDB database file
  if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);
  const walPath = `${dbPath}.wal`;
  if (fs.existsSync(walPath)) fs.unlinkSync(walPath);

  console.log(`⬆️ Uploading ${dataset}-history.parquet to release ${targetTag}...`);
  execSync(`gh release upload ${targetTag} "${masterParquetPath}" --clobber`, { stdio: 'inherit' });

  if (fs.existsSync(masterParquetPath)) fs.unlinkSync(masterParquetPath);

  console.log(`\n🎉 Unified Master Archive ${dataset}-history.parquet successfully created and uploaded!`);
}

main().catch((err) => {
  console.error('❌ Error building master archive:', err);
  process.exit(1);
});
