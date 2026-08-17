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
  const dataset = process.argv[2] || 'cities';
  const sortKey = DATASET_SORT_KEYS[dataset] || 'id';
  const targetTag = 'v-master';

  console.log(`🚀 Streaming Master Consolidation for ${dataset.toUpperCase()} (2020-2025)...`);

  const outputDir = path.resolve('./master_build');
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  ensureRelease(targetTag);

  // Use persistent on-disk DuckDB database file
  const dbPath = path.join(outputDir, `${dataset}_temp.duckdb`);
  if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);

  const instance = await DuckDBInstance.create(dbPath);
  const conn = await instance.connect();

  await conn.run(`SET preserve_insertion_order = false;`);
  await conn.run(`SET memory_limit = '4GB';`);
  await conn.run(`SET threads = 2;`);

  // Download all 6 annual files to inspect full schema
  const annualFilePaths: string[] = [];
  for (const year of PAST_YEARS) {
    const filename = `${dataset}-${year}.parquet`;
    const targetPath = path.join(outputDir, filename);
    const releaseTag = `v${year}`;

    console.log(`📥 Downloading ${filename} from ${releaseTag}...`);
    try {
      execSync(`gh release download ${releaseTag} -p "${filename}" -D "${outputDir}" --clobber`, {
        stdio: 'inherit',
      });
      if (fs.existsSync(targetPath)) annualFilePaths.push(targetPath);
    } catch (err: any) {
      console.warn(`  ⚠️ Could not download ${filename}: ${err.message}`);
    }
  }

  if (annualFilePaths.length === 0) {
    conn.disconnectSync();
    throw new Error(`No files downloaded for ${dataset}`);
  }

  // 1. Create master schema with union_by_name (handles 37 vs 38 columns automatically)
  console.log(`\n🏗️ Initializing master table schema with all columns across all years...`);
  const cleanList = annualFilePaths.map((p) => `'${p.replace(/\\/g, '/')}'`).join(', ');
  await conn.run(`
    CREATE TABLE master_table AS 
    SELECT * FROM read_parquet([${cleanList}], union_by_name=true) 
    LIMIT 0;
  `);

  // 2. Stream insert each year with BY NAME mapping
  for (const filePath of annualFilePaths) {
    const cleanPath = filePath.replace(/\\/g, '/');
    const filename = path.basename(filePath);

    console.log(`➕ Appending and sorting ${filename} BY NAME into master table...`);
    await conn.run(`
      INSERT INTO master_table BY NAME 
      SELECT * FROM read_parquet('${cleanPath}')
      ORDER BY ${sortKey}, snapshot_date;
    `);

    fs.unlinkSync(filePath);
    console.log(`  🧹 Freed local disk space for ${filename}`);
  }

  console.log(`\n⚡ Exporting master table directly to ${dataset}-history.parquet (ZSTD)...`);
  const masterParquetPath = path.join(outputDir, `${dataset}-history.parquet`);
  const cleanMasterPath = masterParquetPath.replace(/\\/g, '/');

  await conn.run(`
    COPY master_table TO '${cleanMasterPath}' (FORMAT PARQUET, COMPRESSION ZSTD);
  `);

  conn.disconnectSync();

  // Remove temporary database
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
