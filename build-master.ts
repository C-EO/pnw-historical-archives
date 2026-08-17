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
  const sortKey = DATASET_SORT_KEYS[dataset] || 'nation_id';
  const targetTag = 'v-master';

  console.log(`🚀 Merging ${dataset.toUpperCase()} (2020-2025) into ${dataset}-history.parquet...`);

  const outputDir = path.resolve('./master_build');
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  ensureRelease(targetTag);

  // Download the 6 annual files from existing releases
  const annualFilePaths: string[] = [];

  for (const year of PAST_YEARS) {
    const filename = `${dataset}-${year}.parquet`;
    const targetPath = path.join(outputDir, filename);
    const releaseTag = `v${year}`;

    console.log(`  📥 Downloading ${filename} from ${releaseTag}...`);
    try {
      execSync(`gh release download ${releaseTag} -p "${filename}" -D "${outputDir}" --clobber`, {
        stdio: 'inherit',
      });
      if (fs.existsSync(targetPath)) {
        annualFilePaths.push(targetPath);
      }
    } catch (err: any) {
      console.warn(`  ⚠️ Could not download ${filename}: ${err.message}`);
    }
  }

  if (annualFilePaths.length === 0) {
    throw new Error(`No annual files found for ${dataset}`);
  }

  console.log(`\n⚡ Merging and sorting ${annualFilePaths.length} annual files with DuckDB...`);
  const instance = await DuckDBInstance.create(':memory:');
  const conn = await instance.connect();

  const masterParquetPath = path.join(outputDir, `${dataset}-history.parquet`);
  const cleanList = annualFilePaths.map((p) => `'${p.replace(/\\/g, '/')}'`).join(', ');

  await conn.run(`
    COPY (
      SELECT * FROM read_parquet([${cleanList}], union_by_name=true)
      ORDER BY ${sortKey}, snapshot_date
    ) TO '${masterParquetPath.replace(/\\/g, '/')}' (FORMAT PARQUET, COMPRESSION ZSTD);
  `);

  conn.disconnectSync();

  console.log(`⬆️ Uploading ${dataset}-history.parquet to release ${targetTag}...`);
  execSync(`gh release upload ${targetTag} "${masterParquetPath}" --clobber`, { stdio: 'inherit' });

  // Cleanup local files
  for (const p of annualFilePaths) {
    if (fs.existsSync(p)) fs.unlinkSync(p);
  }
  if (fs.existsSync(masterParquetPath)) fs.unlinkSync(masterParquetPath);

  console.log(`🎉 Master archive ${dataset}-history.parquet successfully created and uploaded!`);
}

main().catch((err) => {
  console.error('❌ Error building master archive:', err);
  process.exit(1);
});
