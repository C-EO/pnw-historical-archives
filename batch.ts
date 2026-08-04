import { DuckDBInstance } from '@duckdb/node-api';
import { execSync } from 'child_process';
import fs from 'fs';
import path from 'path';
import zlib from 'zlib';

const DATASETS = ['wars', 'trades', 'cities', 'nations', 'alliances'];
const RELEASE_TAG = 'v1.0.0';

function generateDateRange(startStr: string, endStr: string): string[] {
  const dates: string[] = [];
  const curr = new Date(startStr);
  const end = new Date(endStr);
  while (curr <= end) {
    dates.push(curr.toISOString().split('T')[0]);
    curr.setDate(curr.getDate() + 1);
  }
  return dates;
}

function delay(ms: number) {
  return new Promise((res) => setTimeout(res, ms));
}

/**
 * Fetches the set of already uploaded filenames from the GitHub release tag assets.
 */
function getRemoteReleaseAssets(tag: string): Set<string> {
  try {
    const out = execSync(`gh release view ${tag} --json assets --jq ".assets[].name"`, {
      encoding: 'utf-8',
      stdio: ['ignore', 'pipe', 'ignore'],
    });
    return new Set(out.split('\n').map((s) => s.trim()).filter(Boolean));
  } catch {
    return new Set();
  }
}

async function main() {
  const startDate = process.argv[2] || '2020-10-19';
  const endDate = process.argv[3] || new Date().toISOString().split('T')[0];

  console.log(`🚀 Starting Historical Backfill: ${startDate} to ${endDate}...`);

  const outputDir = path.resolve('./parquet');
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  const remoteAssets = getRemoteReleaseAssets(RELEASE_TAG);
  console.log(`ℹ️ Found ${remoteAssets.size} files already attached to ${RELEASE_TAG} release.`);

  const dates = generateDateRange(startDate, endDate);
  const instance = await DuckDBInstance.create(':memory:');
  const conn = await instance.connect();

  for (const ds of DATASETS) {
    console.log(`\n📦 Processing dataset: ${ds.toUpperCase()} (${dates.length} snapshots)...`);

    for (const date of dates) {
      const filename = `${ds}-${date}.parquet`;
      const targetParquet = path.join(outputDir, filename);

      // Skip if already uploaded to GitHub Release
      if (remoteAssets.has(filename)) {
        continue;
      }

      const url = `https://politicsandwar.com/data/${ds}/${ds}-${date}.csv.zip`;

      try {
        const res = await fetch(url);
        if (!res.ok) continue;

        const zipBuf = new Uint8Array(await res.arrayBuffer());
        const csvContent = extractFirstCsvFromZip(zipBuf);

        const tempCsv = path.join(outputDir, `temp_${ds}_${date}.csv`);
        fs.writeFileSync(tempCsv, csvContent, 'utf-8');

        // Convert to Parquet
        await conn.run(
          `COPY (SELECT * FROM read_csv_auto('${tempCsv.replace(/\\/g, '/')}')) TO '${targetParquet.replace(/\\/g, '/')}' (FORMAT PARQUET, COMPRESSION ZSTD);`
        );
        fs.unlinkSync(tempCsv);

        // Upload immediately to GitHub Release via gh CLI
        execSync(`gh release upload ${RELEASE_TAG} "${targetParquet}" --clobber`, { stdio: 'inherit' });

        // Delete local Parquet file immediately to keep disk space at 0 MB
        fs.unlinkSync(targetParquet);

        console.log(`  ✅ [${ds.toUpperCase()}] Uploaded ${filename}`);

        // Micro-throttle delay to respect GitHub API secondary rate limits
        await delay(100);
      } catch (err: any) {
        console.warn(`  ⚠️ Failed ${filename}: ${err.message}`);
        if (fs.existsSync(targetParquet)) fs.unlinkSync(targetParquet);
      }
    }
  }

  conn.disconnectSync();
  console.log('\n🎉 Backfill & Direct Upload Completed Successfully!');
}

function extractFirstCsvFromZip(bytes: Uint8Array): string {
  let offset = 0;
  while (offset < bytes.length - 30) {
    if (
      (bytes[offset] ?? 0) === 0x50 &&
      (bytes[offset + 1] ?? 0) === 0x4b &&
      (bytes[offset + 2] ?? 0) === 0x03 &&
      (bytes[offset + 3] ?? 0) === 0x04
    ) {
      const b8 = bytes[offset + 8] ?? 0;
      const b9 = bytes[offset + 9] ?? 0;
      const comp = b8 | (b9 << 8);
      const b18 = bytes[offset + 18] ?? 0;
      const b19 = bytes[offset + 19] ?? 0;
      const b20 = bytes[offset + 20] ?? 0;
      const b21 = bytes[offset + 21] ?? 0;
      const cSize = b18 | (b19 << 8) | (b20 << 16) | (b21 << 24);
      const fnLen = (bytes[offset + 26] ?? 0) | ((bytes[offset + 27] ?? 0) << 8);
      const exLen = (bytes[offset + 28] ?? 0) | ((bytes[offset + 29] ?? 0) << 8);
      const pStart = offset + 30 + fnLen + exLen;
      const pBytes = bytes.subarray(pStart, pStart + cSize);
      if (comp === 8) return zlib.inflateRawSync(Buffer.from(pBytes)).toString('utf-8');
      if (comp === 0) return new TextDecoder('utf-8').decode(pBytes);
    }
    offset++;
  }
  throw new Error('No CSV found');
}

main();
