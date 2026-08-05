import { DuckDBInstance } from '@duckdb/node-api';
import { execSync } from 'child_process';
import fs from 'fs';
import path from 'path';
import zlib from 'zlib';

const DATASETS = ['wars', 'trades', 'cities', 'nations', 'alliances'];
const BATCH_SIZE = 50; // Upload 50 files at once per CLI command

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

function getReleaseTagForDate(dateStr: string): string {
  const d = new Date(dateStr);
  const year = d.getUTCFullYear();
  const half = d.getUTCMonth() < 6 ? 'h1' : 'h2';
  return `v${year}-${half}`;
}

const remoteAssetsMap = new Map<string, Set<string>>();

function ensureRelease(tag: string): Set<string> {
  if (remoteAssetsMap.has(tag)) return remoteAssetsMap.get(tag)!;
  try {
    const out = execSync(`gh release view ${tag} --json assets --jq ".assets[].name"`, {
      encoding: 'utf-8',
      stdio: ['ignore', 'pipe', 'ignore'],
    });
    const assets = new Set(out.split('\n').map((s) => s.trim()).filter(Boolean));
    remoteAssetsMap.set(tag, assets);
    return assets;
  } catch {
    console.log(`✨ Creating release tag: ${tag}...`);
    execSync(`gh release create ${tag} --title "PnW Parquet Archives ${tag}" --notes "Daily Parquet Extracts for ${tag}"`, {
      stdio: 'inherit',
    });
    const assets = new Set<string>();
    remoteAssetsMap.set(tag, assets);
    return assets;
  }
}

async function main() {
  const startDate = process.argv[2] || '2020-10-19';
  const endDate = process.argv[3] || new Date().toISOString().split('T')[0];

  console.log(`🚀 Starting Fast Batched Backfill: ${startDate} to ${endDate}...`);

  const outputDir = path.resolve('./parquet');
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  const dates = generateDateRange(startDate, endDate);
  const instance = await DuckDBInstance.create(':memory:');
  const conn = await instance.connect();

  for (const ds of DATASETS) {
    console.log(`\n📦 Dataset: ${ds.toUpperCase()} (${dates.length} dates)...`);

    // Group dates by release tag (e.g. v2021-h1)
    const tagGroup = new Map<string, string[]>();
    for (const d of dates) {
      const tag = getReleaseTagForDate(d);
      if (!tagGroup.has(tag)) tagGroup.set(tag, []);
      tagGroup.get(tag)!.push(d);
    }

    for (const [tag, tagDates] of tagGroup.entries()) {
      const existingAssets = ensureRelease(tag);
      const pendingDates = tagDates.filter((d) => !existingAssets.has(`${ds}-${d}.parquet`));

      if (pendingDates.length === 0) continue;

      // Process in chunks of BATCH_SIZE (50 files)
      for (let i = 0; i < pendingDates.length; i += BATCH_SIZE) {
        const chunk = pendingDates.slice(i, i + BATCH_SIZE);
        const generatedPaths: string[] = [];

        for (const date of chunk) {
          const filename = `${ds}-${date}.parquet`;
          const targetParquet = path.join(outputDir, filename);
          const url = `https://politicsandwar.com/data/${ds}/${ds}-${date}.csv.zip`;

          try {
            const res = await fetch(url);
            if (!res.ok) continue;

            const zipBuf = new Uint8Array(await res.arrayBuffer());
            const csvContent = extractFirstCsvFromZip(zipBuf);

            const tempCsv = path.join(outputDir, `temp_${ds}_${date}.csv`);
            fs.writeFileSync(tempCsv, csvContent, 'utf-8');

            await conn.run(
              `COPY (SELECT * FROM read_csv_auto('${tempCsv.replace(/\\/g, '/')}')) TO '${targetParquet.replace(/\\/g, '/')} '(FORMAT PARQUET, COMPRESSION ZSTD);`
            );
            fs.unlinkSync(tempCsv);
            generatedPaths.push(targetParquet);
          } catch {
            if (fs.existsSync(targetParquet)) fs.unlinkSync(targetParquet);
          }
        }

        if (generatedPaths.length > 0) {
          const fileArgs = generatedPaths.map((p) => `"${p}"`).join(' ');
          console.log(`  ⬆️ Uploading batch of ${generatedPaths.length} files to ${tag}...`);
          execSync(`gh release upload ${tag} ${fileArgs} --clobber`, { stdio: 'inherit' });

          // Instant cleanup
          for (const p of generatedPaths) {
            if (fs.existsSync(p)) fs.unlinkSync(p);
            existingAssets.add(path.basename(p));
          }
        }
      }
    }
  }

  conn.disconnectSync();
  console.log('\n🎉 Fast Backfill Completed Successfully!');
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
