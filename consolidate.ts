import { DuckDBInstance } from '@duckdb/node-api';
import { execSync } from 'child_process';
import fs from 'fs';
import path from 'path';
import zlib from 'zlib';

const DATASET_SORT_KEYS: Record<string, string> = {
  nations: 'nation_id',
  alliances: 'alliance_id',
  cities: 'nation_id, city_id',
  wars: 'war_id',
  trades: 'trade_id',
};

function generateDateRange(startStr: string, endStr: string): string[] {
  const dates: string[] = [];
  const curr = new Date(startStr);
  const end = new Date(endStr);
  while (curr <= end) {
    dates.push(curr.toISOString().split('T')[0]!);
    curr.setDate(curr.getDate() + 1);
  }
  return dates;
}

function ensureRelease(tag: string) {
  try {
    execSync(`gh release view ${tag}`, { stdio: 'ignore' });
  } catch {
    console.log(`✨ Creating release tag: ${tag}...`);
    execSync(
      `gh release create ${tag} --title "PnW Parquet Archives ${tag}" --notes "Annual Consolidated Archives for ${tag}"`,
      { stdio: 'inherit' }
    );
  }
}

async function main() {
  const dataset = process.argv[2] || 'nations';
  const startYear = parseInt(process.argv[3] || '2020', 10);
  const endYear = parseInt(process.argv[4] || '2026', 10);
  const sortKey = DATASET_SORT_KEYS[dataset] || 'id';

  console.log(`🚀 Consolidating Dataset: ${dataset.toUpperCase()} (${startYear} - ${endYear})...`);

  const outputDir = path.resolve('./parquet_build');
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  const instance = await DuckDBInstance.create(':memory:');
  const conn = await instance.connect();

  for (let year = startYear; year <= endYear; year++) {
    const tag = `v${year}`;
    ensureRelease(tag);

    const startDate = year === 2020 ? '2020-10-19' : `${year}-01-01`;
    const today = new Date().toISOString().split('T')[0]!;
    const endDate = year === 2026 ? today : `${year}-12-31`;

    const dates = generateDateRange(startDate, endDate);
    console.log(`\n📅 Processing ${year} (${dates.length} days for ${dataset})...`);

    const dailyParquetFiles: string[] = [];

    for (const date of dates) {
      const url = `https://politicsandwar.com/data/${dataset}/${dataset}-${date}.csv.zip`;
      const dailyParquet = path.join(outputDir, `${dataset}_${date}.parquet`);

      try {
        const res = await fetch(url);
        if (!res.ok) continue;

        const zipBuf = new Uint8Array(await res.arrayBuffer());
        const csvContent = extractFirstCsvFromZip(zipBuf);

        const tempCsv = path.join(outputDir, `temp_${date}.csv`);
        fs.writeFileSync(tempCsv, csvContent, 'utf-8');

        await conn.run(`
          COPY (
            SELECT *, CAST('${date}' AS DATE) as snapshot_date 
            FROM read_csv_auto('${tempCsv.replace(/\\/g, '/')}', delim=',', quote='"', ignore_errors=true)
          ) TO '${dailyParquet.replace(/\\/g, '/')}' (FORMAT PARQUET, COMPRESSION ZSTD);
        `);

        fs.unlinkSync(tempCsv);
        dailyParquetFiles.push(dailyParquet);
      } catch (err: any) {
        console.warn(`  ⚠️ Skipped ${date}: ${err.message}`);
      }
    }

    if (dailyParquetFiles.length === 0) continue;

    console.log(`  ⚡ Merging and sorting ${dailyParquetFiles.length} files into ${dataset}-${year}.parquet...`);
    const annualParquet = path.join(outputDir, `${dataset}-${year}.parquet`);

    const parquetList = dailyParquetFiles.map((f) => `'${f.replace(/\\/g, '/')}'`).join(', ');

    await conn.run(`
      COPY (
        SELECT * FROM read_parquet([${parquetList}], union_by_name=true)
        ORDER BY ${sortKey}, snapshot_date
      ) TO '${annualParquet.replace(/\\/g, '/')}' (FORMAT PARQUET, COMPRESSION ZSTD);
    `);

    // Clean up daily temps
    for (const f of dailyParquetFiles) {
      if (fs.existsSync(f)) fs.unlinkSync(f);
    }

    // Upload annual file
    console.log(`  ⬆️ Uploading ${dataset}-${year}.parquet to release ${tag}...`);
    execSync(`gh release upload ${tag} "${annualParquet}" --clobber`, { stdio: 'inherit' });
    fs.unlinkSync(annualParquet);
    console.log(`  ✅ Complete for ${dataset} ${year}!`);
  }

  conn.disconnectSync();
  console.log(`\n🎉 Consolidation complete for ${dataset}!`);
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
  throw new Error('No CSV found in zip');
}

main();
