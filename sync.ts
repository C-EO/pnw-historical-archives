import { DuckDBInstance } from '@duckdb/node-api';
import fs from 'fs';
import path from 'path';
import zlib from 'zlib';

const DATASETS = ['wars', 'trades', 'cities', 'nations', 'alliances'];

function getYesterdayDate(): string {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - 1);
  return d.toISOString().split('T')[0];
}

async function main() {
  const date = process.argv[2] || getYesterdayDate();
  console.log(`🚀 Starting Nightly PnW Archive Sync for ${date}...`);

  const outputDir = path.resolve('./parquet');
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  const instance = await DuckDBInstance.create(':memory:');
  const conn = await instance.connect();

  for (const ds of DATASETS) {
    const url = `https://politicsandwar.com/data/${ds}/${ds}-${date}.csv.zip`;
    const targetParquet = path.join(outputDir, `${ds}-${date}.parquet`);

    try {
      console.log(`📥 Fetching ${ds}...`);
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);

      const zipBuf = new Uint8Array(await res.arrayBuffer());
      const csvContent = extractFirstCsvFromZip(zipBuf);
      
      const tempCsv = path.join(outputDir, `temp_${ds}.csv`);
      fs.writeFileSync(tempCsv, csvContent, 'utf-8');

      await conn.run(`COPY (SELECT * FROM read_csv_auto('${tempCsv.replace(/\\/g, '/')}')) TO '${targetParquet.replace(/\\/g, '/')}' (FORMAT PARQUET, COMPRESSION ZSTD);`);
      fs.unlinkSync(tempCsv);

      console.log(`  ✅ Generated ${ds}-${date}.parquet`);
    } catch (err: any) {
      console.warn(`  ⚠️ Failed ${ds}: ${err.message}`);
    }
  }

  conn.disconnectSync();
  console.log('🎉 Sync Complete!');
}

function extractFirstCsvFromZip(bytes: Uint8Array): string {
  let offset = 0;
  while (offset < bytes.length - 30) {
    if ((bytes[offset] ?? 0) === 0x50 && (bytes[offset+1] ?? 0) === 0x4b && (bytes[offset+2] ?? 0) === 0x03 && (bytes[offset+3] ?? 0) === 0x04) {
      const b8 = bytes[offset + 8] ?? 0; const b9 = bytes[offset + 9] ?? 0;
      const comp = b8 | (b9 << 8);
      const b18 = bytes[offset + 18] ?? 0; const b19 = bytes[offset + 19] ?? 0;
      const b20 = bytes[offset + 20] ?? 0; const b21 = bytes[offset + 21] ?? 0;
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
