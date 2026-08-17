import { DuckDBInstance } from '@duckdb/node-api';
import { execSync } from 'child_process';
import fs from 'fs';
import path from 'path';
import zlib from 'zlib';

const DATASETS: { name: string; sortKey: string }[] = [
  { name: 'nations', sortKey: 'nation_id' },
  { name: 'alliances', sortKey: 'alliance_id' },
  { name: 'cities', sortKey: 'nation_id, city_id' },
  { name: 'wars', sortKey: 'war_id' },
  { name: 'trades', sortKey: 'trade_id' },
];

function getYesterdayDate(): string {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - 1);
  return d.toISOString().split('T')[0]!;
}

function ensureRelease(tag: string) {
  try {
    execSync(`gh release view ${tag}`, { stdio: 'ignore' });
  } catch {
    console.log(`✨ Creating GitHub Release tag: ${tag}...`);
    execSync(
      `gh release create ${tag} --title "PnW Parquet Archives ${tag}" --notes "Annual Consolidated Archives for ${tag}"`,
      { stdio: 'inherit' }
    );
  }
}

async function main() {
  const date = process.argv[2] || getYesterdayDate();
  const year = date.split('-')[0]!;
  const tag = `v${year}`;

  console.log(`🚀 Starting Nightly PnW Archive Sync for ${date} (Target: ${tag})...`);

  const outputDir = path.resolve('./parquet');
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  ensureRelease(tag);
  ensureRelease('v-manifest');

  const instance = await DuckDBInstance.create(':memory:');
  const conn = await instance.connect();

  let maxNationIdForYear: number | null = null;

  for (const { name: ds, sortKey } of DATASETS) {
    const annualFilename = `${ds}-${year}.parquet`;
    const targetAnnualParquet = path.join(outputDir, annualFilename);
    const url = `https://politicsandwar.com/data/${ds}/${ds}-${date}.csv.zip`;

    try {
      console.log(`📥 Fetching daily dump for ${ds} (${date})...`);
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);

      const zipBuf = new Uint8Array(await res.arrayBuffer());
      const csvContent = extractFirstCsvFromZip(zipBuf);

      const tempCsv = path.join(outputDir, `temp_${ds}.csv`);
      fs.writeFileSync(tempCsv, csvContent, 'utf-8');

      // Check if existing annual parquet exists on release
      let hasExisting = false;
      try {
        execSync(`gh release download ${tag} -p "${annualFilename}" -D "${outputDir}" --clobber`, {
          stdio: 'ignore',
        });
        hasExisting = fs.existsSync(targetAnnualParquet);
      } catch {
        hasExisting = false;
      }

      const tempParquetOut = path.join(outputDir, `new_${annualFilename}`);
      const cleanCsvPath = tempCsv.replace(/\\/g, '/');
      const cleanTarget = targetAnnualParquet.replace(/\\/g, '/');
      const cleanOut = tempParquetOut.replace(/\\/g, '/');

      if (hasExisting) {
        console.log(`  🔄 Appending ${date} to existing ${annualFilename}...`);
        await conn.run(`
          CREATE TABLE updated AS 
          SELECT * FROM read_parquet('${cleanTarget}') 
          WHERE snapshot_date != '${date}'
          UNION ALL BY NAME 
          SELECT *, CAST('${date}' AS DATE) as snapshot_date 
          FROM read_csv_auto('${cleanCsvPath}', delim=',', quote='"', ignore_errors=true);
        `);
      } else {
        console.log(`  🆕 Creating new annual ${annualFilename}...`);
        await conn.run(`
          CREATE TABLE updated AS 
          SELECT *, CAST('${date}' AS DATE) as snapshot_date 
          FROM read_csv_auto('${cleanCsvPath}', delim=',', quote='"', ignore_errors=true);
        `);
      }

      if (ds === 'nations') {
        const maxReader = await conn.runAndReadAll(`SELECT MAX(CAST(nation_id AS BIGINT)) FROM updated;`);
        const maxRow = maxReader.getRows()[0];
        if (maxRow && maxRow[0]) {
          maxNationIdForYear = Number(maxRow[0]);
        }
      }

      // Write sorted Parquet with ZSTD compression
      await conn.run(`
        COPY (SELECT * FROM updated ORDER BY ${sortKey}, snapshot_date) 
        TO '${cleanOut}' (FORMAT PARQUET, COMPRESSION ZSTD);
      `);
      await conn.run(`DROP TABLE updated;`);

      fs.unlinkSync(tempCsv);
      if (fs.existsSync(targetAnnualParquet)) fs.unlinkSync(targetAnnualParquet);
      fs.renameSync(tempParquetOut, targetAnnualParquet);

      // Upload updated annual file to GitHub Releases
      execSync(`gh release upload ${tag} "${targetAnnualParquet}" --clobber`, { stdio: 'inherit' });
      fs.unlinkSync(targetAnnualParquet);

      console.log(`  ✅ Uploaded updated ${annualFilename} -> ${tag}`);
    } catch (err: any) {
      console.warn(`  ⚠️ Failed ${ds}: ${err.message}`);
    }
  }

  // Update manifest.json if nations was updated
  if (maxNationIdForYear !== null) {
    const manifestPath = path.resolve('./manifest.json');
    let manifestData: { updatedAt: string; nations: Record<string, number> } = {
      updatedAt: new Date().toISOString(),
      nations: {},
    };

    try {
      execSync(`gh release download v-manifest -p "manifest.json" -D "${outputDir}" --clobber`, {
        stdio: 'ignore',
      });
      const downloadedManifest = path.join(outputDir, 'manifest.json');
      if (fs.existsSync(downloadedManifest)) {
        manifestData = JSON.parse(fs.readFileSync(downloadedManifest, 'utf-8'));
        fs.unlinkSync(downloadedManifest);
      }
    } catch {
      if (fs.existsSync(manifestPath)) {
        manifestData = JSON.parse(fs.readFileSync(manifestPath, 'utf-8'));
      }
    }

    manifestData.updatedAt = new Date().toISOString();
    manifestData.nations[year] = maxNationIdForYear;

    fs.writeFileSync(manifestPath, JSON.stringify(manifestData, null, 2), 'utf-8');
    execSync(`gh release upload v-manifest "${manifestPath}" --clobber`, { stdio: 'inherit' });
    console.log(`📋 Updated and published manifest.json -> v-manifest (Max Nation ID for ${year}: ${maxNationIdForYear})`);
  }

  conn.disconnectSync();
  console.log('🎉 Nightly Sync & Annual Consolidation Complete!');
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
