import { execSync } from 'child_process';

const PROTECTED_TAGS = new Set(['v-master', 'v-manifest', 'v2026']);

// Config
const DRY_RUN = true; // set to false to actually perform deletes
const CHUNK_SIZE = 50; // delete assets in batches of this size
const ASSET_DELETE_RETRIES = 5;
const RELEASE_DELETE_RETRIES = 5;
const INITIAL_BACKOFF_MS = 1000;

function sleep(ms: number) {
  return new Promise((res) => setTimeout(res, ms));
}

function runCmd(cmd: string, capture = true) {
  return execSync(cmd, {
    encoding: 'utf-8',
    stdio: capture ? ['ignore', 'pipe', 'pipe'] : 'inherit',
  });
}

function isTransientError(text: string) {
  return /HTTP 5\d\d|Server Error|We couldn't respond to your request in time|timeout|ECONNRESET|ECONNREFUSED|EAI_/i.test(
    text,
  );
}

async function deleteAssetWithRetries(owner: string, repo: string, assetId: number, maxRetries = ASSET_DELETE_RETRIES) {
  let attempt = 0;
  let backoff = INITIAL_BACKOFF_MS;
  const assetUrl = `/repos/${owner}/${repo}/releases/assets/${assetId}`;

  while (attempt < maxRetries) {
    attempt++;
    try {
      console.log(`      → Deleting asset ${assetId} (attempt ${attempt})`);
      if (!DRY_RUN) {
        execSync(`gh api --method DELETE ${assetUrl}`, { stdio: 'inherit' });
      } else {
        console.log(`        [dry-run] would run: gh api --method DELETE ${assetUrl}`);
      }
      console.log(`      ✅ Asset ${assetId} deleted`);
      return true;
    } catch (err: any) {
      const msg = (err.stderr && err.stderr.toString && err.stderr.toString()) || err.message || String(err);
      console.warn(`      ⚠️ Asset ${assetId} delete attempt ${attempt} failed: ${msg.split('\n').slice(0, 6).join('\n')}`);
      if (!isTransientError(msg)) {
        console.warn('      ✋ Not retrying asset delete because error does not look transient.');
        return false;
      }
      if (attempt < maxRetries) {
        console.log(`      🔁 Retrying asset ${assetId} after ${backoff}ms...`);
        await sleep(backoff);
        backoff *= 2;
      } else {
        console.warn(`      ❌ Reached max retries (${maxRetries}) for asset ${assetId}.`);
        return false;
      }
    }
  }
  return false;
}

async function deleteReleaseWithRetries(tagName: string, releaseId: number | string, maxRetries = RELEASE_DELETE_RETRIES) {
  let attempt = 0;
  let backoff = INITIAL_BACKOFF_MS;

  while (attempt < maxRetries) {
    attempt++;
    try {
      console.log(`   Attempt ${attempt}: gh release delete "${tagName}" --cleanup-tag --yes`);
      if (!DRY_RUN) {
        execSync(`gh release delete "${tagName}" --cleanup-tag --yes`, { stdio: 'inherit' });
      } else {
        console.log(`      [dry-run] would run: gh release delete "${tagName}" --cleanup-tag --yes`);
      }
      console.log(`   ✅ Successfully wiped ${tagName}.\n`);
      return true;
    } catch (err: any) {
      const stderr = (err.stderr && err.stderr.toString && err.stderr.toString()) || err.message || String(err);
      console.warn(`   ⚠️ Delete attempt ${attempt} failed for ${tagName} (release id=${releaseId}).`);
      console.warn(`      Error snippet: ${stderr.split('\n').slice(0, 6).join('\n')}\n`);

      if (!isTransientError(stderr)) {
        console.warn(`   ✋ Not retrying for ${tagName} because the error does not look transient.`);
        return false;
      }

      if (attempt < maxRetries) {
        console.log(`   🔁 Retrying ${tagName} after ${backoff}ms...`);
        await sleep(backoff);
        backoff *= 2;
      } else {
        console.warn(`   ❌ Reached max retries (${maxRetries}) for ${tagName}.`);
        return false;
      }
    }
  }
  return false;
}

function chunkArray<T>(arr: T[], size: number) {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function main() {
  console.log('🧹 Starting Almighty Release Cleanup...\n');
  console.log(`🛡️ Protected Releases: [${Array.from(PROTECTED_TAGS).join(', ')}]\n`);
  if (DRY_RUN) console.log('⚠️ Running in DRY_RUN mode — no deletes will be performed.\n');

  // Determine owner/repo
  const repoEnv = process.env.GITHUB_REPOSITORY || 'C-EO/pnw-historical-archives';
  const [owner, repo] = repoEnv.split('/');

  // Fetch only the fields we need (one small JSON object per line)
  const output = runCmd(
    `gh api repos/${owner}/${repo}/releases --paginate --jq '.[] | {id: .id, tag_name: .tag_name, name: .name}'`,
    true,
  );

  // gh --jq emits one JSON object per line here; parse line-by-line to avoid huge single-buffer JSON
  const lines = (output || '').trim().split(/\r?\n/).filter(Boolean);
  const apiReleases = lines.map((l) => JSON.parse(l));
  const releases: Array<{ tagName: string; name: string; id: number | string }> = apiReleases.map((r: any) => ({
    tagName: r.tag_name,
    name: r.name,
    id: r.id,
  }));
  console.log(`🔍 Found ${releases.length} total releases in repository.\n`);

  const tagsToDelete = releases.filter((r) => !PROTECTED_TAGS.has(r.tagName));

  if (tagsToDelete.length === 0) {
    console.log('✅ No old releases found to delete. Everything is already clean!');
    return;
  }

  console.log(`🗑️ The following ${tagsToDelete.length} release(s) will be processed:`);
  tagsToDelete.forEach((r) => console.log(`   - ${r.tagName} (${r.name}) [id=${r.id}]`));
  console.log('');

  for (const rel of tagsToDelete) {
    console.log(`⏳ Processing release ${rel.tagName} (id=${rel.id})...`);

    // 1) List assets for the release (paged, with retries) to avoid ENOBUFS
    let assets: Array<{ id: number; name: string }> = [];
    try {
      const perPage = 50; // adjust smaller if necessary
      let page = 1;
      let finished = false;

      while (!finished) {
        let attempt = 0;
        let backoff = 500;
        let pageItems: any[] = [];

        while (attempt < 5) {
          attempt++;
          try {
            // Quote the endpoint because of the '&' in the query string (prevents shell backgrounding)
            const pageOut = runCmd(
              `gh api 'repos/${owner}/${repo}/releases/${rel.id}/assets?per_page=${perPage}&page=${page}' --jq '.'`,
              true,
            );
            pageItems = JSON.parse(pageOut || '[]');
            break;
          } catch (err: any) {
            const msg = (err.stderr && err.stderr.toString && err.stderr.toString()) || err.message || String(err);
            console.warn(`      ⚠️ assets page fetch failed (release ${rel.id}) attempt ${attempt}: ${msg.split('\n').slice(0,4).join('\n')}`);
            if (!isTransientError(msg)) {
              // non-transient — abort asset paging
              pageItems = [];
              finished = true;
              break;
            }
            // transient: wait and retry
            await sleep(backoff);
            backoff *= 2;
          }
        }

        if (!pageItems || pageItems.length === 0) {
          // no results => done
          break;
        }

        assets.push(...pageItems.map((a: any) => ({ id: a.id, name: a.name })));

        // if fewer than perPage returned, we're done
        if (pageItems.length < perPage) finished = true;
        else page++;

        // small pause between pages to reduce burst pressure
        await sleep(200);
      }
    } catch (err: any) {
      console.warn(`   ⚠️ Could not list assets for ${rel.tagName}: ${(err && err.message) || String(err)}`);
    }

    console.log(`   🔗 Found ${assets.length} asset(s) attached to ${rel.tagName}.`);

    if (assets.length > 0) {
      const chunks = chunkArray(assets, CHUNK_SIZE);
      for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i];
        console.log(`   🧰 Deleting asset batch ${i + 1}/${chunks.length} (size=${chunk.length})...`);
        for (const a of chunk) {
          try {
            const ok = await deleteAssetWithRetries(owner, repo, a.id, ASSET_DELETE_RETRIES);
            if (!ok) {
              console.warn(`      ⚠️ Failed to delete asset ${a.id} (${a.name}), continuing with next assets.`);
            }
          } catch (err: any) {
            console.warn(`      ⚠️ Unexpected error deleting asset ${a.id}: ${err && err.message ? err.message : String(err)}`);
          }
          // small pause between asset deletes
          await sleep(150);
        }
        // pause between batches
        await sleep(500);
      }
    }

    // 2) Delete the release itself with retries
    const deleted = await deleteReleaseWithRetries(rel.tagName, rel.id, RELEASE_DELETE_RETRIES);
    if (!deleted) {
      console.warn(`   ⚠️ Could not delete release ${rel.tagName} after retries. It may still exist.`);
    }

    // short pause between releases
    await sleep(700);
  }

  console.log('🎉 Cleanup Complete! Remaining Active Releases:');
  if (!DRY_RUN) {
    execSync('gh release list', { stdio: 'inherit' });
  } else {
    console.log('[dry-run] Skipping final gh release list');
  }
}

main().catch((err) => {
  console.error('❌ Cleanup failed:', err);
  process.exit(1);
});
