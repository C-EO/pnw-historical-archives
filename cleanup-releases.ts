import { execSync } from 'child_process';

const PROTECTED_TAGS = new Set(['v-master', 'v-manifest', 'v2026']);

async function main() {
  console.log('🧹 Starting Almighty Release Cleanup...\n');
  console.log(`🛡️ Protected Releases: [${Array.from(PROTECTED_TAGS).join(', ')}]\n`);

  // Fetch all releases in the repository
  const output = execSync('gh release list --limit 100 --json tagName,name,id', {
    encoding: 'utf-8',
    stdio: ['ignore', 'pipe', 'inherit'],
  });

  const releases: Array<{ tagName: string; name: string; id: string }> = JSON.parse(output || '[]');
  console.log(`🔍 Found ${releases.length} total releases in repository.\n`);

  const tagsToDelete = releases.filter((r) => !PROTECTED_TAGS.has(r.tagName));

  if (tagsToDelete.length === 0) {
    console.log('✅ No old releases found to delete. Everything is already clean!');
    return;
  }

  console.log(`🗑️ The following ${tagsToDelete.length} release(s) will be completely wiped:`);
  tagsToDelete.forEach((r) => console.log(`   - ${r.tagName} (${r.name})`));
  console.log('');

  for (const rel of tagsToDelete) {
    console.log(`⏳ Deleting release tag: ${rel.tagName} (ID: ${rel.id})...`);

    try {
      // 1. Delete all attached assets first
      const assetListRaw = execSync(
        `gh api repos/:owner/:repo/releases/${rel.id}/assets --paginate --jq ".[].id"`,
        { encoding: 'utf-8', stdio: ['ignore', 'pipe', 'ignore'] }
      );
      const assetIds = assetListRaw.split('\n').map((s) => s.trim()).filter(Boolean);

      if (assetIds.length > 0) {
        console.log(`   📦 Deleting ${assetIds.length} assets from ${rel.tagName}...`);
        for (const assetId of assetIds) {
          try {
            execSync(`gh api -X DELETE repos/:owner/:repo/releases/assets/${assetId}`, {
              stdio: 'ignore',
            });
          } catch {
            // Ignore asset delete failure if already removed
          }
        }
      }

      // 2. Delete the release and its git tag
      execSync(`gh release delete "${rel.tagName}" --cleanup-tag --yes`, { stdio: 'inherit' });
      console.log(`   ✅ Release ${rel.tagName} wiped cleanly.\n`);
    } catch (err: any) {
      console.warn(`   ⚠️ Could not delete release ${rel.tagName}: ${err.message}\n`);
    }
  }

  console.log('🎉 Cleanup Complete! Only the pristine master archives remain:');
  execSync('gh release list', { stdio: 'inherit' });
}

main().catch((err) => {
  console.error('❌ Cleanup failed:', err);
  process.exit(1);
});
