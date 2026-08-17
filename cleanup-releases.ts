import { execSync } from 'child_process';

const PROTECTED_TAGS = new Set(['v-master', 'v-manifest', 'v2026']);

async function main() {
  console.log('🧹 Starting Almighty Release Cleanup...\n');
  console.log(`🛡️ Protected Releases: [${Array.from(PROTECTED_TAGS).join(', ')}]\n`);

  // Fetch all releases in the repository (tagName, name)
  const output = execSync('gh release list --limit 100 --json tagName,name', {
    encoding: 'utf-8',
    stdio: ['ignore', 'pipe', 'inherit'],
  });

  const releases: Array<{ tagName: string; name: string }> = JSON.parse(output || '[]');
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
    console.log(`⏳ Deleting release and git tag: ${rel.tagName}...`);
    try {
      execSync(`gh release delete "${rel.tagName}" --cleanup-tag --yes`, { stdio: 'inherit' });
      console.log(`   ✅ Successfully wiped ${rel.tagName}.\n`);
    } catch (err: any) {
      console.warn(`   ⚠️ Could not delete ${rel.tagName}: ${err.message}\n`);
    }
  }

  console.log('🎉 Cleanup Complete! Remaining Active Releases:');
  execSync('gh release list', { stdio: 'inherit' });
}

main().catch((err) => {
  console.error('❌ Cleanup failed:', err);
  process.exit(1);
});
