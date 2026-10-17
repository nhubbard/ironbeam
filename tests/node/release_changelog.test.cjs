const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { test } = require('node:test');

const { mergeCandidateChangelog } = require('../../scripts/merge_release_changelog.cjs');
const releaseConfig = require('../../.releaserc.json');

function pluginConfig(name) {
  return releaseConfig.plugins.find(([plugin]) => plugin === name)[1];
}

test('the release plugins select v5 and retain one reviewed changelog entry', async (t) => {
  const pluginNames = releaseConfig.plugins.map(([name]) => name);
  assert.ok(pluginNames.indexOf('./scripts/semantic_release_workspace.cjs') < pluginNames.indexOf('@semantic-release/changelog'));
  assert.ok(pluginNames.indexOf('@semantic-release/changelog') < pluginNames.indexOf('@semantic-release/git'));

  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ironbeam-release-'));
  t.after(() => fs.rmSync(cwd, { recursive: true, force: true }));
  const changelog = path.join(cwd, 'CHANGELOG.md');
  const candidate = [
    '## 5.0.0 (unpublished candidate)',
    '',
    '### Breaking changes',
    '',
    '- Reviewed migration guidance.',
    '',
    '### Features',
    '',
    '- Reviewed feature summary.',
    '',
    'Publication is held. A temporary status.',
    '',
    '## [4.1.1] Previous release',
    '',
    'Previous notes.',
    '',
  ].join('\n');
  fs.writeFileSync(changelog, candidate);

  const commits = [{ hash: '0123456789abcdef', message: 'major: prepare Ironbeam 5 workspace' }];
  const logger = { log() {} };
  const analyzer = await import('@semantic-release/commit-analyzer');
  const releaseType = await analyzer.analyzeCommits(
    pluginConfig('@semantic-release/commit-analyzer'),
    { commits, cwd, logger },
  );
  assert.equal(releaseType, 'major');

  const nextRelease = { version: '5.0.0', gitTag: 'v5.0.0' };
  const generator = await import('@semantic-release/release-notes-generator');
  nextRelease.notes = await generator.generateNotes(
    pluginConfig('@semantic-release/release-notes-generator'),
    {
      commits,
      cwd,
      lastRelease: { version: '4.1.1', gitTag: 'v4.1.1' },
      nextRelease,
      options: { repositoryUrl: 'https://github.com/nhubbard/ironbeam.git' },
      logger,
    },
  );
  assert.match(nextRelease.notes, /^## \[5\.0\.0\]/);
  assert.equal(mergeCandidateChangelog('5.0.0', nextRelease, cwd), true);

  const changelogPlugin = await import('@semantic-release/changelog');
  await changelogPlugin.prepare(pluginConfig('@semantic-release/changelog'), {
    cwd,
    nextRelease,
    logger,
  });
  const released = fs.readFileSync(changelog, 'utf8');
  assert.equal((released.match(/^## .*5\.0\.0/gm) || []).length, 1);
  assert.match(released, /### Release summary\n\n#### Breaking changes/);
  assert.match(released, /Reviewed migration guidance/);
  assert.match(released, /Reviewed feature summary/);
  assert.match(released, /## \[4\.1\.1\] Previous release/);
  assert.doesNotMatch(released, /Publication is held|unpublished candidate/);
});

test('invalid generated notes leave the candidate changelog untouched', (t) => {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ironbeam-release-'));
  t.after(() => fs.rmSync(cwd, { recursive: true, force: true }));
  const changelog = path.join(cwd, 'CHANGELOG.md');
  const candidate = '## 5.0.0 (unpublished candidate)\n\n### Features\n\n- Ready.\n\n## 4.1.1\n';
  fs.writeFileSync(changelog, candidate);
  assert.throws(
    () => mergeCandidateChangelog('5.0.0', { notes: 'invalid' }, cwd),
    /did not generate a 5\.0\.0 notes heading/,
  );
  assert.equal(fs.readFileSync(changelog, 'utf8'), candidate);
  assert.equal(mergeCandidateChangelog('5.0.1', { notes: 'invalid' }, cwd), false);
  fs.writeFileSync(changelog, '## [5.0.0] Already released\n');
  assert.throws(
    () => mergeCandidateChangelog('5.0.0', { notes: '## [5.0.0] Generated notes\n' }, cwd),
    /expected the reviewed 5\.0\.0 candidate/,
  );
});
