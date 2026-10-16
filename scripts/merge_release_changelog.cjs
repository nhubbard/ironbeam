const fs = require('node:fs');
const path = require('node:path');

// Keep the reviewed 5.0.0 summary while allowing semantic-release to write
// one final version entry with its generated commit history.
function mergeCandidateChangelog(version, nextRelease, cwd = process.cwd()) {
  if (version !== '5.0.0') return false;

  const changelog = path.join(cwd, 'CHANGELOG.md');
  const current = fs.readFileSync(changelog, 'utf8');
  const candidateHeading = '## 5.0.0 (unpublished candidate)';
  if (!current.startsWith(`${candidateHeading}\n`)) {
    throw new Error('expected the reviewed 5.0.0 candidate at the start of CHANGELOG.md');
  }

  const nextHeading = current.slice(candidateHeading.length).search(/^## /m);
  if (nextHeading < 0) {
    throw new Error('5.0.0 candidate changelog has no preceding release entry');
  }
  const boundary = candidateHeading.length + nextHeading;
  const candidate = current.slice(candidateHeading.length, boundary).trim();
  const previousReleases = current.slice(boundary);
  const summary = candidate
    .split('\nPublication is held.')[0]
    .trim()
    .replace(/^### /gm, '#### ');
  if (!summary) throw new Error('the reviewed 5.0.0 release summary is empty');

  const notes = nextRelease.notes;
  const firstBreak = notes.indexOf('\n');
  if (firstBreak < 0 || !notes.startsWith('## [5.0.0]')) {
    throw new Error('semantic-release did not generate a 5.0.0 notes heading');
  }
  nextRelease.notes = `${notes.slice(0, firstBreak)}\n\n### Release summary\n\n${summary}\n\n${notes.slice(firstBreak).trimStart()}`;
  fs.writeFileSync(changelog, previousReleases);
  return true;
}

module.exports = { mergeCandidateChangelog };
