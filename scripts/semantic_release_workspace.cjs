// Semantic Release adapter for the versioned Ironbeam workspace.
const { execFileSync } = require('node:child_process');
const { mergeCandidateChangelog } = require('./merge_release_changelog.cjs');

function run(command, args, options = {}) {
  execFileSync(command, args, { stdio: 'inherit', ...options });
}

module.exports = {
  verifyConditions() {
    if (!process.env.CARGO_REGISTRY_TOKEN) {
      throw new Error('crates.io Trusted Publishing did not provide CARGO_REGISTRY_TOKEN.');
    }
  },
  prepare(_config, context) {
    const version = context.nextRelease.version;
    run('python3', ['scripts/prepare_workspace_release.py', version]);
    run('python3', ['scripts/check_v5_release_order.py', '--expected-version', version]);
    mergeCandidateChangelog(version, context.nextRelease);
  },
  publish(_config, context) {
    const version = context.nextRelease.version;
    run('bash', ['scripts/publish_v5_packages.sh', 'publish'], {
      env: {
        ...process.env,
        IRONBEAM_RELEASE_VERSION: version,
        IRONBEAM_RELEASE_CONFIRM: `publish-${version}`,
      },
    });
    return { name: 'Ironbeam workspace', url: `https://crates.io/crates/ironbeam/${version}` };
  },
};
