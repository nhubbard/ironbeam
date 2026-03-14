## [2.2.0](https://github.com/nhubbard/ironbeam/compare/v2.1.0...v2.2.0) (2026-03-14)

### Features

* Add to_hashmap function ([643f886](https://github.com/nhubbard/ironbeam/commit/643f886ca3dad7e5426a6d2dac469072f1b99998))
* Add with_constant_key and with_keys helper methods ([903a18c](https://github.com/nhubbard/ironbeam/commit/903a18ca308eb1e8c756d1c944ec3b507018f299))

### Bug Fixes

* Clippy errors ([e50fc54](https://github.com/nhubbard/ironbeam/commit/e50fc54c02533c30d4451bbee51c7441376be7aa))

## [2.1.0](https://github.com/nhubbard/ironbeam/compare/v2.0.0...v2.1.0) (2026-03-08)

### Features

* Enhanced partitioning and filtering helpers ([cc6ecaf](https://github.com/nhubbard/ironbeam/commit/cc6ecaf7796707afe25e3581e6ab4313e44355b0))
* Flatten transform ([c282bbc](https://github.com/nhubbard/ironbeam/commit/c282bbc4b6ff389cab85559717264477d39ddc41))

### Bug Fixes

* Test error on x86 related to shard length calculation ([116f084](https://github.com/nhubbard/ironbeam/commit/116f08433e0b6efbb2a48e6b2be2c43d19e55e90))
* Test error on x86 related to shard length calculation (attempt 2) ([e2462e3](https://github.com/nhubbard/ironbeam/commit/e2462e36cbc269c3da60486ddb461187368ecb97))

## [2.0.0](https://github.com/nhubbard/ironbeam/compare/v1.1.0...v2.0.0) (2026-03-07)

### ⚠ BREAKING CHANGES

* Bump version to 1.2.0, replace bincode with postcard

### release

* Bump version to 1.2.0, replace bincode with postcard ([d2396d6](https://github.com/nhubbard/ironbeam/commit/d2396d6d437fdbf2853558af87f4fa730c924bd3))

### Features

* **spill:** Add resource spilling support ([849e429](https://github.com/nhubbard/ironbeam/commit/849e429e4c1fbb8148416bdbe68b7919eed3f84c))

### Bug Fixes

* Bump arrow and tempfiles version ([c80f3eb](https://github.com/nhubbard/ironbeam/commit/c80f3eb2c3530113048605cc4ddb622ceea7a6c0))
* Fix clippy errors (may cause test failure in CI?) ([8765b3d](https://github.com/nhubbard/ironbeam/commit/8765b3dfb1b76275855ddcd1243372ff71e212a3))

## [1.1.0](https://github.com/nhubbard/ironbeam/compare/v1.0.1...v1.1.0) (2025-12-06)

### Features

* **cloud:** Improved cloud I/O read helpers, more to come soon; documentation, test, and linting cleanup ([ade9629](https://github.com/nhubbard/ironbeam/commit/ade96293759bd7918beaa0c2e4093f7dfca6cfa2))

## [1.1.0](https://github.com/nhubbard/ironbeam/compare/v1.0.0...v1.0.1) (2025-11-27)

### Features

* Add generic cloud I/O support

### Enhancements

* Clippy issues ([abdae35](https://github.com/nhubbard/ironbeam/commit/abdae359a3bb6f108823d8fd22122ffc7558b36f))

## 1.0.0 (2025-11-15)

Initial release!

### Features

* **checkpoint:** Add checkpoint support ([c0aac7e](https://github.com/nhubbard/ironbeam/commit/c0aac7e237e844a74c6b1f6115c9e2e4aa02b4b9))
* **checkpoint:** Fix checkpoint tests ([4bae5d3](https://github.com/nhubbard/ironbeam/commit/4bae5d386c4d8daf789861d42606dbf6352ad402))
* **compression:** Add compressed I/O support ([727c33e](https://github.com/nhubbard/ironbeam/commit/727c33e7d89d98decced8b39fe34f60454ff4e57))
* **explain:** Add optimization explanation feature ([0964204](https://github.com/nhubbard/ironbeam/commit/09642043d79a83d9bd5443a860fb6a396be2de7d))
* **ext:** Extensible pipelines ([7d2aa0d](https://github.com/nhubbard/ironbeam/commit/7d2aa0d2c53e2c6281d5324423fff671686b2225))
* **glob:** Add globbing support ([1bbc039](https://github.com/nhubbard/ironbeam/commit/1bbc0396dbfae0d09b446d0a136fe01e8a02008d))
* **global_combine:** Add distinct count and K-minimum values support ([0434a96](https://github.com/nhubbard/ironbeam/commit/0434a96e579cb7892285983c6a630ac11b5f2f4e))
* **global_combine:** Add global combine support ([345acc0](https://github.com/nhubbard/ironbeam/commit/345acc0a25344315a457bf967a3be8ffbfb99d4d))
* **metrics:** Add standard and custom Metric support ([6ea8c56](https://github.com/nhubbard/ironbeam/commit/6ea8c5607151dc4c6d3c31576f1250b9d4ab9cd3))
* **quantiles:** Add quantiles support ([acbf1e4](https://github.com/nhubbard/ironbeam/commit/acbf1e4f19692a2a91a12b4485a66ec6e8c8f785))
* **sampling:** Add sampling support ([7555e81](https://github.com/nhubbard/ironbeam/commit/7555e817ec7fdb27506251cfb701fe1cfaf09b7a))
* **test:** Add testing utilities ([7b5c087](https://github.com/nhubbard/ironbeam/commit/7b5c0876373e1f7d91137e7d17a6c05d77fd5ea4))
* **topk:** Add top-K convenience helpers ([1d805cf](https://github.com/nhubbard/ironbeam/commit/1d805cf851e492ee3701c66bece3df8a809eda54))
* **validation:** Add validation support ([06fa70a](https://github.com/nhubbard/ironbeam/commit/06fa70a44e3e34c86ea0839e99e07111eca810f5))
