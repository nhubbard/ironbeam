## [3.2.0](https://github.com/nhubbard/ironbeam/compare/v3.1.0...v3.2.0) (2026-06-20)

### Features

* **msgpack:** add opt-in MessagePack I/O connector (feature 5.1) ([9aeff2f](https://github.com/nhubbard/ironbeam/commit/9aeff2f2467866813d8fb539ee20bf530980b316))

### Bug Fixes

* **features:** make every feature permutation compile (ABI + runtime stubs) ([9cfab9e](https://github.com/nhubbard/ironbeam/commit/9cfab9e68488a5a892561711f0a5a2035119e606))

### Code Refactoring

* remove LiftableCombiner trait ([404f7df](https://github.com/nhubbard/ironbeam/commit/404f7dfcf72f0b3464f6ab274f6f833b85c1f200))

## [3.1.0](https://github.com/nhubbard/ironbeam/compare/v3.0.0...v3.1.0) (2026-05-25)

### Features

* **batches:** add batch_elements / batch_by_size (feature 4.7) ([a7a0496](https://github.com/nhubbard/ironbeam/commit/a7a0496ab74bf114366eb23b8430ff3487d8addf))
* **batches:** add group_into_batches() for PCollection<(K, V)> (feature 4.6) ([c3f4666](https://github.com/nhubbard/ironbeam/commit/c3f466655ca364eb3adf45376be2a8ad11f34624))
* **collection:** add keys() and values() for PCollection<(K, V)> (feature 4.1) ([f9939fb](https://github.com/nhubbard/ironbeam/commit/f9939fbafa60bb2a81c5aab471d691e65840f51b))
* **collection:** add kv_swap() for PCollection<(K, V)> (feature 4.2) ([10484b4](https://github.com/nhubbard/ironbeam/commit/10484b47fcf6109c4d4419e63cf2943fa1494f6b))
* **combiners:** add Mean<O> typed combiner + mean_*_globally/per_key (feature 4.3) ([59b3871](https://github.com/nhubbard/ironbeam/commit/59b387188cf7fe0eecdb65385cafb8efd7413b6c))
* **combiners:** add ToDict combiner + to_dict() helper (feature 4.5) ([4b969f9](https://github.com/nhubbard/ironbeam/commit/4b969f9d34fd1581d8bef80d556e4ebce40adecb))
* **dead-letter:** add DeadLetter type + map_catching / flat_map_catching (feature 4.15) ([4616baf](https://github.com/nhubbard/ironbeam/commit/4616baf9d624b035e8117902ae3181828dc985be))
* **debug:** add log_elements / log_elements_with passthrough tap (feature 4.10) ([c4fd779](https://github.com/nhubbard/ironbeam/commit/c4fd779f8786e71991fd5280ae3e43c931e5367e))
* **display:** add to_display_string() for PCollection<T: Display> (feature 4.8) ([a16e241](https://github.com/nhubbard/ironbeam/commit/a16e2415d276694814520fc8fd5d9a94b5a6b59f))
* **distinct:** add HyperLogLog++ approx distinct count combiner + helpers (feature 4.13) ([aa67b66](https://github.com/nhubbard/ironbeam/commit/aa67b66ad406b5edc7a790af6054b4a9909fba63))
* **naming:** add Pipeline::named_scope composite-naming helper (feature 4.11) ([046f27a](https://github.com/nhubbard/ironbeam/commit/046f27a7e711e996aa52bc8dbc5c74a9387b5375))
* **naming:** add with_name fluent labeller + Pipeline name accessors (feature 4.11) ([863ab4b](https://github.com/nhubbard/ironbeam/commit/863ab4b16af3e1172fabf87826810ece0ea6f974))
* **naming:** per-step name annotation in ExplainStep / explain output (feature 4.11) ([2543cb4](https://github.com/nhubbard/ironbeam/commit/2543cb4790ecc8994a99a2df40f6aa70bc9e35fc))
* **sample:** add Beam-style sample_globally / sample_per_key helpers (feature 4.14) ([daf113b](https://github.com/nhubbard/ironbeam/commit/daf113b7a80c57af64caf3c933e204c0505bb850))
* **tee:** add tee() / tee_n() fan-out helpers (feature 4.9) ([fc9ccac](https://github.com/nhubbard/ironbeam/commit/fc9ccac7ce9dd01a761d94c9a8461f04fcc8854e))
* **wait_on:** add PCollection::wait_on signal-only dependency barrier (feature 4.12) ([4aee2c3](https://github.com/nhubbard/ironbeam/commit/4aee2c31f22d987699c83ca0bef80febcd49830a))

### Documentation

* **plan:** reorganize tiers — Tier 4 = non-I/O, Tier 5 = all I/O formats ([1473df4](https://github.com/nhubbard/ironbeam/commit/1473df4d6be40803b6abd5bc443e5a88a6b81537))

## [3.0.0](https://github.com/nhubbard/ironbeam/compare/v2.11.0...v3.0.0) (2026-05-16)

### Features

* **helpers:** implement 3.3 Reify — reify_timestamps() on PCollection<Timestamped<T>> ([8e699b8](https://github.com/nhubbard/ironbeam/commit/8e699b85825919135f66e0b9c2d4a79ffed8c818))
* **planner:** implement 3.10 Tree Reduction for Associative Combiners ([96b04d4](https://github.com/nhubbard/ironbeam/commit/96b04d48bbb520f360f1f69a968d37c005eef13b))
* **planner:** implement 3.11 Early Termination / Limit Pushdown ([5f24986](https://github.com/nhubbard/ironbeam/commit/5f24986f9ea58e6dcbc62e00a978a64db959ee9d))
* **planner:** implement 3.12 Bloom Filter Semi-Join for CoGroup ([c1405df](https://github.com/nhubbard/ironbeam/commit/c1405dfcee8facc687c00966e1b8217fae3fe484))
* **planner:** implement 3.13 Dominator-Based Cache Placement ([c24fc13](https://github.com/nhubbard/ironbeam/commit/c24fc13264632b40cfc53944e520371c1e93c293))
* **planner:** implement 3.14 Adaptive Inter-Stage Partition Count ([f028a23](https://github.com/nhubbard/ironbeam/commit/f028a23a6e1bea9486e0a9454298fe71ec3c7228))
* **planner:** implement 3.5 Reshuffle Elimination — eliminate_reshuffle_pass() ([92804cb](https://github.com/nhubbard/ironbeam/commit/92804cbee4015d246c4b9b8ec153064ec5fac651))
* **planner:** implement 3.6 Predicate Pushdown Past Reshuffle ([710a116](https://github.com/nhubbard/ironbeam/commit/710a1164a735dbba39e956925e48df1617896bb2))
* **planner:** implement 3.7 Flatten Input Predicate Pushdown ([d8f3502](https://github.com/nhubbard/ironbeam/commit/d8f3502408cb4d041592c0c54badd4c8364bd5af))
* **planner:** implement 3.8 Dead Subtree Elimination — prune_dead_subtrees() ([eff3b17](https://github.com/nhubbard/ironbeam/commit/eff3b179fa7c25ce72dd6e25ed10fd3e00d25a3d))
* **planner:** implement 3.9 CoGroup Join Ordering — reorder_cogroup_inputs_pass() ([ad9b0d8](https://github.com/nhubbard/ironbeam/commit/ad9b0d8e46aec2b5751134847e16702a55472893))
* **testing:** implement 3.4 PAssert — fluent assertion builder ([aab32f4](https://github.com/nhubbard/ironbeam/commit/aab32f47b1ece18d540e871f583c261509084725))

### Bug Fixes

* Clippy issues ([85edb35](https://github.com/nhubbard/ironbeam/commit/85edb350299cae4c4740988037fdaf8e3eca58c5))
* **collection:** remove dead TakeOp::cardinality_reducing() override ([13d4e3b](https://github.com/nhubbard/ironbeam/commit/13d4e3b29629413474773fb8229610dfb35687de))
* **docs:** add missing # Panics / # Errors sections to satisfy clippy::pedantic ([885aa21](https://github.com/nhubbard/ironbeam/commit/885aa2179613ee5cc73d65f21f14c0f3c21d7bf1))
* **docs:** resolve unresolved doc links for CombineFn::is_associative_commutative ([f43c18a](https://github.com/nhubbard/ironbeam/commit/f43c18aa1782c027bda9b0895cf87c381acaceb2))
* **examples:** add missing PushedDownIntoFlattenSubplans match arm ([1fc90a8](https://github.com/nhubbard/ironbeam/commit/1fc90a884302f1570dcb098b63752cdfd7efb0c7))
* Minor Clippy issues ([65c9319](https://github.com/nhubbard/ironbeam/commit/65c9319fa01de4cadab6ec387171e6c192fc8a43))
* **planner:** exclude Flatten/CoGroup from singleton short-circuit; add coverage tests ([61a2cc0](https://github.com/nhubbard/ironbeam/commit/61a2cc014f763ab8a203277343a0188362cc6ec7))

## [2.11.0](https://github.com/nhubbard/ironbeam/compare/v2.10.0...v2.11.0) (2026-05-09)

### Features

* Implement independent subgraph parallelism ([ccde7e2](https://github.com/nhubbard/ironbeam/commit/ccde7e247fc53bd918bda3f2cb2e6b102b66637b))
* **io:** add XML I/O support with glob, streaming, and parallel write ([7a190ee](https://github.com/nhubbard/ironbeam/commit/7a190ee91af773c02c65b56d6699f708133c0735))
* **io:** add XML I/O support with glob, streaming, and parallel write ([77178e9](https://github.com/nhubbard/ironbeam/commit/77178e9d8fadf2bc9967bda299aee2913c165289))
* **planner,runner:** graph-theory optimizations 2 & 3 ([600633d](https://github.com/nhubbard/ironbeam/commit/600633d6685b47f768e547dcd14645657f3ef74d))

### Bug Fixes

* XML test inefficient error handling ([e2d2830](https://github.com/nhubbard/ironbeam/commit/e2d28309ce366a7ba14376c2aa7c20a7cc9b51cb))
* XML test inefficient error handling ([19126c7](https://github.com/nhubbard/ironbeam/commit/19126c7a51e10182d7c9bd3f280a282faaa1762d))

## [2.10.0](https://github.com/nhubbard/ironbeam/compare/v2.9.0...v2.10.0) (2026-04-25)

### Features

* **helpers:** add read_avro/write_avro pipeline helpers with glob support ([8a80976](https://github.com/nhubbard/ironbeam/commit/8a80976158848c2b010ca8aa74b7bd39d35eb758))
* **helpers:** add regex transforms and windowed combining helpers (2.5 + 2.5b) ([773cf01](https://github.com/nhubbard/ironbeam/commit/773cf01506042b4b0616b2c26fda34f0c8bcd6bc))
* **io:** add Avro I/O layer with streaming shards and parallel write ([4e201b6](https://github.com/nhubbard/ironbeam/commit/4e201b6c68110590b008f8099a19dc9d3723bce6))
* register io-avro feature and export Avro helpers ([cdd0c2b](https://github.com/nhubbard/ironbeam/commit/cdd0c2bec23bffd33322a7a651857f530c290155))

### Bug Fixes

* **test:** Correct Clippy failures ([000cb02](https://github.com/nhubbard/ironbeam/commit/000cb02292ecf31c4dd60b712df90d3ed2da9a66))

### Documentation

* mark 2.5 and 2.5b as complete in feature parity plan ([8ca8fad](https://github.com/nhubbard/ironbeam/commit/8ca8fad26ee37ba3b427c9979b44d2b845edf395))

## [2.9.0](https://github.com/nhubbard/ironbeam/compare/v2.8.0...v2.9.0) (2026-04-18)

### Features

* **combiners:** add BottomK combiner with bottom_k_per_key and bottom_k_globally helpers (2.3) ([ff5cc1a](https://github.com/nhubbard/ironbeam/commit/ff5cc1aa00c6905d6922b834647b6602a83240ba))
* **helpers:** add filter_with_side_map, SideSingleton, SideMultimap, and all map/filter variants (2.4) ([3a3818e](https://github.com/nhubbard/ironbeam/commit/3a3818ef32b4f6bdcffa9d324eb4c83499fb7c37))

### Bug Fixes

* **deps:** upgrade sha2 from 0.10 to 0.11 ([6229a97](https://github.com/nhubbard/ironbeam/commit/6229a9746071b75e4b68eefade6aef9cf3243f32))

### Code Refactoring

* **checkpoint:** use fold+write! for hex encoding ([ee4a296](https://github.com/nhubbard/ironbeam/commit/ee4a296b681f7fa6c771a975cedf7e4d9a92e2a7))

## [2.8.0](https://github.com/nhubbard/ironbeam/compare/v2.7.1...v2.8.0) (2026-04-18)

### Features

* **helpers:** add distinct_by for deduplication by projection (2.2) ([bc27a8f](https://github.com/nhubbard/ironbeam/commit/bc27a8fe8abda8dbb8441d332513288989c8311d))

## [2.7.1](https://github.com/nhubbard/ironbeam/compare/v2.7.0...v2.7.1) (2026-04-13)

### Bug Fixes

* Ordered float doctest failure (1/2) ([f039a7d](https://github.com/nhubbard/ironbeam/commit/f039a7df5c26566484da7f3899f681ea40be2aa8))
* Ordered float doctest failure (2/2) ([dbefbae](https://github.com/nhubbard/ironbeam/commit/dbefbaee2938d8dc4116c077da31c1c907a47c72))

## [2.7.0](https://github.com/nhubbard/ironbeam/compare/v2.6.0...v2.7.0) (2026-04-11)

### Features

* trigger release workflow after CI passes via workflow_run ([4aa6d54](https://github.com/nhubbard/ironbeam/commit/4aa6d54f0f06797417de86ec2c04ada3293e1484))

### Bug Fixes

* switch release workflow to workflow_dispatch trigger ([fa6f704](https://github.com/nhubbard/ironbeam/commit/fa6f7045b4186470acf08ec612ee0e9948451d96))

## [2.6.0](https://github.com/nhubbard/ironbeam/compare/v2.5.0...v2.6.0) (2026-04-11)

### Features

* add convenience methods for all combiners (task 2.0) ([e29bf96](https://github.com/nhubbard/ironbeam/commit/e29bf9670f4aac74812e32779a295e0df1586081))

### Bug Fixes

* add crates-io-auth-action for trusted publishing OIDC exchange ([1e3b518](https://github.com/nhubbard/ironbeam/commit/1e3b518a711e57441616e41816c0925aa11fd79e))
* remove inapplicable artifact attestation from release workflow ([c7f10a7](https://github.com/nhubbard/ironbeam/commit/c7f10a77b67bef3a96d0dbcbb0c0e96e2f0f42ab))

## [2.5.0](https://github.com/nhubbard/ironbeam/compare/v2.4.0...v2.5.0) (2026-03-21)

### Features

* add Latest combiner for timestamped values ([186782f](https://github.com/nhubbard/ironbeam/commit/186782f91af8cfe7424d074d240a1011cc776d36))

## [2.4.0](https://github.com/nhubbard/ironbeam/compare/v2.3.0...v2.4.0) (2026-03-21)

### ⚠ BREAKING CHANGES

* None - this is purely additive functionality

### Features

* add Count, ToList, and ToSet combiners ([93c533a](https://github.com/nhubbard/ironbeam/commit/93c533a0a8e2b6af600e72560071838a5a3b9323))

## [2.3.0](https://github.com/nhubbard/ironbeam/compare/v2.2.0...v2.3.0) (2026-03-21)

### Features

* Add N-way CoGroupByKey with macro-generated implementations ([d777c32](https://github.com/nhubbard/ironbeam/commit/d777c3266a64a3d0c9b99c1202f730d70d7c7bcc))

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
