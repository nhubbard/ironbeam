# Phase 0 portable all-feature coverage

Generated October 10, 2026 from the unchanged v4 source tree plus three behavior-corpus tests.

Command: `cargo llvm-cov --workspace --all-features --branch --summary-only -- --skip io::mongodb --skip io::sql_containers`.

The skipped modules require Docker on this host; their instrumented lines remain in the denominator and show zero execution where no other tests cover them. The columns are LLVM regions, functions, lines, and branches, each with total/missed/covered percent.

```text
Filename                        Regions    Missed Regions     Cover   Functions  Missed Functions  Executed       Lines      Missed Lines     Cover    Branches   Missed Branches     Cover
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
bloom_filter.rs                      34                 0   100.00%           6                 0   100.00%          27                 0   100.00%           0                 0         -
checkpoint.rs                       197                12    93.91%          34                 0   100.00%         190                 0   100.00%          28                 3    89.29%
coders.rs                            96                18    81.25%          23                 6    73.91%          90                11    87.78%           0                 0         -
collection.rs                       183                 4    97.81%          38                 1    97.37%         184                 4    97.83%           8                 1    87.50%
combiners/basic.rs                   76                 2    97.37%          18                 0   100.00%          74                 2    97.30%          12                 2    83.33%
combiners/collect.rs                 49                 0   100.00%          16                 0   100.00%          48                 0   100.00%           0                 0         -
combiners/count.rs                   18                 0   100.00%           6                 0   100.00%          18                 0   100.00%           0                 0         -
combiners/distinct.rs               150                 3    98.00%          33                 0   100.00%         160                 3    98.12%          32                 5    84.38%
combiners/latest.rs                  25                 4    84.00%           5                 0   100.00%          25                 4    84.00%           6                 3    50.00%
combiners/quantiles.rs              216                 7    96.76%          28                 0   100.00%         187                 7    96.26%          38                 6    84.21%
combiners/sampling.rs               108                 7    93.52%          10                 0   100.00%          94                 6    93.62%          32                 8    75.00%
combiners/statistical.rs             35                 0   100.00%           9                 0   100.00%          35                 0   100.00%           2                 0   100.00%
combiners/topk.rs                   111                 0   100.00%          12                 0   100.00%          97                 0   100.00%          38                 3    92.11%
extensions.rs                         3                 0   100.00%           1                 0   100.00%           5                 0   100.00%           0                 0         -
helpers/arrow_ipc.rs                101                42    58.42%          15                 6    60.00%          99                21    78.79%           8                 5    37.50%
helpers/avro.rs                      55                16    70.91%           7                 3    57.14%          61                 6    90.16%           4                 1    75.00%
helpers/basic.rs                     35                 0   100.00%          10                 0   100.00%          36                 0   100.00%           0                 0         -
helpers/batches.rs                   57                 0   100.00%           7                 0   100.00%          60                 0   100.00%           0                 0         -
helpers/cbor.rs                      53                11    79.25%           7                 2    71.43%          52                 3    94.23%           4                 0   100.00%
helpers/cloud.rs                    119                11    90.76%          31                 0   100.00%         159                 6    96.23%           0                 0         -
helpers/co_gbk.rs                    15                 0   100.00%           3                 0   100.00%          19                 0   100.00%           0                 0         -
helpers/collect_sorted.rs            22                 3    86.36%           4                 0   100.00%          20                 0   100.00%           0                 0         -
helpers/collect_values.rs            18                 0   100.00%           5                 0   100.00%          19                 0   100.00%           0                 0         -
helpers/combine.rs                  119                15    87.39%          18                 2    88.89%         136                15    88.97%           2                 1    50.00%
helpers/combine_global.rs            94                 2    97.87%          12                 2    83.33%         118                 2    98.31%           0                 0         -
helpers/common.rs                    58                 3    94.83%           8                 1    87.50%          75                 3    96.00%           0                 0         -
helpers/count.rs                     11                 0   100.00%           3                 0   100.00%          12                 0   100.00%           0                 0         -
helpers/csv.rs                       58                19    67.24%           7                 3    57.14%          63                 8    87.30%           4                 2    50.00%
helpers/dead_letter.rs               42                 0   100.00%           9                 0   100.00%          50                 0   100.00%           0                 0         -
helpers/display.rs                    5                 0   100.00%           2                 0   100.00%           4                 0   100.00%           0                 0         -
helpers/distinct.rs                  48                 0   100.00%          15                 0   100.00%          46                 0   100.00%           0                 0         -
helpers/filter.rs                    57                 0   100.00%          18                 0   100.00%          57                 0   100.00%           4                 0   100.00%
helpers/flatten.rs                   57                 3    94.74%           8                 1    87.50%          62                 2    96.77%           2                 0   100.00%
helpers/joins.rs                    296                18    93.92%          24                 3    87.50%         330                21    93.64%           6                 0   100.00%
helpers/jsonl.rs                     53                16    69.81%           7                 3    57.14%          52                 6    88.46%           4                 1    75.00%
helpers/keyed.rs                     58                 1    98.28%          15                 0   100.00%          65                 0   100.00%           0                 0         -
helpers/latest.rs                     7                 0   100.00%           2                 0   100.00%           6                 0   100.00%           0                 0         -
helpers/log_elements.rs              26                 0   100.00%           5                 0   100.00%          33                 0   100.00%           0                 0         -
helpers/mongodb.rs                   40                40     0.00%           4                 4     0.00%          49                49     0.00%           0                 0         -
helpers/msgpack.rs                   53                11    79.25%           7                 2    71.43%          52                 3    94.23%           4                 0   100.00%
helpers/named.rs                      6                 0   100.00%           1                 0   100.00%           5                 0   100.00%           0                 0         -
helpers/parquet.rs                   76                33    56.58%          10                 6    40.00%          70                19    72.86%           8                 2    75.00%
helpers/partition.rs                  4                 0   100.00%           1                 0   100.00%           4                 0   100.00%           0                 0         -
helpers/protobuf.rs                  53                11    79.25%           7                 2    71.43%          52                 3    94.23%           4                 0   100.00%
helpers/regex.rs                     53                 2    96.23%          15                 0   100.00%          50                 0   100.00%           0                 0         -
helpers/reshuffle.rs                 23                 0   100.00%           3                 0   100.00%          27                 0   100.00%           4                 1    75.00%
helpers/sampling.rs                  41                 0   100.00%          11                 0   100.00%          32                 0   100.00%           0                 0         -
helpers/side_inputs.rs               71                 0   100.00%          20                 0   100.00%          91                 0   100.00%           0                 0         -
helpers/sql.rs                       36                 4    88.89%           4                 0   100.00%          49                 0   100.00%           0                 0         -
helpers/statistical.rs               18                 0   100.00%           4                 0   100.00%          20                 0   100.00%           0                 0         -
helpers/stdlib.rs                    29                 0   100.00%           6                 0   100.00%          50                 0   100.00%           0                 0         -
helpers/tee.rs                       15                 0   100.00%           2                 0   100.00%          14                 0   100.00%           2                 0   100.00%
helpers/tfrecord.rs                  64                21    67.19%          10                 4    60.00%          63                10    84.13%           4                 1    75.00%
helpers/timestamped.rs               17                 0   100.00%           6                 0   100.00%          14                 0   100.00%           0                 0         -
helpers/topk.rs                      16                 0   100.00%           4                 0   100.00%          12                 0   100.00%           0                 0         -
helpers/try_process.rs               19                 2    89.47%           5                 0   100.00%          24                 1    95.83%           0                 0         -
helpers/tumbling.rs                  26                 0   100.00%           6                 0   100.00%          24                 0   100.00%           0                 0         -
helpers/validation.rs                67                 3    95.52%           9                 0   100.00%          91                 3    96.70%           4                 2    50.00%
helpers/values.rs                    22                 0   100.00%           2                 0   100.00%          27                 0   100.00%           0                 0         -
helpers/wait_on.rs                   68                 0   100.00%           8                 0   100.00%          68                 0   100.00%           4                 1    75.00%
helpers/windowed_combine.rs          58                 0   100.00%          12                 0   100.00%          92                 0   100.00%           0                 0         -
helpers/xml.rs                       54                16    70.37%           7                 3    57.14%          52                 6    88.46%           4                 1    75.00%
io/arrow_ipc.rs                     324                74    77.16%          43                11    74.42%         264                23    91.29%          22                 6    72.73%
io/avro.rs                          252                84    66.67%          35                15    57.14%         231                35    84.85%          14                 4    71.43%
io/cbor.rs                          219                40    81.74%          30                 8    73.33%         179                15    91.62%          24                 6    75.00%
io/cloud/fake.rs                    890               264    70.34%         183                58    68.31%        1106               303    72.60%          42                19    54.76%
io/cloud/readers.rs                 326               197    39.57%          37                21    43.24%         292               165    43.49%          26                 8    69.23%
io/cloud/traits.rs                   55                18    67.27%          12                 6    50.00%          55                18    67.27%           2                 0   100.00%
io/cloud/utils.rs                   163                58    64.42%          22                 8    63.64%         190                64    66.32%          42                14    66.67%
io/compression.rs                   149                20    86.58%          32                 7    78.12%         134                16    88.06%          20                 2    90.00%
io/csv.rs                           210                53    74.76%          27                12    55.56%         191                26    86.39%          16                 4    75.00%
io/glob.rs                           26                 9    65.38%           4                 2    50.00%          20                 4    80.00%           4                 1    75.00%
io/jsonl.rs                         223                61    72.65%          27                12    55.56%         173                22    87.28%          20                 5    75.00%
io/mongodb.rs                       216               216     0.00%          37                37     0.00%         235               235     0.00%          12                12     0.00%
io/msgpack.rs                       224                42    81.25%          30                 8    73.33%         190                17    91.05%          24                 6    75.00%
io/parquet.rs                       173                35    79.77%          20                 3    85.00%         151                 7    95.36%          14                 2    85.71%
io/protobuf.rs                      306                80    73.86%          38                12    68.42%         256                56    78.12%          26                10    61.54%
io/sql.rs                           205                45    78.05%          39                11    71.79%         201                28    86.07%          12                 4    66.67%
io/tfrecord.rs                      326                61    81.29%          36                 9    75.00%         270                27    90.00%          26                 6    76.92%
io/xml.rs                           188                62    67.02%          32                16    50.00%         159                31    80.50%          14                 5    64.29%
metrics.rs                          251                23    90.84%          39                 3    92.31%         247                15    93.93%          14                 4    71.43%
node.rs                              21                 0   100.00%           7                 0   100.00%          21                 0   100.00%           0                 0         -
node_id.rs                            6                 0   100.00%           2                 0   100.00%           6                 0   100.00%           0                 0         -
pipeline.rs                         121                 0   100.00%          22                 0   100.00%         131                 0   100.00%           8                 0   100.00%
planner.rs                          990                95    90.40%          65                 4    93.85%         927                62    93.31%         184                25    86.41%
runner.rs                           640               279    56.41%          53                20    62.26%         550               210    61.82%          88                38    56.82%
spill.rs                            231                51    77.92%          37                 5    86.49%         229                33    85.59%          30                13    56.67%
spill_integration.rs                 55                14    74.55%           9                 0   100.00%          58                10    82.76%          16                 7    56.25%
testing.rs                           36                22    38.89%           7                 3    57.14%          32                16    50.00%           0                 0         -
testing/assertions.rs               152                15    90.13%          19                 1    94.74%         150                19    87.33%          18                 3    83.33%
testing/builders.rs                 109                43    60.55%          21                 9    57.14%         104                41    60.58%           4                 0   100.00%
testing/debug.rs                     91                 0   100.00%          11                 0   100.00%         104                 0   100.00%           4                 0   100.00%
testing/fixtures.rs                 179                89    50.28%          10                 5    50.00%         173                91    47.40%           0                 0         -
testing/mock_io.rs                  111                47    57.66%          14                 4    71.43%          92                32    65.22%           4                 2    50.00%
type_token.rs                        27                 1    96.30%           7                 0   100.00%          28                 0   100.00%           4                 1    75.00%
utils.rs                             12                 0   100.00%           4                 0   100.00%          12                 0   100.00%           0                 0         -
validation.rs                       143                 7    95.10%          21                 0   100.00%         131                 1    99.24%          34                 3    91.18%
window.rs                            42                 1    97.62%           8                 0   100.00%          37                 1    97.30%           6                 2    66.67%
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
TOTAL                             11386              2466    78.34%        1685               364    78.40%       11229              1847    83.55%        1046               261    75.05%
```

