# Concurrent Write Benchmark Comparison

**Date**: 2026-02-19 (re-run with batch=1000 added)
**Platform**: Linux aarch64 (Docker container), compilation_mode=opt
**Total ops per write workload**: 100,000 | **Total read ops**: 10,000

## Branches

| Branch | Description |
|--------|-------------|
| **master** | Baseline (commit 8f11189dd) |
| **opt/loop-inversion** | Flip locks_vs_writes loop to iterate predecessor writes instead of all locks |
| **opt/bloom-filter** | Loop inversion + bloom filter pre-filtering in compute_dependency |

---

## W1: PureInsert (batch=1000)

`insert $p isa person, has name "...", has age N;` x1000 per tx

| Threads | master Ops/s | loop-inv Ops/s | bloom Ops/s | loop-inv vs master | bloom vs master |
|--------:|-------------:|---------------:|------------:|-------------------:|----------------:|
|       1 |       17,006 |         14,813 |      15,207 |            -12.9%  |         -10.6%  |
|       2 |       30,146 |         28,337 |      28,420 |             -6.0%  |          -5.7%  |
|       4 |       58,937 |         59,003 |      57,597 |             +0.1%  |          -2.3%  |
|       8 |       74,990 |         77,627 |      80,741 |             +3.5%  |          +7.7%  |
|      16 |       79,091 |         82,915 |      76,908 |             +4.8%  |          -2.8%  |
|      32 |       72,919 |         74,340 |      71,889 |             +1.9%  |          -1.4%  |

## W1: PureInsert (batch=100)

`insert $p isa person, has name "...", has age N;` x100 per tx

| Threads | master Ops/s | loop-inv Ops/s | bloom Ops/s | loop-inv vs master | bloom vs master |
|--------:|-------------:|---------------:|------------:|-------------------:|----------------:|
|       1 |       13,782 |         13,547 |      13,544 |             -1.7%  |          -1.7%  |
|       2 |       24,803 |         24,496 |      23,701 |             -1.2%  |          -4.4%  |
|       4 |       43,727 |         41,174 |      41,465 |             -5.8%  |          -5.2%  |
|       8 |       66,805 |         64,215 |      69,784 |             -3.9%  |          +4.5%  |
|      16 |       73,941 |         76,544 |      77,959 |             +3.5%  |          +5.4%  |
|      32 |       71,328 |         68,886 |      69,282 |             -3.4%  |          -2.9%  |

## W1: PureInsert (batch=1)

| Threads | master Ops/s | loop-inv Ops/s | bloom Ops/s | loop-inv vs master | bloom vs master |
|--------:|-------------:|---------------:|------------:|-------------------:|----------------:|
|       1 |          355 |            354 |         354 |             -0.3%  |          -0.3%  |
|       2 |          661 |            662 |         663 |             +0.2%  |          +0.3%  |
|       4 |        1,052 |          1,064 |       1,050 |             +1.1%  |          -0.2%  |
|       8 |        1,916 |          1,952 |       1,949 |             +1.9%  |          +1.7%  |
|      16 |        3,791 |          3,859 |       3,809 |             +1.8%  |          +0.5%  |

## W2: PureUpdate (batch=1000)

`match $p isa person, has name "..."; insert $p has score N;` x1000 per tx

| Threads | master Ops/s | loop-inv Ops/s | bloom Ops/s | loop-inv vs master | bloom vs master |
|--------:|-------------:|---------------:|------------:|-------------------:|----------------:|
|       1 |       14,737 |         14,406 |      14,529 |             -2.2%  |          -1.4%  |
|       2 |       28,367 |         28,253 |      27,881 |             -0.4%  |          -1.7%  |
|       4 |       55,634 |         54,607 |      53,794 |             -1.8%  |          -3.3%  |
|       8 |       90,154 |         94,155 |      90,020 |             +4.4%  |          -0.1%  |
|      16 |       93,074 |         92,456 |      83,763 |             -0.7%  |         -10.0%  |
|      32 |       91,826 |         91,332 |      92,584 |             -0.5%  |          +0.8%  |

## W2: PureUpdate (batch=100)

`match $p isa person, has name "..."; insert $p has score N;` x100 per tx

| Threads | master Ops/s | loop-inv Ops/s | bloom Ops/s | loop-inv vs master | bloom vs master |
|--------:|-------------:|---------------:|------------:|-------------------:|----------------:|
|       1 |       11,891 |         11,790 |      11,432 |             -0.8%  |          -3.9%  |
|       2 |       21,640 |         21,968 |      21,710 |             +1.5%  |          +0.3%  |
|       4 |       40,283 |         40,590 |      39,718 |             +0.8%  |          -1.4%  |
|       8 |       65,909 |         66,001 |      67,017 |             +0.1%  |          +1.7%  |
|      16 |       75,934 |         74,862 |      77,092 |             -1.4%  |          +1.5%  |
|      32 |       73,851 |         79,003 |      82,028 |             +7.0%  |         +11.1%  |

## W2: PureUpdate (batch=1)

| Threads | master Ops/s | loop-inv Ops/s | bloom Ops/s | loop-inv vs master | bloom vs master |
|--------:|-------------:|---------------:|------------:|-------------------:|----------------:|
|       1 |          342 |            343 |         342 |             +0.3%  |          +0.0%  |
|       2 |          658 |            652 |         648 |             -0.9%  |          -1.5%  |
|       4 |        1,029 |          1,040 |       1,035 |             +1.1%  |          +0.6%  |
|       8 |        1,934 |          1,943 |       1,940 |             +0.5%  |          +0.3%  |
|      16 |        3,932 |          3,819 |       3,854 |             -2.9%  |          -2.0%  |

## W3: InsertRelation (batch=1000)

`match $a isa person; $b isa person; insert (friend: $a, friend: $b) isa friendship;` x1000 per tx

| Threads | master Ops/s | loop-inv Ops/s | bloom Ops/s | loop-inv vs master | bloom vs master |
|--------:|-------------:|---------------:|------------:|-------------------:|----------------:|
|       1 |        8,130 |          8,127 |       7,973 |             -0.0%  |          -1.9%  |
|       2 |       14,208 |         14,242 |      14,232 |             +0.2%  |          +0.2%  |
|       4 |       23,113 |         23,228 |      22,798 |             +0.5%  |          -1.4%  |
|       8 |       31,686 |         31,589 |      31,747 |             -0.3%  |          +0.2%  |
|      16 |       27,868 |         30,084 |      23,634 |             +7.9%  |         -15.2%  |
|      32 |       54,530 |         55,127 |      14,560 |             +1.1%  |      **-73.3%** |

**Note**: Bloom filter shows severe regression at 16-32T batch=1000. The bloom filter overhead dominates when each transaction touches 1000 relation keys, causing extreme exec time inflation (1.9s avg exec at 32T bloom vs 377ms master).

## W3: InsertRelation (batch=100)

`match $a isa person; $b isa person; insert (friend: $a, friend: $b) isa friendship;` x100 per tx

| Threads | master Ops/s | loop-inv Ops/s | bloom Ops/s | loop-inv vs master | bloom vs master |
|--------:|-------------:|---------------:|------------:|-------------------:|----------------:|
|       1 |        7,786 |          7,763 |       7,700 |             -0.3%  |          -1.1%  |
|       2 |       14,577 |         14,196 |      14,359 |             -2.6%  |          -1.5%  |
|       4 |       26,406 |         26,393 |      25,951 |             -0.0%  |          -1.7%  |
|       8 |       44,312 |         43,304 |      44,009 |             -2.3%  |          -0.7%  |
|      16 |       48,035 |         45,776 |      45,219 |             -4.7%  |          -5.9%  |
|      32 |       49,018 |         51,016 |      48,783 |             +4.1%  |          -0.5%  |

## W3: InsertRelation (batch=1)

| Threads | master Ops/s | loop-inv Ops/s | bloom Ops/s | loop-inv vs master | bloom vs master |
|--------:|-------------:|---------------:|------------:|-------------------:|----------------:|
|       1 |          344 |            344 |         345 |             +0.0%  |          +0.3%  |
|       2 |          653 |            650 |         651 |             -0.5%  |          -0.3%  |
|       4 |        1,036 |          1,053 |       1,048 |             +1.6%  |          +1.2%  |
|       8 |        1,951 |          1,967 |       1,973 |             +0.8%  |          +1.1%  |
|      16 |        4,101 |          4,031 |       4,096 |             -1.7%  |          -0.1%  |

## W4: Mixed50Write (batch=1000, 50% write / 50% read)

| Threads | master W-ops/s | loop-inv W-ops/s | bloom W-ops/s | master R-ops/s | loop-inv R-ops/s | bloom R-ops/s |
|--------:|---------------:|-----------------:|--------------:|---------------:|-----------------:|--------------:|
|  2 (1W+1R) |         7,996 |           7,938 |        7,831 |         4,394 |           4,281 |        4,212 |
|  4 (2W+2R) |        13,734 |          13,700 |       13,894 |         8,325 |           8,156 |        8,406 |
|  8 (4W+4R) |        21,988 |          20,604 |       22,007 |        13,984 |          14,781 |       15,297 |
| 16 (8W+8R) |        18,660 |          20,799 |       17,820 |        12,075 |          14,621 |       14,799 |
| 32 (16W+16R) |       12,223 |          37,256 |       34,708 |        16,670 |          12,453 |       14,266 |

**Note**: 32T result shows high variance across runs. The large apparent gains at 32T batch=1000 are likely dominated by exec-time variance in the relation match queries, not isolation improvements.

## W4: Mixed50Write (batch=100, 50% write / 50% read)

| Threads | master W-ops/s | loop-inv W-ops/s | bloom W-ops/s | master R-ops/s | loop-inv R-ops/s | bloom R-ops/s |
|--------:|---------------:|-----------------:|--------------:|---------------:|-----------------:|--------------:|
|  2 (1W+1R) |         7,521 |           7,656 |        7,595 |         3,690 |           3,758 |        3,732 |
|  4 (2W+2R) |        14,242 |          14,025 |       13,830 |         6,648 |           6,633 |        6,618 |
|  8 (4W+4R) |        25,348 |          24,666 |       25,311 |        12,015 |          11,238 |       12,166 |
| 16 (8W+8R) |        36,429 |          37,793 |       38,092 |         6,515 |           7,626 |        6,908 |
| 32 (16W+16R) |       38,715 |          35,555 |       36,713 |         7,126 |           8,091 |        7,755 |

## W5: Mixed20Write (batch=1000, 20% write / 80% read)

| Threads | master W-ops/s | loop-inv W-ops/s | bloom W-ops/s | master R-ops/s | loop-inv R-ops/s | bloom R-ops/s |
|--------:|---------------:|-----------------:|--------------:|---------------:|-----------------:|--------------:|
|  2 (1W+1R) |         8,101 |           8,024 |        7,904 |         4,405 |           4,248 |        4,247 |
|  4 (1W+3R) |         7,833 |           7,966 |        7,736 |        12,744 |          12,690 |       12,320 |
|  8 (2W+6R) |        13,201 |          12,449 |       12,902 |        23,523 |          22,971 |       23,492 |
| 16 (3W+13R) |        10,880 |          10,433 |       10,058 |        25,405 |          25,927 |       26,775 |
| 32 (6W+26R) |         9,773 |           9,550 |        9,342 |        25,147 |          26,289 |       23,217 |

## W5: Mixed20Write (batch=100, 20% write / 80% read)

| Threads | master W-ops/s | loop-inv W-ops/s | bloom W-ops/s | master R-ops/s | loop-inv R-ops/s | bloom R-ops/s |
|--------:|---------------:|-----------------:|--------------:|---------------:|-----------------:|--------------:|
|  2 (1W+1R) |         7,615 |           7,705 |        7,735 |         3,723 |           3,780 |        3,748 |
|  4 (1W+3R) |         7,448 |           7,355 |        7,375 |        10,760 |          10,648 |       10,784 |
|  8 (2W+6R) |        13,596 |          13,506 |       13,374 |        19,137 |          19,094 |       18,810 |
| 16 (3W+13R) |        15,184 |          16,589 |       13,830 |        20,543 |          18,530 |       20,439 |
| 32 (6W+26R) |        21,384 |          20,467 |       23,256 |        17,697 |          18,436 |       16,822 |

## W6: PureRead (batch=1)

`match $p isa person, has age > N, has name $n; limit 10;`

| Threads | master Reads/s | loop-inv Reads/s | bloom Reads/s |
|--------:|---------------:|-----------------:|--------------:|
|       1 |          4,611 |            4,542 |         4,591 |
|       2 |          8,997 |            9,075 |         8,978 |
|       4 |         17,401 |           17,164 |        17,029 |
|       8 |         32,313 |           31,749 |        31,620 |
|      16 |         32,395 |           32,054 |        31,964 |
|      32 |         30,074 |           30,865 |        33,198 |
|      64 |         33,716 |           33,639 |        33,196 |

---

## Summary

### batch=1000 observations

With 1000 ops per transaction, exec time dominates (65-92%) and commit is a smaller fraction. This means isolation optimizations have less impact:

- **PureInsert batch=1000**: Commit is 30-72% of tx time. Results are within noise for most thread counts. At 8T, bloom shows +7.7% but this may be variance.
- **PureUpdate batch=1000**: Exec dominates at 85-87%. Very little commit time to optimize. Results are within noise.
- **InsertRelation batch=1000**: Exec dominates at 87-92%. **Bloom filter shows severe regression at 16-32T** (-15% to -73%), likely due to bloom filter construction overhead on large keysets. Loop inversion is neutral.
- **Mixed batch=1000**: High variance due to large per-transaction exec times. No clear trend.

### batch=100 observations

- **PureInsert batch=100**: Commit is 44-61% of tx time at 1-32T. Results mostly within noise (+/- 5%).
- **PureUpdate batch=100 32T**: Loop-inv +7.0%, bloom +11.1% — consistent with isolation improvement benefiting commit-bound high-thread scenarios.
- **InsertRelation batch=100**: Master scales to 49K at 32T (better than previous run). Loop-inv +4.1% at 32T, bloom within noise.
- **Mixed batch=100**: Modest improvements at 16T, within noise elsewhere.

### batch=1 observations

With 1 op per transaction, commit is 70-97% of tx time — the sweet spot for isolation optimizations:

- **PureInsert batch=1 16T**: Loop-inv +1.8%, bloom +0.5%. Smaller gains than previous run (may be run-to-run variance).
- **PureUpdate batch=1 16T**: Loop-inv -2.9%, bloom -2.0%. Within noise.
- **InsertRelation batch=1 16T**: Master reaches 4,101 ops/s. Both branches within noise.

### Key Observations

1. **Isolation optimizations have diminishing returns at batch=1000**: When exec time is 85-92% of total tx time, commit overhead is small and optimizing it yields no measurable improvement.
2. **Bloom filter harmful for large batches of relation inserts**: The bloom filter construction cost on 1000+ keys per transaction causes severe regression in InsertRelation batch=1000 at 16-32T.
3. **Batch=100 is the sweet spot**: Commit is ~45-60% of total time, providing meaningful room for optimization while having enough throughput to show gains.
4. **batch=1 results are noisy in this run**: The previous run showed clearer gains for batch=1 workloads. The current run shows results closer to noise level, suggesting run-to-run variance of ~5%.
5. **Read throughput unaffected**: PureRead peaks at ~32-34K reads/s regardless of branch (expected since reads bypass isolation).
6. **Throughput plateaus vary by batch size**: batch=1000 peaks at 8-16T, batch=100 at 16-32T, batch=1 still scaling at 16T.
7. **Master crashes at 64T for writes**: The isolation manager has a pre-existing panic at >=64 concurrent write threads (evicted records). Both optimization branches also have this issue (inherited from master).
