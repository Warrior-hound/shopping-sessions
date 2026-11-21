# Shopping Sessions: native Rust pipeline design

Code: `src/main.rs` (one file, about 1,100 lines). Submission root: `../rust-app/`. Run `sh sync.sh` there to copy the sources over; it uses a multi-stage Dockerfile.

## Measurements

All 20M figures come from generator seed 7 (`--events 20000000 --clicks 5000000`): 1,814 MB of events and clicks, 143 event files, 32 click files and 7 link files. The answer MATCHes the fixed DuckDB app.

"thread-s" means the CPU seconds summed over the 8 workers, so wall time ≈ thread-s / 8. Two setups were measured:
- **Native:** the Windows build with `NCPU=8`.
- **Docker:** scorer flags (`--cpus 8`), with data in a Docker volume and a warm page cache.

The host was shared with other jobs, at a load of 15 to 24 on 24 logical CPUs. Treat every number as an upper bound, ±30%.

**Per-stage timings (v5, the current code)**

| Stage | Native (thread-s / wall) | Docker, best of 5 (thread-s / wall) |
|---|---|---|
| mmap, manifest, products, campaigns | wall 0.02 | wall 0.02 |
| parse events + clicks | 4.2 + 0.6 / 0.65 | 5.6 + 0.5 / 0.79 |
| links (union-find, own thread, overlapped with parse) | hidden | hidden |
| dedup on event_id | 1.0–1.6 | 0.9 |
| ID→person map + scatter | 2.5 + 0.4 (dedup + map phase: 0.60 wall) | 2.8 + 0.5 (dedup + map phase: 0.53 wall) |
| group by person (counting scatter) | 0.5 | 0.5 |
| sort per person | 0.35 | 0.4 |
| sessionize + credit + assists | 0.6 (all three phase-3 steps: 0.20 wall) | 0.7 (all three phase-3 steps: 0.21 wall) |
| Parquet write (8 user files + 1 campaign file, in parallel) | wall 0.1 | wall 0.06 |
| **Internal total** | **1.55 s** | **1.59 s** (median under load ≈ 1.9 s) |
| Docker wall, container start → exit | — | 2.85 s best |
| Empty container (`/bin/true`, same flags) | — | 0.86–0.95 s |

**Version history (Docker internal time, same data)**

| Version | Change | Time |
|---|---|---|
| v1 | String slices, buckets grown by `Vec` doubling | 3.65 s |
| v3 | Packed u128 ID keys; buckets pre-sized; compact link table | 1.8 s |
| v5 | Dedup table holds the records; prefetching; `process::exit` | 1.59 s |

## Stages

Each stage lists:
- **(a)** alternatives considered
- **(b)** the choice and why
- **(c)** the lower bound on 8 vCPU Cascade Lake (probably 4 cores / 8 hyperthreads), about 15–20 GB/s of memory bandwidth
- **(d)** the measured time and the gap to the bound
- **(e)** what would close the gap

Technique cards are from `state/databricks/RESEARCH.md`.

### 1. Read and mmap
- (a) `read()` into owned buffers; `mmap` + `MADV_SEQUENTIAL`; `MAP_POPULATE`; `pread` into reused 4 MB buffers (card 6).
- (b) `mmap` with every mapping leaked, so every string is a `&'static [u8]` and nothing is copied. Chunks are 4 MB of a file. A line belongs to the chunk in which it starts, and only `\n` ends a line, so any byte offset is a safe split (no quotes span lines, R-CSV.3).
- (c) With a warm page cache this costs only page-table setup: about 450k 4 KB PTEs for 1.8 GB. Fault-around makes that about 30k faults, roughly 0.03 s of CPU.
- (d) Not measured separately; it is inside the parse time.
- (e) A/B against `pread` (card 6). Measure only if parse stays above its bound after SIMD.

### 2. Line and field scan, and CSV validation
- (a) Scalar byte loop; SWAR; AVX2 or AVX-512 structural bitmasks for `,`, `"` and `\n` (card 1, simdjson-style); a branch-free tokenizer (W2.1).
- (b) Now: `memchr` for `\n` (SIMD), then one scalar pass per line. It counts quotes per field, and a field is valid iff it has 0 quotes, or exactly 2 quotes at its first and last byte. The field count must be exact. `""` becomes the empty slice, which means null. An empty line has 1 field, so it is dropped. This matched all 36 fixtures and 7 datasets on the first attempt.
- (c) About 1 GB/s/core including field work (1BRC, simdjson stage 1 at 0.5–3 cycles/byte). That is ≈ 0.23 s for 1.8 GB on 8 threads, and ≈ 0.12 s at pure memory bandwidth.
- (d) Parse total is 4.2–5.6 thread-s, about 0.29 µs per line (≈ 330 MB/s/thread) **including** the timestamp parse, packing and the scatter. That is 0.65–0.79 s wall, about 3× the bound.
- (e) The AVX2/AVX-512 mask tokenizer (card 1). Validate a field with bit operations on the quote mask (fields = popcnt(commas) + 1). Expected gain: about −0.3 s.

### 3. Timestamp parse
- (a) chrono or a general-purpose parser; a length-dispatched scalar parser; SWAR (card 4).
- (b) Dispatch on length and fixed positions:
  - 20 bytes ending in `Z` is F1.
  - 25 bytes with `+`/`-` at 19 and `:` at 22 is F2.
  - Otherwise all-digits is F3.

  Two-digit fields are parsed with `wrapping_sub` checks (ASCII digits only), followed by a full calendar check and Hinnant's `days_from_civil`. F3 strips leading zeros; more than 18 significant digits saturates, so the value lands outside every window instead of overflowing. ingest_ts uses the same parser. It is always F1, but parsing it with the general parser keeps us exact if it is not.
- (c) SWAR does about 2 ns per timestamp. 43M timestamps (ts + ingest for events, ts for clicks) ≈ 0.01 s wall.
- (d) Inside the parse time. Estimated at 15–20% of it, not yet isolated.
- (e) SWAR 8-byte loads (card 4). W2.4 suggests skipping ingest validation entirely: compute it only from F1 positions.

### 4. ID interning: packed keys
- (a) `String`/slice keys with foldhash; a global interner (u32 ids); packed integer keys (card 3); 6-bit packing into a u64 (W2.5).
- (b) Every ID becomes a **u128**. IDs of 15 bytes or fewer are stored as big-endian bytes, zero padded, with the length in the low byte. That packing is exact, injective and **order-preserving** (u128 order equals byte-wise order). Longer IDs get a marker byte plus an index into a mutex-guarded interner; they are rare (the generator writes none), and comparisons on them go through the bytes.

  Moving from slices to keys removed every pointer chase into the 2 GB mapping during dedup, grouping and sorting. This change took **v2 → v3 from 3.3 to 1.83 s natively**. "Event person map" dropped from 24 to 4.4 thread-s, together with the pre-sized buckets.
- (c) Hashing a u128 takes 2 multiplies, about 1 ns.
- (d) Negligible.
- (e) A u64 6-bit pack (W2.5) would halve record sizes, giving less memory traffic in every later stage. It needs a fallback path.

### 5. Union-find for links
- (a) scipy-style CSR connected components; parallel CC (Afforest); sequential union-find (card 8).
- (b) Sequential union-find with path halving. The union is by smaller index. The name of each component is the smallest key by `id_cmp`.

  The result goes into a compact open-addressing table of (id key, person key), at 32 B per slot with load ≤ 0.5. Lookups are linear probing, with software prefetch issued 16 items ahead. The whole thing runs on its **own thread, concurrently with parsing**. Clicks keep their raw user key, and the person mapping happens after the join.
- (c) About 1.2M ids at 20M; roughly 0.1 s on one thread, hidden behind the parse.
- (d) Fully hidden. The `links` log line comes 0–12 ms after parsing ends.
- (e) Nothing needed at 20M. At 60M (about 3.6M ids) it is still hidden behind a parse of about 2 s.

### 6. Dedup (min ingest_ts) and the late filter
- (a) A global concurrent hash table; sort-based dedup; radix partition plus a per-partition hash table (card 2).
- (b) Each event copy is fully validated **at parse time**: user, ts in the window, type, product, price, and late (`ing − ts > 24 h`). Validity depends only on the copy itself, so the verdict is stored in the record (`ts = BAD` means the copy is dropped).

  Records (64 B) are scattered into 512 buckets by hash(event key) for each worker. Each partition then builds a hashbrown `HashTable<ERec>` that holds the records themselves (about 42k per partition). The smallest ingest wins. Equal ingests are identical copies, per the rules, so either may win.

  The winners are then read in one sequential sweep. Storing indices (v3) cost a random fetch per winner, and the v5 change saved about 1.5 thread-s.
- (c) 21.6M inserts at about 10 ns each when cache resident ≈ 0.03 s wall. Reading 1.4 GB once ≈ 0.08 s.
- (d) 0.9–1.6 thread-s, about 0.15 s wall. That is 2–4× the bound.
- (e) Smaller records with a hot/cold split (key + ingest in 24 B, payload elsewhere). Huge pages for the buckets (W2.2: an arena with `MADV_HUGEPAGE`). More partitions, so each table fits in L2.

### 7. ID→person map and the scatter by person
- (b) For each survivor, look up the person key, then push a 32 B record (person key, ts, price, cat) into one of 512 buckets by hash(person). Clicks do the same, using 64k-element slices as tasks. Buckets are pre-sized generously. Untouched reserved pages cost nothing, and the pre-sizing removed realloc copies and their page faults: **parse went from 64 to 4.4 thread-s natively**.
- (c) One DRAM miss per lookup, hidden by prefetching, plus 0.6 GB of sequential writes ≈ 0.05 s wall.
- (d) 2.5–3.7 thread-s (130–190 ns per event), about 0.35 s wall. The NOPUSH experiment put about 1 thread-s on the push itself (first-touch page faults in the VM) and about 2 thread-s on the lookups. **This is the largest remaining gap.**
- (e) **Hugepage arenas** for the buckets (many fewer faults). Or a cache-resident partitioned join: bucket survivors by hash(user) and pre-partition the link table the same way, which replaces DRAM misses with L2 hits. Or do the map in the parse phase once links are ready.

### 8. Group by person and sort
- (a) A global sort by (person, ts); LSD radix on packed u64 keys (card 5); a counting scatter plus a per-person comparison sort.
- (b) For each person partition:
  - Intern the person key to a local pid with a hash table.
  - Do a counting-sort scatter into contiguous per-person slices of 16 B (ts, price, cat).
  - Run `sort_unstable_by_key(ts)` on each slice.

  Clicks go through the same steps, sorted by (ts, click key) with `id_cmp`.
- (c) About 20M × 16 B moved twice ≈ 0.04 s. The per-slice sorts are small (the median person has about 30 events).
- (d) 0.9 thread-s in total, about 0.11 s wall.
- (e) Sort 8-byte keys, with purchases in a side table (W2.5). Radix-sort the whale slices of 400k events.

### 9. Sessionize, credit, assists, aggregation
- (a) For assists: range-join session × click; a sliding window with per-campaign counters (card 10); islands per (person, campaign).
- (b) One sweep per person.
  - **Sessionize:** a new session starts iff `ts − prev_ts > 30 min`. Ties have gap 0, so they always share a session, whatever the read order. This is exactly where the DuckDB app's ROWS-frame bug was; ours is order-free by construction.
  - **Credit:** a two-pointer over clicks sorted by (ts, click_id). The click is the first one with the latest `ts ≤ start`, and it counts only if `start − ts ≤ 7 d`.
  - **Assists:** per campaign, the clicks are merged into islands `[a, b + 7 d]`, where a new island starts iff the gap is over 7 d (ties have gap 0). Session starts are counted with `partition_point`, split by day boundary. The cost is O(islands × days × log n) and does not depend on the number of assist pairs.
  - **Aggregation:** user_daily is folded per (person, day) while the sweep runs. The days are monotone; top_category comes from a small (cat-rank, sum) vector, and category ranks are byte-sorted, so a tie goes to the smallest name. campaign_daily goes into a dense per-worker array (campaigns + organic) × days, which is merged at the end.
- (c) About 20M events + 5M clicks streamed once ≈ 0.02 s.
- (d) 0.7 thread-s, about 0.09 s wall.
- (e) Minor.

### 10. Parquet write
- (a) arrow-rs `ArrowWriter`; the low-level `parquet` crate writer; a hand-rolled PLAIN writer (card 7).
- (b) The `parquet` 53.3 low-level `SerializedFileWriter`, with no Arrow, no dictionary, no statistics and no compression. user_daily is written as **one file per worker, in parallel** (8 files). The user_id column is a single byte buffer with zero-copy `Bytes` slices. day is INT32 (DATE), counts are INT64 and top_category is OPTIONAL. If there are zero rows, it still writes one schema-only file.
- (c) 2.2M rows, about 70 MB ≈ 0.01 s.
- (d) About 0.05–0.1 s wall.
- (e) A hand-rolled writer would save little at 20M. Revisit at 60M (about 7M rows).

### 11. Process start and exit
- (a) glibc vs musl + mimalloc (card 11); exit normally vs `process::exit`; munmap the chunks as you go (W2.3).
- (b) glibc, a debian-slim runtime, no allocator change (the hot loops don't allocate per row), and `std::process::exit(0)` right after the files are closed, so no user-space frees of the buckets.
- (c) Container start is outside our control: 0.86–0.95 s for `/bin/true` here, and the Lead estimates about 0.55 s on the scorer.
- (d) Docker wall minus internal time ≈ 1.25 s, against 0.9 s for an empty container. So about 0.35 s goes to exit teardown of about 3 GB of anonymous pages plus the 1.8 GB mapping, plus startup.
- (e) Hugepage arenas (512× fewer PTEs to tear down). Unmap chunks as they are consumed (W2.3). Shrink the records.

## Correctness gate (all MATCH at v5)

| Gate | Native | Docker |
|---|---|---|
| Fixtures | 36/36 | 36/36 (via `.cache/databricks/rust/drun.sh`) |
| dev (vs dev answer) | MATCH | MATCH |
| Seeds 2–5 (vs reference) | MATCH | MATCH |
| big11, big12 (vs reference) | MATCH | MATCH |
| seed 7 at 20M (vs fixed DuckDB app) | — | MATCH |

Scripts: `.cache/databricks/rust/gate_native.sh`, `bt.sh`, `time20.sh`.

## Next speedups, by expected gain
1. **Hugepage arenas plus smaller records:** about −0.3 to −0.5 s. Covers page faults in parse, dedup and map, plus exit teardown.
2. **SIMD mask tokenizer (card 1):** about −0.3 s on parse.
3. **Partitioned, cache-resident ID→person join:** about −0.25 s of map time.

After those: PGO, and racing the knobs (threads 8 vs 16, partition count, chunk size) as paired A/B runs at low load.
