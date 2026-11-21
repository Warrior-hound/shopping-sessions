# Shopping Sessions: native Rust pipeline

A single static-ish Rust binary (`src/main.rs`) that reads the event, click, link, product and campaign CSVs from `$DATA_DIR` (default `/data`) and writes the `user_daily` and `campaign_daily` Parquet tables to `$OUTPUT_DIR` (default `/output`), following `rules.md` exactly.

Pipeline, all multi-threaded over memory-mapped input:

1. Parse events and clicks with a byte-level tokenizer (the rules guarantee no commas, quotes or newlines inside values, so any newline is a safe split point).
2. Resolve identities with union-find over links, overlapped with parsing; each person is named by the byte-wise smallest user id.
3. Deduplicate events, keeping the minimum `ingest_ts`.
4. Group by person, sort, and sessionize with a 30-minute gap (equal timestamps share a session).
5. Credit purchases and count assists within the 7-day attribution window.
6. Write both Parquet tables.

`DESIGN.md` explains each stage: the alternatives considered, the chosen approach, its lower bound and the measured time.

Build: `docker build -t shopping-sessions .` (toolchain and base images pinned by digest; crates pinned by `Cargo.lock`).
