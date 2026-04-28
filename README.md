# Pandas-IO-Benchmark

Compare Pandas serialization formats across reading, writing, file size, and more.

## Implementation

### Phase 1 — Setup

- [x] Create virtual environment, pin all dependencies
- [ ] Write `generate_dataset(n_rows, variant, seed)` function
- [ ] Write `benchmark_write(df, format, engine, compression)` function
- [ ] Write `benchmark_read(path, format, engine)` function
- [ ] Write `fidelity_check(original, reloaded)` function
- [ ] Write outer loop that sweeps all combinations and appends to results list
- [ ] Save results to `results.parquet` after each completed format block
  (checkpoint)

### Phase 2 — Dry Run

- [ ] Run full matrix at 10K rows, mixed variant only
- [ ] Verify results table shape and that all formats write/read without error
- [ ] Check fidelity table for unexpected failures
- [ ] Spot-check timing plausibility

### Phase 3 — Full Benchmark

- [ ] Run full matrix at 100K and 1M rows, all four dataset variants
- [ ] Run NYC Taxi validation pass
- [ ] Export final `results.parquet` and `fidelity_results.parquet`

### Phase 4 — Analysis

- [ ] Load results into an analysis notebook
- [ ] Plot write time vs. format (faceted by scale)
- [ ] Plot read time vs. format (faceted by scale)
- [ ] Plot file size vs. format × compression
- [ ] Plot read/write speed vs. file size (efficiency frontier)
- [ ] Summarize fidelity issues in a table
- [ ] Write conclusions section
