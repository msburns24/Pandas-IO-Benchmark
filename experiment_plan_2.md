# Pandas IO Benchmark Experiment Plan

## Overview

This experiment benchmarks pandas serialization formats across multiple
dimensions: write speed, read speed, file size, compression behavior, and
round-trip fidelity. Results will be collected into a tidy DataFrame for
downstream analysis and visualization.

---

## Goals

- Identify the fastest formats for read and write at varying data scales
- Identify the most space-efficient formats per dtype composition
- Understand compression codec tradeoffs (speed vs. size)
- Document round-trip fidelity issues (dtype preservation, null handling)
- Produce a reusable, reproducible benchmark harness

---

## Experiment Variables

### Independent Variables (what we manipulate)

| **Variable**        | **Values**                                            |
| :------------------ | :---------------------------------------------------- |
| File format         | CSV, Parquet, Feather, HDF5, Pickle, JSON, Excel, ORC |
| Engine              | Per-format (see Engine Matrix below)                  |
| Compression codec   | Per-format (see Compression Matrix below)             |
| DataFrame row count | 10K, 100K, 1M, 10M                                    |
| Data type mix       | Numeric-heavy, String-heavy, Mixed, Datetime-heavy    |

### Dependent Variables (what we measure)

| **Metric**          | **Method**                                          |
| :------------------ | :-------------------------------------------------- |
| Write time          | `time.perf_counter`, median of N trials             |
| Read time           | `time.perf_counter`, median of N trials             |
| File size           | `os.path.getsize()`                                 |
| Peak memory on read | `tracemalloc`                                       |
| Round-trip fidelity | `pd.testing.assert_frame_equal()` pass/fail + notes |

### Controlled Variables (held constant)

- pandas version
- pyarrow, fastparquet, openpyxl, etc. versions (pinned in `requirements.txt`)
- Hardware (document CPU, RAM, storage type — SSD vs. HDD matters)
- OS page cache strategy (warm reads; document whether cache is flushed)
- Number of timing trials per combination: **7 runs, median reported**
- Random seed for synthetic data generation

---

## Engine Matrix

| **Format** | **Engines to Test**                                 |
| :--------- | :-------------------------------------------------- |
| CSV        | `c` (write), `c` / `python` (read)                  |
| Parquet    | `pyarrow`, `fastparquet`                            |
| Feather    | `pyarrow`                                           |
| HDF5       | `pytables` (`format="fixed"` vs `format="table"`)   |
| Excel      | `openpyxl`, `xlsxwriter` (write); `openpyxl` (read) |
| JSON       | `ujson` if available, default otherwise             |
| Pickle     | protocol 2, 4, 5                                    |
| ORC        | `pyarrow`                                           |

---

## Compression Matrix

| **Format** | **Codecs to Test**                                  |
| :--------- | :-------------------------------------------------- |
| Parquet    | `none`, `snappy`, `gzip`, `brotli`, `lz4`, `zstd`   |
| Feather    | `none`, `lz4`, `zstd`                               |
| HDF5       | `complevel` 0, 3, 6, 9 × `complib` `zlib` / `blosc` |
| CSV        | `none`, `gzip`, `bz2`, `xz`                         |
| JSON       | `none`, `gzip`                                      |
| Pickle     | no compression (inherent in protocol)               |
| Excel      | no compression (format-internal)                    |
| ORC        | `none`, `snappy`, `zlib`, `zstd`                    |

---

## Synthetic Dataset Design

### Dtype Families Represented

Every benchmark dataset includes one column per dtype family:

| **Column Name** | **Dtype**         | **Notes**                  |
| :-------------- | :---------------- | :------------------------- |
| `id`            | `int64`           | Sequential, no nulls       |
| `value_a`       | `float64`         | ~5% NaN                    |
| `value_b`       | `float64`         | ~0% NaN                    |
| `category_low`  | `category`        | 20 unique values           |
| `category_high` | `object` (string) | ~80% unique (near UUID)    |
| `flag`          | `bool`            | No nulls                   |
| `timestamp`     | `datetime64[ns]`  | Timezone-naive             |
| `label`         | `object` (string) | 200 unique values, ~1% NaN |

### Dataset Variants

| **Variant**      | **Description**                                         |
| :--------------- | :------------------------------------------------------ |
| `numeric_heavy`  | 6 numeric cols, 1 string, 1 datetime                    |
| `string_heavy`   | 5 string/category cols, 2 numeric, 1 datetime           |
| `mixed`          | Full schema above (baseline)                            |
| `datetime_heavy` | 4 datetime cols w/varying resolutions, 2 numeric/string |

### Row Count Scale Points

| **Scale** | **Approximate Size (mixed, uncompressed CSV)** |
| --------: | ---------------------------------------------: |
|       10K |                                          ~1 MB |
|      100K |                                         ~10 MB |
|        1M |                                        ~100 MB |
|       10M |                                          ~1 GB |

> Start development and debugging at 10K. Run full benchmarks at 100K and 1M.
> Run 10M only if storage and time permit.

### Real-World Validation Dataset

After synthetic benchmarks are complete, run a single pass of the benchmark
matrix (formats only, no engine/compression sweep) against a slice of the
**NYC Taxi Trip dataset** to validate that synthetic conclusions generalize.
Use the January 2023 yellow taxi parquet file from the NYC TLC open data portal
(~3M rows).

---

## Fidelity Test Protocol

For each format + engine combination, after writing and reading back:

1. Run `pd.testing.assert_frame_equal(original, reloaded, check_dtype=True)`
2. If it fails, log which columns differ and how (dtype changed, nulls
   introduced, precision lost)
3. Record in a separate `fidelity_results` table alongside timing results

Known issues to document explicitly:

- CSV: all dtypes read back as `object` unless `dtype=` is specified
- JSON: datetime precision loss, float rounding
- Excel: no native boolean; stored as 0/1
- Pickle: fidelity is lossless but protocol-version-sensitive

---

## Implementation Plan

> **Approach:** Agile for Solo Python Projects (Kanban + thin vertical slices).
> WIP limit = 1. Each card is a ≤30-minute slice phrased as a verb-noun pair.
> No card enters Doing without a clear Definition of Done. Cycles are 1-2 weeks
> depending on available hours. Retro at each cycle boundary.

### Definition of Done (applies to every card)

- Code merged to `main`
- The slice is reachable end-to-end — not just unit-testable in isolation
- `results.parquet` or the relevant artifact actually writes without error
  after the change
- No card closes with a known breakage silently deferred

### Walking Skeleton (do this first, before any cycle planning)

The skeleton is the crappiest possible end-to-end benchmark run. One format
(CSV), one engine (default), no compression, 10K rows of `mixed` data, 3
trials. It writes a file, reads it back, records one row in `results.parquet`,
and runs `assert_frame_equal`. No sweep loop, no compression matrix, no memory
profiling — just the thinnest thing that touches every layer.
**Everything else is a vertebra added to this spine.**

> **Done when:** `python benchmark.py` produces a `results.parquet` with one
> row and no errors.

### Cycle 1 — Harness foundation

**Goal:** By end of cycle 1, the benchmark loop runs all formats at 10K rows
(mixed variant), writes results to `results.parquet`, and the fidelity check
runs on every format.

**Non-goals:** compression sweep, engine variants, multiple dataset variants,
memory profiling, analysis notebook.

Cards (pull one at a time, WIP=1):

- [x] Create venv, `requirements.txt` with pinned deps, `.python-version`
- [ ] Write `generate_dataset(n_rows, variant="mixed", seed=42)` — returns a
      DataFrame with all 8 dtype columns
- [ ] Write `benchmark_write(df, fmt, engine, compression)` — single trial,
      returns `(write_time_s, path)`
- [ ] Write `benchmark_read(path, fmt, engine)` — single trial, returns
      `read_time_s`
- [ ] Write `fidelity_check(original, reloaded)` —
      returns `(pass: bool, notes: str)`
- [ ] Wire outer loop: sweep all formats at 10K/mixed, 3 trials, append to
      results list, save `results.parquet`
- [ ] Checkpoint save after each format block completes (guards against
      mid-run crash)
- [ ] Smoke test: run full loop, verify `results.parquet` shape and spot-check
      timing plausibility

**Cycle 1 retro trigger:** Can you run the harness top-to-bottom on 10K rows
and trust the numbers?

### Cycle 2 — Scale and compression sweep

**Goal:** By end of cycle 2, the harness runs the full compression matrix at
100K and 1M rows for all formats, and `results.parquet` is complete enough to
draw preliminary conclusions.

**Non-goals:** engine variants, dataset variants, analysis notebook, NYC Taxi
pass.

Cards:

- [ ] Add `n_rows` parameter to outer loop; run at 100K and 1M
- [ ] Add compression sweep per format (see Compression Matrix)
- [ ] Add `peak_memory_mb` via `tracemalloc` to `benchmark_read`
- [ ] Cap Excel at 100K rows — add guard and log skip reason
- [ ] Add graceful skip for ORC if `pyarrow < 3.0`; log to results as `skipped`
- [ ] Run full matrix at 100K; verify `results.parquet` grows correctly
- [ ] Run full matrix at 1M; checkpoint after each format block

**Spike candidate:** If compression sweep runtime looks prohibitive, timebox
30 min to estimate total wall-clock time before committing to the full run.

**Cycle 2 retro trigger:** Is `results.parquet` complete enough to start
drawing conclusions? If yes, the analysis notebook can start in parallel with
Cycle 3.

### Cycle 3 — Engine variants and dataset variants

**Goal:** By end of cycle 3, results cover all engine variants (Parquet
pyarrow vs. fastparquet, Pickle protocol variants, etc.) and all four dataset
compositions.

**Non-goals:** NYC Taxi validation, final analysis write-up.

Cards:

- [ ] Add `engine` parameter to sweep; extend formats that have multiple engines
- [ ] Add `variant` parameter to outer loop; generate `numeric_heavy`,
      `string_heavy`, `datetime_heavy` datasets
- [ ] Run engine × variant sweep at 100K (full 1M run only if time allows)
- [ ] Review fidelity log for unexpected failures; add inline notes to
      known-issues section of this doc

### Cycle 4 — Validation and analysis

**Goal:** By end of cycle 4, the NYC Taxi validation pass is complete and the
analysis notebook has charts and a written conclusions section.

Cards:

- [ ] Download NYC Taxi Jan 2023 parquet slice; verify row count and schema
- [ ] Run formats-only benchmark pass on Taxi data (no engine/compression
      sweep)
- [ ] Load `results.parquet` into `analysis.ipynb`; write time vs. format
      (faceted by scale)
- [ ] Plot read time vs. format (faceted by scale)
- [ ] Plot file size vs. format × compression
- [ ] Plot efficiency frontier: read/write speed vs. file size
- [ ] Summarize fidelity issues in a table cell
- [ ] Write conclusions section — do synthetic results hold on real data?

### Spikes (run these before the slice that needs the answer)

Log spike outputs to `docs/spikes/` as short Markdown notes
(question → finding → recommendation).

- **Spike:** Does `tracemalloc` meaningfully capture peak RSS during
  `pd.read_parquet`, or does it miss C-extension allocations?
  - Timebox: 45 min
  - Trigger: Before adding memory profiling in Cycle 2
- **Spike:** What is the realistic wall-clock time for the full
  compression × format × 1M sweep?
  - Timebox: 30 min
  - Trigger: Before committing to Cycle 2 full run
- **Spike:** Does NYC Taxi Jan 2023 parquet actually fit the dtype profile we
  expect, or does it need preprocessing?
  - Timebox: 30 min
  - Trigger: Before Cycle 4 validation pass

---

## Output Artifacts

| **Artifact**               | **Description**                              |
| :------------------------- | -------------------------------------------- |
| `generate_data.py`         | Synthetic dataset generation module          |
| `benchmark.py`             | Main benchmark harness                       |
| `requirements.txt`         | Pinned dependency versions                   |
| `results.parquet`          | Tidy results table (one row per combination) |
| `fidelity_results.parquet` | Round-trip fidelity log                      |
| `analysis.ipynb`           | Analysis and visualization notebook          |
| `README.md`                | How to reproduce the experiment              |

---

## Results Table Schema

```
format          str       e.g. "parquet"
engine          str       e.g. "pyarrow"
compression     str       e.g. "snappy"
variant         str       e.g. "mixed"
n_rows          int       e.g. 1_000_000
trial           int       1-7
write_time_s    float
read_time_s     float
file_size_bytes int
peak_memory_mb  float
fidelity_pass   bool
```

---

## Environment Documentation

Record the following before running:

```
python --version
pandas.__version__
pyarrow.__version__
fastparquet.__version__
openpyxl.__version__
platform.platform()       # OS + kernel
storage type              # SSD / NVMe / HDD
RAM available
```

---

## Notes & Known Limitations

- Excel benchmarks will be slow at large scales by design — cap at 100K rows
- HDF5 `format="table"` is slower than `format="fixed"` but supports querying;
  both are worth documenting
- ORC support requires `pyarrow >= 3.0`; skip gracefully if unavailable
- 10M row benchmarks may require scratch space > 10 GB; verify disk
  availability first
- Timing reflects a warm OS page cache unless explicitly flushed — document
  this clearly in conclusions
