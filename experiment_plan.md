# Experiment Plan

This experiment benchmarks pandas serialization formats across multiple
dimensions: write speed, read speed, file size, compression behavior, and
round-trip fidelity. Results will be collected into a tidy DataFrame for
downstream analysis and visualization.

--------------------------------------------------------------------------------

## Goals

- Identify the fastest formats for read and write at varying data scales
- Identify the most space-efficient formats per dtype composition
- Understand compression codec tradeoffs (speed vs. size)
- Document round-trip fidelity issues (dtype preservation, null handling)
- Produce a reusable, reproducible benchmark harness

--------------------------------------------------------------------------------

## Experiment Variables

### Independent Variables (what we manipulate)

| **Variable**         | **Values**                                            |
|:---------------------|:------------------------------------------------------|
| File format          | CSV, Parquet, Feather, HDF5, Pickle, JSON, Excel, ORC |
| Writer/reader engine | Per-format (see Engine Matrix below)                  |
| Compression codec    | Per-format (see Compression Matrix below)             |
| DataFrame row count  | 10K, 100K, 1M, 10M                                    |
| Data type mix        | Numeric-heavy, String-heavy, Mixed, Datetime-heavy    |

### Dependent Variables (what we measure)

| **Metric**          | **Method**                                          |
|:--------------------|:----------------------------------------------------|
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

--------------------------------------------------------------------------------

## Engine Matrix

| **Format** | **Engines to Test**                                             |
|:-----------|:----------------------------------------------------------------|
| CSV        | `C` (write), `C` / `python` (read)                              |
| Parquet    | `pyarrow`, `fastparquet`                                        |
| Feather    | `pyarrow`                                                       |
| HDF5       | `pytables` (`format="fixed"` vs `format="table"`)               |
| Excel      | `openpyxl`, `xlsxwriter` (write); `openpyxl` (read)             |
| JSON       | `ujson` if available, default otherwise                         |
| Pickle     | protocol 2, 4, 5                                                |
| ORC        | `pyarrow`                                                       |

--------------------------------------------------------------------------------

## Compression Matrix

| **Format** | **Codecs to Test**                                  |
|:-----------|:----------------------------------------------------|
| Parquet    | `none`, `snappy`, `gzip`, `brotli`, `lz4`, `zstd`   |
| Feather    | `none`, `lz4`, `zstd`                               |
| HDF5       | `complevel` 0, 3, 6, 9 × `complib` `zlib` / `blosc` |
| CSV        | `none`, `gzip`, `bz2`, `xz`                         |
| JSON       | `none`, `gzip`                                      |
| Pickle     | no compression (inherent in protocol)               |
| Excel      | no compression (format-internal)                    |
| ORC        | `none`, `snappy`, `zlib`, `zstd`                    |

--------------------------------------------------------------------------------

## Synthetic Dataset Design

### Dtype Families Represented

Every benchmark dataset includes one column per dtype family:

| **Column Name** | **Type**          | **Notes**                  |
|:----------------|:------------------|:---------------------------|
| `id`            | `int64`           | Sequential, no nulls       |
| `value_a`       | `float64`         | ~5% NaN                    |
| `value_b`       | `float64`         | ~0% NaN                    |
| `category_low`  | `category`        | 20 unique values           |
| `category_high` | `object` (string) | ~80% unique (near UUID)    |
| `flag`          | `bool`            | No nulls                   |
| `timestamp`     | `datetime64[ns]`  | Timezone-naive             |
| `label`         | `object` (string) | 200 unique values, ~1% NaN |

### Dataset Variants

| **Variant**      | **Description**                                       |
|:-----------------|:------------------------------------------------------|
| `numeric_heavy`  | 6 numeric cols, 1 string, 1 datetime                  |
| `string_heavy`   | 5 string/category cols, 2 numeric, 1 datetime         |
| `mixed`          | Full schema above (baseline)                          |
| `datetime_heavy` | 4 columns at varying resolutions, 2 numeric, 2 string |

### Row Count Scale Points

| **Scale** | **Approximate Size (mixed, uncompressed CSV)** |
|----------:|:-----------------------------------------------|
|       10K |                                           1 MB |
|      100K |                                          10 MB |
|        1M |                                         100 MB |
|       10M |                                           1 GB |

> Start development and debugging at 10K. Run full benchmarks at 100K and 1M.
> Run 10M only if storage and time permit.

### Real-World Validation Dataset

After synthetic benchmarks are complete, run a single pass of the benchmark
matrix (formats only, no engine/compression sweep) against a slice of the
**NYC Taxi Trip dataset** to validate that synthetic conclusions generalize.
Use the January 2023 yellow taxi parquet file from the NYC TLC open data portal
(~3M rows).

--------------------------------------------------------------------------------

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

--------------------------------------------------------------------------------

## Implementation Plan

### Phase 1 — Setup

- Create virtual environment, pin all dependencies
- Write `generate_dataset(n_rows, variant, seed)` function
- Write `benchmark_write(df, format, engine, compression)` function
- Write `benchmark_read(path, format, engine)` function
- Write `fidelity_check(original, reloaded)` function
- Write outer loop that sweeps all combinations and appends to results list
- Save results to `results.parquet` after each completed format block
  (checkpoint)

### Phase 2 — Dry Run

- Run full matrix at 10K rows, mixed variant only
- Verify results table shape and that all formats write/read without error
- Check fidelity table for unexpected failures
- Spot-check timing plausibility

### Phase 3 — Full Benchmark

- Run full matrix at 100K and 1M rows, all four dataset variants
- Run NYC Taxi validation pass
- Export final `results.parquet` and `fidelity_results.parquet`

### Phase 4 — Analysis

- Load results into an analysis notebook
- Plot write time vs. format (faceted by scale)
- Plot read time vs. format (faceted by scale)
- Plot file size vs. format × compression
- Plot read/write speed vs. file size (efficiency frontier)
- Summarize fidelity issues in a table
- Write conclusions section

--------------------------------------------------------------------------------

## Output Artifacts

| **Artifact**               | **Description**                              |
|:---------------------------|:---------------------------------------------|
| `generate_data.py`         | Synthetic dataset generation module          |
| `benchmark.py`             | Main benchmark harness                       |
| `requirements.txt`         | Pinned dependency versions                   |
| `results.parquet`          | Tidy results table (one row per combination) |
| `fidelity_results.parquet` | Round-trip fidelity log                      |
| `analysis.ipynb`           | Analysis and visualization notebook          |
| `README.md`                | How to reproduce the experiment              |

--------------------------------------------------------------------------------

## Results Table Schema

```
format            str     e.g. 'parquet'
engine            str     e.g. 'pyarrow'
compression       str     e.g. 'snappy'
variant           str     e.g. 'mixed'
n_rows            int     e.g. 1_000_000
trial             int     1-7
write_time_s      float
read_time_s       float
file_size_bytes   int
peak_memory_mb    float
fidelity_pass     bool
```

--------------------------------------------------------------------------------

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

--------------------------------------------------------------------------------

## Notes & Known Limitations

- Excel benchmarks will be slow at large scales by design — cap at 100K rows
- HDF5 `format="table"` is slower than `format="fixed"` but supports querying;
  both are worth documenting
- ORC support requires `pyarrow >= 3.0`; skip gracefully if unavailable
- 10M row benchmarks may require scratch space > 10 GB; verify disk
  availability first
- Timing reflects a warm OS page cache unless explicitly flushed — document
  this clearly in conclusions
