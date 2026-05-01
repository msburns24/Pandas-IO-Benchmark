# Pandas IO Benchmark — Parameter Reference

Parameters worth varying or controlling per format, sourced from pandas 3.0.2
docs. Wide parameter sets are listed as bullets; narrow ones use tables.

---

## CSV (`read_csv` / `to_csv`)

### Read parameters

- **`engine`** — parse speed
  - `'c'` — default, fast
  - `'python'` — more features, slower
  - `'pyarrow'` — fastest, multithreaded, some feature gaps

- **`na_filter`** — read speed on NA-free data
  - `True` — default; scans for NA markers
  - `False` — skip NA scanning; faster on clean data

- **`float_precision`** — speed vs. float fidelity
  - `None` — default ordinary converter
  - `'high'` — higher precision
  - `'round_trip'` — guarantees round-trip accuracy

- **`dtype_backend`** — memory layout of result
  - `'numpy'` — default
  - `'numpy_nullable'` — nullable dtypes
  - `'pyarrow'` — pyarrow-backed arrays

| Parameter    | Values          | Affects                            |
| ------------ | --------------- | ---------------------------------- |
| `low_memory` | `True`, `False` | Memory vs. type inference (C only) |

### Write parameters

- **`compression`** — file size and write speed
  - `None` — no compression
  - `'gzip'`, `'bz2'`, `'zip'`, `'xz'`, `'zstd'`
  - Dict form: `{'method': 'gzip', 'compresslevel': 1-9}` for codec-level tuning

| Parameter | Values          | Affects   |
| --------- | --------------- | --------- |
| `index`   | `True`, `False` | File size |

### Notes

- All dtypes read back as `object` unless `dtype=` is specified on read —
  document in fidelity log
- The `pyarrow` read engine is fastest but has feature gaps; test carefully
  with mixed dtypes
- Compression level can be varied independently of codec via the dict form of
  `compression`

---

## JSON (`read_json` / `to_json`)

### Read parameters

**`orient`** — file structure, parse speed, fidelity

- `'split'` — dict with index, columns, data as separate keys
- `'records'` — list of row dicts; does not preserve index
- `'index'` — dict of index → row dict
- `'columns'` — dict of column → value dict
- `'values'` — bare array only
- `'table'` — embeds schema metadata; most fidelity-preserving

**`engine`** — parse speed

- `'ujson'` — default
- `'pyarrow'` — faster; only valid when `lines=True`

**`dtype_backend`** — memory layout

- `'numpy'` — default
- `'numpy_nullable'` — nullable dtypes
- `'pyarrow'` — pyarrow-backed arrays

| Parameter       | Values          | Affects                               |
| --------------- | --------------- | ------------------------------------- |
| `lines`         | `True`, `False` | NDJSON mode; enables pyarrow engine   |
| `precise_float` | `True`, `False` | Float precision vs. parse speed       |
| `convert_dates` | `True`, `False` | Whether datetime cols are auto-parsed |

### Write parameters

**`orient`** — file size and structure (must match read orient)

- `'split'`, `'records'`, `'index'`, `'columns'`, `'values'`, `'table'`

**`compression`** — file size and write speed

- `None`, `'gzip'`, `'bz2'`, `'zip'`, `'xz'`, `'zstd'`

**`date_unit`** — timestamp resolution; affects size and fidelity

- `'s'`, `'ms'`, `'us'`, `'ns'`

| Parameter          | Values             | Affects                       |
| ------------------ | ------------------ | ----------------------------- |
| `lines`            | `True`, `False`    | NDJSON mode; faster for large |
| `double_precision` | `0`–`15` (def. 10) | Float digits; size + fidelity |
| `index`            | `True`, `False`    | Whether index is written      |

### Notes

- `orient` must match between write and read for round-trip fidelity
- `'records'` does not preserve the index — mark as fidelity failure if index
  matters
- `'table'` is the most fidelity-preserving orient
- `lines=True` + `engine='pyarrow'` is the fastest read path
- Datetime precision loss is common; `date_unit='ns'` + `convert_dates=True` is
  safest

---

## Parquet (`read_parquet` / `to_parquet`)

### Read parameters

**`dtype_backend`** — memory layout

- `'numpy'` — default
- `'numpy_nullable'` — nullable dtypes
- `'pyarrow'` — pyarrow-backed arrays

**`engine`** — read speed and dtype handling

- `'pyarrow'` — default fallback; broader dtype support
- `'fastparquet'` — alternative; different performance profile

**`columns`** — column pruning; hold at `None` for benchmark

**`filters`** — predicate pushdown; not a benchmark axis

**`to_pandas_kwargs`** — dict passed to `pyarrow.Table.to_pandas()`

### Write parameters

**`compression`** — file size and write speed

- `None` — no compression
- `'snappy'` — default; fast, moderate compression
- `'gzip'` — smaller, slower
- `'brotli'` — smallest files, slowest writes
- `'lz4'` — very fast, moderate compression
- `'zstd'` — best speed-to-size tradeoff

**`engine`** — write speed and file size

- `'pyarrow'` — default
- `'fastparquet'` — alternative; different compression support

**`index`** — whether index is written

- `None` — write if non-default
- `True` — always write
- `False` — never write; saves space for RangeIndex data

**`row_group_size`** — file layout (passed via `**kwargs` to pyarrow)

- e.g. `50_000`, `100_000`; affects read speed for predicate pushdown

### Notes

- `row_group_size` only available via pyarrow; fastparquet has its own
  equivalent
- `brotli` achieves smallest files but writes slowly
- `lz4` and `zstd` offer the best speed-to-size tradeoff

---

## Feather (`read_feather` / `to_feather`)

### Read parameters

**`dtype_backend`** — memory layout

- `'numpy'` — default
- `'numpy_nullable'` — nullable dtypes
- `'pyarrow'` — pyarrow-backed arrays

| Parameter     | Values          | Affects                           |
| ------------- | --------------- | --------------------------------- |
| `use_threads` | `True`, `False` | Read speed on multi-core machines |

### Write parameters

**`compression`** — file size and write speed (only three supported)

- `None` — no compression
- `'lz4'` — default; fast
- `'zstd'` — slower, smaller

| Parameter           | Values           | Affects                        |
| ------------------- | ---------------- | ------------------------------ |
| `compression_level` | int (codec-dep.) | Ratio vs. speed within a codec |
| `chunksize`         | e.g. `10_000`    | Memory usage during write      |
| `version`           | `1`, `2`         | V1 = legacy, no compression    |

### Notes

- V1 does not support compression or chunksize — will raise if passed
- `use_threads=True` on read is especially impactful for wide DataFrames
- Feather requires a default RangeIndex — custom indexes must be reset first
  (fidelity concern)

---

## HDF5 (`read_hdf` / `to_hdf`)

### Read parameters

| Parameter   | Values       | Affects                                 |
| ----------- | ------------ | --------------------------------------- |
| `key`       | string       | Required; not a benchmark axis          |
| `where`     | query string | Predicate pushdown (table format only)  |
| `chunksize` | int          | Iterator chunk size (table format only) |

### Write parameters

**`complib`** — compression algorithm

- `'zlib'`, `'lzo'`, `'bzip2'`, `'blosc'`
- Blosc sub-variants: `'blosc:lz4'`, `'blosc:lz4hc'`, `'blosc:snappy'`,
  `'blosc:zlib'`, `'blosc:zstd'`

**`format`** — speed vs. queryability

- `'fixed'` — faster; no querying or appending
- `'table'` — slower; supports `where` and `append`

**`complevel`** — compression level

- `0` — no compression
- `3`, `6`, `9` — increasing ratio, decreasing speed

**`append`** — append to existing dataset (table format only)

- `True`, `False`

**`data_columns`** — list of columns to make queryable; adds overhead

### Notes

- `complevel=0` disables compression regardless of `complib`
- Blosc sub-variants require the `blosc` C library to be installed
- `format='fixed'` is ~2–3× faster to write but cannot be queried or appended
  to
- `format='table'` required for `where` predicate pushdown

---

## Pickle (`read_pickle` / `to_pickle`)

### Read parameters

**`compression`** — decompression speed

- `'infer'`, `None`, `'gzip'`, `'bz2'`, `'zip'`, `'xz'`, `'zstd'`

### Write parameters

**`compression`** — file size and write speed

- `None`, `'gzip'`, `'bz2'`, `'zip'`, `'xz'`, `'zstd'`

**`protocol`** — serialization speed and compatibility (set via
`pickle.DEFAULT_PROTOCOL`, not a pandas parameter)

- `2` — cross-version compatibility
- `4` — large objects (Python 3.4+)
- `5` — out-of-band buffers; fastest for large arrays (Python 3.8+)

### Notes

- Pickle is lossless — use as the fidelity baseline
- Protocol 5 is fastest for large numeric arrays
- Pickle is not safe to load from untrusted sources — document in conclusions

---

## Excel (`read_excel` / `to_excel`)

### Read parameters

**`dtype_backend`** — type inference

- `'numpy'` — default
- `'numpy_nullable'` — nullable dtypes
- `'pyarrow'` — pyarrow-backed arrays

| Parameter     | Values                | Affects                           |
| ------------- | --------------------- | --------------------------------- |
| `engine`      | `'openpyxl'`          | Only practical option for `.xlsx` |
| `parse_dates` | `True`, `False`, list | Whether datetime cols are parsed  |

### Write parameters

| Parameter | Values                       | Affects                           |
| --------- | ---------------------------- | --------------------------------- |
| `engine`  | `'openpyxl'`, `'xlsxwriter'` | Write speed (`xlsxwriter` faster) |
| `index`   | `True`, `False`              | Whether index is written          |

### Notes

- No compression parameter — `.xlsx` is internally a ZIP archive
- No native boolean dtype: stored as 0/1 — always a fidelity failure
- Cap row count at 100K — Excel is not viable at 1M rows
- `xlsxwriter` can write but cannot read; `openpyxl` does both

---

## ORC (`read_orc` / `to_orc`)

### Read parameters

**`dtype_backend`** — memory layout

- `'numpy'` — default
- `'numpy_nullable'` — nullable dtypes
- `'pyarrow'` — pyarrow-backed arrays

| Parameter | Values       | Affects                         |
| --------- | ------------ | ------------------------------- |
| `columns` | `None`, list | Column pruning — hold at `None` |

### Write parameters

**`engine_kwargs`** — passed through to pyarrow

- `{'compression': 'snappy'}`
- `{'compression': 'zlib'}`
- `{'compression': 'zstd'}`
- `{'compression': 'none'}`

| Parameter | Values          | Affects               |
| --------- | --------------- | --------------------- |
| `index`   | `True`, `False` | Whether index written |

### Notes

- ORC requires `pyarrow >= 3.0` — skip gracefully and log if unavailable
- Most tuning happens at the pyarrow level via `engine_kwargs`

---

## Cross-Format Parameters

Parameters appearing across multiple formats — treat as controlled variables
unless adding a dedicated benchmark axis.

**`dtype_backend`** (CSV, JSON, Parquet, Feather, Excel, ORC)

- Hold constant at `'numpy'` (default) unless testing explicitly
- Varying it would triple result table size without being core to an IO
  speed/size benchmark

**`index` on write** (CSV, JSON, Parquet, Feather, Excel, ORC)

- Hold constant at `False` for RangeIndex data to avoid confounding file size
  results

**`columns` on read** (Parquet, Feather, ORC)

- Hold constant at `None` (read all columns)

---

## Recommended Sweep Axes

| Axis              | Scope                               |
| ----------------- | ----------------------------------- |
| Format            | All 8 formats                       |
| Engine            | Per-format (see Engine Matrix)      |
| Compression       | Per-format (see Compression Matrix) |
| `orient`          | JSON only — all 6 values            |
| `lines`           | JSON only — `True` / `False`        |
| `format`          | HDF5 only — `'fixed'` / `'table'`   |
| `complevel`       | HDF5 only — `0`, `3`, `6`, `9`      |
| `version`         | Feather only — `1` / `2`            |
| `use_threads`     | Feather read only                   |
| `float_precision` | CSV read only                       |
| `na_filter`       | CSV read only                       |

Everything else should be held constant and documented in the environment
section of the plan.
