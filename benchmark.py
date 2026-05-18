"""
Benchmark harness for pandas serialization formats.
"""

import sys
import time
import tracemalloc
from pathlib import Path

import pandas as pd
from pandas import DataFrame
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from generate_data import DataFrameGenerator


N_ROWS = 10_000
N_TRIALS = 5
SEED = 0

VARIANTS = ['mixed', 'numeric_heavy', 'string_heavy', 'datetime_heavy']

ROOT_DIR = Path(__file__).parent.resolve()
TMP_DIR = ROOT_DIR / 'tmp'
RESULTS_PATH = ROOT_DIR / 'results.parquet'

console = Console()

FMT_PATHS: dict[str, Path] = {
    'csv':     TMP_DIR / 'benchmark.csv',
    'parquet': TMP_DIR / 'benchmark.parquet',
    'feather': TMP_DIR / 'benchmark.feather',
    'hdf':     TMP_DIR / 'benchmark.h5',
    'json':    TMP_DIR / 'benchmark.json',
    'pickle':  TMP_DIR / 'benchmark.pkl',
    'excel':   TMP_DIR / 'benchmark.xlsx',
    'orc':     TMP_DIR / 'benchmark.orc',
}

# Each combo: (write_kwargs, read_kwargs, engine_label, compression_label)
Combo = tuple[dict, dict, str, str]


def _csv_combos(df: DataFrame) -> list[Combo]:
    dtypes = dict(df.dtypes)
    timestamp_cols = [c for c in dtypes if 'timestamp' in c]
    read_dtype = {c: v for c, v in dtypes.items() if 'timestamp' not in c}

    combos: list[Combo] = []
    for compression in [None, 'gzip', 'zip', 'bz2', 'xz', 'zstd']:
        for engine in ['c', 'python', 'pyarrow']:
            base_read = {
                'compression': compression,
                'engine': engine,
                'dtype': read_dtype,
                'parse_dates': timestamp_cols,
            }
            if engine == 'c':
                for low_memory in [True, False]:
                    combos.append((
                        {'compression': compression},
                        {**base_read, 'low_memory': low_memory},
                        engine,
                        compression or 'None',
                    ))
            else:
                combos.append((
                    {'compression': compression},
                    base_read,
                    engine,
                    compression or 'None',
                ))
    return combos


def _parquet_combos() -> list[Combo]:
    combos: list[Combo] = []
    for compression in [None, 'snappy', 'gzip', 'brotli', 'lz4', 'zstd']:
        for engine in ['pyarrow', 'fastparquet']:
            combos.append((
                {'compression': compression, 'engine': engine},
                {'engine': engine},
                engine,
                compression or 'None',
            ))
    return combos


def _feather_combos() -> list[Combo]:
    combos: list[Combo] = []
    for compression in [None, 'lz4', 'zstd']:
        combos.append((
            {'compression': compression},
            {},
            'pyarrow',
            compression or 'None',
        ))
    return combos


def _hdf_combos() -> list[Combo]:
    combos: list[Combo] = []
    for hdf_format in ['fixed', 'table']:
        # complevel=0: no compression; complib doesn't matter
        combos.append((
            {'key': 'data', 'format': hdf_format, 'complevel': 0, 'complib': 'zlib'},
            {'key': 'data'},
            'pytables',
            f'{hdf_format}/None',
        ))
        for complib in ['zlib', 'blosc']:
            for complevel in [3, 6, 9]:
                combos.append((
                    {'key': 'data', 'format': hdf_format, 'complevel': complevel, 'complib': complib},
                    {'key': 'data'},
                    'pytables',
                    f'{hdf_format}/{complib}:{complevel}',
                ))
    return combos


def _json_combos() -> list[Combo]:
    combos: list[Combo] = []
    for orient in ['table', 'split', 'records']:
        for compression in [None, 'gzip']:
            combos.append((
                {'orient': orient, 'compression': compression},
                {'orient': orient, 'compression': compression},
                'ujson',
                compression or 'None',
            ))
    return combos


def _pickle_combos() -> list[Combo]:
    return [({}, {}, 'pickle', 'None')]


def _excel_combos() -> list[Combo]:
    return [
        ({'engine': 'openpyxl'},   {'engine': 'openpyxl'},  'openpyxl/openpyxl',   'None'),
        ({'engine': 'openpyxl'},   {'engine': 'calamine'},  'openpyxl/calamine',   'None'),
        ({'engine': 'xlsxwriter'}, {'engine': 'openpyxl'},  'xlsxwriter/openpyxl', 'None'),
        ({'engine': 'xlsxwriter'}, {'engine': 'calamine'},  'xlsxwriter/calamine', 'None'),
    ]


def _orc_combos() -> list[Combo]:
    combos: list[Combo] = []
    for compression in ['uncompressed', 'snappy', 'zlib', 'zstd', 'lz4']:
        combos.append((
            {'engine_kwargs': {'compression': compression}},
            {},
            'pyarrow',
            compression,
        ))
    return combos


def _generate_variant(gen: DataFrameGenerator, variant: str) -> DataFrame:
    if variant == 'mixed':
        return gen.generate_mixed(N_ROWS)
    elif variant == 'numeric_heavy':
        return gen.generate_numeric_heavy(N_ROWS)
    elif variant == 'string_heavy':
        return gen.generate_string_heavy(N_ROWS)
    elif variant == 'datetime_heavy':
        return gen.generate_datetime_heavy(N_ROWS)
    raise ValueError(f"Unknown variant: '{variant}'")


def _build_format_combos(df: DataFrame, *, exclude_orc: bool) -> dict[str, list[Combo]]:
    combos: dict[str, list[Combo]] = {
        'csv':     _csv_combos(df),
        'parquet': _parquet_combos(),
        'feather': _feather_combos(),
        'hdf':     _hdf_combos(),
        'json':    _json_combos(),
        'pickle':  _pickle_combos(),
        'excel':   _excel_combos(),
    }
    if not exclude_orc:
        combos['orc'] = _orc_combos()
    return combos


def main() -> None:
    TMP_DIR.mkdir(exist_ok=True)
    gen = DataFrameGenerator(seed=SEED)
    on_windows = sys.platform == 'win32'

    if on_windows:
        console.print('[yellow]Windows detected — skipping ORC (not supported).[/yellow]')

    # Pre-count total trials so the progress bar has an accurate total
    total_trials = 0
    for variant in VARIANTS:
        df = _generate_variant(gen, variant)
        fc = _build_format_combos(df, exclude_orc=on_windows)
        total_trials += sum(len(c) for c in fc.values()) * N_TRIALS

    results_list: list[dict] = []

    progress = Progress(
        SpinnerColumn(),
        TextColumn('[progress.description]{task.description}'),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    )

    with progress:
        task = progress.add_task('Starting…', total=total_trials)

        for variant in VARIANTS:
            df = _generate_variant(gen, variant)
            format_combos = _build_format_combos(df, exclude_orc=on_windows)

            for fmt, combos in format_combos.items():
                path = FMT_PATHS[fmt]

                for write_kwargs, read_kwargs, engine_label, compression_label in combos:
                    for trial in range(1, N_TRIALS + 1):
                        progress.update(
                            task,
                            description=f'[cyan]{variant}[/cyan] [bold]{fmt}[/bold] '
                                        f'{engine_label}/{compression_label} t{trial}',
                        )

                        write_time_ms = read_time_ms = peak_mb = file_size = None
                        fidelity_pass = False

                        try:
                            write_time_ms = benchmark_write(df, path, fmt, **write_kwargs) * 1000
                            file_size = path.stat().st_size
                            df_read, read_time_ms, peak_mb = benchmark_read(path, fmt, **read_kwargs)
                            read_time_ms *= 1000
                            fidelity_pass = check_fidelity(df, df_read)
                        except Exception as exc:
                            progress.console.print(
                                f'  [red]ERROR[/red] {variant}/{fmt}/{engine_label}/'
                                f'{compression_label} trial {trial}: {exc}'
                            )

                        results_list.append({
                            'format':           fmt,
                            'engine':           engine_label,
                            'compression':      compression_label,
                            'variant':          variant,
                            'n_rows':           N_ROWS,
                            'trial':            trial,
                            'write_time_ms':    write_time_ms,
                            'read_time_ms':     read_time_ms,
                            'file_size_bytes':  file_size,
                            'peak_memory_mb':   peak_mb,
                            'fidelity_pass':    fidelity_pass,
                        })

                        progress.advance(task)

                # Checkpoint after each format block
                pd.DataFrame(results_list).to_parquet(RESULTS_PATH)

    results = pd.DataFrame(results_list)
    console.print('\n[bold green]All variants complete.[/bold green]')
    summary = results.groupby(['variant', 'format']).agg(
        combos=('trial', 'count'),
        fidelity_pct=('fidelity_pass', lambda x: f'{x.mean():.0%}'),
    )
    console.print(summary.to_string())
    results.to_parquet(RESULTS_PATH)


def benchmark_write(df: DataFrame, path: Path, fmt: str, **kwargs) -> float:
    start = time.perf_counter()

    # HDF5 fixed format and ORC don't support pandas category dtype
    if (fmt == 'orc') or (fmt == 'hdf' and kwargs.get('format', 'fixed') == 'fixed'):
        cat_cols = df.select_dtypes('category').columns
        if len(cat_cols):
            df = df.copy()
            df[cat_cols] = df[cat_cols].astype(object)

    # fastparquet doesn't support category or pandas StringDtype; convert to object
    if fmt == 'parquet' and kwargs.get('engine') == 'fastparquet':
        incompatible = [c for c in df.columns if df[c].dtype.name in ('category', 'str')]
        if incompatible:
            df = df.copy()
            df[incompatible] = df[incompatible].astype(object)

    if fmt == 'csv':
        df.to_csv(path, index=False, **kwargs)
    elif fmt == 'parquet':
        df.to_parquet(path, index=False, **kwargs)
    elif fmt == 'feather':
        df.to_feather(path, **kwargs)
    elif fmt == 'hdf':
        df.to_hdf(path, **kwargs)
    elif fmt == 'json':
        df.to_json(path, index=False, **kwargs)
    elif fmt == 'pickle':
        df.to_pickle(path, **kwargs)
    elif fmt == 'excel':
        df.to_excel(path, index=False, **kwargs)
    elif fmt == 'orc':
        df.to_orc(path, index=False, **kwargs)
    else:
        raise ValueError(f"Unknown format: '{fmt}'")

    return time.perf_counter() - start


def benchmark_read(path: Path, fmt: str, **kwargs) -> tuple[DataFrame, float, float]:
    tracemalloc.start()
    start = time.perf_counter()

    if fmt == 'csv':
        df = pd.read_csv(path, **kwargs)
    elif fmt == 'parquet':
        df = pd.read_parquet(path, **kwargs)
    elif fmt == 'feather':
        df = pd.read_feather(path, **kwargs)
    elif fmt == 'hdf':
        df = pd.read_hdf(path, **kwargs)
    elif fmt == 'json':
        df = pd.read_json(path, **kwargs)
    elif fmt == 'pickle':
        df = pd.read_pickle(path, **kwargs)
    elif fmt == 'excel':
        df = pd.read_excel(path, **kwargs)
    elif fmt == 'orc':
        df = pd.read_orc(path, **kwargs)
    else:
        raise ValueError(f"Unknown format: '{fmt}'")

    duration = time.perf_counter() - start
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return df, duration, peak / 1024 / 1024


def check_fidelity(original: DataFrame, reloaded: DataFrame) -> bool:
    try:
        pd.testing.assert_frame_equal(original, reloaded)
        return True
    except AssertionError:
        return False
    except Exception:
        raise


if __name__ == '__main__':
    main()
