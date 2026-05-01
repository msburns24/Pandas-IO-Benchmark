'''
A basic prototype for benchmarking
'''

import itertools
import time
from pathlib import Path

import pandas as pd 
import numpy as np
from pandas import DataFrame
from rich.console import Console

from generate_data import DataFrameGenerator


# Parameters
FORMAT = 'csv'
ENGINE = None
COMPRESSION = None
VARIANT = 'mixed'
N_ROWS = 10_000
N_TRIALS = 3

ROOT_DIR = Path(__file__).parent.resolve()
CSV_PATH = ROOT_DIR / f'{VARIANT}.{FORMAT}'
RESULTS_PATH = ROOT_DIR / 'results.parquet'

console = Console()


def main() -> None:
    results_list = []
    df = DataFrameGenerator(seed=0).generate_mixed(N_ROWS)

    param_grid = itertools.product(
        [None, 'gzip', 'zip', 'bz2', 'xz', 'zstd'],  # compression
        ['c', 'python', 'pyarrow'],  # engine
        [True, False], # low_memory
        range(1, N_TRIALS + 1),
    )
    for compression, engine, low_memory, trial in param_grid:
        if low_memory is False and engine != 'c':
            # low_memory only applicable for engine 'c'
            # default True okay for rest
            continue

        write_time_s = benchmark_write(
            df, CSV_PATH, FORMAT, compression=compression,
        )
        df_read, read_time_s = benchmark_read(
            CSV_PATH, FORMAT, compression=compression, dtype=df.dtypes,
            engine=engine,
        )
        fidelity_pass = check_fidelity(df, df_read)
        results_list.append({
            'format':           FORMAT,
            'engine':           engine,
            'compression':      compression or 'None',
            'variant':          VARIANT,
            'n_rows':           len(df),
            'trial':            trial,
            'write_time_s':     write_time_s,
            'read_time_s':      read_time_s,
            'file_size_bytes':  CSV_PATH.stat().st_size,
            'peak_memory_mb':   0,
            'fidelity_pass':    fidelity_pass,
        })

    results = pd.DataFrame(results_list)
    console.print('Tests complete. Results:')
    console.print(results.to_markdown(index=False, tablefmt='simple'))
    results.to_parquet(RESULTS_PATH)
    return


def benchmark_write(df: DataFrame, path: Path, fmt: str, **kwargs) -> float:
    start_time = time.perf_counter()

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
    
    duration = time.perf_counter() - start_time
    return duration


def benchmark_read(path: Path, fmt: str, **kwargs) -> tuple[DataFrame, float]:
    start_time = time.perf_counter()

    if 'dtype' in kwargs:
        dtypes = dict(kwargs['dtype'])
        timestamp_columns = [c for c in dtypes if 'timestamp' in c]
        for column in timestamp_columns:
            del dtypes[column]
        
        kwargs['dtype'] = dtypes
        kwargs['parse_dates'] = timestamp_columns

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
    
    duration = time.perf_counter() - start_time
    return df, duration


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