'''
A basic prototype for benchmarking
'''

import time
from pathlib import Path
from typing import Optional

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

ROOT_DIR = Path(__file__).parent.resolve()
CSV_PATH = ROOT_DIR / f'{VARIANT}.{FORMAT}'
RESULTS_PATH = ROOT_DIR / 'results.parquet'

console = Console()


# Start write test
def benchmark_write(
    df: DataFrame,
    fmt: str,
    path: Path,
    engine: Optional[str] = None,
    compression: Optional[str] = None,
) -> float:
    start_time = time.perf_counter()
    if fmt == 'csv':
        df.to_csv(path, index=False, compression=compression) # pyright: ignore
    duration = time.perf_counter() - start_time
    return duration



console.print(f"Starting test for format '{FORMAT}'")


# Create dataset (initial, before testing)
console.print(f"Generating dataset '{VARIANT}'")
df = DataFrameGenerator(seed=0).generate_mixed(N_ROWS)

dtypes = df.dtypes.to_dict()
timestamp_cols = [c for c in df.columns if 'timestamp' in c]
for col in timestamp_cols:
    del dtypes[col]  # datetime64[us] not supported for parsing


# Write Test
console.print(f'Starting write test')
write_time_s = benchmark_write(df, FORMAT, CSV_PATH, compression=COMPRESSION)

# Read test
console.print(f'Starting read test')
t_read_start = time.perf_counter()
df_read = pd.read_csv(
    CSV_PATH, compression=COMPRESSION, dtype=dtypes, parse_dates=timestamp_cols,
)
read_time_s = time.perf_counter() - t_read_start

# Fidelity Check
try:
    pd.testing.assert_frame_equal(df, df_read)
    fidelity_pass = True
except AssertionError:
    fidelity_pass = False


# Save results
results = pd.DataFrame([{
    'format':           FORMAT,
    'engine':           ENGINE,
    'compression':      ENGINE,
    'variant':          VARIANT,
    'n_rows':           N_ROWS,
    'trial':            1,
    'write_time_s':     write_time_s,
    'read_time_s':      read_time_s,
    'file_size_bytes':  CSV_PATH.stat().st_size,
    'peak_memory_mb':   0,
    'fidelity_pass':    fidelity_pass,
}])
console.print('Tests complete. Results:')
console.print(results.iloc[0])
results.to_parquet(RESULTS_PATH)
