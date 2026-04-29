'''
A basic prototype for benchmarking
'''

import time
from pathlib import Path

import pandas as pd 
import numpy as np
from rich.console import Console

from generate_data import DataFrameGenerator


# Parameters
FORMAT = 'csv'
ENGINE = None
COMPRESSION = None
VARIANT = 'mixed'

N_ROWS = 10_000
# N = 10_000
# N_NULL_VALUE_A = int(0.05 * N)
# N_NULL_LABEL = int(0.01 * N)
# N_CATEGORIES_LOW = 20
# N_CATEGORIES_HIGH = int(0.8 * N)

ROOT_DIR = Path(__file__).parent.resolve()
CSV_PATH = ROOT_DIR / f'{VARIANT}.{FORMAT}'
RESULTS_PATH = ROOT_DIR / 'results.parquet'

# rng = np.random.default_rng(seed=0)
console = Console()
console.print(f"Starting test for format '{FORMAT}'")


# Create dataset (initial, before testing)
console.print(f"Generating dataset '{VARIANT}'")
df = DataFrameGenerator(seed=0).generate_mixed(N_ROWS)
# df = pd.DataFrame({'id': np.arange(N, dtype='int64')})

# df['value_a'] = rng.random(N, dtype='float64')
# df.loc[rng.choice(N, size=N_NULL_VALUE_A, replace=False), 'value_a'] = np.nan
# df['value_b'] = rng.random(N, dtype='float64')

# low_cats = [f'category_{i+1}' for i in range(N_CATEGORIES_LOW)]
# df['category_low'] = pd.Series(rng.choice(low_cats, size=N), dtype='category')

# high_cats = np.array([f'category_{i+1}' for i in range(N_CATEGORIES_HIGH)])
# high_cats = np.append(high_cats, rng.choice(high_cats, N - N_CATEGORIES_HIGH))
# rng.shuffle(high_cats)
# df['category_high'] = pd.Series(high_cats, dtype='category')

# df['flag'] = rng.choice([True, False], size=N)
# df['timestamp'] = pd.date_range(start='2000-01-01', periods=N, freq='D')

# label_choices = [f'label_{i+1}' for i in range(200)]
# df['label'] = rng.choice(label_choices, size=N)
# df.loc[rng.choice(N, size=N_NULL_LABEL, replace=False), 'label'] = np.nan


dtypes = df.dtypes.to_dict()
timestamp_cols = [c for c in df.columns if 'timestamp' in c]
for col in timestamp_cols:
    del dtypes[col]  # datetime64[us] not supported for parsing


# Start write test
console.print(f'Starting write test')
t_write_start = time.perf_counter()
df.to_csv(CSV_PATH, index=False, compression=COMPRESSION)
write_time_s = time.perf_counter() - t_write_start
file_size_bytes = CSV_PATH.stat().st_size


# Start read test
console.print(f'Starting read test')
t_read_start = time.perf_counter()
df_read = pd.read_csv(
    CSV_PATH, compression=COMPRESSION, dtype=dtypes, parse_dates=timestamp_cols,
)
read_time_s = time.perf_counter() - t_read_start

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
    'file_size_bytes':  file_size_bytes,
    'peak_memory_mb':   0,
    'fidelity_pass':    fidelity_pass,
}])
console.print('Tests complete. Results:')
console.print(results.iloc[0])
results.to_parquet(RESULTS_PATH)
