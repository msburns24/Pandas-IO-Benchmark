from __future__ import annotations
from typing import Literal, Optional

import pandas as pd
import numpy as np
from pandas import DataFrame, Series


class DataFrameGenerator:
    def __init__(self, seed: Optional[int]=None) -> None:
        self.rsg = SeriesGenerator(seed=seed)
        return

    def generate(
            self,
            n_rows: int,
            n_id_cols: int = 1,
            n_float_cols: int = 2,
            n_cat_cols: int = 2,
            n_bool_cols: int = 1,
            n_datetime_cols: int = 1,
            n_string_cols: int = 1,
    ) -> DataFrame:
        df = pd.DataFrame(index=pd.RangeIndex(n_rows))
        
        for i in range(n_id_cols):
            df[f'id_{i+1}'] = self.rsg.ids(n_rows)
        
        for i in range(n_float_cols):
            p_null = 0.05 if (i % 2 == 0) else 0
            df[f'float_{i+1}'] = self.rsg.floating(n_rows, p_null)
        
        for i in range(n_cat_cols):
            n_cats = int(0.8 * n_rows) if (i % 2 == 0) else 20
            df[f'category_{i+1}'] = self.rsg.categorical(n_rows, n_cats)
        
        for i in range(n_bool_cols):
            df[f'bool_{i+1}'] = self.rsg.boolean(n_rows)
        
        for i in range(n_datetime_cols):
            df[f'timestamp_{i+1}'] = self.rsg.datetime(n_rows)
        
        for i in range(n_string_cols):
            p_null = 0.01 if (i % 2 == 0) else 0
            df[f'string_{i+1}'] = self.rsg.string(n_rows, 200, p_null)
        
        return df
    
    def generate_mixed(self, n_rows: int) -> DataFrame:
        return self.generate(n_rows)
    
    def generate_numeric_heavy(self, n_rows: int) -> DataFrame:
        raise NotImplementedError("Only variant 'mixed' currently implemented.")

    def generate_string_heavy(self, n_rows: int) -> DataFrame:
        raise NotImplementedError("Only variant 'mixed' currently implemented.")

    def generate_datetime_heavy(self, n_rows: int) -> DataFrame:
        raise NotImplementedError("Only variant 'mixed' currently implemented.")


class SeriesGenerator:
    def __init__(self, seed: Optional[int]=None) -> None:
        self.rng = np.random.default_rng(seed)
        return

    def set_random_null(self, s: Series, p: float) -> Series:
        if not (0 <= p <= 1):
            raise ValueError(f'Expected % null to be >=0, <=1, found {p}')
        if p == 0:
            return s

        s = s.copy()
        n_null = int(p * len(s))
        indices = self.rng.choice(len(s), size=n_null, replace=False)
        s.iloc[indices] = np.nan
        return s
    
    def ids(self, n_rows: int, p_null: float=0) -> Series:
        s = pd.Series(np.arange(n_rows), dtype='int64')
        s = self.set_random_null(s, p_null)
        return s

    def floating(self, n_rows: int, p_null: float = 0) -> Series:
        s = pd.Series(self.rng.random(size=n_rows), dtype='float64')
        s = self.set_random_null(s, p_null)
        return s

    def categorical(self, n_rows: int, n_cats: int, p_null: float=0) -> Series:
        categories = [f'category_{i+1}' for i in range(n_cats)]
        data = self.rng.choice(categories, size=n_rows, replace=True)
        self.rng.shuffle(data)
        s = pd.Series(data, dtype='category')
        s = self.set_random_null(s, p_null)
        return s
    
    def boolean(self, n_rows: int) -> Series:
        data = self.rng.choice([True, False], size=n_rows)
        return pd.Series(data, dtype='bool')

    def datetime(
            self,
            n_rows: int,
            start: str = '2000-01-01',
            freq: str = 'D',
    ) -> Series:
        return pd.Series(pd.date_range(start, freq=freq, periods=n_rows))
    
    def string(
            self,
            n_rows: int,
            n_unique: int,
            p_null: float = 0.0,
    ) -> Series:
        labels = [f'label_{i+1}' for i in range(n_unique)]
        data = self.rng.choice(labels, size=n_rows, replace=True)
        s = pd.Series(data, dtype='str')
        s = self.set_random_null(s, p_null)
        return s

