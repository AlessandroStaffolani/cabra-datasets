import os
from dataclasses import dataclass
from glob import glob
from multiprocessing.pool import ThreadPool
from typing import Tuple

import pandas as pd

from bs_datasets.data_utils.data_loader import load_station_information, load_csv_rows
from bs_datasets.logger import logger


MAPPING = {
    'station_id': '_id',
    'short_name': '_id',
    'name': '_name'
}


STAGE_NAME = 'Verification stage'


@dataclass
class VerificationStats:
    total_entries: int = 0
    empty_entries: int = 0
    nan_entries: int = 0
    missing_entries: int = 0


def _verify_field(field, bs_stations, stats: VerificationStats) -> Tuple[bool, bool, bool]:
    is_empty = False
    is_nan = False
    is_missing = False
    if len(field) == 0:
        is_empty = True
        stats.empty_entries += 1
    elif field == 'nan':
        is_nan = True
        stats.nan_entries += 1
    elif field not in bs_stations:
        is_missing = True
        stats.missing_entries += 1
    return is_empty, is_nan, is_missing


def verify_station_identifier(
        chunks_folder: str,
        chunk_index: int,
        provider: str,
        stats: VerificationStats,
        verify_field: str
):
    chunk_path = os.path.join(chunks_folder, f'chunk_{chunk_index}.csv')
    if os.path.exists(chunk_path):
        bs_stations = load_station_information(provider, convert_to_map=True, id_field=verify_field)
        df: pd.DataFrame = load_csv_rows(chunk_path)
        stats.total_entries += len(df) * 2
        logger.debug(f'Verifying chunk {chunk_index}')
        for _, row in df.iterrows():
            start_field_name = f'start_station{MAPPING[verify_field]}'
            end_field_name = f'end_station{MAPPING[verify_field]}'
            start_field_value = str(row[start_field_name])
            end_field_value = str(row[end_field_name])
            _verify_field(start_field_value, bs_stations, stats)
            _verify_field(end_field_value, bs_stations, stats)


def verify_data(source: str, provider: str, verify_field: str = 'station_id', parallel: int = 4):
    if os.path.exists(source):
        chunk_files = glob(f'{source}/chunk_*.csv')
        stats = VerificationStats()
        results = []
        pool = ThreadPool(processes=parallel)
        for i in range(len(chunk_files)):
            results.append(pool.apply_async(func=verify_station_identifier, kwds={
                'chunks_folder': source,
                'chunk_index': i,
                'provider': provider,
                'stats': stats,
                'verify_field': verify_field
            }))
        for res in results:
            res.wait()
        pool.close()
        pool.join()
        logger.info(f'{STAGE_NAME} | Total entries are {stats.total_entries}')
        logger.info(f'{STAGE_NAME} | Empty entries are {stats.empty_entries}/{stats.total_entries}')
        logger.info(f'{STAGE_NAME} | NaN entries are {stats.nan_entries}/{stats.total_entries}')
        logger.info(f'{STAGE_NAME} | Missing entries are {stats.missing_entries}/{stats.total_entries}')
