import os.path
from glob import glob
from multiprocessing.pool import ThreadPool
from typing import List

import numpy as np
import pandas as pd
from pymongo import ASCENDING

from bs_datasets import logger, mongo_wrapper
from bs_datasets.data_utils.data_loader import load_csv_rows, get_provider_info, get_all_providers_info

raw_trip_data_collection = 'raw_trip_data'


STAGE_NAME = 'Raw trip data stage'


def raw_trip_data_pipeline(
        provider: str,
        source: str,
        parallel: int,
        **kwargs
):
    if provider == 'all':
        providers = get_all_providers_info()
        for p, _ in providers.items():
            raw_trip_data_pipeline_single_provider(
                p, os.path.join(source, p, 'chunks'), parallel)
    else:
        raw_trip_data_pipeline_single_provider(provider, source, parallel)


def raw_trip_data_pipeline_single_provider(
        provider: str,
        source: str,
        parallel: int,
):
    logger.info(f'{STAGE_NAME} | Started for provider {provider} with chunk folder {source}')
    chunk_files = sorted(glob(f'{source}/chunk_*.csv'), key=lambda x: x.split('/')[-1])
    n_files = len(chunk_files)
    pool = ThreadPool(parallel)
    for i, chunk_path in enumerate(chunk_files):
        kwargs = {
            'provider': provider,
            'chunk_path': chunk_path,
            'index': i,
            'total': n_files
        }
        pool.apply_async(
            _raw_trip_data_single_chunk_pipeline, kwds=kwargs, error_callback=lambda e: logger.exception(e))
    pool.close()
    pool.join()
    logger.info(f'{STAGE_NAME} | Completed')


def _raw_trip_data_single_chunk_pipeline(
        provider: str,
        chunk_path: str,
        index: int,
        total: int
):
    logger.info(f'{STAGE_NAME} | processing chunk {index}/{total}')
    db_name = f'{mongo_wrapper.db_prefix_name}-{provider}'
    collection_name = f'{raw_trip_data_collection}'
    provider_info = get_provider_info(provider)
    filename = chunk_path.split('/')[-1]
    year = filename.split('_')[1][:4]
    csv_head_mapping = provider_info['csv_head_mapping'][year]
    df: pd.DataFrame = load_csv_rows(chunk_path)
    df = df.replace({np.nan: None})
    data = []
    fields_to_keep = [field_name for _, field_name in csv_head_mapping.items()]
    create_raw_trip_data_indexes(db_name, list(csv_head_mapping.keys()) + ['duration'])
    fields_to_keep_mapping = {field_name: global_key for global_key, field_name in csv_head_mapping.items()}
    for _, row in df.iterrows():
        row_data = {
            fields_to_keep_mapping[key]: val
            for key, val in row.items() if key in fields_to_keep and fields_to_keep_mapping[key] != 'extra_column'
        }
        duration = row_data['stop_time'] - row_data['start_time']
        row_data['duration'] = duration.seconds
        if row_data['stop_time'] > row_data['start_time']:
            data.append(row_data)
    mongo_wrapper.client[db_name][collection_name].insert_many(data)
    logger.info(f'{STAGE_NAME} | completed processing for chunk {index}/{total}')


def create_raw_trip_data_indexes(db_name, fields: List[str]):
    collection_name = f'{raw_trip_data_collection}'
    indexes = [(field, ASCENDING) for field in fields if field != 'extra_column']
    mongo_wrapper.client[db_name][collection_name].create_index(indexes, background=True)
