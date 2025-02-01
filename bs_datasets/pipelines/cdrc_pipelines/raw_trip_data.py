import os
from datetime import datetime
from multiprocessing.pool import ThreadPool, Pool
from typing import List

import pandas as pd
from pymongo import ASCENDING

from bs_datasets import logger, mongo_wrapper
from bs_datasets.data_utils.data_loader import load_cdrc_providers_info, load_cdrc_provider_info, load_csv_file, \
    count_file_rows
from bs_datasets.pipelines.cdrc_pipelines.defaults import DB_PREFIX

raw_trip_data_collection = 'raw_trip_data'

STAGE_NAME = 'Raw trip data stage'


def raw_trip_data_pipeline(
        provider: str,
        year: str,
        parallel: int = 4,
        batch_size: int = 50000,
        **kwargs
):
    if provider == 'all':
        providers_info = load_cdrc_providers_info()
        logger.info(f'{STAGE_NAME} | Starting for all {len(providers_info)} providers')
        pool = Pool(processes=len(providers_info))
        for p, _ in providers_info.items():
            pool.apply_async(_raw_trip_single_provider, args=(p, year, parallel, batch_size),
                             error_callback=lambda e: logger.exception(e))
        pool.close()
        pool.join()
        logger.info(f'{STAGE_NAME} | Completed all {len(providers_info)} providers')
    else:
        _raw_trip_single_provider(provider, year, parallel, batch_size)


def _raw_trip_single_provider(provider: str, year: str, parallel: int, batch_size: int):
    db_name = f'{DB_PREFIX}-{provider}'
    logger.info(f'{STAGE_NAME} | Starting provider {provider} for year {year} with {parallel} parallel workers,'
                f' and saving on {db_name} db')
    provider_info = load_cdrc_provider_info(provider)
    csv_folder_path = provider_info['base_path']
    collection_name = f'{raw_trip_data_collection}'
    csv_head_mapping = provider_info['csv_head_mapping']
    create_raw_trip_data_indexes(db_name, list(csv_head_mapping.values()))
    if year == 'all':
        csv_files = [
            os.path.join(csv_folder_path, file) for y, file in provider_info['observation_files'].items()]
    else:
        year_split = year.split(',')
        csv_files = [
            os.path.join(csv_folder_path, file)
            for y, file in provider_info['observation_files'].items() if y in year_split]
    for i, file in enumerate(csv_files):
        worker_pool = ThreadPool(processes=parallel)
        total_rows = count_file_rows(file) - 1
        chunks = (total_rows // batch_size) + 1
        logger.info(f'{STAGE_NAME} | Processing file {i+1}/{len(csv_files)} - filename: {file}'
                    f' - total rows: {total_rows} - chucks: {chunks}')
        for chunk_i, data_chunk in enumerate(load_csv_file(csv_path=file, n_rows=batch_size, verbose=False)):
            worker_pool.apply_async(
                func=_process_raw_data_chunk,
                kwds={
                    'data': data_chunk, 'db_name': db_name, 'collection_name': collection_name,
                    'csv_head_mapping': csv_head_mapping, 'total_rows': total_rows, 'chunk_i': chunk_i,
                    'chunks': chunks, 'file_i': i, 'n_files': len(csv_files), 'filename': file
                },
                error_callback=lambda e: logger.exception(e)
            )
        worker_pool.close()
        worker_pool.join()
    logger.info(f'{STAGE_NAME} | Completed')


def _process_raw_data_chunk(
        data: pd.DataFrame,
        db_name: str,
        collection_name: str,
        csv_head_mapping: dict,
        total_rows: int,
        chunk_i: int,
        chunks: int,
        file_i: int,
        n_files: int,
        filename: str,
):
    docs = []
    for _, row in data.iterrows():
        row_data = {
            csv_head_mapping[key]: val for key, val in row.items() if key in csv_head_mapping
        }
        row_data['station_id'] = str(row_data['station_id'])
        row_data['timestamp'] = datetime.fromisoformat(row_data['timestamp'])
        docs.append(row_data)
    mongo_wrapper.client[db_name][collection_name].insert_many(docs)
    logger.debug(
        f'{STAGE_NAME} | Flushed {len(docs) * (chunk_i + 1)}/{total_rows} docs from chunk {chunk_i +1}/{chunks}'
        f' for file {file_i + 1}/{n_files} - filename: {filename}')


def create_raw_trip_data_indexes(db_name, fields: List[str]):
    collection_name = f'{raw_trip_data_collection}'
    indexes = [(field, ASCENDING) for field in fields]
    mongo_wrapper.client[db_name][collection_name].create_index(indexes, background=True)
