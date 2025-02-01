import os.path
from glob import glob
from multiprocessing.pool import ThreadPool
from typing import Dict, List, Any

import pandas as pd
from pymongo import ASCENDING

from bs_datasets import logger, mongo_wrapper
from bs_datasets.data_utils.data_loader import load_csv_rows, get_provider_info, get_all_providers_info

trip_data_collection = 'trip_data'


STAGE_NAME = 'Trip data stage'


def trip_data_pipeline(
        provider: str,
        source: str,
        year: int,
        parallel: int,
        clean_only: bool = False,
        aggregation_frequency: str = '4h'
):
    if provider == 'all':
        providers = get_all_providers_info()
        for p, _ in providers.items():
            trip_data_pipeline_single_provider(
                p, os.path.join(source, p, 'chunks'), year, parallel, clean_only, aggregation_frequency)
    else:
        trip_data_pipeline_single_provider(provider, source, year, parallel, clean_only, aggregation_frequency)


def trip_data_pipeline_single_provider(
        provider: str,
        source: str,
        year: int,
        parallel: int,
        clean_only: bool = False,
        aggregation_frequency: str = '4h'
):
    logger.info(f'{STAGE_NAME} | Started for provider {provider} with chunk folder {source}')
    if not clean_only:
        chunk_files = sorted(glob(f'{source}/chunk_*.csv'), key=lambda x: x.split('/')[-1])
        n_files = len(chunk_files)
        pool = ThreadPool(parallel)
        for i, chunk_path in enumerate(chunk_files):
            kwargs = {
                'provider': provider,
                'chunk_path': chunk_path,
                'year': year,
                'aggregation_frequency': aggregation_frequency,
                'index': i,
                'total': n_files
            }
            pool.apply_async(
                _trip_data_single_chunk_pipeline, kwds=kwargs, error_callback=lambda e: logger.exception(e))
        pool.close()
        pool.join()
    logger.info(f'{STAGE_NAME} | Merging duplicates')
    clean_duplicates(provider, year)
    logger.info(f'{STAGE_NAME} | Completed')


def _trip_data_single_chunk_pipeline(
        provider: str,
        chunk_path: str,
        year: int,
        aggregation_frequency: str,
        index: int,
        total: int
):
    logger.info(f'{STAGE_NAME} | processing chunk {index}/{total}')
    db_name = f'{mongo_wrapper.db_prefix_name}-{provider}'
    collection_name = f'{trip_data_collection}-{year}'
    create_trip_data_indexes(db_name, year)
    provider_info = get_provider_info(provider)
    csv_head_mapping = provider_info['csv_head_mapping'][str(year)]
    df: pd.DataFrame = load_csv_rows(chunk_path)
    dataset = handle_df(df, year, aggregation_frequency, csv_head_mapping)
    mongo_wrapper.client[db_name][collection_name].insert_many(dataset)
    logger.info(f'{STAGE_NAME} | completed processing for chunk {index}/{total}')


def handle_df(df, year: int, aggregation_frequency: str, csv_head_mapping: Dict[str, str]) -> List[Dict[str, Any]]:
    grouped_start_data = df.groupby(
        [pd.Grouper(key=csv_head_mapping['start_time'], freq=aggregation_frequency),
         csv_head_mapping['start_trip_id']]
    ).agg({csv_head_mapping['extra_column']: 'count'})

    grouped_end_data = df.groupby(
        [pd.Grouper(key=csv_head_mapping['stop_time'], freq=aggregation_frequency),
         csv_head_mapping['stop_trip_id']]
    ).agg({csv_head_mapping['extra_column']: 'count'})

    return flatten_and_merge_groups(grouped_start_data, grouped_end_data, year, csv_head_mapping)


def create_trip_data_indexes(db_name, year):
    collection_name = f'{trip_data_collection}-{year}'
    mongo_wrapper.client[db_name][collection_name].create_index([
        ('date', ASCENDING),
        ('operation', ASCENDING),
        ('station', ASCENDING),
        ('weekday', ASCENDING),
        ('value', ASCENDING)
    ], background=True)


def flatten_and_merge_groups(
        start_df: pd.DataFrame,
        end_df: pd.DataFrame,
        year: int,
        csv_head_mapping: Dict[str, str]
) -> List[Dict[str, Any]]:
    data: List[Dict[str, Any]] = []
    extra_col = csv_head_mapping['extra_column']
    for (dtime, station), value in start_df.iterrows():
        data.append({
            'month': dtime.month,
            'day': dtime.day,
            'weekday': dtime.weekday(),
            'hour': dtime.hour,
            'station': station,
            'value': value[extra_col].item(),
            'operation': 'start',
            'date': dtime
        })
    for (dtime, station), value in end_df.iterrows():
        data.append({
            'month': dtime.month,
            'day': dtime.day,
            'weekday': dtime.weekday(),
            'hour': dtime.hour,
            'station': station,
            'value': value[extra_col].item(),
            'operation': 'end',
            'date': dtime
        })
    return data


def clean_duplicates(provider: str, year: int):
    db_name = f'{mongo_wrapper.db_prefix_name}-{provider}'
    collection_name = f'{trip_data_collection}-{year}'
    options = {'allowDiskUse': True}
    duplicates_result = mongo_wrapper.client[db_name][collection_name].aggregate([
        {
            '$group': {
                '_id': {
                    'station': '$station',
                    'date': '$date',
                    'operation': '$operation'
                },
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$match': {
                'count': {
                    '$gte': 2
                }
            }
        }
    ], **options)
    duplicates_removed = 0
    ids_to_remove = []
    for duplicate_info in duplicates_result:
        duplicates = [r for r in mongo_wrapper.client[db_name][collection_name].find({
            'station': duplicate_info['_id']['station'],
            'operation': duplicate_info['_id']['operation'],
            'date': duplicate_info['_id']['date']
        })]
        total_value = duplicates[0]['value']
        for i in range(1, len(duplicates)):
            total_value += duplicates[i]['value']
            ids_to_remove.append(duplicates[i]['_id'])
        mongo_wrapper.client[db_name][collection_name].find_one_and_update(
            {'_id': duplicates[0]['_id']}, {'$set': {'value': total_value}})
        if len(ids_to_remove) >= 5000:
            removed = mongo_wrapper.client[db_name][collection_name].delete_many({'_id': {'$in': ids_to_remove}})
            duplicates_removed += removed.deleted_count
            ids_to_remove = []
            logger.debug(
                f'{STAGE_NAME} | Still merging duplicates | Duplicates merged so far {duplicates_removed} ')
    removed = mongo_wrapper.client[db_name][collection_name].delete_many({'_id': {'$in': ids_to_remove}})
    duplicates_removed += removed.deleted_count
    logger.info(f'{STAGE_NAME} | Merged {duplicates_removed} duplicates')
