import os.path
from typing import List

from bs_datasets import logger
from bs_datasets.pipelines.split import split_csv_files_pipeline
from bs_datasets.pipelines.verify import verify_data
from bs_datasets.pipelines.dataset import dataset_pipeline
from bs_datasets.pipelines.docking_stations import docking_station_pipeline
from bs_datasets.pipelines.downloader import download_trace_files
from bs_datasets.pipelines.trip_data import trip_data_pipeline
from bs_datasets.pipelines.raw_trip_data import raw_trip_data_pipeline
from bs_datasets.pipelines.sub_dataset import multiple_sub_datasets
from bs_datasets.pipelines.weather_data import fetch_weather_data
from bs_datasets.pipelines.zones import zones_pipeline


LOGGER_PREFIX = 'All pipeline'


def execute_or_skip(skip_commands: List[str], command_name: str, command, *args, **kwargs):
    if command_name in skip_commands:
        logger.info(f'{LOGGER_PREFIX} | stage {command_name} skipped')
    else:
        command(*args, **kwargs)


def all_pipeline(
        year: str,
        provider: str,
        download_path: str,
        n_rows: int,
        split_path: str,
        parallel: int,
        # aggregation_frequency: str,
        # dataset_path: str,
        skip: str
):
    logger.info(f'{LOGGER_PREFIX} | Started')
    skip_commands = skip.split(',') if skip is not None else []
    execute_or_skip(skip_commands, 'downloader', download_trace_files, provider, download_path, year, parallel)
    execute_or_skip(skip_commands, 'split', split_csv_files_pipeline, provider, split_path, n_rows, download_path)
    execute_or_skip(skip_commands, 'docking', docking_station_pipeline, provider)
    raw_source_path = split_path if provider == 'all' else os.path.join(split_path, provider, 'chunks')
    execute_or_skip(skip_commands, 'raw', raw_trip_data_pipeline, provider, raw_source_path, parallel)
    logger.info(f'{LOGGER_PREFIX} | Completed')


ACTION_MAPPING = {
    'downloader': download_trace_files,
    'split': split_csv_files_pipeline,
    'docking': docking_station_pipeline,
    'raw': raw_trip_data_pipeline,
    'subdataset': multiple_sub_datasets,
    'all': all_pipeline,
    'weather': fetch_weather_data,
    'zones': zones_pipeline,
}
