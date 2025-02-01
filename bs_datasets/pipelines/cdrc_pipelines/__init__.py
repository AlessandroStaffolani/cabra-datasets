from typing import Optional

from bs_datasets import logger
from bs_datasets.pipelines import execute_or_skip
from bs_datasets.pipelines.cdrc_pipelines.dataset import dataset_pipeline
from bs_datasets.pipelines.cdrc_pipelines.docking_stations import docking_station_pipeline
from bs_datasets.pipelines.cdrc_pipelines.raw_trip_data import raw_trip_data_pipeline
from bs_datasets.pipelines.cdrc_pipelines.zones import zones_pipeline

LOGGER_PREFIX = 'All pipeline'


def all_pipeline(
        year: str,
        provider: str,
        dataset_path: str,
        skip: str,
        min_date: str,
        max_date: str,
        aggregation_unit: str = 'minute',
        aggregation_size: int = 10,
        name_suffix: Optional[str] = None,
        add_weather_data: bool = False,
        weather_db: Optional[str] = None,
        weather_collection: str = 'observations',
        **kwargs
):
    logger.info(f'{LOGGER_PREFIX} | Started')
    skip_commands = skip.split(',') if skip is not None else []
    execute_or_skip(skip_commands, 'docking', docking_station_pipeline, provider)
    execute_or_skip(skip_commands, 'raw', raw_trip_data_pipeline, provider, year)
    execute_or_skip(skip_commands, 'zones', zones_pipeline, provider, -1)
    execute_or_skip(skip_commands, 'subdataset', dataset_pipeline, provider, dataset_path,
                    min_date, max_date, aggregation_unit, aggregation_size, name_suffix, add_weather_data, weather_db,
                    weather_collection, True)
    logger.info(f'{LOGGER_PREFIX} | Completed')


CDRC_ACTION_MAPPING = {
    'docking': docking_station_pipeline,
    'raw': raw_trip_data_pipeline,
    'subdataset': dataset_pipeline,
    'zones': zones_pipeline,
    'all': all_pipeline,
}

