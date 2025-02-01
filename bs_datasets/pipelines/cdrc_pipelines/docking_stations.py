import os.path
from datetime import datetime
from multiprocessing.pool import ThreadPool
from typing import Dict, Union, List, Optional
from uuid import uuid4

import pandas as pd
from pymongo import ASCENDING, GEOSPHERE

from bs_datasets import mongo_wrapper, logger
from bs_datasets.data_utils.data_loader import load_cdrc_providers_info, load_cdrc_provider_info
from bs_datasets.pipelines.cdrc_pipelines.defaults import DB_PREFIX

STAGE_NAME = 'Docking station stage'

MIN_CAPACITY = 5


class DockingStation:
    collection_name = 'docking_stations'

    def __init__(
            self,
            ucl_id: str,
            curr_size: int,
            lon: float,
            lat: float,
            operator_name: str,
            updated_dt: str,
            **kwargs
    ):
        self.station_id: str = str(ucl_id)
        self.name: str = operator_name
        self.capacity: int = int(curr_size)
        self.lon: float = lon
        self.lat: float = lat
        self.position: Dict[str, Union[str, List[float]]] = {'type': 'Point', 'coordinates': [lon, lat]}
        self.station_updated_at: datetime = datetime.fromisoformat(
            updated_dt) if updated_dt is not None and isinstance(updated_dt, str) else None
        self.distances: Dict[str, float] = {}

    def update_lat(self, lat: float):
        self.lat = lat
        self.position['coordinates'][1] = lat

    def update_lon(self, lon: float):
        self.lon = lon
        self.position['coordinates'][0] = lon

    def to_dict(self) -> dict:
        return {
            'station_id': self.station_id,
            'name': self.name,
            'capacity': self.capacity,
            'position': self.position,
            'station_updated_at': self.station_updated_at,
            'distances': self.distances
        }

    def update_distances(self, db_name: str):
        self.distances = get_station_distances(db_name, self.collection_name, self.position)
        mongo_wrapper.client[db_name][self.collection_name].update_one(
            {'station_id': self.station_id}, {'$set': {'distances': self.distances}})


def get_station_distances(db_name: str, collection_name: str, point: dict) -> Dict[str, float]:
    distances: Dict[str, float] = {}
    result = mongo_wrapper.client[db_name][collection_name].aggregate([
        {
            '$geoNear': {
                'near': point,
                'distanceField': 'distance',
                'spherical': True
            }
        },
        {
            '$sort': {
                'distance': 1
            }
        }
    ])
    for res in result:
        distances[res['station_id']] = res['distance']
    return distances


def create_indexes(db_name: str, collection_name: str):
    mongo_wrapper.client[db_name][collection_name].create_index(
        [('station_id', ASCENDING)], background=True, unique=True)
    mongo_wrapper.client[db_name][collection_name].create_index([('position', GEOSPHERE)], background=True)


def docking_station_pipeline(provider: str):
    if provider == 'all':
        providers_info = load_cdrc_providers_info()
        logger.info(f'{STAGE_NAME} | Starting for all {len(providers_info)} providers')
        pool = ThreadPool(processes=len(providers_info))
        for p, _ in providers_info.items():
            pool.apply_async(_docking_station_pipeline, args=(p, ), error_callback=lambda e: logger.exception(e))
        pool.close()
        pool.join()
        logger.info(f'{STAGE_NAME} | Completed all {len(providers_info)} providers')
    else:
        _docking_station_pipeline(provider)


def flush_on_db(db_name: str, collection_name: str, documents: List[dict]) -> List:
    mongo_wrapper.client[db_name][collection_name].insert_many(documents)
    return []


def _docking_station_pipeline(provider: str):
    db_name = f'{DB_PREFIX}-{provider}'
    logger.info(f'{STAGE_NAME} | Started provider {provider} and saving on {db_name} db')
    provider_info = load_cdrc_provider_info(provider)
    create_indexes(db_name, DockingStation.collection_name)
    df = pd.read_csv(
        os.path.join(provider_info['base_path'], provider_info['docking_stations_file'])
    )
    years = [2021, 2022]

    documents: Dict[str, Optional[dict]] = {}
    for year in years:
        obs_file = os.path.join(provider_info['base_path'], provider_info['observation_files'][str(year)])
        obs_df = pd.read_csv(obs_file)
        unique_ids = obs_df['tfl_id'].unique().tolist()
        documents = {**documents, **{str(bs_id): None for bs_id in unique_ids}}
        del obs_df

    n_stations = len(df)
    i = 0
    for index, row in df.iterrows():
        d_station = DockingStation(**row.to_dict())
        if d_station.capacity > MIN_CAPACITY and not skip_docking_entry(d_station):
            if d_station.station_id in documents:
                if documents[d_station.station_id] is None:
                    documents[d_station.station_id] = d_station.to_dict()
                else:
                    if d_station.station_updated_at is None or \
                            d_station.station_updated_at > documents[d_station.station_id]['station_updated_at']:
                        replace_docking_station(original_dock=documents[d_station.station_id],
                                                new_dock=d_station)
                        documents[d_station.station_id] = d_station.to_dict()
        if i % (n_stations // 4) == 0:
            logger.debug(f'{STAGE_NAME} | Processing docking stations processing line {i}/{n_stations}')
        i += 1
    flush_on_db(db_name, DockingStation.collection_name, [doc for _, doc in documents.items() if doc is not None])
    logger.info(f'{STAGE_NAME} | Updating docking station distances')
    i = 0
    for index, row in df.iterrows():
        d_station = DockingStation(**row.to_dict())
        if d_station.capacity > MIN_CAPACITY and not skip_docking_entry(d_station):
            try:
                d_station.update_distances(db_name)
            except Exception as e:
                logger.error(f'Unable to update distances for docking station {d_station.to_dict()}')
                logger.exception(e)
        if i % (n_stations // 4) == 0:
            logger.debug(f'{STAGE_NAME} | Updated {i}/{n_stations} docking stations distances')
        i += 1
    logger.info(f'{STAGE_NAME} | Completed provider {provider}')


def skip_docking_entry(dock: DockingStation) -> bool:
    return dock.lat < -90 or dock.lat > 90 or dock.lon < -180 or dock.lon > 180


def replace_docking_station(original_dock: dict, new_dock: DockingStation):
    original_dock_lat = original_dock['position']['coordinates'][1]
    original_dock_lon = original_dock['position']['coordinates'][0]
    if (new_dock.lat < -90 or new_dock.lat > 90) and -90 < original_dock_lat < 90:
        new_dock.update_lat(original_dock_lat)
    if (new_dock.lon < -180 or new_dock.lon > 180) and -180 < original_dock_lon < 180:
        new_dock.update_lon(original_dock_lon)
