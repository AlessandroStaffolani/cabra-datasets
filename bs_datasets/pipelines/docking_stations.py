import json
from multiprocessing.pool import ThreadPool
from typing import Dict, List, Union, Optional

from pymongo import GEO2D, GEOSPHERE, ASCENDING

from bs_datasets import logger, mongo_wrapper
from bs_datasets.data_utils.data_loader import load_station_information, load_provider_stats, DATASETS_MAPPING_PATH, \
    get_provider_info


STAGE_NAME = 'Docking station stage'


class DockingStation:
    collection_name = 'docking_stations'

    def __init__(
            self,
            short_name: str,
            station_id: str,
            capacity: int,
            lon: float,
            lat: float,
            name: str,
            region_id: Optional[str] = None,
            legacy_id: Optional[str] = None,
            external_id: Optional[str] = None,
            is_special: bool = False,
            **kwargs
    ):
        self.trip_id: str = short_name if not is_special else station_id
        self.name: str = name
        self.capacity: int = capacity
        self.position: Dict[str, Union[str, List[float]]] = {'type': 'Point', 'coordinates': [lon, lat]}
        self.station_id: str = station_id
        self.short_name: str = short_name
        self.region_id: str = region_id
        self.legacy_id: str = legacy_id
        self.external_id: str = external_id
        self.initial_bikes: Dict[str, Dict[str, int]] = {}
        self.distances: Dict[str, float] = {}

    def to_dict(self) -> dict:
        return {
            'trip_id': self.trip_id,
            'name': self.name,
            'capacity': self.capacity,
            'position': self.position,
            'station_id': self.station_id,
            'short_name': self.short_name,
            'region_id': self.region_id,
            'legacy_id': self.legacy_id,
            'external_id': self.external_id,
            'initial_bikes': self.initial_bikes,
            'distances': self.distances
        }

    def set_initial_bikes(self, provider_stats: Dict[str, Dict[str, int]]):
        for date, info in provider_stats.items():
            total_bikes = info['bikes']
            total_docks = info['docks']
            total_stations = info['stations']
            percent = (total_bikes*100)/total_docks
            available_bikes = round(self.capacity * (percent/100), 0)
            self.initial_bikes[date] = {
                'available_bikes': available_bikes,
                'total_bikes': total_bikes,
                'total_docks': total_docks,
                'total_stations': total_stations,
            }

    def update_distances(self, db_name: str):
        self.distances = get_station_distances(db_name, self.collection_name, self.position)
        mongo_wrapper.client[db_name][self.collection_name].update_one(
            {'trip_id': self.trip_id}, {'$set': {'distances': self.distances}})


def flush_on_db(db_name: str, collection_name: str, documents: List[dict]) -> List:
    mongo_wrapper.client[db_name][collection_name].insert_many(documents)
    return []


def create_indexes(db_name: str, collection_name: str):
    mongo_wrapper.client[db_name][collection_name].create_index([('trip_id', ASCENDING)], background=True, unique=True)
    mongo_wrapper.client[db_name][collection_name].create_index([('position', GEOSPHERE)], background=True)


def get_station_distances(db_name: str, collection_name: str, point: dict) -> Dict[str, float]:
    distances: Dict[str, float] = {}
    result = mongo_wrapper.client[db_name][collection_name].aggregate([
        {
            '$geoNear': {
                'near': point,
                'distanceField': 'distance',
                'spherical': True
            }
        }
    ])
    for res in result:
        distances[res['trip_id']] = res['distance']
    return distances


def docking_station_pipeline(provider: str):
    if provider == 'all':
        with open(DATASETS_MAPPING_PATH, 'r') as f:
            mappings = json.load(f)
        logger.info(f'{STAGE_NAME} | Starting for all {len(mappings)} providers')
        pool = ThreadPool(processes=len(mappings))
        for p, _ in mappings.items():
            pool.apply_async(_docking_station_pipeline, args=(p, ), error_callback=lambda e: logger.exception(e))
        pool.close()
        pool.join()
        logger.info(f'{STAGE_NAME} | Completed all {len(mappings)} providers')
    else:
        _docking_station_pipeline(provider)


def _docking_station_pipeline(provider: str):
    db_name = f'{mongo_wrapper.db_prefix_name}-{provider}'
    logger.info(f'{STAGE_NAME} | Started provider {provider} and saving on {db_name} db')
    provider_stats = load_provider_stats(provider)
    create_indexes(db_name, DockingStation.collection_name)
    docking_stations = load_station_information(provider, convert_to_map=False)
    provider_info = get_provider_info(provider)
    n_stations = len(docking_stations)
    documents: List[dict] = []
    for i, station in enumerate(docking_stations):
        d_station = DockingStation(is_special=provider_info['is_special'], **station)
        d_station.set_initial_bikes(provider_stats)
        documents.append(d_station.to_dict())
        if i % (n_stations // 4) == 0:
            logger.debug(f'{STAGE_NAME} | Processing docking stations processing line {i}/{n_stations}')
        if len(documents) > 1000:
            documents = flush_on_db(db_name, DockingStation.collection_name, documents)
    documents = flush_on_db(db_name, DockingStation.collection_name, documents)
    logger.info(f'{STAGE_NAME} | Updating docking station distances')
    for i, station in enumerate(docking_stations):
        d_station = DockingStation(**station)
        d_station.update_distances(db_name)
        if i % (n_stations // 4) == 0:
            logger.debug(f'{STAGE_NAME} | Updated {i}/{n_stations} docking stations distances')
    logger.info(f'{STAGE_NAME} | Completed provider {provider}')
