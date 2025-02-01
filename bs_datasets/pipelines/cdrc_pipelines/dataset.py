import json
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

import pandas as pd

from bs_datasets import logger, mongo_wrapper
from bs_datasets.data_utils.data_loader import load_cdrc_providers_info, load_cdrc_provider_info
from bs_datasets.filesystem import create_directory
from bs_datasets.pipelines.cdrc_pipelines.defaults import DB_PREFIX
from bs_datasets.pipelines.cdrc_pipelines.docking_stations import DockingStation, MIN_CAPACITY
from bs_datasets.pipelines.raw_trip_data import raw_trip_data_collection
from bs_datasets.pipelines.sub_dataset import TIME_UNITS_MAPPING

STAGE_NAME = 'Dataset stage'


def dataset_pipeline(
        provider: str,
        output: str,
        min_date: str,
        max_date: str,
        aggregation_unit: str = 'minute',
        aggregation_size: int = 10,
        name_suffix: Optional[str] = None,
        add_weather_data: bool = False,
        weather_db: Optional[str] = None,
        weather_collection: str = 'observations',
        nodes_from_zones: bool = False,
        **kwargs
):
    args = {
        'output': output,
        'min_date': min_date,
        'max_date': max_date,
        'aggregation_unit': aggregation_unit,
        'aggregation_size': aggregation_size,
        'name_suffix': name_suffix,
        'add_weather_data': add_weather_data,
        'weather_db': weather_db,
        'weather_collection': weather_collection,
        'nodes_from_zones': nodes_from_zones,
    }
    if provider == 'all':
        providers_info = load_cdrc_providers_info()
        logger.info(f'{STAGE_NAME} | Starting for all {len(providers_info)} providers')
        for p, _ in providers_info.items():
            args['output'] = os.path.join(output, p)
            _single_proivder_dataset(provider, **args)
        logger.info(f'{STAGE_NAME} | Completed all {len(providers_info)} providers')
    else:
        args['output'] = os.path.join(output, provider)
        _single_proivder_dataset(provider, **args)


def _single_proivder_dataset(
        provider: str,
        output: str,
        min_date: str,
        max_date: str,
        aggregation_unit: str = 'minute',
        aggregation_size: int = 10,
        name_suffix: Optional[str] = None,
        add_weather_data: bool = False,
        weather_db: Optional[str] = None,
        weather_collection: str = 'observations',
        nodes_from_zones: bool = False,
):
    db_name = f'{DB_PREFIX}-{provider}'
    logger.info(f'{STAGE_NAME} | Started provider {provider}')
    provider_info = load_cdrc_provider_info(provider)
    zones_path = os.path.join(provider_info['base_path'], 'zones.json')
    raw_collection_name = f'{raw_trip_data_collection}'
    min_date = datetime.fromisoformat(min_date)
    max_date = datetime.fromisoformat(max_date)

    dataset = init_empty_dataset(
        aggregation_size * TIME_UNITS_MAPPING[aggregation_unit],
        min_date, max_date, add_weather_data, weather_db, weather_collection
    )

    nodes = get_valid_nodes(db_name, DockingStation.collection_name)
    zones = None
    if nodes_from_zones:
        zones = get_zones(zones_path)
    nodes_last_bikes: Dict[str, Optional[int]] = {node: None for node in nodes}
    previous_interval: Optional[datetime] = None

    for date_interval_str, _ in dataset.items():
        date_interval = datetime.fromisoformat(date_interval_str)
        stations = {}
        if previous_interval is not None:
            nodes_in_interval = get_record_in_interval(db_name, raw_collection_name, nodes,
                                                       min_date=previous_interval, max_date=date_interval)
            for node_data in nodes_in_interval:
                station_id = node_data['station_id']
                last_bikes = nodes_last_bikes[station_id]
                current_bikes = int(node_data['bikes'])
                if last_bikes is not None:
                    # verify the difference of bikes in the interval
                    if last_bikes != current_bikes:
                        difference = current_bikes - last_bikes
                        # negative value -> node has fewer bikes than before
                        # positive value -> node has more bikes than before
                        stations[station_id] = difference
                nodes_last_bikes[station_id] = current_bikes

        dataset[date_interval.isoformat()]['stations'] = stations
        dataset[date_interval.isoformat()]['n_stations'] = len(stations)
        previous_interval = date_interval

    # change index
    final_dataset = {}
    for _, val in dataset.items():
        final_dataset[val['index']] = val

    # nodes data
    nodes_data = build_nodes_file(nodes, db_name, DockingStation.collection_name,
                                  sum_filepath=os.path.join(
                                      provider_info['base_path'],
                                      provider_info['sum_file']
                                  ),
                                  zones=zones)

    if name_suffix is None:
        output = os.path.join(output, f'dataset-s={min_date.isoformat()}-e={max_date.isoformat()}')
    else:
        output = os.path.join(output, f'dataset-{name_suffix}')

    create_directory(output)
    with open(os.path.join(output, 'dataset.json'), 'w') as f:
        json.dump(final_dataset, f, indent=2)
    with open(os.path.join(output, 'nodes.json'), 'w') as f:
        json.dump(nodes_data, f, indent=2)
    if nodes_from_zones:
        with open(os.path.join(output, 'zones.json'), 'w') as f:
            json.dump(get_zones_to_save(zones_path), f, indent=2)

    return dataset, nodes_data


def init_empty_dataset(
        aggregation_size: int,
        min_date: datetime,
        max_date: datetime,
        add_weather_data: bool = False,
        weather_db: Optional[str] = None,
        weather_collection: str = 'observations',
) -> Dict[str, Dict[str, Any]]:
    dataset: Dict[str, Dict[str, Any]] = {}
    current_date = min_date
    end_date = max_date
    index = 0
    last_weather = None
    none_counter = 0
    last_hour = None
    while current_date < end_date:
        weather_data = {}
        if add_weather_data:
            # TODO weather is missing now
            weather_date_str = current_date.strftime('%Y-%m-%d %H')
            if current_date.hour != last_hour or last_hour is None:
                fetched = mongo_wrapper.client[weather_db][weather_collection].find_one({'time_str': weather_date_str})
                if fetched is None:
                    fetched = last_weather
                    none_counter += 1
                else:
                    none_counter = 0
            else:
                fetched = last_weather
            weather_data['condition'] = fetched['condition']
            weather_data['temperature'] = fetched['temperature']
            weather_data['wind_speed'] = fetched['wind_speed']
            last_weather = fetched
            last_hour = current_date.hour
            if none_counter > 2:
                logger.warn(f'Weather for {weather_date_str} None in the last {none_counter} step')
        dataset[current_date.isoformat()] = {
            'index': index,
            'date': current_date.isoformat(),
            'n_stations': 0,
            'weather': weather_data,
            'stations': {}
        }
        index += 1
        current_date += timedelta(seconds=aggregation_size)
    return dataset


def get_valid_nodes(db_name: str, collection_name: str) -> List[str]:
    results = mongo_wrapper.client[db_name][collection_name].find({
        'capacity': {'$gt': MIN_CAPACITY}
    }, {'station_id': 1})
    return [row['station_id'] for row in results]


def get_zones(
        zones_path: str,
) -> dict:
    with open(zones_path, 'r') as f:
        zones = json.load(f)
    zones_dict = {}
    for zone_data in zones['zones']:
        zones_dict[zone_data['zone']] = zone_data['nodes']
    return zones_dict


def get_zones_to_save(zones_path: str) -> dict:
    with open(zones_path, 'r') as f:
        zones = json.load(f)
    zones_dict = {}
    for zone_data in zones['zones']:
        zones_dict[zone_data['zone']] = zone_data
    return zones_dict


def get_record_in_interval(
        db_name: str,
        collection_name: str,
        nodes: List[str],
        min_date: datetime,
        max_date: datetime
):
    options = {'allowDiskUse': True}
    pipeline = [
        {
            '$sort': {
                'timestamp': 1
            }
        }, {
            '$match': {
                'timestamp': {
                    '$gte': min_date,
                    '$lte': max_date
                },
                'station_id': {
                    '$in': nodes
                }
            }
        }
    ]
    return mongo_wrapper.client[db_name][collection_name].aggregate(pipeline, **options)


def build_nodes_file(
        nodes: List[str],
        db_name: str,
        collection_name: str,
        sum_filepath: str,
        zones: Optional[Dict[str, List[str]]] = None
) -> dict:
    nodes_data = {}
    results = mongo_wrapper.client[db_name][collection_name].find({'station_id': {'$in': nodes}}, {
        '_id': False,
        'station_updated_at': False
    })
    sum_df = pd.read_csv(sum_filepath)
    sum_perc_col = ((sum_df['bikes'] + sum_df['ebikes']) * 100) / sum_df['total_docks']
    mean_capacity_ratio = sum_perc_col.mean() / 100
    for node_data in results:
        node_extra = {'bikes_percentage': mean_capacity_ratio}
        if zones is not None:
            node_zone_id = get_node_zone(node_data['station_id'], zones)
            node_extra['zone_id'] = node_zone_id
        node_data_to_save = {
            **node_extra,
            **{key: value for key, value in node_data.items()}
        }
        nodes_data[node_data['station_id']] = node_data_to_save

    return {
        'ids': nodes,
        'nodes': nodes_data
    }


def get_node_zone(node_id: str, zones: Dict[str, List[str]]) -> str:
    for zone_id, zone_nodes in zones.items():
        for node in zone_nodes:
            if node == node_id:
                return zone_id

