import json
import os
from datetime import datetime, timedelta
from typing import List, Optional, Any, Dict, Tuple

from bs_datasets import logger, mongo_wrapper
from bs_datasets.data_utils.data_loader import get_all_providers_info, get_provider_info
from bs_datasets.filesystem import create_directory
from bs_datasets.pipelines.docking_stations import DockingStation
from bs_datasets.pipelines.raw_trip_data import raw_trip_data_collection

STAGE_NAME = 'Sub dataset stage'

TIME_UNITS_MAPPING = {
    'second': 1,
    'minute': 60,
    'hour': 3600,
    'day': 86400
}


def multiple_sub_datasets(
        pivot: str,
        n: str,
        output: str,
        provider: str,
        aggregation_unit: str = 'minute',
        aggregation_size: int = 10,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        name_suffix: Optional[str] = None,
        min_trip_duration: int = 60,
        add_weather_data: bool = False,
        weather_db: Optional[str] = None,
        weather_collection: str = 'observations',
        return_and_not_save: bool = False,
        nodes_from_zones: bool = True,
        zones_path: str = 'data/zones/ny/zones.json'
):
    n_values = n.split(',')
    if provider == 'all':
        providers_info = get_all_providers_info()
        providers = list(providers_info.keys())
    elif ',' in provider:
        providers = provider.split(',')
    else:
        providers = [provider]

    results = {}
    for p in providers:
        if p not in results:
            results[p] = {}
        for n_val in n_values:
            res = filter_nodes_from_dataset(
                pivot,
                int(n_val) if not nodes_from_zones else n_val,
                os.path.join(output, p),
                p,
                aggregation_unit,
                aggregation_size,
                min_date,
                max_date,
                name_suffix,
                min_trip_duration,
                add_weather_data,
                weather_db,
                weather_collection,
                return_and_not_save,
                nodes_from_zones,
                zones_path
            )
            results[p][n_val] = res
    if return_and_not_save:
        return results


def filter_nodes_from_dataset(
        pivot: str,
        n: int,
        output: str,
        provider: str,
        aggregation_unit: str,
        aggregation_size: int,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        name_suffix: Optional[str] = None,
        min_trip_duration: int = 60,
        add_weather_data: bool = False,
        weather_db: Optional[str] = None,
        weather_collection: str = 'observations',
        return_and_not_save: bool = False,
        nodes_from_zones: bool = True,
        zones_path: str = 'data/zones/ny/zones.json'
):
    db_name = f'{mongo_wrapper.db_prefix_name}-{provider}'
    provider_info = get_provider_info(provider)
    node_ids = get_nodes_id(pivot, n, provider_info, db_name, nodes_from_zones, zones_path)
    aggregated_data = filter_sub_dataset_query(
        db_name, raw_trip_data_collection, node_ids, aggregation_unit,
        aggregation_size, min_date, max_date, min_trip_duration)

    dataset, initial_date, end_date = init_empty_dataset(
        aggregation_size=aggregation_size * TIME_UNITS_MAPPING[aggregation_unit],
        min_date=min_date, max_date=max_date,
        add_weather_data=add_weather_data, weather_db=weather_db, weather_collection=weather_collection
    )
    all_out_interval_trips = []

    for date_interval_str, _ in dataset.items():
        date_interval = datetime.fromisoformat(date_interval_str)
        stations = {}
        max_duration = TIME_UNITS_MAPPING[aggregation_unit] * aggregation_size
        in_interval_trips = []
        out_interval_trips = []
        if date_interval in aggregated_data:
            interval_data = aggregated_data[date_interval]
            for i, trip in enumerate(interval_data['trips']):
                end_trip_id = trip['stop_trip_id']
                if trip['stop_time'] > date_interval + timedelta(seconds=max_duration):
                    out_interval_trips.append(trip)
                    all_out_interval_trips.append(trip)
                else:
                    in_interval_trips.append(trip)
                    if end_trip_id not in stations:
                        stations[end_trip_id] = {
                            'started': {'in_interval': 0, 'out_interval': 0},
                            'ended': {}
                        }
        # filter the all_out_interval_trips by removing all the previous trips
        # that ended in the current time interval and consider them as the in_interval_trips
        previous_interval_trips = find_and_remove_inside_interval_trips(
            all_out_interval_trips, date_interval, max_duration)
        if len(previous_interval_trips) > 0:
            for trip in previous_interval_trips:
                stop_trip_id = trip['stop_trip_id']
                if stop_trip_id not in stations:
                    stations[stop_trip_id] = {
                        'started': {'in_interval': 0, 'out_interval': 0},
                        'ended': {}
                    }
            in_interval_trips += previous_interval_trips

        if len(out_interval_trips) > 0:
            for trip in out_interval_trips:
                start_id = trip['start_trip_id']
                if start_id in stations:
                    stations[start_id]['started']['out_interval'] += 1
                else:
                    stations[start_id] = {
                        'started': {'in_interval': 0, 'out_interval': 1},
                        'ended': {}
                    }

        for station_id, station_data in stations.items():
            station_data['started']['out_interval'] = len(
                [out_trip for out_trip in out_interval_trips if out_trip['start_trip_id'] == station_id])
            in_interval_count = 0

            for in_trip in in_interval_trips:
                start_trip_id = in_trip['start_trip_id']
                if in_trip['stop_trip_id'] == station_id:
                    in_interval_count += 1
                    if start_trip_id not in station_data['ended']:
                        station_data['ended'][start_trip_id] = {
                            'trip_start_station_id': start_trip_id,
                            'n_bikes': 1
                        }
                    else:
                        station_data['ended'][start_trip_id]['n_bikes'] += 1
            station_data['started']['in_interval'] = in_interval_count

        dataset[date_interval.isoformat()]['stations'] = stations
        dataset[date_interval.isoformat()]['n_stations'] = len(stations)

    # change index
    final_dataset = {}
    for _, val in dataset.items():
        final_dataset[val['index']] = val

    unique_stations = get_unique_stations(node_ids, provider)
    geojson_stations = build_geojson_feature_collection(unique_stations)

    # get filtered zones
    filtered_zones = get_zones_filtered(pivot, zones_path)
    # build nodes data
    nodes_data = {}
    for node_id in node_ids:
        node_data = mongo_wrapper.client[db_name][DockingStation.collection_name].find_one(
            {'trip_id': node_id},
            projection={'_id': False}
        )
        distances = {}
        for n_id, distance in node_data['distances'].items():
            if n_id in node_ids:
                distances[n_id] = distance
        node_data['zone_id'] = get_node_zone(node_id, filtered_zones)
        node_data['distances'] = distances
        mean_percentages = []
        for _, info in node_data['initial_bikes'].items():
            total = info['total_docks']
            bikes = info['total_bikes']
            mean_percentages.append((bikes * 100) / total)

        node_data['bikes_percentage'] = (sum(mean_percentages) / len(mean_percentages)) / 100
        del node_data['initial_bikes']
        nodes_data[node_id] = node_data

    nodes_data = {
        'ids': node_ids,
        'nodes': nodes_data
    }

    if name_suffix is None:
        output = os.path.join(output, f'{n}_nodes-s={initial_date.isoformat()}-e={end_date.isoformat()}')
    else:
        output = os.path.join(output, f'{n}_nodes-{name_suffix}')

    if not return_and_not_save:
        create_directory(output)
        with open(os.path.join(output, 'dataset.json'), 'w') as f:
            json.dump(final_dataset, f, indent=2)
        with open(os.path.join(output, 'nodes.geojson'), 'w') as f:
            json.dump(geojson_stations, f, indent=2)
        with open(os.path.join(output, 'nodes.json'), 'w') as f:
            json.dump(nodes_data, f, indent=2)
        if add_weather_data:
            with open(os.path.join(output, 'weather_stats.json'), 'w') as f:
                json.dump(get_weather_stats(weather_db, weather_collection), f, indent=2)
        if nodes_from_zones:
            with open(os.path.join(output, 'zones.json'), 'w') as f:
                json.dump(get_full_zones_filtered(pivot, zones_path), f, indent=2)
    else:
        return final_dataset, nodes_data


def get_node_zone(node_id: str, filtered_zones: Dict[str, List[str]]) -> str:
    for zone_id, zone_nodes in filtered_zones.items():
        for node in zone_nodes:
            if node == node_id:
                return zone_id


def get_zones_filtered(
        pivot: str,
        zones_path: str,
) -> dict:
    with open(zones_path, 'r') as f:
        zones = json.load(f)
    if pivot == 'none':
        zones_to_filter = None
    else:
        zones_to_filter = pivot.split(',')
    zones_dict = {}
    for zone_data in zones['zones']:
        if zones_to_filter is None:
            zones_dict[zone_data['zone']] = zone_data['nodes']
        elif zone_data['zone'] in zones_to_filter:
            zones_dict[zone_data['zone']] = zone_data['nodes']
    return zones_dict


def get_full_zones_filtered(
        pivot: str,
        zones_path: str
) -> dict:
    with open(zones_path, 'r') as f:
        zones = json.load(f)
    if pivot == 'none':
        zones_to_filter = None
    else:
        zones_to_filter = pivot.split(',')
    zones_dict = {}
    for zone_data in zones['zones']:
        if zones_to_filter is None:
            zones_dict[zone_data['zone']] = zone_data
        elif zone_data['zone'] in zones_to_filter:
            zones_dict[zone_data['zone']] = zone_data
    return zones_dict


def get_nodes_id(
        pivot: str,
        n: int,
        provider_info,
        db_name: str,
        nodes_from_zones: bool,
        zones_path: str,
) -> List[str]:
    if nodes_from_zones:
        with open(zones_path, 'r') as f:
            zones = json.load(f)
        nodes_id: List[str] = []
        if pivot == 'none':
            zones_to_filter = None
            logger.info(f'{STAGE_NAME} | Dataset splitting from all the zones in {zones_path}')
        else:
            zones_to_filter = pivot.split(',')
            logger.info(f'{STAGE_NAME} | Dataset splitting from {zones_to_filter} zones in {zones_path}')
        for zone_data in zones['zones']:
            if zones_to_filter is None:
                nodes_id += zone_data['nodes']
            elif zone_data['zone'] in zones_to_filter:
                nodes_id += zone_data['nodes']
        return nodes_id
    else:
        if pivot == 'none':
            pivot = provider_info['pivot_node']
        logger.info(f'{STAGE_NAME} | Dataset splitting for {n} nodes starting from {pivot}')
        pivot_station = mongo_wrapper.client[db_name][DockingStation.collection_name].find_one({'trip_id': pivot})
        return [n for n in list(pivot_station['distances'].keys())[: n]]


def find_and_remove_inside_interval_trips(
        trips: List[Dict[str, Any]],
        date_interval: datetime,
        interval_duration: int
) -> List[Dict[str, Any]]:
    inside_interval_trips: List[Dict[str, Any]] = []
    max_date = date_interval + timedelta(seconds=interval_duration)
    for i in reversed(range(len(trips))):
        trip = trips[i]
        if trip['stop_time'] < max_date:
            inside_interval_trips.append(trip)
            del trips[i]
    return inside_interval_trips


def init_empty_dataset(
        aggregation_size: int,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        add_weather_data: bool = False,
        weather_db: Optional[str] = None,
        weather_collection: str = 'observations',
) -> Tuple[Dict[str, Dict[str, Any]], datetime, datetime]:
    dataset: Dict[str, Dict[str, Any]] = {}
    current_date = datetime.fromisoformat('2020-01-01')
    end_date = datetime.fromisoformat('2023-01-01')
    if min_date is not None or max_date is not None:
        assert min_date is not None and max_date is not None, \
            'min_date and max_date must be both defined if one is defined'
        current_date = datetime.fromisoformat(min_date)
        end_date = datetime.fromisoformat(max_date)

    initial_date = current_date

    index = 0
    last_weather = None
    none_counter = 0
    last_hour = None
    while current_date < end_date:
        weather_data = {}
        if add_weather_data:
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
    return dataset, initial_date, end_date


def filter_sub_dataset_query(
        db_name: str,
        collection_name: str,
        node_ids: List[str],
        aggregation_unit: str,
        aggregation_size: int,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        min_trip_duration: int = 60,
) -> Dict[datetime, Dict[str, Any]]:
    match_stage_conditions = [
        {
            '$expr': {
                '$in': [
                    '$start_trip_id', node_ids
                ]
            }
        }, {
            '$expr': {
                '$in': [
                    '$stop_trip_id', node_ids
                ]
            }
        }, {
            '$expr': {
                '$gte': [
                    '$duration', min_trip_duration
                ]
            }
        }
    ]
    if min_date is not None or max_date is not None:
        assert min_date is not None and max_date is not None, \
            'min_date and max_date must be both defined if one is defined'

        match_stage_conditions.append({
            '$expr': {
                '$gte': [
                    '$start_time', datetime.fromisoformat(min_date)
                ]
            }
        })
        match_stage_conditions.append({
            '$expr': {
                '$lt': [
                    '$stop_time', datetime.fromisoformat(max_date)
                ]
            }
        })
    options = {'allowDiskUse': True}
    pipeline = [
        {
            '$match': {
                '$and': match_stage_conditions
            }
        }, {
            '$sort': {
                'start_time': 1
            }
        }, {
            '$group': {
                '_id': {
                    'started_hour': {
                        '$dateTrunc': {
                            'date': '$start_time',
                            'unit': aggregation_unit,
                            'binSize': aggregation_size
                        }
                    }
                },
                'n_started': {
                    '$sum': 1
                },
                'trips': {
                    '$push': {
                        'start_time': '$start_time',
                        'start_trip_id': '$start_trip_id',
                        'stop_time': '$stop_time',
                        'stop_trip_id': '$stop_trip_id',
                        'duration': '$duration'
                    }
                },
                'max_duration': {
                    '$max': '$duration'
                }
            }
        }, {
            '$project': {
                '_id': 0,
                'n_trips': '$n_started',
                'started_hour': '$_id.started_hour',
                'max_duration': '$max_duration',
                'trips': '$trips'
            }
        }
    ]
    result_cursor = mongo_wrapper.client[db_name][collection_name].aggregate(pipeline, **options)
    result: Dict[datetime, Dict[str, Any]] = {
        row['started_hour']: row for row in result_cursor
    }
    return result


def get_unique_stations(node_ids: List[str], provider: str) -> List[Dict[str, Any]]:
    db_name = f'{mongo_wrapper.db_prefix_name}-{provider}'
    return list(mongo_wrapper.client[db_name][DockingStation.collection_name].find(
        {'trip_id': {'$in': node_ids}},
        projection={
            '_id': False,
            'distances': False,
            'initial_bikes': False
        }
    ))


def build_geojson_feature_collection(unique_stations: List[Dict[str, Any]]) -> dict:
    features = []
    for station_data in unique_stations:
        properties = {}
        for key, value in station_data.items():
            if key != 'position':
                properties[key] = value
        features.append({
            "type": "Feature",
            "properties": properties,
            "geometry": station_data['position']
        })
    return {
        "type": "FeatureCollection",
        "features": features
    }


def get_weather_stats(db_name, collection_name) -> Dict[str, Any]:
    result = list(mongo_wrapper.client[db_name][collection_name].aggregate([
        {
            '$group': {
                '_id': None,
                'conditions': {
                    '$addToSet': '$condition'
                },
                'maxTemperature': {
                    '$max': '$temperature'
                },
                'minTemperature': {
                    '$min': '$temperature'
                },
                'maxWindSpeed': {
                    '$max': '$wind_speed'
                },
                'minWindSpeed': {
                    '$min': '$wind_speed'
                }
            }
        }
    ]))
    return {
        'conditions': result[0]['conditions'],
        'maxTemperature': result[0]['maxTemperature'],
        'minTemperature': result[0]['minTemperature'],
        'maxWindSpeed': result[0]['maxWindSpeed'],
        'minWindSpeed': result[0]['minWindSpeed'],
    }


