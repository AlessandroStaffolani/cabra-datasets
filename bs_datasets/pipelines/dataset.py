import json
import os.path
from typing import Dict, Any, List

from bs_datasets import logger, mongo_wrapper
from bs_datasets.data_utils.data_loader import get_all_providers_info
from bs_datasets.filesystem import create_directory_from_filepath, create_directory
from bs_datasets.pipelines.docking_stations import DockingStation
from bs_datasets.pipelines.trip_data import trip_data_collection


STAGE_NAME = 'Dataset stage'

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def dataset_pipeline(
        provider: str,
        output: str,
        year: str
):
    if provider == 'all':
        providers = get_all_providers_info()
        for p, _ in providers.items():
            dataset_pipeline_single_provider(p, os.path.join(output, p, f'{year}.json'), year)
    else:
        dataset_pipeline_single_provider(provider, output, year)


def dataset_pipeline_single_provider(
        provider: str,
        output: str,
        year: str,
):
    logger.info(f'{STAGE_NAME} | Started for provider {provider}')
    create_dataset_from_trip_data(provider, output, year)
    logger.info(f'{STAGE_NAME} | Completed')


def create_dataset_from_trip_data(provider: str, output: str, year: str,):
    db_name = f'{mongo_wrapper.db_prefix_name}-{provider}'
    trip_data_collection_name = f'{trip_data_collection}-{year}'
    options = {'allowDiskUse': True}
    aggregate_query = [
        {
            '$sort': {
                'date': 1
            }
        }, {
            '$group': {
                '_id': {
                    'date': '$date',
                    'station': '$station'
                },
                'operations': {
                    '$push': {
                        'value': '$value',
                        'operation': '$operation'
                    }
                },
                'station': {
                    '$first': '$station'
                },
                'date': {
                    '$first': '$date'
                },
                'n_stations': {
                    '$sum': 1
                },
                'started_rides': {
                    '$sum': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$operation', 'start'
                                ]
                            },
                            'then': '$value',
                            'else': 0
                        }
                    }
                },
                'ended_rides': {
                    '$sum': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$operation', 'end'
                                ]
                            },
                            'then': '$value',
                            'else': 0
                        }
                    }
                }
            }
        }, {
            '$project': {
                '_id': 0,
                '_station_id': '$station',
                'date': '$date',
                'started_rides': '$started_rides',
                'ended_rides': '$ended_rides'
            }
        }, {
            '$lookup': {
                'from': 'docking_stations',
                'localField': '_station_id',
                'foreignField': 'trip_id',
                'as': 'station_doc'
            }
        }, {
            '$replaceRoot': {
                'newRoot': {
                    '$mergeObjects': [
                        {
                            '$arrayElemAt': [
                                '$station_doc', 0
                            ]
                        }, '$$ROOT'
                    ]
                }
            }
        }, {
            '$project': {
                '_id': 0,
                'position': 0,
                'distances': 0,
                'initial_bikes': 0,
                'station_doc': 0
            }
        }, {
            '$group': {
                '_id': {
                    'date': '$date'
                },
                'stations': {
                    '$push': {
                        'station_id': '$_station_id',
                        'date': '$date',
                        'started_rides': '$started_rides',
                        'ended_rides': '$ended_rides',
                        'capacity': '$capacity',
                        'slots_necessary': {
                            '$subtract': [
                                '$started_rides', '$ended_rides'
                            ]
                        }
                    }
                },
                'n_stations': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                '_id.date': 1
            }
        }
    ]
    dataset_result = mongo_wrapper.client[db_name][trip_data_collection_name].aggregate(aggregate_query, **options)
    dataset_docs = {}
    missing_stations = {}
    invalid_stations = {}
    i = 0
    for row in dataset_result:
        stations = {}
        for station_data in row['stations']:
            if 'capacity' in station_data:
                capacity = station_data['capacity']
                slots_necessary = station_data['slots_necessary']
                station_doc = {
                    'station_id': station_data['station_id'],
                    'started_rides': station_data['started_rides'],
                    'ended_rides': station_data['ended_rides'],
                    'capacity': station_data['capacity'],
                    'slots_necessary': station_data['slots_necessary']
                }
                if abs(slots_necessary) > capacity:
                    logger.warning(
                        f'Dataset pipeline | '
                        f'Station {station_data["station_id"]} '
                        f'needs more slots than its capacity for date {row["_id"]["date"]}')
                    if station_data["station_id"] not in invalid_stations:
                        invalid_stations[station_data["station_id"]] = True
                stations[station_data['station_id']] = station_doc
            else:
                if station_data['station_id'] in missing_stations:
                    missing_stations[station_data['station_id']] += 1
                else:
                    missing_stations[station_data['station_id']] = 1
        dataset_docs[i] = {
            'index': i,
            'date': row['_id']['date'].strftime(DATE_FORMAT),
            'n_stations': row['n_stations'],
            'stations': stations
        }
        i += 1
        if i % 30 == 0:
            logger.debug(f'Dataset pipeline | processed {i} entries')
    clean_invalid_stations(dataset_docs, invalid_stations)
    unique_stations = get_unique_stations(dataset_docs, provider)
    geojson_stations = build_geojson_feature_collection(unique_stations)
    create_directory_from_filepath(output)
    with open(output, 'w') as f:
        json.dump(dataset_docs, f, indent=2)
    with open(output.replace('.json', '-stations.json'), 'w') as f:
        json.dump(unique_stations, f, indent=2)
    with open(output.replace('.json', '-stations.geojson'), 'w') as f:
        json.dump(geojson_stations, f, indent=2)
    logger.warning(f'{STAGE_NAME} | '
                   f'{len(missing_stations)} stations are not present in '
                   f'{DockingStation.collection_name} collection for provider {provider} | '
                   f'missing stations: {missing_stations}')
    logger.info(f'{STAGE_NAME} | Saved {len(dataset_docs)} entries in {output} '
                f'and found {len(unique_stations)} unique stations')


def clean_invalid_stations(dataset_docs: dict, invalid_stations: Dict[str, bool]):
    for invalid_s, _ in invalid_stations.items():
        for date, info in dataset_docs.items():
            if invalid_s in info['stations']:
                del dataset_docs[date]['stations'][invalid_s]
                dataset_docs[date]['n_stations'] -= 1
    logger.info(f'{STAGE_NAME} | Removed {len(invalid_stations)} invalid stations')


def get_unique_stations(dataset_docs: dict, provider: str) -> List[Dict[str, Any]]:
    station_ids = {}
    for date, info in dataset_docs.items():
        for station_id, _ in info['stations'].items():
            if station_id not in station_ids:
                station_ids[station_id] = True

    db_name = f'{mongo_wrapper.db_prefix_name}-{provider}'
    return list(mongo_wrapper.client[db_name][DockingStation.collection_name].find(
        {'trip_id': {'$in': list(station_ids.keys())}},
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


def multiple_subdatasets(pivot: str, n: str, source: str, output: str, provider: str):
    n_values = n.split(',')
    dataset_filename = source.split('/')[-1]
    for n_val in n_values:
        filter_nodes_from_dataset(
            pivot,
            int(n_val),
            source,
            os.path.join(output, dataset_filename.replace('.json', f'-{n_val}_nodes')),
            provider)


def filter_nodes_from_dataset(pivot: str, n: int, source: str, output: str, provider: str):
    logger.info(f'{STAGE_NAME} | Dataset splitting for {n} nodes starting from {pivot}')
    db_name = f'{mongo_wrapper.db_prefix_name}-{provider}'
    pivot_station = mongo_wrapper.client[db_name][DockingStation.collection_name].find_one({'trip_id': pivot})
    nodes_ids: List[str] = [n for n in list(pivot_station['distances'].keys())[: n]]
    with open(source, 'r') as f:
        dataset = json.load(f)

    new_dataset = {}
    new_index = 0
    for index, nodes_info in dataset.items():
        n_stations = 0
        stations = {}
        for station_id, station_info in nodes_info['stations'].items():
            if station_id in nodes_ids:
                n_stations += 1
                stations[station_id] = station_info
        if n_stations > 0:
            new_dataset[new_index] = {
                'index': new_index,
                'date': nodes_info['date'],
                'n_stations': n_stations,
                'stations': stations
            }
            new_index += 1
    create_directory(output)
    with open(os.path.join(output, 'dataset.json'), 'w') as f:
        json.dump(new_dataset, f, indent=2)

    # build nodes data
    nodes_data = {}
    for node_id in nodes_ids:
        node_data = mongo_wrapper.client[db_name][DockingStation.collection_name].find_one(
            {'trip_id': node_id},
            projection={'_id': False}
        )
        distances = {}
        for n_id, distance in node_data['distances'].items():
            if n_id in nodes_ids:
                distances[n_id] = distance
        node_data['distances'] = distances
        mean_percentages = []
        for _, info in node_data['initial_bikes'].items():
            total = info['total_docks']
            bikes = info['total_bikes']
            mean_percentages.append((bikes * 100)/total)

        node_data['bikes_percentage'] = (sum(mean_percentages) / len(mean_percentages)) / 100
        del node_data['initial_bikes']
        nodes_data[node_id] = node_data

    nodes_data = {
        'ids': nodes_ids,
        'nodes': nodes_data
    }
    with open(os.path.join(output, 'nodes.json'), 'w') as f:
        json.dump(nodes_data, f, indent=2)
    logger.info(f'{STAGE_NAME} | Dataset split completed and saved in {output}')
