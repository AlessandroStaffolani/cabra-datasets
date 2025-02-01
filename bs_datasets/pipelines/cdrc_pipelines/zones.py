import json
import os
from multiprocessing.pool import ThreadPool
from typing import List, Dict

import haversine
import pandas as pd
import plotly.express as px
from haversine import Unit
from pandas import DataFrame
from shapely import Point, Polygon
from sklearn.cluster import KMeans

from bs_datasets import mongo_wrapper, logger
from bs_datasets.data_utils.data_loader import load_cdrc_providers_info, load_cdrc_provider_info
from bs_datasets.filesystem import create_directory
from bs_datasets.pipelines.cdrc_pipelines.defaults import DB_PREFIX
from bs_datasets.pipelines.cdrc_pipelines.docking_stations import MIN_CAPACITY

STAGE_NAME = 'Zones extraction stage'


def zones_pipeline(
        provider: str,
        n_zones: int,
        docking_station_collection_name: str = 'docking_stations',
        show_zones: bool = False,
        **kwargs,
):
    if provider == 'all':
        providers_info = load_cdrc_providers_info()
        logger.info(f'{STAGE_NAME} | Starting for all {len(providers_info)} providers')
        pool = ThreadPool(processes=len(providers_info))
        for p, _ in providers_info.items():
            pool.apply_async(_zones_pipeline,
                             args=(n_zones, p, docking_station_collection_name,
                                   show_zones),
                             error_callback=lambda e: logger.exception(e))
        pool.close()
        pool.join()
        logger.info(f'{STAGE_NAME} | Completed all {len(providers_info)} providers')
    else:
        _zones_pipeline(n_zones, provider, docking_station_collection_name, show_zones)


def _zones_pipeline(
        n_zones: int,
        provider: str,
        docking_station_collection_name: str = 'docking_stations',
        show_zones: bool = False,
        **kwargs,
):
    logger.info(f'{STAGE_NAME} | Started provider {provider}')
    provider_info = load_cdrc_provider_info(provider)
    output = provider_info['base_path']
    n_zones = n_zones if n_zones != -1 else provider_info['suggested_zones']
    recursive = provider_info['use_recursion']
    recursion_threshold = provider_info['recursion_threshold']
    db_name = f'{DB_PREFIX}-{provider}'
    docks_df = load_data_structures(db_name, docking_station_collection_name)

    zones, zones_centroid = _get_zones(docks_df, n_zones)
    if recursive:
        nodes_to_exclude = []
        excluded_zones_arr = []
        excluded_centroid_arr = []
        for zone_id, zone_nodes in zones.items():
            if len(zone_nodes) <= recursion_threshold:
                nodes_to_exclude += zone_nodes
                excluded_zones_arr.append(zone_nodes)
                excluded_centroid_arr.append(zones_centroid[zone_id])
        excluded_zones = {}
        excluded_centroid = {}
        for id_counter in range(len(excluded_zones_arr)):
            excluded_zones[str(id_counter)] = excluded_zones_arr[id_counter]
            excluded_centroid[str(id_counter)] = excluded_centroid_arr[id_counter]
        new_zones, new_zones_centroid = _get_zones(
            docks_df[~docks_df['station_id'].isin(nodes_to_exclude)].reset_index(drop=True),
            n_zones,
            len(excluded_zones)
        )
        zones = excluded_zones
        zones_centroid = excluded_centroid
        for z_id, zone in new_zones.items():
            zones[z_id] = zone
            zones_centroid[z_id] = new_zones_centroid[z_id]

    sanity_check_zones(zones, docks_df, provider)

    save_zones(zones, output, zones_centroid)

    if show_zones:
        visualize_zones(zones, docks_df)
    logger.info(f'{STAGE_NAME} | Completed provider {provider}')


def _get_zones(docks_df: pd.DataFrame, n_zones: int, zone_index_start: int = 0):
    nodes_coords_to_station_id = {}
    for _, row in docks_df.iterrows():
        nodes_coords_to_station_id[(row['lng'], row['lat'])] = row['station_id']
    nodes_features = docks_df[['lng', 'lat']].to_numpy()
    km = KMeans(
        n_clusters=n_zones, init='random',
        n_init=100, max_iter=30000,
        tol=1e-18, random_state=0
    )
    y_km = km.fit_predict(nodes_features)
    u_labels = km.labels_

    zones = {}
    zones_centroid = {}

    for i in u_labels:
        cluster_nodes_coordinates = nodes_features[y_km == i, :]
        zones[str(i + zone_index_start)] = []
        zones_centroid[str(i + zone_index_start)] = Point(
            cluster_nodes_coordinates[:, 0].mean(), cluster_nodes_coordinates[:, 1].mean())
        for node_coords in cluster_nodes_coordinates:
            node_id = nodes_coords_to_station_id[tuple(node_coords)]
            zones[str(i + zone_index_start)].append(node_id)

    return zones, zones_centroid


def sanity_check_zones(zones: Dict[str, List[str]], docks_df: pd.DataFrame, provider: str):
    nodes = {}
    nodes_to_find = {row['station_id']: False for _, row in docks_df.iterrows()}
    zones_with_errors = {}
    duplicated_nodes = {}
    for zone_id, zone_nodes in zones.items():
        for node in zone_nodes:
            nodes_to_find[node] = True
            if node not in nodes:
                nodes[node] = zone_id
            else:
                if zone_id not in zones_with_errors:
                    zones_with_errors[zone_id] = True
                if nodes[node] not in zones_with_errors:
                    zones_with_errors[nodes[node]] = True
                if node not in duplicated_nodes:
                    duplicated_nodes[node] = 2
                else:
                    duplicated_nodes[node] += 2

    nodes_without_zone = [node_id for node_id, found in nodes_to_find.items() if not found]
    if len(zones_with_errors) > 0 or len(duplicated_nodes) > 0:
        raise Exception(f'For provider {provider} - Some nodes are in duplicated across some zones\n'
                        f'\tZones with duplicates {len(zones_with_errors)}: {zones_with_errors}\n'
                        f'\tNodes in multiple zones {len(duplicated_nodes)}: {duplicated_nodes}')
    if len(nodes_without_zone) > 0:
        raise Exception(f'For provider {provider} - Some nodes are left without a zone\n'
                        f'\tNodes without a zone are {len(nodes_without_zone)}: {nodes_without_zone}')
    logger.debug(f'For provider {provider} - Zones sanity check: OK')


def load_data_structures(
        db_name: str,
        docking_station_collection_name: str,
) -> DataFrame:
    docking_station_data = list(mongo_wrapper.client[db_name][docking_station_collection_name].find(
        {'capacity': {'$gt': MIN_CAPACITY}},
        {'_id': 1, 'station_id': 1, 'name': 1, 'capacity': 1, 'position': 1}
    ))
    return get_docks_df(docking_station_data)


def get_docks_df(docking_station_data: List[dict]) -> DataFrame:
    data = []
    columns = ['station_id', 'name', 'capacity', 'lng', 'lat']

    for dock in docking_station_data:
        tmp = []
        for key, val in dock.items():
            if key == 'position':
                tmp.append(val['coordinates'][0])
                tmp.append(val['coordinates'][1])
            elif key != '_id':
                tmp.append(val)
        data.append(tmp)
    return DataFrame(data=data, columns=columns)


def save_zones(zones, output: str, zones_centroid: Dict[str, Point]):
    zones_to_save = []
    for zone_id, nodes in zones.items():
        zone_centroid = zones_centroid[zone_id]
        distances = {}
        for other_zone_id, other_zone_centroid in zones_centroid.items():
            distances[other_zone_id] = haversine.haversine(
                (zone_centroid.coords[0][1], zone_centroid.coords[0][0]),
                (other_zone_centroid.coords[0][1], other_zone_centroid.coords[0][0]),
                Unit.METERS
            )
        inverted_key_dict = {dist: z_id for z_id, dist in distances.items()}
        sorted_distances = {}
        for dist in sorted(inverted_key_dict.keys()):
            sorted_distances[inverted_key_dict[dist]] = dist
        zones_to_save.append({
            'zone': zone_id,
            'n_nodes': len(nodes),
            'nodes': [node for node in nodes],
            'centroid': zone_centroid.coords[0],
            'distances': sorted_distances
        })

    create_directory(output)
    with open(os.path.join(output, 'zones.json'), 'w') as f:
        json.dump({'zones': zones_to_save}, f, indent=2)


def visualize_zones(zones, docks_df):
    columns = ['zone', 'station_id', 'lat', 'lng', 'size', 'node_capacity']
    data = []

    for zone, zone_nodes in zones.items():
        for node in zone_nodes:
            node_data = docks_df[docks_df['station_id'] == node]
            data.append([
                str(zone),
                node,
                node_data['lat'].item(),
                node_data['lng'].item(),
                len(zone_nodes),
                node_data['capacity'].item()
            ])

    df = pd.DataFrame(data=data, columns=columns)
    print(f'Zones generated {len(zones)}')
    print(df.describe())

    fig = px.scatter_mapbox(df, lat='lat', lon='lng', color='zone',
                            mapbox_style='carto-positron',
                            center={"lat": df['lat'].mean(), "lon": df['lng'].mean()}, height=850, width=1200, zoom=10.2,
                            hover_name="station_id", hover_data=['size', 'node_capacity'],
                            color_continuous_scale=px.colors.sequential.Viridis)

    fig.update_layout(
        title=f'Docking station distribution',
        autosize=True,
    )
    fig.show()
