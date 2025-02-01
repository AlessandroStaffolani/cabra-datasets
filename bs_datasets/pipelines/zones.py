import json
import os
from typing import Tuple, List, Dict, Union, NewType, Optional

import pandas as pd
import plotly.express as px
import haversine
from haversine import Unit
from pandas import DataFrame
from shapely import Point, Polygon
from shapely.geometry import shape
from sklearn.cluster import KMeans

from bs_datasets import mongo_wrapper
from bs_datasets.filesystem import create_directory

ZipCode = NewType('ZipCode', Dict[str, Union[Polygon, Dict[str, Union[str, int, float, bool]]]])


def zones_pipeline(
        n_zones: int,
        zone_size: int,
        output: str,
        db_name: str = 'bs_dataset-citibike',
        docking_station_collection_name: str = 'docking_stations',
        zip_codes_geojson_path: str = 'data/ny_map/zip_codes.geojson',
        filter_region: Optional[str] = None,
        show_zones: bool = False,
        **kwargs,
):
    docks_df, zip_codes_geo, zip_codes_df, zip_codes_mapping = load_data_structures(
        db_name, docking_station_collection_name, zip_codes_geojson_path
    )
    if filter_region is not None:
        docks_df = docks_df[docks_df.region_id == filter_region]

    assert n_zones * zone_size == len(docks_df), 'Number of docking stations is not equal to n_zones * zones_size'

    sorted_zip_codes = get_sorted_by_distance_zip_codes(docks_df, zip_codes_mapping)
    all_nodes = []
    for _, zip_code in sorted_zip_codes.items():
        for node in zip_code['nodes']:
            all_nodes.append(node)

    nodes_features_mapping = {}
    for _, row in docks_df.iterrows():
        nodes_features_mapping[(row['lng'], row['lat'])] = row['trip_id']
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
        zone_nodes = nodes_features[y_km == i, :]
        zones[str(i)] = []
        zones_centroid[str(i)] = Polygon(zone_nodes).centroid
        for node in zone_nodes:
            node_id = nodes_features_mapping[tuple(node)]
            zones[str(i)].append(node_id)

    save_zones(zones, output, zones_centroid)

    if show_zones:
        visualize_zones(zones, docks_df)


def load_data_structures(
        db_name: str,
        docking_station_collection_name: str,
        zip_codes_geojson_path: str
) -> Tuple[DataFrame, dict, DataFrame, dict]:
    docking_station_data = list(mongo_wrapper.client[db_name][docking_station_collection_name].find(
        {'capacity': {'$gt': 0}},
        {'_id': 1, 'trip_id': 1, 'name': 1, 'capacity': 1, 'position': 1, 'station_id': 1, 'short_name': 1,
         'region_id': 1, 'legacy_id': 1, 'external_id': 1}
    ))
    docks_df = get_docks_df(docking_station_data)
    zip_codes_geo, zip_codes_df, zip_codes_mapping = get_zip_codes_data(zip_codes_geojson_path)

    docks_df = get_docks_zip_merged(docks_df, zip_codes_mapping)

    return docks_df, zip_codes_geo, zip_codes_df, zip_codes_mapping


def get_docks_df(docking_station_data: List[dict]) -> DataFrame:
    data = []
    columns = ['trip_id', 'name', 'capacity', 'lng', 'lat',
               'station_id', 'short_name', 'region_id', 'legacy_id', 'external_id']

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


def get_zip_codes_data(geojson_path: str) -> Tuple[dict, DataFrame, Dict[int, ZipCode]]:
    with open(geojson_path, 'r') as f:
        zip_codes_geo = json.load(f)

    zip_codes_mapping = {}
    zip_codes_data = []
    zip_codes_columns_filter = ['OBJECTID', 'postalCode', 'PO_NAME', 'STATE', 'borough']
    zip_codes_columns = ['id', 'postal_code', 'name', 'state', 'borough']
    for code_data in zip_codes_geo['features']:
        tmp = []
        for key, val in code_data['properties'].items():
            if key in zip_codes_columns_filter:
                tmp.append(val)
        zip_codes_data.append(tmp)
        zip_codes_mapping[code_data['properties']['OBJECTID']] = {
            'id': code_data['properties']['OBJECTID'],
            'shape': shape(code_data['geometry']),
            'properties': {zip_codes_columns[i]: tmp[i] for i in range(len(zip_codes_columns))}
        }

    zip_codes_df = DataFrame(data=zip_codes_data, columns=zip_codes_columns)

    return zip_codes_geo, zip_codes_df, zip_codes_mapping


def get_docks_zip_merged(docks_df, zip_codes_mapping):
    columns = ['trip_id', 'zip_code', 'zip_name', 'zip_state', 'zip_borough']
    data = []

    for index, row in docks_df.iterrows():
        dock_point = Point(row['lng'], row['lat'])
        found = False
        min_distance = None
        min_id = None
        for zip_id, zip_info in zip_codes_mapping.items():
            poligon = zip_info['shape']
            distance = poligon.distance(dock_point)
            if min_distance is None or distance < min_distance:
                min_distance = distance
                min_id = zip_id
            if poligon.intersects(dock_point):
                found = True
                data.append([
                    row['trip_id'],
                    zip_info['properties']['postal_code'],
                    zip_info['properties']['name'],
                    zip_info['properties']['state'],
                    zip_info['properties']['borough']
                ])
                break
        if not found:
            min_distant_zip_info = zip_codes_mapping[min_id]
            data.append([
                row['trip_id'],
                min_distant_zip_info['properties']['postal_code'],
                min_distant_zip_info['properties']['name'],
                min_distant_zip_info['properties']['state'],
                min_distant_zip_info['properties']['borough']
            ])

    df = pd.DataFrame(data=data, columns=columns)
    return pd.merge(docks_df, df, on='trip_id')


def get_sorted_by_distance_zip_codes(docks_df: DataFrame, zip_codes_mapping: Dict[int, ZipCode]) -> Dict[float, dict]:
    used_codes = docks_df['zip_code'].unique().tolist()

    southeast_centroid_lat = None
    southeast_post_code = None

    unique_zip_codes = {}
    for obj_id, zip_code in zip_codes_mapping.items():
        postal_code = zip_code['properties']['postal_code']
        if postal_code in used_codes:
            if postal_code not in unique_zip_codes:
                unique_zip_codes[postal_code] = {
                    'shape': zip_code['shape'],
                    'id': zip_code['id'],
                    'postal_code': postal_code,
                    'properties': zip_code['properties'],
                    'nodes': nodes_df_to_dict_point(docks_df[docks_df['zip_code'] == postal_code]),
                    'centroid': zip_code['shape'].centroid
                }
            else:
                new_polygon = unique_zip_codes[postal_code]['shape'].union(zip_code['shape'])
                unique_zip_codes[postal_code]['shape'] = new_polygon
                unique_zip_codes[postal_code]['centroid'] = new_polygon.centroid
            centroid_lat = unique_zip_codes[postal_code]['centroid'].coords[0][1]
            if southeast_centroid_lat is None or centroid_lat < southeast_centroid_lat:
                southeast_centroid_lat = centroid_lat
                southeast_post_code = unique_zip_codes[postal_code]

    distances_zip_codes = {}
    for postal_code, postal_code_struct in unique_zip_codes.items():
        distance = postal_code_struct['centroid'].distance(southeast_post_code['centroid'])
        distances_zip_codes[distance] = postal_code_struct

    sorted_zip_codes = {}
    for key in sorted(distances_zip_codes.keys()):
        sorted_zip_codes[key] = distances_zip_codes[key]
        sorted_zip_codes[key]['nodes'] = sorted(
            sorted_zip_codes[key]['nodes'],
            key=lambda x: sorted_zip_codes[key]['centroid'].distance(x['point'])
        )

    return sorted_zip_codes


def nodes_df_to_dict_point(nodes):
    nodes_list = []
    for _, node_data in nodes.iterrows():
        nodes_list.append({
            'trip_id': node_data['trip_id'],
            'point': Point(node_data['lng'], node_data['lat'])
        })
    return nodes_list


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
            'nodes': [node for node in nodes],
            'centroid': zone_centroid.coords[0],
            'distances': sorted_distances
        })

    create_directory(output)
    with open(os.path.join(output, 'zones.json'), 'w') as f:
        json.dump({'zones': zones_to_save}, f, indent=2)


def visualize_zones(zones, docks_df):
    columns = ['zone', 'trip_id', 'lat', 'lng', 'size', 'node_capacity']
    data = []

    for zone, zone_nodes in zones.items():
        for node in zone_nodes:
            node_data = docks_df[docks_df['trip_id'] == node]
            data.append([
                str(zone),
                node,
                node_data['lat'].item(),
                node_data['lng'].item(),
                len(zone_nodes),
                node_data['capacity'].item()
            ])

    df = pd.DataFrame(data=data, columns=columns)

    fig = px.scatter_mapbox(df, lat='lat', lon='lng', color='zone',
                            mapbox_style='carto-positron',
                            center={"lat": 40.7831, "lon": -73.9712}, height=850, width=1200, zoom=10.2,
                            hover_name="trip_id", hover_data=['size', 'node_capacity'],
                            color_continuous_scale=px.colors.sequential.Viridis)

    fig.update_layout(
        title=f'Docking station distribution',
        autosize=True,
    )
    fig.show()
