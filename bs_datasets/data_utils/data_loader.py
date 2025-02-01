import json
import os
from dataclasses import dataclass
from typing import List, Optional, Dict, Union

import pandas as pd
import requests

from bs_datasets.utils import log_info

DATASETS_MAPPING_PATH = 'data/datasets_mappings.json'
CDRC_DATASETS_MAPPING_PATH = 'data/cdrc_datasets_mappings.json'


def count_file_rows(path: str) -> int:
    return sum(1 for _ in open(path))


def load_csv_header(csv_path: str) -> List[str]:
    df = pd.read_csv(csv_path, nrows=1)
    return df.columns.tolist()


def load_csv_rows(csv_path: str, n_rows: Optional[int] = None,
                  skiprows: Optional[int] = None, names: Optional[List[str]] = None):
    if names is None:
        names = load_csv_header(csv_path)
    col_types = {}
    parse_dates = []
    mapping_date_fields = get_providers_mapping_date_fields()
    for n in names:
        if 'lat' in n or 'lng' in n:
            col_types[n] = float
        if n in mapping_date_fields:
            col_types[n] = str
            parse_dates.append(n)
        else:
            col_types[n] = str
    return pd.read_csv(csv_path, nrows=n_rows, skiprows=skiprows, header=0,
                       names=names, dtype=col_types, parse_dates=parse_dates)


def load_csv_file(
        csv_path: str,
        n_rows: Optional[int] = 50,
        verbose: bool = False,
        log_prefix: str = ''
) -> pd.DataFrame:
    if os.path.exists(csv_path):
        if n_rows is None:
            yield pd.read_csv(csv_path)
        else:
            columns = load_csv_header(csv_path)
            total_rows = count_file_rows(csv_path) - 1
            chunks = (total_rows // n_rows) + 1
            log_info(f'{log_prefix}CSV rows: {total_rows} - Chunks: {chunks}', verbose)
            loaded_rows = 0
            for i in range(chunks):
                log_info(f'{log_prefix}Processing chuck {i+1}/{chunks}', verbose)
                yield load_csv_rows(csv_path, n_rows, loaded_rows, columns)
                loaded_rows += n_rows

    else:
        raise AttributeError(f'csv file path not exists at "{csv_path}"')


def load_station_information(provider: str, convert_to_map: bool = False,
                             id_field: str = 'station_id'):
    with open(DATASETS_MAPPING_PATH, 'r') as f:
        mappings = json.load(f)

    gbfs_url = mappings[provider]['gbfs_url']
    gbfs_data = requests.get(gbfs_url).json()
    stations_information = gbfs_data['data']['en']['feeds'][mappings[provider]['station_information_index']]
    assert stations_information['name'] == 'station_information'
    stations_data_url = stations_information['url']
    stations_data = requests.get(stations_data_url).json()
    stations = stations_data['data']['stations']
    if convert_to_map:
        stations_dict = {}
        for station in stations:
            stations_dict[station[id_field]] = station
        return stations_dict
    else:
        return stations


def load_provider_stats(provider: str) -> Dict[str, Dict[str, int]]:
    with open(DATASETS_MAPPING_PATH, 'r') as f:
        mappings = json.load(f)

    obsd_url = mappings[provider]['obsd_url']
    obsd_data = requests.get(obsd_url).json()
    all_stats = obsd_data['stats']
    stats = {}
    for s in all_stats:
        if s['sdate'] not in stats:
            stats[s['sdate']] = {}
        if s['stat'] == 'bikes':
            stats[s['sdate']]['bikes'] = int(s['svalue'])
        if s['stat'] == 'docks':
            stats[s['sdate']]['docks'] = int(s['svalue'])
        if s['stat'] == 'stations':
            stats[s['sdate']]['stations'] = int(s['svalue'])
    to_delete = [key for key, val in stats.items() if len(val) != 3]
    for key in to_delete:
        del stats[key]
    return stats


def get_all_providers_info() -> dict:
    with open(DATASETS_MAPPING_PATH, 'r') as f:
        mappings = json.load(f)
    return mappings


def get_provider_info(provider: str) -> dict:
    return get_all_providers_info()[provider]


def get_providers_mapping_date_fields() -> List[str]:
    fields: List[str] = []
    providers = get_all_providers_info()
    for provider, data in providers.items():
        for _, csv_fields_mapping in data['csv_head_mapping'].items():
            fields.append(csv_fields_mapping['start_time'])
            fields.append(csv_fields_mapping['stop_time'])
    return fields


def load_cdrc_providers_info() -> dict:
    with open(CDRC_DATASETS_MAPPING_PATH, 'r') as f:
        mappings = json.load(f)
    return mappings


def load_cdrc_provider_info(provider: str) -> Dict[str, Union[str, Dict[str, str]]]:
    return load_cdrc_providers_info()[provider]
