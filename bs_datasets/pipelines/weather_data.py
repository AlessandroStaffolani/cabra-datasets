import json
import requests
import pytz
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Any, Dict

from pymongo import ASCENDING

from bs_datasets import logger, mongo_wrapper

API_MAPPING_FILE = 'data/weather_api.json'

MAX_DAYS_PER_REQUEST = 30

DATE_FORMAT_STR = '%Y-%m-%d'
DATE_FORMAT_API = '%Y%m%d'

STAGE_NAME = 'Weather data'

GMT_TIME_ZONE = pytz.timezone('GMT')


@dataclass
class TimeInterval:
    startDate: datetime
    endDate: datetime

    def to_dict(self, apply_api_format: bool = True):
        return {
            'startDate': self.startDate if not apply_api_format else self.startDate.strftime(DATE_FORMAT_API),
            'endDate': self.endDate if not apply_api_format else self.endDate.strftime(DATE_FORMAT_API)
        }

    def __str__(self):
        return f'<startDate={self.startDate.strftime(DATE_FORMAT_STR)} endDate={self.endDate.strftime(DATE_FORMAT_STR)}>'

    def to_log(self):
        return f'{self.startDate.strftime(DATE_FORMAT_STR)} - {self.endDate.strftime(DATE_FORMAT_STR)}'


def fetch_weather_data(
        city: str,
        start: str,
        end: str,
        collection_name: str = 'observations',
):
    logger.info(
        f'{STAGE_NAME} | Started weather data acquisition for city {city} and period: {start} - {end}')
    db_name = f'weather-{city}'
    with open(API_MAPPING_FILE, 'r') as f:
        api_info = json.load(f)[city]
    start_date = datetime.fromisoformat(start)
    end_date = datetime.fromisoformat(end)
    intervals = get_date_intervals(start_date, end_date)
    db_fields = list(api_info['fieldsMapping'].values())
    create_weather_data_indexes(db_name, collection_name, db_fields)
    for i, time_interval in enumerate(intervals):
        logger.info(f'{STAGE_NAME} | Processing time interval {i+1}/{len(intervals)}: {time_interval.to_log()}')
        fetch_data(time_interval, api_info, db_name, collection_name)
    logger.info(f'{STAGE_NAME} | Completed')


def get_date_intervals(start: datetime, end: datetime) -> List[TimeInterval]:
    days_interval = (end - start).days
    intervals: List[TimeInterval] = []
    if days_interval <= MAX_DAYS_PER_REQUEST:
        intervals.append(TimeInterval(start, end))
    else:
        tmp_start = start
        tmp_end = start + timedelta(days=MAX_DAYS_PER_REQUEST)
        while tmp_start < end:
            intervals.append(TimeInterval(tmp_start, min(tmp_end, end)))
            tmp_start = tmp_end
            tmp_end = tmp_end + timedelta(days=MAX_DAYS_PER_REQUEST)
    return intervals


def fetch_data(interval: TimeInterval, api_info: Dict[str, Any], db_name: str, collection_name: str):
    url = api_info['url']
    params = {
        'apiKey': api_info['apiKey'],
        'units': api_info['units'],
        **interval.to_dict(apply_api_format=True)
    }
    data_json = requests.get(url, params).json()
    db_data: List[Dict[str, Any]] = []
    if data_json['metadata']['status_code'] == 200:
        fields_mapping = api_info['fieldsMapping']
        time_fields = api_info['timeFields']
        timezone = pytz.timezone(api_info['timeZone'])
        for observation in data_json['observations']:
            obs_data = {}
            for key, value in observation.items():
                if key in fields_mapping:
                    if key in time_fields:
                        value_date = datetime.fromtimestamp(value)
                        obs_data[f'{fields_mapping[key]}_utc'] = value_date
                        obs_data[f'{fields_mapping[key]}_str'] = value_date.astimezone(timezone).strftime('%Y-%m-%d %H')
                        obs_data[f'{fields_mapping[key]}_timezone'] = timezone.zone
                    else:
                        obs_data[fields_mapping[key]] = value
            db_data.append(obs_data)
        mongo_wrapper.client[db_name][collection_name].insert_many(db_data)
    else:
        logger.error('An error occurred while fetching the data from api source')
        logger.error(data_json['errors'])


def create_weather_data_indexes(db_name, collection_name, fields: List[str]):
    indexes = [(field, ASCENDING) for field in fields if field != 'extra_column']
    mongo_wrapper.client[db_name][collection_name].create_index(indexes, background=True)
