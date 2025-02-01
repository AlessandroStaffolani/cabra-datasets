import json
import os.path
import zipfile
from multiprocessing.pool import ThreadPool

import wget

from bs_datasets import logger
from bs_datasets.data_utils.data_loader import get_provider_info, DATASETS_MAPPING_PATH
from bs_datasets.filesystem import create_directory


STAGE_NAME = 'Downloader stage'


def download_trace_files(provider: str, output: str, year: str, parallel: int):
    logger.info(f'{STAGE_NAME} | Starting for provider {provider} and year {year}')
    if provider == 'all':
        with open(DATASETS_MAPPING_PATH, 'r') as f:
            mappings = json.load(f)
        for p, _ in mappings.items():
            download_single_provider_files(p, os.path.join(output, p), year, parallel)
    else:
        download_single_provider_files(provider, os.path.join(output, provider), year, parallel)
    logger.info(f'{STAGE_NAME} | Completed')


def download_single_provider_files(provider: str, output: str, year: str, parallel: int):
    if year == 'all':
        provider_info = get_provider_info(provider)
        for y, _ in provider_info['trace_files'].items():
            download_single_provider_single_year_files(provider, output, y, parallel)
    else:
        download_single_provider_single_year_files(provider, output, year, parallel)


def download_single_provider_single_year_files(provider: str, output: str, year: str, parallel: int):
    logger.info(f'{STAGE_NAME} | Downloading all the file for provider {provider} year {year} ')
    provider_info = get_provider_info(provider)
    traces_files = provider_info['trace_files'][year]
    create_directory(output)
    pool = ThreadPool(parallel)
    for i, file in enumerate(traces_files):
        pool.apply_async(
            _handle_download,
            kwds={
                'file': file,
                'output': output,
                'index': i,
                'total': len(traces_files)
            },
            error_callback=lambda e: logger.exception(e)
        )
    pool.close()
    pool.join()
    logger.info(f'{STAGE_NAME} | Downloaded all the file for provider {provider} year {year} ')


def _handle_download(file, output, index, total):
    logger.debug(f'{STAGE_NAME} | downloading file {index + 1}/{total}')
    wget.download(file, output)
    file_path = os.path.join(output, file.split('/')[-1])
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(output)
    os.remove(file_path)
