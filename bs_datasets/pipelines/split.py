import os
from glob import glob

from bs_datasets.data_utils.data_loader import load_csv_header, count_file_rows, load_csv_rows, get_all_providers_info
from bs_datasets.logger import logger

BASE_FOLDER = 'data/post-processing'

STAGE_NAME = 'Split stage'


def split_csv_files_pipeline(source: str, output: str, n_rows: int = 20000, provider_path: str = BASE_FOLDER):
    logger.info(f'{STAGE_NAME} | Starting split stage with source {source}')
    providers_info = get_all_providers_info()
    if source in providers_info or source == 'all':
        # split for all the files of the provider
        if source == 'all':
            for provider, _ in providers_info.items():
                base_folder = os.path.join(output, provider, 'chunks')
                files = sorted(glob(f'{os.path.join(provider_path, provider)}/*.csv'), key=lambda x: x.split('/')[-1])
                logger.info(f'{STAGE_NAME} | Splitting {len(files)} files')
                for i, file_path in enumerate(files):
                    split_csv_into_chunks(file_path, base_folder, n_rows)
                    logger.debug(f'{STAGE_NAME} | Split completed for {i + 1}/{len(files)} files')
        else:
            base_folder = os.path.join(output, source, 'chunks')
            files = sorted(glob(f'{os.path.join(provider_path, source)}/*.csv'), key=lambda x: x.split('/')[-1])
            logger.info(f'{STAGE_NAME} | Splitting {len(files)} files')
            for i, file_path in enumerate(files):
                split_csv_into_chunks(file_path, base_folder, n_rows)
                logger.debug(f'{STAGE_NAME} | Split completed for {i+1}/{len(files)} files')
    else:
        # split using source
        path_parts = source.split('/')
        filename = path_parts[-1].replace('.csv', '')
        base_folder = os.path.join(output, filename, 'chunks')
        split_csv_into_chunks(source, base_folder, n_rows)
    logger.info(f'{STAGE_NAME} | Completed')


def split_csv_into_chunks(source: str, output: str, n_rows: int):
    if os.path.exists(source):
        path_parts = source.split('/')
        filename = path_parts[-1].replace('.csv', '')
        source_year = filename.split('-')[0]
        base_folder = output
        if not os.path.exists(base_folder):
            os.makedirs(base_folder)
        columns = load_csv_header(source)
        total_rows = count_file_rows(source) - 1
        logger.debug(f'CSV rows: {total_rows}')
        if total_rows % n_rows == 0:
            chunks = total_rows // n_rows
        else:
            chunks = (total_rows // n_rows) + 1
        logger.debug(f'Chunks: {chunks}')
        loaded_rows = 1
        for i in range(chunks):
            df = load_csv_rows(source, n_rows=n_rows, skiprows=loaded_rows, names=columns)
            if len(df) > 1:
                df.to_csv(os.path.join(base_folder, f'chunk_{source_year}-{i}.csv'), index=False)
                logger.debug(f'Processed chunk {i+1}/{chunks}')
            loaded_rows += n_rows
    else:
        raise AttributeError(f'csv file path not exists at "{source}"')



