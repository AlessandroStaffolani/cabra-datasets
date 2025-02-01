from argparse import ArgumentParser

from bs_datasets import logger


def print_status(current, total, pre_message='', loading_len=20, unit=''):
    perc = int((current * loading_len) / total)
    message = f'{pre_message}:\t{"#" * perc}{"." * (loading_len - perc)}\t{current}{unit}/{total}{unit}'
    print(message, end='\r')


def log_info(message: str, show: bool = True):
    if show:
        logger.debug(message)


def parse_args():
    main_parser = ArgumentParser(description='Bike-sharing Datasets Utility')
    action_parser = main_parser.add_subparsers(dest='action')
    main_parser.add_argument('--cdrc', action='store_true',
                             help='If present it executes the pipelines using the CDRC data')

    # DOWNLOADER COMMAND
    sub_downloader_parser = action_parser.add_parser('downloader', help='Download the traces files')
    sub_downloader_parser.add_argument('provider',
                                       help='Name of the provider of the source. If "all" is given, it executes '
                                            'the pipelines for all the providers in data/datasets_mapping.json')
    sub_downloader_parser.add_argument('output', help='path for saving the dataset')
    sub_downloader_parser.add_argument('year', help='year of the dataset. If "all" is given, it executes '
                                                    'the pipelines for all the years available for the provider')
    sub_downloader_parser.add_argument('-p', '--parallel', type=int, default=6,
                                       help='Number of parallel workers. Default 6')

    # SPLIT COMMAND
    sub_split_parser = action_parser.add_parser('split', help='Split dataset in chunks')
    sub_split_parser.add_argument('source',
                                  help='path to the csv file to use for splitting '
                                       'or a provider name from the file "data/datasets_mapping.json"')
    sub_split_parser.add_argument('output', help='path for saving the results')
    sub_split_parser.add_argument('-r', '--n-rows', default=10000, type=int, help='Number of rows per chunk')
    sub_split_parser.add_argument('--provider-path', default='data/trip_data',
                                  help='Alternative path for the providers files to split. Default: "data/trip_data"')

    # VERIFY COMMAND
    # sub_verify_parser = action_parser.add_parser('verify', help='Verify dataset quality')
    # sub_verify_parser.add_argument('source', help='path to the folder containing the chunks to verify')
    # sub_verify_parser.add_argument('provider', help='Name of the provider of the source')
    # sub_verify_parser.add_argument('-f', '--verify-field', default='short_name',
    #                                help='Field to use for verifying the data')
    # sub_verify_parser.add_argument('-p', '--parallel', type=int, default=4, help='Number of parallel work to use')
    #
    # DOCKING COMMAND
    sub_ds_parser = action_parser.add_parser('docking', help='Start the docking station pipeline')
    sub_ds_parser.add_argument('provider',
                               help='Name of the provider of the source. If "all" is given, '
                                    'it executes the pipelines for all the providers in data/datasets_mapping.json')
    # TRIPS COMMAND
    # sub_trips_parser = action_parser.add_parser('trips', help='Start the trip data pipeline')
    # sub_trips_parser.add_argument('provider', help='Name of the provider of the source.')
    # sub_trips_parser.add_argument('source', help='path to the folder containing the chunks (result of split command)')
    # sub_trips_parser.add_argument('year', type=int, help='year of the dataset')
    # sub_trips_parser.add_argument('-p', '--parallel', type=int, default=4, help='Number of parallel work to use')
    # sub_trips_parser.add_argument('-a', '--aggregation-frequency', default='10m',
    #                               help='Aggregation frequency used for grouping together trip data. '
    #                                    'Default "10m" group every 10 minutes. '
    #                                    'Use any possible value for "freq" field of "pandas.Grouper"')
    # sub_trips_parser.add_argument('--clean-only', action='store_true', default=False,
    #                               help='If present, the pipeline performs only the final cleaning')

    # RAW TRIPS COMMAND
    sub_raw_trips_parser = action_parser.add_parser('raw', help='Start the raw trip data pipeline')
    sub_raw_trips_parser.add_argument('provider', help='Name of the provider of the source.')
    sub_raw_trips_parser.add_argument('source',
                                      help='path to the folder containing the chunks (result of split command)')
    sub_raw_trips_parser.add_argument('-y', '--year', default='all',
                                      help='year to process, use "all" to process all available years. Default: "all"')
    sub_raw_trips_parser.add_argument('-p', '--parallel', type=int, default=4, help='Number of parallel work to use')
    sub_raw_trips_parser.add_argument('-b', '--batch-size', type=int, default=50000,
                                      help='Batch size for file reading and db flushing operations')

    # # DATASET COMMAND
    # sub_dataset_parser = action_parser.add_parser('dataset', help='Create the final dataset from the trip data')
    # sub_dataset_parser.add_argument('provider', help='Name of the provider of the source.')
    # sub_dataset_parser.add_argument('output', help='path for saving the dataset')
    # sub_dataset_parser.add_argument('year', type=int,
    #                                 help='year of the dataset, used for naming when provider equal "all"')

    # SUB-DATASET COMMAND
    sub_subdataset_parser = action_parser.add_parser('subdataset',
                                                     help='Create a sub dataset with limited number of ndoes')
    sub_subdataset_parser.add_argument('pivot',
                                       help='trip_id of the station to use as a starting point '
                                            'or "none" for using the default "pivot_node" from the '
                                            '"data/datasets_mapping.json" file')
    sub_subdataset_parser.add_argument('n',
                                       help='Comma separated list of numbers to use'
                                            ' for selecting nodes closeset to the pivot node')
    sub_subdataset_parser.add_argument('output', help='folder path for saving the new dataset')
    sub_subdataset_parser.add_argument('provider', help='Name of the provider of the source.')
    sub_subdataset_parser.add_argument('--aggregation-unit', default='minute',
                                       help='Aggregation unit used for grouping together trip data. '
                                            'Default "minute" group based on minutes. '
                                            'Use any possible value for "$dateTrunc.unit" field of mongoDb')
    sub_subdataset_parser.add_argument('--aggregation-size', type=int, default=10,
                                       help='Aggregation size used for grouping together trip data. '
                                            'Default "4" group every 4 aggregation units. '
                                            'Use any possible value for "$dateTrunc.binSize" field of mongoDb')
    sub_subdataset_parser.add_argument('--min-date',
                                       help='Optional date for filtering the dataset. '
                                            'Min date is inclusive and should be in ISO format. '
                                            'Example: "2020-01-01"')
    sub_subdataset_parser.add_argument('--max-date',
                                       help='Optional date for filtering the dataset. '
                                            'Max date is not inclusive and should be in ISO format. '
                                            'Example: "2021-01-01"')
    sub_subdataset_parser.add_argument('--name-suffix', help='Value to append to the dataset folder name')
    sub_subdataset_parser.add_argument('--min-trip-duration', type=int, default=60,
                                       help='Value in seconds used to trim out trips with duration'
                                            ' lower than this value. Default: 60 seconds')
    sub_subdataset_parser.add_argument('--add-weather-data', action='store_true',
                                       help='Include weather data in the dataset')
    sub_subdataset_parser.add_argument('--weather-db', help='Weather data db name')
    sub_subdataset_parser.add_argument('--weather-collection',
                                       help='Weather data collection name. Default: observations',
                                       default='observations')
    sub_subdataset_parser.add_argument('--nodes-from-zones', default=True, action='store_true',
                                       help='Filter nodes from zones. If provided, use the pivot argument to provide'
                                            ' a comma separated list of zones to read from the zones-path argument or '
                                            'leave it to "none" to get all nodes from all the zones')
    sub_subdataset_parser.add_argument('--zones-path',
                                       help='Zones file path',
                                       default='data/zones/ny/zones.json')

    # ALL COMMAND
    sub_all_parser = action_parser.add_parser('all',
                                              help='Execute all the stage '
                                                   '(downloader, split, docking, raw),'
                                                   ' while it executs (docking, raw, zones, subdataset) for cdrc case')
    sub_all_parser.add_argument('year',
                                help='year of the dataset, used for naming when provider equal "all"')
    sub_all_parser.add_argument('provider',
                                help='Name of the provider of the source. If "all" is given, '
                                     'it executes the pipelines for all the providers in data/datasets_mapping.json')
    sub_all_parser.add_argument('--download-path', default='data/trip_data',
                                help='Path used for saving the downloaded traces. Default "data/trip_data"')
    sub_all_parser.add_argument('-r', '--n-rows', default=50000, type=int,
                                help='Number of rows per chunk in the split stage. Default 50000')
    sub_all_parser.add_argument('--split-path', default='data/post_processing',
                                help='Path used for saving the traces after the split stage. '
                                     'Default "data/post_processing"')
    sub_all_parser.add_argument('-p', '--parallel', type=int, default=6,
                                help='Number of parallel work to use for the trips stage. Default 6')
    sub_all_parser.add_argument('--dataset-path', default='data/datasets',
                                help='Path used for saving the final datasets. Default "data/datasets"')
    # sub_all_parser.add_argument('-a', '--aggregation-frequency', default='4h',
    #                             help='Aggregation frequency used for grouping together trip data in the trips stage. '
    #                                  'Default "4h" group every 4 hours. '
    #                                  'Use any possible value for "freq" field of "pandas.Grouper"')
    sub_all_parser.add_argument('--skip', help='Comma seperated list of stages to skip')
    sub_all_parser.add_argument('--aggregation-unit', default='minute',
                                help='Aggregation unit used for grouping together trip data. '
                                     'Default "minute" group based on minutes. '
                                     'Use any possible value for "$dateTrunc.unit" field of mongoDb')
    sub_all_parser.add_argument('--aggregation-size', type=int, default=10,
                                help='Aggregation size used for grouping together trip data. '
                                     'Default "4" group every 4 aggregation units. '
                                     'Use any possible value for "$dateTrunc.binSize" field of mongoDb')
    sub_all_parser.add_argument('--min-date',
                                help='Optional date for filtering the dataset. '
                                     'Min date is inclusive and should be in ISO format. '
                                     'Example: "2020-01-01"')
    sub_all_parser.add_argument('--max-date',
                                help='Optional date for filtering the dataset. '
                                     'Max date is not inclusive and should be in ISO format. '
                                     'Example: "2021-01-01"')
    sub_all_parser.add_argument('--name-suffix', help='Value to append to the dataset folder name')
    sub_all_parser.add_argument('--min-trip-duration', type=int, default=60,
                                help='Value in seconds used to trim out trips with duration'
                                     ' lower than this value. Default: 60 seconds')
    sub_all_parser.add_argument('--add-weather-data', action='store_true',
                                help='Include weather data in the dataset')
    sub_all_parser.add_argument('--weather-db', help='Weather data db name')
    sub_all_parser.add_argument('--weather-collection',
                                help='Weather data collection name. Default: observations',
                                default='observations')
    sub_all_parser.add_argument('--nodes-from-zones', default=True, action='store_true',
                                help='Filter nodes from zones. If provided, use the pivot argument to provide'
                                     ' a comma separated list of zones to read from the zones-path argument or '
                                     'leave it to "none" to get all nodes from all the zones')
    sub_all_parser.add_argument('--zones-path',
                                help='Zones file path',
                                default='data/zones/ny/zones.json')

    # WEATHER DATA COMMAND
    weather_parser = action_parser.add_parser('weather', help='Start the weather pipeline')
    weather_parser.add_argument('city', help='Name of the city from the "data/weather_api.json" file')
    weather_parser.add_argument('start',
                                help='Start date. '
                                     'It should be in ISO format. '
                                     'Example: "2022-01-01"')
    weather_parser.add_argument('end',
                                help='End date. '
                                     'It should be in ISO format. '
                                     'Example: "2022-02-01"')
    weather_parser.add_argument('--collection-name', default='observations',
                                help='Name of the collection in which the observation data will be saved')

    # ZONES
    zones_parser = action_parser.add_parser('zones', help='Create zones pipeline')
    zones_parser.add_argument('n_zones', type=int, help='Number of zones to create')
    zones_parser.add_argument('zone_size', type=int, help='Size of each zone')
    zones_parser.add_argument('output', help='Folder path where the zones will be saved')
    zones_parser.add_argument('provider', help='Name of the provider of the source. If "all" is given, '
                                               'it executes the pipelines for all the providers in data/datasets_mapping.json')
    zones_parser.add_argument('--db-name', default='bs_dataset-citibike',
                              help='Name of DB where to load docking stations')
    zones_parser.add_argument('--docking-station-collection-name', default='docking_stations',
                              help='Name of collection where to load docking stations')
    zones_parser.add_argument('--zip-codes-geojson-path', default='data/ny_map/zip_codes.geojson',
                              help='Path to the zip codes geojson')
    zones_parser.add_argument('--filter-region', default=None,
                              help='Region id to use as a filter')
    zones_parser.add_argument('--show-zones', action='store_true', default=False,
                              help='Plot the created zones at the end of the pipeline')

    return main_parser.parse_args()
