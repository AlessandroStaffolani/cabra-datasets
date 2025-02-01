from bs_datasets import mongo_init, logger
from bs_datasets.pipelines import ACTION_MAPPING
from bs_datasets.pipelines.cdrc_pipelines import CDRC_ACTION_MAPPING
from bs_datasets.utils import parse_args


if __name__ == '__main__':
    args = parse_args()
    use_cdrc = args.cdrc
    action = args.action
    mongo_init()
    logger.info(f'Bike-sharing Datasets Utility{" - cdrc dataset" if use_cdrc else ""}')
    kwargs = {}
    for key, val in vars(args).items():
        if key != 'action' and key != 'cdrc':
            kwargs[key] = val
    mapping = ACTION_MAPPING if not use_cdrc else CDRC_ACTION_MAPPING
    if action in mapping:
        action_fn = mapping[action]
        action_fn(**kwargs)
    else:
        raise AttributeError('Action not available')
