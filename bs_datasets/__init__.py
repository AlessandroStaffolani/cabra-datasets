import os

from dotenv import load_dotenv

from bs_datasets.logger import logger
from bs_datasets.filesystem import ROOT_DIR
from bs_datasets.mongo import MongoWrapper

load_dotenv(dotenv_path=os.path.join(ROOT_DIR, '.env'))

mongo_wrapper = MongoWrapper(
    host=os.getenv('MONGO_HOST'),
    port=os.getenv('MONGO_PORT'),
    user=os.getenv('MONGO_USER'),
    db=os.getenv('MONGO_DB'),
    password=os.getenv('MONGO_PASSWORD')
)


def mongo_init():
    mongo_wrapper.init()
    mongo_wrapper.db_prefix_name = 'bs_dataset'
    logger.info('MongoDB connection established with success')
