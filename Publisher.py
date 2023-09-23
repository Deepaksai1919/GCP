from google.cloud import pubsub_v1
import time
import argparse
import os
from Log import logger
from constants import (
    SERVICE_ACCOUNT_NAME, TOPIC, PROJECT_ID
)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_NAME

parser = argparse.ArgumentParser()
parser.add_argument(
    '--input', dest='input', required=True, help='Input File for Streaming'
)
path_args = parser.parse_args()
input_file = path_args.input

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC)

try:
    logger.info(f'Trying to create Topic `{TOPIC}`')
    publisher.create_topic({'name':topic_path})
    logger.info(f'Topic `{TOPIC}` created')
except Exception as e:
    logger.warning(f'Topic `{TOPIC}` already exists')

with open(input_file, 'r') as f:
    for line in f:
        future = publisher.publish(topic_path, line.encode('utf-8'))
        logger.info(future.result())
        time.sleep(1)