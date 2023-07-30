from google.cloud import pubsub_v1
from auth import Credentials
import time


credentials = Credentials().get_credentails()
project_id = 'pubsub-394306'
topic_id = 'order_topic'
publisher = pubsub_v1.PublisherClient(credentials = credentials)
topic_path = publisher.topic_path(project_id, topic_id)

def publish_messages(num):
    for i in range(num):
        data_str = f'Message number {i}'
        data = data_str.encode('utf-8')
        future = publisher.publish(topic_path, data)
        print(f'Message {i}', ':', future.result())
        time.sleep(0.1)

publish_messages(100)