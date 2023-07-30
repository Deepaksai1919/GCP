from google.cloud import pubsub_v1
from auth import Credentials

credentials = Credentials().get_credentails()
project_id = 'pubsub-394306'
subscription_name = 'notification_sub'


def callback(message):
    print('Received Message : ', message.data)
    message.ack()

with pubsub_v1.SubscriberClient(credentials = credentials) as subscriber:
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    future = subscriber.subscribe(subscription_path, callback)
    try:
        future.result()
    except KeyboardInterrupt:
        future.cancel()
        exit()

