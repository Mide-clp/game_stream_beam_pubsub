from google.cloud import pubsub_v1
import time
import os
from dotenv import load_dotenv

if __name__ == "__main__":

    # Replace 'my-service-account-path' with your service account path
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("SERVICE_ACCOUNT")

    # Replace 'my-subscription' with your subscription id
    subscription_path = os.getenv("SUB_PATH")

    subscriber = pubsub_v1.SubscriberClient()


    def callback(message):
        print(('Received message: {}'.format(message)))
        message.ack()


    subscriber.subscribe(subscription_path, callback=callback)

    while True:
        time.sleep(60)