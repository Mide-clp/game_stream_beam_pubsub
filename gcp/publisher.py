import os
import time
from google.cloud import pubsub_v1
from dotenv import load_dotenv

if __name__ == "__main__":

    #  project id
    project = 'my-project'

    #  your pubsub topic
    pubsub_topic = os.getenv("PUB_SUB_TOPIC")

    # Replace 'my-service-account-path' with your service account path
    path_service_account = os.getenv("SERVICE_ACCOUNT")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account

    # Replace 'my-input-file-path' with your input file path
    input_file = os.getenv("FILE_DIR")

    # create publisher
    publisher = pubsub_v1.PublisherClient()

    with open(input_file, 'rb') as ifp:
        # skip header
        header = ifp.readline()

        # loop over each record
        for line in ifp:
            event_data = line  # entire line of input CSV is the message
            print('Publishing {0} to {1}'.format(event_data, pubsub_topic))
            publisher.publish(pubsub_topic, event_data)
            time.sleep(5)
