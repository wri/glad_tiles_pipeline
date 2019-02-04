from google.cloud import storage
from google.cloud.storage.bucket import Bucket
from google.cloud.storage.client import Client
from google.auth.exceptions import TransportError
import time


def get_gs_bucket():
    tries: int = 0
    while True:
        try:
            client: Client = storage.Client()
            bucket: Bucket = client.bucket("earthenginepartners-hansen")
        except TransportError as e:
            time.sleep(10)
            tries += 1
            if tries == 10:
                raise e
        else:
            return bucket
