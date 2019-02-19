from google.cloud import storage
from google.auth.exceptions import TransportError
import time
import argparse


def download_file(remote_file, output):

    tries = 0
    success = False
    while tries < 10 or not success:
        try:
            client = storage.Client()
            bucket = client.bucket("earthenginepartners-hansen")
        except TransportError as e:
            time.sleep(10)
            tries += 1
            if tries == 10:
                raise e
        else:
            success = True

    blob = bucket.blob(remote_file)
    blob.download_to_filename(output)


def main():
    parser = argparse.ArgumentParser(
        description="Download GLAD tiles from Google Storage."
    )
    parser.add_argument("--remote_file", "-r", help="remote file", required=True)
    parser.add_argument(
        "--output_raster", "-o", help="the output raster", required=True
    )
    args = parser.parse_args()

    download_file(args.remote_file, args.output_raster)


if __name__ == "__main__":
    main()
