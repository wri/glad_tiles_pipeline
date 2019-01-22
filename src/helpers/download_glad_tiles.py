from google.cloud import storage
from google.auth.exceptions import TransportError
import time
import argparse


def download_file(remote_file, output):
    try:
        client = storage.Client()
        bucket = client.bucket("earthenginepartners-hansen")
    except TransportError:
        time.sleep(10)
        client = storage.Client()
        bucket = client.bucket("earthenginepartners-hansen")
    finally:

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
