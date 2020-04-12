import json

import requests
from google.auth.exceptions import TransportError
from google.cloud import storage
from math import floor
from retrying import retry
import os
import datetime
import boto3
import botocore

S3 = boto3.resource("s3")
LASTRUN = S3.Object("gfw2-data", "forest_change/umd_landsat_alerts/prod/events/lastrun")
STATUS = S3.Object("gfw2-data", "forest_change/umd_landsat_alerts/prod/events/status")


def lambda_handler(event, context):
    kwargs = {
        "lastrun": get_lastrun(),
        "status": get_status(),
        "tile_ids": get_tile_ids_by_bbox(-120, -40, 180, 30),
        "years": get_current_years(),
        "num_tiles": 115,
    }

    if "report" in event.keys() and event["report"]:
        return report(**kwargs)
    else:
        return trigger_pipeline(**kwargs)


def report(**kwargs):
    try:
        lastrun = kwargs["lastrun"]
        status = kwargs["status"]

        available_tiles = get_export_history(**kwargs)
        r = requests.get(
            "https://production-api.globalforestwatch.org/v1/glad-alerts/latest"
        )
        latest_alert = r.json()["data"][0]["attributes"]["date"]

        msg = f"""
Lastest GEE export processed: {lastrun.strftime("%Y-%m-%d")}\n
Status of last run: {status}\n
Latest detected alert: {latest_alert}\n
\n
Export status of GEE tiles:\n
{available_tiles}\n
\n
Next update will be triggered as soon as all 115 tiles for a newer date are available.
        """

        slack_webhook(level="INFO", message=msg)
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": {"msg": msg},
        }
    except Exception as e:

        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": {"message": str(e)},
        }


def get_export_history(**kwargs):
    tile_ids = kwargs["tile_ids"]
    years = kwargs["years"]
    num_tiles = kwargs["num_tiles"]
    today = datetime.datetime.today()

    available_tiles = ""
    # check for most recent day of GLAD data
    for day_offset in range(0, 11):
        process_date = (today - datetime.timedelta(days=day_offset)).strftime(
            "%Y/%m_%d"
        )
        tiles = _check_tifs_exist(process_date, tile_ids, years)
        available_tiles += f"GEE process date: {datetime.datetime.strptime(process_date, '%Y/%m_%d').strftime('%Y-%m-%d')} - {len(tiles)} tiles for {years} exported\n"
        if len(tiles) >= num_tiles:
            return available_tiles

    available_tiles += "Checked GCS for last 10 days - none had all {} tiled TIFs."
    return available_tiles


def trigger_pipeline(**kwargs):
    lastrun = kwargs["lastrun"]
    status = kwargs["status"]
    try:
        try:
            tile_date = get_most_recent_day(**kwargs)
        except ValueError:
            return {
                "statusCode": 500,
                "headers": {"Content-Type": "application/json"},
                "body": {"message": "Cannot find recently processes tiles."},
            }

        tile_date_str = tile_date.strftime("%Y-%m-%d")

        subnet_ids = [
            "subnet-00335589f5f424283",  # GFW subnet zone us-east-1a
            "subnet-8c2b5ea1",  # GFW subnet zone us-east-1b
            "subnet-08458452c1d05713b",  # GFW subnet zone us-east-1c
            "subnet-116d9a4a",  # GFW subnet zone us-east-1d
            # "subnet-037b97cff4493e3a1", # GFW subnet zone us-east-1e
            "subnet-0360516ee122586ff",  # GFW subnet zone us-east-1f
        ]
        if tile_date > lastrun and (
            status == "FAILED" or status == "HADOOP FAILED" or status == "COMPLETED"
        ):

            message = None
            for subnet_id in subnet_ids:
                try:
                    response = serialize_dates(start_pipline(subnet_id))
                except botocore.exceptions.ClientError as e:
                    message = str(e)
                else:
                    return {
                        "statusCode": 200,
                        "headers": {"Content-Type": "application/json"},
                        "body": {
                            "Lastest data": tile_date_str,
                            "Last Update": lastrun.strftime("%Y-%m-%d"),
                            "Status": status,
                            "Action": "Glad Pipeline triggered",
                            "Details": response,
                        },
                    }

            return {
                "statusCode": 500,
                "headers": {"Content-Type": "application/json"},
                "body": {"message": message},
            }

        else:
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": {
                    "Latest data": tile_date_str,
                    "Last Update": lastrun.strftime("%Y-%m-%d"),
                    "Status": status,
                    "Action": "No action taken",
                },
            }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": {"message": "An error occurred", "traceback": e},
        }


def retry_if_transport_error(exception):
    """Return True if we should retry
    Retry if Google throws a transport error"""
    return isinstance(exception, (TransportError,))


def get_lastrun():
    body = LASTRUN.get()["Body"].read().strip().decode("utf-8")
    return datetime.datetime.strptime(body, "%Y-%m-%d")


def get_status():
    body = STATUS.get()["Body"].read().strip().decode("utf-8")
    return body


def update_lastrun(d):
    LASTRUN.put(Body=d.strftime("%Y-%m-%d").encode("utf-8"))
    return


def update_status(status):
    STATUS.put(Body=status)
    return


def get_google_json():
    client = boto3.client("secretsmanager", region_name="us-east-1")

    response = client.get_secret_value(
        SecretId="google_cloud/earthenginepartners-hansen"
    )

    json_file = "/tmp/credentials.json"
    with open(json_file, "w+") as f:
        f.write(response["SecretString"])

    return json_file


@retry(retry_on_exception=retry_if_transport_error, wait_fixed=2000)
def get_gs_bucket():
    client = storage.Client.from_service_account_json(get_google_json())

    return client.bucket("earthenginepartners-hansen")


def get_current_years():
    now = datetime.datetime.now()
    year = now.year
    month = now.month

    if month < 7:
        return [year - 1, year]
    else:
        return [year]


def lower_bound(y):
    return floor(y / 10) * 10


def upper_bound(y):
    if y == lower_bound(y):
        return y
    else:
        return (floor(y / 10) * 10) + 10


def get_longitude(x):
    if x >= 0:
        return str(x).zfill(3) + "E"
    else:
        return str(-x).zfill(3) + "W"


def get_latitude(y):
    if y >= 0:
        return str(y).zfill(2) + "N"
    else:
        return str(-y).zfill(2) + "S"


def get_tile_ids_by_bbox(left, bottom, right, top):
    tile_ids = list()
    left = lower_bound(left)
    bottom = lower_bound(bottom)
    right = upper_bound(right)
    top = upper_bound(top)

    for y in range(bottom, top, 10):

        for x in range(left, right, 10):
            west = get_longitude(x)
            south = get_latitude(y)
            east = get_longitude(x + 10)
            north = get_latitude(y + 10)

            tile_ids.append("{}_{}_{}_{}".format(west, south, east, north))

    return tile_ids


def get_most_recent_day(**kwargs):
    tile_ids = kwargs["tile_ids"]
    years = kwargs["years"]
    num_tiles = kwargs["num_tiles"]
    today = datetime.datetime.today()

    # check for most recent day of GLAD data
    for day_offset in range(0, 11):
        process_date = (today - datetime.timedelta(days=day_offset)).strftime(
            "%Y/%m_%d"
        )

        available_tiles = _check_tifs_exist(process_date, tile_ids, years)
        if len(available_tiles) >= num_tiles:
            return datetime.datetime.strptime(process_date, "%Y/%m_%d")

    msg: str = ("Checked GCS for last 10 days - none had all {} tiled TIFs.")

    raise ValueError(msg)


def _check_tifs_exist(process_date, tile_ids, years):
    bucket = get_gs_bucket()

    name_list = [
        blob.name
        for blob in bucket.list_blobs(prefix="GLADalert/{}".format(process_date))
    ]

    available_tiles = list()

    for tile_id in tile_ids:
        c = 0
        for year in years:

            year_dig = str(year)[2:]

            conf_str = "GLADalert/{0}/alert{1}_{2}.tif".format(
                process_date, year_dig, tile_id
            )

            alert_str = "GLADalert/{0}/alertDate{1}_{2}.tif".format(
                process_date, year_dig, tile_id
            )

            filtered_names = [x for x in name_list if conf_str in x or alert_str in x]

            # if both alert and conf rasters exist, tile is ready to download
            if len(filtered_names) == 2:
                c += 1

        # TODO: Need to see if it's better to check for == or >=
        #  currently checking for >= b/c it adds some more flexibility in case new tiles were added
        if c >= len(years):
            available_tiles.append(tile_id)

    return available_tiles


def start_pipline(subnet):
    dirname = os.path.dirname(__file__)
    with open(os.path.join(dirname, "bootstrap.sh"), "r") as f:
        bootstrap = f.read()

    client = boto3.client("ec2")
    response = client.run_instances(
        ImageId="ami-02da3a138888ced85",
        InstanceType="r5d.24xlarge",
        KeyName="tmaschler_wri2",
        UserData=bootstrap,
        EbsOptimized=True,
        IamInstanceProfile={"Name": "gfw_docker_host"},
        InstanceInitiatedShutdownBehavior="terminate",
        MaxCount=1,
        MinCount=1,
        NetworkInterfaces=[
            {
                "AssociatePublicIpAddress": True,
                "DeviceIndex": 0,
                "Groups": ["sg-d7a0d8ad", "sg-6c6a5911"],
                "SubnetId": subnet,
            }
        ],
        TagSpecifications=[
            {
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Project", "Value": "Global Forest Watch"},
                    {"Key": "Project Lead", "Value": "Thomas Maschler"},
                    {"Key": "Pricing", "Value": "Spot"},
                    {"Key": "Job", "Value": "Glad update"},
                    {"Key": "Name", "Value": "Glad update"},
                ],
            }
        ],
        InstanceMarketOptions={
            "MarketType": "spot",
            "SpotOptions": {
                "SpotInstanceType": "one-time",
                "BlockDurationMinutes": 120,
                # "ValidUntil": datetime.datetime.now() + datetime.timedelta(hours=12),
                "InstanceInterruptionBehavior": "terminate",
            },
        },
    )

    return response


def serialize_dates(d):
    if isinstance(d, dict):
        new_d = dict()
        for k, v in d.items():
            if isinstance(v, dict):
                new_d[k] = serialize_dates(v)
            elif isinstance(v, list):
                new_d[k] = list()
                for i in v:
                    new_d[k].append(serialize_dates(i))
            else:
                if isinstance(v, datetime.datetime):
                    new_d[k] = v.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    new_d[k] = v

        return new_d
    elif isinstance(d, datetime.datetime):
        return d.strftime("%Y-%m-%d %H:%M:%S")
    else:
        return d


def get_slack_webhook(channel):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId="slack/gfw-sync")
    return json.loads(response["SecretString"])[channel]


def slack_webhook(level, message):
    app = "GFW SYNC - DAILY GLAD UPDATE REPORT"

    if level.upper() == "WARNING":
        color = "#E2AC37"
    elif level.upper() == "ERROR" or level.upper() == "CRITICAL":
        color = "#FF0000"

    else:
        color = "#36A64F"

    attachement = {
        "attachments": [
            {
                "fallback": "{} - {} - {}".format(app, level.upper(), message),
                "color": color,
                "title": app,
                "fields": [{"title": level.upper(), "value": message, "short": False}],
            }
        ]
    }

    url = get_slack_webhook("data-updates")
    return requests.post(url, json=attachement)


if __name__ == "__main__":
    print(lambda_handler({"report": True}, None))
