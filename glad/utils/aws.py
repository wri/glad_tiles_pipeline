import datetime
import boto3
import json


S3 = boto3.resource("s3")
LASTRUN = S3.Object("gfw2-data", "forest_change/umd_landsat_alerts/prod/events/lastrun")
STATUS = S3.Object("gfw2-data", "forest_change/umd_landsat_alerts/prod/events/status")


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


def get_slack_webhook(channel):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId="slack/gfw-sync")
    return json.loads(response["SecretString"])[channel]
