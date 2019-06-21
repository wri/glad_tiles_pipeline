from glad.utils.aws import get_slack_webhook
import requests


def slack_webhook(level, message, **kwargs):
    env = kwargs["env"]

    if env != "test":
        app = "GFW SYNC - GLAD Tile Pipeline"

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
                    "fields": [
                        {"title": level.upper(), "value": message, "short": False}
                    ],
                }
            ]
        }

        url = get_slack_webhook("data-updates")
        return requests.post(url, json=attachement)
