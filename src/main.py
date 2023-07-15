import os


def handler(event, context):
    message = event["message"]
    return {"message": message, "configVar": os.environ["EXAMPLE_ENVIRONMENT_VARIABLE"]}
