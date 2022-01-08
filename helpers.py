import logging
import os
import sys

from loguru import logger


def get_heroku_config(heroku_app_name: str, aws_secret_name: str) -> dict:
    import json

    import heroku3

    config = json.loads(get_aws_secret(secret_name=aws_secret_name))
    heroku_api_key = config["API_KEY"]
    heroku_conn = heroku3.from_key(heroku_api_key)

    app = heroku_conn.apps()[heroku_app_name]

    return app.config()


def get_aws_secret(secret_name: str, **kwargs) -> dict:
    import base64

    import boto3
    from botocore.exceptions import ClientError

    region_name = kwargs.get("region_name") or "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "DecryptionFailureException":
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InternalServiceErrorException":
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InvalidParameterException":
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InvalidRequestException":
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "ResourceNotFoundException":
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
            return secret
        else:
            decoded_binary_secret = base64.b64decode(
                get_secret_value_response["SecretBinary"]
            )
            return decoded_binary_secret


def set_aws_secret(secret_name: str, json_data: dict, **kwargs) -> None:
    import json

    import boto3
    from botocore.exceptions import ClientError

    region_name = kwargs.get("region_name") or "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    client.put_secret_value(SecretId=secret_name, SecretString=json.dumps(json_data))


def setup_logging():
    logger.remove()

    stage_name = os.getenv("STAGE", "dev")
    log_level = os.getenv("LOG_LEVEL", None)

    if log_level:
        if log_level == "INFO":
            log_level_value = logging.INFO
        elif log_level == "DEBUG":
            log_level_value = logging.DEBUG
    else:
        log_level_value = logging.INFO if stage_name == "prod" else logging.DEBUG

    logger.add(sys.stderr, level=log_level_value)