import json
import logging

import base64
import boto3
import datetime
import os
from aws_xray_sdk.core import patch_all, xray_recorder
from boto3_type_annotations.lambda_ import Client as Client
from pathlib import Path
from typing import List, Dict

patch_all()
boto3.set_stream_logger('', logging.INFO)


@xray_recorder.capture('invoke')
def handler(event, context):
    lamb: Client = boto3.client('lambda')
    records: List[Dict] = event.get('Records')
    sqs_arn: str = records[0].get('eventSourceARN')
    source_queue = sqs_arn.split(':')[5]
    source_environment = source_queue.rsplit('-', 1)[1]
    current_time = f"{datetime.datetime.utcnow().isoformat(timespec='minutes')}Z"
    payload = {
        'message_type': 'email',
        'to': os.getenv('TO_EMAIL'),
        'template_id': os.getenv('TEMPLATE_ID'),
        'template_vars': {
            'source_queue': source_queue,
            'source_environment': source_environment
        },
        'attachment': base64.b64encode(json.dumps(records, indent=2).encode('utf-8', errors='strict')).decode("utf-8",
                                                                                                              "strict"),
        'attachment_name': f'{source_queue}_dead_messages_{current_time}.json'
    }
    lambda_name = os.getenv('NOTIFY_LAMBDA_NAME')
    resp = lamb.invoke(FunctionName=lambda_name, Payload=json.dumps(payload))
    if resp.get('FunctionError') is not None:
        raise RuntimeError(f'{lambda_name} failed to notify with {payload}')
    else:
        return resp['Payload'].read()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--event', help='Path to event json file', type=Path)
    args = parser.parse_args()
    with open(args.event) as event_file:
        evt = json.load(event_file)
    handler(evt, {})
