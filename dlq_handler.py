import base64
import datetime
import json
import logging
import os
from pathlib import Path
from typing import List, Dict

import boto3
from aws_xray_sdk.core import patch_all, xray_recorder
from aws_xray_sdk.core.models.subsegment import Subsegment
from boto3_type_annotations.lambda_ import Client

import sqs_event
from lambda_context import LambdaContext

patch_all()
logger = logging.getLogger()


@xray_recorder.capture('Send Email')
def send_email(records: List[Dict]) -> str:
    lamb: Client = boto3.client('lambda')
    sqs_arn: str = records[0].get('eventSourceARN')
    source_queue = sqs_arn.split(':')[5]
    source_environment = source_queue.rsplit('-', 1)[1]
    partial_fail = False
    try:
        for record in records:
            body: Dict[str, str] = json.loads(record.get('body'))
            if body.get('testStationEmail') == 'teststationname@dvsa.gov.uk':
                # Automation, should break
                break
            current_time = f"{datetime.datetime.utcnow().isoformat(timespec='microseconds')}Z"

            payload = {
                'message_type': 'email',
                'to': os.getenv('TO_EMAIL'),
                'template_id': os.getenv('TEMPLATE_ID'),
                'template_vars': {
                    'source_queue': source_queue,
                    'source_environment': source_environment
                },
                'attachment': base64.b64encode(json.dumps(record, indent=2).encode('utf-8', errors='strict')).decode(
                    "utf-8",
                    "strict"),
                'attachment_name': f'{source_queue}_dead_messages_{current_time}.json'
            }

            logger.info(json.dumps(payload))
            lambda_name = os.getenv('NOTIFY_LAMBDA_NAME')

            resp = lamb.invoke(FunctionName=lambda_name, Payload=json.dumps(payload))
            if resp.get('FunctionError') is not None:
                raise RuntimeError(f'{lambda_name} failed to notify with {payload}')
            else:
                response = resp['Payload'].read()
                logger.info(response)
    except RuntimeError as e:
        logger.exception("Failed to send email", exc_info=e)
        partial_fail = True
    return "Success!" if not partial_fail else "Partial Fail!"


def handler(event: sqs_event, context: LambdaContext) -> str:
    xray: Subsegment = xray_recorder.begin_subsegment('Invocation Data')
    xray.put_metadata('event', event)
    xray.put_metadata('context', context)
    xray_recorder.end_subsegment()
    records: List[Dict] = event.get('Records')
    return json.dumps({"Result": send_email(records)})


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--event', help='Path to event json file', type=Path)
    args = parser.parse_args()
    with open(args.event) as event_file:
        evt = json.load(event_file)
    handler(evt, LambdaContext())
