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
from boto3_type_annotations.sqs import Client as SClient

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


@xray_recorder.capture('Requeue Messages')
def requeue(records) -> None:
    subsegment: Subsegment = xray_recorder.current_subsegment()
    sqs: SClient = boto3.client('sqs')
    sqs_arn = records[0].get('eventSourceARN').split(':')
    queue_url = f"https://sqs.{sqs_arn[3]}.amazonaws.com/{sqs_arn[4]}/{sqs_arn[5]}"

    request_list: List[Dict] = []
    for message in records:
        request_list.append({
            'Id': message['messageId'],
            'MessageBody': message['body'],
            'MessageAttributes': message['messageAttributes'],
        })

    subsegment.put_metadata("Message List", request_list)
    response = sqs.send_message_batch(QueueUrl=queue_url, Entries=request_list)
    message_ids: List[str] = [x['MessageId'] for x in response['Successful']]

    i = 0
    while len(message_ids) != 0:
        loop_subsegment = xray_recorder.begin_subsegment(f'Increase Timeout Attempt {i}')
        response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=5)
        loop_subsegment.put_metadata("Receive Message Response", response)

        received_messages: List[Dict] = []
        for message in response['Messages']:
            placed_message = True if message['MessageId'] in message_ids else False
            received_messages.append({
                "Id": message['MessageId'],
                "ReceiptHandle": message['ReceiptHandle'],
                "VisibilityTimeout": 43200 if placed_message else 0
            })

            if placed_message:
                message_ids.remove(message['MessageId'])

        if len(received_messages) == 0:
            logger.info('Did not receive any messages')
            xray_recorder.end_subsegment()
            break

        response = sqs.change_message_visibility_batch(QueueUrl=queue_url, Entries=received_messages)
        loop_subsegment.put_metadata("Change Visibility Response", response)
        i = i + 1
        xray_recorder.end_subsegment()


def handler(event: sqs_event, context: LambdaContext) -> str:
    records: List[Dict] = event.get('Records')
    requeue(records)
    return json.dumps({"Result": send_email(records)})


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--event', help='Path to event json file', type=Path)
    args = parser.parse_args()
    with open(args.event) as event_file:
        evt = json.load(event_file)
    handler(evt, LambdaContext())
