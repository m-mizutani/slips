import os
import logging
import sys
import boto3
import json
import traceback
import time

import utils


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def main(args, event):
    logger.info('args > %s', args)
    func_name = args['FUNC_NAME']
    client = boto3.client("lambda")
    delay_seconds = int(args.get('DELAY') or '0')
    
    event_list = list(utils.extract_kinesis_event(event))

    logger.info('Event > %s', json.dumps(event_list, indent=4))
    res = client.invoke(FunctionName=func_name, InvocationType='Event',
                        Payload=json.dumps(event_list))

    if res['ResponseMetadata']['HTTPStatusCode'] != 202:
        logger.error('Lambda invoke error: %s', res)
        raise Exception('Lambda invoke error')

    if delay_seconds > 0:
        time.sleep(delay_seconds)
    
    return 'ok'


def lambda_handler(event, context):
    logger.info('Event: %s', json.dumps(event, indent=4))
    arg_keys = [
        'TASK_TABLE',
        'FUNC_NAME',
        'REGION',
        'DELAY',
    ]
    args = dict([(k, os.environ.get(k)) for k in arg_keys])

    try:
        return main(args, event)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error(e)
        raise e
    

if __name__ == '__main__':
    lambda_handler(json.load(open(sys.argv[1])), None)
