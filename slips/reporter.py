import os
import logging
import sys
import json
import traceback
import boto3

import utils

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def main(args, event):
    logger.info('Event: %s', json.dumps(event, indent=4))

    table_name = args['ERROR_TABLE']
    dynamodb = boto3.client('dynamodb')

    event_list = list(utils.extract_kinesis_event(event))

    logger.info('Event > %s', json.dumps(event_list, indent=4))

    for msg, attrs in utils.extract_dlq_event(event):
        item = {
            'request_id': {'S': attrs.get('RequestID', {}).get('Value')},
            'argument': {'S': json.dumps(msg, separators=(',', ':'))},
        }
        put_res = dynamodb.put_item(TableName=table_name, Item=item)
        logging.info('Put result > %s', put_res)

    return 'ok'


def lambda_handler(event, context):
    logger.info('Event: %s', json.dumps(event, indent=4))
    arg_keys = ['ERROR_TABLE']
    args = dict([(k, os.environ.get(k)) for k in arg_keys])
                            
    try:
        return main(args, event)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error(e)
        raise e
    

if __name__ == '__main__':
    lambda_handler(json.load(open(sys.argv[1])), None)
