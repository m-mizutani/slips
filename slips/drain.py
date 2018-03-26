import os
import logging

import json
import traceback
import boto3
import uuid
import collections
from functools import reduce

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def push_stream(dst_stream, items, table_name):
    dynamodb = boto3.client('dynamodb')
    kinesis = boto3.client('kinesis')

    PUT_RECORDS_MAX = 500
    for i in range(0, len(items), PUT_RECORDS_MAX):
        target = items[i:(i+PUT_RECORDS_MAX)]
    
        records = [{
            'Data': json.dumps(rec).encode('utf8'),
            'PartitionKey': str(uuid.uuid4()),
        } for req_id, rec in target]
        
        res = kinesis.put_records(Records=records, StreamName=dst_stream)
        if ('FailedRecordCount' not in res or res['FailedRecordCount'] > 0):
            logger.error('kinesis.put_records: %s', res)
            raise Exception('Fail to push kinesis stream')
        
        logger.info(res)

        for req_id, rec in items:
            key = {'request_id': {'S': req_id}}
            res = dynamodb.delete_item(TableName=table_name, Key=key)
            if res['ResponseMetadata']['HTTPStatusCode'] != 200:
                logger.error('dynamodb.delete_item > %s', res)
                raise Exception('Fail to delete items')

    return (dst_stream, len(items))


def main(args):
    table_name = args['ERROR_TABLE']
    
    dynamodb = boto3.client('dynamodb')
    
    db_res = dynamodb.scan(TableName=table_name)
    items_set = [(x['request_id']['S'], json.loads(x['argument']['S']))
                 for x in db_res.get('Items', [])]
    items = [(req_id, arg) for req_id, arg_set in items_set for arg in arg_set]
     
    queues = collections.defaultdict(list)
    logger.info(items)
    for req_id, args in items:
        queues[args['dest_stream']].append((req_id, args))

    results = dict([push_stream(k, v, table_name) for k, v in queues.items()])
    logger.info('results > %s', results)
    return results


def lambda_handler(event, context):
    logger.info('Event: %s', json.dumps(event, indent=4))
    arg_keys = [
        'ERROR_TABLE', 'DST_KINESIS_STREAM',
    ]
    args = dict([(k, os.environ.get(k)) for k in arg_keys])
                            
    try:
        return main(args)
    except Exception as e:
        logger.info('Event: %s, %s', json.dumps(event, indent=4), context)
        logger.error('%s > %s', e, traceback.format_exc())
        raise e
    

if __name__ == '__main__':
    lambda_handler(None, None)
