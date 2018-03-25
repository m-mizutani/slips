import os
import logging
import sys
import boto3
import json
import traceback
import collections

import utils

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def routing(ev, policies, routes):
    for policy in policies:
        logger.info('%s for %s', policy, ev)
        if 'bucket' in policy and policy['bucket'] != ev['bucket_name']:
            continue
        if 'prefix' in policy and not ev['object_key'].startswith(policy['prefix']):
            continue

        if policy['dest'] not in routes:
            logger.error('No destination {} of {}'.format(policy['dest'], policy))
            logger.error('RouteMap: {}'.format(routes))
            raise Exception('No destination {}'.format(policy['dest']))

        logger.info('matched %s and %s', policy, ev)
        return routes[policy['dest']]

    raise Exception('No route for {}'.format(ev))


def main(args, event):
    # client = boto3.client('kinesis', region_name=args['REGION'])
    client = boto3.client('kinesis')
    
    routes = {
        'fast': args['DST_KINESIS_STREAM_FAST'],
        'slow': args['DST_KINESIS_STREAM_SLOW'],
        'drop': None,
    }
    policies = json.loads(args['ROUTING_POLICY'])
    logger.debug('Routing policy: %s', policies)
    
    event_queue = collections.defaultdict(list)
    results = collections.defaultdict(int)
    for ev in utils.extract_s3_event(event):
        dest = routing(ev, policies, routes)
        if not dest:
            logger.debug('Drop route, ignore')
            continue
        
        ev['dest_stream'] = dest
        event_queue[dest].append(ev)
    
    for dest_stream, queue in event_queue.items():
        records = [{
            'Data': json.dumps(rec).encode('utf8'),
            'PartitionKey': rec['object_etag'],
        } for rec in queue]
        
        logger.info('%s output ot %s',
                    json.dumps(queue, indent=4), dest_stream)
        
        if records:
            res = client.put_records(Records=records,
                                     StreamName=dest_stream)
            logger.info(res)
            results[dest_stream] += len(queue)
        else:
            logger.warn('No available record')
            logger.warn(event)
        
    return dict(results)

    
def lambda_handler(event, context):
    arg_keys = [
        'DST_KINESIS_STREAM_FAST',
        'DST_KINESIS_STREAM_SLOW',
        'ROUTING_POLICY',
        'REGION',
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
