#!/usr/bin/env python

import copy
import json
import io
import yaml


def obj2yml(obj):
    ss = io.StringIO()
    
    # Disable an alias feature in PyYAML because CFn does not support YAML alias
    noalias_dumper = yaml.dumper.SafeDumper
    noalias_dumper.ignore_aliases = lambda self, data: True
    yaml.dump(obj, ss, default_flow_style=False, Dumper=noalias_dumper)
    ss.seek(0)
    return ss.read()



SAM_TEMPLATE = {
    'AWSTemplateFormatVersion': '2010-09-09',
    'Transform': 'AWS::Serverless-2016-10-31',
    'Description': 'Security Log & Alert Management: Log Collectors',
    'Resources': {
    },
}

FUNC_TEMPLATE = {
    'Type': 'AWS::Serverless::Function',
    'Properties': {
        'CodeUri': None,
        'Handler': None,
        'Runtime': 'python3.6',
        'Role': None,
        'MemorySize': 128,
        'Timeout': 300,
        'Environment': {
            'Variables': {},
        },
    },
}


def build_event_pusher(processor, routing, kinesis_stream_fast,
                       kinesis_stream_slow):
    config = copy.deepcopy(FUNC_TEMPLATE)
    
    config['Properties']['Environment']['Variables'] = {
        'DST_KINESIS_STREAM_FAST': kinesis_stream_fast,
        'DST_KINESIS_STREAM_SLOW': kinesis_stream_slow,
        'ROUTING_POLICY': json.dumps(routing, separators=(',', ':')),
    }
    config['Properties']['Role'] = processor['role_arn']['event_pusher']
    config['Properties']['Handler'] = 'event_pusher.lambda_handler'
    config['Properties']['Events'] = dict([(x['name'], {
        'Type': 'SNS', 'Properties': {'Topic': x['arn']},
    }) for x in processor['sns_topics']])
    
    return config


def build_dispatcher(base, backend, lane, kinesis_stream_arn):
    config = copy.deepcopy(FUNC_TEMPLATE)
    config['Properties']['Environment']['Variables'] = {
        'FUNC_NAME': { 'Fn::Sub': '${MainFunc}' },
        'DELAY': lane.get('delay', 0),
        'REGION': base['aws']['region'],
    }
    config['Properties']['Role'] = backend['role_arn']['dispatcher']
    config['Properties']['Handler'] = 'dispatcher.lambda_handler'
    config['Properties']['Events'] = {
        'StreamEvent': {
            'Type': 'Kinesis',
            'Properties': {
                'Stream': kinesis_stream_arn,
                'StartingPosition': 'TRIM_HORIZON',
                'BatchSize': lane.get('batch_size', 1),
            },
        },
    }
    
    return config


def build_main_func(base, bucket_mapping, handler, sns_topic_arn):
    args_jdata = json.dumps(handler.get('args', {}), separators=(',', ':'))
    bmap_jdata = json.dumps(bucket_mapping, separators=(',', ':'))
    config = copy.deepcopy(FUNC_TEMPLATE)    
    config['Properties'].update({
        'Role': handler['role_arn'],
        'Handler': 'main.lambda_handler',
        'Environment': {
            'Variables': {
                'HANDLER_PATH': handler['path'],
                'HANDLER_ARGS': args_jdata,
                'BUCKET_MAPPING': bmap_jdata,
            },
        },
        'DeadLetterQueue': {
            'Type': 'SNS',
            'TargetArn': sns_topic_arn,
        },
        'MemorySize': 1024,
        'ReservedConcurrentExecutions': handler.get('concurrency', 5),
    })

    if ('security_group_ids' in base['aws'] and 'subnet_ids' in base['aws']):
        config['Properties']['VpcConfig'] = {
            'SecurityGroupIds':  base['aws']['security_group_ids'],
            'SubnetIds':         base['aws']['subnet_ids'],
        }
    
    return config
    

def build_reporter(base, processor, sns_topic_arn, dynamodb_table_name):
    config = copy.deepcopy(FUNC_TEMPLATE)
    config['Properties']['Role'] = processor['role_arn']['reporter']
    config['Properties']['Handler'] = 'reporter.lambda_handler'
    config['Properties']['Environment']['Variables'] = {
        'ERROR_TABLE': dynamodb_table_name,
    }

    config['Properties']['Events'] = {
        'FailedMainFunc': {
            'Type': 'SNS',
            'Properties': {'Topic': sns_topic_arn},
        }
    }

    return config


def build_drain(base, processor, dynamodb_table_name):
    config = copy.deepcopy(FUNC_TEMPLATE)
    config['Properties']['Role'] = processor['role_arn']['drain']
    config['Properties']['Handler'] = 'drain.lambda_handler'
    config['Properties']['Environment']['Variables'] = {
        'ERROR_TABLE': dynamodb_table_name,
    }
    return config


def build_task_table():
    config = {
        'Type': 'AWS::DynamoDB::Table',
        'Properties': {
            'AttributeDefinitions': [
                {
                    'AttributeName': 'request_id',
                    'AttributeType': 'S',
                },
                {
                    'AttributeName': 's3_key',
                    'AttributeType': 'S',
                },
            ],
            'KeySchema': [
                {
                    'AttributeName': 'request_id',
                    'KeyType': 'HASH',
                },
                {
                    'AttributeName': 's3_key',
                    'KeyType': 'RANGE',
                },
            ],
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10,
            },
            'TimeToLiveSpecification': {
                'AttributeName': 'ttl',
                'Enabled': True,
            },
        }
    }

    return config


def build_error_table():
    config = {
        'Type': 'AWS::DynamoDB::Table',
        'Properties': {
            'AttributeDefinitions': [
                {
                    'AttributeName': 'request_id',
                    'AttributeType': 'S',
                },
            ],
            'KeySchema': [
                {
                    'AttributeName': 'request_id',
                    'KeyType': 'HASH',
                },
            ],
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10,
            },
        }
    }

    return config


def build_error_notification_sns():
    config = {
        'Type' : 'AWS::SNS::Topic',
    }
    return config


def build_error_notification_policy():
    topic = 'arn:aws:sns:${AWS::Region}:${AWS::AccountId}:${ErrorNotify}'
    config = {
        'Type' : 'AWS::SNS::TopicPolicy',
        'Properties': {
            'Topics': [
                {'Fn::Sub': topic},
            ],
        },
    }
    return config


def build_kinesis_stream(processor):
    config = {
        'Type' : 'AWS::Kinesis::Stream',
        'Properties' : {
            'RetentionPeriodHours' : 48,
            'ShardCount' : 1,
        }
    }
    return config


def get_kinesis_stream(key_name, label, backend):
    if 'kinesis_stream_fast_arn' in backend:
        arn = backend.get(key_name)
        return {
            'config': None,
            'arn': arn,
            'name': arn.split(':')[5].split('/')[1],
        }
    else:
        return {
            'config': build_kinesis_stream(backend),
            'arn': {'Fn::GetAtt': '{}.Arn'.format(label)},
            'name': {'Fn::Sub': '${{{}}}'.format(label)},
        }


def build(meta, zpath):
    FUNC_TEMPLATE['Properties']['CodeUri'] = zpath
    base_conf =        meta['base']
    backend =          meta.get('backend', {})
    hdlr_conf =        meta['handler']
    bucket_mapping =   meta['bucket_mapping']
    routing =          meta['routing']
    lane_conf =        backend.get('lane', {})
    
    sam_config = copy.deepcopy(SAM_TEMPLATE)
    rsc = sam_config['Resources']
    if 'description' in meta:
        sam_config['Description'] = meta['description']

    #
    # Create KinesisStream if needed.
    #
    kinesis_streams = [
        ('EventFastStream', 'kinesis_stream_fast_arn'),
        ('EventSlowStream', 'kinesis_stream_slow_arn'),
    ]
    ks_set = {}
    for label, key_name in kinesis_streams:
        ks = get_kinesis_stream(key_name, label, backend)
        sam_config['Resources'][label] = ks['config']
        ks_set[label] = ks
        
    #
    # Create DynamoDB table if needed.
    #
    if 'dynamodb_table_name' in backend:
        dynamodb_table_name = backend['dynamodb_table_name']
    else:
        rsc['ErrorTable'] = build_error_table()
        dynamodb_table_name = { 'Fn::Sub': '${ErrorTable}' }

    #
    # Create SNS topic if needed.
    #
    if 'dlq_sns_arn' in backend:
        sns_topic_arn = backend['dlq_sns_arn']
    else:
        rsc['ErrorNotify'] = build_error_notification_sns()
        sns_topic_arn = {'Ref': 'ErrorNotify'}
    
    #
    # Configure functions.
    #
    rsc.update({
        # Backend Functions
        'EventPusher': build_event_pusher(backend, routing,
                                          ks_set['EventFastStream']['name'],
                                          ks_set['EventSlowStream']['name']),
        'FastDispatcher': build_dispatcher(base_conf, backend,
                                           lane_conf.get('fast', {}),
                                           ks_set['EventFastStream']['arn']),
        'SlowDispatcher': build_dispatcher(base_conf, backend,
                                           lane_conf.get('slow', {}),
                                           ks_set['EventSlowStream']['arn']),
        'Reporter':    build_reporter(base_conf, backend, sns_topic_arn,
                                      dynamodb_table_name),
        'Drain':       build_drain(base_conf, backend, dynamodb_table_name),
        
        # Main Function
        'MainFunc':    build_main_func(base_conf, bucket_mapping,
                                       hdlr_conf, sns_topic_arn),
    })
    
    return obj2yml(sam_config)
