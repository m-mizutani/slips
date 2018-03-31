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

ROLE_TEMPLATE = {
    'Type': 'AWS::IAM::Role',
    'Properties': {
        'AssumeRolePolicyDocument': {
            'Version' : '2012-10-17',
            'Statement': [ {
                'Effect': 'Allow',
                'Principal': {
                    'Service': [ 'lambda.amazonaws.com' ]
                },
                'Action': [ 'sts:AssumeRole' ]
            } ]
        },
        'Path': '/',
        'ManagedPolicyArns': [
            'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
        ],
        'Policies': [],
    }
}


def build_event_pusher(processor, routing, kinesis_stream_fast,
                       kinesis_stream_slow, role_arn):
    config = copy.deepcopy(FUNC_TEMPLATE)
    
    config['Properties']['Environment']['Variables'] = {
        'DST_KINESIS_STREAM_FAST': kinesis_stream_fast,
        'DST_KINESIS_STREAM_SLOW': kinesis_stream_slow,
        'ROUTING_POLICY': json.dumps(routing, separators=(',', ':')),
    }
    config['Properties']['Role'] = role_arn
    config['Properties']['Handler'] = 'event_pusher.lambda_handler'
    config['Properties']['Events'] = dict([(x['name'], {
        'Type': 'SNS', 'Properties': {'Topic': x['arn']},
    }) for x in processor['sns_topics']])
    
    return config


def build_dispatcher(base, backend, lane, kinesis_stream_arn, role_dispatcher):
    config = copy.deepcopy(FUNC_TEMPLATE)
    config['Properties']['Environment']['Variables'] = {
        'FUNC_NAME': { 'Fn::Sub': '${MainFunc}' },
        'DELAY': lane.get('delay', 0),
    }
    config['Properties']['Role'] = role_dispatcher
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


def build_main_func(base, bucket_mapping, handler, sns_topic_arn, role_arn):
    args_jdata = json.dumps(handler.get('args', {}), separators=(',', ':'))
    bmap_jdata = json.dumps(bucket_mapping, separators=(',', ':'))
    config = copy.deepcopy(FUNC_TEMPLATE)    
    config['Properties'].update({
        'Role': role_arn,
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

    if ('security_group_ids' in handler and 'subnet_ids' in handler):
        config['Properties']['VpcConfig'] = {
            'SecurityGroupIds':  handler['security_group_ids'],
            'SubnetIds':         handler['subnet_ids'],
        }
    
    return config
    

def build_reporter(base, processor, sns_topic_arn, dynamodb_table_name,
                   role_reporter):
    config = copy.deepcopy(FUNC_TEMPLATE)
    config['Properties']['Role'] = role_reporter
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


def build_drain(base, processor, dynamodb_table_name, role_arn):
    config = copy.deepcopy(FUNC_TEMPLATE)
    config['Properties']['Role'] = role_arn
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
    if key_name in backend:
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

    
def build_role_main_func(mapping, sns_topic_arn):
    resources = ['arn:aws:s3:::{}/{}*'.format(b, c['prefix'])
                 for b, x in mapping.items() for c in x]

    config = copy.deepcopy(ROLE_TEMPLATE)
    config['Properties']['Policies'] = [
        {
            'PolicyName': 'S3ObjectReadable',
            'PolicyDocument': {
                'Version' : '2012-10-17',
                'Statement': [ {
                    'Effect': 'Allow',
                    'Action': ['s3:GetObject'],
                    'Resource': resources,
                } ]
            }
        },
        {
            'PolicyName': 'SNSPublishable',
            'PolicyDocument': {
                'Version' : '2012-10-17',
                'Statement': [ {
                    'Effect': 'Allow',
                    'Action': ['sns:Publish'],
                    'Resource': sns_topic_arn,
                } ]
            }
        },
    ]

    return config


def build_role_event_pusher(ks_set):
    config = copy.deepcopy(ROLE_TEMPLATE)
    config['Properties']['Policies'] = [
        {
            'PolicyName': 'KinesisPutRecord',
            'PolicyDocument': {
                'Version' : '2012-10-17',
                'Statement': [ {
                    'Effect': 'Allow',
                    'Action': [
                        'kinesis:PutRecord',
                        'kinesis:PutRecords'
                    ],
                    'Resource': [ks['arn'] for ks in ks_set.values()],
                } ]
            }
        }
    ]
    return config


def build_role_reporter(dynamodb_arn):
    config = copy.deepcopy(ROLE_TEMPLATE)
    config['Properties']['Policies'] = [
        {
            'PolicyName': 'DynamoDBWriteable',
            'PolicyDocument': {
                'Version' : '2012-10-17',
                'Statement': [ {
                    'Effect': 'Allow',
                    'Action': [
                        'dynamodb:BatchWriteItem',
                        'dynamodb:PutItem',
                        'dynamodb:UpdateItem',
                    ],
                    'Resource': [
                        { 'Fn::Sub': [
                            '${TableARN}*', {'TableARN': dynamodb_arn}
                        ] },
                    ]
                } ]
            }
        },
    ]
    
    return config


def build_role_dispatcher(ks_set):
    config = copy.deepcopy(ROLE_TEMPLATE)
    config['Properties']['Policies'] = [
        {
            'PolicyName': 'KinesisReadable',
            'PolicyDocument': {
                'Version' : '2012-10-17',
                'Statement': [ {
                    'Effect': 'Allow',
                    'Action': [
                        'kinesis:GetShardIterator',
                        'kinesis:GetRecords',
                        'kinesis:DescribeStream',
                    ],
                    'Resource': [ks['arn'] for ks in ks_set.values()],
                } ]
            }
        },
        {
            'PolicyName': 'LambdaInvoke',
            'PolicyDocument': {
                'Version' : '2012-10-17',
                'Statement': [ {
                    'Effect': 'Allow',
                    'Action': ['lambda:InvokeFunction'], 
                    'Resource': {'Fn::GetAtt': ['MainFunc', 'Arn']},
                } ]
            }
        },
    ]
    
    return config


def build_role_drain(dynamodb_arn, ks_set):
    config = copy.deepcopy(ROLE_TEMPLATE)
    config['Properties']['Policies'] = [
        {
            'PolicyName': 'KinesisPutRecord',
            'PolicyDocument': {
                'Version' : '2012-10-17',
                'Statement': [ {
                    'Effect': 'Allow',
                    'Action': [
                        'kinesis:PutRecord',
                        'kinesis:PutRecords'
                    ],
                    'Resource': [ks['arn'] for ks in ks_set.values()],
                } ]
            }
        },
        {
            'PolicyName': 'DynamoDBWriteable',
            'PolicyDocument': {
                'Version' : '2012-10-17',
                'Statement': [ {
                    'Effect': 'Allow',
                    'Action': [
                        'dynamodb:DeleteItem',
                        'dynamodb:Scan',
                    ],
                    'Resource': [
                        { 'Fn::Sub': [
                            '${TableARN}*', {'TableARN': dynamodb_arn}
                        ] },
                    ]
                } ]
            }
        },
    ]
    
    return config


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
    if 'dynamodb_arn' in backend:
        dynamodb_arn = backend['dynamodb_arn']
        dynamodb_table_name = backend['dynamodb_arn'].split('/')[-1]
    else:
        rsc['ErrorTable'] = build_error_table()
        dynamodb_arn = { 'Fn::GetAtt': 'ErrorTable.Arn' }
        dynamodb_table_name = { 'Fn::Sub': '${ErrorTable}' }

    #
    # Create SNS topic if needed.
    #
    if 'dlq_sns_arn' in backend:
        sns_topic_arn = backend['dlq_sns_arn']
    else:
        rsc['ErrorNotify'] = build_error_notification_sns()
        sns_topic_arn = {'Ref': 'ErrorNotify'}

    if 'role_arn' in hdlr_conf:
        role_main_func = hdlr_conf['role_arn']
    else:
        rsc['MainFuncRole'] = build_role_main_func(bucket_mapping, sns_topic_arn)
        role_main_func = {'Fn::GetAtt' : 'MainFuncRole.Arn' }

    # Roles
    roles_conf = backend.get('role_arn', {})

    role_builders = [
        ('reporter',   'ReporterRole',   build_role_reporter(dynamodb_arn)),
        ('dispatcher', 'DispatcherRole', build_role_dispatcher(ks_set)),
        ('event_pusher', 'EventPusherRole', build_role_event_pusher(ks_set)),
        ('drain', 'DrainRole', build_role_drain(dynamodb_arn, ks_set)),
    ]
    role_arn = {}
    for role_name, logic_name, role_config in role_builders:
        if role_name in roles_conf:
            role_arn[role_name] = roles_conf[role_name]
        else:
            rsc[logic_name] = role_config
            role_arn[role_name] = {'Fn::GetAtt' : '{}.Arn'.format(logic_name)}

        
    #
    # Configure functions.
    #
    rsc.update({
        # Backend Functions
        'EventPusher': build_event_pusher(backend, routing,
                                          ks_set['EventFastStream']['name'],
                                          ks_set['EventSlowStream']['name'],
                                          role_arn['event_pusher']),
        'FastDispatcher': build_dispatcher(base_conf, backend,
                                           lane_conf.get('fast', {}),
                                           ks_set['EventFastStream']['arn'],
                                           role_arn['dispatcher']),
        'SlowDispatcher': build_dispatcher(base_conf, backend,
                                           lane_conf.get('slow', {}),
                                           ks_set['EventSlowStream']['arn'],
                                           role_arn['dispatcher']),
        'Reporter':    build_reporter(base_conf, backend, sns_topic_arn,
                                      dynamodb_table_name, role_arn['reporter']),
        'Drain':       build_drain(base_conf, backend, dynamodb_table_name,
                                   role_arn['drain']),
        
        # Main Function
        'MainFunc':    build_main_func(base_conf, bucket_mapping,
                                       hdlr_conf, sns_topic_arn, role_main_func),
    })
    
    return obj2yml(sam_config)
