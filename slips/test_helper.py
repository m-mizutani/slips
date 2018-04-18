import boto3
import botocore
import yaml
import os
import json
import gzip

def s3_object_size(s3_bucket, s3_key):
    client = boto3.client('s3')
    try:
        res = client.head_object(Bucket=s3_bucket, Key=s3_key)
        return res['ContentLength']
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return None


def setup(hdlr_path, hdlr_args, parsers, dpath, suffix=None):
    CONFIG_PATH = os.environ.get('CONFIG_PATH') or './tests/config.yml'
    config = yaml.load(open(CONFIG_PATH))

    if suffix is None:
        suffix = os.path.normpath(dpath)
    
    s3_bucket = config['s3']['bucket']
    s3_prefix = config['s3']['prefix']
    s3_key = os.path.normpath('{}{}'.format(s3_prefix, suffix))

    if not suffix.endswith('.gz'):
        s3_key += '.gz'

    if not os.environ.get('SLIPS_TEST_FORCE_UPLOAD'):
        s3_size = s3_object_size(s3_bucket, s3_key)
    else:
        s3_size = None
        
    if not s3_size:
        client = boto3.client('s3')
        data = open(dpath, 'rb').read()
        if not suffix.endswith('.gz'):
            data = gzip.compress(data)
            
        client.put_object(Bucket=s3_bucket, Key=s3_key, Body=data)
        s3_size = len(data)
        
    event = [{
        'aws_region': 'ap-northeast-1',
        'event_time': '2018-03-08T13:35:13.059Z',
        'event_name': 'ObjectCreated:Put',
        'bucket_name': s3_bucket,
        'bucket_arn': 'arn:aws:s3:::{}'.format(s3_bucket),
        'object_key': s3_key,
        'object_size': s3_size,
        'object_etag': 'efa488bdd2c7ae80697730216bbdbfb3'
    }]

    mapping = {}
    mapping[s3_bucket] = [{
        'prefix': s3_key,
        'format': parsers,
    }]
    
    args = {
        'HANDLER_PATH': hdlr_path,
        'HANDLER_ARGS': json.dumps(hdlr_args),
        'BUCKET_MAPPING': json.dumps(mapping),
    }
    
    return args, event


def make_args(path, args, mapping):
    args = {
        'HANDLER_PATH': path,
        'HANDLER_ARGS': json.dumps(args),
        'BUCKET_MAPPING': json.dumps(mapping),
    }
    return args
