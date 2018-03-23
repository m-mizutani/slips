import base64
import json
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def to_s3_record(record, s3event):
    return {
        'aws_region':  record['awsRegion'],
        'event_time':  record['eventTime'],
        'event_name':  record['eventName'],
        'bucket_name': s3event['bucket']['name'],
        'bucket_arn':  s3event['bucket']['arn'],
        'object_key':  s3event['object']['key'],
        'object_size': s3event['object']['size'],
        'object_etag': s3event['object']['eTag'],
    }


def extract_dlq_event(event):
    for record in event.get('Records', []):
        ev_src = record.get('eventSource') or record.get('EventSource')
        
        # From SNS
        if ev_src == 'aws:sns':
            sns_data = record.get('Sns', {})
            jdata = json.loads(sns_data.get('Message', '{}'))
            attrs = sns_data.get('MessageAttributes', {})
            yield jdata, attrs
        else:
            raise Exception('Unsupported event source: {}'.format(ev_src))


def extract_s3_event(event):
    for record in event.get('Records', []):
        ev_src = record.get('eventSource') or record.get('EventSource')
        
        # From SNS
        if ev_src == 'aws:sns':
            jdata = record.get('Sns', {}).get('Message')
            for rec in extract_s3_event(json.loads(jdata)):
                yield rec
        # From S3
        elif ev_src == 'aws:s3':
            yield to_s3_record(record, record.get('s3'))
        # Unsupported.
        else:
            raise Exception('Unsupported event source: {}'.format(ev_src))


def extract_kinesis_event(event):
    for record in event.get('Records', []):
        # Only need kinesis event
        kinesis_event = record.get('kinesis')
        if (record.get('eventSource') == 'aws:kinesis' and kinesis_event):
            obj = base64.b64decode(kinesis_event['data'])
            yield json.loads(obj)


def log_requests(res):
    try:
        logger.info('HTTP Result (JSON): %s -> %s, %s', res.url,
                    res.status_code, json.dumps(res.json(), indent=4))
    except Exception:
        logger.info('HTTP Result (TEXT): %s -> %s, %s', res.url,
                    res.status_code, res.text)


def escape_url(s):
    if s:
        return s.replace('.', '[.]').replace('http', 'hxxp')
    else:
        return '(none)'


def decrypt(enc):
    if not enc:
        raise Exception('No available credential for GHE')
    
    kms = boto3.client('kms')
    raw = kms.decrypt(CiphertextBlob=base64.b64decode(enc))['Plaintext']
    return raw.decode('utf8')
