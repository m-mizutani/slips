# -*- coding: utf-8 -*-

import abc
import datetime
import dateutil
import logging
import json
import tempfile
import os
import boto3
import gzip
import re
import csv
import io

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class MetaData:
    def __init__(self, orig=None):
        attrs = [
            ('tag', None),
            ('timestamp', int(datetime.datetime.now().timestamp())),
            ('source', {}),
            ('message', None),
        ]

        for attr_name, default in attrs:
            if orig:
                value = getattr(orig, attr_name)
                if isinstance(value, set):
                    value = value.copy()
            else:
                value = default

            setattr(self, attr_name, value)

    def copy(self):
        meta = MetaData(self)
        return meta

    def __repr__(self):
        dt = datetime.datetime.fromtimestamp(self.timestamp)
        msg = '<tag:{}, timestamp:{} ({}), source:{}>' \
              ''.format(self.tag, self.timestamp, str(dt), self.source)
        return msg


class Task(object):
    def __init__(self):
        self._dst = None
        self._closed = False

    def set_params(self, s3_bucket, s3_key):
        self._s3_bucket = s3_bucket
        self._s3_key =    s3_key

    def pipe(self, dst):
        self._dst = dst

    def emit(self, meta: MetaData, data: dict):
        if self._dst:
            self._dst.recv(meta, data)
        else:
            logger.warning('No destination')

    def close(self):
        self._closed = True
        if self._dst:
            self._dst.close()

    @property
    def closed(self):
        return self._closed


# --------------------------------------------------------
# Tasks
# --------------------------------------------------------

class Spout(Task, abc.ABC):
    @abc.abstractmethod
    def run(self, s3_bucket, s3_key):
        pass


def download_s3_object(s3_bucket, s3_key):
    # Prepare a temporary file.
    fname = s3_key.split('/')[-1]
    tfd, tpath = tempfile.mkstemp(suffix=fname)
    os.close(tfd)

    # Downloading s3 object.
    s3 = boto3.client('s3')
    logger.info('Downloading %s/%s to %s', s3_bucket, s3_key, tpath)
    res = s3.download_file(s3_bucket, s3_key, tpath)
    logger.info('Download completed > %s', res)

    return tpath


class S3Lines(Spout):
    def run(self, s3_bucket, s3_key):
        fpath = download_s3_object(s3_bucket, s3_key)
        if s3_key.endswith('.gz'):
            fd = gzip.open(fpath, 'rb')
        else:
            fd = open(fpath, 'rb')

        for raw in fd:
            try:
                line = raw.decode('utf8').rstrip()
                meta = MetaData()
                self.emit(meta, {'message': line})
            except UnicodeDecodeError as e:
                logger.error(e)
                logger.error('Decoding error: %s', raw)

        os.remove(fpath)


class S3TextFile(Spout):
    def run(self, s3_bucket, s3_key):
        fpath = download_s3_object(s3_bucket, s3_key)
        if s3_key.endswith('.gz'):
            data = gzip.open(fpath, 'rt').read()
        else:
            data = open(fpath, 'rt').read()

        meta = MetaData()
        self.emit(meta, {'message': data})
        os.remove(fpath)


class Ignore(Spout):
    def run(self, s3_bucket, s3_key):
        return # Nothing to do


# --------------------------------------------------------
# Parser
# --------------------------------------------------------

#
# Base classes
#
class Parser(Task, abc.ABC):
    @abc.abstractmethod
    def recv(self, meta: MetaData, data: dict):
        pass


class ParseError(Exception):
    pass


#
# Parsers
#
class Json(Parser):
    def recv(self, meta: MetaData, data: dict):
        msg = data['message']
        self.emit(meta, json.loads(msg))


class Syslog(Parser):
    BASE_DAY = datetime.datetime.now()
    MSG_REGEX = re.compile('^(\S{3} \d{1,2} \d{2}:\d{2}:\d{2}) '
                              '(\S+) (\S+)\\[(\d+)\]:\s*(.*)$')
    DATE_FMT = '%b %d %H:%M:%S'

    @staticmethod
    def parse(line):
        # Nov 21 06:00:24 ip-172-31-7-118 sshd[23511]:
        mo = Syslog.MSG_REGEX.search(line)
        if not mo:
            raise ParseError('not syslog format "{}"'.format(line))

        data = {
            'datetime':  mo.group(1),
            'hostname':  mo.group(2),
            'proc_name': mo.group(3),
            'proc_id':   mo.group(4),
            'message':   mo.group(5)
        }
        return data

    def recv(self, meta: MetaData, data: dict):
        msg = data['message']
        obj = Syslog.parse(msg)
        dt = datetime.datetime.strptime(obj['datetime'], Syslog.DATE_FMT)

        # Currently, don't inherit previous object message.
        # To be fixed.
        m = meta.copy()
        m.timestamp = dt.timestamp()
        self.emit(m, obj)


class GSuiteLogin(Parser):
    def recv(self, meta: MetaData, data: dict):
        dt_fmt = '%Y-%m-%dT%H:%M:%S%z'
        sdt = data.get('id', {}).get('time', {})
        if sdt:
            dt = datetime.datetime.strptime(sdt[:19] + '+0000', dt_fmt)
            meta.timestamp = int(dt.timestamp())

        meta.tag = 'gsuite.login'
        self.emit(meta, data)


class FluentdJson(Parser):
    def recv(self, meta: MetaData, data: dict):
        row = data['message'].split('\t')
        assert len(row) == 3
        dt = dateutil.parser.parse(row[0])
        jdata = json.loads(row[2])

        meta.timestamp = dt.timestamp()
        meta.tag = row[1]
        self.emit(meta, jdata)


class AzureAdAudit(Parser):
    def recv(self, meta: MetaData, data: dict):
        dt_txt = data.get('activityDate')
        if dt_txt:
            dt = datetime.datetime.strptime(dt_txt[:19], '%Y-%m-%dT%H:%M:%S')
            meta.timestamp = dt.timestamp()

        meta.tag = 'azure_ad.audit'
        self.emit(meta, data)


class AzureAdEvent(Parser):
    def recv(self, meta: MetaData, data: dict):
        dt_txt = data.get('signinDateTime')
        if dt_txt:
            dt = datetime.datetime.strptime(dt_txt[:19], '%Y-%m-%dT%H:%M:%S')
            meta.timestamp = dt.timestamp()

        meta.tag = 'azure_ad.signin_event'
        self.emit(meta, data)


class AzureAdRiskEvent(Parser):
    def recv(self, meta: MetaData, data: dict):
        dt_txt = data.get('riskEventDateTime')
        if dt_txt:
            dt = datetime.datetime.strptime(dt_txt[:19], '%Y-%m-%dT%H:%M:%S')
            meta.timestamp = dt.timestamp()

        meta.tag = 'azure_ad.risk_event'
        self.emit(meta, data)
        

class CylanceEvent(Parser):
    def recv(self, meta: MetaData, data: dict):
        dt_txt = data.get('datetime')
        if dt_txt:
            dt = datetime.datetime.strptime(dt_txt[:19], '%Y-%m-%dT%H:%M:%S')
            meta.timestamp = dt.timestamp()

        meta.tag = 'cylance.event'
        self.emit(meta, data)


class CylanceThreat(Parser):
    def recv(self, meta: MetaData, data: dict):
        dt_txt = data.get('datetime')
        if dt_txt:
            dt = datetime.datetime.strptime(dt_txt[:19], '%Y-%m-%dT%H:%M:%S')
            meta.timestamp = dt.timestamp()

        meta.tag = 'cylance.threat'
        self.emit(meta, data)


class AwsCloudtrailEvent(Parser):
    def recv(self, meta: MetaData, data: dict):
        msg = data.get('message')
        if not msg:
            raise ParseError('No "message": {}'.format(str(data)))

        jdata = json.loads(msg)
        if ('Records' not in jdata or not isinstance(jdata['Records'], list)):
            raise ParseError('No "Records" array in message: '
                             '{}'.format(str(jdata)))

        for rec in jdata['Records']:
            rec_meta = meta.copy()
            if 'eventTime' in rec:
                dt = datetime.datetime.strptime(rec['eventTime'],
                                                '%Y-%m-%dT%H:%M:%SZ')
                rec_meta.timestamp = int(dt.timestamp())

            rec['description'] = '{} {} by {} on {}'.format(
                rec.get('eventType'), rec.get('eventName'),
                rec.get('userIdentity', {}).get('arn'),
                rec.get('sourceIPAddress')
            )

            ev_type = 'aws.cloudtrail.{}'.format(rec.get('eventType'))
            rec_meta.tag = ev_type
            self.emit(rec_meta, rec)


class AwsGuardDuty(Parser):
    def recv(self, meta: MetaData, data: dict):
        meta.tag = 'aws.guardduty'
        self.emit(meta, data)


class Kea(Parser):
    PATTERN = re.compile('^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) ([A-Z]+)\s+'
                         '\[(\S+?)\] (\S+) \[hwtype=(\S+) (\S+)\], cid=\[(.*?)\], '
                         'tid=(\S+): (.*)')
    MSG_REGEX = {
        'DHCP4_INIT_REBOOT':  re.compile('requests address (\S+)'),
        'DHCP4_LEASE_ADVERT': re.compile('lease (\S+) will be advertised'),
        'DHCP4_LEASE_ALLOC':  re.compile('lease (\S+) has been allocated'),
    }
    
    def recv(self, meta: MetaData, data: dict):
        dt_fmt = '%Y-%m-%d %H:%M:%S'
        msg = data.get('message')
        if not msg:
            logger.error('No message of Kea: %s', data)
            raise Exception('No "message" attribute')
        
        mo = Kea.PATTERN.search(msg)
        if not mo:
            logger.error('Invalid format of kea message: %s', msg)
            raise Exception('Invalid format of kea message')

        keys = ['event_datetime', 'msg_level', 'proc', 'event', 'hwtype',
                'hwaddr', 'client_id', 'tx_id', 'msg']
                
        data.update(dict(zip(keys, mo.groups())))

        regex = Kea.MSG_REGEX.get(data['event'])
        if not regex:
            logger.error('Not supported event: %s', data['event'])
            raise Exception('Not supported event of kea')
        
        mo2 = regex.search(data['msg'])
        if not mo2:
            logger.error('Regex is not matched: %s', data)
            raise Exception('Regex is not matched')

        data['ipaddr'] = mo2.group(1)

        
        # Setting metadata.
        dt_s = data['event_datetime'].split('.')[0]
        dt = datetime.datetime.strptime(dt_s, dt_fmt)        
        meta.timestamp = int(dt.timestamp())
        meta.tag = 'kea.log'

        self.emit(meta, data)


class PacketBeat(Parser):
    def recv(self, meta: MetaData, data: dict):
        meta.tag = 'packetbeat.{}'.format(data['type'])
        dt_fmt = '%Y-%m-%dT%H:%M:%S'
        dt_txt = data.get('@timestamp')
        if dt_txt:
            dt = datetime.datetime.strptime(dt_txt.split('.')[0], dt_fmt)
            meta.timestamp = int(dt.timestamp())

        if data['type'] == 'dns':
            data['message'] = '{} from {}'.format(data.get('query'),
                                                  data.get('client_ip'))
            
        self.emit(meta, data)

        
class AuditBeat(Parser):
    def recv(self, meta: MetaData, data: dict):
        meta.tag = 'auditbeat.log'
        dt_fmt = '%Y-%m-%dT%H:%M:%S'
        dt_txt = data.get('@timestamp')
        if dt_txt:
            dt = datetime.datetime.strptime(dt_txt.split('.')[0], dt_fmt)
            meta.timestamp = int(dt.timestamp())

        audit = data.get('audit')
        if not audit:
            return

        if isinstance(audit.get('kernel'), dict):
            meta.tag = 'auditbeat.kernel'
            log = audit.get('kernel')
            data['message'] = '{} {} {} by {}'.format(
                log.get('actor', {}).get('primary'),
                log.get('action'),
                log.get('thing', {}).get('primary'),
                log.get('how'))
        elif isinstance(audit.get('file'), dict):
            meta.tag = 'auditbeat.file'
            log = audit.get('file')
            data['message'] = '{} is {} ({})'.format(
                log.get('path'),
                log.get('action'),
                log.get('sha256'))

            
        self.emit(meta, data)

        
class EcsHako(Parser):
    def recv(self, meta: MetaData, data: dict):
        meta.tag = 'ecs.hako'
        dt_fmt = '%Y-%m-%dT%H:%M:%SZ'
        dt_txt = data.get('time')
        if dt_txt:
            dt = datetime.datetime.strptime(dt_txt, dt_fmt)
            meta.timestamp = int(dt.timestamp())
            
        self.emit(meta, data)

        
class PaloAlto(Parser):
    TRAFFIC_COLUMN = [
        'Domain', 'Receive Time', 'Serial #', 'Type', 'Threat/Content Type',
        'Config Version', 'Generate Time', 'Source address',
        'Destination address', 'NAT Source IP', 'NAT Destination IP', 'Rule',
        'Source User', 'Destination User', 'Application', 'Virtual System',
        'Source Zone', 'Destination Zone', 'Inbound Interface',
        'Outbound Interface', 'Log Action', 'Time Logged', 'Session ID',
        'Repeat Count', 'Source Port', 'Destination Port', 'NAT Source Port',
        'NAT Destination Port', 'Flags', 'IP Protocol', 'Action', 'Bytes',
        'Bytes Sent', 'Bytes Received', 'Packets', 'Start Time',
        'Elapsed Time (sec)', 'Category', 'Padding', 'seqno', 'actionflags',
        'Source Country', 'Destination Country', 'cpadding', 'pkts_sent',
        'pkts_received', 'session_end_reason', 'dg_hier_level_1',
        'dg_hier_level_2', 'dg_hier_level_3', 'dg_hier_level_4', 'vsys_name',
        'device_name', 'action_source'
    ]
    THREAT_COLUMN = [
        'Domain', 'Receive Time', 'Serial #', 'Type', 'Threat/Content Type',
        'Config Version', 'Generate Time', 'Source address',
        'Destination address', 'NAT Source IP', 'NAT Destination IP', 'Rule',
        'Source User', 'Destination User', 'Application', 'Virtual System',
        'Source Zone', 'Destination Zone', 'Inbound Interface',
        'Outbound Interface', 'Log Action', 'Time Logged', 'Session ID',
        'Repeat Count', 'Source Port', 'Destination Port', 'NAT Source Port',
        'NAT Destination Port', 'Flags', 'IP Protocol', 'Action', 'URL',
        'Threat/Content Name', 'Category', 'Severity', 'Direction', 'seqno',
        'actionflags', 'Source Country', 'Destination Country', 'cpadding',
        'contenttype', 'pcap_id', 'filedigest', 'cloud', 'url_idx',
        'user_agent', 'filetype', 'xff', 'referer', 'sender', 'subject',
        'recipient', 'reportid', 'dg_hier_level_1', 'dg_hier_level_2',
        'dg_hier_level_3', 'dg_hier_level_4', 'vsys_name', 'device_name',
        'file_url'
    ]

    COLUMN_MAP = {
        'TRAFFIC': TRAFFIC_COLUMN,
        'THREAT':  THREAT_COLUMN,
    }
    TAG_MAP = {
        'TRAFFIC': 'paloalto.traffic',
        'THREAT':  'paloalto.threat',
    }

    def recv(self, meta: MetaData, data: dict):
        msg = data.get('message')

        if not msg:
            raise ParseError('No "message": {}'.format(str(data)))

        ss = io.StringIO()
        ss.write(msg)
        ss.seek(0)
        row = next(csv.reader(ss))

        if len(row) < 4:
            raise ParseError('No enough column: "{}"'.format(row))

        column = PaloAlto.COLUMN_MAP.get(row[3])
        if not column:
            raise ParseError('Unsupported log type "{}": "{}"'
                             ''.format(row[3], str(row)))

        if len(row) != len(column):
            raise ParseError('Column length is not matched, '
                             'Expected = {}, Actual = {}: {}'
                             ''.format(len(column), len(row), str(row)))

        data.update(dict(zip(column, row)))

        if 'URL' in data:
            data['URL'] = data['URL'].strip('"')

        dt_txt = data.get('Start Time')
        if dt_txt:
            dt = datetime.datetime.strptime(dt_txt, '%Y/%m/%d %H:%M:%S')
            meta.timestamp = int(dt.timestamp())

        msg_param_keys = [
            'Source address', 'Source Port',
            'Destination address', 'Destination Port',
            'IP Protocol',
            'Bytes Sent', 'Bytes Received',
        ]

        msg_fmt = '{0}:{1} => {2}:{3} ({4}), Sent {5} byte, Recv {6} byte'
        msg_params = [data.get(x) for x in msg_param_keys]
        data['raw_message'] = msg
        data['message'] = msg_fmt.format(*msg_params)

        if row[3] == 'THREAT':
            data['message'] += ' {}'.format(data.get('Threat/Content Name'))

        meta.tag = PaloAlto.TAG_MAP.get(row[3]) or meta.tag
        self.emit(meta, data)


class FalconEventLog(Parser):
    def recv(self, meta: MetaData, data: dict):
        meta.tag = 'falcon'
        dt_fmt = '%Y-%m-%dT%H:%M:%S'
        ts_txt = data.get('timestamp')
        if ts_txt:
            meta.timestamp = int(ts_txt) / 1000

        tgt_value = (data.get('RemoteAddressIP4') or
                     data.get('TargetFileName') or
                     data.get('DomainName') or
                     data.get('CommandLine'))

        data['message'] = '{} at {} to {}'.format(data.get('name'),
                                                  data.get('aip'), tgt_value)
        self.emit(meta, data)

        
# --------------------------------------------------------
# Data Stream
# --------------------------------------------------------


class Callback(Parser):
    def set_func(self, func):
        self._func = func

    def recv(self, meta: MetaData, data: dict):
        if self._func:
            self._func(meta, data)
        else:
            logger.warning('No destination')


class Stream:
    FUCTORY_MAP = {
        # fetchers
        's3-lines':         S3Lines,
        's3-text':          S3TextFile,
        # general parsers
        'json':             Json,
        'syslog':           Syslog,
        'fluentd-json':     FluentdJson,
        # specific products
        'paloalto':         PaloAlto,
        'g-suite-login':    GSuiteLogin,
        'cloudtrail':       AwsCloudtrailEvent,
        'guardduty':        AwsGuardDuty,
        'azure-ad-audit':   AzureAdAudit,
        'azure-ad-event':   AzureAdEvent,
        'azure-ad-risk-event':   AzureAdRiskEvent,
        'cylance':          CylanceEvent,
        'cylance-event':    CylanceEvent,
        'cylance-threat':   CylanceThreat,
        'kea':              Kea,
        'packetbeat':       PacketBeat,
        'auditbeat':        AuditBeat,
        'falcon':           FalconEventLog,
        # Special task
        'ignore':           Ignore,
    }

    def __init__(self, args):
        self._root = None
        self._head = None
        self._callback = Callback()
        self._callback.set_func(None)

        for arg in args:
            builder = Stream.FUCTORY_MAP.get(arg)
            if not builder:
                raise Exception('No such parser "{}"'.format(arg))

            task = builder()
            if self._head:
                self._head.pipe(task)
                self._head = task
            else:
                self._root = self._head = task

        self._head.pipe(self._callback)

    def read(self, s3_bucket, s3_key, callback):
        if not self._root:
            raise Exception('No task is configured')

        self._callback.set_func(callback)
        self._root.run(s3_bucket, s3_key)
        self._callback.set_func(None)
