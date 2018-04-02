#!/usr/bin/env python

import abc
import yaml
import json
import io
import boto3
import base64
import os
import zipfile
from functools import reduce
import argparse
import logging
import tempfile
import subprocess

from . import sam


logger = logging.getLogger()


BASE_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
REL_BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def obj2yml(obj):
    ss = io.StringIO()
    
    # Disable an alias feature in PyYAML because CFn does not support YAML alias
    noalias_dumper = yaml.dumper.SafeDumper
    noalias_dumper.ignore_aliases = lambda self, data: True
    yaml.dump(obj, ss, default_flow_style=False, Dumper=noalias_dumper)
    ss.seek(0)
    return ss.read()


def choose(func, base, key_set):
    def safe_lookup(c, keys):
        v = c
        for k in keys:
            if k not in v: return None
            v = v[k]

        return v

    fv = safe_lookup(func, key_set)
    bv = safe_lookup(base, key_set)
    if not fv and not bv:
        raise Exception('No available value for {}'.format(key_set))
    
    return fv or bv


def encrypt(meta, base, data):
    key_id = choose(meta, base, ('aws', 'kms_arn'))
    kms = boto3.client('kms')
    enc_data = kms.encrypt(KeyId=key_id, Plaintext=data)
    return base64.b64encode(enc_data['CiphertextBlob']).decode('utf8')


def fetch_file_path(dpath, root_dir):
    tf = []
    for root, dirs, files in os.walk(dpath):
        for fname in files:
            if not root.endswith('/__pycache__'):
                fpath = os.path.join(root, fname)
                tf.append((fpath, fpath[len(root_dir) + 1:]))

    return tf


def search_pkg_dir(pkg_dir):
    for dname in os.listdir(pkg_dir):
        fpath = os.path.join(pkg_dir, dname)
        if os.path.isdir(fpath):
            if fpath.endswith('.egg'):
                yield (fpath, fpath)
            else:
                yield (fpath, pkg_dir)

        
def pack_zip_file(out_path, base_dir, own_dir):
    target_files = []
    cwd = os.path.abspath(os.getcwd())
    def up_to_pkgdir(pdir):        
        up = os.path.dirname(pdir)
        return up if up.endswith('site-packages') else up_to_pkgdir(up)
    
    pkg_dir = os.path.normpath(up_to_pkgdir(boto3.__path__[0]))
    abs_own_dir = os.path.abspath(own_dir)
    
    logger.debug('BASE DIR: %s', base_dir)
    src_dir = os.path.join(base_dir, 'slips')
    
    src_dirs = list(search_pkg_dir(pkg_dir)) + [
        (src_dir, src_dir),
        (src_dir, os.path.normpath(os.path.join(src_dir, '..'))),
        (abs_own_dir, cwd),
    ]

    wrote_path = set()    
    target_files = list(reduce(lambda x, y: x + y,
                               [fetch_file_path(*d) for d in src_dirs]))
    
    exclude_suffix = [
        'boto3', 'botocore', 'pip', 'EGG-INFO', '__pycache__', 'setuptools'
    ]
    with zipfile.ZipFile(out_path, 'w', zipfile.ZIP_DEFLATED) as z:
        for fpath, wpath in target_files:
            if wpath in wrote_path:
                logger.debug('avoid duplicated path: %s -> %s', fpath, wpath)
                continue
            if any(map(wpath.startswith, exclude_suffix)):
                logger.debug('avoid excluded path: %s -> %s', fpath, wpath)
                continue

            logger.debug('zip %s -> %s', fpath, wpath)
            z.write(fpath, wpath)
            wrote_path.add(wpath)


class Job(abc.ABC):
    @abc.abstractmethod
    def exec(self, args, meta):
        pass

    @staticmethod
    def _get_resource_info(meta, logical_name):
        cfn = boto3.client('cloudformation')
        res = cfn.describe_stack_resources(StackName=meta['stack_name'])

        resources = [x for x in res['StackResources']
                     if x['LogicalResourceId'] == logical_name]
        if len(resources) != 1:
            logger.error('Available resources: %s',
                         [x['LogicalResourceId'] for x in res['StackResources']])
            raise Exception('{} is not found'.format(logical_name))

        return resources[0]
        

class Package(Job):
    def exec(self, args, meta):
        # ----------
        # Create zip file including Python sorce codes
        logger.info('no package file is given, building')
        tmp_fd, pkg_file = tempfile.mkstemp(suffix='.zip')
        os.close(tmp_fd)
        pack_zip_file(pkg_file, args.root_dir, args.src_dir)
        return pkg_file
    

class ShowErrors(Job):
    def exec(self, args, meta):
        resource = Job._get_resource_info(meta, 'ErrorTable')
        table_name = resource['PhysicalResourceId']
        logger.debug('Physical Table Name: %s', table_name)
        
        dynamodb = boto3.client('dynamodb')
        table_res = dynamodb.scan(TableName=table_name)

        logger.info('Total number of error items: %s', table_res['Count'])
        import pprint

        rows = []
        for item in table_res['Items']:
            req_id = item.get('request_id', {}).get('S')
            jdata = item.get('argument', {}).get('S')
            if not req_id or not jdata:
                logger.error('Invalid format item: {}'.format(item))
                continue

            args = json.loads(jdata)
            for arg in args:
                print('{}:  {}  {:16s} {} ({} byte)'
                      ''.format(arg['event_time'], req_id, arg['bucket_name'],
                                arg['object_key'], arg['object_size']))

        
class Drain(Job):
    def exec(self, args, meta):
        resource = Job._get_resource_info(meta, 'Drain')
        func_name = resource['PhysicalResourceId']
        logger.debug('Physical Function Name: %s', func_name)

        client = boto3.client('lambda')
        res = client.invoke(FunctionName=func_name, Payload=b'{}')
        logger.debug('Result: %s', res)
        logger.info('Return value: %s', res['Payload'].read())
        

class Limit(Job):
    def exec(self, args, meta):
        lanes = {
            'fast': 'FastDispatcher',
            'slow': 'SlowDispatcher',
        }

        func_logic_name = lanes[args.lane_name]
        func_resrc = Job._get_resource_info(meta, func_logic_name)
        func_name = func_resrc['PhysicalResourceId']
        logger.info('Physical Function Name: %s', func_name)

        client = boto3.client('lambda')

        # ---------------------------------
        # Update DELAY environment variable
        #
        func = client.get_function_configuration(FunctionName=func_name)
        env_vars = func['Environment']['Variables']
        if hasattr(args, 'delay') and args.delay is not None:
            print('delay:     ', env_vars['DELAY'], '->', args.delay)
            env_vars['DELAY'] = str(args.delay)
            env = {'Variables': env_vars}
            logger.info('New env vars: %s', json.dumps(env, indent=4))
            r = client.update_function_configuration(FunctionName=func_name,
                                                     Environment=env)
            if r['ResponseMetadata']['HTTPStatusCode'] not in [200, 202]:
                logger.error('Fail to update environment vairable DELAY: %s', r)
                raise Exception('Failt to update DELAY parameter')
        else:
            print('delay:     ', env_vars['DELAY'])

        # ---------------------------------
        # Update BatchSize
        #
        ev_src_res = client.list_event_source_mappings(FunctionName=func_name)
        ev_src_list = ev_src_res['EventSourceMappings']
        import pprint
        # pprint.pprint(ev_src_list)
        
        if len(ev_src_list) != 1:
            logger.error('Invalid event source mapping: %s', ev_src_list)
            raise Exception('Number of event source have to be one')

        ev_src = ev_src_list[0]

        batch_size = ev_src['BatchSize']
        if hasattr(args, 'batch_size') and args.batch_size is not None:
            print('batch_size:', batch_size, '->', args.batch_size)
            r = client.update_event_source_mapping(UUID=ev_src['UUID'],
                                                   BatchSize=args.batch_size)
            if r['ResponseMetadata']['HTTPStatusCode'] not in [200, 202]:
                logger.error('Fail to update BatchSize: %s', r)
                raise Exception('Failt to update BatchSize')
        else:
            print('batch_size:', batch_size)
            
        
        
        
# -------------------------------------------------------------------
# Deployment section
#

    
class Deploy(Job):
    @staticmethod
    def configure(yml_file, pkg_file, code_bucket, code_prefix):
        sam_fd, sam_file = tempfile.mkstemp(suffix='.yml')
        os.close(sam_fd)
    
        # ---------------------
        # Packaging and generating SAM yaml file for actual deploy
        #
        pkg_cmd = [
            'aws', 'cloudformation', 'package', '--template-file', yml_file,
            '--s3-bucket', code_bucket, '--output-template-file', sam_file,
        ]
        logger.info('package command: %s', ' '.join(pkg_cmd))
        if code_prefix:
            pkg_cmd += ['--s3-prefix', code_prefix]
    
        logger.debug('Run: %s', ' '.join(pkg_cmd))
        pkg_res = subprocess.run(pkg_cmd, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
        logger.debug('Return code: %d\nSTDOUT: %s\nSTDERR: %s',
                     pkg_res.returncode, pkg_res.stdout, pkg_res.stderr)
        if pkg_res.returncode != 0:
            logger.error('aws command failed => %s', pkg_res.stderr)
            raise Exception('aws command failed: {}'.format(pkg_res.stderr))
        
        logger.info('generated SAM file: %s', sam_file)
        return sam_file

    @staticmethod
    def deploy(stack_name, sam_file):
        # ---------------------
        # Deploying
        #
        deploy_cmd = [
            'aws', 'cloudformation', 'deploy', '--template-file', sam_file,
            '--stack-name', stack_name, '--capabilities', 'CAPABILITY_IAM',
        ]
        logger.debug('Run: %s', ' '.join(deploy_cmd))

        deploy_res = subprocess.run(deploy_cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
        logger.debug('Return code: %d\nSTDOUT: %s\nSTDERR: %s',
                     deploy_res.returncode, deploy_res.stdout.decode('utf8'),
                     deploy_res.stderr.decode('utf8'))
        
        changed = True
        if deploy_res.returncode != 0:
            errmsg = deploy_res.stderr.decode('utf8')
            if errmsg.startswith('\nNo changes to deploy.'):
                changed = False
            else:
                logger.error('aws command failed (%d) => %s',
                             deploy_res.returncode, errmsg)
                raise Exception('aws command failed: {}'.format(errmsg))
        
        logger.info('Completed (%s)', 'Applied' if changed else 'No changes')
    
        return None
    
    def exec(self, args, meta):
        logger.info('Bulding stack: %s', meta['stack_name'])
        
        given_pkg_file = args.package_file
        pkg_file = given_pkg_file if given_pkg_file else Package().exec(args, meta)
        logger.info('package file: %s', pkg_file)

        yml_file = args.generated_sam_yaml
        if not yml_file:
            logger.info('no SAM template file is given, building')

            
            sam_template = sam.build(meta, pkg_file)
            tmp_fd, yml_file = tempfile.mkstemp(suffix='.yml')
            os.write(tmp_fd, sam_template.encode('utf8'))
            
        logger.info('SAM template file: %s', yml_file)
        code_bucket = meta['base']['sam']['code_bucket']
        code_prefix = meta['base']['sam'].get('code_prefix')

        sam_file = Deploy.configure(yml_file, pkg_file, code_bucket, code_prefix)
        
        if args.dry_run:
            print(open(sam_file).read())
        else:
            Deploy.deploy(meta['stack_name'], sam_file)


class Task:
    DEFAULT_CONFIG_PATH = './config.yml'
    
    def __init__(self):
        logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s '
                            '[%(filename)s:%(lineno)d] %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

    def run(self, argv):
        psr = argparse.ArgumentParser()
        psr.add_argument('-v', '--verbose', action='count', default=0,
                         help='v=info, vv=debug')
        psr.add_argument('-c', '--meta-file', default=None)

        subpsr = psr.add_subparsers()

        # -----------------------------
        # Deploy
        psr_deploy = subpsr.add_parser('deploy', help='Deploy CFn stack')
        psr_deploy.add_argument('-p', '--package-file')
        psr_deploy.add_argument('-y', '--generated-sam-yaml')
        psr_deploy.add_argument('-d', '--root-dir', default=BASE_DIR)
        psr_deploy.add_argument('-s', '--src-dir', default='./src',
                                help='Your source directory')
        psr_deploy.add_argument('--dry-run', action='store_true')
        psr_deploy.set_defaults(handler=Deploy)

        # -----------------------------
        # Show Errors
        psr_errors = subpsr.add_parser('errors', help='Show errors')
        psr_errors.set_defaults(handler=ShowErrors)
        psr_errors.add_argument('-s', '--stack-name')

        # -----------------------------
        # Drain
        psr_drain = subpsr.add_parser('drain', help='Drain error items for retry')
        psr_drain.set_defaults(handler=Drain)
        psr_drain.add_argument('-s', '--stack-name')

        # -----------------------------
        # Limit
        psr_limit = subpsr.add_parser('limit', help='Set delay and batch_size')
        psr_limit.set_defaults(handler=Limit)
        psr_limit.add_argument('lane_name')
        psr_limit.add_argument('-s', '--stack-name')
        psr_limit.add_argument('-b', '--batch-size', type=int)
        psr_limit.add_argument('-d', '--delay', type=int)

        args = psr.parse_args(argv)

        if args.meta_file:
            meta = yaml.load(open(args.meta_file, 'rt'))
        elif hasattr(args, 'stack_name') and args.stack_name:
            meta = {'stack_name': args.stack_name}
        else:
            meta = yaml.load(open(Task.DEFAULT_CONFIG_PATH, 'rt'))

        if args.verbose == 1:
            logger.setLevel(logging.INFO)
        elif args.verbose >= 2:
            logger.setLevel(logging.DEBUG)

        if hasattr(args, 'handler'):
            args.handler().exec(args, meta)
        else:
            psr.print_help()
            
        logger.info('exit')
