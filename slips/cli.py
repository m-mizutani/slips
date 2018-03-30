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
logger.setLevel(logging.INFO)

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
            raise Exception('ErrorTable is not found')

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
        for item in table_res['Items']:
            print(item)

        
class Drain(Job):
    def exec(self, args, meta):
        resource = Job._get_resource_info(meta, 'Drain')
        func_name = resource['PhysicalResourceId']
        logger.debug('Physical Function Name: %s', func_name)

        client = boto3.client('lambda')
        res = client.invoke(FunctionName=func_name, Payload=b'{}')
        logger.debug('Result: %s', res)
        logger.info('Return value: %s', res['Payload'].read())
        

# -------------------------------------------------------------------
# Deployment section
#

class Deploy(Job):
    @staticmethod
    def deploy(stack_name, yml_file, pkg_file, code_bucket, code_prefix):
        sam_fd, sam_file = tempfile.mkstemp(suffix='.yml')
        os.close(sam_fd)
    
        # ---------------------
        # Packaging and generating SAM yaml file for actual deploy
        #
        pkg_cmd = [
            'aws', 'cloudformation', 'package', '--template-file', yml_file,
            '--s3-bucket', code_bucket, '--output-template-file', sam_file,
        ]
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
    
        # ---------------------
        # Deploying
        #
        deploy_cmd = [
            'aws', 'cloudformation', 'deploy', '--template-file', sam_file,
            '--stack-name', stack_name,
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

        if args.dry_run:
            print('- - - - built SAM configuration file - - - -\n')
            print(open(yml_file).read())
        else:
            Deploy.deploy(meta['stack_name'], yml_file, pkg_file,
                          code_bucket, code_prefix)
        

class Task:
    def __init__(self):
        logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s '
                            '[%(filename)s:%(lineno)d] %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

    def run(self, argv):
        psr = argparse.ArgumentParser()
        psr.add_argument('-v', '--verbose', action='store_true')
        psr.add_argument('-c', '--meta-file', default='config.yml')

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

        # -----------------------------
        # Drain
        psr_drain = subpsr.add_parser('drain', help='Drain error items for retry')
        psr_drain.set_defaults(handler=Drain)
        
        args = psr.parse_args(argv)
        
        meta = yaml.load(open(args.meta_file, 'rt'))
        if args.verbose:
            logger.setLevel(logging.DEBUG)

        if hasattr(args, 'handler'):
            args.handler().exec(args, meta)
        else:
            psr.print_help()
            
        logger.info('DOOOOOOOOOOONEEEEEE!')
