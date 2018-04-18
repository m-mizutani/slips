import os
import logging
import sys
import json
import traceback
import inspect
import importlib.machinery as imm

import slips.interface
import slips.parser

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class FormatError(Exception):
    pass


def load_handlers(fpath):
    full_path = os.path.abspath(fpath)
    mod_name = os.path.splitext(fpath)[0].replace('/', '.').lstrip('.')

    logger.info('Loading handler code from %s as %s', full_path, mod_name)
    sys.path.append(os.path.dirname(full_path))
    src_file = imm.SourceFileLoader(mod_name, full_path)
    mod = src_file.load_module()
    handlers = [m[1]() for m in inspect.getmembers(mod)
                if inspect.isclass(m[1]) and slips.interface.Handler in m[1].__bases__]
    logger.info('Loaded handlers: %s', handlers)
    return handlers


def create_parser(bucket_mapping, s3_bucket, s3_key):
    bucket_config = bucket_mapping.get(s3_bucket)
    logger.debug(bucket_mapping)

    if not bucket_config:
        raise FormatError('No format config for bucket "{}"'.format(s3_bucket))

    configs = sorted([x for x in bucket_config if s3_key.startswith(x['prefix'])],
                     key=lambda x: len(x['prefix']), reverse=True)

    if len(configs) == 0:
        raise FormatError('No format config for '
                          '{}/{}'.format(s3_bucket, s3_key))

    if len(configs) > 1:
        logger.warning('multiple configs (%d entries) for %s/%s'
                       ''.format(len(configs), s3_bucket, s3_key))
    logger.debug('Use config for %s/%s'.format(s3_bucket, configs[0]['prefix']))

    stream = slips.parser.Stream(configs[0]['format'])

    return stream


def main(args, events):
    logger.info('Event: %s', json.dumps(events, indent=4))
    logger.info('Env: \n%s', '\n'.join(["export {}='{}'".format(k, json.dumps(v))
                                        for k, v in args.items() if v]))

    bucket_mapping = json.loads(args['BUCKET_MAPPING'])
    handler_path =   args['HANDLER_PATH']
    handler_args =   args.get('HANDLER_ARGS') or '{}'

    handlers = load_handlers(handler_path)

    results = {}
    for hdlr in handlers:
        handler_args = json.loads(handler_args)
        hdlr.setup(handler_args)

        for ev in events:
            s3_bucket = ev['bucket_name']
            s3_key =    ev['object_key']
            stream = create_parser(bucket_mapping, s3_bucket, s3_key)
            stream.read(s3_bucket, s3_key, hdlr.recv)

        res = hdlr.result()
        logger.info('A result of %s -> %s', str(hdlr), res)
        results['.'.join([hdlr.__module__, hdlr.__class__.__name__])] = res

    return results


def lambda_handler(event, context):
    logger.info('Event: %s', json.dumps(event, indent=4))
    arg_keys = [
        'HANDLER_PATH',
        'HANDLER_ARGS',
        'BUCKET_MAPPING',
    ]
    args = dict([(k, os.environ.get(k)) for k in arg_keys])

    try:
        return main(args, event)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error(e)
        raise e


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s '
                        '[%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    lambda_handler(json.load(open(sys.argv[1])), None)
