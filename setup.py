#!/usr/bin/env python
# -*- coding:utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from setuptools import setup

setup(
    name='slips',
    version='0.0.1',
    description='Serverless Log Iterative Processing from S3',
    author='Masayoshi Mizutani',
    author_email='mizutani@cookpad.com',
    install_requires=['boto3', 'PyYAML', 'python-dateutil'],
    packages=['slips'],
    scripts=['bin/slips'],
)
