#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
:copyright: (c) 2017 by Mitchell Lisle
:license: MIT, see LICENSE for more details.
"""
import os
import sys

from setuptools import setup
from setuptools import find_packages
from setuptools.command.install import install

setup(name='c3po',
      version='SomeGiraffe',
      description='A python package for working with various services',
      url='http://github.com/mitchelllisle/c3po',
      author='Mitchell Lisle',
      author_email='m.lisle90@gmail.com',
      packages=find_packages(),
      license='MIT',
      install_requires=[
          'boto3',
          'pandas',
          'psycopg2',
          'numpy'
      ],
      python_requires='>=3',
      zip_safe=False)
