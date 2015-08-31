#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

from setuptools import setup, find_packages

from s3util import __version__

install_requires = [
    "boto>=2.38.0",
    "filechunkio>=1.6.0"
]

setup(name="s3util",
      version=__version__,
      author="Xin Yin",
      author_email="killkeeper@gmail.com",
      description="AWS S3 (Simple Storage Service) Utilities",
      install_requires=install_requires,
      scripts=["bin/s3util"],
      packages=find_packages(),
      )
