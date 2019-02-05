#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from setuptools import setup, find_packages

__pckg__ = 'aioflow'
__dpckg__ = __pckg__.replace('-', '_')
__version__ = "0.0.1"


def load_requirements():
    with open(os.path.join(os.getcwd(), "requirements.txt")) as requirements:
        return requirements.read().splitlines()


setup(name=__pckg__,
      version=__version__,
      description='A simple workflow implementation using asyncio.',
      author='Andrey Lemets',
      author_email='a.a.lemets@gmail.com',
      packages=find_packages(),
      include_package_data=True,
      install_requires=load_requirements(),
      license='MIT License')
