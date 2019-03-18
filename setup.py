#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

__pckg__ = "aioflow"
__dpckg__ = __pckg__.replace("-", "_")
__version__ = "0.0.9"

setup(
    name=__pckg__,
    version=__version__,
    description="A simple workflow implementation using asyncio.",
    author="Andrey Lemets",
    author_email="a.a.lemets@gmail.com",
    packages=find_packages(),
    include_package_data=True,
    license="MIT License",
    install_requires=[
        "pyyaml==3.13",
    ],
    extras_require={
        "tests": [
            "pytest==4.2.0",
            "pytest-cov==2.6.1",
            "pytest-asyncio==0.10.0",
        ],
        "aioredis": [
            "aioredis==1.2.0",
        ],
    }
)
