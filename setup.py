#!/usr/bin/env python

from setuptools import setup, find_packages

from riakcached import __version__

setup(
    name="riakcached",
    version=__version__,
    author="Brett Langdon",
    author_email="brett@blangdon.com",
    packages=find_packages(),
    install_requires=["urllib3==1.7"],
    setup_requires=["nose>=1.0"],
    description="A Memcached like interface to Riak",
    license="MIT",
    url='https://github.com/brettlangdon/riakcached',
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "License :: OSI Approved :: MIT License",
        "Topic :: Database",
    ],
)
