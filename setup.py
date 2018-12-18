#!/usr/bin/env python
# coding=utf-8

from setuptools import setup, find_packages

setup(
    name='cdnn',
    version= '0.1',
    description=(
        'error data detact'
    ),
    long_description=open('README.rst').read(),
    author='fourcat',
    author_email='435612413@qq.com',
    maintainer='fourcat',
    maintainer_email='435612413@qq.com',
    license='BSD License',
    packages=find_packages(),
    platforms=["all"],
    url='https://github.com/antybody/cdnn.git',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: Implementation',
        'Programming Language :: Python :: 3.7',
        'Topic :: Software Development :: Libraries'
    ],
    install_requires=[
        'grpcio>=1.7.0',
        'numpy',
        'requests',
    ]
)