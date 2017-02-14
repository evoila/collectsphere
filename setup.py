#!/usr/bin/env python

from setuptools import setup

setup(
    name='collectsphere',
    version='1.0.0',
    description='VMware performance metric collector for collectd',
    author='evoila GmbH',
    author_email='info@evoila.de',
    url='evoila.de',
    license='MIT',
    setup_requires=['setuptools>=17.1'],
    install_requires=['pyVmomi', 'tzlocal'],
    py_modules=['collectsphere'],

)