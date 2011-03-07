#!/usr/bin/env python
from distutils.core import setup


setup(
    name = 'crew.metrics.aggregator',
    version = '0.1',
    packages = ['crew', 'crew.aggregator', 'crew.aggregator.linux'],
    author = 'Crew',
    author_email = 'crew@ccs.neu.edu',
    description = 'Crew Metrics aggregators',
    keywords = 'crew',
    scripts = ['scripts/windows.py', 'scripts/linux.py'],
)
