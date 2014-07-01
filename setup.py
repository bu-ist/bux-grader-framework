#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='bux-grader-framework',
    version='0.4.0',
    author='Boston University',
    author_email='webteam@bu.edu',
    url='https://github.com/bu-ist/bux-grader-framework/',
    description='An external grader framework for the edX platform.',
    long_description=open('README.md').read(),
    packages=['bux_grader_framework', 'bux_grader_test_framework'],
    scripts=['bin/grader', 'bin/list_queued_submissions', 'bin/fail_queued_submissions'],
    license='LICENSE',
    install_requires=[
        'requests>=2.0, <3.0',
        'pika>=0.9.12, <0.10',
        'statsd>=2.0, <3.0',
        'lxml>=3.3, <3.4'
        ]
)
