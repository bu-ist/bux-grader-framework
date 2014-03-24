#!/usr/bin/env python

from distutils.core import setup

setup(
    name='bux-grader-framework',
    version='0.1.0',
    author='Boston University',
    author_email='webteam@bu.edu',
    url='https://github.com/bu-ist/bux-grader-framework/',
    description='An external grader framework for the edX platform.',
    long_description=open('README.md').read(),
    packages=['bux_grader_framework'],
    scripts=['bin/grader'],
    license='LICENSE',
)
