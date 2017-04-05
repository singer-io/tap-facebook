#!/usr/bin/env python

from setuptools import setup

setup(name='tap-facebook',
      version='0.1.0',
      description='Singer.io tap for extracting data from the Facebook Marketing API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_facebook'],
      install_requires=[
          'singer-python>=0.4.0',
          'requests==2.12.4',
          'facebookads==2.8.1',
          'attrs==16.3.0',
      ],
      entry_points='''
          [console_scripts]
          tap-facebook=tap_facebook:main
      ''',
      packages=['tap_facebook'],
      package_data = {
          'tap_facebook/schemas': [
              # add schema.json filenames here
          ]
      },
      include_package_data=True,
)
