#!/usr/bin/env python

from setuptools import setup

setup(name='tap-facebook',
      version='0.1.7',
      description='Singer.io tap for extracting data from the Facebook Ads API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_facebook'],
      install_requires=[
          'attrs==16.3.0',
          'backoff==1.4.0',
          'facebookads==2.8.1',
          'pendulum==1.2.0',
          'requests==2.12.4',
          'singer-python==1.6.0',
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
