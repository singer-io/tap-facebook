#!/usr/bin/env python

from setuptools import setup

setup(name='tap-facebook',
      version='1.16.0',
      description='Singer.io tap for extracting data from the Facebook Ads API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_facebook'],
      install_requires=[
          'attrs==17.3.0',
          'backoff==1.8.0',
          'pendulum==1.2.0',
          'facebook_business==12.0.0',
          'requests==2.20.0',
          'singer-python==5.10.0',
      ],
      extras_require={
          'dev': [
              'pylint',
              'ipdb',
              'nose',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-facebook=tap_facebook:main
      ''',
      packages=['tap_facebook'],
      package_data = {
          'tap_facebook/schemas': [
              # add schema.json filenames here
          ],
          'tap_facebook/schemas/shared': [
          ]
      },
      include_package_data=True,
)
