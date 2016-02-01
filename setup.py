#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name = 'muse-tool',
      author = 'Shenglai Li',
      author_email = 'sli6@uchicago.edu',
      version = 0.1,
      description = 'Baylor MuSE tools',
      url = 'https://github.com/NCI-GDC/muse-tool',
      license = 'Apache 2.0',
      packages = find_packages(),
      install_requires = [
          'pandas',
          'sqlalchemy',
          'psycopg2'
      ],
      classifiers = [
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 3',
      ],
)
