#!/usr/bin/env python

from setuptools import find_packages, setup

with open('README.md') as f:
    long_description = f.read()

setup(name="pipelinewise-target-snowflake",
      version="1.16.0",
      description="Singer.io target for loading data to Snowflake - PipelineWise compatible",
      long_description=long_description,
      long_description_content_type='text/markdown',
      author="Wise",
      url='https://github.com/transferwise/pipelinewise-target-snowflake',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Python :: 3.10',
          'Programming Language :: Python :: 3.11',
      ],
      py_modules=["target_snowflake"],
      install_requires=[
          'pipelinewise-singer-python==1.*',
          'snowflake-connector-python[pandas]==3.13.1',
          'cryptography>=44.0.1,<45',
          'inflection==0.5.1',
          'joblib==1.2.0',
          'numpy>=1.23.2,<2',
          'pyarrow>=23.0.1,<24; python_version >= "3.10"',
          'pyarrow<22; python_version < "3.10"',
          'boto3==1.20.*',
          'azure-storage-blob==2.1.0',
      ],
      extras_require={
          "test": [
              "pylint==2.12.*",
              'pytest==6.2.5',
              'pytest-cov==3.0.0',
              "python-dotenv==0.19.*"
          ]
      },
      entry_points="""
          [console_scripts]
          target-snowflake=target_snowflake:main
      """,
      packages=find_packages(exclude=['tests*']),
      )
