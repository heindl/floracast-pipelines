import setuptools
import inspect
import os
from setuptools.command.build_py import build_py as _build_py
import subprocess

if __name__ == '__main__':

    setuptools.setup(
        name='FloracastRecords',
        version='0.0.2',
        description='Record Aggregation Pipeline for Floracast',
        packages=setuptools.find_packages(),
        url="https://bitbucket.org/heindl/florabeam",
        author="mph",
        author_email="matt@floracast.com",
    )

# "google-cloud==0.32.0",
# "tensorflow==1.7.0",
# "tensorflow-transform==0.6.0",
# "astral==1.6",
# "geopy==1.13.0",
# "numpy==1.14.2",
# "pandas==0.22.0",
# "scikit-learn==0.19.1",
# "google-cloud-bigquery==1.1.0"
