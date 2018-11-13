#!/usr/bin/env python
# encoding: utf-8

import sys
import os

import apache_beam as beam
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions, StandardOptions, SetupOptions
# from pipelines.occurrences.fetch import FetchOccurrencePipeline
from features.aggregate import FeaturePipeline

import pprint
# import os
# import subprocess

# def default_project():
#     get_project = [
#         'gcloud', 'config', 'list', 'project', '--format=value(core.project)'
#     ]
#
#     with open(os.devnull, 'w') as dev_null:
#         return subprocess.check_output(get_project, stderr=dev_null).strip()

def _print_metrics(res):
    final = {}
    for r in res.metrics().query()['counters']:
        final[r.key.metric.name.encode('ascii')] = int(r.committed)
    pprint.pprint(final)


def _pipeline(argv=None):

    options = PipelineOptions(flags=argv)
    # ['--setup_file', os.path.abspath(os.path.join(os.path.dirname(__file__), 'setup.py'))],
    # )
    cloud_options = options.view_as(GoogleCloudOptions)
    cloud_options.project = "floracast-firestore"
    standard_options = options.view_as(StandardOptions)
    options.view_as(SetupOptions).save_main_session = True

    pipe = beam.Pipeline(standard_options.runner, options=options)
    return pipe, cloud_options.project

def run(argv=None):

    pipe, project = _pipeline(argv)

    # requests = pipe | FetchOccurrencePipeline(project)

    requests = pipe | FeaturePipeline(project)

    result = pipe.run()
    result.wait_until_finish()
    _print_metrics(result)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
