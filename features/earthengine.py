#!/usr/bin/env python
# encoding: utf-8

import apache_beam as beam
from florecords.features.generator import BaseFeatureGenerator
from florecords.occurrences import Occurrence
from apache_beam.metrics.metric import Metrics
from apache_beam import pvalue
import os, logging




# class OccurrenceBigQueryEncoder(beam.coders.Coder):
#
#     def __init__(self, table_schema=None):
#         # The table schema is needed for encoding TableRows as JSON (writing to
#         # sinks) because the ordered list of field names is used in the JSON
#         # representation.
#         self.table_schema = table_schema
#         # Precompute field names since we need them for row encoding.
#         if self.table_schema:
#             self.field_names = tuple(fs.name for fs in self.table_schema.fields)
#
#     def encode(self, occurrence): # type: (OccurrenceCompiler) -> str
#         print('encode', occurrence)
#         try:
#             return json.dumps(occurrence.encode(), allow_nan=False)
#         except ValueError as e:
#             raise ValueError('%s. %s' % (e, 'NAN, INF and -INF values are not JSON compliant.'))
#
#     def decode(self, encoded_table_row): # type: (str) -> OccurrenceCompiler
#         print('encoded', encoded_table_row)
#         return OccurrenceCompiler.decode(
#             json.loads(
#                 encoded_table_row,
#                 object_pairs_hook=collections.OrderedDict
#             )
#         )

class EarthEngineFeaturePipeline(beam.PTransform):

    def __init__(self, generator, project): # type: (BaseFeatureGenerator, str) -> None

        super(EarthEngineFeaturePipeline, self).__init__()

        logging.info('PrintAllEnvironmentVariables: %s' % ','.join(os.environ.keys()))

        self._generator = generator

        self._features_fetched = Metrics.counter(self.__class__, '%sEarthEngineRecordsFetched' % self._generator.table_name())
        self._complete_counter = Metrics.counter(self.__class__, 'Complete%sEarthEngineRecords' % self._generator.table_name())
        self._incomplete_counter = Metrics.counter(self.__class__, 'Incomplete%sEarthEngineRecords' % self._generator.table_name())

        self._group_counter = Metrics.counter(self.__class__, '%sTokenGroups' % self._generator.table_name())

        self._project = project

    def _count_groups(self, g):
        self._group_counter.inc()
        return g

    def _partition_fn(self, occurrence, partitions): # type: (Occurrence) -> int
        if self._generator.is_complete(occurrence):
            self._complete_counter.inc()
            return 1
        else:
            self._incomplete_counter.inc()
            return 0

    def _node_label(self, s):
        return '%s, %s' % (s, self._generator.table_name())

    def _fetch(self, (key, docs)):
        res = self._generator().fetch(list(docs))
        for r in res.bigquery_records:
            self._features_fetched.inc()
            yield pvalue.TaggedOutput('bigquery_records', r)
        for o in res.occurrences:
            yield pvalue.TaggedOutput('occurrences', o)

    def expand(self, pipeline):

        occurrences = pipeline | self._node_label('Read&JoinRequestsFromBigQuery') >> beam.io.Read(beam.io.BigQuerySource(
                    table=None,
                    dataset='Floracast',
                    project=self._project,
                    query=self._generator.query('OccurrencesV3'),
                    validate=True,
                    use_standard_sql=True,
                    flatten_results=True
                )) | 'DecodeOccurrence' >> beam.Map(lambda o: Occurrence.decode(o))

        # Read from BigQuery for the features we already have.
        # _ Ensure all nulled values are converted to a known type\
        # (numpy.nan?) so as not to try and refetch.
        incomplete, complete = occurrences \
                | self._node_label('PartitionRequests') >> beam.Partition(self._partition_fn, 2)

        incomplete_request_groups = incomplete \
                | self._node_label('KeyIncompleteRequests') >> beam.Map(lambda r: (self._generator.partition_key(r), r)) \
                | self._node_label('GroupIncompleteRequestsByKey') >> beam.GroupByKey() \
                | self._node_label('CountGroups') >> beam.Map(self._count_groups)

        # For each location group, fetch that particular feature.
        bigquery_records, occurrence_responses = incomplete_request_groups \
            | self._node_label('FetchFromEarthEngineSource') >> beam.FlatMap(self._fetch).with_outputs(
                'bigquery_records', 'occurrences'
            )

        # Write new points to BigQuery.
        _ = bigquery_records \
            | self._node_label('WriteFeaturesToBigQuery') >> beam.io.WriteToBigQuery(
                table=self._generator.table_name(),
                dataset='Floracast',
                project=self._project,
                # schema=self._generator.schema(),
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )

        # # Combine records from BigQuery and newly generated.
        return  (complete, occurrence_responses) \
                | self._node_label('FlattenNew&OldFeatures') >> beam.Flatten() \
                | self._node_label('KeyCompletedFeatures') >> beam.Map(lambda r: ((r.source_id(), r.source_key()), r)) \
                | self._node_label('GroupCompletedFeaturesIntoOccurrences') >> beam.GroupByKey()
        #         | self.
            #  Possibly as a side input, combine all records by (date, location) with original points (date, location).

        # For each group, for each feature, sort by date or -t and combine values into a vector.

        # Emit fullfilled feature arrays: (date, location, feature_vector1, feature_vector2)
        # _ Easy to filter invalid values here and only yield those of sufficient length,
        #   though maybe wait to the end. But here we could attach known requirements to each feature.
