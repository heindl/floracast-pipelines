# !/usr/bin/env python
# encoding: utf-8

import apache_beam as beam

from florecords.occurrences.fetch import FetchOccurrences
from florecords.occurrences.sync import FetchOccurrenceSyncHistory
from florecords.occurrences.names import ScientificNameParser
from florecords.features.request import FeatureRequest
from florecords.backport import timestamp_from_date, timestamp
from apache_beam.metrics import Metrics
from apache_beam.transforms.util import BatchElements
import apache_beam.transforms.combiners as combine
import json
# import datetime

# class PrintNameCount(beam.PTransform):
#
#     def _print(self, names):
#         for n in names:
#             print(n)
#
#     def expand(self, pcoll):
#         return (pcoll
#                 | 'NameAsKeyForPrint' >> beam.Map(lambda x: (x.name, 1))
#                 | 'GroupByGivenScientificNameForPrint' >> beam.GroupByKey()
#                 | 'CountScientificNames' >> beam.Map(lambda (name, ones): (name, sum(ones)))
#                 | 'CombineTopScientificNames' >> combine.Top.Of(1000, lambda a, b: a[1] < b[1])
#                 | 'PrintTopScientificNames' >> beam.Map(self._print))
#
# class RecordNameDistribution(beam.PTransform):
#     def expand(self, pcoll):
#         return (pcoll
#                 | 'pair_with_one' >> beam.Map(lambda x: (x.name, 1))
#                 | 'group' >> beam.GroupByKey()
#                 | 'count' >> beam.Map(lambda (name, ones): (name, sum(ones))))


class _QueueOccurrenceSources(beam.DoFn):
    def __init__(self, project):
        super(_QueueOccurrenceSources, self).__init__()
        self._project = project
    def process(self, source):
        for doc in FetchOccurrenceSyncHistory(project=self._project, source=source):
            yield doc

class _FetchOccurrences(beam.DoFn):
    def __init__(self):
        super(_FetchOccurrences, self).__init__()
        self.occurrence_counter = Metrics.counter(self.__class__, 'Occurrences Fetched')

    def process(self, sync):

        for o in FetchOccurrences(FetchParams(
                observed_after=timestamp_from_date(2015, 1, 1),
                observed_before=timestamp_from_date(2019, 1, 1),
                updated_after=sync.fetched_at,
                family=sync.family
        ), [sync.source]):
            self.occurrence_counter.inc()
            yield o

class _NormalizeNames(beam.DoFn):
    def __init__(self, project):
        super(_NormalizeNames, self).__init__()
        self._project = project
        self.name_fetch_counter = Metrics.counter(self.__class__, 'Names Fetched')
    def process(
            self,
            (given_name, occurrence_list), # type: tuple[str, list[Occurrence]]
    ):
        parser = ScientificNameParser(self._project)
        # Occurrences expected to be grouped by name, so all names should be the same.
        scientific_name = parser.parse(given_name)
        self.name_fetch_counter.inc()
        for o in occurrence_list:
            assert o.given_name() == given_name # Just to be safe
            o.set_scientific_name(scientific_name)
            yield o

class _FilterOccurrenceGroups(beam.DoFn):
    def __init__(self, threshold):
        self._threshold = threshold
        self._c_taxa = Metrics.counter(self.__class__, 'ValidOccurrenceTaxaCount')
        self._c_occurrences = Metrics.counter(self.__class__, 'ValidOccurrenceCount')

    def process(self, (name, occurrences)):
        occurrences = list(occurrences)
        has_count = len(occurrences) > self._threshold
        has_name = len(name.split()) > 1
        if has_count and has_name:
            self._c_taxa.inc()
            for o in occurrences:
                self._c_occurrences.inc()
                yield o

def unwind_requests(x):
    for cell_id in x.decompose():
        yield FeatureRequest(
            observed_at=x.observed_at(),
            cell_id=cell_id,
            occurrence_ids=[x.source_id()],
        )

def format_for_write(o):
    j = o.to_bigquery()
    if j is not None:
        yield json.dumps(j)

class FetchOccurrencePipeline(beam.PTransform):

    def __init__(self, project, occurrence_count_threshold=200):
        super(FetchOccurrencePipeline, self).__init__()
        self._project = project
        self._occurrence_count_threshold = occurrence_count_threshold

    def expand(self, p):
        # | 'BeginOccurrenceFetch' >> beam.Create(['idigbio', 'inaturalist', 'gbif', 'mycoportal', 'mushroomobserver']) \

        occurrence_sources = p \
            | 'BeginOccurrenceFetch' >> beam.Create(['idigbio', 'inaturalist', 'gbif', 'mycoportal', 'mushroomobserver']) \
           | 'QueueOccurrenceSources' >> beam.ParDo(_QueueOccurrenceSources(project=self._project))

        occurrences = occurrence_sources | 'FetchOccurrences' >> beam.ParDo(_FetchOccurrences())

        occurrences = occurrences \
          | 'ProjectGivenScientificNameAsKey' >> beam.Map(lambda x: (x.given_name(), x)) \
          | 'GroupOccurrencesByScientificName' >> beam.GroupByKey() \
          | 'NormalizeScientificNames' >> beam.ParDo(_NormalizeNames(project=self._project))

        _ = occurrences | 'FormatForWrite' >> beam.FlatMap(format_for_write) \
                        | 'WriteJSONToGCS' >> beam.io.WriteToText(
                                                file_path_prefix='gs://pyflora/occurrence_upload/v2/',
                                                file_name_suffix='.json'
                                          )

        return occurrences

        # occurrences = occurrences \
        #        | 'ProjectNameForTaxaOccurrenceThreshold' >> beam.Map(lambda x: (x.scientific_name(), x)) \
        #        | 'GroupByNameForOccurrenceThreshold' >> beam.GroupByKey() \
        #        | 'FilterOccurrenceGroupsByThreshold' >> beam.ParDo(
        #             _FilterOccurrenceGroups(self._occurrence_count_threshold)
        #         )
        #
        # occurrences = occurrences \
        #        | 'OccurrencesToRequests' >> beam.FlatMap(unwind_requests) \
        #        | 'ProjectCellID' >> beam.Map(lambda x: (x.cell_id(), x)) \
        #        | 'GroupByCellID' >> beam.GroupByKey() \
        #        | 'UnwindRequests' >> beam.Map(lambda (cell_id, requests): list(requests)[0])
        #
        # return occurrences \
        #         | 'BatchRequests' >> BatchElements(min_batch_size=50, max_batch_size=100) \
        #         | 'TruncateBatchesIntoOne' >> combine.Top.Of(
        #             3,
        #             lambda a, b: True
        #         ) \
        #         | 'UnwindTruncated' >> beam.FlatMap(lambda (x): x)

