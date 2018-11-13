#!/usr/bin/env python
# encoding: utf-8

from apache_beam.io import iobase
from apache_beam.transforms import PTransform
from apache_beam.io import range_trackers
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.options.value_provider import ValueProvider
from records.occurrences.sync import FetchOccurrenceSyncHistory

from records.occurrences.fetchers.utils import FetchParams
from records.occurrences.fetch import FetchOccurrences
from apache_beam.metrics import Metrics

# class _QueueOccurrenceSources(beam.DoFn):
#     def __init__(self, project):
#         super(_QueueOccurrenceSources, self).__init__()
#         self._project = project
#     def process(self, i):
#         for doc in FetchOccurrenceSyncHistory(project=self._project):
#             yield doc


class FetchOccurrences(PTransform):

    def __init__(self, project_id):
        super(FetchOccurrences, self).__init__()
        self._project_id = project_id

    def expand(self, pcoll):
        return pcoll | iobase.Read(_OccurrenceSync(self._project_id))

MONTH_IN_SECONDS = 30 * 43800 * 60

# class _OccurrenceSource(iobase.BoundedSource):
#     def __init__(self,
#                  source,
#                  family,
#                  modified_after,
#                  modified_before=None,
#                  observed_after=None,
#                  observed_before=None,
#         ):
#         self.occurrence_counter = Metrics.counter(self.__class__, 'OccurrencesFetched')
#         self._source = source
#         self._family = family
#         self._modified_after = modified_after
#         self._modified_before = modified_before
#         self._observed_after = observed_after
#         self._observed_before = observed_before
#
#     def read(self, range_tracker):
#         for o in FetchOccurrences(FetchParams(
#                 observed_after=self._observed_after,
#                 observed_before=self._observed_before,
#                 updated_before=self._modified_before,
#                 updated_after=self._modified_after,
#                 family=self._family
#         ), [self._source]):
#             self.occurrence_counter.inc()
#             yield o

class _OccurrenceSync(iobase.BoundedSource):

    def __init__(self,
                 project_id,
                 modified_before=None,
                 observed_after=None,
                 observed_before=None,
                 restrict_sources=None,
                 restrict_families=None,
         ):
        self.occurrence_source_counter = Metrics.counter(self.__class__, 'OccurrenceSources')
        self.occurrence_counter = Metrics.counter(self.__class__, 'OccurrencesFetched')


        if not isinstance(project_id, (basestring, ValueProvider)):
            raise TypeError('%s: file_pattern must be of type string'
                            ' or ValueProvider; got %r instead'
                            % (self.__class__.__name__, project_id))

        if isinstance(project_id, basestring):
            project_id = StaticValueProvider(str, project_id)

        self._project_id = project_id
        self._observed_before = observed_before
        self._observed_after = observed_after
        self._modified_before = modified_before
        self._restrict_sources = restrict_sources
        self._restrict_families = restrict_families

        self._sync_records = None

    def _fetch_sync_history(self):

        print('fetching history')

        project_id = self._project_id.get()
        if project_id is None or len(str(project_id)) == 0:
            raise ValueError("Invalid GCloud Project ID")

        if self._sync_records is not None:
            return self._sync_records

        print('actual fetch')

        self._sync_records = FetchOccurrenceSyncHistory(
            project=project_id,
            restrict_sources=self._restrict_sources,
            restrict_families=self._restrict_families
        )

        return self._sync_records


    def estimate_size(self):
        return len(self._fetch_sync_history())

    def get_range_tracker(self, start_position, stop_position):
        # if start_position is None:
        #     start_position = 0
        # if stop_position is None:
        #     stop_position = self._count

        history = self._fetch_sync_history()

        return range_trackers.OrderedPositionRangeTracker(0, len(history))

    def read(self, range_tracker):

        for i in range(range_tracker.stop_position()):
            if not range_tracker.try_claim(i):
                return
            self.occurrences_read.inc()

            for o in FetchOccurrences(FetchParams(
                    observed_after=self._observed_after,
                    observed_before=self._observed_before,
                    updated_before=self._modified_before,
                    updated_after=self._modified_after,
                    family=self._family
            ), [self._source]):
                self.occurrence_counter.inc()
                yield o

    def split(self, desired_bundle_size=None, start_position=None,stop_position=None):

        print('splitting', desired_bundle_size, start_position, stop_position)

        yield None

        # if start_position is None:
        #     start_position = 0
        # if stop_position is None:
        #     stop_position = self._count



        # bundle_start = start_position
        # while bundle_start < self._count:
        #     bundle_stop = max(self._count, bundle_start + desired_bundle_size)
        #     yield iobase.SourceBundle(weight=(bundle_stop - bundle_start),
        #                               source=self,
        #                               start_position=bundle_start,
        #                               stop_position=bundle_stop)
        #     bundle_start = bundle_stop
