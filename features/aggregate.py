#!/usr/bin/env python
# encoding: utf-8

import apache_beam as beam
from .earthengine import EarthEngineFeaturePipeline
from florecords.features import Landcover
# from apache_beam.metrics.metric import Metrics

# class _CombineOccurrencesIntoFeatureRequests(beam.PTransform):
#     def __init__(self):
#         super(_CombineOccurrencesIntoFeatureRequests, self).__init__()
#         self._c_requests = Metrics.counter(self.__class__, 'FeatureRequestCount')
#
#     def _squash_request_group(self, (
#         temporal_geospatial_key, # type: Tuple(float, float, int, str)
#         occurrence_group # type: List[Occurrence]
#     )):
#         self._c_requests.inc()
#         return FeatureRequest(
#             observation_date=temporal_geospatial_key[3],
#             latitude=temporal_geospatial_key[0],
#             longitude=temporal_geospatial_key[1],
#             coordinate_uncertainty=temporal_geospatial_key[2],
#             occurrence_keys=[o.key_id() for o in list(occurrence_group)]
#         )
#
#     def expand(self, p):
#         return p \
#            | 'ProjectDateCoordKey' >> beam.Map(lambda o: (o.key_temporal_geospatial(), o)) \
#            | 'GroupRequestsByDateCoordKey' >> beam.GroupByKey() \
#            | 'SquashRequestGroups' >> beam.Map(self._squash_request_group)


class FeaturePipeline(beam.PTransform):
    def __init__(self, project):
        super(FeaturePipeline, self).__init__()
        self._project = project

    def expand(self, pipeline):

        # Group occurrences by geospatial key into feature requests.
        # request_pipe = pipeline | beam.Create[feature_source_pipe \
        #                | 'CombineOccurrencesIntoFeatureRequests' >> _CombineOccurrencesIntoFeatureRequests() \


        # Initial output to new pipeline is (location[lat, lng, decimal], date)
        # Break location into bounding box string?
        # - The question is how earth engine will generate results.
        # - Min is 30m resolution and max is around 1km.
        # _ Probably best then to buffer everything by minimum 30m.
        # - A benefit of doing it here is that we can normalize coordinate resolution,
        #   for elements that do not have coordinate_precision.
        # _ Worried that reading them out all at once will not work at a terabyte scale,
        #   so will need some kind of bigquery partition, or use BigQuery for the combining.
        # _ gsod2017 is 775MB, with 4,293,349 rows and 30 columns. Reading cost is $5 per terabyte (1000000mb)
        # _ Meaning I could read that table 1,290 for $5. So for now the expensive but simple option is best.


        return pipeline | EarthEngineFeaturePipeline(generator=Landcover, project=self._project)

        # 5. Combine all results on (date, location) for original occurrences.
        # Probably use side-input because there will be more than one occurrence sometimes for each feature,
        # as many labels share a location, and add a dictionary key for each feature.
