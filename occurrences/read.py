#!/usr/bin/env python
# encoding: utf-8

import apache_beam as beam
from apache_beam.metrics import Metrics

_occurrence_query = """
    SELECT * FROM (
      SELECT *, ROW_NUMBER() OVER(PARTITION BY id, source ORDER BY timestamp DESC) AS r
      FROM Floracast.Occurrences
    ) WHERE r = 1
"""

_valid_name_query = """
SELECT scientific_name FROM (
  SELECT scientific_name, COUNT(1) AS name_count FROM (
    SELECT scientific_name, ROW_NUMBER() OVER(
      PARTITION BY source_id ORDER BY created_at DESC
    ) AS r
    FROM Floracast.Occurrences
  )  
  WHERE r = 1 
  GROUP BY scientific_name
) WHERE name_count >= 200
"""



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

class ReadOccurrences(beam.PTransform):

    def __init__(self, project, occurrence_count_threshold=200):
        super(ReadOccurrences, self).__init__()
        self._project = project
        self._occurrence_count_threshold=occurrence_count_threshold

    def expand(self, p):

        occurrences = p | 'ReadOccurrencesfromBigQuery' >> beam.io.Read(beam.io.BigQuerySource(
            project=self._project,
            query=_occurrence_query.format()
        ))

        occurrences = occurrences | 'DeduplicateByName&GeoSpatialKey' >> beam.Map(
                lambda x: ((
                           x['name'],
                           x['latitude'],
                           x['longitude'],
                           x['coordinate_uncertainty'],
                           x['date']
                       ), x)
                ) \
                     | 'GroupByName&GeoSpatialKey' >> beam.GroupByKey() \
                     | 'SelectFirstOccurrenceFromDeduplicationGroup' >> beam.Map(lambda x: list(x[1])[0])

        # Group by name and filter taxa with too few occurrences.
        # Eventually maybe select the top N taxa. But for now it is irrelevant.
        return occurrences | 'ProjectNameForTaxaOccurrenceThreshold' >> beam.Map(lambda x: (x['name'], x)) \
              | 'GroupByNameForOccurrenceThreshold' >> beam.GroupByKey() \
              | 'FilterOccurrenceGroupsByThreshold' >> beam.ParDo(
                    _FilterOccurrenceGroups(self._occurrence_count_threshold)
                )
