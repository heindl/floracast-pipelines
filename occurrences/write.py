#!/usr/bin/env python
# encoding: utf-8

import apache_beam as beam
from records.occurrences.compiler import OccurrenceCompiler
from apache_beam.io.gcp.internal.clients import bigquery

def _parse_schema(fields):
    table_schema = bigquery.TableSchema()
    for f in fields:
        s = bigquery.TableFieldSchema()
        s.name = f[0]
        s.type = f[1]
        s.mode = f[2]
        table_schema.fields.append(s)
    return table_schema

class WriteOccurrencesToGCS(beam.Transform):
    def __init__(self, project):
        super(WriteOccurrencesToGCS, self).__init__()
        self._project = project

    def expand(self, p):

        p = p | 'FormatOccurrencesToWrite' >> beam.Map(lambda x: x.to_bigquery())

        _ = p | 'WriteOccurrencesToBigQuery' >> beam.io.WriteToBigQuery(
            table='Occurrences', # Output BigQuery table for results specified as: DATASET.TABLE
            dataset='Floracast',
            project=self._project,
            schema=_parse_schema(OccurrenceCompiler.schema()),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


class WriteOccurrencesToBigQuery(beam.PTransform):

    def __init__(self, project):
        super(WriteOccurrencesToBigQuery, self).__init__()
        self._project = project

    def expand(self, p):

        p = p | 'FormatOccurrencesToWrite' >> beam.Map(lambda x: x.to_bigquery())

        _ = p | 'WriteOccurrencesToBigQuery' >> beam.io.WriteToBigQuery(
            table='Occurrences', # Output BigQuery table for results specified as: DATASET.TABLE
            dataset='Floracast',
            project=self._project,
            schema=_parse_schema(OccurrenceCompiler.schema()),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
