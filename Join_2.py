from __future__ import absolute_import

import argparse
import logging
import re
from builtins import next

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class DataIngestion:
    def parse_method(self, string_input):
        # Strip out carriage return, newline and quote characters.
        values = re.split(",",
                          re.sub('\r\n', '', re.sub(u'"', '', string_input)))
        row = dict(
            zip(('name', 'phone', 'address'),
                values))
        return row

data_ingestion = DataIngestion()

def run(argv=None, assert_results=None):

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_name',
      required=True,
      default='gs://data_files/Fake_name.csv',
      help='Email database, with each line formatted as "name<TAB>email".')
  parser.add_argument(
      '--input_phone',
      required=True,
      default='gs://data_files/Fake_phone.csv',
      help='Phonebook, with each line formatted as "name<TAB>phone number".')
  parser.add_argument(
      '--input_address',
      required=True,
      default='gs://data_files/Fake_address.csv',
      help='Address database, with each line formatted as "name<TAB>address".')
  parser.add_argument('--output',
                      required=True,
                      help='Tab-delimited output file.')

  known_args, pipeline_args = parser.parse_known_args(argv)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True


  p = beam.Pipeline(options=PipelineOptions(pipeline_args))


  # Helper: read a tab-separated key-value mapping from a text file,
  # escape all quotes/backslashes, and convert it a PCollection of
  # (key, value) pairs.
  def read_csvfile(label, csvfile):
      return (p
              | 'Read: ' % label >> ReadFromText(csvfile)
              | 'Parse input: ' >> beam.Map(lambda line: csv.reader([line]).next()))

  # Read input databases.
  name = read_csvfile('name', known_args.input_name)
  phone = read_csvfile('phone', known_args.input_phone)
  address = read_csvfile('address', known_args.input_address)

  # Group together all entries under the same name.
  grouped = (name, phone, address) | 'group_by_name' >> beam.CoGroupByKey()

  (p
   | 'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s))
   | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(
              known_args.grouped,
              schema='name:STRING,phone:STRING,address:STRING',
              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
              write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
  p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()