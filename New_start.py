import logging
import argparse
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText


def run(argv=None):
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--input_name', dest='input', required=False, default='gs://data_files/Fake_name.csv')
    #
    # parser.add_argument('--output', dest='output', required=False, default='lake.fake_data')
    #
    # known_args, pipeline_args = parser.parse_known_args(argv)

    p = beam.Pipeline(option=PipelineOptions)

    # class Transaction(beam.DoFn):
    #     def process(self, element):
    #         name, email = element.split(',')
    #         return [{'name': name, 'email': email}]

    class Split(beam.DoFn):
        def process(self, element):
            name, email = element.split(',')
            return [{
                'name': name,
                'email': email,
            }]

    class MapBigQueryRow(beam.DoFn):
        def process(self, element, key_column):
            key = element.get(key_column)
            yield key, element

    csv_lines = (p
                 | beam.io.ReadFromText('gs://data_files/Fake_name.csv')
                 | beam.ParDo(Split())
                 )

    read_query = """ SELECT name, job FROM `spikey-dataproc-238820.lake.fake_data`"""
    bq_source = beam.io.BigQuerySource(query=read_query, use_standard_sql=True)

    bq_data = (p
               | "Read From BigQuery" >> beam.io.Read(bq_source)
               | "Map to KV" >> beam.ParDo(MapBigQueryRow(), key_column='name')
               )

    co_grouped = ({'email': csv_lines, 'job': bq_data} | beam.CoGroupByKey())

    co_grouped | 'Write to BigQuery' >> beam.io.Write(
        beam.io.BigQuerySink(
            # The table name is a required argument for the BigQuery sink.
            # In this case we use the value passed in from the command line.
            table='fake_data',
            dataset='lake',
            project='spikey-dataproc-238820',
            # Here we use the simplest way of defining a schema:
            # fieldName:fieldType
            schema='name:STRING,job:STRING',
            # Creates the table in BigQuery if it does not yet exist.
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            # Deletes all data in the BigQuery table before writing.
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    p.run().wait_until_finish()

    # data = (p
    #         | 'Read Text File' >> ReadFromText('gs://data_files/Fake_name.csv')
    #         | 'Split the data' >> beam.Map(lambda x: x.split(',')[0])
    #         )

    # read_query = """ SELECT name, job FROM `spikey-dataproc-238820.lake.fake_data`"""
    # bq_source = beam.io.BigQuerySource(query=read_query, use_standard_sql=True)
    #
    # bq_data = (p
    #            | "Read From BigQuery" >> beam.io.Read(bq_source)
    #            | "Split the data" >> beam.Map(lambda row: (row['name'], row['job']))
    #            )

    # (p
    #  (data, bq_data) | 'Join the data' >> beam.CoGroupByKey()
    #  | 'Write to File' >> WriteToText('gs://data_files/join', '.csv')
    #  )

    # project_id = "spikey-dataproc-238820"  # replace with your project ID
    # dataset_id = 'lake'  # replace with your dataset ID
    # table_id = 'fake_data'  # replace with your table ID
    # table_schema = ('name:TIMESTAMP, email:STRING')

    # name_join | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
    #     table=table_id,
    #     dataset=dataset_id,
    #     project=project_id,
    #     schema=table_schema,
    #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    # )

    # p.run().wait_until_finish()
    # result = p.run()
    # result.wait_until_finish()


if __name__ == '__main__':
    logger = logging.getLogger().setLevel(logging.INFO)
    run()

# Run the code
# python main_file.py --key /path/to/the/key.json --project gcp_project_id
