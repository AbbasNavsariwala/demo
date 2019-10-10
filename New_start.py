import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText

def run(argv=None):

    p = beam.Pipeline(option=PipelineOptions)

    class Transaction(beam.DoFn):
        def process(self, element):
            name, email = element.split(',')
            return [{'name': name, 'email': email}]

    data = (p
            | 'Read Text File' >> ReadFromText('gs://data_files/Fake_name.csv')
            | 'Split the data' >> beam.Map(lambda x: x.split(',')[0])
            )

    read_query = """ SELECT name, job FROM `spikey-dataproc-238820.lake.fake_data`"""
    bq_source = beam.io.BigQuerySource(query=read_query, use_standard_sql=True)

    bq_data = (p
                 | "Read From BigQuery" >> beam.io.Read(bq_source)
                 | "Split the data" >> beam.Map(lambda row: (row['name'], row['job']))
                 )

    name_join = (p
                 |'Join the Data' >> (data, bq_data)beam.CoGroupByKey())



    # project_id = "spikey-dataproc-238820"  # replace with your project ID
    # dataset_id = 'lake'  # replace with your dataset ID
    # table_id = 'fake_data'  # replace with your table ID
    # table_schema = ('name:TIMESTAMP, email:STRING')

    name_join | 'Write to File' >> beam.write('gs://data_files/join.csv')

    # name_join | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
    #     table=table_id,
    #     dataset=dataset_id,
    #     project=project_id,
    #     schema=table_schema,
    #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    # )

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logger = logging.getLogger().setLevel(logging.INFO)
    run()

# Run the code
# python main_file.py --key /path/to/the/key.json --project gcp_project_id