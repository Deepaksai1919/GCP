import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

pipeline_config = {
    'project': 'dataflow-392201',
    'runner': 'DataFlowRunner',
    'region': 'asia-south1',
    'staging_location': 'gs://dataflow-course-392201/staging',
    'temp_location': 'gs://dataflow-course-392201/temp',
    'template_location': 'gs://dataflow-course-392201/templates/batch_job_gcs_flights'
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_config)

class Filter(beam.DoFn):
  def process(self, record):
    if int(record[8]) > 0:
      return [record]

pipeline = beam.Pipeline(options = pipeline_options)

flight_delay_pc_kv = (
    pipeline
    | 'Import Data' >> beam.io.ReadFromText('gs://dataflow-course-392201/inputs/flights_sample.csv')
    | 'Split by ,' >> beam.Map(lambda record: record.split(','))
    | 'Filter Delays' >> beam.ParDo(Filter())
    | 'Create Key-Value pair' >> beam.Map(lambda record: (record[4], int(record[8])))
)

total_delayed_time = (
    flight_delay_pc_kv
    | 'Combine Per Key' >> beam.CombinePerKey(sum)
)

count_delayed_time = (
    flight_delay_pc_kv
    | 'Count Per Key' >> beam.combiners.Count.PerKey()
)

delay_table = (
    {'delayed_num': count_delayed_time, 'delayed_time': total_delayed_time}
    | beam.CoGroupByKey()
    | beam.io.WriteToText('gs://dataflow-course-392201/outputs/flights_output.csv')
)

pipeline.run()