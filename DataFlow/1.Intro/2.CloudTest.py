# For authentication, service account is used and the path of key file is stored in
# Env variable as GOOGLE_APPLICATION_CREDENTIALS

import apache_beam as beam

class Filter(beam.DoFn):
  def process(self, record):
    if int(record[8]) > 0:
      return [record]

pipeline = beam.Pipeline()

flight_delay_pc_kv = (
    pipeline
    | 'Import Data' >> beam.io.ReadFromText('../Basics/flights_sample.csv')
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