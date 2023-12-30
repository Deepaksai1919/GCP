# Read the departments dataset and find the attendance (column 3) of employees in accoutns section

import apache_beam as beam

pipeline = beam.Pipeline()

PCollection = (
    pipeline
    | 'Read the Input' >> beam.io.ReadFromText('datasets/dept_data.txt')
    | 'Split the string to list' >> beam.Map(lambda record: record.strip().split(','))
    | 'Filter the employees in Accounts section' >> beam.Filter(lambda record: record[3] == 'Accounts' )
    | 'Select the employee name and attendance' >> beam.Map(lambda record: (record[1], int(record[2])))
    | 'Combine Per Key' >> beam.CombinePerKey(sum)
    | 'Write result tot output' >> beam.io.WriteToText('outputs/problem1/out')
)

pipeline.run()
