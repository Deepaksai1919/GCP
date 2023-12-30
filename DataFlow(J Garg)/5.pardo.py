# Read the departments dataset and find the attendance (column 3) of employees in accoutns section
# Use only ParDo

import apache_beam as beam

class SplitString(beam.DoFn):
    def process(self, element):
        return [element.strip().split(',')]
    
class FilterAccounts(beam.DoFn):
    def process(self, element):
        return [element] if element[3] == 'Accounts' else None

class KeyValuePairs(beam.DoFn):
    def process(self, element):
        return [(element[1], int(element[2]))]

class Counting(beam.DoFn):
    def process(self, element):
        return [(element[0], sum(element[1]))]

with beam.Pipeline() as pipeline:
    (
        pipeline
        | 'Read the Input' >> beam.io.ReadFromText('datasets/dept_data.txt')
        | 'Split the string to list' >> beam.ParDo(SplitString())
        | 'Filter the employees in Accounts section' >> beam.ParDo(FilterAccounts())
        | 'Select the employee name and attendance' >> beam.ParDo(KeyValuePairs())
        | 'Group By Key' >> beam.GroupByKey()
        | 'Count' >> beam.ParDo(Counting())
        | 'Write result to output' >> beam.io.WriteToText('outputs/pardo/out')
    )
