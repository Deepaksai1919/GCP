# Read the departments dataset and find the attendance (column 3) of employees in accounts and HR sections
# Filter where attendance >= 365

import apache_beam as beam

def method1():
    def map_and_print(PCollection, section):
        (
            PCollection
            | f'{section} Select the employee name and attendance' >> beam.Map(lambda record: (record[1], int(record[2])))
            | f'{section} Combine Per Key' >> beam.CombinePerKey(sum)
            | f'{section} Filter the attendance' >> beam.Filter(lambda record: record[1] >= 365)
            | f'{section} Write result to output' >> beam.Map(print)
        )

    with beam.Pipeline() as pipeline:
        Records = (
            pipeline
            | 'Read the Input' >> beam.io.ReadFromText('datasets/dept_data.txt')
            | 'Split the string to list' >> beam.Map(lambda record: record.strip().split(','))
        )

        Accounts = (
            Records
            | 'Filter the employees in Accounts section' >> beam.Filter(lambda record: record[3] == 'Accounts' )
        )

        Hr = (
            Records
            | 'Filter the employees in HR section' >> beam.Filter(lambda record: record[3] == 'HR' )
        )
        map_and_print(Accounts, 'accounts')
        map_and_print(Hr, 'hr')

def method2():
    """
        This is the composite transform. It is used for defining a custom transform.
    """
    class MyTransform(beam.PTransform):
        """
            Overide the expand method
        """
        def expand(self, inputPCollection):
            """
                Accepts a PCollection and returns a PCollection
            """
            transformed_PCollection = (
                inputPCollection
                | 'Select the employee name and attendance' >> beam.Map(lambda record: (record[1], int(record[2])))
                | 'Combine Per Key' >> beam.CombinePerKey(sum)
                | 'Filter the attendance' >> beam.Filter(lambda record: record[1] >= 365)
                | 'Write result to output' >> beam.Map(print)
            )
            return transformed_PCollection

    with beam.Pipeline() as pipeline:
        Records = (
            pipeline
            | 'Read the Input' >> beam.io.ReadFromText('datasets/dept_data.txt')
            | 'Split the string to list' >> beam.Map(lambda record: record.strip().split(','))
        )

        Accounts = (
            Records
            | 'Filter the employees in Accounts section' >> beam.Filter(lambda record: record[3] == 'Accounts' )
            | 'Print Accounts' >> MyTransform()
        )

        Hr = (
            Records
            | 'Filter the employees in HR section' >> beam.Filter(lambda record: record[3] == 'HR' )
            | 'Print HR' >> MyTransform()
        )
        


# method1()
method2()