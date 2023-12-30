import apache_beam as beam

class Transform:
    def __init__(self):
        pass

    def get_pipeline(self):
        return beam.Pipeline()
    
    def map_(self):
        pipeline = self.get_pipeline()

        PCollection = (
            pipeline
            | beam.Create([i for i in range(5)])
            | beam.Map(lambda x: x**3)
            | beam.io.WriteToText('outputs/transforms/map_out/out')
        )

        pipeline.run()
    
    def flat_map(self):
        pipeline = self.get_pipeline()

        PCollection = (
            pipeline
            | beam.Create([(i,(i**2,i**3,i**4)) for i in range(5)])
        )

        (
            PCollection
            | beam.Map(lambda x: x[1])
            | 'Write Map' >> beam.io.WriteToText('outputs/transforms/flat_map_out/map_out')
        )

        (
            PCollection
            | beam.FlatMap(lambda x: x[1])
            | 'Write Flat Map' >> beam.io.WriteToText('outputs/transforms/flat_map_out/flat_map_out')
        )

        pipeline.run()

    def filter_(self):
        pipeline = self.get_pipeline()

        PCollection = (
            pipeline
            | beam.Create([i for i in range(10)])
            | 'Filter Even Records' >> beam.Filter(lambda x: x%2 == 0)
            | 'Write Output' >> beam.io.WriteToText('outputs/transforms/filter_out/out')
        )

        pipeline.run()

    def combine_per_key(self):
        pipeline = self.get_pipeline()

        PCollection = (
            pipeline
            | beam.Create([
                    (1, 10),
                    (1, 20),
                    (2, 5),
                    (3, 15),
                    (3, 6),
                    (3, 10),
                    (4, 0)
                ])
            | beam.CombinePerKey(sum)
            | 'Write Output' >> beam.io.WriteToText('outputs/transforms/combine_by_key/out')
        )

        pipeline.run()

    def flatten_(self):
        with self.get_pipeline() as pipeline:
            odd_nums = (
                pipeline
                | 'Create Odd Numbers' >> beam.Create([i for i in range(1,10,2)])
            )
            even_nums = (
                pipeline
                | 'Create Even Numbers' >> beam.Create([i for i in range(0,10,2)])
            )
            (
                (even_nums, odd_nums)
                | 'Similar to Union' >> beam.Flatten()
                | beam.io.WriteToText('outputs/transforms/flatten/out')
            )

transform = Transform()
# transform.map_()
# transform.flat_map()
# transform.filter_()
# transform.combine_per_key()
transform.flatten_()