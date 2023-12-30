import apache_beam as beam

class Basics:
    def __init__(self):
        pass
    def get_pipeline(self):
        return beam.Pipeline()
    def gen_in_mem(self):
        pipeline = self.get_pipeline()

        lines = (
            pipeline
            | beam.Create([
                'Using Create Transform',
                'to generate in memory data.',
                'This is 3rd line.',
                'Thanks!'
            ])
            | beam.io.WriteToText('outputs/basics/p1_out/out')
        )

        pipeline.run()

    def gen_two_cols(self):
        pipeline = self.get_pipeline()

        lines = (
            pipeline
            | beam.Create([
                ('English',90),
                ('Maths', 100),
                ('Science', 99)
            ])
            | beam.io.WriteToText('outputs/basics/p2_out/out')
        )

        pipeline.run()

    def gen_key_value(self):
        pipeline = self.get_pipeline()

        lines = (
            pipeline
            | beam.Create({
                'number': [i for i in range(5)],
                'number_square': [i*i for i in range(5)]
            })
            | beam.io.WriteToText('outputs/basics/p3_out/out')
        )

        pipeline.run()

    def another_syntax(self):
        with beam.Pipeline() as pipeline:
            (
                pipeline
                | beam.Create([i for i in range(5)])
                | 'Print to console' >> beam.Map(print)
            )
        # No need of pipeline.run() in this syntax



basics = Basics()
# basics.gen_in_mem()
# basics.gen_two_cols()
# basics.gen_key_value()
basics.another_syntax()