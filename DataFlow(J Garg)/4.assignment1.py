# Read the given paragraph and WAP to count the words
import re
import apache_beam as beam
# s = "[ab (c)'d.?|]:/\\"
# print(re.sub(r"[^A-Za-z0-9\s']",'',s))
# ab c'd

input_file = 'datasets/assignment1.txt'

def extract_words(line):
    return re.sub(r"[^A-Za-z0-9\s']",'',line.strip().lower())

with beam.Pipeline() as pipeline:
    (
        pipeline
        | 'Read the input' >> beam.io.ReadFromText(input_file)
        | 'Remove the special characters' >> beam.Map(lambda record: extract_words(record))
        | 'Extract the words' >> beam.FlatMap(lambda record: record.split())
        | 'Create Key Value pairs for words' >> beam.Map(lambda word: (word, 1))
        | 'Count the word frequence' >> beam.CombinePerKey(sum)
        | 'Write the output' >> beam.io.WriteToText('outputs/assignment1/out')
    )