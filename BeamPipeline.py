import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.runners.runner import PipelineState
import argparse
import os



from Log import logger
from utils import DataClean, DataTransform
from BQ import BigQuery
from constants import (
    PIPELINE_RUN_SUCCESS_MESSAGE, PIPELINE_RUN_FAILURE_MESSAGE,
    DATASET_NAME, DATASET_DESC, SERVICE_ACCOUNT_NAME,
    DELIVERED_ORDERS_TABLE, OTHER_ORDERS_TABLE, DAILY_VIEW, TEMP_LOCATION
)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_NAME

parser = argparse.ArgumentParser()
parser.add_argument(
    '--input', dest='input', required=True, help='Input file to process'
)
path_args, pipeline_args = parser.parse_known_args()

inputs_pattern = path_args.input

options = PipelineOptions(pipeline_args)
pipeline = beam.Pipeline(options = options)

data_clean_rules = DataClean()
data_transform_rules = DataTransform()

logger.info('Cleaning Data')

cleaned_data = (
    pipeline
    | 'Read input file' >> beam.io.ReadFromText(inputs_pattern, skip_header_lines=1)
    | 'Make the row uniform' >> beam.Map(lambda record: record.lower())
    | 'Split the data' >> beam.Map(lambda record: record.split(','))
    | 'Clean the items' >> beam.Map(data_clean_rules.clean_order_item)
    | 'Remove Special Characters' >> beam.Map(data_clean_rules.remove_special_characters)
    | 'Cast rating and amount to float' >> beam.Map(data_transform_rules.cast_float)
    | 'Create Json Records' >> beam.Map(data_transform_rules.convert_to_json)
    # | 'Get count of records' >> beam.combiners.Count.Globally()
    # | 'Print Data' >> beam.Map(print)
)
logger.info('Filtering Data')
delivered_orders = (
    cleaned_data
    | 'Filter Delivered Orders' >> beam.Filter(lambda record: record['status'] == 'delivered')
)

# delivered_orders_count = (
#     delivered_orders
#     | 'Count Delivered Orders' >> beam.combiners.Count.Globally()
#     # | 'Print Delivered Orders Count' >> beam.Map(print)
# )

other_orders = (
    cleaned_data
    | 'Filter Other Orders' >> beam.Filter(lambda record: record['status'] != 'delivered')
)

# other_orders_count = (
#     other_orders
#     | 'Count Other Orders' >> beam.combiners.Count.Globally()
#     # | 'Print Other Orders Count' >> beam.Map(print)
# )
bq = BigQuery(DATASET_NAME, DATASET_DESC)
table_schema = '''
    customer_id:STRING,
    date:STRING,
    time:TIME,
    order_id:STRING,
    items:STRING,
    amount:FLOAT,
    mode:STRING,
    restaurant:STRING,
    status:STRING,
    rating:FLOAT,
    feedback:STRING
'''
def write_to_bigquery(PCollection, label, table_name, table_schema):
    logger.info(f'Inserting Data into BQ table {table_name}')
    (
        PCollection
        | label >> beam.io.WriteToBigQuery(
            table = f'{bq.get_dataset_id()}.{table_name}',
            schema = table_schema,
            create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND,
            additional_bq_parameters = {'timePartitioning': {'type':'DAY'}},
            custom_gcs_temp_location = TEMP_LOCATION
        )
    )

write_to_bigquery(
    delivered_orders,
    'Write Delivered Orders To BigQuery',
    DELIVERED_ORDERS_TABLE,
    table_schema
)

write_to_bigquery(
    other_orders,
    'Write Other Orders To BigQuery',
    OTHER_ORDERS_TABLE,
    table_schema
)

run = pipeline.run()
if run.state == PipelineState.DONE:
    logger.info(PIPELINE_RUN_SUCCESS_MESSAGE)
else:
    logger.error(PIPELINE_RUN_FAILURE_MESSAGE)
view_ddl = f'''
    select * from 
    {bq.get_dataset_id()}.{DELIVERED_ORDERS_TABLE} 
    where _PARTITIONDATE = DATE(current_date())
'''
bq.create_view(DAILY_VIEW, view_ddl)