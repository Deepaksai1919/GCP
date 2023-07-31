from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from Log import logger




class BigQuery:
    def __init__(self, dataset_name, dataset_desc):
        self.client = bigquery.Client()
        self.__dataset_id = f'{self.client.project}.{dataset_name}'
        self.dataset_name = dataset_name
        self.create_dataset(self.__dataset_id, dataset_desc)
    def get_dataset_id(self):
        return self.__dataset_id
    def create_dataset(self, dataset_id, dataset_desc):
        try:
            logger.info(f'Checking if DataSet "{dataset_id}" exists')
            self.client.get_dataset(dataset_id)
            logger.warning(f'DataSet "{dataset_id}" already exists')
            return True
        except NotFound:
            logger.info(f'Creating DataSet "{dataset_id}"')
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = 'asia-south1'
            dataset.description = dataset_desc
            dataset_ref = self.client.create_dataset(dataset, timeout=30)
    def create_view(self, view_name, view_query):
        dataset_ref = self.client.dataset(self.dataset_name)
        view_ref = dataset_ref.table(view_name)
        view_to_create = bigquery.Table(view_ref)
        view_to_create.view_query = view_query
        view_to_create.view_use_legacy_sql = False
        try:
            logger.info(f'Creating view "{view_name}"')
            self.client.create_table(view_to_create)
        except:
            logger.warning(f'View "{view_name}" already exists')

