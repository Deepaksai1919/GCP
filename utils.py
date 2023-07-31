import re

class DataClean:
    def __init__(self):
        pass

    def clean_order_item(self, record):
        """
            The items are separated by delimiter. Few records have just one item
            but still a delimiter is present. This function cleans the unwanted delimiter
            Eg: Chow M?ein:
        """
        items_index = 4
        items = record[items_index]
        record[items_index] = items.rstrip(':')
        return record
   
    def remove_special_characters(self, record):
        """
            The function is for removing the special characters from the record.
            The allowed characters are 
                1) Alpha Numeric
                2) '-' in date column
                3) ':' in time and order items columns
                4) White space character
        """
        for index, column in enumerate(record):
            record[index] = re.sub(r'[^a-zA-Z0-9-:\s]','',column.strip())
        return record
    
class DataTransform:
    def __init__(self):
        self.__create_header()
        self.amount_index = self.__get_column_index('amount')
        self.rating_index = self.__get_column_index('rating')

    def __get_column_index(self, column):
        return self.header.index(column)

    def __create_header(self):
        if hasattr(self,'header'):
            return
        # print('Creating Header')
        header = 'customer_id,date,time,order_id,items,amount,mode,restaurant,status,rating,feedback'
        self.header = header.split(',')

    def cast_float(self, record):
        record[self.amount_index] = float(record[self.amount_index])
        record[self.rating_index] = float(record[self.rating_index])
        return record

    def convert_to_json(self, record):        
        json_record = {}
        for key, value in zip(self.header, record):
            json_record[key] = value
        return json_record
