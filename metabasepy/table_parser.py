# -*- coding: utf-8 -*-

__author__ = "mertsalik"
__copyright__ = "Copyright 2018"
__credits__ = ["mertsalik", ""]
__license__ = "Private"
__email__ = ""


class MetabaseResultInvalidException(Exception):
    pass


class MetabaseTable(object):
    def __init__(self):
        self.status = None
        self.native_query = None
        self.columns = []
        self.rows = []
        self.database = None

    @property
    def column_count(self):
        return len(self.columns)

    @property
    def row_count(self):
        return len(self.rows)


class MetabaseTableParser(object):
    @staticmethod
    def validate_metabase_response(metabase_response):
        response_requirements = {
            'json_query',
            'data'
        }
        if not response_requirements <= set(metabase_response):
            raise MetabaseResultInvalidException()

        json_query_requirements = {
            'database'
        }
        if not json_query_requirements <= set(metabase_response['json_query']):
            raise MetabaseResultInvalidException()

        data_requirements = {
            'columns',
            'rows',
            'native_form'
        }
        if not data_requirements <= set(metabase_response['data']):
            raise MetabaseResultInvalidException()

        native_form_requirements = {
            'query'
        }
        if not native_form_requirements <= set(
                metabase_response['data']['native_form']):
            raise MetabaseResultInvalidException()

    @staticmethod
    def get_table(metabase_response):
        MetabaseTableParser.validate_metabase_response(metabase_response)

        table = MetabaseTable()
        table.rows = metabase_response['data']['rows']

        table.columns = metabase_response['data']['columns']
        table.native_query = metabase_response['data']['native_form']['query']
        table.status = metabase_response['status']
        table.database = metabase_response['json_query']['database']

        return table
