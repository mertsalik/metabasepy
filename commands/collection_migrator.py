import argparse
import json
import sys
import os
from dataclasses import dataclass
from exceptions import ConfigurationException, InvalidCardException, CollectionException
from migrator_config import MigratorConfig, MetabaseConnectionCredentials

sys.path.insert(1, os.path.join(sys.path[0], '..'))
from metabasepy import Client, RequestException


class MetabaseCollectionMigrator:

    def __init__(self, config: MigratorConfig) -> None:
        self.src_client: Client = self._init_client(config.source_credentials)
        self.dest_client: Client = self._init_client(config.destination_credentials)
        self.src_collection_id: int = config.source_collection_id
        self.dest_collection_id: int = None

    def _init_client(self, credentials: MetabaseConnectionCredentials):
        client = Client(
            username=credentials.username,
            password=credentials.password,
            base_url=credentials.base_url,
            db_id=credentials.db_id
        )
        client.authenticate()
        return client

    def migrate_collection(self):
        src_collection_name = self.src_client.collections.get_name(self.src_collection_id)
        destination_collection_id = self.dest_client.collections.post(
            name=f"{src_collection_name} migrated"
            )['id']
        self.dest_collection_id = destination_collection_id
        dashboards = self.src_client.collections.get_dashboards(self.src_collection_id)
        self.copy_dashboards(dashboards)

    def copy_dashboards(self, dashboards: list):
        for dashboard in dashboards:
            dashboard_id = dashboard['id']
            dashboard_name = dashboard['name']
            print('Copying the dashboard "{}" ...'.format(dashboard_name))
            source_dashboard = self.src_client.dashboards.get(dashboard_id)
            source_dashboard["collection_id"] = self.dest_collection_id
            dest_dashboard_id = self.dest_client.dashboards.post(**source_dashboard)
            parameters = {"parameters": source_dashboard["parameters"]}
            self.dest_client.dashboards.put(dest_dashboard_id, **parameters)
            self.copy_cards(source_dashboard["ordered_cards"], dest_dashboard_id)

    def copy_cards(self, cards: list, dest_dashboard_id: int):
        for card in cards:
            card_id = card["card_id"]
            if card_id is not None:
                card_id = self.copy_card(
                    source_card_id=card_id, 
                    )
                if not card_id:
                    continue
            dest_card_id = self.dest_client.dashboards.post_cards(dest_dashboard_id, card_id)
            source_parameters_mappings = card["parameter_mappings"]
            card["parameter_mappings"] = self.prepare_parameters_mapping(source_parameters_mappings, card_id)
            self.dest_client.dashboards.put_cards(dest_dashboard_id, dest_card_id, card)


    def copy_card(self, source_card_id):
        source_card = self.src_client.cards.get(source_card_id)
        source_card['collection_id'] = self.dest_collection_id
        if source_card.get('description') == '': 
            source_card['description'] = None
        dest_card_id = self.create_card(custom_json=source_card)
        return dest_card_id

    def create_card(self, custom_json):
        assert type(custom_json) == dict
        is_complete_json = True
        for item in ['name', 'dataset_query', 'display']:
            if item not in custom_json:
                is_complete_json = False
                print('The provided json is detected as partial.')
                break

        if custom_json.get('description') == '': 
            custom_json['description'] = None

        if self.dest_collection_id == "root":
            custom_json['collection_id'] = None
        elif self.dest_collection_id:
            custom_json['collection_id'] = self.dest_collection_id

        if is_complete_json:
            if 'visualization_settings' not in custom_json:
                custom_json['visualization_settings'] = {}
            if not custom_json["table_id"]:
                return
            source_table = self.src_client.tables.get(custom_json["table_id"])
            if not source_table:
                return
            destination_table = self.dest_client.tables.get_by_name_and_schema_and_db(source_table['name'], source_table["schema"], 4)
            if not destination_table:
                raise ValueError(f"There is no such table in destination bigquery - {source_table['schema']}.{source_table['name']}")

            destination_table_id = destination_table["id"]
            destination_table_database = destination_table['db_id']
            custom_json_query = custom_json["dataset_query"]["query"]
            self.map_query(custom_json_query)
            custom_json["table_id"] = destination_table_id
            custom_json["dataset_query"]["query"]["source-table"] = destination_table_id
            custom_json["dataset_query"]["database"] = 4
            
            res = self.dest_client.cards.post(json=custom_json)
            if res and not res.get('error'):
                print(f'The card {res["name"]} was created successfully.')
                return res["id"]
            else:
                print('Card Creation Failed.\n', res)
                return res
        raise ValueError("card cannot be created")


    def get_destination_table_id(self, source_table_id):
        source_table = self.src_client.tables.get(source_table_id)
        table_name = source_table['name']
        destination_table = self.dest_client.tables.get_by_name_and_schema_and_db(table_name, f"bi", 4)
        return destination_table['id']



    def prepare_parameters_mapping(self, card_parameter_mappings: list, destination_card_id):
        for parameter_mapping in card_parameter_mappings:
            parameter_mapping["card_id"] = destination_card_id
            self.map_query(parameter_mapping["target"])
        return card_parameter_mappings


    def map_query(self, query):
        query_type_index = 0
        field_id_index = 1
        fk_field_index = 2
        if isinstance(query, dict):
            query = query.values()
        for field in query:

            if type(field) not in (list, dict):
                continue

            if type(field) == list and field[query_type_index] == 'field':
                source_field_id = field[field_id_index]
                if not isinstance(source_field_id, int):
                    continue
                destination_field_id = self.get_destination_field_id(source_field_id)
                field[field_id_index] = destination_field_id
                self.update_destination_field(source_field_id, destination_field_id)
                fk_field_id = field[fk_field_index]
                if fk_field_id and "source-field" in fk_field_id:
                    fk_field_id["source-field"] = self.get_destination_field_id(fk_field_id["source-field"])

            elif type(field) == list and field[query_type_index] == 'metric':
                source_metric_id = field[field_id_index]
                destination_metric_id = self.get_destination_metric_id(source_metric_id)
                field[field_id_index] = destination_metric_id

            self.map_query(field)

        return 

    def update_destination_field(self, source_field_id, destination_field_id):
        source_field = self.src_client.fields.get(source_field_id)
        source_field['id'] = destination_field_id
        source_field.pop('table_id')
        if source_field.get("fk_target_field_id"):
            source_field["fk_target_field_id"] = self.get_destination_field_id(source_field["fk_target_field_id"])
        self.dest_client.fields.put(destination_field_id, **source_field)


    def get_destination_field_id(self, source_field_id):
        source_field = self.src_client.fields.get(source_field_id)
        source_table_id = source_field["table_id"]
        source_table = self.src_client.tables.get(source_table_id)
        destination_table = self.dest_client.tables.get_by_name_and_schema_and_db(source_table['name'], source_table['schema'], 4)
        if not destination_table:
            raise ValueError(f"There is no such table in destination bigquery - {source_table['name']}")
        destination_table_id = destination_table["id"]
        destination_table_fields = self.dest_client.tables.fields(destination_table_id)
        destination_table_names = {field['name']:field['id'] for field in destination_table_fields}
        return destination_table_names[source_field['name']]


    
    # def migrate_metrics(source_client: Client, destination_client: Client, env: str):
    #     print(f"Migrating metrics for env {env}")
    #     source_metrics = source_client.metrics.get()
    #     destination_metrics = destination_client.metrics.get()
    #     for metric in source_metrics:
    #         destination_table_id = get_destination_table_id(metric["table_id"])
    #         metric["table_id"] = destination_table_id
    #         metric["definition"]["source-table"] = destination_table_id
    #         metric_query = metric["definition"]["aggregation"]
    #         map_query(metric_query)
    #         metric["name"] = f'{metric["name"]} {env.capitalize()}'
    #         # if metric["name"] not in [destination_metric['name'] for destination_metric in destination_metrics]:
    #         destination_client.metrics.post(**metric)
    
    # def get_destination_metric_id(source_metric_id):
    #     source_metric = source_client.metrics.get(source_metric_id)
    #     source_table_id = source_metric["definition"]["source-table"]
    #     source_table = source_client.tables.get(source_table_id)
    #     destination_table = destination_client.tables.get_by_name_and_schema_and_db(source_table['name'], f"bi", 4)
    #     destination_metric_id = destination_client.metrics.get_by_name_and_table_id(f'{source_metric["name"]}', destination_table['id'])["id"]
    #     return destination_metric_id

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog="metabase_query_migrator",
        usage="migrator -c /your/config/file/path.json",
        description="Copy / move sql queries from one "
                    "metabase server to another"
    )

    parser.add_argument('--configuration', '-c',
                        dest='configuration_file_path',
                        type=str,
                        required=True,
                        help='configuration file path for credentials of '
                             'destination & source metabase servers'
                        )
    args = parser.parse_args()

    with open(args.configuration_file_path, 'r') as config_file:
        configuration = json.load(config_file)

    src = configuration.get("source")
    src_creds = MetabaseConnectionCredentials(src["username"], src["password"], src["base_url"], src["db_id"])
    dest = configuration.get("destination")
    dest_creds = MetabaseConnectionCredentials(dest["username"], dest["password"], dest["base_url"], dest["db_id"])
    config = MigratorConfig(src_creds, dest_creds, configuration.get("source_collection_id"))

    # tables_ids = [table["id"] for table in source_client.tables.get() if table['schema'] == f"bi"]
    # for table_id in tables_ids:
    #     source_fields = source_client.tables.fields(table_id)
    #     for source_field in source_fields:
    #         try:
    #             destination_field_id = get_destination_field_id(source_field["id"])
    #         except ValueError:
    #             continue
    #         update_destination_field(source_field['id'], destination_field_id)
    # migrate_metrics(source_client=source_client, destination_client=destination_client, env=env)
    migrator = MetabaseCollectionMigrator(config)
    migrator.migrate_collection()
