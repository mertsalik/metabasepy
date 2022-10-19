import argparse
import json
import sys
import os
import logging

sys.path.insert(1, os.path.join(sys.path[0], '..'))

from metabasepy import Client, RequestException

logger = logging.getLogger(__name__)

ENVIRONMENTS = ["test"]

class ConfigurationException(Exception):
    def __init__(self, msg=None, *args, **kwargs):
        Exception.__init__(self, msg, *args, **kwargs)


class InvalidCardException(Exception):
    def __init__(self, msg=None, *args, **kwargs):
        Exception.__init__(self, msg, *args, **kwargs)


class CollectionException(Exception):
    def __init__(self, msg=None, *args, **kwargs):
        Exception.__init__(self, msg, *args, **kwargs)


def migrate_collection(source_client: Client, destination_client: Client, collection_id: int, env: str):
    source_collection_name = source_client.collections.get(collection_id)['name']
    destination_collection_id = destination_client.collections.post(
        name=f"{source_collection_name} {env.capitalize()}"
        )['id']
    collection_items = source_client.collections.items(collection_id=collection_id)
    
    for item in collection_items:
        if item['model'] == 'dashboard':
            dashboard_id = item['id']
            dashboard_name = item['name']
            print('Copying the dashboard "{}" ...'.format(dashboard_name))
            copy_dashboard(
                source_dashboard_id=dashboard_id,
                destination_collection_id=destination_collection_id,
                destination_dashboard_name=dashboard_name,
            )

def migrate_metrics(source_client: Client, destination_client: Client, env: str):
    print(f"Migrating metrics for env {env}")
    source_metrics = source_client.metrics.get()
    destination_metrics = destination_client.metrics.get()
    for metric in source_metrics:
        destination_table_id = get_destination_table_id(metric["table_id"])
        metric["table_id"] = destination_table_id
        metric["definition"]["source-table"] = destination_table_id
        metric_query = metric["definition"]["aggregation"]
        map_query(metric_query)
        metric["name"] = f'{metric["name"]} {env.capitalize()}'
        # if metric["name"] not in [destination_metric['name'] for destination_metric in destination_metrics]:
        destination_client.metrics.post(**metric)



def get_destination_table_id(source_table_id):
    source_table = source_client.tables.get(source_table_id)
    table_name = source_table['name']
    destination_table = destination_client.tables.get_by_name_and_schema(table_name, f"dbt_{env}")
    return destination_table['id']

def copy_dashboard(
    source_dashboard_id, 
    destination_dashboard_name, 
    destination_collection_id,
    ):
    source_dashboard = source_client.dashboards.get(source_dashboard_id)
    source_dashboard["collection_id"] = destination_collection_id
    dup_dashboard_id = destination_client.dashboards.post(**source_dashboard)
    parameters = {"parameters": source_dashboard["parameters"]}
    destination_client.dashboards.put(dup_dashboard_id, **parameters)

    for card in source_dashboard['ordered_cards']:
        card_id = card["card_id"]
        if card_id is not None:
            card_id = copy_card(source_card_id=card_id, destination_collection_id=destination_collection_id)["id"]

        dashboard_card_id = destination_client.dashboards.post_cards(dup_dashboard_id, card_id)
        source_parameters_mappings = card["parameter_mappings"]
        card["parameter_mappings"] = prepare_parameters_mapping(source_parameters_mappings, card_id)
        destination_client.dashboards.put_cards(dup_dashboard_id, dashboard_card_id, card)

    return dup_dashboard_id

def prepare_parameters_mapping(card_parameter_mappings: list, destination_card_id):
    for parameter_mapping in card_parameter_mappings:
        parameter_mapping["card_id"] = destination_card_id
        map_query(parameter_mapping["target"])
    return card_parameter_mappings

def copy_card(
    source_card_id, 
    destination_collection_id,
    ):

    source_card = source_client.cards.get(source_card_id)
    source_card['collection_id'] = destination_collection_id

    if source_card.get('description') == '': 
        source_card['description'] = None

    # Save as a new card
    res = create_card(custom_json=source_card, collection_id=destination_collection_id)

    # Return the id of the created card
    return res


def create_card(collection_id=None, custom_json=None):
    
    if not custom_json:
        raise ValueError
    assert type(custom_json) == dict
    # Check whether the provided json has the required info or not
    is_complete_json = True
    for item in ['name', 'dataset_query', 'display']:
        if item not in custom_json:
            is_complete_json = False
            print('The provided json is detected as partial.')
            break

    # Fix for the issue #10
    if custom_json.get('description') == '': 
        custom_json['description'] = None

    # Set the collection
    if collection_id == "root":
        custom_json['collection_id'] = None
    elif collection_id:
        custom_json['collection_id'] = collection_id


    if is_complete_json:
        if 'visualization_settings' not in custom_json:
            custom_json['visualization_settings'] = {}
        
        source_table = source_client.tables.get(custom_json["table_id"])
        source_table_fields = source_client.tables.fields(custom_json["table_id"])

        destination_table = destination_client.tables.get_by_name_and_schema(source_table['name'], f"dbt_{env}")
        if not destination_table:
            raise ValueError(f"There is no such table in destination bigquery - {source_table['name']}")

        destination_table_id = destination_table["id"]
        destination_table_database = destination_table['db_id']
        custom_json_query = custom_json["dataset_query"]["query"]
        map_query(custom_json_query)
        custom_json["table_id"] = destination_table_id
        custom_json["dataset_query"]["query"]["source-table"] = destination_table_id
        custom_json["dataset_query"]["database"] = destination_table_database
        
        res = destination_client.cards.post(json=custom_json)
        if res and not res.get('error'):
            print(f'The card {res["name"]} was created successfully.')
            return res
        else:
            print('Card Creation Failed.\n', res)
            return res
    raise ValueError("card cannot be created")


def map_query(query):
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
            destination_field_id = get_destination_field_id(source_field_id)
            field[field_id_index] = destination_field_id
            update_destination_field(source_field_id, destination_field_id)
            fk_field_id = field[fk_field_index]
            if fk_field_id and "source-field" in fk_field_id:
                fk_field_id["source-field"] = get_destination_field_id(fk_field_id["source-field"])

        elif type(field) == list and field[query_type_index] == 'metric':
            source_metric_id = field[field_id_index]
            destination_metric_id = get_destination_metric_id(source_metric_id)
            field[field_id_index] = destination_metric_id

        map_query(field)

    return 


def get_destination_metric_id(source_metric_id):
    source_metric = source_client.metrics.get(source_metric_id)
    source_table_id = source_metric["definition"]["source-table"]
    source_table = source_client.tables.get(source_table_id)
    destination_table = destination_client.tables.get_by_name_and_schema(source_table['name'], f"dbt_{env}")
    destination_metric_id = destination_client.metrics.get_by_name_and_table_id(f'{source_metric["name"]} {env.capitalize()}', destination_table['id'])["id"]
    return destination_metric_id

def get_destination_field_id(source_field_id):
    source_field = source_client.fields.get(source_field_id)
    source_table_id = source_field["table_id"]
    source_table = source_client.tables.get(source_table_id)
    destination_table = destination_client.tables.get_by_name_and_schema(source_table['name'], f"dbt_{env}")
    if not destination_table:
        raise ValueError(f"There is no such table in destination bigquery - {source_table['name']}")
    destination_table_id = destination_table["id"]
    destination_table_fields = destination_client.tables.fields(destination_table_id)
    destination_table_names = {field['name']:field['id'] for field in destination_table_fields}
    return destination_table_names[source_field['name']]

def update_destination_field(source_field_id, destination_field_id):
    source_field = source_client.fields.get(source_field_id)
    source_field['id'] = destination_field_id
    source_field.pop('table_id')
    if source_field.get("fk_target_field_id"):
        source_field["fk_target_field_id"] = get_destination_field_id(source_field["fk_target_field_id"])
    destination_client.fields.put(destination_field_id, **source_field)
    

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

    source = configuration.get('source')
    destination = configuration.get('destination')

    source_client = Client(**source)
    destination_client = Client(**destination)

    source_client.authenticate()
    destination_client.authenticate()

    collection_id = configuration.get("source_collection_id", "root")

    for env in ENVIRONMENTS:
        tables_ids = [table["id"] for table in source_client.tables.get() if table['schema'] == f"dbt_{env}"]
        for table_id in tables_ids:
            source_fields = source_client.tables.fields(table_id)
            for source_field in source_fields:
                try:
                    destination_field_id = get_destination_field_id(source_field["id"])
                except ValueError:
                    continue
                update_destination_field(source_field['id'], destination_field_id)
        migrate_metrics(source_client=source_client, destination_client=destination_client, env=env)
        migrate_collection(source_client=source_client, destination_client=destination_client, collection_id=collection_id, env=env)
