import argparse
import json
import sys
import os
import logging

sys.path.insert(1, os.path.join(sys.path[0], '..'))

from metabasepy import Client, RequestException

logger = logging.getLogger(__name__)


class ConfigurationException(Exception):
    def __init__(self, msg=None, *args, **kwargs):
        Exception.__init__(self, msg, *args, **kwargs)


class InvalidCardException(Exception):
    def __init__(self, msg=None, *args, **kwargs):
        Exception.__init__(self, msg, *args, **kwargs)


class CollectionException(Exception):
    def __init__(self, msg=None, *args, **kwargs):
        Exception.__init__(self, msg, *args, **kwargs)


def migrate(source_client, destination_client, collection_id):
    collection_items = source_client.collections.items(collection_id=collection_id)
    
    for item in collection_items:

        if item['model'] == 'dashboard':
            dashboard_id = item['id']
            dashboard_name = item['name']
            print('Copying the dashboard "{}" ...'.format(dashboard_name))
            copy_dashboard(
                source_dashboard_id=dashboard_id,
                destination_collection_id=collection_id,
                destination_dashboard_name=dashboard_name,
            )


def copy_dashboard(
    source_dashboard_id, 
    destination_dashboard_name, 
    destination_collection_id,
    ):
    source_dashboard = source_client.dashboards.get(source_dashboard_id)
    dup_dashboard_id = destination_client.dashboards.post(destination_dashboard_name, **source_dashboard)

    source_dashboard_card_IDs = [ i['card_id'] for i in source_dashboard['ordered_cards'] if i['card_id'] is not None ]
    for card_id in source_dashboard_card_IDs:
        dup_card_id = copy_card(source_card_id=card_id, destination_collection_id=destination_collection_id)

        destination_client.dashboards.post_cards(dup_dashboard_id, dup_card_id)

    return dup_dashboard_id

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
    complete_json = True
    for item in ['name', 'dataset_query', 'display']:
        if item not in custom_json:
            complete_json = False
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


    if complete_json:
        # Add visualization_settings if it is not present in the custom_json
        if 'visualization_settings' not in custom_json:
            custom_json['visualization_settings'] = {}

        # Create the card using only the provided custom_json 
        res = destination_client.cards.post(json=custom_json)
        if res and not res.get('error'):
            print('The card was created successfully.')
            return res
        else:
            print('Card Creation Failed.\n', res)
            return res


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

    credentials = []
    with open(args.configuration_file_path, 'r') as config_file:
        configuration = json.load(config_file)

    source = configuration.get('source')
    destination = configuration.get('destination')

    source_client = Client(**source)
    destination_client = Client(**destination)

    source_client.authenticate()
    destination_client.authenticate()

    collection_id = configuration.get("source_collection_id", "root")

    migrate(source_client=source_client, destination_client=destination_client, collection_id=collection_id)
