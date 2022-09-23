from pydoc import cli
from metabasepy import Client, RequestException
import json
import argparse
from os.path import exists


def delete_cards(client):
    cards = client.cards.get()
    card_ids = [card['id'] for card in cards]
    for card_id in card_ids:
        client.cards.delete(card_id)

 
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

    delete_cards(client=destination_client)
