import argparse
import json
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], '..'))

from metabasepy.client import Client

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog="flush_queries",
        usage="flusher -c /your/config/file/path.json",
        description="Delete cards (queries) from metabase server."
    )
    parser.add_argument('--configuration_file', '-c',
                        dest='conf_file_path',
                        type=str,
                        required=True,
                        help='configuration file path for credentials',
                        )
    args = parser.parse_args()

    credentials = {}
    with open(args.conf_file_path, 'r') as config_file:
        credentials = json.load(config_file)

    client = Client(**credentials)
    client.authenticate()

    # delete cards
    for card in client.cards.get():
        client.cards.delete(card_id=card['id'])
