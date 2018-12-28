import argparse
import json

from metabasepy.client import Client

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog="metabase_query_exporter",
        usage="exporter -c /your/config/file/path.json",
        description="Download and save metabase Cards into folders."
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
