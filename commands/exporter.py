import argparse
import os
import json
from urllib.parse import urlparse
from slugify import slugify
import logging

from metabasepy.client import Client, AuthorizationFailedException

logger = logging.getLogger(__name__)


def create_dir(dirname):
    try:
        os.mkdir(dirname)
    except FileExistsError:
        pass


def download_cards(username, password, base_url, destination_directory,
                   **kwargs):
    cli = Client(username=username, password=password, base_url=base_url)
    cli.authenticate()

    create_dir(destination_directory)

    all_collections = cli.collections.get()
    if not all_collections:
        # save all cards for one default collection
        default_collection_path = os.path.join(destination_directory, "default")
        create_dir(default_collection_path)
        for card_info in cli.cards.get():
            card_name = slugify(card_info.get('name', "Question"))
            try:
                sql_query = card_info['dataset_query']['native']['query']
            except KeyError as ke:
                # Probably this is not a native query, skip this
                logger.error(ke)
                continue

            sql_save_path = os.path.join(default_collection_path,
                                         "{}.sql".format(card_name))
            with open(sql_save_path, 'w') as f:
                f.write(sql_query)
    else:
        for collection_data in all_collections:
            collection_directory = os.path.join(destination_directory,
                                                collection_data.get('name'))
            create_dir(collection_directory)

            for card_info in cli.cards.get_by_collection(
                    collection_data.get('slug')):
                card_name = slugify(card_info.get('name', "Question"))
                try:
                    sql_query = card_info['dataset_query']['native']['query']
                except KeyError as ke:
                    # Probably this is not a native query, skip this
                    logger.error(ke)
                    continue

                sql_save_path = os.path.join(collection_directory,
                                             "{}.sql".format(card_name))
                with open(sql_save_path, 'w') as f:
                    f.write(sql_query)


if __name__ == '__main__':
    current_directory_path = os.getcwd()
    default_export_path = os.path.join(current_directory_path,
                                       "metabase_export")
    parser = argparse.ArgumentParser(
        prog="metabase_query_exporter",
        usage="exporter -c /your/config/file/path.json",
        description="Download and save metabase Cards into folders."
    )
    parser.add_argument('--download_path', '-d',
                        dest='download_path',
                        default=default_export_path,
                        type=str,
                        help='sql path to save sql-query files',
                        )
    parser.add_argument('--configuration_file', '-c',
                        dest='conf_file_path',
                        type=str,
                        required=True,
                        help='configuration file path for credentials',
                        )

    args = parser.parse_args()

    credentials = []
    with open(args.conf_file_path, 'r') as config_file:
        credentials = json.load(config_file)
        if type(credentials) is not list:
            raise ValueError("Credentials must be list of dictionary!")

        for credential_info in credentials:
            if "username" not in credential_info or "password" not in credential_info or "base_url" not in credential_info:
                raise ValueError(
                    "Invalid configuration. Credential object must include "
                    "'username', 'password' and 'base_url' values ")

    for credential_info in credentials:
        metabase_uri = urlparse(credential_info.get('base_url'))
        destination_directory = os.path.join(args.download_path,
                                             metabase_uri.netloc)
        create_dir(destination_directory)
        try:
            download_cards(destination_directory=destination_directory,
                           **credential_info)
        except AuthorizationFailedException as afex:
            logger.error("Authentication failed for {} -> {}".format(
                credential_info.get('username'),
                credential_info.get('base_url')))
            logger.error("Skipping {}".format(credential_info.get('username')))
