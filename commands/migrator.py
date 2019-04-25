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


def migrate(source_client, destination_client, database_mappings):
    created_card_ids = []
    all_collections = source_client.collections.get()

    # import queries that placed in collections
    for collection_data in all_collections:

        # create collection
        collection_id = create_collection(collection_data, destination_client)

        for card_info in source_client.cards.get_by_collection(
                collection_data.get('slug')):
            create_card(card_info, collection_id)
            created_card_ids.append(card_info['id'])

    for card_info in source_client.cards.get():
        if card_info.get('id') not in created_card_ids:
            create_card(card_info)
            created_card_ids.append(card_info['id'])


def create_card(card_info, collection_id=None):
    card_name = card_info.get('name', "Question")
    try:
        dataset_query = card_info.get('dataset_query', None)
        if not dataset_query:
            raise InvalidCardException(
                msg="dataset_query does not exists")
        native = dataset_query.get('native', None)
        if not native:
            raise InvalidCardException(
                msg="dataset_query->native does not exists")
        sql_query = native.get('query', None)
        if not sql_query:
            raise InvalidCardException(
                msg="dataset_query->native->query does not exists")
        template_tags = native.get('template_tags', None)
        destination_db_id = database_mappings.get(
            card_info['database_id'], None)
        destination_client.cards.post(database_id=destination_db_id,
                                      name=card_name,
                                      query=sql_query,
                                      template_tags=template_tags,
                                      collection_id=collection_id)
    except InvalidCardException as icex:
        logger.info(icex)
    except KeyError as ke:
        # Probably this is not a native query, skip this

        logger.error(ke)
        logger.error("skipping {}.".format(card_name))
    except Exception as any_ex:
        logger.error(any_ex)


def create_collection(collection_data, destination_client):
    collection_id = None
    try:
        collection_response = destination_client.collections.post(
            **collection_data)
        collection_id = collection_response.get('id')
    except RequestException as rex:
        if "already exists" in rex.message:
            dest_collections = destination_client.collections.get()
            for collection in dest_collections:
                if collection["name"] == collection_data["name"]:
                    collection_id = collection['id']
                    break
        if not collection_id:
            raise CollectionException("Collections cant be created!")
    return collection_id


def get_database_mappings(source_client, destination_client,
                          migration_config):
    mapping_conf = migration_config.get('mappings')
    database_mappings = mapping_conf.get('databases')  # must be a list of dict
    directions = {}
    for index, mapping in enumerate(database_mappings):
        # there is no get by name call for databases, yet.
        source_db_id = None
        destination_db_id = None

        source_databases = source_client.databases.get()
        for _db in source_databases:
            if _db["name"] == mapping["source"]:
                source_db_id = _db.get('id')
                break
        if not source_db_id:
            raise ConfigurationException(
                msg="{} not found in source databases".format(
                    mapping["source"]))
        destination_databases = destination_client.databases.get()
        for _db in destination_databases:
            if _db["name"] == mapping["destination"]:
                destination_db_id = _db.get('id')
                break
        if not destination_db_id:
            raise ConfigurationException(
                msg="{} not found in destination databases".format(
                    mapping["source"]))

        directions.update({
            source_db_id: destination_db_id
        })
    return directions


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

    # validate mappings
    database_mappings = get_database_mappings(
        source_client=source_client, destination_client=destination_client,
        migration_config=configuration)

    migrate(source_client=source_client, destination_client=destination_client,
            database_mappings=database_mappings)
