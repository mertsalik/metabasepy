import re

import requests
import json

try:
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlencode


def get_file_export_path(file_name):
    from os import getcwd
    from os.path import join
    return join(getcwd(), file_name)


def parse_filename_from_response_header(response):
    if type(response) != requests.Response:
        raise ValueError("{} is not a valid Response object!")

    content_disposition = response.headers.get('Content-Disposition')
    if not content_disposition:
        return None

    filenames = re.findall('filename=(.+)', content_disposition)
    if not filenames:
        return None
    selected_filename = filenames[0]
    return selected_filename.strip('"').strip("'")


class AuthorizationFailedException(Exception):
    pass


class RequestException(Exception):
    def __init__(self, message=None):
        self.message = message


class Resource(object):
    def __init__(self, **kwargs):
        self.base_url = kwargs.get('base_url')
        self.token = kwargs.get('token')
        self.verify = kwargs.get('verify', True)
        self.proxies = kwargs.get('proxies')

    def prepare_headers(self):
        return {
            'X-Metabase-Session': self.token,
            'Content-Type': 'application/json'
        }

    @staticmethod
    def validate_response(response):
        request_method = response.request.method
        status_code = response.status_code
        if request_method == "GET":
            if status_code != 200:
                raise RequestException(message=response.content)
        elif request_method == "POST":
            if status_code not in [200, 201, 202]:
                raise RequestException(message=response.content)
        elif request_method == "PUT":
            if status_code != 204:
                raise RequestException(message=response.content)
        elif request_method == "DELETE":
            if status_code != 204:
                raise RequestException(message=response.content)

    @property
    def endpoint(self):
        raise NotImplementedError()

    def get(self):
        raise NotImplementedError()

    def post(self, **kwargs):
        raise NotImplementedError()

    def put(self, **kwargs):
        raise NotImplementedError()

    def delete(self, **kwargs):
        raise NotImplementedError()


class ApiCommand(object):
    """ This is a general interface to implement a wrapper of endpoints which
    only allows POST methods and not a resource representation. """

    def __init__(self, **kwargs):
        self.base_url = kwargs.get('base_url')
        self.token = kwargs.get('token')
        self.verify = kwargs.get('verify', True),
        self.proxies = kwargs.get('proxies')

    def prepare_headers(self):
        return {
            'X-Metabase-Session': self.token,
            'Content-Type': 'application/json'
        }

    def post(self, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def validate_response(response):
        status_code = response.status_code
        if status_code not in [200, 201, 202]:
            raise RequestException(message=response.content)

    @property
    def endpoint(self):
        raise NotImplementedError()


class DatabaseResource(Resource):

    @property
    def endpoint(self):
        return "{}/api/database".format(self.base_url)

    def get(self, database_id=None):
        url = self.endpoint
        if database_id:
            url = "{}/{}".format(url, database_id)
        resp = requests.get(
            url=self.endpoint,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        return resp.json()

    def get_by_name(self, name):
        all_dbs = self.get()
        return [db for db in all_dbs if db['name'] == name]

    def delete(self, database_id):
        url = "{}/{}".format(self.endpoint, database_id)
        resp = requests.delete(
            url=url,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(resp)

    def post(self, name, engine, host, port, dbname, user, password, ssl=False,
             tunnel_port=22):
        request_data = {
            "name": name,
            "engine": engine,
            "details": {
                "host": host,
                "port": port,
                "dbname": dbname,
                "user": user,
                "password": password,
                "ssl": ssl,
                "tunnel_port": tunnel_port
            }
        }
        resp = requests.post(
            url=self.endpoint,
            json=request_data,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        json_response = resp.json()
        return json_response['id']


class CardResource(Resource):

    @property
    def endpoint(self):
        return "{}/api/card".format(self.base_url)

    def get(self, card_id=None):
        url = self.endpoint
        if card_id:
            url = "{}/{}".format(self.endpoint, card_id)
        resp = requests.get(
            url=url,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        return resp.json()

    def get_by_collection(self, collection_slug):
        """
        :param collection_slug:
        :return:
        """
        url = "{}?f=all&collection={}".format(self.endpoint, collection_slug)
        resp = requests.get(
            url=url,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        return resp.json()

    def post(self, database_id, name, query, **kwargs):
        request_data = {
            "name": name,
            "display": kwargs.get('display', 'scalar'),
            "visualization_settings": kwargs.get('visualization_settings', {}),
            "dataset_query": {
                "database": database_id,
                "type": "native",
                "native": {
                    "query": query,
                    "collection": kwargs.get('collection', None),
                    "template_tags": kwargs.get('template_tags', {})
                }
            },
            "description": kwargs.get('description', None),
            "collection_id": kwargs.get('collection_id', None)
        }
        resp = requests.post(
            url=self.endpoint,
            json=request_data,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        json_response = resp.json()
        return json_response['id']

    def put(self, card_id, **kwargs):
        url = "{}/{}".format(self.endpoint, card_id)
        resp = requests.put(
            url=url,
            json=kwargs,
            headers=self.prepare_headers(),
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)

    def delete(self, card_id):
        url = "{}/{}".format(self.endpoint, card_id)
        resp = requests.delete(
            url=url,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)

    def query(self, card_id, parameters=None):
        # TODO : add parameters usage
        url = "{}/{}/query".format(self.endpoint, card_id)
        resp = requests.post(
            url=url,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        return resp.json()

    def download(self, card_id, format, parameters=None):
        url = "{}/{}/query".format(self.endpoint, card_id)
        if format not in ['csv', 'json', 'xlsx']:
            raise ValueError('{} format not supported.'.format(format))
        url = "{}/{}".format(url, format)
        if parameters:
            parameters = urlencode({k: json.dumps(v)
                                    for k, v in parameters.items()})
        resp = requests.post(
            url=url,
            headers=self.prepare_headers(),
            params=parameters, verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        return resp.json()


class CollectionResource(Resource):

    @property
    def endpoint(self):
        return "{}/api/collection".format(self.base_url)

    def get(self, collection_id=None, archived=False):
        url = self.endpoint
        if collection_id:
            url = "{}/{}".format(self.endpoint, collection_id)
        elif archived:
            url = "{}?archived=true"
        resp = requests.get(
            url=url,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        return resp.json()

    def post(self, name, color="#000000", **kwargs):
        request_data = {
            "name": name,
            "description": kwargs.get('description'),
            "color": color
        }
        resp = requests.post(
            url=self.endpoint,
            json=request_data,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        return resp.json()

    def delete(self, collection_id):
        url = "{}/{}".format(self.endpoint, collection_id)
        resp = requests.delete(
            url=url,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)


class UserResource(Resource):

    @property
    def endpoint(self):
        return "{}/api/user".format(self.base_url)

    def get(self, user_id=None):
        url = self.endpoint
        if user_id:
            url = "{}/{}".format(self.endpoint, user_id)

        resp = requests.get(
            url=url,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        return resp.json()

    def current(self):
        url = "{}/current".format(self.endpoint)
        resp = requests.get(
            url=url,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        return resp.json()

    def post(self, first_name, last_name, email, password):
        request_data = {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "password": password
        }
        resp = requests.post(
            url=self.endpoint,
            json=request_data,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        json_response = resp.json()
        return json_response['id']

    def delete(self, user_id):
        url = "{}/{}".format(self.endpoint, user_id)
        resp = requests.delete(
            url=url,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)

    def send_invite(self, user_id):
        url = "{}/{}/send_invite".format(self.endpoint, user_id)
        resp = requests.post(
            url=url,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        return resp.json()

    def password(self, user_id, password, old_password):
        url = "{}/{}/password".format(self.endpoint, user_id)
        request_data = {
            "password": password,
            "old_password": old_password
        }
        resp = requests.put(
            url=url,
            json=request_data,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        return resp.json()


class UtilityResource(Resource):

    @property
    def endpoint(self):
        return "{}/api/util".format(self.base_url)

    def logs(self):
        url = "{}/logs".format(self.endpoint)
        resp = requests.get(
            url=url,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        return resp.json()

    def random_token(self):
        url = "{}/random_token".format(self.endpoint)
        resp = requests.get(
            url=url,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        return resp.json()

    def stats(self):
        url = "{}/stats".format(self.endpoint)
        resp = requests.get(
            url=url,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        return resp.json()

    def password_check(self, password):
        url = "{}/password_check".format(self.endpoint)
        request_data = {
            "password": password,
        }
        resp = requests.post(
            url=url,
            json=request_data,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        return resp.json()

    def connection_pool_info(self):
        url = "{}/diagnostic_info/connection_pool_info".format(self.endpoint)
        resp = requests.get(
            url=url,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        return resp.json()


class DatasetCommand(ApiCommand):

    @staticmethod
    def validate_export_format(export_format_value):
        allowed_export_formats = ['api', 'csv', 'json', 'xlsx']
        if export_format_value not in allowed_export_formats:
            raise ValueError('{} not supported!'.format(export_format_value))

    @property
    def endpoint(self):
        return "{}/api/dataset".format(self.base_url)

    def post(self, database_id, query):
        """ Execute a query and retrieve the results in the usual format."""
        request_data = {
            "type": "native",
            "native": {
                "query": query,
                "template-tags": {}
            },
            "database": database_id,
            "parameters": []
        }
        resp = requests.post(
            url=self.endpoint,
            json=request_data,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        json_response = resp.json()
        return json_response

    def export(self, database_id, query, export_format, full_path=None):
        """ redirects dataset query to available export endpoint,
         saves it in folder given with to_file_path parameter
         or current working directory by default."""

        query_request_data = {
            "type": "native",
            "native": {
                "query": query,
                "template-tags": {}
            },
            "database": database_id,
            "parameters": [],
        }
        request_data = {
            "query": json.dumps(query_request_data)
        }

        headers = self.prepare_headers()
        headers.update({'Content-Type': 'application/x-www-form-urlencoded'})

        DatasetCommand.validate_export_format(
            export_format_value=export_format)
        command_url = "{command_endpoint}/{export_param}".format(
            command_endpoint=self.endpoint,
            export_param=export_format
        )
        resp = requests.post(
            url=command_url,
            data=request_data,
            headers=headers,
            verify=self.verify,
            proxies=self.proxies
        )

        if not full_path:
            file_name = parse_filename_from_response_header(response=resp) \
                        or "metabase_dataset_export.{extension}".format(
                extension=export_format)
            export_file_path = get_file_export_path(file_name=file_name)
        else:
            export_file_path = full_path

        file_access_mode = "w"
        if type(resp.content) == bytes:
            file_access_mode = "wb"

        with open(file=export_file_path, mode=file_access_mode) as f:
            f.write(resp.content)

        return export_file_path

    def duration(self, database_id, query):
        """ Get historical query execution duration. """
        request_data = {
            "type": "native",
            "native": {
                "query": query,
                "template-tags": {}
            },
            "database": database_id,
            "parameters": []
        }
        command_url = "{}/duration".format(self.endpoint)
        resp = requests.post(
            url=command_url,
            json=request_data,
            headers=self.prepare_headers(),
            verify=self.verify,
            proxies=self.proxies
        )
        Resource.validate_response(response=resp)
        json_response = resp.json()
        return json_response


class Client(object):
    def __init__(self, username, password, base_url, **kwargs):
        self.__username = username
        self.__passw = password
        self.base_url = base_url
        self.token = kwargs.get('token')
        self.verify = kwargs.get('verify', True)
        self.proxies = kwargs.get('proxies')

    def __get_auth_url(self):
        return "{}/api/session".format(self.base_url)

    def authenticate(self):
        request_data = {
            "username": self.__username,
            "password": self.__passw
        }
        request_headers = {
            'Content-Type': 'application/json'
        }
        resp = requests.post(
            url=self.__get_auth_url(),
            json=request_data,
            headers=request_headers,
            verify=self.verify,
            proxies=self.proxies
        )

        json_response = resp.json()
        if "id" not in json_response:
            raise AuthorizationFailedException()

        self.token = json_response['id']

    @property
    def databases(self):
        return DatabaseResource(base_url=self.base_url,
                                token=self.token,
                                verify=self.verify)

    @property
    def cards(self):
        return CardResource(base_url=self.base_url,
                            token=self.token,
                            verify=self.verify)

    @property
    def collections(self):
        return CollectionResource(base_url=self.base_url,
                                  token=self.token,
                                  verify=self.verify)

    @property
    def users(self):
        return UserResource(base_url=self.base_url,
                            token=self.token,
                            verify=self.verify)

    @property
    def utils(self):
        return UtilityResource(base_url=self.base_url,
                               token=self.token,
                               verify=self.verify)

    @property
    def dataset(self):
        return DatasetCommand(base_url=self.base_url,
                              token=self.token,
                              verify=self.verify)
