import requests
import json

try:
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlencode


class AuthorizationFailedException(Exception):
    pass


class RequestException(Exception):
    def __init__(self, message=None):
        self.message = message


class Resource(object):
    def __init__(self, **kwargs):
        self.base_url = kwargs.get('base_url')
        self.token = kwargs.get('token', None)

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
            if status_code not in [200, 201]:
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

    def put(self):
        raise NotImplementedError()

    def delete(self, **kwargs):
        raise NotImplementedError()


class DatabaseResource(Resource):

    @property
    def endpoint(self):
        return "{}/api/database".format(self.base_url)

    def get(self, database_id=None):
        url = self.endpoint
        if database_id:
            url = "{}/{}".format(url, database_id)
        resp = requests.get(url=self.endpoint, headers=self.prepare_headers())
        Resource.validate_response(response=resp)
        return resp.json()

    def get_by_name(self, name):
        all_dbs = self.get()
        return [db for db in all_dbs if db['name'] == name]

    def delete(self, database_id):
        url = "{}/{}".format(self.endpoint, database_id)
        resp = requests.delete(url=url,
                               headers=self.prepare_headers())
        Resource.validate_response(resp)

    def post(self, name, engine, host, port, dbname, user, ssl=False,
             tunnel_port=22):
        request_data = {
            "name": name,
            "engine": engine,
            "details": {
                "host": host,
                "port": port,
                "dbname": dbname,
                "user": user,
                "ssl": ssl,
                "tunnel_port": tunnel_port
            }
        }
        resp = requests.post(url=self.endpoint, json=request_data,
                             headers=self.prepare_headers())
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
        resp = requests.get(url=url, headers=self.prepare_headers())
        Resource.validate_response(response=resp)
        return resp.json()

    def get_by_collection(self, collection_slug):
        """
        :param collection_slug:
        :return:
        """
        url = "{}?f=all&collection={}".format(self.endpoint, collection_slug)
        resp = requests.get(url=url, headers=self.prepare_headers())
        Resource.validate_response(response=resp)
        return resp.json()

    def post(self, database_id, name, query, **kwargs):
        request_data = {
            "name": name,
            "display": "scalar",
            "visualization_settings": {
            },
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
        resp = requests.post(url=self.endpoint, json=request_data,
                             headers=self.prepare_headers())
        Resource.validate_response(response=resp)
        json_response = resp.json()
        return json_response['id']

    def delete(self, card_id):
        url = "{}/{}".format(self.endpoint, card_id)
        resp = requests.delete(url=url,
                               headers=self.prepare_headers())
        Resource.validate_response(response=resp)

    def query(self, card_id, parameters=None):
        # TODO : add parameters usage
        url = "{}/{}/query".format(self.endpoint, card_id)
        resp = requests.post(url=url, headers=self.prepare_headers())
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
        resp = requests.post(url=url, headers=self.prepare_headers(),
                             params=parameters)
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
        resp = requests.get(url=url, headers=self.prepare_headers())
        Resource.validate_response(response=resp)
        return resp.json()

    def post(self, name, color="#000000", **kwargs):
        request_data = {
            "name": name,
            "description": kwargs.get('description'),
            "color": color
        }
        resp = requests.post(url=self.endpoint, json=request_data,
                             headers=self.prepare_headers())
        Resource.validate_response(response=resp)
        return resp.json()

    def delete(self, collection_id):
        url = "{}/{}".format(self.endpoint, collection_id)
        resp = requests.delete(url=url,
                               headers=self.prepare_headers())
        Resource.validate_response(response=resp)


class UserResource(Resource):

    @property
    def endpoint(self):
        return "{}/api/user".format(self.base_url)

    def get(self, user_id=None):
        url = self.endpoint
        if user_id:
            url = "{}/{}".format(self.endpoint, user_id)

        resp = requests.get(url=url, headers=self.prepare_headers())
        Resource.validate_response(response=resp)
        return resp.json()

    def current(self):
        url = "{}/current".format(self.endpoint)
        resp = requests.get(url=url, headers=self.prepare_headers())
        Resource.validate_response(response=resp)
        return resp.json()

    def post(self, first_name, last_name, email, password):
        request_data = {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "password": password
        }
        resp = requests.post(url=self.endpoint, json=request_data,
                             headers=self.prepare_headers())
        Resource.validate_response(response=resp)
        json_response = resp.json()
        return json_response['id']

    def delete(self, user_id):
        url = "{}/{}".format(self.endpoint, user_id)
        resp = requests.delete(url=url,
                               headers=self.prepare_headers())
        Resource.validate_response(response=resp)

    def send_invite(self, user_id):
        url = "{}/{}/send_invite".format(self.endpoint, user_id)
        resp = requests.post(url=url, headers=self.prepare_headers())
        Resource.validate_response(response=resp)
        return resp.json()

    def password(self, user_id, password, old_password):
        url = "{}/{}/password".format(self.endpoint, user_id)
        request_data = {
            "password": password,
            "old_password": old_password
        }
        resp = requests.put(url=url, json=request_data,
                            headers=self.prepare_headers())
        Resource.validate_response(response=resp)
        return resp.json()


class UtilityResource(Resource):

    @property
    def endpoint(self):
        return "{}/api/util".format(self.endpoint)

    def logs(self):
        url = "{}/logs".format(self.endpoint)
        resp = requests.get(url=url, headers=self.prepare_headers())
        Resource.validate_response(response=resp)
        return resp.json()

    def random_token(self):
        url = "{}/random_token".format(self.endpoint)
        resp = requests.get(url=url, headers=self.prepare_headers())
        Resource.validate_response(response=resp)
        return resp.json()

    def stats(self):
        url = "{}/stats".format(self.endpoint)
        resp = requests.get(url=url, headers=self.prepare_headers())
        Resource.validate_response(response=resp)
        return resp.json()

    def password_check(self, password):
        url = "{}/password_check".format(self.endpoint)
        request_data = {
            "password": password,
        }
        resp = requests.post(url=url, json=request_data,
                             headers=self.prepare_headers())
        Resource.validate_response(response=resp)
        return resp.json()


class Client(object):
    def __init__(self, username, password, base_url, **kwargs):
        self.__username = username
        self.__passw = password
        self.base_url = base_url
        self.token = kwargs.get('token', None)

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
        resp = requests.post(url=self.__get_auth_url(), json=request_data,
                             headers=request_headers)

        json_response = resp.json()
        if "id" not in json_response:
            raise AuthorizationFailedException()

        self.token = json_response['id']

    @property
    def databases(self):
        return DatabaseResource(base_url=self.base_url, token=self.token)

    @property
    def cards(self):
        return CardResource(base_url=self.base_url, token=self.token)

    @property
    def collections(self):
        return CollectionResource(base_url=self.base_url, token=self.token)

    @property
    def users(self):
        return UserResource(base_url=self.base_url, token=self.token)

    @property
    def utils(self):
        return UtilityResource(base_url=self.base_url, token=self.token)
