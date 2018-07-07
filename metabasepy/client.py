import requests


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
                    "template_tags": {
                    }
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
