class ConfigurationException(Exception):
    def __init__(self, msg=None, *args, **kwargs):
        Exception.__init__(self, msg, *args, **kwargs)


class InvalidCardException(Exception):
    def __init__(self, msg=None, *args, **kwargs):
        Exception.__init__(self, msg, *args, **kwargs)


class CollectionException(Exception):
    def __init__(self, msg=None, *args, **kwargs):
        Exception.__init__(self, msg, *args, **kwargs)