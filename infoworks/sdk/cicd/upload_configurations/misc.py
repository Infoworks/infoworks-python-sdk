from functools import reduce
import json


def deep_get(dictionary, keys, default=None):
    return reduce(lambda d, key: d.get(key, default) if isinstance(d, dict) else default, keys.split("."), dictionary)


def get_data(source_config_path, keys):
    with open(source_config_path, 'r') as f:
        json_dict = json.load(f)
    return deep_get(json_dict, keys)


class CustomError(Exception):
    def __init__(self, message):
        self.message = message
        super(CustomError, self).__init__(self.message)
