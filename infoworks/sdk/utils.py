import logging
import urllib.request, urllib.parse, urllib.error
import json

import requests
from bson import ObjectId
from infoworks.sdk import ejson, local_configurations
from infoworks.sdk.local_configurations import Response
from infoworks.sdk import url_builder


def validate_client_configs(client_config, bearer_token=None):
    """
    validates client configurations against null values
    :param client_config: client configurations
    :type client_config: dict
    :param bearer_token: bearer_token to authenticate the requests
    :return: boolean true of false based on success
    """
    if bearer_token is not None:
        client_config['auth_token'] = bearer_token

    for element in client_config.items():
        if not element[1]:
            return False, 'Missing {parameter} in client configurations'.format(parameter=element)
    return True, 'YAY!!!! client configurations are correct'


class IWUtils(object):

    def __init__(self):
        pass

    @staticmethod
    # returns a encoded string associated with the value provided. Throws exception in case of error.
    def encode(value):
        return urllib.parse.quote(value)

    @staticmethod
    def ejson_deserialize(data):
        try:
            cust_hooks = {
                'oid': ObjectId
            }
            return ejson.loads(data, custom_type_hooks=cust_hooks)
        except Exception as e:
            print('Exception: ' + str(e))
            raise RuntimeError(str(e))

    @staticmethod
    def ejson_serialize(data):
        try:
            cust_hooks = [(ObjectId, 'oid', str)]
            return ejson.dumps(data, custom_type_hooks=cust_hooks)
        except Exception as e:
            print('Exception: ' + str(e))
            raise RuntimeError(str(e))

    @staticmethod
    # returns polling frequency for polling methods
    def get_polling_frequency_in_secs():
        return local_configurations.POLLING_FREQUENCY_IN_SEC

    @staticmethod
    def get_polling_failure_retries():
        return local_configurations.NUM_POLLING_RETRIES

    @staticmethod
    def get_default_header_for_v3(delegation_token):
        return {'Authorization': 'Bearer {del_token}'.format(del_token=delegation_token)}

    @staticmethod
    def get_query_params_string_from_dict(params=None):
        if not params:
            return ""
        string = "?"
        string = string + "&limit=" + str(params.get('limit')) if params.get('limit') else string
        string = string + "&sort_by=" + str(params.get('sort_by')) if params.get('sort_by') else string
        string = string + "&order_by=" + str(params.get('order_by')) if params.get('order_by') else string
        string = string + "&offset=" + str(params.get('offset')) if params.get('offset') else string
        string = string + "&filter=" + urllib.parse.quote(json.dumps(params.get('filter'))) if params.get(
            'filter') else string
        string = string + "&projections=" + urllib.parse.quote(json.dumps(params.get('projections'))) if params.get(
            'projections') else string
        return string

    @staticmethod
    def get_parent_entity(entity_type, entity_id, client_config):
        try:
            if entity_id is None or entity_type is None:
                return Response.Status.ERROR, 'Failed to get Parent Id for: {entity_type}: {entity_id}'.format(
                    entity_type=entity_type, entity_id=entity_id)
            data = {
                'entity_type': entity_type,
                'entity_id': entity_id
            }
            result = requests.get(
                url_builder.get_parent_entity_url(client_config),
                json=data
            )

            response = IWUtils.ejson_deserialize(result.content)
            if str(result.status_code) != '200' or 'result' not in response:
                logging.error('Failed to get Parent Id for: {}: {}', entity_type, entity_id)
                return Response.Status.ERROR, 'Failed to get Parent Id for: {entity_type}: {entity_id}'.format(
                    entity_type=entity_type, entity_id=entity_id)
            return Response.Status.SUCCESS, response.get('result', [])
        except Exception as e:
            logging.error('Exception:' + str(e))
            return Response.Status.ERROR, 'Exception: ' + str(e)

    @staticmethod
    def is_json(myjson):
        try:
            json.loads(myjson)
        except ValueError as e:
            return False
        return True
