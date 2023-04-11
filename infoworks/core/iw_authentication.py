#!/usr/bin/env python
import traceback
import requests
import logging
import infoworks.error
from infoworks.sdk import url_builder, local_configurations
from infoworks.sdk.utils import IWUtils


def validate_bearer_token(protocol, ip, port, delegation_token):
    client_config = {"protocol": protocol, "ip": ip, "port": port}
    headers = {'Authorization': 'Bearer ' + delegation_token, 'Content-Type': 'application/json'}
    response = IWUtils.ejson_deserialize(
        requests.get(url_builder.validate_bearer_token_url(client_config), headers=headers, verify=False).content)
    if len(response.get("result", {})) > 0:
        return response.get("result")["is_valid"]
    else:
        return False


def get_bearer_token(protocol, ip, port, refresh_token, http_client=None):
    if any([protocol is None, ip is None, port is None, refresh_token is None]):
        raise infoworks.error.AuthenticationError("Some of the arguments are invalid or has None")
    client_config = {"protocol": protocol, "ip": ip, "port": port}
    headers = {'Authorization': 'Basic ' + refresh_token, 'Content-Type': 'application/json'}
    if http_client is not None:
        response = http_client.get(url_builder.get_bearer_token_url(client_config), headers=headers,
                                   timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC,
                                   verify=False)
    else:
        response = requests.get(url_builder.get_bearer_token_url(client_config), headers=headers,
                                timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC,
                                verify=False)
    if response:
        delegation_token = response.json().get("result").get("authentication_token")
        return delegation_token
    else:
        traceback.print_stack()
        raise infoworks.error.AuthenticationError("Refresh token invalid")


def is_token_valid(client_config, http_client=None):
    try:
        if client_config['bearer_token'] is None:
            logging.info('Invalid Delegation token: ' + str(client_config['bearer_token']))
            return False
        if http_client is not None:
            result = http_client.get(
                url_builder.validate_bearer_token_url(client_config),
                timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC,
                headers=IWUtils.get_default_header_for_v3(client_config['bearer_token']), verify=False
            )
        else:
            result = requests.get(
                url_builder.validate_bearer_token_url(client_config),
                timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC,
                headers=IWUtils.get_default_header_for_v3(client_config['bearer_token']), verify=False
            )
        response = IWUtils.ejson_deserialize(result.content)
        logging.info('Response from rest api: ' + str(response))
        return response.get('result', {}).get('is_valid', False)
    except Exception as e:
        logging.error('Failed to check token validity: ' + str(e))
        return False
