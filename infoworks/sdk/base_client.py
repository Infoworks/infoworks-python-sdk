import logging.config
import os
from configparser import ConfigParser
from pathlib import Path
import requests
from infoworks.core.iw_authentication import is_token_valid, get_bearer_token
from infoworks.sdk import local_configurations
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from infoworks.sdk.utils import IWUtils
import urllib3
urllib3.disable_warnings()


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = local_configurations.REQUEST_TIMEOUT_IN_SEC
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


def initialise_http_client():
    retries = Retry(total=local_configurations.MAX_RETRIES, backoff_factor=1,
                    status_forcelist=[429, 500, 502, 503, 504])
    http = requests.Session()
    adapter = TimeoutHTTPAdapter(max_retries=retries)
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    return http


class BaseClient(object):
    def __init__(self):
        # print("Inside BaseClient")
        self.client_config = {
            'protocol': None,
            'ip': None,
            'port': None,
            'refresh_token': None,
            'bearer_token': None,
            'default_environment_id': None,
            'default_storage_id': None,
            'default_compute_id': None
        }
        self.secrets_config = {"custom_secrets_read": False}
        self.mappings = {}
        self.http = initialise_http_client()
        log_path = Path(local_configurations.LOG_LOCATION)
        if os.path.isdir(log_path.parent.absolute()):
            logging.basicConfig(filename=local_configurations.LOG_LOCATION, filemode='w',
                                format='%(asctime)s - %(module)s - %(pathname)s - %(lineno)d - %(levelname)s - %(message)s',
                                level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
        else:
            logging.basicConfig(
                format='%(asctime)s - %(module)s - %(pathname)s - %(lineno)d - %(levelname)s - %(message)s',
                level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
        self.logger = logging.getLogger('infoworks_sdk_logs')

    def initialize_client(self, protocol=None, ip=None, port=None, ):
        """
        initialize the client
        :param protocol: protocol to be used for server call
        :type protocol: String
        :param ip: Client IP
        :type ip: String
        :param port: Client Port
        :type port: String
        """

        if all(v is not None for v in [protocol, ip, port]):
            self.client_config['protocol'] = protocol
            self.client_config['ip'] = ip
            self.client_config['port'] = port
        else:
            logging.error(ValueError("Pass valid values"))
            raise ValueError("Pass valid values")

    def initialize_client_with_defaults(self, protocol, ip, port, refresh_token, default_environment_id=None,
                                        default_storage_id=None, default_compute_id=None):
        """
        initializes the client and a user with given configuration
        :param protocol: protocol to be used for server call
        :type protocol: String
        :param ip: ip address of the server
        :type ip: String
        :param port: port on which the rest service resides
        :type port: String
        :param refresh_token: refresh_token of the user
        :type refresh_token: String
        :param default_compute_id: Pass the default compute id to be used for all the artifacts to be created
        :param default_storage_id: Pass the default storage id to be used for all the artifacts to be created
        :param default_environment_id: Pass the default environment id to be used for all the artifacts to be created
        """
        self.client_config['refresh_token'] = refresh_token
        self.client_config['bearer_token'] = get_bearer_token(protocol, ip, port, refresh_token)
        self.initialize_client(protocol, ip, port)
        self.client_config[
            'default_environment_id'] = default_environment_id if default_environment_id is not None else None
        self.client_config['default_storage_id'] = default_storage_id if default_storage_id is not None else None
        self.client_config['default_compute_id'] = default_compute_id if default_compute_id is not None else None

    def get_configurations(self):
        """
        returns client configurations
        :return: configuration dictionary
        """
        return self.client_config

    def regenerate_bearer_token_if_needed(self, headers):
        if not is_token_valid(self.client_config, self.http):
            self.client_config['bearer_token'] = get_bearer_token(protocol=self.client_config["protocol"],
                                                                  ip=self.client_config["ip"],
                                                                  port=self.client_config["port"],
                                                                  refresh_token=self.client_config["refresh_token"])
            headers = IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
        return headers

    def call_api(self, method, url, headers=None, data=None):
        # headers = self.regenerate_bearer_token_if_needed(headers)
        if method.upper() == "GET":
            self.logger.info(f"Calling {url}")
            response = self.http.get(url, headers=headers, timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC,
                                     verify=False, data=data)
            if response.status_code in [401, 406]:
                headers = self.regenerate_bearer_token_if_needed(headers)
                return self.http.get(url, headers=headers, timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC,
                                     verify=False, data=data)
            else:
                return response
        elif method.upper() == "POST":
            self.logger.info(f"Calling {url}")
            response = self.http.post(url, headers=headers, json=data,
                                      timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC, verify=False)
            if response.status_code in [401, 406]:
                headers = self.regenerate_bearer_token_if_needed(headers)
                return self.http.post(url, headers=headers, json=data,
                                      timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC, verify=False)
            else:
                return response
        elif method.upper() == "PUT":
            self.logger.info(f"Calling {url}")
            response = self.http.put(url, headers=headers, json=data,
                                     timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC, verify=False)
            if response.status_code in [401, 406]:
                headers = self.regenerate_bearer_token_if_needed(headers)
                return self.http.put(url, headers=headers, json=data,
                                     timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC, verify=False)
            else:
                return response
        elif method.upper() == "PATCH":
            self.logger.info(f"Calling {url}")
            response = self.http.patch(url, headers=headers, json=data,
                                       timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC, verify=False)
            if response.status_code in [401, 406]:
                headers = self.regenerate_bearer_token_if_needed(headers)
                return self.http.put(url, headers=headers, json=data,
                                     timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC, verify=False)
            else:
                return response
        elif method.upper() == "DELETE":
            self.logger.info(f"Calling {url}")
            response = self.http.delete(url, headers=headers, timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC,
                                        verify=False)
            if response.status_code in [401, 406]:
                headers = self.regenerate_bearer_token_if_needed(headers)
                return self.http.delete(url, headers=headers, timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC,
                                        verify=False)
            else:
                return response

    def get_mappings_from_config_file(self, ini_config_file_path):
        config = ConfigParser()
        config.optionxform = str
        config.read(ini_config_file_path)
        if "environment" in config:
            default_environment_id = config.get("environment", "environment_id")
            default_storage_id = config.get("environment", "storage_id")
            default_interactive_id = config.get("environment", "interactive_id")
            self.client_config["default_environment_id"] = default_environment_id
            self.client_config["default_storage_id"] = default_storage_id
            self.client_config["default_compute_id"] = default_interactive_id
        if "environment_mappings" in config:
            environment_mappings = dict(config.items("environment_mappings"))
            self.mappings["environment_mappings"] = environment_mappings
        if "storage_mappings" in config:
            storage_mappings = dict(config.items("storage_mappings"))
            self.mappings["storage_mappings"] = storage_mappings
        if "compute_mappings" in config:
            compute_mappings = dict(config.items("compute_mappings"))
            self.mappings["compute_mappings"] = compute_mappings
        if "gcp_details" in config:
            gcp_details = dict(config.items("gcp_details"))
            self.mappings["gcp_details"] = gcp_details
        if "gcp_project_id_mappings" in config:
            gcp_project_id_mappings = dict(config.items("gcp_project_id_mappings"))
            self.mappings["gcp_project_id_mappings"] = gcp_project_id_mappings
        if "service_json_mappings" in config:
            service_json_mappings = dict(config.items("service_json_mappings"))
            self.mappings["service_json_mappings"] = service_json_mappings
        if "table_group_compute_mappings" in config:
            table_group_compute_mappings = dict(config.items("table_group_compute_mappings"))
            self.mappings["table_group_compute_mappings"] = table_group_compute_mappings
        if "kms" in config:
            kms = dict(config.items("kms"))
            self.mappings["kms"] = kms
        for section in config.sections():
            self.mappings[section] = dict(config.items(section))

    def _topological_sort_grouping(self, g):
        # copy the graph
        _g = g.copy()
        res = []
        # while _g is not empty
        while _g:
            zero_indegree = [v for v, d in _g.in_degree() if d == 0]
            res.append(zero_indegree)
            _g.remove_nodes_from(zero_indegree)
        return res

