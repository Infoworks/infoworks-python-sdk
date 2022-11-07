import json
import logging.config
import os
from configparser import ConfigParser
from pathlib import Path
import requests
from google.cloud import kms, storage
import boto3
import base64
from cryptography.fernet import Fernet
from infoworks.core.iw_authentication import is_token_valid, get_bearer_token, aes_encrypt
from infoworks.sdk import local_configurations
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from infoworks.sdk.utils import IWUtils


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
            if response.status_code == 406:
                headers = self.regenerate_bearer_token_if_needed(headers)
                return self.http.get(url, headers=headers, timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC,
                                     verify=False, data=data)
            else:
                return response
        elif method.upper() == "POST":
            self.logger.info(f"Calling {url}")
            response = self.http.post(url, headers=headers, json=data,
                                      timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC, verify=False)
            if response.status_code == 406:
                headers = self.regenerate_bearer_token_if_needed(headers)
                return self.http.post(url, headers=headers, json=data,
                                      timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC, verify=False)
            else:
                return response
        elif method.upper() == "PUT":
            self.logger.info(f"Calling {url}")
            response = self.http.put(url, headers=headers, json=data,
                                     timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC, verify=False)
            if response.status_code == 406:
                headers = self.regenerate_bearer_token_if_needed(headers)
                return self.http.put(url, headers=headers, json=data,
                                     timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC, verify=False)
            else:
                return response
        elif method.upper() == "PATCH":
            self.logger.info(f"Calling {url}")
            response = self.http.patch(url, headers=headers, json=data,
                                       timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC, verify=False)
            if response.status_code == 406:
                headers = self.regenerate_bearer_token_if_needed(headers)
                return self.http.put(url, headers=headers, json=data,
                                     timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC, verify=False)
            else:
                return response
        elif method.upper() == "DELETE":
            self.logger.info(f"Calling {url}")
            response = self.http.delete(url, headers=headers, timeout=local_configurations.REQUEST_TIMEOUT_IN_SEC,
                                        verify=False)
            if response.status_code == 406:
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

    def get_all_secrets(self, secret_type, ini_config_file_path=None, keys=None):
        # Read all secrets from file and load inmemory
        # Encrypt using infoworks encoding
        if secret_type == "gcp_kms" and ini_config_file_path is not None:
            config = ConfigParser()
            config.optionxform = str
            config.read(ini_config_file_path)
            # Cloud KMS CryptoKey Encrypter
            service_json_path = config.get("gcp_kms", "service_json_path")
            # Reading Config Parameters
            projectId = config.get("gcp_kms", "projectId")
            locationId = config.get("gcp_kms", "locationId")
            keyRingId = config.get("gcp_kms", "keyRingId")
            keyId = config.get("gcp_kms", "keyId")
            bucketName = config.get("gcp_kms", "bucketName")
            blobName = config.get("gcp_kms", "encrypted_key_filename")
            # Reading Encrypted Password File from Cloud Storage
            storageClient = storage.Client.from_service_account_json(service_json_path)
            bucket = storageClient.get_bucket(bucketName)
            blobFile = bucket.get_blob(blobName)
            cipherText = blobFile.download_as_string()
            kmsClient = kms.KeyManagementServiceClient.from_service_account_json(service_json_path)
            keyName = kmsClient.crypto_key_path(projectId, locationId, keyRingId, keyId)
            # Decrypt Password
            try:
                decryptPassword = kmsClient.decrypt(request={'name': keyName, 'ciphertext': cipherText})
                plainText = str(decryptPassword.plaintext, 'utf-8')
                plainTextStr = plainText.strip()
                passwordsDictPlain = json.loads(plainTextStr)
                passwordsDictEncrypted = {}
                for key in passwordsDictPlain:
                    passwordsDictEncrypted[key] = aes_encrypt(passwordsDictPlain[key])
                self.secrets_config = passwordsDictEncrypted
            except Exception as e:
                print(str(e))
                return ""
        elif secret_type == "aws_kms" and ini_config_file_path is not None:
            config = ConfigParser()
            config.optionxform = str
            config.read(ini_config_file_path)
            # Read the encrypted file into memory
            encrypted_filename = config.get("aws_kms", "encrypted_filename")
            available_keys = dict(config.items("aws_kms")).keys()
            if "access_id" in available_keys and "secret_key" in available_keys:
                ACCESS_KEY = config.get("aws_kms", "access_id")
                SECRET_KEY = config.get("aws_kms", "secret_key")
            else:
                ACCESS_KEY, SECRET_KEY = None, None
            # Read
            NUM_BYTES_FOR_LEN = 4
            try:
                with open(encrypted_filename, 'rb') as file:
                    file_contents = file.read()
            except IOError as e:
                self.logger.error(str(e))
                return False
            data_key_encrypted_len = int.from_bytes(file_contents[:NUM_BYTES_FOR_LEN],
                                                    byteorder='big') + NUM_BYTES_FOR_LEN
            data_key_encrypted = file_contents[NUM_BYTES_FOR_LEN:data_key_encrypted_len]
            # Decrypt the data key before using it
            if ACCESS_KEY is not None and SECRET_KEY is not None:
                kms_client = boto3.client('kms', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
            else:
                kms_client = boto3.client('kms')
            try:
                response = kms_client.decrypt(CiphertextBlob=data_key_encrypted)
                data_key_plaintext = base64.b64encode((response['Plaintext']))
            except Exception as e:
                self.logger.error(str(e))
                return False

            if data_key_plaintext is None:
                return False
            # Decrypt the rest of the file
            f = Fernet(data_key_plaintext)
            file_contents_decrypted = f.decrypt(file_contents[data_key_encrypted_len:])
            passwordsDictPlain = json.loads(file_contents_decrypted)
            passwordsDictEncrypted = {}
            for key in passwordsDictPlain:
                passwordsDictEncrypted[key] = aes_encrypt(passwordsDictPlain[key])
            self.secrets_config = passwordsDictEncrypted
            return True
        elif secret_type == "aws_secrets":
            ACCESS_KEY, SECRET_KEY = None, None
            region = 'us-west-2'
            if ini_config_file_path is not None:
                config = ConfigParser()
                config.optionxform = str
                config.read(ini_config_file_path)
                available_keys = dict(config.items("aws_secrets")).keys()
                if "access_id" in available_keys and "secret_key" in available_keys:
                    ACCESS_KEY = config.get("aws_secrets", "access_id")
                    SECRET_KEY = config.get("aws_secrets", "secret_key")
                else:
                    ACCESS_KEY, SECRET_KEY = None, None
                if "region" in available_keys:
                    region = config.get("aws_secrets", "region")
            if keys:
                output_values = []
                for key in keys.split(","):
                    secrets_client = boto3.client("secretsmanager", region_name=region)
                    kwargs = {'SecretId': key}
                    try:
                        value = secrets_client.get_secret_value(**kwargs)["SecretString"]
                        self.secrets_config[key] = value
                        output_values.append(value)
                    except Exception as e:
                        self.logger.error(str(e))
                return output_values
            else:
                # Get all the secrets
                if ACCESS_KEY is not None and SECRET_KEY is not None:
                    secrets_client = boto3.client("secretsmanager", aws_access_key_id=ACCESS_KEY,
                                                  aws_secret_access_key=SECRET_KEY, region=region)
                else:
                    secrets_client = boto3.client("secretsmanager",region=region)
                temp = secrets_client.list_secrets()["SecretList"]
                all_secrets = [i["Name"] for i in temp]
                for key in all_secrets:
                    kwargs = {'SecretId': key}
                    try:
                        value = secrets_client.get_secret_value(**kwargs)["SecretString"]
                        self.secrets_config[key] = value
                    except Exception as e:
                        self.logger.error(str(e))
        elif secret_type == "azure_keyvault" and ini_config_file_path is not None:
            from azure.keyvault.secrets import SecretClient
            from azure.identity import ClientSecretCredential
            config = ConfigParser()
            config.optionxform = str
            config.read(ini_config_file_path)
            # Read the encrypted file into memory
            available_keys = dict(config.items("azure_keyvault")).keys()
            if all(x in ['keyVaultName', 'tenant_id', 'client_id', 'client_secret'] for x in available_keys):
                keyVaultName = config.get("azure_keyvault", "keyVaultName")
                KVUri = f"https://{keyVaultName}.vault.azure.net"
                tenant_id = config.get("azure_keyvault", "tenant_id")
                client_id = config.get("azure_keyvault", "client_id")
                client_secret = config.get("azure_keyvault", "client_secret")
                credential = ClientSecretCredential(tenant_id, client_id, client_secret)
                client = SecretClient(vault_url=KVUri, credential=credential)
                output_values=[]
                if keys:
                    for key in keys.split(","):
                        retrieved_secret = client.get_secret(key)
                        self.secrets_config[key] = aes_encrypt(retrieved_secret.value)
                        output_values.append(aes_encrypt(retrieved_secret.value))
                else:
                    secret_properties = client.list_properties_of_secrets()
                    all_secrets = [i.name for i in secret_properties]
                    for key in all_secrets:
                        retrieved_secret = client.get_secret(key)
                        self.secrets_config[key] = aes_encrypt(retrieved_secret.value)
                        output_values.append(retrieved_secret.value)
                return output_values

        self.secrets_config["custom_secrets_read"] = True
