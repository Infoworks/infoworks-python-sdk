import json
import time
import traceback

from infoworks.error import GenericError
from infoworks.sdk import url_builder, local_configurations
from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.generic_response import GenericResponse
from infoworks.sdk.local_configurations import Response, ErrorCode
from infoworks.sdk.source_response import SourceResponse
from infoworks.sdk.utils import IWUtils


class ReplicatorClient(BaseClient):
    def __init__(self):
        super(ReplicatorClient, self).__init__()

    # Replicator Sources
    def get_list_of_replicator_sources(self, params=None):
        """
        Function to list the replicator sources
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type params: JSON dict
        :return: response dict
        """
        response = None
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_replicator_sources = url_builder.list_replicator_sources_url(
            self.client_config) + IWUtils.get_query_params_string_from_dict(params=params)
        replicator_source_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_replicator_sources,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            # print(json.dumps(response))
            if response is not None:
                initial_msg = response.get("message", "")
                result = response.get("result", {})
                records = result.get("records", [])
                while len(records) > 0:
                    replicator_source_list.extend(records)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", {})
                    records = result.get("records", [])
            else:
                self.logger.error("Failed to get list of replicator sources")
                return GenericResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                    error_desc="Failed to get list of replicator sources",
                                                    response=response)
            response["result"] = replicator_source_list
            response["message"] = initial_msg
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.error("Error in listing replicator sources")
            raise GenericError("Error in listing replicator sources " + str(e))

    def create_replicator_source(self, source_config):
        """
        Create a new Infoworks Replicator source
        :param source_config: a JSON object containing source configurations
        :type source_config: JSON Object
        ```
            source_config_example = {
              "name": "Sample source",
              "type": "hive",
              "properties": {
                "hadoop_version": "2.x",
                "hdfs_root": "wasb://hdfsstorage.blob.core.windows.net/",
                "hive_metastore_url": "thrift://microsoft.com:9083",
                "network_throttling": 20000,
                "temp_directory": "/root/iwxr/temp",
                "output_directory": "/root/iwxr/output"
              },
              "advanced_configurations": [
                {
                  "key": "DATABASE_FILTER",
                  "value": ".*"
                }
              ]
            }
        ```
        :return: response dict
        """
        response = None
        if source_config is None:
            self.logger.error("source config cannot be None")
            raise Exception("source config cannot be None")
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", url_builder.create_replicator_source_url(self.client_config),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              source_config).content)

            source_id = response.get('result', None)
            if source_id is None:
                self.logger.error('Cannot create a new source.')
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc='Failed to create source',
                                                    response=response)
            else:
                source_id = str(source_id)
                self.logger.info('Source {id} has been created.'.format(id=source_id))
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to create a new source.')
            raise GenericError(response.get("message", "Error occurred while trying to create a new source."))

    def get_replicator_source(self, source_id):
        """
        Function to retrieve the details of a specific replicator source
        :param source_id: Identifier of Source
        :type source_id: String
        :return: response dict
        """
        response = None
        try:
            if source_id is None:
                self.logger.error("source id cannot be None")
                raise Exception("source id cannot be None")

            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_builder.get_replicator_source_url(self.client_config, source_id=source_id),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)

            source_id = response.get('result', {}).get('id')
            if source_id is not None:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc="Failed to retrieve Source Details",
                                                    response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.error("Error in retrieving replicator source")
            raise GenericError("Error in retrieving replicator source " + str(e))

    def update_replicator_source(self, source_id, source_config):
        """
        Update Infoworks Replicator source
        :param source_id: Identifier of Source
        :type source_id: String
        :param source_config: a JSON object containing source configurations
        :type source_config: JSON Object
        ```
            source_config_example = {
              "name": "Sample source",
              "type": "hive",
              "properties": {
                "hadoop_version": "2.x",
                "hdfs_root": "wasb://hdfsstorage.blob.core.windows.net/",
                "hive_metastore_url": "thrift://microsoft.com:9083",
                "network_throttling": 20000,
                "temp_directory": "/root/iwxr/temp",
                "output_directory": "/root/iwxr/output"
              },
              "advanced_configurations": [
                {
                  "key": "DATABASE_FILTER",
                  "value": ".*"
                }
              ]
            }
        ```
        :return: response dict
        """
        response = None
        if source_id is None or source_config is None:
            self.logger.error("source_id / source_config cannot be None")
            raise Exception("source_id / source_config cannot be None")
        try:
            api_response = self.call_api("PATCH", url_builder.get_replicator_source_url(self.client_config, source_id),
                                         IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                         source_config)

            response = IWUtils.ejson_deserialize(api_response.content)
            if api_response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc="Failed to update Source",
                                                    response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to update source.')
            raise GenericError(response.get("message", "Error occurred while trying to update source."))

    def delete_replicator_source(self, source_id):
        """
        Function to delete a specific replicator source
        :param source_id: Identifier of Source
        :type source_id: String
        :return: response dict
        """
        response = None
        try:
            if source_id is None:
                self.logger.error("source id cannot be None")
                raise Exception("source id cannot be None")

            api_response = self.call_api("DELETE",
                                         url_builder.get_replicator_source_url(self.client_config, source_id=source_id),
                                         IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))

            response = IWUtils.ejson_deserialize(api_response.content)

            if api_response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc="Failed to delete Source",
                                                    response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.error("Error in deleting replicator source")
            raise GenericError("Error in deleting replicator source " + str(e))

    # Replicator Destinations
    def get_list_of_replicator_destinations(self, params=None):
        """
        Function to list the replicator destinations
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type params: JSON dict
        :return: response dict
        """
        response = None
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_replicator_destinations = url_builder.list_replicator_destinations_url(
            self.client_config) + IWUtils.get_query_params_string_from_dict(params=params)
        replicator_destinations_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_replicator_destinations,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)

            if response is not None:
                initial_msg = response.get("message", "")
                result = response.get("result", {})
                records = result.get("records", [])
                while len(records) > 0:
                    replicator_destinations_list.extend(records)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", {})
                    records = result.get("records", [])
            else:
                self.logger.error("Failed to get list of replicator destinations")
                return GenericResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                    error_desc="Failed to get list of replicator destinations",
                                                    response=response)
            response["result"] = replicator_destinations_list
            response["message"] = initial_msg
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.error("Error in listing replicator destinations")
            raise GenericError("Error in listing replicator destinations " + str(e))

    def create_replicator_destination(self, destination_config):
        """
        Create a new Infoworks Replicator destination
        :param destination_config: a JSON object containing destination configurations
        :type destination_config: JSON Object
        ```
            destination_config_example = {
                "name": "Sample Destination",
                "type": "hive",
                "properties": {
                "hadoop_version": "2.x",
                "hdfs_root": "wasb://hdfsstorage.blob.core.windows.net/",
                "hive_metastore_url": "thrift://microsoft.com:9083",
                "temp_directory": "/root/iwxr/temp",
                "output_directory": "/root/iwxr/output"
                },
                "advanced_configurations": [
                    {
                      "key": "DATABASE_FILTER",
                      "value": ".*"
                    }
                ]
            }
        ```
        :return: response dict
        """
        response = None
        if destination_config is None:
            self.logger.error("destination config cannot be None")
            raise Exception("destination config cannot be None")
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", url_builder.create_replicator_destination_url(self.client_config),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              destination_config).content)

            destination_id = response.get('result', None)
            if destination_id is None:
                self.logger.error('Cannot create a new destination.')
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc='Failed to create destination',
                                                    response=response)
            destination_id = str(destination_id)
            self.logger.info('Destination {id} has been created.'.format(id=destination_id))
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to create a new destination.')
            raise GenericError(response.get("message", "Error occurred while trying to create a new destination."))

    def get_replicator_destination(self, destination_id):
        """
        Function to retrieve the details of a specific replicator destination
        :param destination_id: Identifier of Destination
        :type destination_id: String
        :return: response dict
        """
        response = None
        try:
            if destination_id is None:
                self.logger.error("destination id cannot be None")
                raise Exception("destination id cannot be None")

            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_builder.get_replicator_destination_url(self.client_config,
                                                                                destination_id=destination_id),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)

            destination_id = response.get('result', {}).get('id')
            if destination_id is not None:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc="Failed to retrieve Destination Details",
                                                    response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.error("Error in retrieving replicator Destination")
            raise GenericError("Error in retrieving replicator Destination " + str(e))

    def update_replicator_destination(self, destination_id, destination_config):
        """
        Update Infoworks Replicator source
        :param destination_id: Identifier of Destination
        :type destination_id: String
        :param destination_config: a JSON object containing destination configurations
        :type destination_config: JSON Object
        ```
            destination_config_example = {
                "name": "Sample Destination",
                "type": "hive",
                "properties": {
                "hadoop_version": "2.x",
                "hdfs_root": "wasb://hdfsstorage.blob.core.windows.net/",
                "hive_metastore_url": "thrift://microsoft.com:9083",
                "temp_directory": "/root/iwxr/temp",
                "output_directory": "/root/iwxr/output"
                },
                "advanced_configurations": [
                    {
                      "key": "DATABASE_FILTER",
                      "value": ".*"
                    }
                ]
            }
        ```
        :return: response dict
        """
        response = None
        if destination_id is None or destination_config is None:
            self.logger.error("destination_id / destination_config cannot be None")
            raise Exception("destination_id / destination_config cannot be None")
        try:
            api_response = self.call_api("PATCH",
                                         url_builder.get_replicator_destination_url(self.client_config, destination_id),
                                         IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                         destination_config)

            response = IWUtils.ejson_deserialize(api_response.content)
            if api_response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc="Failed to update Destination",
                                                    response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to update destination.')
            raise GenericError(response.get("message", "Error occurred while trying to update destination."))

    def delete_replicator_destination(self, destination_id):
        """
        Function to delete a specific replicator destination
        :param destination_id: Identifier of Destination
        :type destination_id: String
        :return: response dict
        """
        response = None
        try:
            if destination_id is None:
                self.logger.error("destination_id cannot be None")
                raise Exception("destination_id cannot be None")

            api_response = self.call_api("DELETE",
                                         url_builder.get_replicator_destination_url(self.client_config, destination_id),
                                         IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))

            response = IWUtils.ejson_deserialize(api_response.content)
            if api_response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc="Failed to delete Destination",
                                                    response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.error("Error in deleting replicator destination")
            raise GenericError("Error in deleting replicator destination " + str(e))

    # Replicator Definitions
    def get_list_of_replicator_definitions(self, domain_id, params=None):
        """
        Function to list the replicator definitions
        :param domain_id: Identifier of Domain
        :type domain_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type params: JSON dict
        :return: response dict
        """
        response = None
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_replicator_definitions = url_builder.list_replicator_definitions_url(
            self.client_config, domain_id) + IWUtils.get_query_params_string_from_dict(params=params)
        replicator_definitions_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_replicator_definitions,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)

            if response is not None:
                initial_msg = response.get("message", "")
                result = response.get("result", {})
                records = result.get("records", [])
                while len(records) > 0:
                    replicator_definitions_list.extend(records)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", {})
                    records = result.get("records", [])
            else:
                self.logger.error("Failed to get list of replicator definitions")
                return GenericResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                    error_desc="Failed to get list of replicator definitions",
                                                    response=response)
            response["result"] = replicator_definitions_list
            response["message"] = initial_msg
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.error("Error in listing replicator definitions")
            raise GenericError("Error in listing replicator definitions " + str(e))

    def create_replicator_definition(self, definition_config):
        """
        Create a new Infoworks Replicator definition
        :param definition_config: a JSON object containing definition configurations
        :type definition_config: JSON Object
        ```
        definition_config_example = {
            "name": "Sample replicator definition",
            "replicator_source_name": "Sample source name",
            "replicator_destination_name": "Sample destination name",
            "replicator_source_id": "6303aea5beda7f1484c74f49",
            "replicator_destination_id": "6303aedfbeda7f1484c74f4a",
            "domain_id": "195ebb5847e08c1eca8368e4",
            "job_bandwidth_mb": 1000,
            "replication_type": "batch",
            "copy_parallelism_factor": 4,
            "metastore_parallelism_factor": 4,
            "advanced_configurations": [
                {
                  "key": "DATABASE_FILTER",
                  "value": ".*"
                }
            ]
        }
        ```
        :return: response dict
        """
        response = None
        if definition_config is None:
            self.logger.error("definition config cannot be None")
            raise Exception("definition config cannot be None")
        domain_id = definition_config["domain_id"]
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", url_builder.create_replicator_definition_url(self.client_config, domain_id),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              definition_config).content)

            definition_id = response.get('result', None)
            if definition_id is None:
                self.logger.error('Cannot create a new replicator definition.')
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc='Failed to create replicator definition',
                                                    response=response)
            definition_id = str(definition_id)
            self.logger.info('Definition {id} has been created.'.format(id=definition_id))
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to create a new definition.')
            raise GenericError(response.get("message", "Error occurred while trying to create a new definition."))

    def get_replicator_definition(self, domain_id, definition_id):
        """
        Function to retrieve the details of a specific replicator source
        :param domain_id: Identifier of Domain
        :type domain_id: String
        :param definition_id: Identifier of Definition
        :type definition_id: String
        :return: response dict
        """
        response = None
        try:
            if domain_id is None or definition_id is None:
                self.logger.error("domain_id / definition_id cannot be None")
                raise Exception("domain_id / definition_id cannot be None")

            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_builder.get_replicator_definition_url(self.client_config, domain_id,
                                                                               definition_id),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)

            definition_id = response.get('result', {}).get('id')
            if definition_id is not None:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc="Failed to retrieve Definition details",
                                                    response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.error("Error in retrieving replicator definition")
            raise GenericError("Error in retrieving replicator definition " + str(e))

    def update_replicator_definition(self, domain_id, definition_id, definition_config):
        """
        Update Infoworks Replicator source
        :param domain_id: Identifier of Domain
        :type domain_id: String
        :param definition_id: Identifier of Definition
        :type definition_id: String
        :param definition_config: a JSON object containing definition configurations
        :type definition_config: JSON Object
        ```
            definition_config_example = {
            "name": "Sample replicator definition",
            "replicator_source_name": "Sample source name",
            "replicator_destination_name": "Sample destination name",
            "replicator_source_id": "6303aea5beda7f1484c74f49",
            "replicator_destination_id": "6303aedfbeda7f1484c74f4a",
            "domain_id": "195ebb5847e08c1eca8368e4",
            "job_bandwidth_mb": 1000,
            "replication_type": "batch",
            "copy_parallelism_factor": 4,
            "metastore_parallelism_factor": 4,
            "advanced_configurations": [
                {
                  "key": "DATABASE_FILTER",
                  "value": ".*"
                }
            ]
        }
        ```
        :return: response dict
        """
        response = None
        if domain_id is None or definition_id is None or definition_config is None:
            self.logger.error("domain_id / definition_id / definition_config cannot be None")
            raise Exception("domain_id / definition_id / definition_config cannot be None")
        try:
            api_response = self.call_api("PATCH",
                                         url_builder.get_replicator_definition_url(self.client_config, domain_id,
                                                                                   definition_id),
                                         IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                         definition_config)

            response = IWUtils.ejson_deserialize(api_response.content)
            if api_response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc="Failed to update Definition",
                                                    response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to update definition.')
            raise GenericError(response.get("message", "Error occurred while trying to update definition."))

    def delete_replicator_definition(self, domain_id, definition_id):
        """
        Function to delete a specific replicator definition
        :param domain_id: Identifier of Domain
        :type domain_id: String
        :param definition_id: Identifier of Definition
        :type definition_id: String
        :return: response dict
        """
        response = None
        try:
            if domain_id is None or definition_id is None:
                self.logger.error("domain_id/definition_id cannot be None")
                raise Exception("domain_id/definition_id cannot be None")

            api_response = self.call_api("DELETE",
                                         url_builder.get_replicator_definition_url(self.client_config, domain_id,
                                                                                   definition_id),
                                         IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))
            response = IWUtils.ejson_deserialize(api_response.content)
            if api_response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc="Failed to delete Definition",
                                                    response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.error("Error in deleting replicator definition")
            raise GenericError("Error in deleting replicator definition " + str(e))

    # Replicator Definition Tables
    def add_tables_to_replicator_definition(self, domain_id, definition_id, tables_config):
        """
        Add tables to Infoworks Replicator definition
        :param domain_id: Identifier of domain
        :type domain_id: String
        :param definition_id: Identifier of Replication Definition
        :type definition_id: String
        :param tables_config: a JSON object containing tables configurations
        :type tables_config: JSON Object
        ```
        tables_config_example = {
          "selected_objects": [
            {
              "id": "e77850ad5127a2d7dab870ff",
              "table_name": "Sample table name",
              "schema_name": "Sample schema name"
            }
          ]
        }
        ```
        :return: response dict
        """
        response = None
        if domain_id is None or definition_id is None or tables_config is None:
            self.logger.error("domain_id or definition_id or tables_config cannot be None")
            raise Exception("domain_id or definition_id or tables_config cannot be None")
        try:
            api_response = self.call_api("POST",
                                         url_builder.add_replicator_definition_tables_url
                                         (self.client_config, domain_id, definition_id),
                                         IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                         tables_config)
            response = IWUtils.ejson_deserialize(api_response.content)

            if api_response.status_code == 200:
                # print(json.dumps(response))
                # result = response.get('result', {})
                self.logger.info("Tables added Successfully")
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                self.logger.error(f'Cannot add tables to definition : '
                                  f'Received API Status Code {api_response.status_code}')
                self.logger.error(f"Response: {response}")
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc='Failed to add tables to definition',
                                                    response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to add tables to definition.')
            raise GenericError(response.get("message", "Error occurred while trying to add tables to definition."))

    def delete_replicator_definition_tables(self, domain_id, definition_id):
        """
        Function to delete a specific replicator definition tables
        :param domain_id: Identifier of Domain
        :type domain_id: String
        :param definition_id: Identifier of Definition
        :type definition_id: String
        :return: response dict
        """
        response = None
        try:
            if domain_id is None or definition_id is None:
                self.logger.error("domain_id/definition_id cannot be None")
                raise Exception("domain_id/definition_id cannot be None")

            api_response = self.call_api("DELETE",
                                         url_builder.add_replicator_definition_tables_url(self.client_config, domain_id,
                                                                                          definition_id),
                                         IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))
            response = IWUtils.ejson_deserialize(api_response.content)
            if api_response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc="Failed to delete definition tables",
                                                    response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.error("Error in deleting replicator definition tables")
            raise GenericError("Error in deleting replicator definition tables " + str(e))

    # Replicator Jobs
    def poll_replicator_job(self, job_id, poll_timeout=local_configurations.POLLING_TIMEOUT,
                            polling_frequency=local_configurations.POLLING_FREQUENCY_IN_SEC,
                            retries=local_configurations.NUM_POLLING_RETRIES):
        """
        Polls Infoworks Replicator job and returns its status
        :param job_id: Job Identifier
        :type job_id: String
        :param poll_timeout: Polling timeout(default : 1200 seconds).If -1 then till the job completes polling will be done.
        :type poll_timeout: Integer
        :param polling_frequency: Polling Frequency(default : 15 seconds)
        :type polling_frequency: Integer
        :param retries: Number of Retries (default : 3)
        :type retries: Integer
        :return: response Object with Job status
        """
        failed_count = 0
        response = {}
        if poll_timeout != -1:
            timeout = time.time() + poll_timeout
        else:
            # 2,592,000 is 30 days assuming it to be max time a job can run
            timeout = time.time() + 2592000

        while True:
            if time.time() > timeout:
                break
            try:
                self.logger.info(f"Failed poll job status count: {failed_count}")
                job_monitor_url = url_builder.get_job_status_url(self.client_config, job_id)
                response = IWUtils.ejson_deserialize(self.call_api("GET", job_monitor_url,
                                                                   IWUtils.get_default_header_for_v3(
                                                                       self.client_config['bearer_token'])).content)
                result = response.get('result', {})
                if len(result) != 0:
                    job_status = result["status"]
                    # print(f"replicator_job_status : {job_status}.Sleeping for {polling_frequency} seconds")
                    self.logger.info(
                        f"Job poll status : {result['status']}  - Job completion percentage: "
                        f"{result.get('percentage', 0)} - Sleeping for {polling_frequency} seconds")
                    if job_status.lower() in ["completed", "failed", "aborted", "canceled"]:
                        return GenericResponse.parse_result(status=Response.Status.SUCCESS,
                                                            job_id=job_id, response=response)
                else:
                    self.logger.error(f"Error occurred during job {job_id} status poll")
                    if failed_count >= retries - 1:
                        return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                            error_code=ErrorCode.GENERIC_ERROR,
                                                            error_desc=f"Error occurred during job {job_id} status poll",
                                                            response=response, job_id=job_id,
                                                            )
                    failed_count = failed_count + 1
            except Exception as e:
                self.logger.exception("Error occurred during job status poll")
                self.logger.info(str(e))
                if failed_count >= retries - 1:
                    print(traceback.print_stack())
                    print(response)
                    raise GenericError(response.get("message", "Error occurred during job status poll"))
                failed_count = failed_count + 1
            time.sleep(polling_frequency)

        return GenericResponse.parse_result(status=Response.Status.FAILED,
                                            error_code=ErrorCode.POLL_TIMEOUT,
                                            error_desc="Job status poll timeout occurred", response=response,
                                            job_id=job_id,
                                            )

    def submit_replication_meta_crawl_job(self, domain_id, replicator_source_id, poll_timeout=300,
                                          polling_frequency=15, retries=3):
        """
        Submit Replication meta crawl job
        :param domain_id: Identifier of domain
        :type domain_id: String
        :param replicator_source_id: Identifier of Replication Source
        :type replicator_source_id: String
        :param poll_timeout: meta crawl job timeout in seconds
        :type poll_timeout: integer. Default 300 seconds
        :param polling_frequency: polling frequency for the job in seconds. Default 15 seconds
        :type polling_frequency: integer
        :param retries: Number of retries during job poll. Default is 3
        :type retries: integer
        :return: response dict
        """
        meta_crawl_config = {
            "job_type": "replicator_metacrawl"
        }
        response = None
        if domain_id is None or replicator_source_id is None:
            self.logger.error("domain_id or replicator_source_id cannot be None")
            raise Exception("domain_id or replicator_source_id cannot be None")
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", url_builder.submit_replication_metacrawl_job_url(
                    self.client_config, domain_id, replicator_source_id),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              meta_crawl_config).content)

            job_id = response.get('result', {}).get('id', None)

            if job_id is None:
                self.logger.error('Cannot submit replication meta crawl job')
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc='Failed to submit replication meta crawl job',
                                                    response=response)
            else:
                self.logger.info('Job has been submitted: {id}'.format(id=job_id))
                return self.poll_replicator_job(job_id=job_id, poll_timeout=poll_timeout,
                                                polling_frequency=polling_frequency,
                                                retries=retries)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to submit/poll meta crawl job.')
            raise GenericError(response.get("message", "Error occurred while trying to submit/poll meta crawl job."))

    def submit_replication_data_job(self, domain_id, definition_id, poll=False, poll_timeout=300,
                                    polling_frequency=15, retries=3):
        """
        Submit Replication Data job
        :param domain_id: Identifier of domain
        :type domain_id: String
        :param definition_id: Identifier of Replication Definition
        :type definition_id: String
        :param poll: True/False to poll Job
        :type: Boolean
        :param poll_timeout: meta crawl job timeout in seconds
        :type poll_timeout: integer. Default 300 seconds
        :param polling_frequency: polling frequency for the job in seconds. Default 15 seconds
        :type polling_frequency: Integer
        :param retries: Number of retries during job poll. Default is 3
        :type retries: Integer
        :return: response dict
        """
        data_config = {
            "job_type": "replicate_data"
        }
        response = None
        if domain_id is None or definition_id is None:
            self.logger.error("domain_id or definition_id cannot be None")
            raise Exception("domain_id or definition_id cannot be None")
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", url_builder.submit_replication_data_job_url(self.client_config, domain_id,
                                                                                  definition_id),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              data_config).content)
            result = response.get('result', {})
            job_id = result.get('id')
            if job_id is None:
                self.logger.error('Cannot submit replication data job')
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc='Failed to submit replication data job',
                                                    response=response)
            else:
                self.logger.info('Job has been submitted: {id}.'.format(id=job_id))
                if poll:
                    return self.poll_replicator_job(job_id=job_id, poll_timeout=poll_timeout,
                                                    polling_frequency=polling_frequency,
                                                    retries=retries)
                else:
                    return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to submit data job.')
            print('Error occurred while trying to submit replication data job.')
            raise GenericError(response.get("message", "Error occurred while trying to submit data job."))

    def get_all_jobs_for_replicator_source(self, domain_id, source_id, params=None):
        """
        Function to get all jobs for a particular replicator source
        :param domain_id: Identifier of Domian
        :type domain_id: String
        :param source_id: entity identifier for which the jobs are to be fetched
        :type: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response list of dict
        """
        if None in {source_id, domain_id}:
            self.logger.error("source_id/domain_id cannot be None")
            raise Exception("source_id/domain_id cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}

        url_to_list_jobs = url_builder.submit_replication_metacrawl_job_url(
            self.client_config, domain_id, source_id) + IWUtils.get_query_params_string_from_dict(params=params)
        job_details = []
        initial_msg = ""
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_jobs,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                initial_msg = response.get("message", "")
                result = response.get("result", None)
                if result is None:
                    self.logger.error(f"Failed to get the source jobs details.")
                    return GenericResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                        error_desc=f"Failed to get the source jobs details.",
                                                        response=response)

                while len(result) > 0:
                    job_details.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", [])
            response["message"] = initial_msg
            response["result"] = job_details
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error(f"Error in getting job details")
            raise GenericError(f"Error in getting job details : {e}")

    def get_replicator_job_summary(self, domain_id, definition_id, job_id):
        """
        Function to get job summary for given job_id
        :param domain_id: Identifier of Domain
        :type domain_id: String
        :param definition_id: Identifier of Definition
        :type definition_id: String
        :param job_id: Identifier of job
        :type job_id: String
        :return: response dict
        """
        if None in {job_id, domain_id, definition_id}:
            self.logger.error("job_id / domain_id / definition_id cannot be None")
            raise Exception("job_id / domain_id / definition_id cannot be None")
        try:
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.get_replicator_job_summary_url(
                self.client_config, domain_id, definition_id, job_id),
                IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            result = response.get('result', None)
            if result is None:
                self.logger.error(f"Failed to get the crawl job summary for job_id {job_id}.")
                return GenericResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                    error_desc=f"Failed to get the crawl job summary for job_id {job_id}.",
                                                    response=response, job_id=job_id)
            else:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            raise GenericError(f"Failed to get the crawl job summary for job_id {job_id}." + str(e))

    # Accessible Replicator Sources
    def add_replicator_sources_to_domain(self, domain_id, config):
        """
        Add Replicator Sources to Domain
        :param domain_id: Identifier of domain
        :type domain_id: String
        :param config: Configurations
        :type config: JSON
        ```
        sample_config = {
          "entity_details": [
            {
              "entity_id": "e77850ad5127a2d7dab870ff",
              "schema_filter": ".*ss"
            }
          ]
        }
        ```
        :return: response dict
        """
        response = None
        if domain_id is None or config is None:
            self.logger.error("domain_id or config cannot be None")
            raise Exception("domain_id or config cannot be None")
        try:
            api_response = self.call_api("POST", url_builder.add_replicator_sources_to_domain_url(
                self.client_config, domain_id),
                                         IWUtils.get_default_header_for_v3(self.client_config['bearer_token']), config)

            response = IWUtils.ejson_deserialize(api_response.content)

            if api_response.status_code == 200:
                # print(json.dumps(response))
                # result = response.get('result', {})
                self.logger.info('Source added successfully.')
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                self.logger.error(f'Cannot add replicator sources to domain : '
                                  f'Received API Status Code {api_response.status_code}')
                self.logger.error(f"Response: {response}")
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc='Failed to add replicator sources to domain',
                                                    response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to add replicator sources to domain')
            raise GenericError(response.get("message", "Error occurred while trying to add "
                                                       "replicator sources to domain."))

    # Accessible Replicator Destinations
    def add_replicator_destinations_to_domain(self, domain_id, config):
        """
        Add Replicator Destinations to Domain
        :param domain_id: Identifier of domain
        :type domain_id: String
        :param config: Configurations
        :type config: JSON
        ```
        sample_config = {
          "entity_details": [
            {
              "entity_id": "e77850ad5127a2d7dab870ff",
            }
          ]
        }
        ```
        :return: response dict
        """
        response = None
        if domain_id is None or config is None:
            self.logger.error("domain_id or config cannot be None")
            raise Exception("domain_id or config cannot be None")
        try:
            api_response = self.call_api("POST", url_builder.add_replicator_destinations_to_domain_url(
                self.client_config, domain_id),
                                         IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                         config)

            response = IWUtils.ejson_deserialize(api_response.content)

            if api_response.status_code == 200:
                # print(json.dumps(response))
                # result = response.get('result', {})
                self.logger.info('Destination added successfully.')
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                self.logger.error(f'Cannot add replicator destinations to domain : '
                                  f'Received API Status Code {api_response.status_code}')
                self.logger.error(f"Response: {response}")
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc='Failed to add replicator destinations to domain',
                                                    response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to add replicator destinations to domain')
            raise GenericError(response.get("message", "Error occurred while trying to add "
                                                       "replicator destinations to domain."))

    # Replication Schedules
    '''
    def get_list_of_replication_schedules(self, domain_id, definition_id, params=None):
        """
        Function to list the replication schedules
        :param domain_id: Identifier of Domain
        :type domain_id: String
        :param definition_id: Identifier of Definition
        :type definition_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type params: JSON dict
        :return: response dict
        """
        response = None
        if domain_id is None or definition_id is None:
            self.logger.error("domain_id or definition_id cannot be None")
            raise Exception("domain_id or definition_id cannot be None")

        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_replication_schedules = url_builder.list_replication_schedules_url(
            self.client_config, domain_id, definition_id) + IWUtils.get_query_params_string_from_dict(params=params)
        replication_schedules_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_replication_schedules,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                initial_msg = response.get("message", "")
                result = response.get("result", {})
                records = result.get("records", [])
                while len(records) > 0:
                    replication_schedules_list.extend(records)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", {})
                    records = result.get("records", [])
            else:
                self.logger.error("Failed to get list of replication schedules")
                return GenericResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                    error_desc="Failed to get list of replication schedules",
                                                    response=response)
            response["result"] = replication_schedules_list
            response["message"] = initial_msg
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.error("Error in listing replication schedules")
            raise GenericError("Error in listing replication schedules " + str(e))
    '''
    def create_replication_schedule(self, domain_id, definition_id, schedule_config):
        """
        Create replication schedule
        :param domain_id: Identifier of domain
        :type domain_id: String
        :param definition_id: Identifier of replication definition
        :type definition_id: String
        :param schedule_config: Schedule Configurations
        :type schedule_config: JSON
        ```
        schedule_sample_config = {
          "entity_type": "replicate_definition",
          "properties": {
            "schedule_status": "enabled",
            "repeat_interval": "[hour,minute,day,week,month]",
            "starts_on": "08/30/2022",
            "time_hour": 7,
            "time_min": 20,
            "ends": "on",
            "repeat_every": "1",
            "ends_on": "{number}",
            "end_hour": "{number}",
            "end_min": "{number}"
          },
          "scheduler_username": "admin@infoworks.io",
          "scheduler_auth_token": "1TGf748u1I0mvZj3efuwr8y"
        }
        ```
        :return: response dict
        """
        response = None
        if domain_id is None or definition_id is None or schedule_config is None:
            self.logger.error("domain_id or definition_id or schedule_config cannot be None")
            raise Exception("domain_id or definition_id or schedule_config cannot be None")
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", url_builder.list_replication_schedules_url(self.client_config, domain_id,
                                                                                 definition_id),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              schedule_config).content)

            schedule_id = response.get('result', {})

            if schedule_id is not None:
                self.logger.info('Schedule {id} has been created.'.format(id=schedule_id))
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                self.logger.error('Cannot create replication schedule')
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc='Failed to create replication schedule',
                                                    response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to create replication schedule')
            raise GenericError(response.get("message", "Error occurred while trying to create "
                                                       "replication schedule"))

    def get_replication_schedule(self, domain_id, definition_id, schedule_id):
        """
        Function to retrieve the details of a specific replicator source
        :param domain_id: Identifier of Domain
        :type domain_id: String
        :param definition_id: Identifier of Definition
        :type definition_id: String
        :param schedule_id: Identifier of Schedule
        :type schedule_id: String
        :return: response dict
        """
        response = None
        try:
            if domain_id is None or definition_id is None or schedule_id is None:
                self.logger.error("domain_id / definition_id / schedule_id cannot be None")
                raise Exception("domain_id / definition_id / schedule_id cannot be None")

            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_builder.get_replication_schedule_url(self.client_config, domain_id,
                                                                              definition_id, schedule_id),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)

            schedule_id = response.get('result', {}).get('id')
            if schedule_id is not None:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc="Failed to retrieve schedule details",
                                                    response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.error("Error in retrieving replication schedule")
            raise GenericError("Error in retrieving replication schedule " + str(e))

    def update_replication_schedule(self, domain_id, definition_id, schedule_id, schedule_config):
        """
        Update Infoworks Replication Schedule
        :param domain_id: Identifier of Domain
        :type domain_id: String
        :param definition_id: Identifier of Definition
        :type definition_id: String
        :param schedule_id: Identifier of Schedule
        :type schedule_id: String
        :param schedule_config: a JSON object containing schedule configurations
        :type schedule_config: JSON Object
        ```
        schedule_sample_config = {
          "entity_type": "replicate_definition",
          "properties": {
            "schedule_status": "enabled",
            "repeat_interval": "[hour,minute,day,week,month]",
            "starts_on": "08/30/2022",
            "time_hour": 7,
            "time_min": 20,
            "ends": "on",
            "repeat_every": "1",
            "ends_on": "{number}",
            "end_hour": "{number}",
            "end_min": "{number}"
          },
          "scheduler_username": "admin@infoworks.io",
          "scheduler_auth_token": "1TGf748u1I0mvZj3efuwr8y"
        }
        ```
        :return: response dict
        """
        response = None
        if domain_id is None or definition_id is None or schedule_id is None or schedule_config is None:
            self.logger.error("domain_id / definition_id / schedule_id / schedule_config cannot be None")
            raise Exception("domain_id / definition_id / schedule_id / schedule_config cannot be None")
        try:
            api_response = self.call_api("PATCH",
                                         url_builder.get_replication_schedule_url(self.client_config, domain_id,
                                                                                  definition_id, schedule_id),
                                         IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                         schedule_config)

            response = IWUtils.ejson_deserialize(api_response.content)
            if api_response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc="Failed to update schedule",
                                                    response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to update schedule.')
            raise GenericError(response.get("message", "Error occurred while trying to update schedule."))

    def delete_replication_schedule(self, domain_id, definition_id, schedule_id):
        """
        Function to delete a specific replication schedule
        :param domain_id: Identifier of Domain
        :type domain_id: String
        :param definition_id: Identifier of Definition
        :type definition_id: String
        :param schedule_id: Identifier of Schedule ID
        :type schedule_id: String
        :return: response dict
        """
        response = None
        try:
            if domain_id is None or definition_id is None or schedule_id is None:
                self.logger.error("domain_id/definition_id/schedule_id cannot be None")
                raise Exception("domain_id/definition_id/schedule_id cannot be None")

            api_response = self.call_api("DELETE",
                                         url_builder.get_replication_schedule_url(self.client_config, domain_id,
                                                                                  definition_id, schedule_id),
                                         IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))
            response = IWUtils.ejson_deserialize(api_response.content)
            if api_response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_desc="Failed to delete replication schedule",
                                                    response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.error("Error in deleting replication schedule")
            raise GenericError("Error in deleting replication schedule " + str(e))

    # Replicator Source Tables
    def get_replicator_source_table_id_from_name(self, source_id, schema_name, table_name):
        """
        Get table ID from name under source
        :param source_id: Identifier of source
        :type source_id: String
        :param schema_name: Source Schema Name
        :type schema_name: String
        :param table_name: Source Table Name
        :type table_name: String

        Returns table ID
        """
        response = None
        if source_id is None or schema_name is None or table_name is None:
            self.logger.error("source id or schema_name or table name  cannot be None")
            raise Exception("source id or schema_name or table name cannot be None")
        params = {"filter": {"schema_name": schema_name, "table_name": table_name}}
        url_to_list_tables = url_builder.get_replicator_source_tables_url(
            self.client_config, source_id) + IWUtils.get_query_params_string_from_dict(params=params)
        try:

            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_tables,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)

            records = response.get("result", {}).get("records", [])
            if len(records) > 0:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS,
                                                    response={"id": records[0]["id"]})
            else:
                raise GenericResponse.parse_result(status=Response.Status.FAILED,
                                                   error_desc="Failed to get source table id from name",
                                                   response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.error("Error in getting source table id from name")
            raise GenericError("Error in getting source table id from name " + str(e))
