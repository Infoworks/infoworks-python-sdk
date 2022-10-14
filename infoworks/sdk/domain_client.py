import requests

from infoworks.error import DomainError
from infoworks.sdk import url_builder
from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.local_configurations import Response, ErrorCode
from infoworks.sdk.generic_response import GenericResponse
from infoworks.sdk.url_builder import get_parent_entity_url
from infoworks.sdk.utils import IWUtils


class DomainClient(BaseClient):
    def __init__(self):
        super(DomainClient, self).__init__()

    def create_domain(self, config_body):
        """
        Function to create domain
        :param config_body: JSON payload for the domain creation

        config_body_example = {
         "name": "Abhi",
         "description": "Example Domain",
         "environment_id": "d60a47e2c8438a3d6daf7958"
        }
        :return: response dict
        """
        try:
            create_domain_url = url_builder.create_domain_url(self.client_config)
            response = self.call_api("POST", create_domain_url,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response, job_id=None)
        except Exception as e:
            raise DomainError(f"Failed to create domain" + str(e))

    def list_domains(self, params=None):
        """
        Function to list domains
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_domains = url_builder.create_domain_url(
            self.client_config) + IWUtils.get_query_params_string_from_dict(params=params)
        domains_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_domains,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                while len(result) > 0:
                    domains_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", [])
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=domains_list)
        except Exception as e:
            self.logger.error("Error in listing domains")
            raise DomainError("Error in listing domains" + str(e))

    def get_domain_details(self, domain_id):
        """
        Function to get the domain details
        :param domain_id: Entity identifier for the domain
        :return: response dict
        """
        try:
            url_to_get_domain_details = url_builder.create_domain_url(self.client_config) + f"/{domain_id}"
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_to_get_domain_details,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get('result', False)
            if not result:
                self.logger.error(f"Failed to get the domain details for {domain_id} ")
                return GenericResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                    error_desc=response, job_id=None)
            else:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=result)
        except Exception as e:
            raise DomainError(f"Failed to get the domain details for {domain_id} " + str(e))

    def update_domain(self, domain_id, config_body):
        """
        Function to update the domain
        :param domain_id: Entity identifier for the domain
        :param config_body: JSON payload for the domain creation
        :return: response dict
        """
        try:
            url_to_update_domain = url_builder.create_domain_url(self.client_config) + f"/{domain_id}"
            response = self.call_api("PATCH", url_to_update_domain,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response, job_id=None)
        except Exception as e:
            raise DomainError(f"Failed to update the domain {domain_id}" + str(e))

    def delete_domain(self, domain_id):
        """
        Function to delete the domain
        :param domain_id: Entity identifier for the domain
        :return: response dict
        """
        url_to_delete_domain = url_builder.create_domain_url(self.client_config) + f"/{domain_id}"
        try:
            response = self.call_api("DELETE",
                                     url_to_delete_domain,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(
                response.content,
            )
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in deleting domain")
                                                    )
        except Exception as e:
            self.logger.error(f"Error in deleting the domain {domain_id}")
            raise DomainError(f"Error in deleting the domain {domain_id} " + str(e))

    def get_sources_associated_with_domain(self, domain_id, params=None):
        """
        Function to get sources associated with the domain
        :param domain_id: Entity identifier for the domain
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_sources_under_domain = url_builder.add_sources_to_domain_url(self.client_config,
                                                                                 domain_id) + IWUtils.get_query_params_string_from_dict(
            params=params)
        src_under_domain_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_sources_under_domain,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                while len(result) > 0:
                    src_under_domain_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", [])
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=src_under_domain_list)
        except Exception as e:
            self.logger.error("Error in listing sources under domain")
            raise DomainError("Error in listing sources under domain" + str(e))

    def add_source_to_domain(self, domain_id, config_body):
        """
        Function to add source to the domain
        :param domain_id: Entity identifier for the domain
        :param config_body: JSON payload
        config_body_example =
            {
             "entity_ids": ["33164bbaa811058db5c3941d"]
            }
        :return: response dict
        """
        try:
            url_to_add_source_domain = url_builder.add_sources_to_domain_url(self.client_config, domain_id)
            response = self.call_api("POST", url_to_add_source_domain,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response)
        except Exception as e:
            raise DomainError(f"Failed to add source to the domain {domain_id}" + str(e))

    def remove_source_from_domain(self, domain_id, config_body):
        """
        Function to remove source from the domain
        :param domain_id: Entity identifier for the domain
        :param config_body: JSON payload

        config_body_example =
            {
             "entity_ids": ["33164bbaa811058db5c3941d"]
            }
        :return: response dict
        """
        try:
            url_to_delete_source_domain = url_builder.add_sources_to_domain_url(self.client_config, domain_id)
            response = self.call_api("DELETE", url_to_delete_source_domain,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response)
        except Exception as e:
            raise DomainError(f"Failed to remove source from the domain {domain_id}" + str(e))

    def get_data_connection(self, domain_id, data_connection_id=None, params=None):
        """
        Function to create data connection in the domain
        :param domain_id: Entity identifier for the domain
        :param data_connection_id: id of dataconnection for which config has to be fetched
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}

        if data_connection_id is None:
            url_to_get_data_connection = url_builder.create_data_connection(
                self.client_config, domain_id) + IWUtils.get_query_params_string_from_dict(params=params)
        else:
            url_to_get_data_connection = url_builder.create_data_connection(
                self.client_config, domain_id) + f"/{data_connection_id}"
        dataconnection_list = []
        try:
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_to_get_data_connection,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                if data_connection_id is not None:
                    dataconnection_list.extend([result])
                else:
                    while len(result) > 0:
                        dataconnection_list.extend(result)
                        nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                        response = IWUtils.ejson_deserialize(
                            self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                                self.client_config['bearer_token'])).content)
                        result = response.get("result", [])
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=dataconnection_list)
        except Exception as e:
            raise DomainError(f"Failed to get data connection details" + str(e))

    def create_data_connection(self, domain_id, config_body):
        """
        Function to create data connection in the domain
        :param domain_id: Entity identifier for the domain
        :param config_body: JSON payload
        :return: response dict
        """
        try:
            create_data_connection_url = url_builder.create_data_connection(self.client_config, domain_id)
            response = self.call_api("POST", create_data_connection_url,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response, job_id=None)
        except Exception as e:
            raise DomainError(f"Failed to create data connection" + str(e))

    def delete_data_connection(self, domain_id, data_connection_id):
        """
        Function to delete data connection in the domain
        :param domain_id: Entity identifier for the domain
        :param data_connection_id: Entity identifier for the data connection
        :return: response dict
        """
        try:
            create_data_connection_url = url_builder.create_data_connection(self.client_config, domain_id).strip(
                "/") + f"/{data_connection_id}"
            response = self.call_api("DELETE", create_data_connection_url,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     )
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response, job_id=None)
        except Exception as e:
            raise DomainError(f"Failed in deleting data connection" + str(e))

    def get_parent_entity_id(self, json_obj):
        """
        Function to get domain
        :param json_obj: Example: {"entity_id": workflow_id, "entity_type": "workflow"}
        :return: response dict
        """
        parent_entity_url = get_parent_entity_url(self.client_config)
        response = requests.request("GET", parent_entity_url, data=json_obj)
        if response.status_code == 200 and len(response.json().get("result", [])) > 0:
            result = response.json().get("result", [])
            if len(result) > 0:
                domain_id = result[0]["entity_id"]
                self.logger.info("Domain ID is {} ".format(domain_id))
                return domain_id
            else:
                self.logger.info("Domain ID is {} ".format(None))
                return None

    def get_domain_id(self, domain_name):
        """
        Function to get domain id using domain name
        :param domain_name: Name of the domain
        :return: Entity identifier of the domain
        """
        params = {"filter": {"name": domain_name}}
        url_to_list_domains = url_builder.create_domain_url(
            self.client_config) + IWUtils.get_query_params_string_from_dict(params=params)
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_domains,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                domain_id = result[0]["id"]
                return domain_id
        except Exception as e:
            self.logger.error("Error in finding domain id")
            raise DomainError("Error in finding domain id" + str(e))
