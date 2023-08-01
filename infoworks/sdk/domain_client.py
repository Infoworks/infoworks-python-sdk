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
        :type config_body: JSON dict
        ```
        config_body_example = {
         "name": "Abhi",
         "description": "Example Domain",
         "environment_id": "d60a47e2c8438a3d6daf7958"
        }
        ```
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
            self.logger.error(f"Failed to create domain" + str(e))
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
                initial_msg = response.get("message", "")
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
                response["result"] = domains_list
                response["message"] = initial_msg
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    response="Failed to get list of domains"
                                                    )
        except Exception as e:
            self.logger.error("Error in listing domains")
            raise DomainError("Error in listing domains" + str(e))

    def get_domain_details(self, domain_id):
        """
        Function to get the domain details
        :param domain_id: Entity identifier for the domain
        :type domain_id: String
        :return: response dict
        """
        if domain_id is None:
            raise Exception("domain_id cannot be None")
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
                                                    error_desc=f"Failed to get the domain details for {domain_id} ",
                                                    job_id=None, response=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error(f"Failed to get the domain details for {domain_id} " + str(e))
            raise DomainError(f"Failed to get the domain details for {domain_id} " + str(e))

    def update_domain(self, domain_id, config_body):
        """
        Function to update the domain
        :param domain_id: Entity identifier for the domain
        :type domain_id: String
        :param config_body: JSON payload for the domain creation
        :type config_body: JSON dict
        :return: response dict
        """
        if domain_id is None or config_body is None:
            raise Exception("domain_id or config_body cannot be None")
        try:
            url_to_update_domain = url_builder.create_domain_url(self.client_config) + f"/{domain_id}"
            response = self.call_api("PATCH", url_to_update_domain,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                self.logger.error(f"{parsed_response}")
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    response=parsed_response, job_id=None)
        except Exception as e:
            self.logger.error(f"Failed to update the domain {domain_id}" + str(e))
            raise DomainError(f"Failed to update the domain {domain_id}" + str(e))

    def delete_domain(self, domain_id):
        """
        Function to delete the domain
        :param domain_id: Entity identifier for the domain
        :type domain_id: String
        :return: response dict
        """
        if domain_id is None:
            raise Exception("domain_id cannot be None")
        url_to_delete_domain = url_builder.create_domain_url(self.client_config) + f"/{domain_id}"
        try:
            response = self.call_api("DELETE",
                                     url_to_delete_domain,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(
                response.content,
            )
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                self.logger.error(f"Error in deleting the domain {domain_id}")
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in deleting domain"),
                                                    response=parsed_response
                                                    )
        except Exception as e:
            self.logger.error(f"Error in deleting the domain {domain_id}")
            raise DomainError(f"Error in deleting the domain {domain_id} " + str(e))

    def get_sources_associated_with_domain(self, domain_id, params=None):
        """
        Function to get sources associated with the domain
        :param domain_id: Entity identifier for the domain
        :type domain_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type params: JSON dict
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_sources_under_domain = url_builder.add_sources_to_domain_url(self.client_config,
                                                                                 domain_id) + IWUtils.get_query_params_string_from_dict(
            params=params)
        src_under_domain_list = []
        if domain_id is None:
            raise Exception("domain_id cannot be None")
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_sources_under_domain,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                initial_msg = response.get("message", "")
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
                response["result"] = src_under_domain_list
                response["message"] = initial_msg
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    response="Failed to get sources associated with the domain"
                                                    )
        except Exception as e:
            self.logger.error("Error in listing sources under domain")
            raise DomainError("Error in listing sources under domain" + str(e))

    def add_source_to_domain(self, domain_id, config_body):
        """
        Function to add source to the domain
        :param domain_id: Entity identifier for the domain
        :type domain_id: String
        :param config_body: JSON payload
        :type config_body: JSON dict
        ```
        config_body_example =
            {
             "entity_ids": ["33164bbaa811058db5c3941d"]
            }
        ```
        :return: response dict
        """
        if domain_id is None or config_body is None:
            raise Exception("domain_id or config_body cannot be None")
        try:
            url_to_add_source_domain = url_builder.add_sources_to_domain_url(self.client_config, domain_id)
            response = self.call_api("POST", url_to_add_source_domain,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                self.logger.error(f"Failed to add source to the domain {domain_id}")
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response.get('details',
                                                                                   "Failed to add source to the domain"),
                                                    response=parsed_response)
        except Exception as e:
            raise DomainError(f"Failed to add source to the domain {domain_id}" + str(e))

    def remove_source_from_domain(self, domain_id, config_body):
        """
        Function to remove source from the domain
        :param domain_id: Entity identifier for the domain
        :type domain_id: String
        :param config_body: JSON payload
        :type config_body: JSON dict
        ```
        config_body_example =
            {
             "entity_ids": ["33164bbaa811058db5c3941d"]
            }
        ```
        :return: response dict
        """
        if domain_id is None or config_body is None:
            raise Exception("domain_id or config_body cannot be None")
        try:
            url_to_delete_source_domain = url_builder.add_sources_to_domain_url(self.client_config, domain_id)
            response = self.call_api("DELETE", url_to_delete_source_domain,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                self.logger.error(f"Failed to remove source from the domain {domain_id}")
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response.get('details',
                                                                                   "Failed to remove source from the domain"),
                                                    response=parsed_response)
        except Exception as e:
            raise DomainError(f"Failed to remove source from the domain {domain_id}" + str(e))

    def get_parent_entity_id(self, json_obj):
        """
        Function to get parent entity for any entity
        :param json_obj: Example: {"entity_id": workflow_id, "entity_type": "workflow"}
        :type json_obj: JSON dict
        :return: response dict
        """
        parent_entity_url = get_parent_entity_url(self.client_config)
        response = requests.request("GET", parent_entity_url, data=json_obj, verify=False)
        if response.status_code == 200 and len(response.json().get("result", [])) > 0:
            result = response.json().get("result", [])
            if len(result) > 0:
                domain_id = result[0]["entity_id"]
                self.logger.info("Domain ID is {} ".format(domain_id))
                return GenericResponse.parse_result(status=Response.Status.SUCCESS,
                                                    response={"parent_entity_id": domain_id})
            else:
                self.logger.error("Failed to get parent entity id")
                self.logger.info("Domain ID is {} ".format(None))
                return GenericResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.GENERIC_ERROR,
                                                    response={"parent_entity_id": None})

    def get_domain_id(self, domain_name):
        """
        Function to get domain id using domain name
        :param domain_name: Name of the domain
        :type domain_name: String
        :return: Entity identifier of the domain
        """
        params = {"filter": {"name": domain_name}}
        url_to_list_domains = url_builder.create_domain_url(
            self.client_config) + IWUtils.get_query_params_string_from_dict(params=params)
        if domain_name is None:
            raise Exception("domain_name cannot be None")
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_domains,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                if len(result) > 0:
                    domain_id = result[0]["id"]
                    return GenericResponse.parse_result(status=Response.Status.SUCCESS,
                                                        response={"domain_id": domain_id})
                else:
                    self.logger.error(f"Failed to get domain id with name {domain_name}")
                    return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                        error_code=ErrorCode.GENERIC_ERROR,
                                                        response={"domain_id": None})
            else:
                self.logger.error("Failed to get domain id")
                return GenericResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.GENERIC_ERROR,
                                                    response={"domain_id": None})
        except Exception as e:
            self.logger.error("Error in finding domain id")
            raise DomainError("Error in finding domain id" + str(e))

    def get_pipeline_extensions_associated_with_domain(self, domain_id, params=None):
        """
        Function to get pipeline extensions associated with the domain
        :param domain_id: Entity identifier for the domain
        :type domain_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type params: JSON dict
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_pl_extns_under_domain = url_builder.accessible_pipeline_extensions_url(self.client_config,
                                                                                           domain_id) + IWUtils.get_query_params_string_from_dict(
            params=params)
        pl_extn_under_domain_list = []
        if domain_id is None:
            raise Exception("domain_id cannot be None")
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_pl_extns_under_domain,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                initial_msg = response.get("message", "")
                while len(result) > 0:
                    pl_extn_under_domain_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", [])
                response["result"] = pl_extn_under_domain_list
                response["message"] = initial_msg
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    response="Failed to get pipeline extensions associated with the domain"
                                                    )
        except Exception as e:
            self.logger.error("Error in listing pipeline extensions under domain")
            raise DomainError("Error in listing pipeline extensions under domain" + str(e))

    def add_update_pipeline_extensions_to_domain(self, domain_id, config_body=None, action_type="create"):
        """
        Function to add pipeline extensions to the domain
        :param domain_id: Entity identifier for the domain
        :type domain_id: String
        :param config_body: JSON payload
        :type config_body: JSON dict
        ```
        config_body_example =
            {
             "entity_ids": ["33164bbaa811058db5c3941d"]
            }
        ```
        :param action_type: Pass either create/update
        :return: response dict
        :return: response dict
        """
        if domain_id is None or config_body is None:
            raise Exception("domain_id or config_body cannot be None")
        try:
            if action_type.lower() == "create":
                request_type = "POST"
            else:
                request_type = "PUT"
            url_to_add_plextn_domain = url_builder.accessible_pipeline_extensions_url(self.client_config, domain_id)
            response = self.call_api(request_type, url_to_add_plextn_domain,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                self.logger.error(f"Failed to add/update pipeline extensions to the domain {domain_id}")
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response.get('details',
                                                                                   "Failed to add/update pipeline extensions to the domain"),
                                                    response=parsed_response)
        except Exception as e:
            raise DomainError(f"Failed to add/update pipeline extensions to the domain {domain_id}" + str(e))

    def delete_pipeline_extensions_from_domain(self, domain_id, config_body=None):
        """
        Function to delete the pipeline extension from domain
        :param domain_id: Entity identifier for the domain
        :type domain_id: String
        :param config_body: JSON payload
        :type config_body: JSON dict
        ```
        config_body_example =
            {
             "entity_ids": ["33164bbaa811058db5c3941d"]
            }
        ```
        :return: response dict
        """
        if domain_id is None or config_body is None:
            raise Exception("domain_id or config_body cannot be None")
        url_to_delete_pl_extn = url_builder.accessible_pipeline_extensions_url(self.client_config, domain_id)
        try:
            response = self.call_api("DELETE",
                                     url_to_delete_pl_extn,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(
                response.content,
            )
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                self.logger.error(f"Error in deleting the pipeline extension")
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in deleting pipeline extension"),
                                                    response=parsed_response
                                                    )
        except Exception as e:
            self.logger.error(f"Error in deleting the pipeline extension")
            raise DomainError(f"Error in deleting the pipeline extension " + str(e))

    def modify_advanced_config_for_domain(self, domain_id, adv_config_body,
                                          action_type="update", key=None):
        """
        Function to add/update the adv config for the domain
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :param action_type: values can be either create/update. default update
        :type action_type: String
        :param adv_config_body: JSON dict
        ```
        adv_config_body_example = {
            "key" : "",
            "value": "",
            "description": "",
            "is_active": True
            }
        ```
        :param key: In case of update, name of the adv config to update
        :return: response dict
        """
        if None in {domain_id} or adv_config_body is None:
            raise Exception(f"domain_id and adv_config_body cannot be None")
        try:
            if action_type.lower() == "create":
                request_type = "POST"
                request_url = url_builder.advanced_config_domain_url(self.client_config, domain_id)
            else:
                request_type = "PUT"
                request_url = url_builder.advanced_config_domain_url(self.client_config, domain_id) + f"{key}"
            response = IWUtils.ejson_deserialize(
                self.call_api(request_type, request_url,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              adv_config_body).content)
            result = response.get('result', {})
            message = response.get('message', "")
            adv_config_id = result.get('id', None)
            if adv_config_id is not None:
                adv_config_id = str(adv_config_id)
                self.logger.info(
                    'Advanced Config has been created {id}.'.format(id=adv_config_id))
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, entity_id=adv_config_id,
                                                    response=response)
            elif message == "Successfully updated Advance configuration":
                self.logger.info('Advanced Config has been updated')
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                self.logger.error(f'Failed to {action_type} advanced config for domain.')
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    error_desc=f'Failed to {action_type} advanced config for domain',
                                                    response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to add adv config for domain.')
            raise DomainError('Error occurred while trying to add adv config for domain')

    def get_or_delete_advance_config_details_for_domain(self, domain_id, key,
                                                        action_type="get"):
        """
        Gets/Deletes advance configuration of domain
        :param domain_id: Entity identifier of the domain
        :type domain_id: String
        :param key: name of the advanced configurations
        :param action_type: values can be get/delete
        :return: response dict
        """
        try:
            if None in {domain_id, key}:
                raise Exception(f"domain_id, key cannot be None")
            request_type = "GET" if action_type.lower() == "get" else "DELETE"
            response = IWUtils.ejson_deserialize(
                self.call_api(request_type, url_builder.advanced_config_domain_url(
                    self.client_config, domain_id) + f"{key}", IWUtils.get_default_header_for_v3(
                    self.client_config['bearer_token'])).content)
            result = response.get('result', {})
            message = response.get("message", "")
            if action_type.lower() == "delete" and message == "Successfully removed Advance configuration":
                self.logger.info(
                    'Successfully deleted the advanced config')
                return GenericResponse.parse_result(status=Response.Status.SUCCESS,
                                                    response=response)

            if result.get('entity_id', None) is None:
                self.logger.error('Failed to find the advance config details')
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    error_desc='Failed to find the advance config details',
                                                    response=response)

            self.logger.info(
                'Successfully got the advanced config details.')
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, entity_id=result.get('entity_id', ''),
                                                response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to get/delete adv config details.')
            raise DomainError('Error occurred while trying to get/delete adv config details.')

    def get_accessible_pipelines_under_domain(self, domain_id):
        """
        Function to get the pipelines accessible under a domain
        :param domain_id: Entity identifier for domain
        :type domain_id: String
        :return: Response Dict
        """
        if domain_id is None:
            self.logger.error("domain_id cannot be None")
            raise Exception("domain_id cannot be None")

        response = None

        try:
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.url_to_get_accessible_pipelines(
                self.client_config, domain_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            return GenericResponse.parse_result(status=Response.Status.SUCCESS, entity_id=domain_id,
                                                response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get accessible pipelines')
            raise DomainError('Error occurred while trying to get accessible pipelines')

    def get_accessible_workflows_under_domain(self, domain_id):
        """
        Function to get the workflows accessible under a domain
        :param domain_id: Entity identifier for domain
        :type domain_id: String
        :return: Response Dict
        """
        if domain_id is None:
            self.logger.error("domain_id cannot be None")
            raise Exception("domain_id cannot be None")

        response = None

        try:
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.url_to_get_accessible_workflows(
                self.client_config, domain_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            return GenericResponse.parse_result(status=Response.Status.SUCCESS, entity_id=domain_id,
                                                response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get accessible workflows')
            raise DomainError('Error occurred while trying to get accessible workflows')
