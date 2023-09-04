from infoworks.error import AdminError
from infoworks.sdk import url_builder
from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.local_configurations import Response, ErrorCode
from infoworks.sdk.generic_response import GenericResponse
from infoworks.sdk.utils import IWUtils


class AdminClient(BaseClient):
    def __init__(self):
        super(AdminClient, self).__init__()
        self._datalineage = {"path": [], "dataflow_objects": [], "master_pipeline_ids": [],
                             "master_sourcetable_ids": []}

    def list_users(self, params=None):
        """
        Function to list the users
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_users = url_builder.list_users_url(self.client_config) + IWUtils.get_query_params_string_from_dict(
            params=params)

        users_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_users,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                while len(result) > 0:
                    users_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", None)
                    if result is None:
                        return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                            error_code=ErrorCode.GENERIC_ERROR,
                                                            error_desc="Error in listing users",
                                                            response=response
                                                            )

                response["result"] = users_list

            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing users")
            raise AdminError("Error in listing users" + str(e))

    def create_new_user(self, data=None, name=None, email=None, roles=None, password=None):
        """
        Function to create new user in Infoworks
        :param data: JSON Payload with user details
        :type data: JSON
        :param name: Name of the user
        :type name: String
        :param email: Email Id of the user
        :type email: String
        :param roles: Comma seperated list of roles
        :type roles: List
        :param password: Password of the user
        :type password: String
        ```
        example_data = {
            "profile": {
                "name": "iwx_sdk_user",
                "email": "iwx_sdk_user@infoworks.io",
                "needs_password_reset": False
            },
            "roles": ["modeller"],
            "password": ""
        }
        ```
        :return: response dict
        """
        if roles is None:
            roles = "modeller"
        if data is None:
            if None in {name, email, roles, password}:
                raise Exception(
                    f"User creation body (data) or all of the name, email, roles, password has to be passed")
            else:
                data = {
                    "profile": {
                        "name": name,
                        "email": email,
                        "needs_password_reset": False
                    },
                    "roles": roles.split(","),
                    "password": password
                }
        try:
            response = self.call_api("POST",
                                     url_builder.list_users_url(self.client_config),
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=data)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    response=parsed_response,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in creating user ")
                                                    )
        except Exception as e:
            self.logger.error("Error in creating user " + str(e))
            raise AdminError("Error in creating user " + str(e))

    def update_the_user(self, user_id, data):
        """
        Function to update the user
        :param user_id: Entity identifier of the user
        :type user_id: String
        :param data: JSON Payload with user details
        :type data: JSON dict
        :return: response dict
        """
        try:
            response = self.call_api("PATCH",
                                     url_builder.list_users_url(self.client_config) + f"/{user_id}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=data)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    response=parsed_response,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in updating user ")
                                                    )
        except Exception as e:
            self.logger.error("Error in updating user " + str(e))
            raise AdminError("Error in updating user " + str(e))

    def delete_the_user(self, user_id):
        """
        Function to delete the user
        :param user_id: Entity identifier of the user
        :type user_id: String
        :return: response dict
        """
        try:
            response = self.call_api("DELETE",
                                     url_builder.list_users_url(self.client_config) + f"/{user_id}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                                     )
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    response=parsed_response,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in deleting user ")
                                                    )
        except Exception as e:
            self.logger.error("Error in deleting user " + str(e))
            raise AdminError("Error in deleting user " + str(e))

    def get_user_details(self, user_id=None, params=None):
        """
        Function to get the user details
        :param user_id: Entity identifier of the user
        :type user_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type params: JSON dict
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}
        if user_id is not None:
            url_to_list_user_details = url_builder.list_users_url(self.client_config) + f"/{user_id}"
        else:
            url_to_list_user_details = url_builder.list_users_url(
                self.client_config) + IWUtils.get_query_params_string_from_dict(params=params)
        users_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_user_details,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                initial_msg = response.get("message", "")
                result = response.get("result", [])
                if user_id is not None:
                    users_list.extend([result])
                else:
                    while len(result) > 0:
                        users_list.extend(result)
                        nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                        response = IWUtils.ejson_deserialize(
                            self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                                self.client_config['bearer_token'])).content)
                        result = response.get("result", [])
                response["result"] = users_list
                response["message"] = initial_msg
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                self.logger.error("Failed to get user details")
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    response="Failed to get user details"
                                                    )

        except Exception as e:
            self.logger.error("Error in listing user information")
            raise AdminError("Error in listing user information" + str(e))

    def add_domains_to_user(self, domain_id, user_id=None, user_email=None):
        """
        Function to make domains accessible to the user
        :param domain_id: Entity identifier for domain
        :type domain_id: String
        :param user_id: Entity identifier for user
        :type user_id: String
        :param user_email: email_id of the user
        :type user_email: String
        :return: response dict
        """
        status_flag = "failed"
        if user_id is None and user_email is not None:
            self.logger.info('Getting userID from given user email....')
            url_for_list_users = url_builder.list_users_url(self.client_config)
            filter_condition = IWUtils.ejson_serialize({"profile.email": user_email})
            get_userid_url = url_for_list_users + f"?filter={{filter_condition}}".format(
                filter_condition=filter_condition)
            if get_userid_url is not None:
                try:
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", get_userid_url,
                                      IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
                    self.logger.info(response)
                    if response is not None and "result" in response:
                        result = response.get("result", [])
                        if len(result) > 0:
                            user_id = result[0]["id"]
                except Exception as e:
                    self.logger.error('Couldnt get result for the user {}'.format(user_email))
                    self.logger.error(str(e))
                    raise AdminError('Couldnt get result for the user {}'.format(user_email))
                finally:
                    if user_id is None:
                        self.logger.error('Couldnt get result for the user {}'.format(user_email))
                        raise AdminError('Couldnt get result for the user {}'.format(user_email))
        response = ""
        if user_id is not None:
            url_for_adding_user_to_domain = url_builder.add_user_to_domain_url(self.client_config, user_id)
            self.logger.info('url for adding user to domain - ' + url_for_adding_user_to_domain)
            if url_for_adding_user_to_domain is not None:
                self.logger.info('Adding user {user} to domain {domain}'.format(user=user_id, domain=domain_id))
                try:
                    json_data = {"entity_ids": [str(domain_id)]}
                    response = IWUtils.ejson_deserialize(
                        self.call_api("POST",
                                      url_for_adding_user_to_domain,
                                      IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                      data=json_data).content)
                    if response.get('message', None) is not None:
                        self.logger.info(response.get('message'))
                        if response.get('message') == "Added Domain(s) to User":
                            status_flag = "success"
                    else:
                        self.logger.error('Error in adding user to domain - {}'.format(response))
                except Exception as e:
                    self.logger.error('Response from server while adding user to domain: {}'.format(user_id))
                    self.logger.error(str(e))
                    raise AdminError('Response from server while adding user to domain: {}'.format(user_id))
        if status_flag == "success":
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        else:
            return GenericResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                response=response)

    def alation_compatible_lineage_for_source(self, src_id, table_id):
        """
        Function to get the lineage of source
        :param src_id: Entity identifier for source
        :type src_id: String
        :param table_id: Entity identifier for table
        :type table_id: String
        :return: source lineage info
        """
        url_to_get_table_details = url_builder.get_table_configuration(self.client_config, src_id, table_id)
        response = self.call_api("GET", url_to_get_table_details,
                                 IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                                 )
        parsed_response = IWUtils.ejson_deserialize(
            response.content)
        if response.status_code == 200 and "result" in parsed_response:
            result = parsed_response.get("result", {})
            target_schema_name = result["configuration"]["target_schema_name"]
            target_table_name = result["configuration"]["target_table_name"]
            orig_db_name = result.get("schema_name_at_source", "csv_source_db")
            orig_table_name = result.get("original_table_name", "")

            path_to_add = [[{"otype": "table",
                             "key": f"<DS_ID>.{orig_db_name}.{orig_table_name}"}],
                           [{"otype": "table",
                             "key": f"<DS_ID>.{target_schema_name}.{target_table_name}"}]]
            self._datalineage["path"].append(path_to_add[::])

            for column in result["columns"]:
                orig_name = column["original_name"]
                col_name = column["name"]
                path_to_add = [[{"otype": "column",
                                 "key": f"<DS_ID>.{orig_db_name}.{orig_table_name}.{orig_name}"}],
                               [{"otype": "column",
                                 "key": f"<DS_ID>.{target_schema_name}.{target_table_name}.{col_name}"}]]
                self._datalineage["path"].append(path_to_add[::])
        output = {"dataflow_objects": self._datalineage["dataflow_objects"], "paths": self._datalineage["path"]}
        return output

    def alation_compatible_lineage_for_pipeline(self, domain_id, pipeline_id, pipeline_version_id,
                                                pl_description=""):
        """
        Function to get the lineage of pipeline
        :param domain_id: Entity identifier for domain
        :type domain_id: String
        :param pipeline_id: Entity identifier for pipeline
        :type pipeline_id: String
        :param pipeline_version_id: Entity identifier for pipeline version
        :type pipeline_version_id: String
        :param pl_description: pipeline description
        :type pl_description: String
        :return: response dict
        """
        url_to_get_pipelineversion_details = url_builder.list_pipeline_versions_url(self.client_config, domain_id,
                                                                                    pipeline_id) + f"/{pipeline_version_id}"
        response = self.call_api("GET", url_to_get_pipelineversion_details,
                                 IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                                 )
        parsed_response = IWUtils.ejson_deserialize(
            response.content)
        if response.status_code == 200 and len(parsed_response.get("result", {})) > 0:
            pipeline_details = parsed_response.get("result", {})
            # hash_object = hashlib.md5(f"{pipeline_id}_{pipeline_version_id}".encode())
            # hashcode = hash_object.hexdigest()
            hashcode = f"{pipeline_id}_{pipeline_version_id}"
            for node in pipeline_details["model"].get("nodes", []):
                req_dict = pipeline_details["model"]["nodes"][node]
                if req_dict['type'].upper() in ["TARGET", "BIGQUERY_TARGET"]:
                    src_node_details = []
                    if req_dict['type'].upper() == "TARGET":
                        target_schema_name = req_dict['properties'].get("target_schema_name", "")
                        target_table_name = req_dict['properties'].get("target_table_name", "")
                    else:
                        target_schema_name = req_dict['properties'].get("dataset_name", "")
                        target_table_name = req_dict['properties'].get("table_name", "")

                    output_columns = [i['name'] for i in req_dict["output_entities"]]
                    for column_name in output_columns:
                        # print(column_name)
                        if not column_name.upper().startswith("ZIW"):
                            url_to_get_pipeline_lineage = url_builder.get_pipeline_lineage_url(self.client_config,
                                                                                               domain_id,
                                                                                               pipeline_id,
                                                                                               pipeline_version_id,
                                                                                               column_name,
                                                                                               node)
                            response = self.call_api("GET", url_to_get_pipeline_lineage,
                                                     IWUtils.get_default_header_for_v3(
                                                         self.client_config['bearer_token'])
                                                     )
                            parsed_response = IWUtils.ejson_deserialize(
                                response.content)
                            if response.status_code == 200 and len(parsed_response.get("result", [])) > 0:
                                graph = parsed_response.get("result", [])
                                temp_output = []
                                source_node = graph[-1]
                                if source_node.get("node_type", "").upper() == "SOURCE_TABLE":
                                    pl_id = source_node.get("pipeline_id", None)
                                    if pl_id is not None and pl_id not in self._datalineage["master_pipeline_ids"]:
                                        self._datalineage["master_pipeline_ids"].append(pl_id)
                                    src_id = source_node.get("source_id", None)
                                    table_id = source_node.get("table_id", "")
                                    if src_id is not None and f"{src_id}:{table_id}" not in self._datalineage[
                                        "master_sourcetable_ids"]:
                                        self._datalineage["master_sourcetable_ids"].append(f"{src_id}:{table_id}")
                                    targetdl_table_name = source_node["target_table_name"]
                                    targetdl_schema_name = source_node["target_schema_name"]
                                    if f"<DS_ID>.{targetdl_schema_name}.{targetdl_table_name}" not in src_node_details:
                                        src_node_details.append(f"<DS_ID>.{targetdl_schema_name}.{targetdl_table_name}")
                                        path_to_add = [[{"otype": "table",
                                                         "key": f"<DS_ID>.{targetdl_schema_name}.{targetdl_table_name}"}],
                                                       [{"otype": "dataflow", "key": f"iwx_pipeline/{hashcode}"}],
                                                       [{"otype": "table",
                                                         "key": f"<DS_ID>.{target_schema_name}.{target_table_name}"}]]
                                        self._datalineage["path"].append(path_to_add[::])
                                    input_port_column = ""
                                    if source_node.get("input_port_column", {}) is not None:
                                        input_port_column = source_node.get("input_port_column", {}).get("name", "")
                                    key = f"<DS_ID>.{targetdl_schema_name}.{targetdl_table_name}.{input_port_column}"
                                    temp = [{"otype": "column", "key": key}]
                                    temp_output.append(temp[::])
                                temp_output.append([{"otype": "dataflow", "key": f"iwx_pipeline/{hashcode}"}])
                                key = f"<DS_ID>.{target_schema_name}.{target_table_name}.{column_name}"
                                temp = [{"otype": "column", "key": key}]
                                temp_output.append(temp[::])
                                self._datalineage["path"].append(temp_output[::])
                        else:
                            # ziw column direct connect to pipeline
                            path_to_add = [[{"otype": "dataflow", "key": f"iwx_pipeline/{hashcode}"}],
                                           [{"otype": "column",
                                             "key": f"<DS_ID>.{target_schema_name}.{target_table_name}.{column_name}"}]]
                            self._datalineage["path"].append(path_to_add[::])
            self._datalineage["dataflow_objects"].append(
                {"external_id": f"iwx_pipeline/{hashcode}", "content": pl_description})
            output = {"dataflow_objects": self._datalineage["dataflow_objects"], "paths": self._datalineage["path"]}
            return output

    def alation_compatible_lineage_for_iwx_metadata(self, domain_id, pipeline_id, sourcetable_ids=None):
        """
        Function to ger complete lineage info
        :param sourcetable_ids: Comma seperated src_id:table_id combination
        :type sourcetable_ids: Comma seperated src_id:table_id combination
        :param domain_id: Entity identifier for domain
        :type domain_id: String
        :param pipeline_id: Entity identifier for pipeline
        :type pipeline_id: String
        :return: lineage info
        """
        self._datalineage["master_pipeline_ids"].append(pipeline_id)
        # Check if there are any more master pipeline ids
        while len(self._datalineage["master_pipeline_ids"]) > 0:
            parent_pipeline_id = self._datalineage["master_pipeline_ids"].pop()
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_builder.get_pipeline_url(self.client_config, domain_id, parent_pipeline_id),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                              ).content)
            if response is not None and "result" in response:
                pipeline_version_id = response.get("result").get("active_version_id")
                pipeline_description = response.get("result").get("description", "")
                self.alation_compatible_lineage_for_pipeline(domain_id, parent_pipeline_id, pipeline_version_id,
                                                             pipeline_description)
        if sourcetable_ids is not None:
            self._datalineage["master_sourcetable_ids"].extend(sourcetable_ids.split(","))
        while len(self._datalineage["master_sourcetable_ids"]) > 0:
            src_table_id = self._datalineage["master_sourcetable_ids"].pop()
            src_id, table_id = src_table_id.split(":")
            self.alation_compatible_lineage_for_source(src_id, table_id)

    def get_environment_details(self, environment_id=None, params=None):
        """
        Function to get environment details
        :param environment_id: Entity identifier of the environment
        :type environment_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type params: Dict
        :return: Response Dict
        """
        if params is None and environment_id is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_environments = url_builder.get_environment_details(
            self.client_config, environment_id) + IWUtils.get_query_params_string_from_dict(params=params)
        env_details = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_environments,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", None)
                if result is None:
                    self.logger.error("Failed to get environment details for given id")
                    return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                        error_code=ErrorCode.USER_ERROR,
                                                        response=response,
                                                        error_desc="Failed to get environment details"
                                                        )
                initial_msg = response.get("message", "")
                if environment_id is not None:
                    env_details.extend([result])
                else:
                    while len(result) > 0:
                        env_details.extend(result)
                        nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                        response = IWUtils.ejson_deserialize(
                            self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                                self.client_config['bearer_token'])).content)
                        result = response.get("result", [])
                response["result"] = env_details
                response["message"] = initial_msg
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                self.logger.error("Failed to get environment details")
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    error_desc="Failed to get environment details",
                                                    response=response
                                                    )
        except Exception as e:
            self.logger.error("Error in getting environment details " + str(e))
            raise AdminError("Error in getting environment details" + str(e))

    def create_environment(self, environment_body=None):
        """
        Function to create data environment
        :param environment_body: Body of the data environment
        :type environment_body: Dict
        :return: Response Dict
        """
        if environment_body is None:
            self.logger.error("environment_body cannot be None")
            raise Exception("environment_body cannot be None")
        url_to_list_environments = url_builder.get_environment_details(
            self.client_config)
        try:
            response = self.call_api("POST", url_to_list_environments,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),environment_body)
            parsed_response = IWUtils.ejson_deserialize(response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    response=parsed_response,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in creating environment ")
                                                    )
        except Exception as e:
            self.logger.error("Error in creating environment" + str(e))
            raise AdminError("Error in creating environment" + str(e))

    def update_environment(self, environment_id = None,environment_body=None):
        """
        Function to update data environment
        :param environment_id: Id of the data environment
        :type environment_body: string
        :param environment_body: Body of the data environment
        :type environment_body: Dict
        :return: Response Dict
        """
        if None in [environment_body,environment_id]:
            self.logger.error("environment_id or environment_body cannot be None")
            raise Exception("environment_id or environment_body cannot be None")
        url_to_list_environments = url_builder.get_environment_details(
            self.client_config)+f"/{environment_id}"
        try:
            response = self.call_api("PATCH", url_to_list_environments,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),environment_body)
            parsed_response = IWUtils.ejson_deserialize(response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    response=parsed_response,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in updating environment ")
                                                    )
        except Exception as e:
            self.logger.error("Error in updating environment" + str(e))
            raise AdminError("Error in updating environment" + str(e))

    def get_storage_details(self, environment_id, storage_id=None, params=None):
        """
        Function to get storage details
        :param environment_id: Entity identifier of the environment
        :type environment_id: String
        :param storage_id: Entity identifier of the storage
        :type storage_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type params: Dict
        :return: Response Dict
        """
        if params is None and storage_id is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_storages = url_builder.get_environment_storage_details(
            self.client_config, environment_id, storage_id) + IWUtils.get_query_params_string_from_dict(params=params)
        storage_details = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_storages,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", None)
                if result is None:
                    self.logger.error("Failed to get environment storage details for given id")
                    return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                        error_code=ErrorCode.USER_ERROR,
                                                        response=response,
                                                        error_desc="Failed to get environment storage details"
                                                        )
                initial_msg = response.get("message", "")
                if storage_id is not None:
                    storage_details.extend([result])
                else:
                    while len(result) > 0:
                        storage_details.extend(result)
                        nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                        response = IWUtils.ejson_deserialize(
                            self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                                self.client_config['bearer_token'])).content)
                        result = response.get("result", [])
                response["result"] = storage_details
                response["message"] = initial_msg
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                self.logger.error("Failed to get storage details")
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    response="Failed to get storage details"
                                                    )
        except Exception as e:
            self.logger.error("Error in getting storage details " + str(e))
            raise AdminError("Error in getting storage details" + str(e))

    def get_compute_template_details(self, environment_id, compute_id=None, is_interactive=False, params=None):
        """
         Function to get compute template details
         :param environment_id: Entity identifier of the environment
         :type environment_id: String
         :param compute_id: Entity identifier of compute cluster
         :type compute_id: String
         :param is_interactive: True/False. If True only the details of interactive clusters is fetched
         :type is_interactive: Boolean
         :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
         :type params: Dict
         :return: Response Dict
         """
        if params is None and compute_id is None:
            params = {"limit": 20, "offset": 0}
        if is_interactive:
            url_to_list_computes = url_builder.get_environment_interactive_compute_details(
                self.client_config, environment_id, compute_id) + IWUtils.get_query_params_string_from_dict(
                params=params)
        else:
            url_to_list_computes = url_builder.get_environment_compute_details(
                self.client_config, environment_id, compute_id) + IWUtils.get_query_params_string_from_dict(
                params=params)
        compute_details = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_computes,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None and "result" in response:
                result = response.get("result", None)
                if result is None:
                    self.logger.error("Failed to get environment compute details for given id")
                    return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                        error_code=ErrorCode.USER_ERROR,
                                                        response=response,
                                                        error_desc="Failed to get environment compute details"
                                                        )
                initial_msg = response.get("message", "")
                if compute_id is not None:
                    compute_details.extend([result])
                else:
                    while len(result) > 0:
                        compute_details.extend(result)
                        nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                        response = IWUtils.ejson_deserialize(
                            self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                                self.client_config['bearer_token'])).content)
                        result = response.get("result", [])
                response["result"] = compute_details
                response["message"] = initial_msg
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                self.logger.error("Failed to get compute template details")
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    error_desc="Failed to get compute template details",
                                                    response=response
                                                    )
        except Exception as e:
            self.logger.error("Error in getting compute template details " + str(e))
            raise AdminError("Error in getting compute template details" + str(e))

    def get_environment_id_from_name(self, environment_name):
        """
        Function to return environment Id from name
        :param environment_name: Infoworks Environment Name
        :type environment_name: String
        :return: Response Dict
        """
        response = self.get_environment_details(environment_id=None, params={"filter": {"name": environment_name}})
        if response.get("result", {}).get("status", "") == Response.Status.SUCCESS:
            result = response.get("result", {}).get("response", {}).get("result", {})
            if len(result) > 0:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS,
                                                    response={"environment_id": result[0]["id"]})
            else:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS,
                                                    response={"environment_id": None}
                                                    )
        else:
            return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                error_code=ErrorCode.USER_ERROR,
                                                response=response.get("result", {}).get("response", {})
                                                )

    def get_environment_default_warehouse(self, environment_id):
        """
        Function to return default warehouse name
        :param environment_id: Infoworks Environment Id
        :type environment_id: String
        :return: Response Dict
        """
        response = self.get_environment_details(environment_id=environment_id, params=None)
        if response.get("result", {}).get("status", "") == Response.Status.SUCCESS:
            result = response.get("result", {}).get("response", {}).get("result", [])
            if len(result) > 0:
                if result[0].get("data_warehouse_type", "") == "":
                    return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                        error_code=ErrorCode.USER_ERROR,
                                                        error_desc="get data warehouse method is not available for this environment",
                                                        response={"default_warehouse": None})
                data_warehouse_configuration = result[0]["data_warehouse_configuration"]
                if data_warehouse_configuration["type"].lower() == "snowflake":
                    snowflake_profiles = data_warehouse_configuration.get("snowflake_profiles", [])
                    for profile in snowflake_profiles:
                        if profile.get("is_default_profile", False):
                            for warehouse in profile.get("warehouse", []):
                                if warehouse.get("is_default_warehouse", False):
                                    return GenericResponse.parse_result(status=Response.Status.SUCCESS,
                                                                        response={
                                                                            "default_warehouse": warehouse["name"]})
            else:
                if result.get("data_warehouse_type", "") == "":
                    return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                        error_code=ErrorCode.USER_ERROR,
                                                        error_desc="get data warehouse method is not available for this environment",
                                                        response={"default_warehouse": None})
                data_warehouse_configuration = result["data_warehouse_configuration"]
                if data_warehouse_configuration["type"].lower() == "snowflake":
                    snowflake_profiles = data_warehouse_configuration.get("snowflake_profiles", [])
                    for profile in snowflake_profiles:
                        if profile.get("is_default_profile", False):
                            for warehouse in profile.get("warehouse", []):
                                if warehouse.get("is_default_warehouse", False):
                                    return GenericResponse.parse_result(status=Response.Status.SUCCESS,
                                                                        response={
                                                                            "default_warehouse": warehouse["name"]})
                    return GenericResponse.parse_result(status=Response.Status.SUCCESS,
                                                        response={"default_warehouse": None})
        else:
            return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                error_code=ErrorCode.USER_ERROR,
                                                response=response.get("result", {}).get("response", {})
                                                )

    def get_compute_id_from_name(self, environment_id, compute_name):
        """
        Function to return Environment Compute Id from name
        :param environment_id: Environment identifier
        :type environment_id: String
        :param compute_name: Environment Compute Name
        :type compute_name: String
        :return: Response Dict
        """
        response = self.get_compute_template_details(environment_id, compute_id=None, is_interactive=True,
                                                     params={"filter": {"name": compute_name}})
        if response.get("result", {}).get("status", "") == Response.Status.SUCCESS:
            result = response.get("result", {}).get("response", {}).get("result", [])
            if len(result) > 0:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS,
                                                    response={"compute_id": result[0]["id"]})
            else:
                response_non_interactive = self.get_compute_template_details(environment_id, compute_id=None,
                                                                             is_interactive=False,
                                                                             params={"filter": {"name": compute_name}})
                result_non_interactive = response_non_interactive.get("result", {}).get("response", {}).get("result",
                                                                                                            [])
                if len(result_non_interactive) > 0:
                    return GenericResponse.parse_result(status=Response.Status.SUCCESS,
                                                        response={"compute_id": result_non_interactive[0]["id"]})
                else:
                    return GenericResponse.parse_result(status=Response.Status.SUCCESS,
                                                        response={"compute_id": None}
                                                        )
        else:
            return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                error_code=ErrorCode.USER_ERROR,
                                                response=response.get("result", {}).get("response", {})
                                                )

    def get_storage_id_from_name(self, environment_id, storage_name):
        """
        Function to return Environment Storage Id from name
        :param environment_id: Environment identifier
        :type environment_id: String
        :param storage_name: Environment Storage Name
        :type storage_name: String
        :return: Response Dict
        """
        response = self.get_storage_details(environment_id=environment_id,
                                            params={"filter": {"name": storage_name}})
        if response.get("result", {}).get("status", "") == Response.Status.SUCCESS:
            result = response.get("result", {}).get("response", {}).get("result", [])
            if len(result) > 0:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS,
                                                    response={"storage_id": result[0]["id"]})
            else:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS,
                                                    response={"storage_id": None}
                                                    )
        else:
            return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                error_code=ErrorCode.USER_ERROR,
                                                response=response.get("result", {}).get("response", {})
                                                )

    def get_source_extension(self, extension_id):
        """
        Function to get a source extension details
        :param extension_id: ID of the source extension
        :type extension_id: String
        :return: Response Dict
        """
        try:
            response = self.call_api("GET",
                                     url_builder.create_source_extension_url(self.client_config) + f"/{extension_id}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                                     )
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response.get("details",
                                                                                   f"Error getting source extension {extension_id}."),
                                                    response=parsed_response
                                                    )
        except Exception as e:
            self.logger.error(f"Error getting source extension {extension_id}.")
            raise AdminError(
                f"Error getting source extension {extension_id}." + str(e))

    def create_source_extension(self, data):
        """
        Function to create a source extension
        :param data: Source extension details body
        :type data: Dict
        :return: Response Dict
        """
        try:
            response = self.call_api("POST",
                                     url_builder.create_source_extension_url(self.client_config),
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=data)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response.get("details",
                                                                                   f"Error creating source extension {data['name']}")
                                                    )
        except Exception as e:
            self.logger.error(f"Error creating source extension {data['name']}.Please do it by yourself from UI.")
            raise AdminError(
                f"Error creating source extension  {data['name']}.Please do it by yourself from UI." + str(e))

    def update_source_extension(self, extension_id, data=None):
        """
        Function to update a source extension
        :param extension_id: Id of the source extension to update
        :type extension_id: String
        :param data: JSON dict of the body to update
        :type data: JSON dict
        :return: Response Dict
        """
        if None in {extension_id} and data is None:
            self.logger.error("Extension id or data cannot be None")
            raise Exception("Extension id or data cannot be None")
        try:
            response = self.call_api("PUT",
                                     url_builder.create_source_extension_url(self.client_config) + f"/{extension_id}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                                     , data=data)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response.get("details",
                                                                                   f"Error updating source extension {extension_id}."),
                                                    response=parsed_response
                                                    )
        except Exception as e:
            self.logger.error(f"Error updating source extension {extension_id}.")
            raise AdminError(
                f"Error updating source extension {extension_id}." + str(e))

    def delete_source_extension(self, extension_id):
        """
        Function to delete a source extension
        :param extension_id: ID of the source extension
        :type extension_id: String
        :return: Response Dict
        """
        try:
            response = self.call_api("DELETE",
                                     url_builder.create_source_extension_url(self.client_config) + f"/{extension_id}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                                     )
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response.get("details",
                                                                                   f"Error deleting source extension {extension_id}."),
                                                    response=parsed_response
                                                    )
        except Exception as e:
            self.logger.error(f"Error deleting source extension {extension_id}.")
            raise AdminError(
                f"Error deleting source extension {extension_id}." + str(e))

    def create_data_connection(self, config_body):
        """
        Function to create data connection in the domain
        :param config_body: JSON payload
        :type config_body: JSON dict
        ```
        config_body_example = {
        "name": "Snowflake-connection",
        "type": "TARGET",
        "sub_type": "SNOWFLAKE",
        "properties": {
            "url": "https://account_name.snowflakecomputing.com",
            "account": "",
            "user_name": "ramesh",
            "password": {
                "password_type": "infoworks_managed"
                "secret_id": "6418304b7eeb1c40de2b6008" --> Needed if password_type is secret_store
            },
            "password": "", -->  Needed if the password_type is infoworks_managed
            "additional_params": "",
            "warehouse": "TEST_WH"
        }
        }
        ```
        :return: response dict
        """
        try:
            if config_body is None:
                raise Exception("config_body cannot be None/Null")
            create_data_connection_url = url_builder.create_data_connection(self.client_config)
            response = self.call_api("POST", create_data_connection_url,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                self.logger.error("Failed to create data connection")
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Failed to create data connection"),
                                                    job_id=None, response=parsed_response)
        except Exception as e:
            raise AdminError(f"Failed to create data connection" + str(e))

    def update_data_connection(self, data_connection_id, config_body):
        """
        Function to create data connection in the domain
        :param data_connection_id: Entity identifier of the data connection id
        :param config_body: JSON payload
        :type config_body: JSON dict
        ```
        config_body_example = {
        "name": "Snowflake-connection",
        "type": "TARGET",
        "sub_type": "SNOWFLAKE",
        "properties": {
            "url": "https://account_name.snowflakecomputing.com",
            "account": "",
            "username": "ramesh",
            "password": {
                "password_type": "infoworks_managed",
                "secret_id": "6418304b7eeb1c40de2b6008" --> Needed if password_type is secret_store
            },
            "password": "", -->  Needed if the password_type is infoworks_managed
            "additional_params": "",
            "warehouse": "TEST_WH"
        }
        }
        ```
        :return: response dict
        """
        try:
            if config_body is None or data_connection_id is None:
                raise Exception("config_body and data_connection_id cannot be None/Null")
            create_data_connection_url = url_builder.create_data_connection(
                self.client_config) + f"/{data_connection_id}"
            response = self.call_api("PUT", create_data_connection_url,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                self.logger.error("Failed to update the data connection")
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Failed to update data connection"),
                                                    job_id=None, response=parsed_response)
        except Exception as e:
            raise AdminError(f"Failed to update the data connection" + str(e))

    def get_data_connection(self, data_connection_id=None, params=None):
        """
        Function to create data connection in the domain
        :param data_connection_id: id of dataconnection for which config has to be fetched
        :type data_connection_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}

        if data_connection_id is None:
            url_to_get_data_connection = url_builder.create_data_connection(
                self.client_config) + IWUtils.get_query_params_string_from_dict(params=params)
        else:
            url_to_get_data_connection = url_builder.create_data_connection(
                self.client_config) + f"/{data_connection_id}"
        dataconnection_list = []
        try:
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_to_get_data_connection,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                initial_msg = response.get("message", "")
                if data_connection_id is not None:
                    dataconnection_list.append(result)
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
                response["result"] = dataconnection_list
                response["message"] = initial_msg
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                self.logger.error("Failed to get data connection details")
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    response="Failed to get data connection details"
                                                    )
        except Exception as e:
            raise AdminError(f"Failed to get data connection details" + str(e))

    def delete_data_connection(self, data_connection_id):
        """
        Function to delete data connection in the domain
        :param data_connection_id: Entity identifier for the data connection
        :type data_connection_id: String
        :return: response dict
        """
        try:
            if data_connection_id is None:
                raise Exception("data_connection_id cannot be None")
            create_data_connection_url = url_builder.create_data_connection(self.client_config).strip(
                "/") + f"/{data_connection_id}"
            response = self.call_api("DELETE", create_data_connection_url,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     )
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                self.logger.error("Failed to delete data connection")
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Failed to delete data connection"),
                                                    response=parsed_response)
        except Exception as e:
            raise AdminError(f"Failed in delete data connection" + str(e))

    def list_secret_stores(self, params=None):
        """
        Function to list the secret stores
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_secret_stores = url_builder.list_secret_stores_url(self.client_config) \
                                    + IWUtils.get_query_params_string_from_dict(params=params)

        secret_stores_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_secret_stores,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                while len(result) > 0:
                    secret_stores_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", None)
                    if result is None:
                        return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                            error_code=ErrorCode.GENERIC_ERROR,
                                                            error_desc="Error in listing secret store",
                                                            response=response
                                                            )

                response["result"] = secret_stores_list

            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing secret stores")
            raise AdminError("Error in listing secret store" + str(e))

    def create_secret_store(self, data=None):
        """
        Function to create new secret store in Infoworks
        :param data: JSON Payload with secret store details
        :type data: JSON
        ```
        example_data = {
            "name": "iwx-sp-secret-store",
            "service_type": "azure",
            "key_vault_uri": "https://iwazurecsdbkey.vault.azure.net/",
            "service_authentication": "64182fac35298f0d05a8b689",
        }
        ```
        :return: response dict
        """
        try:
            response = self.call_api("POST",
                                     url_builder.list_secret_stores_url(self.client_config),
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=data)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    response=parsed_response,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in creating secret store ")
                                                    )
        except Exception as e:
            self.logger.error("Error in creating secret store " + str(e))
            raise AdminError("Error in creating secret store " + str(e))

    def get_secret_store_details(self, secret_store_id=None):
        """
        Function to get secret store details in Infoworks
        :param secret_store_id: secret store id in infoworks
        :type secret_store_id: String
        :return: response dict
        """
        try:
            if secret_store_id is None:
                self.logger.error("secret_store_id cannot be None")
                raise Exception("secret_store_id cannot be None")
            response = self.call_api("GET",
                                     url_builder.list_secret_stores_url(self.client_config) + f"/{secret_store_id}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    response=parsed_response,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in getting secret store ")
                                                    )
        except Exception as e:
            self.logger.error("Error in getting secret store " + str(e))
            raise AdminError("Error in getting secret store " + str(e))

    def update_secret_store_details(self, secret_store_id=None, data=None):
        """
        Function to updates secret store details in Infoworks
        :param secret_store_id: secret store id in infoworks
        :type secret_store_id: String
        :param data: JSON Payload with secret store details
        :type data: JSON
        ```
        example_data = {
            "name": "iwx-sp-secret-store",
            "service_type": "azure",
            "key_vault_uri": "https://iwazurecsdbkey.vault.azure.net/",
            "service_authentication": "64182fac35298f0d05a8b689",
        }
        ```
        :return: response dict
        """
        try:
            if None in [secret_store_id, data]:
                self.logger.error("secret_store_id or data cannot be None")
                raise Exception("secret_store_id or data cannot be None")
            response = self.call_api("PATCH",
                                     url_builder.list_secret_stores_url(self.client_config) + f"/{secret_store_id}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']), data)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    response=parsed_response,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in updating secret store ")
                                                    )
        except Exception as e:
            self.logger.error("Error in updating secret store " + str(e))
            raise AdminError("Error in updating secret store " + str(e))

    def delete_secret_store(self, secret_store_id=None):
        """
        Function to delete secret store details in Infoworks
        :param secret_store_id: secret store id in infoworks
        :type secret_store_id: String
        :return: response dict
        """
        try:
            if secret_store_id is None:
                self.logger.error("secret_store_id cannot be None")
                raise Exception("secret_store_id cannot be None")
            response = self.call_api("DELETE",
                                     url_builder.list_secret_stores_url(self.client_config) + f"/{secret_store_id}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    response=parsed_response,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in deleting secret store ")
                                                    )
        except Exception as e:
            self.logger.error("Error in deleting secret store " + str(e))
            raise AdminError("Error in deleting secret store " + str(e))

    def list_service_authentication(self, params=None):
        """
        Function to list the service authentication mechanisms in Infoworks
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_service_authentication = url_builder.list_service_authentication_url(self.client_config) \
                                             + IWUtils.get_query_params_string_from_dict(params=params)

        secret_stores_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_service_authentication,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                while len(result) > 0:
                    secret_stores_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", None)
                    if result is None:
                        return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                            error_code=ErrorCode.GENERIC_ERROR,
                                                            error_desc="Error in listing service authentication",
                                                            response=response
                                                            )

                response["result"] = secret_stores_list

            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing service authentication")
            raise AdminError("Error in listing service authentication" + str(e))

    def create_service_authentication(self, data=None):
        """
        Function to create new service authentication in Infoworks
        :param data: JSON Payload with service auth details
        :type data: JSON
        ```
        example_data = {
            "name": "iw-azure-cs-db-cluster-sp",
            "service_type": "azure",
            "authentication_type": "service_principal",
            "authentication_properties": {
                "subscription_id": "xxx",
                "client_id": "xxx",
                "object_id": "xxx",
                "tenant_id": "xxx"
            }
        }
        ```
        :return: response dict
        """
        try:
            if data is None:
                self.logger.error("data cannot be None")
                raise Exception("data cannot be None")
            response = self.call_api("POST",
                                     url_builder.list_service_authentication_url(self.client_config),
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=data)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    response=parsed_response,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in creating service authentication mechanism ")
                                                    )
        except Exception as e:
            self.logger.error("Error in creating service authentication mechanism " + str(e))
            raise AdminError("Error in creating service authentication mechanism" + str(e))

    def get_service_authentication_details(self, service_auth_id=None):
        """
        Function to get service authentication in Infoworks
        :param service_auth_id: secret auth id in infoworks
        :type service_auth_id: String
        :return: response dict
        """
        try:
            if service_auth_id is None:
                self.logger.error("service_auth_id cannot be None")
                raise Exception("service_auth_id cannot be None")
            response = self.call_api("GET",
                                     url_builder.list_service_authentication_url(
                                         self.client_config) + f"/{service_auth_id}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    response=parsed_response,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in getting service authentication ")
                                                    )
        except Exception as e:
            self.logger.error("Error in getting service authentication" + str(e))
            raise AdminError("Error in getting service authentication " + str(e))

    def update_service_authentication_details(self, service_auth_id=None, data=None):
        """
        Function to updates service auth details in Infoworks
        :param service_auth_id: service auth id in infoworks
        :type service_auth_id: String
        :param data: JSON Payload with service auth details
        :type data: JSON
        ```
        example_data = {
            "name": "iw-azure-cs-db-cluster-sp",
            "service_type": "azure",
            "authentication_type": "service_principal",
            "authentication_properties": {
                "subscription_id": "xxx",
                "client_id": "xxx",
                "object_id": "xxx",
                "tenant_id": "xxx"
            }
        }
        ```
        :return: response dict
        """
        try:
            if None in [service_auth_id, data]:
                self.logger.error("service_auth_id or data cannot be None")
                raise Exception("service_auth_id or data cannot be None")
            response = self.call_api("PATCH",
                                     url_builder.list_service_authentication_url(
                                         self.client_config) + f"/{service_auth_id}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']), data)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    response=parsed_response,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in updating service auth ")
                                                    )
        except Exception as e:
            self.logger.error("Error in updating service auth " + str(e))
            raise AdminError("Error in updating service auth " + str(e))

    def delete_service_authentication(self, service_auth_id=None):
        """
        Function to delete service authentication in Infoworks
        :param service_auth_id: secret auth id in infoworks
        :type service_auth_id: String
        :return: response dict
        """
        try:
            if service_auth_id is None:
                self.logger.error("service_auth_id cannot be None")
                raise Exception("service_auth_id cannot be None")
            response = self.call_api("DELETE",
                                     url_builder.list_service_authentication_url(
                                         self.client_config) + f"/{service_auth_id}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    response=parsed_response,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in deleting service auth ")
                                                    )
        except Exception as e:
            self.logger.error("Error in deleting service auth " + str(e))
            raise AdminError("Error in deleting service auth " + str(e))

    def list_secrets(self, params=None):
        """
        Function to list the secrets in Infoworks
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_secrets = url_builder.list_secrets_url(self.client_config) \
                              + IWUtils.get_query_params_string_from_dict(params=params)

        secret_stores_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_secrets,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                while len(result) > 0:
                    secret_stores_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", None)
                    if result is None:
                        return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                            error_code=ErrorCode.GENERIC_ERROR,
                                                            error_desc="Error in listing secrets",
                                                            response=response
                                                            )

                response["result"] = secret_stores_list

            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing secrets")
            raise AdminError("Error in listing secrets" + str(e))

    def create_secret(self, data=None):
        """
        Function to create new secret in Infoworks
        :param data: JSON Payload with secret details
        :type data: JSON
        ```
        example_data = {
            "name": "mongo-pg-pwd",
            "secret_store": "64182fd27eeb1c40de2b6007",
            "secret_name": "mongo-pg-pwd"
        }
        ```
        :return: response dict
        """
        try:
            response = self.call_api("POST",
                                     url_builder.list_secrets_url(self.client_config),
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=data)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    response=parsed_response,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in creating secret")
                                                    )
        except Exception as e:
            self.logger.error("Error in creating secret" + str(e))
            raise AdminError("Error in creating secret" + str(e))

    def get_secret_details(self, secret_id=None):
        """
        Function to get secret details in Infoworks
        :param secret_id: secret id in infoworks
        :type secret_id: String
        :return: response dict
        """
        try:
            if secret_id is None:
                self.logger.error("secret_id cannot be None")
                raise Exception("secret id cannot be None")
            response = self.call_api("GET",
                                     url_builder.list_secrets_url(self.client_config) + f"/{secret_id}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    response=parsed_response,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in getting secret details")
                                                    )
        except Exception as e:
            self.logger.error("Error in getting secret details" + str(e))
            raise AdminError("Error in getting secret details " + str(e))

    def update_secret_details(self, secret_id=None, data=None):
        """
        Function to updates secret details in Infoworks
        :param secret_id: secret id in infoworks
        :type secret_id: String
        :param data: JSON Payload with secret store details
        :type data: JSON
        ```
        example_data = {
            "name": "mongo-pg-pwd",
            "secret_store": "64182fd27eeb1c40de2b6007",
            "secret_name": "mongo-pg-pwd"
        }
        ```
        :return: response dict
        """
        try:
            response = self.call_api("PATCH",
                                     url_builder.list_secrets_url(self.client_config) + f"/{secret_id}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']), data)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    response=parsed_response,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in updating secret ")
                                                    )
        except Exception as e:
            self.logger.error("Error in updating secret " + str(e))
            raise AdminError("Error in updating secret " + str(e))

    def delete_secret(self, secret_id=None):
        """
        Function to delete secret details in Infoworks
        :param secret_id: secret id in infoworks
        :type secret_id: String
        :return: response dict
        """
        try:
            response = self.call_api("DELETE",
                                     url_builder.list_secrets_url(self.client_config) + f"/{secret_id}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    response=parsed_response,
                                                    error_desc=parsed_response.get("details",
                                                                                   "Error in deleting secret ")
                                                    )
        except Exception as e:
            self.logger.error("Error in deleting secret " + str(e))
            raise AdminError("Error in deleting secret " + str(e))

    def list_domains_as_admin(self, params=None):
        """
        Function to list the domains as admin user
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_domains = url_builder.list_domains_admin_url(
            self.client_config) + IWUtils.get_query_params_string_from_dict(
            params=params)

        users_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_domains,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                while len(result) > 0:
                    users_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", None)
                    if result is None:
                        return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                            error_code=ErrorCode.GENERIC_ERROR,
                                                            error_desc="Error in listing domains",
                                                            response=response
                                                            )

                response["result"] = users_list

            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing domains")
            raise AdminError("Error in listing domains" + str(e))

    def stop_interactive_cluster(self, data):
        """
        Function to stop an interactive cluster
        :param data: Body to be passed to the API.
        example: {
          "compute_template_id": "computer_template_id"
        }
        :type data: Dict
        :return: Response Dict
        """
        try:
            response = self.call_api("POST",
                                     url_builder.stop_interactive_cluster_url(self.client_config),
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=data)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response.get("details",
                                                                                   f"Error stopping the cluster")
                                                    )
        except Exception as e:
            self.logger.error(f"Error stopping the cluster. Please do it by yourself from UI.")
            raise AdminError(
                f"Error stopping the cluster. Please do it by yourself from UI." + str(e))

    def stop_cluster_async(self, data):
        """
        Function to stop a persistent cluster
        :param data: Body to be passed to the API.
        example: {
          "compute_template_id": "computer_template_id"
        }
        :type data: Dict
        :return: Response Dict
        """
        try:
            response = self.call_api("POST",
                                     url_builder.terminate_persistent_cluster_aysnc(self.client_config),
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=data)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response.get("details",
                                                                                   f"Error stopping the cluster")
                                                    )
        except Exception as e:
            self.logger.error(f"Error stopping the cluster. Please do it by yourself from UI.")
            raise AdminError(
                f"Error stopping the cluster. Please do it by yourself from UI." + str(e))

    def restart_persistent_cluster(self, data):
        """
        Function to restart a persistent cluster
        :param data: Body to be passed to the API.
        example: {
          "compute_template_id": "computer_template_id"
        }
        :type data: Dict
        :return: Response Dict
        """
        try:
            response = self.call_api("PUT",
                                     url_builder.restart_persistent_cluster_url(self.client_config),
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=data)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response.get("details",
                                                                                   f"Error restarting the cluster")
                                                    )
        except Exception as e:
            self.logger.error(f"Error restarting the cluster. Please do it by yourself from UI.")
            raise AdminError(
                f"Error restarting the cluster. Please do it by yourself from UI." + str(e))

    def stop_persistent_cluster(self, data):
        """
        Function to stop a persistent cluster
        :param data: Body to be passed to the API.
        example: {
          "compute_template_id": "computer_template_id"
        }
        :type data: Dict
        :return: Response Dict
        """
        try:
            response = self.call_api("PUT",
                                     url_builder.stop_persistent_cluster_url(self.client_config),
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=data)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code == 200:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.GENERIC_ERROR,
                                                    error_desc=parsed_response.get("details",
                                                                                   f"Error stop the cluster")
                                                    )
        except Exception as e:
            self.logger.error(f"Error stop the cluster. Please do it by yourself from UI.")
            raise AdminError(
                f"Error stop the cluster. Please do it by yourself from UI." + str(e))

    def list_accessible_sources_under_domain_as_admin(self, domain_id, params=None):
        """
        Function to list the accessible sources under domain
        :param domain_id: Entity identifier for domain
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}
        accessible_sources_url = url_builder.url_to_list_accessible_sources(self.client_config,
                                                                            domain_id) + IWUtils.get_query_params_string_from_dict(
            params=params)

        output_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", accessible_sources_url,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                while len(result) > 0:
                    output_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", None)
                    if result is None:
                        return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                            error_code=ErrorCode.GENERIC_ERROR,
                                                            error_desc="Error in listing accessible sources",
                                                            response=response
                                                            )

                response["result"] = output_list
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing accessible sources")
            raise AdminError("Error in listing accessible sources" + str(e))

    def delete_user_schedules(self, user_id=None, user_email=None):
        """
        Function to delete the user schedules. Pass either user_id or user_email
        :param user_id: Entity identifier of the user
        :param user_email: Email of the user
        :type user_id: String
        :return: response dict
        """
        if user_id is None and user_email is not None:
            self.logger.info('Getting userID from given user email....')
            url_for_list_users = url_builder.list_users_url(self.client_config)
            filter_condition = IWUtils.ejson_serialize({"profile.email": user_email})
            get_userid_url = url_for_list_users + f"?filter={{filter_condition}}".format(
                filter_condition=filter_condition)
            if get_userid_url is not None:
                try:
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", get_userid_url,
                                      IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
                    self.logger.info(response)
                    if response is not None and "result" in response:
                        result = response.get("result", [])
                        if len(result) > 0:
                            user_id = result[0]["id"]
                except Exception as e:
                    self.logger.error('Couldnt get result for the user {}'.format(user_email))
                    self.logger.error(str(e))
                    raise AdminError('Couldnt get result for the user {}'.format(user_email))
                finally:
                    if user_id is None:
                        self.logger.error('Couldnt get result for the user {}'.format(user_email))
                        raise AdminError('Couldnt get result for the user {}'.format(user_email))
        if user_id is not None:
            try:
                response = self.call_api("DELETE",
                                         url_builder.delete_user_schedules_url(self.client_config, user_id),
                                         IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                                         )
                parsed_response = IWUtils.ejson_deserialize(
                    response.content)
                if response.status_code == 200:
                    return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
                else:
                    return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                        error_code=ErrorCode.GENERIC_ERROR,
                                                        response=parsed_response,
                                                        error_desc=parsed_response.get("details",
                                                                                       "Error in deleting schedules ")
                                                        )
            except Exception as e:
                self.logger.error("Error in deleting schedules " + str(e))
                raise AdminError("Error in deleting schedules " + str(e))

    def list_schedules_as_admin(self, params=None):
        """
        Function to list all the schedules as an admin
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}
        schedules_url = url_builder.list_all_schedules_url(
            self.client_config) + IWUtils.get_query_params_string_from_dict(
            params=params)

        output_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", schedules_url,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                while len(result) > 0:
                    output_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", None)
                    if result is None:
                        return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                            error_code=ErrorCode.GENERIC_ERROR,
                                                            error_desc="Error in listing schedules",
                                                            response=response
                                                            )

                response["result"] = output_list
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing schedules")
            raise AdminError("Error in listing schedules" + str(e))
