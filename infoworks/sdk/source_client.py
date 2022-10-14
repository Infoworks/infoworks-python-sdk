import time

from infoworks.error import SourceError
from infoworks.sdk import url_builder, local_configurations
from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.local_configurations import Response, ErrorCode, SourceMappings
from infoworks.sdk.source_response import SourceResponse
from infoworks.sdk.utils import IWUtils


class SourceClient(BaseClient):

    def __init__(self):
        super(SourceClient, self).__init__()

    def poll_job(self, source_id=None, job_id=None, poll_timeout=local_configurations.POLLING_TIMEOUT,
                 polling_frequency=local_configurations.POLLING_FREQUENCY_IN_SEC,
                 retries=local_configurations.NUM_POLLING_RETRIES):
        """
                Polls Infoworks source job and returns its status
                :param source_id: Source Identifier
                :type source_id: String
                :param job_id: Job Identifier
                :type job_id: String
                :param poll_timeout: Polling timeout(default : 1200 seconds)
                :type poll_timeout: Integer
                :param polling_frequency: Polling Frequency(default : 15 seconds)
                :type polling_frequency: Integer
                :param retries: Number of Retries (default : 3)
                :type retries: Integer
                :return: response Object with Job status
        """
        failed_count = 0
        response = {}
        timeout = time.time() + poll_timeout
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
                    self.logger.info(
                        "Job poll status : " + result["status"] + "Job completion percentage: " + str(result.get(
                            "percentage", 0)))
                    if job_status in ["completed", "failed", "aborted"]:
                        if job_status == "completed":
                            return SourceResponse.parse_result(status=Response.Status.SUCCESS, source_id=source_id,
                                                               job_id=job_id)
                        else:
                            return SourceResponse.parse_result(status=job_status, source_id=source_id,
                                                               job_id=job_id)
                else:
                    self.logger.error(f"Error occurred during job {job_id} status poll")
                    if failed_count >= retries - 1:
                        return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                           error_code=ErrorCode.GENERIC_ERROR,
                                                           error_desc=response, job_id=job_id,
                                                           source_id=source_id)
                    failed_count = failed_count + 1
            except Exception as e:
                self.logger.exception("Error occurred during job status poll")
                self.logger.info(str(e))
                if failed_count >= retries - 1:
                    # traceback.print_stack()
                    print(response)
                    raise SourceError(response.get("message", "Error occurred during job status poll"))
                failed_count = failed_count + 1
            time.sleep(polling_frequency)

    def create_source(self, source_config=None):
        """
        Create a new Infoworks source
        :param source_config: a JSON object containing source configurations
        :type source_config: JSON Object

        source_config_example = {
            "name": "source_name",
            "type": "rdbms",
            "sub_type": "subtype",
            "data_lake_path": "datalake_path",
            "environment_id": "environment_id",
            "storage_id": "storage_id",
            "is_source_ingested": True
        }
        :return: response dict
        """
        response = None
        try:
            if source_config is None:
                self.logger.error('Invalid source configuration. Cannot create a new source.')
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_desc='Invalid source configuration. '
                                                              'Cannot create a new source.')
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", url_builder.create_source_url(self.client_config),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              source_config).content)

            result = response.get('result', {})
            source_id = result.get('id', None)

            if source_id is None:
                self.logger.error('Source failed to create.')
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_desc=response)

            source_id = str(source_id)
            self.logger.info('Source {id} has been created.'.format(id=source_id))
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, source_id=source_id)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to create a new source.')
            raise SourceError(response.get("message", "Error occurred while trying to create a new source."))

    def configure_source_connection(self, source_id, connection_object, read_passwords_from_secrets=False):
        """
        Function to configure the source connection
        :param read_passwords_from_secrets: True/False. If True then the passwords are read from the secret manager info provided
        :param source_id: source identifier entity id
        :type source_id: String
        :param connection_object: The json dict containing the source connection details
        :type connection_object: Json object

        "connection_obj_rdbms": {
                "enable_log_based_cdc": false,
                "driver_name": "oracle.jdbc.driver.OracleDriver",
                "driver_version": "v2",
                "connection_url": "jdbc:oracle:thin:@52.73.246.109:1521:xe",
                "username": "automation_db",
                "password": "eEBcRuPkw0zh9oIPvKnal+1BNKmFH5TfdI1ieDinruUv47Z5+f/oPjb+uyqUmfcQusM2DjoHc3OM", # can be iwx encrypted password or plain text password
                "connection_mode": "JDBC",
                "database": "ORACLE"
                }

        "connection_obj_csv" = {
                "source_base_path_relative": "",
                "source_base_path": "",
                "storage": {
                    "storage_type": "",
                    "cloud_type": "",
                    "access_id": "",
                    "secret_key": "",
                    "account_type": "",
                    "access_type": ""
                }
            }
        :return: response dict
        """

        if read_passwords_from_secrets:
            src_name = self.get_sourcename_from_id(source_id)
            if src_name is not None:
                encrypted_key_name = "iwxsrc-" + src_name + "-password"
                decrypt_pass = self.secrets_config.get(encrypted_key_name, "")
                connection_object['password'] = decrypt_pass if decrypt_pass else connection_object.get('password', "")
        try:
            source_connection_configuration_url = url_builder.get_source_connection_details_url(self.client_config,
                                                                                                source_id)
            response = IWUtils.ejson_deserialize(self.call_api("PUT", source_connection_configuration_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               connection_object).content)
            result = response.get('result', False)
            if not result:
                self.logger.error(f"Failed to configure the source connection for {source_id} ")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=response, job_id=None, source_id=None)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS)
        except Exception as e:
            raise SourceError(f"Failed to configure the source connection for {source_id} " + str(e))

    def source_test_connection_job_poll(self, source_id, poll_timeout=300, polling_frequency=15, retries=3):
        """
        Function to do test connection
        :param source_id: source identifier entity id
        :type source_id: String
        :param poll_timeout: test connection job timeout in seconds
        :type poll_timeout: integer. Default 300 seconds
        :param polling_frequency: polling frequency for the job in seconds. Default 15 seconds
        :type polling_frequency: integer
        :param retries: Number of retries during job poll. Default is 3
        :type retries: integer
        :return: response dict
        """
        url_for_test_connection = url_builder.get_test_connection_url(self.client_config, source_id)
        test_connection_dict = {
            "job_type": "source_test_connection"
        }
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", url_for_test_connection,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              test_connection_dict).content)
            result = response.get('result', {})
        except Exception as e:
            raise SourceError(f"Failed to create test connection job for {source_id} " + str(e))
        if len(result) == 0 or "id" not in result:
            self.logger.error(f"Failed to create test connection job")
            return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.GENERIC_ERROR,
                                               error_desc=response, job_id=None, source_id=source_id)
        else:
            job_id = result["id"]
            self.logger.info(response.get("message", ""))
            return self.poll_job(source_id=source_id, job_id=job_id, poll_timeout=poll_timeout,
                                 polling_frequency=polling_frequency,
                                 retries=retries)

    def source_metacrawl_job_poll(self, source_id, poll_timeout=300, polling_frequency=15, retries=3):
        """
        Function to do meta crawl connection
        :param source_id: source identifier entity id
        :type source_id: String
        :param poll_timeout: meta crawl job timeout in seconds
        :type poll_timeout: integer. Default 300 seconds
        :param polling_frequency: polling frequency for the job in seconds. Default 15 seconds
        :type polling_frequency: integer
        :param retries: Number of retries during job poll. Default is 3
        :type retries: integer
        :return: response dict
        """
        url_for_metacrawl = url_builder.get_test_connection_url(self.client_config, source_id)
        test_connection_dict = {
            "job_type": "source_fetch_metadata"
        }
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", url_for_metacrawl,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              test_connection_dict).content)
            result = response.get('result', {})
        except Exception as e:
            raise SourceError(f"Failed to create meta crawl job for {source_id} " + str(e))
        if len(result) == 0 or "id" not in result:
            self.logger.error(f"Failed to create meta crawl job")
            return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.GENERIC_ERROR,
                                               error_desc=response, job_id=None, source_id=source_id)
        else:
            job_id = result["id"]
            self.logger.info(response.get("message", ""))
            return self.poll_job(source_id=source_id, job_id=job_id, poll_timeout=poll_timeout,
                                 polling_frequency=polling_frequency,
                                 retries=retries)

    def browse_source_tables(self, source_id, filter_tables_properties=None, poll_timeout=300, polling_frequency=15,
                             retries=3, poll=True):
        """
        Function to browse the source based on the filter_tables_properties passed
        :param source_id: source identifier entity id
        :type source_id: String
        :param filter_tables_properties:
        :type filter_tables_properties: json object

        filter_tables_properties = {
            "schemas_filter" : "%dbo",
            "catalogs_filter" : "%",
            "tables_filter" : "%csv_incremental_test",
            "is_data_sync_with_filter" : true,
            "is_filter_enabled" : true
        }

        :param poll_timeout: test connection job timeout in seconds
        :type poll_timeout: integer. Default 300 seconds
        :param polling_frequency: polling frequency for the job in seconds. Default 15 seconds
        :type polling_frequency: integer
        :param retries: Number of retries during job poll. Default is 3
        :type retries: integer
        :param poll: Poll job until its completion
        :type poll: Boolean
        :return: response dict
        """
        try:
            url_for_browse_source = url_builder.browse_source_tables_url(self.client_config, source_id)
            if filter_tables_properties is not None:
                filter_condition = f"?is_filter_enabled=true&tables_filter={filter_tables_properties['tables_filter']}&catalogs_filter={filter_tables_properties['catalogs_filter']}&schemas_filter={filter_tables_properties['schemas_filter']}"
                url_for_browse_source = url_for_browse_source + filter_condition
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_for_browse_source,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            result = response.get('result', {})
        except Exception as e:
            raise SourceError(f"Failed to create browse table job for {source_id} " + str(e))
        if len(result) == 0 and "id" not in result:
            self.logger.error(f"Failed to create browse table job")
            return SourceResponse.parse_result(status=Response.Status.FAILED,
                                               error_code=ErrorCode.GENERIC_ERROR,
                                               error_desc=response, job_id=None,
                                               source_id=source_id)
        else:
            job_id = result.get("id")
            self.logger.info(response.get("message", ""))
            if not poll:
                self.logger.info(f"Browse table job for source {source_id} was submitted.")
                return SourceResponse.parse_result(status=Response.Status.SUCCESS)
            job_status = "running"
            failed_count = 0
            timeout = time.time() + poll_timeout
            while True:
                if time.time() > timeout:
                    break
                try:
                    self.logger.info(f"Failed poll job status count: {failed_count}")
                    url_for_interactive_job_poll = url_builder.interactive_job_poll_url(self.client_config, source_id,
                                                                                        job_id)
                    self.logger.info('url to poll interactive job - ' + url_for_interactive_job_poll)
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", url_for_interactive_job_poll,
                                      IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
                    result = response.get('result', {})
                    if len(result) == 0:
                        self.logger.error(f"Failed to poll interactive job {job_id}")
                        job_status = None
                    else:
                        job_status = result["status"]
                    self.logger.info("Browse source job poll status : " + job_status)
                    if job_status in ["completed", "failed", "aborted"]:
                        break
                    if job_status is None:
                        self.logger.info(f"Error occurred during job {job_id} status poll")
                        if failed_count >= retries - 1:
                            return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                               error_code=ErrorCode.GENERIC_ERROR,
                                                               error_desc=response, job_id=job_id,
                                                               source_id=source_id)
                        failed_count = failed_count + 1
                except Exception as e:
                    self.logger.info("Error occurred during job status poll")
                    if failed_count >= retries - 1:
                        raise SourceError(f"Error occurred during job status poll {source_id} " + str(e))
                    failed_count = failed_count + 1
                time.sleep(polling_frequency)
            if job_status == "completed":
                self.logger.info(f"Browse table job for source {source_id} was successful")
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, job_id=job_id)
            else:
                self.logger.error(f"Browse table job for source {source_id} failed with {job_status}")
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=response, job_id=job_id,
                                                   source_id=source_id)

    def add_tables_to_source(self, source_id, tables_to_add_config, poll_timeout=300, polling_frequency=15,
                             retries=3, poll=True):
        """
        Function to add tables to source
        :param source_id: source identifier entity id
        :type source_id: String
        :param tables_to_add_config: Array of JSON configuration object
        :type tables_to_add_config: List

        tables_to_add_config = [{
                "table_name": "",
                "schema_name": "",
                "table_type": "TABLE",
                "target_table_name": "",
                "target_schema_name": ""
        }]

        :param poll_timeout: test connection job timeout in seconds
        :type poll_timeout: integer. Default 300 seconds
        :param polling_frequency: polling frequency for the job in seconds. Default 15 seconds
        :type polling_frequency: integer
        :param retries: Number of retries during job poll. Default is 3
        :type retries: integer
        :param poll: Poll job until its completion
        :type poll: Boolean
        :return: response dict
        """
        try:
            url_for_add_tables_to_source = url_builder.add_tables_to_source_url(self.client_config, source_id)
            add_tables_dict = {"tables_to_add": tables_to_add_config}
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", url_for_add_tables_to_source,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              add_tables_dict).content)
            result = response.get('result', {})
            self.logger.debug(response)
            if len(result) != 0:
                self.logger.info(f"Added the below table Ids to the source {source_id}")
                self.logger.info(result["added_tables"])
                self.logger.info(response["message"])
                self.logger.info(f"Triggered metacrawl job for tables. Infoworks JobID {result['job_created']}")
                job_id = result['job_created']
                if not poll:
                    self.logger.info(f"Tables added to source {source_id} and metacrawl job was submitted {job_id}")
                    return SourceResponse.parse_result(status=Response.Status.SUCCESS)
                else:
                    return self.poll_job(source_id=source_id, job_id=job_id, poll_timeout=poll_timeout,
                                         polling_frequency=polling_frequency,
                                         retries=retries)
            else:
                self.logger.error(f"Failed to add the tables to the source {source_id}")
                self.logger.debug(response)
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=response, job_id=None,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to add the tables to the source {source_id} " + str(e))

    def configure_tables_and_tablegroups(self, source_id, configuration_obj):
        """
        Function to configure tables and table-groups.
        The configuration_obj should be similar to the json object that is the output of GET source configuration API
        :param source_id: source identifier entity id
        :type source_id: String
        :param configuration_obj: Configurations for the tables and tablegroups
        :type configuration_obj: JSON object
        :return:  response dict
        """
        try:
            configure_tables_tg_url = url_builder.configure_tables_and_tablegroups_url(self.client_config, source_id)
            errors = {}
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", configure_tables_tg_url,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              {"configuration": configuration_obj}).content)
            result = response.get('result', {})
            count = 0
            # self.logger.debug(response)
            if len(result) != 0:
                for config_item in result:
                    table_upsert_status = config_item.get('table_upsert_status', None)
                    if table_upsert_status is not None and len(table_upsert_status.get("error", [])) != 0:
                        self.logger.info(f"{table_upsert_status}")
                        count = count + 1
                        errors[count] = table_upsert_status["error"]
                        self.logger.error("Found errors during table and table group configurations")
                        self.logger.error(table_upsert_status.get("error", []))
                    elif table_upsert_status is None:
                        self.logger.info(f"Could not get table insert status from {config_item}")
                    else:
                        pass
                    self.logger.debug(str(config_item))
                if len(errors) != 0:
                    self.logger.error(f"Failed due to multiple errors\n{errors}")
                    return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                       error_code=ErrorCode.GENERIC_ERROR,
                                                       error_desc=f"Failed to configure tables and table groups {response} ",
                                                       job_id=None,
                                                       source_id=source_id)
                else:
                    self.logger.info(f"Successfully configured tables and table groups for the source")
                    return SourceResponse.parse_result(status=Response.Status.SUCCESS)
            else:
                self.logger.error("Failed to configure tables and table groups")
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=f"Failed to configure tables and table groups {response} ",
                                                   job_id=None,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to configure tables and table groups" + str(e))

    def create_table_group(self, source_id, table_group_obj):
        """
        Function to create table group
        :param source_id: entity identifier for source
        :param table_group_obj:

        table_group_obj = {
         "environment_compute_template": {"environment_compute_template_id": "536592c8ceb69bbbe730d452"},
         "name": "tg_name",
         "max_connections": 1,
         "max_parallel_entities": 1,
         "tables": [
          {"table_id":"1123","connection_quota":100}
         ]
        }

        :return:  response dict
        """
        try:
            create_tg_url = url_builder.create_table_group_url(self.client_config, source_id)
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", create_tg_url,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              table_group_obj).content)
            if len(response.get("result", {})) > 0:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS,
                                                   response=response.get("result")["id"])
            else:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=response, job_id=None,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to create table group" + str(e))

    def update_table_group(self, source_id, table_group_id, table_group_obj):
        """
        Function to create table group
        :param source_id: entity identifier for source
        :param table_group_id: entity identifier for tablegroup
        :param table_group_obj:

        table_group_obj = {
         "environment_compute_template": {"environment_compute_template_id": "536592c8ceb69bbbe730d452"},
         "name": "tg_name",
         "max_connections": 1,
         "max_parallel_entities": 1,
         "tables": [
          {"table_id":"1123","connection_quota":100}
         ]
        }
        :return: response dict
        """
        try:
            create_tg_url = url_builder.create_table_group_url(self.client_config, source_id) + f"/{table_group_id}"
            response = IWUtils.ejson_deserialize(
                self.call_api("PUT", create_tg_url,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              table_group_obj).content)
            if len(response.get("result", {})) > 0:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS,
                                                   response=response.get("result")["id"])
            else:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=response, job_id=None,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to update table group" + str(e))

    def get_list_of_table_groups(self, source_id, params=None, tg_id=None):
        """
        Function to list the table groups under source
        :param source_id: entity identifier for source
        :param tg_id: id of table group config to fetch
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}

        if tg_id is None:
            url_to_list_tg = url_builder.create_table_group_url(
                self.client_config, source_id) + IWUtils.get_query_params_string_from_dict(params=params)
        else:
            url_to_list_tg = url_builder.create_table_group_url(
                self.client_config, source_id) + f"/{tg_id}"
        tg_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_tg,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                if tg_id is not None:
                    tg_list.extend([result])
                else:
                    while len(result) > 0:
                        tg_list.extend(result)
                        nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                        response = IWUtils.ejson_deserialize(
                            self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                                self.client_config['bearer_token'])).content)
                        result = response.get("result", [])
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=tg_list)
        except Exception as e:
            self.logger.error("Error in listing table groups")
            raise SourceError("Error in listing table groups" + str(e))

    def delete_table_group(self, source_id, tg_id):
        """
        Function to delete table group under the source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param tg_id: id of table group config to delete
        :type tg_id: String
        :return: response dict
        """
        url_to_delete_tg = url_builder.create_table_group_url(self.client_config, source_id).strip("/") + f"/{tg_id}"
        try:
            response = self.call_api("DELETE",
                                     url_to_delete_tg,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(
                response.content
            )
            if response.status_code == 200:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS)
            else:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=parsed_response.get("details",
                                                                                  "Error in deleting table group")
                                                   )
        except Exception as e:
            self.logger.error("Error in deleting the table group")
            raise SourceError("Error in deleting table group" + str(e))

    def submit_source_job(self, source_id, body, poll=False, poll_timeout=300, polling_frequency=15,
                          retries=3):
        """
        Function to trigger the jobs related to source
        :param source_id: source entity id
        :param body:

        metadata_job_body_example = {
                "job_type": "source_fetch_metadata",
                "overwrite": true
            }

        ingest_table_body_example = {
            "job_type": "cdc_merge" or "truncate_reload",
            "job_name": "testing",
            "interactive_cluster_id": "536592c8ceb69bbbe730d452",
            "table_ids": [
                     "614618debf711275204a1b1f"
                ]
        }

        ingest_table_group_body_example = {
            "job_type": "truncate_reload",
            "table_group_id": "efb9d3e810c643b9930e1a00"
        }
        :param poll_timeout: test connection job timeout in seconds
        :type poll_timeout: integer. Default 300 seconds
        :param polling_frequency: polling frequency for the job in seconds. Default 15 seconds
        :type polling_frequency: integer
        :param retries: Number of retries during job poll. Default is 3
        :type retries: integer
        :param poll: Poll job until its completion
        :type poll: Boolean
        :return:  response dict
        """
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", url_builder.submit_source_job(self.client_config, source_id),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              body).content)
            result = response.get('result', {})
            if len(result) != 0:
                job_id = result["id"]
                if not poll:
                    self.logger.info(f"Job successfully submitted for {source_id}. JobID to track is: {job_id}")
                    return SourceResponse.parse_result(status=Response.Status.SUCCESS, job_id=job_id)
                else:
                    return self.poll_job(source_id=source_id, job_id=job_id, poll_timeout=poll_timeout,
                                         polling_frequency=polling_frequency,
                                         retries=retries)
        except Exception as e:
            raise SourceError(f"Failed to create source job: " + str(e))

    def resubmit_source_job(self, job_id, poll=False, poll_timeout=300, polling_frequency=15, retries=3):
        """
        Function to resubmit the jobs
        :param job_id: infoworks ob id of the failed job
        :type job_id: String
        :param poll_timeout: test connection job timeout in seconds
        :type poll_timeout: integer. Default 300 seconds
        :param polling_frequency: polling frequency for the job in seconds. Default 15 seconds
        :type polling_frequency: integer
        :param retries: Number of retries during job poll. Default is 3
        :type retries: integer
        :param poll: Poll job until its completion
        :type poll: Boolean
        :return:  response dict
        """
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", url_builder.resubmit_job_url(self.client_config, job_id),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            result = response.get('result', {})
            if len(result) != 0:
                job_id = result["id"]
                if not poll:
                    self.logger.info(f"Job resubmitted successfully. JobID to track is: {job_id}")
                    return SourceResponse.parse_result(status=Response.Status.SUCCESS, job_id=job_id)
                else:
                    return self.poll_job(source_id=None, job_id=job_id, poll_timeout=poll_timeout,
                                         polling_frequency=polling_frequency,
                                         retries=retries)
        except Exception as e:
            raise SourceError(f"Failed to create source job: " + str(e))

    def get_list_of_sources(self, params=None):
        """
        Function to list the sources
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_sources = url_builder.list_sources_url(
            self.client_config) + IWUtils.get_query_params_string_from_dict(params=params)
        source_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_sources,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                while len(result) > 0:
                    source_list.extend([result])
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", [])
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=source_list)
        except Exception as e:
            self.logger.error("Error in listing sources")
            raise SourceError("Error in listing sources " + str(e))

    def update_source(self, source_id, update_body):
        """
        Function to update the source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param update_body: JSON body with key-value pair to update
        :type update_body: JSON dict
        :return: response dict
        """
        try:
            response = self.call_api("PATCH", url_builder.source_info(self.client_config, source_id),
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=update_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content
            )
            if response.status_code == 200:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS)
            else:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=parsed_response.get("details", "Error in updating source")
                                                   )
        except Exception as e:
            self.logger.error("Error in updating the source")
            raise SourceError("Error in updating the source" + str(e))

    def get_source_connection_details(self, source_id):
        """
        Function to get source connection details like jdbc url, source type etc
        :param source_id: Entity identifier for source
        :type source_id: String
        :return: response dict
        """
        try:
            source_connection_configuration_url = url_builder.get_source_connection_details_url(self.client_config,
                                                                                                source_id)
            response = IWUtils.ejson_deserialize(self.call_api("GET", source_connection_configuration_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get('result', False)
            if not result:
                self.logger.error(f"Failed to get the source connection for {source_id} ")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=response, job_id=None, source_id=None)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=result)
        except Exception as e:
            raise SourceError(f"Failed to get the source connection for {source_id} " + str(e))

    def add_source_advanced_configuration(self, source_id, config_body):
        """
        Function to add advanced configuration to the source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param config_body: JSON body with key-value advanced config
        :type config_body: JSON dict
        :return: response dict
        """
        try:
            response = self.call_api("POST", url_builder.get_advanced_config_url(self.client_config, source_id),
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content
            )
            if response.status_code == 200:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=parsed_response.get("details",
                                                                                  "Error in adding source advanced configs")
                                                   )
        except Exception as e:
            self.logger.error("Error in adding the source advanced config")
            raise SourceError("Error in adding the source advanced config" + str(e))

    def update_source_advanced_configuration(self, source_id, key, config_body):
        """
        Function to update advanced configuration of the source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param key: Name of advanced config to update
        :type key: String
        :param config_body: JSON body with key-value advanced config
        :type config_body: JSON dict
        :return: response dict
        """
        try:
            response = self.call_api("PUT",
                                     url_builder.get_advanced_config_url(self.client_config, source_id).strip(
                                         "/") + f"/{key}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content
            )
            if response.status_code == 200:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS)
            else:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=parsed_response.get("details",
                                                                                  "Error in updating source advanced configs")
                                                   )
        except Exception as e:
            self.logger.error("Error in updating the source advanced configs")
            raise SourceError("Error in updating the source advanced config" + str(e))

    def get_advanced_configuration_of_sources(self, source_id, params=None, key=None):
        """
        Function to list the advanced configuration of source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param key: Name of advanced config to get
        :type key: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}
        if key is None:
            url_to_list_adv_config = url_builder.get_advanced_config_url(
                self.client_config, source_id) + IWUtils.get_query_params_string_from_dict(params=params)
        else:
            url_to_list_adv_config = url_builder.get_advanced_config_url(
                self.client_config, source_id).strip("/") + f"/{key}"
        adv_config_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_adv_config,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                if key is not None:
                    adv_config_list.extend([result])
                else:
                    while len(result) > 0:
                        adv_config_list.extend(result)
                        nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                        response = IWUtils.ejson_deserialize(
                            self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                                self.client_config['bearer_token'])).content)
                        result = response.get("result", [])
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=adv_config_list)
        except Exception as e:
            self.logger.error("Error in listing advanced configurations")
            raise SourceError("Error in listing advanced configurations" + str(e))

    def delete_source_advanced_configuration(self, source_id, key):
        """
        Function to delete advanced configuration of the source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param key: Name of advanced config to delete
        :type key: String
        :return: response dict
        """
        try:
            response = self.call_api("DELETE",
                                     url_builder.get_advanced_config_url(self.client_config, source_id).strip(
                                         "/") + f"/{key}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(
                response.content
            )
            if response.status_code == 200:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS)
            else:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=parsed_response.get("details",
                                                                                  "Error in deleting source advanced config")
                                                   )
        except Exception as e:
            self.logger.error("Error in deleting the source advanced config")
            raise SourceError("Error in deleting advanced configurations" + str(e))

    def get_source_configurations_json_export(self, source_id):
        """
        Function to get source configurations
        :param source_id: Entity identifier for source
        :type source_id: String
        :return: response dict
        """
        try:
            source_configuration_url = url_builder.configure_source_url(self.client_config, source_id)
            response = IWUtils.ejson_deserialize(self.call_api("GET", source_configuration_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get('result', False)
            if not result:
                self.logger.error(f"Failed to get the source configurations for {source_id} ")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=response, job_id=None, source_id=None)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=result)
        except Exception as e:
            raise SourceError(f"Failed to get the source configurations for {source_id} " + str(e))

    def post_source_configurations_json_import(self, source_id, config_body):
        """
        Function to configure the source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param config_body: JSON body
        :type config_body: JSON dict
        :return: response dict
        """
        source_configuration_url = url_builder.configure_source_url(self.client_config, source_id)
        try:
            response = self.call_api("POST",
                                     source_configuration_url,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=config_body)
            parsed_response = IWUtils.ejson_deserialize(
                self.call_api("POST",
                              source_configuration_url,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              data=config_body).content
            )
            if response.status_code == 200:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS)
            else:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=parsed_response.get("details",
                                                                                  "Error in updating source configs")
                                                   )
        except Exception as e:
            self.logger.error("Error in updating the source configs")
            raise SourceError("Error in updating the source configs" + str(e))

    def list_tables_in_source(self, source_id, params=None):
        """
        Function to list the tables part of the source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_tables = url_builder.list_tables_under_source(
            self.client_config, source_id) + IWUtils.get_query_params_string_from_dict(params=params)

        tables_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_tables,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                while len(result) > 0:
                    tables_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", [])
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=tables_list)
        except Exception as e:
            self.logger.error("Error in listing tables under source")
            raise SourceError("Error in listing tables under source" + str(e))

    def get_table_columns_details(self, source_id, table_name, schema_name, database_name):
        """
        Function to get the table column details
        :param source_id: Entity identifier for source
        :type source_id: String
        :param table_name: Snowflake Table Name
        :type table_name: String
        :param schema_name: Snowflake Schema Name
        :type schema_name: String
        :param database_name: Snowflake Database Name
        :type database_name: String
        :return: response dict
        """
        url_to_list_tables = url_builder.list_tables_under_source(self.client_config, source_id)
        filter_cond = "?filter={\"table\":\"" + table_name + "\",\"catalog_name\":\"" + database_name + "\",\"schemaNameAtSource\": \"" + schema_name + "\"} "
        get_source_table_info_url = url_to_list_tables + filter_cond
        try:
            response = self.call_api("GET", get_source_table_info_url,
                                     headers=IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                                     )
            if response.status_code != 200:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=f"Failed to get metasync table information for {table_name}")
            else:
                if IWUtils.ejson_deserialize(response.content).get('message') == "No Tables found for given source":
                    return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                       error_code=ErrorCode.GENERIC_ERROR,
                                                       error_desc=f"No Tables found for given source with name {table_name}")

                result = IWUtils.ejson_deserialize(response.content).get('result', [])
                if len(result) != 0:
                    lookup_columns = result[0]["columns"]
                    lookup_columns_dict = {}
                    for column in lookup_columns:
                        lookup_columns_dict[column["name"]] = {}
                        lookup_columns_dict[column["name"]]["target_sql_type"] = column["target_sql_type"]
                        lookup_columns_dict[column["name"]]["target_precision"] = column.get("target_precision", "")
                        lookup_columns_dict[column["name"]]["target_scale"] = column.get("target_scale", "")
                        lookup_columns_dict[column["name"]]["col_size"] = column.get("col_size", "")
                    return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=lookup_columns_dict)
        except Exception as e:
            self.logger.error(f"Error in getting column details for table {table_name}")
            raise SourceError(f"Error in getting column details for table {table_name}" + str(e))

    def get_table_configurations(self, source_id, table_id, ingestion_config_only=False):
        """
        Function to get table configurations
        :param table_id: Entity identifier for table
        :type table_id: String
        :param source_id: Entity identifier for source
        :type source_id: String
        :param ingestion_config_only: True/False. Config to retrieve only ingestion configurations
        :type ingestion_config_only: Boolean
        :return: response dict
        """
        try:
            table_configurations_url = url_builder.get_table_configuration(self.client_config, source_id, table_id)
            if ingestion_config_only:
                table_configurations_url = table_configurations_url + "/configurations/ingestion"
            response = IWUtils.ejson_deserialize(self.call_api("GET", table_configurations_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get('result', False)
            if not result:
                self.logger.error(f"Failed to get the table configurations for {table_id} ")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=response, job_id=None, source_id=None)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=result)
        except Exception as e:
            raise SourceError(f"Failed to get the table configurations for {table_id} " + str(e))

    def update_table_configuration(self, source_id, table_id, config_body):
        """
        Function to update table configuration
        :param source_id: Entity identifier for source
        :type source_id: String
        :param table_id: Entity identifier for table
        :type table_id: String
        :param config_body: JSON body with key-value configs
        :type config_body: JSON dict
        :return: response dict
        """
        try:
            response = self.call_api("PUT",
                                     url_builder.get_table_configuration(self.client_config, source_id, table_id),
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content
            )
            if response.status_code == 200:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS)
            else:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=parsed_response.get("details",
                                                                                  "Error in updating table configuration")
                                                   )
        except Exception as e:
            self.logger.error("Error in updating the table configuration")
            raise SourceError(f"Error in updating the table configuration for {table_id} " + str(e))

    def add_table_advanced_configuration(self, source_id, table_id, config_body):
        """
        Function to add advanced configuration to the source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param table_id: Entity identifier for table
        :type table_id: String
        :param config_body: JSON body with key-value advanced config
        :type config_body: JSON dict
        :return: response dict
        """
        try:
            response = self.call_api("POST",
                                     url_builder.get_advanced_config_url(self.client_config, source_id, table_id),
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content
            )
            if response.status_code == 200:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS)
            else:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=parsed_response.get("details",
                                                                                  "Error in adding table advanced config")
                                                   )
        except Exception as e:
            self.logger.error("Error in adding table level advanced configuration")
            raise SourceError(f"Error in adding table level advanced configuration for {table_id} " + str(e))

    def update_table_advanced_configuration(self, source_id, table_id, key, config_body):
        """
        Function to update advanced configuration of the source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param table_id: Entity identifier for table
        :type table_id: String
        :param key: Name of advanced config to update
        :type key: String
        :param config_body: JSON body with key-value advanced config
        :type config_body: JSON dict
        :return: response dict
        """
        try:
            response = self.call_api("PUT",
                                     url_builder.get_advanced_config_url(self.client_config, source_id, table_id).strip(
                                         "/") + f"/{key}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content
            )
            if response.status_code == 200:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS)
            else:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=parsed_response.get("details",
                                                                                  "Error in updating table advanced config")
                                                   )
        except Exception as e:
            self.logger.error("Error in updating the table advanced configs")
            raise SourceError(f"Error in updating table level advanced configuration for {table_id} " + str(e))

    def delete_table_advanced_configuration(self, source_id, table_id, key):
        """
        Function to delete advanced configuration of the table
        :param source_id: Entity identifier for source
        :type source_id: String
        :param table_id: Entity identifier for table
        :type table_id: String
        :param key: Name of advanced config to delete
        :type key: String
        :return: response dict
        """
        try:
            response = self.call_api("DELETE",
                                     url_builder.get_advanced_config_url(self.client_config, source_id, table_id).strip(
                                         "/") + f"/{key}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(
                response.content
            )
            if response.status_code == 200:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS)
            else:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=parsed_response.get("details",
                                                                                  "Error in deleting table advanced config")
                                                   )
        except Exception as e:
            self.logger.error("Error in deleting the table advanced config")
            raise SourceError(f"Error in deleting table level advanced configuration for {table_id} " + str(e))

    def get_table_export_configurations(self, source_id, table_id, connection_only=False):
        """
        Function to get table export configurations
        :param connection_only: Get export configuration connection details only
        :param table_id: Entity identifier for table
        :type table_id: String
        :param source_id: Entity identifier for source
        :type source_id: String
        :return: response dict
        """
        try:
            if connection_only:
                table_export_configurations_url = url_builder.table_export_config_url(self.client_config, source_id,
                                                                                      table_id) + "/connection"
            else:
                table_export_configurations_url = url_builder.table_export_config_url(self.client_config, source_id,
                                                                                      table_id)
            response = IWUtils.ejson_deserialize(self.call_api("GET", table_export_configurations_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get('result', False)
            if not result:
                self.logger.error(f"Failed to get the table export configurations for {table_id} ")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=response, job_id=None, source_id=None)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=result)
        except Exception as e:
            raise SourceError(f"Failed to get the table export configurations for {table_id} " + str(e))

    def update_table_export_configuration(self, source_id, table_id, config_body, connection_only=False):
        """
        Function to update export configuration of the table
        :param source_id: Entity identifier for source
        :type source_id: String
        :param table_id: Entity identifier for table
        :type table_id: String
        :param config_body: JSON body with key-value pairs
        :type config_body: JSON dict
        :param connection_only: Get export configuration connection details only
        :return: response dict
        """
        if connection_only:
            table_export_configurations_url = url_builder.table_export_config_url(self.client_config, source_id,
                                                                                  table_id) + "/connection"
        else:
            table_export_configurations_url = url_builder.table_export_config_url(self.client_config, source_id,
                                                                                  table_id)
        try:
            response = self.call_api("PUT",
                                     table_export_configurations_url,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content
            )
            if response.status_code == 200:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS)
            else:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=parsed_response.get("details",
                                                                                  "Error in updating export configuration of the table")
                                                   )
        except Exception as e:
            self.logger.error("Error in updating the export configuration of the table")
            raise SourceError(f"Error in updating the export configuration of the table for {table_id} " + str(e))

    def get_table_ingestion_metrics(self, source_id, table_id):
        """
        Function to fetch ingestion metrics of source tables
        :param source_id: Entity identifier for source
        :type source_id: String
        :param table_id: table entity id
        :type table_id: String
        :return: response dict
        """
        if table_id is None:
            url_to_get_ing_metrics = url_builder.get_ingestion_metrics_source_url(self.client_config, source_id)
        else:
            url_to_get_ing_metrics = url_builder.get_ingestion_metrics_table_url(self.client_config, source_id,
                                                                                 table_id)
        metric_results = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_ing_metrics,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                while len(result) > 0:
                    metric_results.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", [])
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=metric_results)
        except Exception as e:
            self.logger.error("Error in fetching ingestion metrics for source")
            raise SourceError(f"Error in fetching ingestion metrics for source for {source_id} " + str(e))

    def add_table_and_file_mappings_for_csv(self, source_id, file_mappings_config):
        """
        Function to Create table and Add a new file mapping (table) in the csv source
        :param source_id: Entity identifier for source
        :type source_id: String
        :file_mappings_config : Configurations for file_mappings
        :type file_mappings_config: JSON object

        file_mappings_config example = {
                "configuration": {
                    "source_file_properties": {
                        "column_enclosed_by": "\"",
                        "column_separator": ",",
                        "encoding": "UTF-8",
                        "escape_character": "\\",
                        "header_rows_count": 1
                    },
                    "target_relative_path": "/table_path",
                    "deltaLake_table_name": "table_name",
                    "source_file_type": "csv",
                    "ingest_subdirectories": true,
                    "source_relative_path": "",
                    "exclude_filename_regex": "",
                    "include_filename_regex": "",
                    "is_archive_enabled": false,
                    "target_schema_name": "target_schema_name",
                    "target_table_name": "target_table_name"
                },
                "source": "src_id",
                "name": "table_name"
            }
        :return: response dict
        """
        try:
            add_filemappings_url = url_builder.list_tables_under_source(self.client_config, source_id)
            response = IWUtils.ejson_deserialize(self.call_api("POST", add_filemappings_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               file_mappings_config).content)
            result = response.get('result', False)
            if not result:
                self.logger.error(f"Failed to add table and configure file mappings ")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=response, job_id=None, source_id=None)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=result)
        except Exception as e:
            raise SourceError(f"Failed to add table and configure file mappings " + str(e))

    def get_sourceid_from_name(self, source_name):
        """
        Function to return source id from name
        :param source_name: Source name
        :return: Source Identifier
        """
        params = {"filter": {"name": source_name}}
        url_to_list_sources = url_builder.list_sources_url(
            self.client_config) + IWUtils.get_query_params_string_from_dict(params=params)
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_sources,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                if len(result) > 0:
                    return result[0]["id"]
        except Exception as e:
            self.logger.error("Error in listing sources")
            raise SourceError("Error in listing sources " + str(e))

    def get_sourcename_from_id(self, src_id):
        """
        Function to return source name from Id
        :param src_id: Source identifier
        :return: Source name
        """
        url_to_list_sources = url_builder.list_sources_url(
            self.client_config) + f"/{src_id}"
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_sources,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", {})
                return result["name"]
        except Exception as e:
            self.logger.error("Error in getting source name")
            raise SourceError("Error in getting source name " + str(e))

    def create_onboard_rdbms_source(self, src_name, src_type, data_lake_path, connection_url, username, password,
                                    environment_id=None, storage_id=None, schemas_filter=None, tables_filter=None):
        """
        Function to jumpstart creation and onboard of RDBMS source
        :param src_name: Name with which rdbms source has to be created. ["oracle", "mysql", "sqlserver", "teradata", "netezza"]
        :param src_type: Type of the source.
        :param data_lake_path: Path in which data has to be written in datalake
        :param connection_url: JDBC url
        :param username: username for authentication
        :param password: password for authentication. Can be ecrypted using Infoworks encryption algo
        :param environment_id: Entity id of the environment in which the source has to be created
        :param storage_id: Entity id of the source in which the data has to be written
        :param schemas_filter: Filter the schemas to browse
        :param tables_filter: Filter the tables to browse
        """
        if src_type.lower() not in ["oracle", "mysql", "sqlserver", "teradata", "netezza"]:
            print("Invalid source type. Exiting!!!")
            return None
        if src_type.lower() == "oracle":
            sub_type = SourceMappings.oracle.get("sub_type")
            driver_name = SourceMappings.oracle.get("driver_name")
            driver_version = SourceMappings.oracle.get("driver_version")
            database = SourceMappings.oracle.get("database")
        elif src_type.lower() == "sqlserver":
            pass
        elif src_type.lower() == "teradata":
            sub_type = SourceMappings.teradata.get("sub_type")
            driver_name = SourceMappings.teradata.get("driver_name")
            driver_version = SourceMappings.teradata.get("driver_version")
            database = SourceMappings.teradata.get("database")
        elif src_type.lower() == "mysql":
            pass
        else:
            sub_type = SourceMappings.oracle.get("sub_type")
            driver_name = SourceMappings.oracle.get("driver_name")
            driver_version = SourceMappings.oracle.get("driver_version")
            database = SourceMappings.oracle.get("database")
        src_create_response = self.create_source(source_config={
            "name": src_name,
            "type": "rdbms",
            "sub_type": sub_type,
            "data_lake_path": data_lake_path,
            "environment_id": environment_id if environment_id is not None else self.client_config[
                'default_environment_id'],
            "storage_id": storage_id if storage_id is not None else self.client_config['default_storage_id'],
            "is_source_ingested": True
        })
        if src_create_response["result"]["status"].upper() == "SUCCESS":
            src_id = src_create_response["result"]["source_id"]
            response = self.configure_source_connection(src_id, {
                "driver_name": driver_name,
                "driver_version": driver_version,
                "connection_url": connection_url,
                "username": username,
                "password": password,
                "connection_mode": "jdbc",
                "database": database
            })
            if response["result"]["status"].upper() == "SUCCESS":
                response = self.source_test_connection_job_poll(src_id)
                if response["result"]["status"].upper() == "SUCCESS" and response["result"]["job_id"] not in ["", None]:
                    response = self.browse_source_tables(src_id,
                                                         filter_tables_properties={
                                                             "schemas_filter": schemas_filter if schemas_filter is not None else "%",
                                                             "catalogs_filter": "%",
                                                             "tables_filter": tables_filter if tables_filter is not None else "%",
                                                             "is_data_sync_with_filter": True,
                                                             "is_filter_enabled": True
                                                         }, poll_timeout=300, polling_frequency=15, retries=1)
                    if response["result"]["status"].upper() == "SUCCESS" and response["result"]["job_id"] not in ["",
                                                                                                                  None]:
                        print(
                            "Source has been browsed successfully. Please proceed to add tables to source (add_tables_to_source) ")
                        return src_id

    def configure_table_ingestion_properties(self, source_id, table_id, natural_keys=None, sync_type="full-load",
                                             update_strategy=None,
                                             watermark_column=None, partition_key=None, derived_partition=False,
                                             derive_partition_format=None, split_by_key=None, storage_format=None):
        """
        Function to configure the ingestion configs of table
        :param source_id: Entity identifier for source
        :type source_id: String
        :param table_id: table entity id
        :type table_id: String
        :param natural_keys: natural keys List
        :type natural_keys: List
        :param sync_type: full-load/incremental
        :type sync_type: String
        :param update_strategy: append/merge
        :type update_strategy: String
        :param watermark_column: Watermark column to be used during incremental ingestion
        :type watermark_column: String
        :param partition_key: Partition by Column
        :type partition_key: String
        :param derived_partition: True/False. If True pass the derive_partition_format
        :type derived_partition: Boolean
        :param derive_partition_format: Derived partition format. Choose one from ['dd', 'MM', 'yyyy', 'yyyyMM', 'MMdd', 'yyyyMMdd']
        :type derive_partition_format: String
        :param split_by_key: Split by Column
        :type split_by_key: String
        :param storage_format: Storage format for table. orc, parquet, delta
        :type storage_format: String
        :return: response dict
        """
        if natural_keys is None:
            natural_keys = []
        try:
            table_config_body = {}
            if table_id is None:
                self.logger.error("Table id for this method cannot be None")
            if source_id is None:
                self.logger.error("Source id for this method cannot be None")
            if natural_keys != []:
                table_config_body['natural_keys'] = natural_keys
            if sync_type in ['full-load', 'incremental']:
                table_config_body["sync_type"] = "full-load"
                if update_strategy in ["append", "merge"] and watermark_column is not None:
                    if update_strategy == "append":
                        table_config_body["sync_type"] = "incremental"
                        table_config_body["update_strategy"] = "append"
                        table_config_body["watermark_column"] = watermark_column
                    elif update_strategy == "merge" and watermark_column is not None:
                        table_config_body["sync_type"] = "incremental"
                        table_config_body["update_strategy"] = "merge"
                        table_config_body["watermark_column"] = watermark_column
            if partition_key:
                if not derived_partition:
                    table_config_body["partition_keys"] = [{
                        "partition_column": partition_key,
                        "is_derived_partition": False}]
                else:
                    allowed_values_for_date_partition = ['dd', 'MM', 'yyyy', 'yyyyMM', 'MMdd', 'yyyyMMdd']
                    if derive_partition_format in allowed_values_for_date_partition:
                        derive_func_map = {'dd': "day num in month", 'MM': "month", 'yyyy': "year",
                                           'yyyyMM': "year month",
                                           'MMdd': "month day", 'yyyyMMdd': "year month day"}
                        table_config_body["partition_keys"] = [
                            {
                                "parent_column": partition_key,
                                "derive_format": derive_partition_format,
                                "derive_function": derive_func_map[derive_partition_format],
                                "is_derived_partition": True,
                                "partition_column": "iw_partition_column"
                            }]

            if split_by_key:
                table_config_body["split_by_key"] = {
                    "is_derived_split": False,
                    "split_column": split_by_key
                }
            if storage_format is not None:
                table_config_body["storage_format"] = storage_format
            response = IWUtils.ejson_deserialize(
                self.call_api("PUT",
                              url_builder.update_table_ingestion_config_url(self.client_config, source_id, table_id),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              data=table_config_body).content,
            )
            if response is not None:
                result = response.get("result", [])

            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error Updating the table ingestion configuration details")
            raise SourceError(
                f"Error Updating the table ingestion configuration details for table for {table_id} " + str(e))

    def configure_table_ingestion_properties_with_payload(self, source_id, table_id, table_payload):
        """
        Function to configure the ingestion configs of table
        :param source_id: Entity identifier for source
        :type source_id: String
        :param table_id: table entity id
        :type table_id: String
        :param table_payload: Payload to configure
        :type table_payload: Dict
        :return: response dict
        """
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("PUT",
                              url_builder.update_table_ingestion_config_url(self.client_config, source_id, table_id),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              data=table_payload).content,
            )
            if response is not None:
                result = response.get("result", [])

            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error Updating the table ingestion configuration details")
            raise SourceError(
                f"Error Updating the table ingestion configuration details for table for {table_id} " + str(e))

    def get_tableid_from_name(self, source_id, table_name):
        """
                Function to return table id from name
                :param source_id: Source Identifier
                :type source_id: String
                :param table_name: Table name
                :type table_name: String
                :return: Table id
        """
        params = {"filter": {"source": source_id, "table": table_name}}
        url_to_list_tables_under_source = url_builder.list_tables_under_source(
            self.client_config, source_id) + IWUtils.get_query_params_string_from_dict(params=params)
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_tables_under_source,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                if len(result) > 0:
                    return result[0]["id"]
        except Exception as e:
            self.logger.error("Error in listing tables under source")
            raise SourceError("Error in listing tables under source " + str(e))

    def get_tablename_from_id(self, source_id, table_id):
        """
                Function to return table name from id
                :param source_id: Source identifier
                :type source_id: String
                :param table_id: Table identifier
                :type table_id: String
                :return: Table name
        """
        params = {"filter": {"source": source_id, "_id": table_id}}
        url_to_list_tables_under_source = url_builder.list_tables_under_source(
            self.client_config, source_id) + IWUtils.get_query_params_string_from_dict(params=params)
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_tables_under_source,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                if len(result) > 0:
                    return result[0]["name"]
        except Exception as e:
            self.logger.error("Error in listing tables under source")
            raise SourceError("Error in listing tables under source " + str(e))
