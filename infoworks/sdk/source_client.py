import time
import traceback
from infoworks.error import SourceError
from infoworks.sdk import url_builder, local_configurations
from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.generic_response import GenericResponse
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
        :param poll_timeout: Polling timeout(default : 1200 seconds).If -1 then till the job completes polling will be done.
        :type poll_timeout: Integer
        :param polling_frequency: Polling Frequency(default : 15 seconds)
        :type polling_frequency: Integer
        :param retries: Number of Retries (default : 3)
        :type retries: Integer
        :return: response Object with Job status
        """
        if None in {source_id}:
            self.logger.error("source id is mandatory parameters for this method")
            raise Exception("source id cannot be None")
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
                    job_type = result.get("type", "source_job")
                    print(f"{job_type}_status : {job_status}.Sleeping for {polling_frequency} seconds")
                    self.logger.info(
                        "Job poll status : " + result["status"] + "Job completion percentage: " + str(result.get(
                            "percentage", 0)))
                    if job_status.lower() in ["completed", "failed", "aborted", "canceled"]:
                        return SourceResponse.parse_result(status=Response.Status.SUCCESS, source_id=source_id,
                                                           job_id=job_id, response=response)
                else:
                    self.logger.error(f"Error occurred during job {job_id} status poll")
                    if failed_count >= retries - 1:
                        return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                           error_code=ErrorCode.GENERIC_ERROR,
                                                           error_desc=f"Error occurred during job {job_id} status poll",
                                                           response=response, job_id=job_id,
                                                           source_id=source_id)
                    failed_count = failed_count + 1
            except Exception as e:
                self.logger.exception("Error occurred during job status poll")
                self.logger.info(str(e))
                if failed_count >= retries - 1:
                    print(traceback.print_stack())
                    print(response)
                    raise SourceError(response.get("message", "Error occurred during job status poll"))
                failed_count = failed_count + 1
            time.sleep(polling_frequency)

        return SourceResponse.parse_result(status=Response.Status.FAILED,
                                           error_code=ErrorCode.POLL_TIMEOUT,
                                           error_desc="Job status poll timeout occurred", response=response,
                                           job_id=job_id,
                                           source_id=source_id)

    def create_source(self, source_config=None):
        """
        Create a new Infoworks source
        :param source_config: a JSON object containing source configurations
        :type source_config: JSON Object
        ```
        source_config_example = {
            "name": "source_name",
            "type": "rdbms",
            "sub_type": "subtype",
            "data_lake_path": "datalake_path",
            "environment_id": "environment_id",
            "storage_id": "storage_id",
            "is_source_ingested": True
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
                self.call_api("POST", url_builder.create_source_url(self.client_config),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              source_config).content)

            result = response.get('result', {})
            source_id = result.get('id', None)

            if source_id is None:
                self.logger.error('Cannot create a new source.')
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_desc='Source failed to create.',
                                                   response=response)

            source_id = str(source_id)
            self.logger.info('Source {id} has been created.'.format(id=source_id))
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, source_id=source_id, response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to create a new source.')
            print('Error occurred while trying to create a new source.')
            raise SourceError(response.get("message", "Error occurred while trying to create a new source."))

    def configure_source_connection(self, source_id=None, connection_object=None, read_passwords_from_secrets=False):
        """
        Function to configure the source connection
        :param read_passwords_from_secrets: True/False. If True then the passwords are read from the secret manager info provided
        :type read_passwords_from_secrets: Boolean
        :param source_id: source identifier entity id
        :type source_id: String
        :param connection_object: The json dict containing the source connection details
        :type connection_object: Json object
        ```
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
        ```
        :return: response dict
        """
        if None in {source_id} or connection_object is None:
            self.logger.error("Both source id and connection object are mandatory parameters for this method")
            raise Exception("source id and connection object cannot be None")
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
            result = response.get('result', None)
            if result is None:
                self.logger.error(f"Failed to configure the source connection for {source_id} ")
                print(f"Failed to configure the source connection for {source_id}")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to configure the source connection for {source_id}",
                                                   response=response, job_id=None, source_id=None)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            raise SourceError(f"Failed to configure the source connection for {source_id} " + str(e))

    def source_test_connection_job_poll(self, source_id=None, poll_timeout=300, polling_frequency=15, retries=3):
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

        if None in {source_id}:
            self.logger.error("source id cannot be None")
            raise Exception("source id cannot be None")
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
                                               error_desc=f"Failed to configure the source connection for {source_id}",
                                               response=response, job_id=None, source_id=source_id)
        else:
            job_id = result["id"]
            self.logger.info(response.get("message", ""))
            return self.poll_job(source_id=source_id, job_id=job_id, poll_timeout=poll_timeout,
                                 polling_frequency=polling_frequency,
                                 retries=retries)

    def source_metacrawl_job_poll(self, source_id=None, poll_timeout=300, polling_frequency=15, retries=3):
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
        if None in {source_id}:
            self.logger.error("source id cannot be None")
            raise Exception("source id cannot be None")
        url_for_metacrawl = url_builder.get_test_connection_url(self.client_config, source_id)
        test_connection_dict = {
            "job_type": "source_fetch_metadata",
            "overwrite": True
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
                                               error_desc=f"Failed to create meta crawl job", response=response,
                                               job_id=None, source_id=source_id)
        else:
            job_id = result["id"]
            self.logger.info(response.get("message", ""))
            return self.poll_job(source_id=source_id, job_id=job_id, poll_timeout=poll_timeout,
                                 polling_frequency=polling_frequency,
                                 retries=retries)

    def browse_source_tables(self, source_id=None, filter_tables_properties=None, poll_timeout=300,
                             polling_frequency=15,
                             retries=3, poll=True):
        """
        Function to browse the source based on the filter_tables_properties passed
        :param source_id: source identifier entity id
        :type source_id: String
        :param filter_tables_properties:
        :type filter_tables_properties: json object
        ```
        filter_tables_properties = {
            "schemas_filter" : "%dbo",
            "catalogs_filter" : "%",
            "tables_filter" : "%csv_incremental_test",
            "is_data_sync_with_filter" : true,
            "is_filter_enabled" : true
        }
        ```
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
        if None in {source_id}:
            self.logger.error("source id cannot be None")
            raise Exception("source id cannot be None")
        try:
            url_for_browse_source = url_builder.browse_source_tables_url(self.client_config, source_id)
            if filter_tables_properties is not None:
                filter_condition = f"?is_filter_enabled=true&tables_filter={filter_tables_properties.get('tables_filter', '')}&catalogs_filter={filter_tables_properties.get('catalogs_filter', '')}&schemas_filter={filter_tables_properties.get('schemas_filter', '')}"
                url_for_browse_source = url_for_browse_source + filter_condition
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_for_browse_source,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            result = response.get('result', {})
            if result.get("message", "") == "Interactive Cluster is not running. Please bring up cluster and retry":
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=f"Interactive Cluster is not running. Please bring up cluster and retry",
                                                   response=response, job_id=None,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to create browse table job for {source_id} " + str(e))
        if len(result) == 0 and "id" not in result:
            self.logger.error(f"Failed to create browse table job")
            return SourceResponse.parse_result(status=Response.Status.FAILED,
                                               error_code=ErrorCode.GENERIC_ERROR,
                                               error_desc=f"Failed to create browse table job",
                                               response=response, job_id=None,
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
                    self.logger.info(f"Browse source job poll status : {job_status}")
                    if job_status in ["completed", "failed", "aborted"]:
                        break
                    if job_status is None:
                        self.logger.info(f"Error occurred during job {job_id} status poll")
                        if failed_count >= retries - 1:
                            return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                               error_code=ErrorCode.GENERIC_ERROR,
                                                               error_desc=f"Error occurred during job {job_id} status poll",
                                                               response=response, job_id=job_id,
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
                                                   error_desc=f"Browse table job for source {source_id} failed with {job_status}",
                                                   response=response, job_id=job_id,
                                                   source_id=source_id)

    def add_tables_to_source(self, source_id=None, tables_to_add_config=None, poll_timeout=300, polling_frequency=15,
                             retries=3, poll=True):
        """
        Function to add tables to source
        :param source_id: source identifier entity id
        :type source_id: String
        :param tables_to_add_config: Array of JSON configuration object
        :type tables_to_add_config: List
        ```
        tables_to_add_config = [{
                "table_name": "",
                "schema_name": "",
                "table_type": "TABLE",
                "target_table_name": "",
                "target_schema_name": ""
        }]
        ```
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
        if None in {source_id} or tables_to_add_config is None:
            self.logger.error("source id or tables_to_add_config cannot be None")
            raise Exception("source id or tables_to_add_config cannot be None")
        try:
            url_for_add_tables_to_source = url_builder.add_tables_to_source_url(self.client_config, source_id)
            add_tables_dict = {"tables_to_add": tables_to_add_config}
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", url_for_add_tables_to_source,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              add_tables_dict).content)
            result = response.get('result', None)
            self.logger.debug(response)
            if result is not None:
                self.logger.info(f"Added the below table Ids to the source {source_id}")
                self.logger.info(result["added_tables"])
                self.logger.info(response["message"])
                self.logger.info(f"Triggered metacrawl job for tables. Infoworks JobID {result['job_created']}")
                job_id = result['job_created']
                if not poll:
                    self.logger.info(f"Tables added to source {source_id} and metacrawl job was submitted {job_id}")
                    return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
                else:
                    return self.poll_job(source_id=source_id, job_id=job_id, poll_timeout=poll_timeout,
                                         polling_frequency=polling_frequency,
                                         retries=retries)
            else:
                self.logger.error(f"Failed to add the tables to the source {source_id}")
                self.logger.debug(response)
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=f"Failed to add the tables to the source {source_id}",
                                                   response=response, job_id=None,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to add the tables to the source {source_id} " + str(e))

    def configure_tables_and_tablegroups(self, source_id=None, configuration_obj=None):
        """
        Function to configure tables and table-groups.
        The configuration_obj should be similar to the json object that is the output of GET source configuration API
        :param source_id: source identifier entity id
        :type source_id: String
        :param configuration_obj: Configurations for the tables and tablegroups under given source id
        :type configuration_obj: JSON object
        :return:  response dict
        """
        try:
            if None in {source_id} or configuration_obj is None:
                self.logger.error("source id or configuration_obj cannot be None")
                raise Exception("source id or configuration_obj cannot be None")
            configure_tables_tg_url = url_builder.configure_tables_and_tablegroups_url(self.client_config, source_id)
            errors = {}
            response = self.call_api("POST", configure_tables_tg_url,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              {"configuration": configuration_obj})
            parsed_response = IWUtils.ejson_deserialize(response.content)
            result = parsed_response.get('result', {}).get("configuration", {}).get("iw_mappings", [])
            count = 0
            print("Config Migration API Response:", response)

            self.logger.debug(response)
            if result is not None:
                for config_item in result:
                    table_upsert_status = config_item.get('table_upsert_status', None)
                    if table_upsert_status is not None and len(table_upsert_status.get("error", [])) != 0:
                        if not table_upsert_status["error"][0].startswith("Truncate the table:"):
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
                    result = parsed_response.get("result",[])
                    return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                       error_code=ErrorCode.GENERIC_ERROR,
                                                       error_desc=f"Failed to configure tables and table groups",
                                                       job_id=None,
                                                       response=result,
                                                       source_id=source_id)
                elif response.status_code != 200:
                    self.logger.error(f"Failed during config migration")
                    return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                       error_code=ErrorCode.GENERIC_ERROR,
                                                       error_desc=f"Failed to configure tables and table groups {parsed_response} ",
                                                       job_id=None,
                                                       response=parsed_response,
                                                       source_id=source_id)
                else:
                    self.logger.info(f"Successfully configured tables and table groups for the source")
                    return SourceResponse.parse_result(status=Response.Status.SUCCESS,response=parsed_response)
            else:
                self.logger.error("Failed to configure tables and table groups")
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=f"Failed to configure tables and table groups {parsed_response} ",
                                                   job_id=None,
                                                   response=parsed_response,
                                                   source_id=source_id)
        except Exception as e:
            traceback.print_exc()
            raise SourceError(f"Failed to configure tables and table groups" + str(e))

    def create_table_group(self, source_id=None, table_group_obj=None):
        """
        Function to create table group
        :param source_id: entity identifier for source
        :type source_id: String
        :param table_group_obj: JSON object containing create table group payload
        :type table_group_obj: JSON object
        ```
        table_group_obj = {
         "environment_compute_template": {"environment_compute_template_id": "536592c8ceb69bbbe730d452"},
         "name": "tg_name",
         "max_connections": 1,
         "max_parallel_entities": 1,
         "tables": [
          {"table_id":"1123","connection_quota":100}
         ]
        }
        ```
        :return:  response dict
        """
        try:
            if None in {source_id} or table_group_obj is None:
                self.logger.error("source id or table_group_obj cannot be None")
                raise Exception("source id or table_group_obj cannot be None")
            create_tg_url = url_builder.create_table_group_url(self.client_config, source_id)
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", create_tg_url,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              table_group_obj).content)
            if len(response.get("result", {})) > 0:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS,
                                                   response=response.get("result")["id"])
            else:
                self.logger.error("Failed to create table group")
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc="Failed to create table groups",
                                                   response=response, job_id=None,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to create table group" + str(e))

    def update_table_group(self, source_id=None, table_group_id=None, table_group_obj=None):
        """
        Function to update table group
        :param source_id: entity identifier for source
        :type source_id: String
        :param table_group_id: entity identifier for tablegroup
        :type table_group_id: String
        :param table_group_obj: JSON object containing update table group body
        :type table_group_obj: JSON object
        ```
        table_group_obj = {
         "environment_compute_template": {"environment_compute_template_id": "536592c8ceb69bbbe730d452"},
         "name": "tg_name",
         "max_connections": 1,
         "max_parallel_entities": 1,
         "tables": [
          {"table_id":"1123","connection_quota":100}
         ]
        }
        ```
        :return: response dict
        """
        try:
            if None in {source_id, table_group_id} or table_group_obj is None:
                self.logger.error("source id or table_group_id or table_group_obj cannot be None")
                raise Exception("source id or table_group_id or table_group_obj cannot be None")
            create_tg_url = url_builder.create_table_group_url(self.client_config, source_id) + f"/{table_group_id}"
            response = IWUtils.ejson_deserialize(
                self.call_api("PUT", create_tg_url,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              table_group_obj).content)
            if len(response.get("result", {})) > 0:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS,
                                                   response=response.get("result")["id"])
            else:
                self.logger.error("Failed to create table groups")
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc="Failed to create table groups",
                                                   response=response, job_id=None,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to update table group" + str(e))

    def get_table_group_details(self, source_id=None, params=None, tg_id=None):
        """
        Function to list the table groups under source
        :param source_id: entity identifier for source
        :type source_id: String
        :param tg_id: id of table group config to fetch
        :type tg_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if None in {source_id}:
            self.logger.error("source id cannot be None")
            raise Exception("source id cannot be None")
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
                result = response.get("result", None)
                if result is None:
                    self.logger.error(f'Failed to list the table groups under source {source_id}')
                    return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                       error_code=ErrorCode.USER_ERROR,
                                                       error_desc=f'Failed to list the table groups under source {source_id}',
                                                       response=response)
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
            response["result"] = tg_list
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing table groups")
            raise SourceError("Error in listing table groups" + str(e))

    def delete_table_group(self, source_id=None, tg_id=None):
        """
        Function to delete table group under the source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param tg_id: id of table group config to delete
        :type tg_id: String
        :return: response dict
        """
        if None in {source_id, tg_id}:
            self.logger.error("source id or tg_id cannot be None")
            raise Exception("source id or tg_id cannot be None")
        url_to_delete_tg = url_builder.create_table_group_url(self.client_config, source_id).strip("/") + f"/{tg_id}"
        try:
            response = IWUtils.ejson_deserialize(self.call_api("DELETE",
                                                               url_to_delete_tg,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token'])).content)
            result = response.get("result", None)
            if result is None:
                self.logger.error(f'Failed to delete table groups under source {source_id}')
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f'Failed to delete table groups under source {source_id}',
                                                   response=response)
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)

        except Exception as e:
            self.logger.error("Error in deleting the table group")
            raise SourceError("Error in deleting table group" + str(e))

    def submit_source_job(self, source_id=None, body=None, poll=False, poll_timeout=300, polling_frequency=15,
                          retries=3):
        """
        Initiates jobs for source and its artifacts.
        The job types that can be submitted are export_data, streaming_start, segmentation_load, truncate_reload, cdc_merge, truncate_table, source_test_connection, source_fetch_metadata, table_fetch_metadata.
        :param source_id: source entity id
        :type source_id: String
        :param body: JSON body containing type of job to trigger
        :type body: JSON dict
        ```
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
        ```
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
            if None in {source_id} or body is None:
                self.logger.error("source id or body cannot be None")
                raise Exception("source id or body cannot be None")
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", url_builder.submit_source_job(self.client_config, source_id),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              body).content)
            result = response.get('result', {})
            if len(result) != 0:
                job_id = result["id"]
                if not poll:
                    self.logger.info(f"Job successfully submitted for {source_id}. JobID to track is: {job_id}")
                    return SourceResponse.parse_result(status=Response.Status.SUCCESS, job_id=job_id, response=response)
                else:
                    return self.poll_job(source_id=source_id, job_id=job_id, poll_timeout=poll_timeout,
                                         polling_frequency=polling_frequency,
                                         retries=retries)
            else:
                self.logger.error(f"Failed to submit the source job.")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to submit the source job.",
                                                   response=response)
        except Exception as e:
            raise SourceError(f"Failed to create source job: " + str(e))

    def resubmit_source_job(self, job_id=None, poll=False, poll_timeout=300, polling_frequency=15, retries=3):
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
            if None in {job_id}:
                self.logger.error("job id cannot be None")
                raise Exception("job id cannot be None")
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
            else:
                self.logger.error("Failed to submit the Job")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc="Failed to submit the Job", response=response)
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
                initial_msg = response.get("message", "")
                result = response.get("result", [])
                while len(result) > 0:
                    source_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", [])
            else:
                self.logger.error("Failed to get list of sources")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc="Failed to get list of sources", response=response)
            response["result"] = source_list
            response["message"] = initial_msg
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)

        except Exception as e:
            self.logger.error("Error in listing sources")
            raise SourceError("Error in listing sources " + str(e))

    def update_source(self, source_id=None, update_body=None):
        """
        Function to update the source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param update_body: JSON body with key-value pair to update
        :type update_body: JSON dict
        :return: response dict
        """
        try:
            if None in {source_id} or update_body is None:
                self.logger.error("source id or update_body cannot be None")
                raise Exception("source id or update body cannot be None")
            response = self.call_api("PATCH", url_builder.source_info(self.client_config, source_id),
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=update_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content
            )
            result = parsed_response.get("result", None)
            if result is not None:
                self.logger.info("Updated the source successfully")
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response,
                                                   source_id=source_id)
            else:
                self.logger.error("Failed to Update the source")
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc="Failed to Update the source",
                                                   response=parsed_response
                                                   )
        except Exception as e:
            self.logger.error("Error in updating the source")
            raise SourceError("Error in updating the source" + str(e))

    def get_source_connection_details(self, source_id=None):
        """
        Function to get source connection details like jdbc url, source type etc
        :param source_id: Entity identifier for source
        :type source_id: String
        :return: response dict
        """
        try:
            if None in {source_id}:
                self.logger.error("source id cannot be None")
                raise Exception("source id cannot be None")
            source_connection_configuration_url = url_builder.get_source_connection_details_url(self.client_config,
                                                                                                source_id)
            response = IWUtils.ejson_deserialize(self.call_api("GET", source_connection_configuration_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get('result', None)
            if not result:
                self.logger.error(f"Failed to get the source connection for {source_id} ")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to get the source connection for {source_id} ",
                                                   response=response, job_id=None, source_id=None)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            raise SourceError(f"Failed to get the source connection for {source_id} " + str(e))

    def add_source_advanced_configuration(self, source_id=None, config_body=None):
        """
        Function to add advanced configuration to the source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param config_body: JSON body with key-value advanced config
        :type config_body: JSON dict
        :return: response dict
        """
        try:
            if None in {source_id} or config_body is None:
                self.logger.error("source id or config_body cannot be None")
                raise Exception("source id or config_body cannot be None")
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
                                                   error_desc="Error in adding source advanced configs",
                                                   response=parsed_response)
        except Exception as e:
            self.logger.error("Error in adding the source advanced config")
            raise SourceError("Error in adding the source advanced config" + str(e))

    def update_source_advanced_configuration(self, source_id=None, key=None, config_body=None):
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
            if None in {source_id, key} or config_body is None:
                self.logger.error("source id or key or config_body cannot be None")
                raise Exception("source id or key or config_body cannot be None")
            response = self.call_api("PUT",
                                     url_builder.get_advanced_config_url(self.client_config, source_id).strip(
                                         "/") + f"/{key}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content
            )
            result = parsed_response.get("result", None)
            if response.status_code == 200:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc="Error in updating source advanced configs",
                                                   response=parsed_response
                                                   )
        except Exception as e:
            self.logger.error("Error in updating the source advanced configs")
            raise SourceError("Error in updating the source advanced config" + str(e))

    def get_advanced_configuration_of_sources(self, source_id=None, params=None, key=None):
        """
        Function to get the advanced configuration of source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param key: Name of advanced config to get
        :type key: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if None in {source_id}:
            self.logger.error("source id cannot be None")
            raise Exception("source id cannot be None")
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
            initial_msg = ""
            if response is not None:
                initial_msg = response.get("message", "")
                result = response.get("result", None)
                if result is None:
                    return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                       error_code=ErrorCode.GENERIC_ERROR,
                                                       error_desc="Error in getting source advanced configs",
                                                       response=response
                                                       )
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
            response["result"] = adv_config_list
            response["message"] = initial_msg
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing advanced configurations")
            raise SourceError("Error in listing advanced configurations" + str(e))

    def delete_source_advanced_configuration(self, source_id=None, key=None):
        """
        Function to delete advanced configuration of the source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param key: Name of advanced config to delete
        :type key: String
        :return: response dict
        """
        try:
            if None in {source_id, key}:
                self.logger.error("source id or key cannot be None")
                raise Exception("source id or key cannot be None")
            response = IWUtils.ejson_deserialize(self.call_api("DELETE",
                                                               url_builder.get_advanced_config_url(self.client_config,
                                                                                                   source_id).strip(
                                                                   "/") + f"/{key}",
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token'])).content)
            if response.get("message", "") == "Successfully removed Advance configuration":
                self.logger.info("Successfully deleted the source advance configuration")
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc="Error in deleting source advanced config",
                                                   response=response
                                                   )
        except Exception as e:
            self.logger.error("Error in deleting the source advanced config")
            raise SourceError("Error in deleting advanced configurations" + str(e))

    def get_source_configurations_json_export(self, source_id=None):
        """
        Function to get source configurations
        :param source_id: Entity identifier for source
        :type source_id: String
        :return: response dict
        """
        try:
            if None in {source_id}:
                self.logger.error("source id cannot be None")
                raise Exception("source id cannot be None")
            source_configuration_url = url_builder.configure_source_url(self.client_config, source_id)
            response = IWUtils.ejson_deserialize(self.call_api("GET", source_configuration_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get('result', None)
            if not result:
                self.logger.error(f"Failed to get the source configurations for {source_id} ")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to get the source configurations for {source_id}",
                                                   response=response, job_id=None, source_id=None)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            raise SourceError(f"Failed to get the source configurations for {source_id} " + str(e))

    def post_source_configurations_json_import(self, source_id=None, config_body=None):
        """
        Function to configure the source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param config_body: JSON body
        :type config_body: JSON dict
        :return: response dict
        """
        if None in {source_id} or config_body is None:
            self.logger.error("source id or config_body cannot be None")
            raise Exception("source id or config_body cannot be None")
        source_configuration_url = url_builder.configure_source_url(self.client_config, source_id)
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("POST",
                              source_configuration_url,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              data=config_body).content
            )
            result = response.get("result", None)
            if result is not None:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc="Error in updating source configs",
                                                   response=response
                                                   )
        except Exception as e:
            self.logger.error("Error in updating the source configs")
            raise SourceError("Error in updating the source configs" + str(e))

    def list_tables_in_source(self, source_id=None, params=None):
        """
        Function to list the tables part of the source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if None in {source_id}:
            self.logger.error("source id cannot be None")
            raise Exception("source id cannot be None")
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
                initial_msg = response.get("message", "")
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
                    result = response.get("result", None)
                    if result is None:
                        return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                           error_code=ErrorCode.GENERIC_ERROR,
                                                           error_desc="Error in listing tables under the source",
                                                           response=response
                                                           )

                response["result"] = tables_list
                response["message"] = initial_msg
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing tables under source")
            raise SourceError("Error in listing tables under source" + str(e))

    def get_list_of_table_groups(self, source_id=None, params=None):
        """
        Function to list the tables groups part of the source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if None in {source_id}:
            self.logger.error("source id cannot be None")
            raise Exception("source id cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_tablegrps = url_builder.create_table_group_url(
            self.client_config, source_id) + IWUtils.get_query_params_string_from_dict(params=params)

        tablegrp_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_tablegrps,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                initial_msg = response.get("message", "")
                while len(result) > 0:
                    tablegrp_list.extend(result)
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
                        return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                           error_code=ErrorCode.GENERIC_ERROR,
                                                           error_desc="Error in listing table groups under the source",
                                                           response=response
                                                           )

                response["result"] = tablegrp_list
                response["message"] = initial_msg
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing table groups under source")
            raise SourceError("Error in listing table groups under source" + str(e))

    def get_table_columns_details(self, source_id=None, table_name=None, schema_name=None, database_name=None):
        """
        Function to get the table column details
        :param source_id: Entity identifier for source
        :type source_id: String
        :param table_name: table name at source
        :type table_name: String
        :param schema_name: Schema name at source
        :type schema_name: String
        :param database_name: Catalog name at source
        :type database_name: String
        :return: response dict
        """
        if None in {source_id, table_name, schema_name, database_name}:
            self.logger.error("source id or table_name or schema_name or database_name cannot be None")
            raise Exception("source id or table_name or schema_name or database_name cannot be None")
        url_to_list_tables = url_builder.list_tables_under_source(self.client_config, source_id)
        filter_cond = "?filter={\"table\":\"" + table_name + "\",\"catalog_name\":\"" + database_name + "\",\"schemaNameAtSource\": \"" + schema_name + "\"} "
        get_source_table_info_url = url_to_list_tables + filter_cond
        try:
            response = self.call_api("GET", get_source_table_info_url,
                                     headers=IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                                     )

            result = IWUtils.ejson_deserialize(response.content).get('result', None)
            if result is not None:
                lookup_columns = result[0]["columns"]
                lookup_columns_dict = {}
                for column in lookup_columns:
                    lookup_columns_dict[column["name"]] = {}
                    lookup_columns_dict[column["name"]]["target_sql_type"] = column["target_sql_type"]
                    lookup_columns_dict[column["name"]]["target_precision"] = column.get("target_precision", "")
                    lookup_columns_dict[column["name"]]["target_scale"] = column.get("target_scale", "")
                    lookup_columns_dict[column["name"]]["col_size"] = column.get("col_size", "")
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=lookup_columns_dict)
            else:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc=f"No Tables found for given source with name {table_name}",
                                                   response=response)
        except Exception as e:
            self.logger.error(f"Error in getting column details for table {table_name}")
            raise SourceError(f"Error in getting column details for table {table_name}" + str(e))

    def get_table_configurations(self, source_id=None, table_id=None, ingestion_config_only=False):
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
        if None in {source_id, table_id}:
            self.logger.error("source id or table_id cannot be None")
            raise Exception("source id or table_id cannot be None")
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
                self.logger.error(f"Failed to get the table configurations for {table_id}")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to get the table configurations for {table_id}",
                                                   response=response, job_id=None, source_id=None)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            raise SourceError(f"Failed to get the table configurations for {table_id} " + str(e))

    def update_table_configuration(self, source_id=None, table_id=None, config_body=None):
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
        if None in {source_id, table_id} or config_body is None:
            self.logger.error("source id or table_id or config_body cannot be None")
            raise Exception("source id or table_id or config_body cannot be None")
        try:
            response = self.call_api("PUT",
                                     url_builder.get_table_configuration(self.client_config, source_id, table_id),
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content
            )
            result = parsed_response.get("result", None)

            if result is not None:
                self.logger.info("Updated the table configurations successfully!")
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                self.logger.error(f"Failed to update the table {table_id} configurations under source {source_id}")
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc="Error in updating table configuration",
                                                   response=parsed_response
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
        if None in {source_id, table_id} or config_body is None:
            self.logger.error("source id or table_id or config_body cannot be None")
            raise Exception("source id or table_id or config_body cannot be None")
        try:
            response = self.call_api("POST",
                                     url_builder.get_advanced_config_url(self.client_config, source_id, table_id),
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content
            )
            result = parsed_response.get("result", None)
            if result is not None:
                self.logger.info("Added the table level advance configurations successfully")
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response,
                                                   source_id=source_id)
            else:
                self.logger.info("Failed to add the table level advance configurations")
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc="Error in adding table advanced config",
                                                   response=parsed_response
                                                   )
        except Exception as e:
            self.logger.error("Error in adding table level advanced configuration")
            raise SourceError(f"Error in adding table level advanced configuration for {table_id} " + str(e))

    def update_table_advanced_configuration(self, source_id=None, table_id=None, key=None, config_body=None):
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
        if None in {source_id, table_id, key} or config_body is None:
            self.logger.error("source id or table_id or config_body or key cannot be None")
            raise Exception("source id or table_id or config_body or key cannot be None")
        try:
            response = self.call_api("PUT",
                                     url_builder.get_advanced_config_url(self.client_config, source_id, table_id).strip(
                                         "/") + f"/{key}",
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                     data=config_body)
            parsed_response = IWUtils.ejson_deserialize(
                response.content
            )
            if parsed_response.get("message", "") == "Successfully updated Advance configuration":
                self.logger.info("Successfully updated the table advanced config")
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=parsed_response)
            else:
                self.logger.error("Error in updating table advanced config")
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc="Error in updating table advanced config",
                                                   response=parsed_response
                                                   )
        except Exception as e:
            self.logger.error("Error in updating the table advanced configs")
            raise SourceError(f"Error in updating table level advanced configuration for {table_id} " + str(e))

    def delete_table_advanced_configuration(self, source_id=None, table_id=None, key=None):
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
        if None in {source_id, table_id, key}:
            self.logger.error("source id or table_id or config_body cannot be None")
            raise Exception("source id or table_id or config_body cannot be None")
        try:
            delete_advanced_config_url = url_builder.get_advanced_config_url(self.client_config, source_id,
                                                                             table_id).strip(
                "/") + f"/{key}"
            response = IWUtils.ejson_deserialize(
                self.call_api("DELETE", delete_advanced_config_url, IWUtils.get_default_header_for_v3(
                    self.client_config['bearer_token'])).content)
            if response.get("message", "") == "Successfully removed Advance configuration":
                self.logger.info("Successfully deleted the table advance config")
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                self.logger.error("Failed to delete the table advance configuration")
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc="Error in deleting table advanced config",
                                                   response=response
                                                   )
        except Exception as e:
            self.logger.error("Error in deleting the table advanced config")
            raise SourceError(f"Error in deleting table level advanced configuration for {table_id} " + str(e))

    def get_table_export_configurations(self, source_id=None, table_id=None, connection_only=False):
        """
        Function to get table export configurations
        :param connection_only: Get export configuration connection details only
        :param table_id: Entity identifier for table
        :type table_id: String
        :param source_id: Entity identifier for source
        :type source_id: String
        :return: response dict
        """
        if None in {source_id, table_id}:
            self.logger.error("source id or table_id  cannot be None")
            raise Exception("source id or table_id cannot be None")
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
            result = response.get('result', None)
            if not result:
                self.logger.error(f"Failed to get the table export configurations for {table_id} ")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to get the table export configurations for {table_id} ",
                                                   response=response, job_id=None, source_id=None)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            raise SourceError(f"Failed to get the table export configurations for {table_id} " + str(e))

    def update_table_export_configuration(self, source_id=None, table_id=None, config_body=None, connection_only=False):
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
        if None in {source_id, table_id}:
            self.logger.error("source id or table_id  cannot be None")
            raise Exception("source id or table_id cannot be None")
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
            result = parsed_response.get("result", None)
            if result is not None:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.GENERIC_ERROR,
                                                   error_desc="Error in updating export configuration of the table",
                                                   response=response)

        except Exception as e:
            self.logger.error("Error in updating the export configuration of the table")
            raise SourceError(f"Error in updating the export configuration of the table for {table_id} " + str(e))

    def get_table_ingestion_metrics(self, source_id=None, table_id=None):
        """
        Function to fetch ingestion metrics of source tables
        :param source_id: Entity identifier for source
        :type source_id: String
        :param table_id: table entity id
        :type table_id: String
        :return: response dict
        """
        if None in {source_id}:
            self.logger.error("source id cannot be None")
            raise Exception("source id cannot be None")
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
                    result = response.get("result", None)
                    if result is None:
                        return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                           error_code=ErrorCode.GENERIC_ERROR,
                                                           error_desc="Error in getting the table ingestion metrics",
                                                           response=response)

            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=metric_results)
        except Exception as e:
            self.logger.error("Error in fetching ingestion metrics for source")
            raise SourceError(f"Error in fetching ingestion metrics for source for {source_id} " + str(e))

    def add_table_and_file_mappings_for_csv(self, source_id=None, file_mappings_config=None):
        """
        Function to Create table and Add a new file mapping (table) in the csv source
        :param source_id: Entity identifier for source
        :type source_id: String
        :file_mappings_config : Configurations for file_mappings
        :type file_mappings_config: JSON object
        ```
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
        ```
        :return: response dict
        """
        if None in {source_id} or file_mappings_config is None:
            self.logger.error("source id or file_mappings_config  cannot be None")
            raise Exception("source id or file_mappings_config cannot be None")
        try:
            add_filemappings_url = url_builder.list_tables_under_source(self.client_config, source_id)
            response = IWUtils.ejson_deserialize(self.call_api("POST", add_filemappings_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               file_mappings_config).content)
            result = response.get('result', None)
            if result is None:
                self.logger.error(f"Failed to add table and configure file mappings ")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to add the table and configure file mappings",
                                                   response=response, job_id=None, source_id=None)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            raise SourceError(f"Failed to add table and configure file mappings " + str(e))

    def get_sourceid_from_name(self, source_name=None):
        """
        Function to return source id from name
        :param source_name: Source name
        :type: String
        :return: Source Identifier
        """
        if None in {source_name}:
            self.logger.error("source name  cannot be None")
            raise Exception("source name cannot be None")
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
                    return SourceResponse.parse_result(status=Response.Status.SUCCESS, response={"id": result[0]["id"]})
                else:
                    raise Exception("Could not get the source details")
        except Exception as e:
            self.logger.error("Error in listing sources")
            raise SourceError("Error in listing sources " + str(e))

    def get_sourcename_from_id(self, src_id):
        """
        Function to return source name from Id
        :param src_id: Source identifier
        :type: String
        :return: Source name
        """
        if None in {src_id}:
            self.logger.error("source id cannot be None")
            raise Exception("source id cannot be None")
        url_to_list_sources = url_builder.list_sources_url(
            self.client_config) + f"/{src_id}"
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_sources,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            result = response.get("result", None)
            if result is not None:
                result = response.get("result", {})
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response={"name": result["name"]})
            else:
                raise Exception("Could not get the source name")
        except Exception as e:
            self.logger.error("Error in getting source name")
            raise SourceError("Error in getting source name " + str(e))

    def create_onboard_rdbms_source(self, src_name=None, src_type=None, data_lake_path=None, connection_url=None,
                                    username=None, password=None,
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
        if None in {src_name, src_type, data_lake_path, connection_url, username, password}:
            self.logger.error(
                "src_name or src_type or data_lake_path or connection_url or username or password cannot be None")
            raise Exception(
                "src_name or src_type or data_lake_path or connection_url or username or password cannot be None")
        if src_type.lower() not in ["oracle", "mysql", "sqlserver", "teradata", "netezza"]:
            print("Invalid source type. Exiting!!!")
            return None
        if src_type.lower() == "oracle":
            sub_type = SourceMappings.oracle.get("sub_type")
            driver_name = SourceMappings.oracle.get("driver_name")
            driver_version = SourceMappings.oracle.get("driver_version")
            database = SourceMappings.oracle.get("database")
        elif src_type.lower() == "sqlserver":
            sub_type = SourceMappings.sqlserver.get("sub_type")
            driver_name = SourceMappings.sqlserver.get("driver_name")
            driver_version = SourceMappings.sqlserver.get("driver_version")
            database = SourceMappings.sqlserver.get("database")
        elif src_type.lower() == "teradata":
            sub_type = SourceMappings.teradata.get("sub_type")
            driver_name = SourceMappings.teradata.get("driver_name")
            driver_version = SourceMappings.teradata.get("driver_version")
            database = SourceMappings.teradata.get("database")
        elif src_type.lower() == "mysql":
            sub_type = SourceMappings.mysql.get("sub_type")
            driver_name = SourceMappings.mysql.get("driver_name")
            driver_version = SourceMappings.mysql.get("driver_version")
            database = SourceMappings.mysql.get("database")
        elif src_type.lower() == "netezza":
            sub_type = SourceMappings.netezza.get("sub_type")
            driver_name = SourceMappings.netezza.get("driver_name")
            driver_version = SourceMappings.netezza.get("driver_version")
            database = SourceMappings.netezza.get("database")
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
            else:
                self.logger.error(f"Failed to configure source connection for source {src_name}", response)
                raise Exception(f"Failed to configure source connection for source {src_name}", response)
        else:
            self.logger.error(f"Failed to create source {src_name}", src_create_response)
            raise Exception(f"Failed to create source  {src_name}", src_create_response)

    def configure_table_ingestion_properties(self, source_id=None, table_id=None, natural_keys=None,
                                             sync_type="full-load",
                                             update_strategy=None,
                                             watermark_columns=None, partition_key=None, derived_partition=False,
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
        :param watermark_columns: Watermark columns to be used during incremental ingestion
        :type watermark_columns: Array
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
        if None in {source_id, table_id}:
            self.logger.error("source id or table_id cannot be None")
            raise Exception("source id or table_id cannot be None")
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
                if update_strategy in ["append", "merge"] and watermark_columns is not None:
                    if update_strategy == "append":
                        table_config_body["sync_type"] = "incremental"
                        table_config_body["update_strategy"] = "append"
                        table_config_body["watermark_columns"] = watermark_columns
                    elif update_strategy == "merge" and watermark_columns is not None:
                        table_config_body["sync_type"] = "incremental"
                        table_config_body["update_strategy"] = "merge"
                        table_config_body["watermark_columns"] = watermark_columns
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
                result = response.get("result", None)
                if result is None:
                    return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                       error_desc="Could not configure table ingestion properties",
                                                       response=response)

            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error Updating the table ingestion configuration details")
            raise SourceError(
                f"Error Updating the table ingestion configuration details for table for {table_id} " + str(e))

    def configure_table_ingestion_properties_with_payload(self, source_id=None, table_id=None, table_payload=None):
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
            if None in {source_id, table_id} or table_payload is None:
                self.logger.error("source id or table_id or table_payload cannot be None")
                raise Exception("source id or table_id or table_payload cannot be None")
            response = IWUtils.ejson_deserialize(
                self.call_api("PUT",
                              url_builder.update_table_ingestion_config_url(self.client_config, source_id, table_id),
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              data=table_payload).content,
            )
            if response is not None:
                result = response.get("result", None)
                if result is None:
                    return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                       error_desc="Could not configure table ingestion properties",
                                                       response=response)

            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error Updating the table ingestion configuration details")
            raise SourceError(
                f"Error Updating the table ingestion configuration details for table for {table_id} " + str(e))

    def get_tableid_from_name(self, source_id=None, table_name=None):
        """
                Function to return table id from name
                :param source_id: Source Identifier
                :type source_id: String
                :param table_name: Table name
                :type table_name: String
                :return: Table id
        """
        if None in {source_id, table_name}:
            self.logger.error("source id or table_name cannot be None")
            raise Exception("source id or table_name cannot be None")
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
                else:
                    raise Exception("Error while getting table id from name")
        except Exception as e:
            self.logger.error("Error in listing tables under source")
            raise SourceError("Error in listing tables under source " + str(e))

    def get_tablename_from_id(self, source_id=None, table_id=None):
        """
                Function to return table name from id
                :param source_id: Source identifier
                :type source_id: String
                :param table_id: Table identifier
                :type table_id: String
                :return: Table name
        """
        if None in {source_id, table_id}:
            self.logger.error("source id or table_id  cannot be None")
            raise Exception("source id or table_id cannot be None")
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
                else:
                    raise Exception("Error while getting table name from id")
        except Exception as e:
            self.logger.error("Error in listing tables under source")
            raise SourceError("Error in listing tables under source " + str(e))

    def get_source_configurations(self, source_id=None):
        """
        Function to get source configurations
        :param source_id: Entity identifier for source
        :type source_id: String
        :return: response dict
        """
        if None in {source_id}:
            self.logger.error("source id cannot be None")
            raise Exception("source id cannot be None")
        try:
            src_configurations_url = url_builder.get_source_configurations_url(self.client_config, source_id)
            response = IWUtils.ejson_deserialize(self.call_api("GET", src_configurations_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get('result', False)
            if not result:
                self.logger.error(f"Failed to get the source configurations for {source_id} ")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to get the table configurations for {source_id} ",
                                                   response=response, job_id=None, source_id=None)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            raise SourceError(f"Failed to get the source configurations for {source_id} " + str(e))

    def list_tables_under_source(self, source_id=None, params=None):
        """
        Function to list tables under source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        tables_list = []
        if None in {source_id}:
            self.logger.error("source id cannot be None")
            raise Exception("source id cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        try:
            table_list_url = url_builder.list_tables_under_source(self.client_config, source_id)
            table_list_url = table_list_url + IWUtils.get_query_params_string_from_dict(params=params)
            response = IWUtils.ejson_deserialize(self.call_api("GET", table_list_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            initial_msg = response.get("message", "")
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
            else:
                self.logger.error("Failed to get list of tables")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc="Failed to submit get list of tables", response=response)
            response["result"] = tables_list
            response["message"] = initial_msg
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response, source_id=source_id)
        except Exception as e:
            self.logger.error(f"Failed to get the tables under {source_id} " + str(e))
            raise SourceError(f"Failed to get the tables under {source_id} " + str(e))

    def get_source_metadata(self, source_id=None):
        """
        Function to get source metadata (like tags,description etc)
        :param source_id: Entity identifier for table
        :type source_id: String
        :return: response dict
        """
        if None in {source_id}:
            self.logger.error("source id cannot be None")
            raise Exception("source id cannot be None")
        try:
            source_configurations_url = url_builder.get_source_details_url(self.client_config)
            source_configurations_url = source_configurations_url + f"/{source_id}/metadata"
            response = IWUtils.ejson_deserialize(self.call_api("GET", source_configurations_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get('result', False)
            if not result:
                self.logger.error(f"Failed to get the source metadata for {source_id}")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to get the source metadata for {source_id}",
                                                   response=response, job_id=None, source_id=source_id)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to get the source metadata for {source_id} " + str(e))

    def update_source_metadata(self, source_id, tags_to_add: str = None, tags_to_remove: str = None,
                               description: str = None, is_favorite=False, data=None):
        """
        Function to update source metadata (like tags,description etc)
        :param source_id: Entity identifier for source
        :type source_id: String
        :param tags_to_add: Pass comma seperated tags to add. Optional field
        :param tags_to_remove: Pass comma seperated tags to remove. Optional field
        :param description: Pass the description of the tag
        :param is_favorite: Boolean. True/False
        :param data : Json body to update source metadata. You can pass only this param if the body of API is known. Else pass other individual params
        :type data: JSON dict
        :return: response dict
        """
        if None in {source_id}:
            self.logger.error("source id cannot be None")
            raise Exception("source id cannot be None")
        try:
            if data is None:
                data = {
                    "tags_to_add": [] if tags_to_add is None else tags_to_add.split(","),
                    "tags_to_remove": [] if tags_to_remove is None else tags_to_remove.split(","),
                    "is_favorite": is_favorite,
                    "description": "" if description is None else description
                }
            source_configurations_url = url_builder.get_source_details_url(self.client_config)
            source_configurations_url = source_configurations_url + f"/{source_id}/metadata"
            response = IWUtils.ejson_deserialize(self.call_api("PUT", source_configurations_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']), data=data
                                                               ).content)
            result = response.get('result', False)
            if not result:
                self.logger.error(f"Failed to update the source metadata for {source_id}")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to get the source metadata for {source_id}",
                                                   response=response, job_id=None, source_id=source_id)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to update the source metadata for {source_id} " + str(e))

    def get_table_metadata(self, source_id=None, table_id=None):
        """
        Function to get table metadata (like tags,description etc)
        :param table_id: Entity identifier for table
        :type table_id: String
        :param source_id: Entity identifier for source
        :type source_id: String
        :return: response dict
        """
        if None in {source_id, table_id}:
            self.logger.error("source id or table_id cannot be None")
            raise Exception("source id or table_id cannot be None")
        try:
            table_configurations_url = url_builder.get_table_configuration(self.client_config, source_id, table_id)
            table_configurations_url = table_configurations_url + "/metadata"
            response = IWUtils.ejson_deserialize(self.call_api("GET", table_configurations_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get('result', False)
            if not result:
                self.logger.error(f"Failed to get the table metadata for {table_id}")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to get the table metadata for {table_id}",
                                                   response=response, job_id=None, source_id=source_id)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to get the table metadata for {table_id} " + str(e))

    def update_table_metadata(self, source_id, table_id, tags_to_add: str = None, tags_to_remove: str = None,
                              description: str = None, is_favorite=False, data=None):
        """
        Function to update table metadata (like tags,description etc)
        :param table_id: Entity identifier for table
        :type table_id: String
        :param source_id: Entity identifier for source
        :type source_id: String
        :param tags_to_add: Pass comma seperated tags to add. Optional field
        :param tags_to_remove: Pass comma seperated tags to remove. Optional field
        :param description: Pass the description of the tag
        :param is_favorite: Boolean. True/False
        :param data : Json body to update source metadata. You can pass only this param if the body of API is known. Else pass other individual params
        :type data: JSON dict
        :return: response dict
        """
        if None in {source_id, table_id}:
            self.logger.error("source id or table_id cannot be None")
            raise Exception("source id or table_id cannot be None")
        try:
            if data is None:
                data = {
                    "tags_to_add": [] if tags_to_add is None else tags_to_add.split(","),
                    "tags_to_remove": [] if tags_to_remove is None else tags_to_remove.split(","),
                    "is_favorite": is_favorite,
                    "description": "" if description is None else description
                }
            table_configurations_url = url_builder.get_table_configuration(self.client_config, source_id, table_id)
            table_configurations_url = table_configurations_url + "/metadata"
            response = IWUtils.ejson_deserialize(self.call_api("PUT", table_configurations_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']), data=data
                                                               ).content)
            result = response.get('result', False)
            if not result:
                self.logger.error(f"Failed to get the table metadata for {table_id}")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to get the table metadata for {table_id}",
                                                   response=response, job_id=None, source_id=source_id)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to get the table metadata for {table_id} " + str(e))

    def create_query_as_table(self, source_id, tables_to_add_body: list):
        """
        Function to create query as table under source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param tables_to_add_body: Pass the body required to create query as a table
        :type: array of dict
        :return: response dict
        """
        try:
            query_as_table_url = url_builder.get_query_as_table_url(self.client_config, source_id)
            table_payload = {"tables_to_add": tables_to_add_body}
            response = IWUtils.ejson_deserialize(
                self.call_api("POST",
                              query_as_table_url,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              data=table_payload).content,
            )
            if response is not None:
                result = response.get("result", None)
                if result is None:
                    return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                       error_desc="Could not create query as table",
                                                       response=response)
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error(f"Unable to create query as a table under {source_id} " + str(e))
            raise SourceError(f"Unable to create query as a table under {source_id} " + str(e))

    def get_table_schema(self, source_id=None, table_id=None):
        """
        Function to get table schema
        :param table_id: Entity identifier for table
        :type table_id: String
        :param source_id: Entity identifier for source
        :type source_id: String
        :return: response dict
        """
        if None in {source_id, table_id}:
            self.logger.error("source id or table_id cannot be None")
            raise Exception("source id or table_id cannot be None")
        try:
            table_schema_url = url_builder.source_table_schema_url(self.client_config, source_id, table_id)
            response = IWUtils.ejson_deserialize(self.call_api("GET", table_schema_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get('result', False)
            if not result:
                self.logger.error(f"Failed to get the table schema for {table_id}")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to get the table schema for {table_id}",
                                                   response=response, job_id=None, source_id=source_id)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to get the table schema for {table_id} " + str(e))

    def update_table_schema(self, source_id=None, table_id=None, config_body=None):
        """
        Function to update the table schema
        :param table_id: Entity identifier for table
        :type table_id: String
        :param source_id: Entity identifier for source
        :type source_id: String
        :param config_body: JSON body
        config_body_example: {
            "type" : "column",
            "columns": [
                    {
                        "is_deleted": false,
                        "name": "STATE_ID",
                        "original_name": "STATE_ID",
                        "target_sql_type": 12,
                        "target_scale": 0,
                        "col_size": 11,
                        "target_precision": 10
                    }
            ]
        }
        :return: response dict
        """
        if None in {source_id, table_id} or config_body is None:
            self.logger.error("source id or table_id or config_body cannot be None")
            raise Exception("source id or table_id or config_body cannot be None")
        try:
            table_schema_url = url_builder.source_table_schema_url(self.client_config, source_id, table_id)
            response = IWUtils.ejson_deserialize(self.call_api("PATCH", table_schema_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']), data=config_body
                                                               ).content)
            result = response.get('result', {})
            if "id" not in result:
                self.logger.error(f"Failed to update the table schema for {table_id}")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to update the table schema for {table_id}",
                                                   response=response, job_id=None, source_id=source_id)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to update the table schema for {table_id} " + str(e))

    def get_file_mappings_for_json_source(self, source_id=None, file_mapping_id=None, file_mapping_name=None,
                                          params=None):
        """
        Function to get file mappings
        :param source_id: Entity identifier for source
        :type source_id: String
        :param file_mapping_id: Entity identifier of the file mapping
        :param file_mapping_name: Name of the file mapping for which details are to be fetched
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :return: response dict
        """
        if None in {source_id}:
            self.logger.error("source id cannot be None")
            raise Exception("source id cannot be None")

        if params is None:
            params = {"limit": 20, "offset": 0}

        try:
            if file_mapping_id is None:
                if file_mapping_name is not None:
                    params = {"filter": {"name": file_mapping_name}}
                file_mappings_url = url_builder.get_file_mappings_for_json_source_url(self.client_config,
                                                                                      source_id) + IWUtils.get_query_params_string_from_dict(
                    params=params)
                file_mappings_list = []
                response = IWUtils.ejson_deserialize(
                    self.call_api("GET", file_mappings_url,
                                  IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
                if response is not None:
                    initial_msg = response.get("message", "")
                    result = response.get("result", [])
                    while len(result) > 0:
                        file_mappings_list.extend(result)
                        nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                        response = IWUtils.ejson_deserialize(
                            self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                                self.client_config['bearer_token'])).content)
                        result = response.get("result", [])
                else:
                    self.logger.error("Failed to get list of file mappings")
                    return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                       error_desc="Failed to get list of file mappings",
                                                       response=response)
                response["result"] = file_mappings_list
                response["message"] = initial_msg
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                file_mappings_url = url_builder.get_file_mappings_for_json_source_url(self.client_config,
                                                                                      source_id) + f"/{file_mapping_id}"
                response = IWUtils.ejson_deserialize(self.call_api("GET", file_mappings_url,
                                                                   IWUtils.get_default_header_for_v3(
                                                                       self.client_config['bearer_token']),
                                                                   ).content)
                result = response.get('result', False)
                if not result:
                    self.logger.error(f"Failed to get file mappings")
                    return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                       error_desc=f"Failed to get file mappings",
                                                       response=response, job_id=None, source_id=source_id)
                else:
                    return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response,
                                                       source_id=source_id)
        except Exception as e:
            self.logger.error("Error in getting file mappings")
            raise SourceError("Error in getting file mappings " + str(e))

    def modify_file_mappings_for_json_source(self, source_id=None, config_body=None, action_type="create",
                                             file_mappings_id=None):
        """
        Function to create file mappings
        :param source_id: Entity identifier for source
        :type source_id: String
        :param file_mappings_id: Entity identifier of the file mapping. Pass this incase you want to update the file mapping
        :param action_type: create/update
        :param config_body: JSON dict with necessary keys
        ```
        config_body_example =
            {       "name" :"xyz",
                "source_relative_path": "/test",
                "record_scope": "file",
                "include_subdirectories": true,
                "character_encoding": "UTF-8",
                "include_filename_regex": ".*",
                "exclude_filename_regex": ""
            }
        ```
        :return: response dict
        """
        if None in {source_id} or config_body is None:
            self.logger.error("source id or config_body cannot be None")
            raise Exception("source id or config_body cannot be None")
        try:
            if action_type.lower() == "create":
                request_type = "POST"
                file_mappings_url = url_builder.get_file_mappings_for_json_source_url(self.client_config, source_id)
            else:
                request_type = "PATCH"
                file_mappings_url = url_builder.get_file_mappings_for_json_source_url(self.client_config,
                                                                                      source_id) + f"/{file_mappings_id}"

            response = IWUtils.ejson_deserialize(self.call_api(request_type, file_mappings_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               data=config_body).content)
            result = response.get('result', {})
            message = response.get('message', "")
            created_file_mapping_id = result.get('id', None)
            if created_file_mapping_id is not None:
                adv_config_id = str(created_file_mapping_id)
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response,
                                                   source_id=source_id)
            elif message == "Updated File mappings":
                self.logger.info('File Mappings has been updated')
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                self.logger.error(f'Failed to {action_type} file mappings')
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f'Failed to {action_type} file mappings',
                                                   response=response)
        except Exception as e:
            raise SourceError(f"Failed to {action_type} the file mappings for {source_id} " + str(e))

    def delete_file_mappings_for_json_source(self, source_id=None, file_mapping_id=None):
        """
        Function to get table schema
        :param file_mapping_id: Entity identifier of the file mapping
        :param source_id: Entity identifier for source
        :type source_id: String
        :return: response dict
        """
        if None in {source_id, file_mapping_id}:
            self.logger.error("source id or file_mapping_id cannot be None")
            raise Exception("source id or file_mapping_id cannot be None")
        try:
            file_mappings_url = url_builder.get_file_mappings_for_json_source_url(self.client_config,
                                                                                  source_id) + f"/{file_mapping_id}"
            response = IWUtils.ejson_deserialize(self.call_api("DELETE", file_mappings_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get("result", None)
            if result is None:
                self.logger.error(f"Failed to delete the file mappings for {file_mapping_id}")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to delete the file mappings for {file_mapping_id}",
                                                   response=response, job_id=None, source_id=source_id)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to delete the file mappings for {file_mapping_id} " + str(e))

    def crawl_schema_for_file_mapping_json_source(self, source_id=None, file_mapping_id=None):
        """
        Function to get table schema
        :param file_mapping_id: Entity identifier for file mapping
        :type file_mapping_id: String
        :param source_id: Entity identifier for source
        :type source_id: String
        :return: response dict
        """
        if None in {source_id, file_mapping_id}:
            self.logger.error("source id or file_mapping_id cannot be None")
            raise Exception("source id or file_mapping_id cannot be None")
        try:
            crawl_schema_url = url_builder.get_file_mappings_for_json_source_url(self.client_config,
                                                                                 source_id) + f"/{file_mapping_id}/crawl-schema"
            response = IWUtils.ejson_deserialize(self.call_api("GET", crawl_schema_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get('result', False)
            if not result:
                self.logger.error(f"Failed to get the crawl schema for file mapping {file_mapping_id}")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to get the crawl schema for file mapping {file_mapping_id}",
                                                   response=response, job_id=None, source_id=source_id)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to get the crawl schema for file mapping {file_mapping_id} " + str(e))

    def get_table_schema_from_file_mapping_json_source(self, source_id=None, file_mapping_id=None):
        """
        Function to get table schema
        :param file_mapping_id: Entity identifier for file mapping
        :type file_mapping_id: String
        :param source_id: Entity identifier for source
        :type source_id: String
        :return: response dict
        """
        if None in {source_id, file_mapping_id}:
            self.logger.error("source id or file_mapping_id cannot be None")
            raise Exception("source id or file_mapping_id cannot be None")
        try:
            table_schema_url = url_builder.get_file_mappings_for_json_source_url(self.client_config,
                                                                                 source_id) + f"/{file_mapping_id}/table-schema"
            response = IWUtils.ejson_deserialize(self.call_api("GET", table_schema_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get('result', False)
            if not result:
                self.logger.error(f"Failed to get the table schema from file mapping {file_mapping_id}")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to get the table schema from file mapping {file_mapping_id}",
                                                   response=response, job_id=None, source_id=source_id)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to get the table schema from file mapping {file_mapping_id} " + str(e))

    def create_json_source_table(self, source_id=None, file_mapping_id=None):
        """
        Function to create Infoworks JSON Source Table
        :param file_mapping_id: Entity identifier for file mapping
        :type file_mapping_id: String
        :param source_id: Entity identifier for source
        :type source_id: String
        :return: response dict
        """
        response = None
        if None in {source_id, file_mapping_id}:
            self.logger.error("source id or file_mapping_id cannot be None")
            raise Exception("source id or file_mapping_id cannot be None")
        try:
            table_create_url = url_builder.get_file_mappings_for_json_source_url(self.client_config,
                                                                                 source_id) + f"/{file_mapping_id}/tables"
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", table_create_url,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              "").content)

            result = response.get('result', {})
            table_id = result.get('id', None)
            if table_id is None:
                self.logger.error('Table creation failed')
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_desc='Table creation failed',
                                                   response=response)

            table_id = str(table_id)
            self.logger.info('Table {id} has been created.'.format(id=table_id))
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, source_id=source_id, response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to create a json table.')
            raise SourceError(response.get("message", "Error occurred while trying to create a json table."))

    def get_json_source_table(self, source_id=None, file_mapping_id=None, table_id=None, params=None):
        """
        Function to get json source table details
        :param file_mapping_id: Entity identifier for file mapping
        :type file_mapping_id: String
        :param source_id: Entity identifier for source
        :type source_id: String
        :param table_id: Entity identifier for table
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :return: response dict
        """
        if None in {source_id, file_mapping_id}:
            self.logger.error("source id or file_mapping_id cannot be None")
            raise Exception("source id or file_mapping_id cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        try:
            if table_id is None:
                get_json_src_tbls_url = url_builder.get_file_mappings_for_json_source_url(self.client_config,
                                                                                          source_id) + IWUtils.get_query_params_string_from_dict(
                    params=params)
                tables_list = []
                response = IWUtils.ejson_deserialize(
                    self.call_api("GET", get_json_src_tbls_url,
                                  IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
                if response is not None:
                    initial_msg = response.get("message", "")
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
                else:
                    self.logger.error("Failed to get list of json source tables")
                    return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                       error_desc="Failed to get list of json source tables",
                                                       response=response)
                response["result"] = tables_list
                response["message"] = initial_msg
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)

            else:
                get_json_src_tbl_url = url_builder.get_file_mappings_for_json_source_url(self.client_config,
                                                                                         source_id) + f"/{file_mapping_id}/tables/{table_id}"
                response = IWUtils.ejson_deserialize(self.call_api("GET", get_json_src_tbl_url,
                                                                   IWUtils.get_default_header_for_v3(
                                                                       self.client_config['bearer_token']),
                                                                   ).content)
                result = response.get('result', False)
                if not result:
                    self.logger.error(f"Failed to get the table details for {table_id}")
                    return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                       error_desc=f"Failed to get the table details for {table_id}",
                                                       response=response, job_id=None, source_id=source_id)
                else:
                    return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response,
                                                       source_id=source_id)
        except Exception as e:
            self.logger.error(f"Failed to get the table details for {table_id} " + str(e))
            raise SourceError(f"Failed to get the table details for {table_id} " + str(e))

    def delete_json_source_table(self, source_id=None, file_mapping_id=None, table_id=None):
        """
        Function to delete json source table
        :param file_mapping_id: Entity identifier for file mapping
        :type file_mapping_id: String
        :param source_id: Entity identifier for source
        :type source_id: String
        :param table_id: Entity identifier for table
        :return: response dict
        """
        if None in {source_id, file_mapping_id, table_id}:
            self.logger.error("source id or file_mapping_id or table_id cannot be None")
            raise Exception("source id or file_mapping_id or table_id cannot be None")
        try:
            get_json_src_tbl_url = url_builder.get_file_mappings_for_json_source_url(self.client_config,
                                                                                     source_id) + f"/{file_mapping_id}/tables/{table_id}"
            response = IWUtils.ejson_deserialize(self.call_api("DELETE", get_json_src_tbl_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            message = response.get('message', "")
            if message != "Table Deleted Successfully":
                self.logger.error(f"Failed to delete the json source table {table_id}")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to delete the json source table {table_id}",
                                                   response=response, job_id=None, source_id=source_id)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to delete the json source table {table_id} " + str(e))

    def get_source_audit_logs(self, source_id=None, params=None):
        """
        Function to get audit logs of source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        audit_logs = []
        if None in {source_id}:
            self.logger.error("source id cannot be None")
            raise Exception("source id cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        try:
            src_audit_logs_url = url_builder.get_source_audit_logs_url(self.client_config, source_id)
            src_audit_logs_url = src_audit_logs_url + IWUtils.get_query_params_string_from_dict(params=params)
            response = IWUtils.ejson_deserialize(self.call_api("GET", src_audit_logs_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            initial_msg = response.get("message", "")
            if response is not None:
                result = response.get("result", [])
                while len(result) > 0:
                    audit_logs.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", [])
            else:
                self.logger.error("Failed to get audit logs of source")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc="Failed to get audit logs of source", response=response)
            response["result"] = audit_logs
            response["message"] = initial_msg
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response, source_id=source_id)
        except Exception as e:
            self.logger.error(f"Failed to get the audit logs of {source_id} " + str(e))
            raise SourceError(f"Failed to get the audit logs of {source_id} " + str(e))

    def get_table_audit_logs(self, source_id=None, table_id=None, params=None):
        """
        Function to get audit logs of source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param table_id: Entity identifier of table
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        audit_logs = []
        if None in {source_id, table_id}:
            self.logger.error("source id or table_id cannot be None")
            raise Exception("source id or table_id cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        try:
            tbl_audit_logs_url = url_builder.get_table_audit_logs_url(self.client_config, source_id, table_id)
            tbl_audit_logs_url = tbl_audit_logs_url + IWUtils.get_query_params_string_from_dict(params=params)
            response = IWUtils.ejson_deserialize(self.call_api("GET", tbl_audit_logs_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            initial_msg = response.get("message", "")
            if response is not None:
                result = response.get("result", [])
                while len(result) > 0:
                    audit_logs.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", [])
            else:
                self.logger.error("Failed to get audit logs of table")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc="Failed to get audit logs of table", response=response)
            response["result"] = audit_logs
            response["message"] = initial_msg
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response, source_id=source_id)
        except Exception as e:
            self.logger.error(f"Failed to get the audit logs of table {table_id} " + str(e))
            raise SourceError(f"Failed to get the audit logs of table {table_id} " + str(e))

    def get_tablegroup_audit_logs(self, source_id=None, table_group_id=None, params=None):
        """
        Function to get audit logs of source
        :param source_id: Entity identifier for source
        :type source_id: String
        :param table_group_id: Entity identifier of table group
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        audit_logs = []
        if None in {source_id, table_group_id}:
            self.logger.error("source id or table_group_id cannot be None")
            raise Exception("source id or table_group_id cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        try:
            tblgrp_audit_logs_url = url_builder.get_table_group_audit_logs_url(self.client_config, source_id,
                                                                               table_group_id)
            tblgrp_audit_logs_url = tblgrp_audit_logs_url + IWUtils.get_query_params_string_from_dict(params=params)
            response = IWUtils.ejson_deserialize(self.call_api("GET", tblgrp_audit_logs_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            initial_msg = response.get("message", "")
            if response is not None:
                result = response.get("result", [])
                while len(result) > 0:
                    audit_logs.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", [])
            else:
                self.logger.error("Failed to get audit logs of table group")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc="Failed to get audit logs of table group",
                                                   response=response)
            response["result"] = audit_logs
            response["message"] = initial_msg
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response, source_id=source_id)
        except Exception as e:
            self.logger.error(f"Failed to get the audit logs of table group {table_group_id} " + str(e))
            raise SourceError(f"Failed to get the audit logs of table group {table_group_id} " + str(e))

    def get_list_of_advanced_config_of_table(self, source_id=None, table_id=None, params=None):
        """
            Function to get list of Advance Config of the table
            :param source_id: Entity identifier of the source
            :param table_id: Entity identifier of the table
            :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
            :type: JSON dict
            :return: response dict
        """
        if None in {source_id, table_id}:
            raise Exception(f"source_id, table_id cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_get_table_adv_config = url_builder.table_advanced_base_url(self.client_config, source_id,
                                                                          table_id) + IWUtils.get_query_params_string_from_dict(
            params=params)
        adv_config_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_table_adv_config,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                initial_msg = response.get("message", "")
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
                    result = response.get("result", None)
                    if result is None:
                        return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                           error_code=ErrorCode.GENERIC_ERROR,
                                                           error_desc="Error in listing adv config of table",
                                                           response=response
                                                           )

                response["result"] = adv_config_list
                response["message"] = initial_msg
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing adv config of table")
            raise SourceError("Error in listing adv config of table" + str(e))

    def modify_advanced_config_of_table(self, source_id=None, table_id=None, adv_config_body=None,
                                        action_type="update", key=None):
        """
        Function to add/update the adv config for the table
        :param source_id: Entity identifier of the source
        :param table_id: Entity identifier of the table
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
        if None in {source_id, table_id} or adv_config_body is None:
            raise Exception(f"source_id, table_id and adv_config_body cannot be None")
        try:
            if action_type.lower() == "create":
                request_type = "POST"
                request_url = url_builder.table_advanced_base_url(self.client_config, source_id,
                                                                  table_id)
            else:
                request_type = "PUT"
                request_url = url_builder.table_advanced_base_url(self.client_config, source_id,
                                                                  table_id) + f"/{key}"
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
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                self.logger.error(f'Failed to {action_type} advanced config for table.')
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f'Failed to {action_type} advanced config for table',
                                                   response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to add/update adv config for table')
            raise SourceError('Error occurred while trying to add/update adv config for table')

    def get_or_delete_advance_config_of_table(self, source_id=None, table_id=None, key=None,
                                              action_type="get"):
        """
        Gets/Deletes advance configuration of pipeline group
        :param source_id: Entity identifier of the source
        :param table_id: Entity identifier of the table
        :param key: name of the advanced configuration
        :param action_type: values can be get/delete
        :return: response dict
        """
        try:
            if None in {source_id, table_id, key}:
                raise Exception(f"source_id, table_id, key cannot be None")
            request_type = "GET" if action_type.lower() == "get" else "DELETE"
            response = IWUtils.ejson_deserialize(
                self.call_api(request_type, url_builder.table_advanced_base_url(
                    self.client_config, source_id, table_id) + f"{key}", IWUtils.get_default_header_for_v3(
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
            raise SourceError('Error occurred while trying to get/delete adv config details.')

    def update_table_configurations(self, source_id=None, table_id=None, config_body=None):
        """
        Function to update the table configurations including columns and ingestion configurations
        :param table_id: Entity identifier for table
        :type table_id: String
        :param source_id: Entity identifier for source
        :type source_id: String
        :param config_body: JSON body
        ```
        config_body_example: {
            "id": "9376bf97a286e35efe86d321",
            "name": "dim_state",
            "original_table_name": "dim_state",
            "data_lake_path": "/iw/sources/customer_360_sql_server_schema/9376bf97a286e35efe86d321",
            "meta_crawl_performed": true,
            "columns": [
                {
                    "sql_type": 4,
                    "is_deleted": false,
                    "name": "STATE_ID",
                    "original_name": "STATE_ID",
                    "target_sql_type": 12,
                    "is_audit_column": false,
                    "col_size": 11,
                    "precision": 10,
                    "target_precision": 10,
                    "scale": 0
                },
                {
                    "sql_type": 93,
                    "is_deleted": false,
                    "name": "ziw_target_timestamp",
                    "original_name": "ziw_target_timestamp",
                    "target_sql_type": 93,
                    "is_audit_column": true,
                    "target_scale": "6",
                    "precision": 0,
                    "target_precision": "0",
                    "scale": 6,
                    "format": "yyyy-MM-dd HH:mm:ss"
                },
                {
                    "sql_type": 16,
                    "is_deleted": false,
                    "name": "ziw_is_deleted",
                    "original_name": "ziw_is_deleted",
                    "target_sql_type": 16,
                    "is_audit_column": true,
                    "target_scale": "0",
                    "precision": 0,
                    "target_precision": "0",
                    "scale": 0
                }
            ],
            "configuration": {
                "exclude_legacy_audit_columns": true,
                "sync_type": "full-load",
                "write_supported_engines": [
                    "SNOWFLAKE",
                    "SPARK"
                ],
                "read_supported_engines": [
                    "SNOWFLAKE",
                    "SPARK"
                ],
                "target_table_name": "DIM_STATE",
                "storage_format": "delta",
                "target_schema_name": "CUSTOMER_360",
                "is_table_case_sensitive": false,
                "is_schema_case_sensitive": false,
                "target_database_name": "CUSTOMER_360",
                "is_database_case_sensitive": false,
                "is_scd2_table": false,
                "is_archive_enabled": false,
                "natural_keys": [],
                "generate_history_view": false,
                "segmented_load_columns": [],
                "segmentation_status": "disabled"
            },
            "source": "1f9dd7b2bc9f656a4743f458",
            "last_ingested_cdc_value": null,
            "is_jtds_driver": false,
            "schema_name_at_source": "dbo",
            "catalog_name": "customer_360",
            "catalog_is_database": true,
            "has_catalog": true
        }
        ```
        :return: response dict
        """
        if None in {source_id, table_id} or config_body is None:
            self.logger.error("source id or table_id or config_body cannot be None")
            raise Exception("source id or table_id or config_body cannot be None")
        try:
            table_config_url = url_builder.get_table_configuration(self.client_config, source_id, table_id)
            response = IWUtils.ejson_deserialize(self.call_api("PUT", table_config_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']), data=config_body
                                                               ).content)
            result = response.get('result', {})
            if "id" not in result:
                self.logger.error(f"Failed to update the table configurations for {table_id}")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to update the table configurations for {table_id}",
                                                   response=response, job_id=None, source_id=source_id)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to update the table configurations for {table_id} " + str(e))

    def get_source_job_details(self, source_id=None, job_id=None):
        """
        Function to get the source job details
        :param job_id: Entity identifier for the job
        :type job_id: String
        :param source_id: Entity identifier for source
        :type source_id: String
        :return: response dict
        """
        if None in {source_id, job_id}:
            self.logger.error("source id or job_id cannot be None")
            raise Exception("source id or job_id cannot be None")
        try:
            source_job_url = url_builder.get_source_job_url(self.client_config, source_id, job_id)
            response = IWUtils.ejson_deserialize(self.call_api("GET", source_job_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get('result', False)
            if not result:
                self.logger.error(f"Failed to get the job details for {job_id}")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to get the job details for {job_id}",
                                                   response=response, job_id=None, source_id=source_id)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to get the job details for {job_id} " + str(e))

    def get_source_crawl_job_summary(self, source_id=None, job_id=None, num_lines=10000):
        """
        Function to get source job crawl summary
        :param source_id: Entity identifier for source
        :param job_id: Entity identifier for job
        :param num_lines: Default is 10000
        :return: response dict
        """
        try:
            if None in {source_id, job_id}:
                self.logger.error("source id or job_id cannot be None")
                raise Exception("source id or job_id cannot be None")
            source_crawl_job_summary_url = url_builder.get_crawl_job_summary_url(self.client_config,
                                                                                 source_id,
                                                                                 job_id) + f"?num_lines={num_lines}"
            response = IWUtils.ejson_deserialize(self.call_api("GET", source_crawl_job_summary_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get('result', None)
            if not result:
                self.logger.error(f"Failed to get the source job crawl summary fro job {job_id} ")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to get source job crawl summary for job {job_id} ",
                                                   response=response, job_id=None, source_id=None)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            raise SourceError(f"Failed to get source job crawl summary for job {job_id} " + str(e))

    def get_source_details(self, source_id=None):
        """
        Function to get source details
        :param source_id: Entity identifier for source
        :type source_id: String
        :return: response dict
        """
        if None in {source_id}:
            self.logger.error("source id cannot be None")
            raise Exception("source id cannot be None")
        try:
            source_configurations_url = url_builder.get_source_details_url(self.client_config) + f"/{source_id}"
            response = IWUtils.ejson_deserialize(self.call_api("GET", source_configurations_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get('result', False)
            if not result:
                self.logger.error(f"Failed to get the source details for {source_id}")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f"Failed to get the source details for {source_id}",
                                                   response=response, job_id=None, source_id=source_id)
            else:
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response,
                                                   source_id=source_id)
        except Exception as e:
            raise SourceError(f"Failed to get the source details for {source_id} " + str(e))

    def modify_data_validation_spec_of_table(self, source_id=None, table_id=None, config_body=None,
                                             action_type="update", validation_spec_id=None):
        """
        Function to add/update the validation spec for the table
        :param source_id: Entity identifier of the source
        :param table_id: Entity identifier of the table
        :param action_type: values can be either create/update. default update
        :type action_type: String
        :param config_body: JSON dict
        ```
        config_body = {
            "name": "test2",
            "column_name": "CUSTOMERID",
            "table_id": "64df051587f3520007fa84c0",
            "type": "regex",
            "expression": "[a-zA-Z]"
        }
        ```
        :param validation_spec_id: In case of update, pass the entity identifier of the validation_spec. This can be found from list_validation_specs_for_table function
        :return: response dict
        """
        if None in {source_id, table_id} or config_body is None:
            raise Exception(f"source_id, table_id and config_body cannot be None")
        try:
            if action_type.lower() == "create":
                request_type = "POST"
                request_url = url_builder.get_table_configuration(self.client_config, source_id,
                                                                  table_id) + "/validation-specs"
            else:
                request_type = "PATCH"
                request_url = url_builder.get_table_configuration(self.client_config, source_id,
                                                                  table_id) + f"/validation-specs/{validation_spec_id}"
            response = IWUtils.ejson_deserialize(
                self.call_api(request_type, request_url,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              config_body).content)
            validation_spec_id = response.get('result', None)
            message = response.get('message', "")
            if validation_spec_id is not None:
                validation_spec_id = str(validation_spec_id)
                self.logger.info(
                    'Validation Spec has been created {id}.'.format(id=validation_spec_id))
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, entity_id=validation_spec_id,
                                                    response=response)
            elif message == "Validation Specs updated successfully":
                self.logger.info('Validation Specs updated successfully')
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                self.logger.error(f'Failed to {action_type} validation spec for table')
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f'Failed to {action_type} validation spec for table',
                                                   response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to add/update validation spec for table')
            raise SourceError('Error occurred while trying to add/update validation spec for table')

    def list_validation_specs_for_table(self, source_id=None, table_id=None, params=None):
        """
        Function to list the validation specs for a table
        :param source_id: Entity identifier of the source
        :param table_id: Entity identifier of the table
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if None in {source_id, table_id}:
            raise Exception(f"source_id, table_id cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_validation_specs = url_builder.get_table_configuration(self.client_config, source_id,
                                                                           table_id) + "/validation-specs" + IWUtils.get_query_params_string_from_dict(
            params=params)
        validations_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_validation_specs,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                initial_msg = response.get("message", "")
                result = response.get("result", [])
                while len(result) > 0:
                    validations_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", [])
            else:
                self.logger.error("Failed to get list of validations for table")
                return SourceResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc="Failed to get list of validations for table",
                                                   response=response)
            response["result"] = validations_list
            response["message"] = initial_msg
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)

        except Exception as e:
            self.logger.error("Error in listing validations for table")
            raise SourceError("Error in listing validations for table " + str(e))

    def delete_validation_spec_table(self, source_id=None, table_id=None, validation_spec_id=None):
        """
        Function to delete validation spec for a table
        :param source_id: Entity identifier for source
        :param table_id: Entity identifier for table
        :param validation_spec_id: Entity identifier of  validation spec to delete. Can be found using the list_validation_specs_for_table function
        :return: response dict
        """
        if None in {source_id, table_id, validation_spec_id}:
            self.logger.error("source id or table_id or validation_spec_id cannot be None")
            raise Exception("source id or table_id or validation_spec_id cannot be None")
        url_to_delete_validation_spec = url_builder.get_table_configuration(self.client_config, source_id,
                                                                            table_id) + f"/validation-specs/{validation_spec_id}"
        try:
            response = IWUtils.ejson_deserialize(self.call_api("DELETE",
                                                               url_to_delete_validation_spec,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token'])).content)
            result = response.get("message", "None")

            if result != "Validation Specs deleted successfully":
                self.logger.error(f'Failed to delete validation spec of the table {table_id}')
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.USER_ERROR,
                                                   error_desc=f'Failed to delete validation spec of the table {table_id}',
                                                   response=response)
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, response=response)

        except Exception as e:
            self.logger.error("Error in deleting the validation spec of the table")
            raise SourceError("Error in deleting the validation spec of the table" + str(e))

    def configure_table_group_schedule_user(self, source_id, table_group_id):
        """
        Configure Table Group schedule user of a particular Table Group belonging to the Source
        :param source_id: Source ID of which Table Group belongs
        :type source_id: String
        :param table_group_id: Table Group ID to fetch schedule for.
        :type table_group_id: String
        :return: Response Dict
        """
        if source_id is None or table_group_id is None:
            self.logger.error("source_id or table_group_id cannot be None")
            raise Exception("source_id or table_group_id cannot be None")
        response = None
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("PUT", url_builder.configure_table_group_schedule_user_url(
                    self.client_config, source_id, table_group_id), IWUtils.get_default_header_for_v3(
                    self.client_config['bearer_token'])).content)

            result = response.get('result', None)

            if result is None:
                self.logger.error('Failed to configure table group schedule user')
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.USER_ERROR,
                                                   error_desc='Failed to configure table group schedule user',
                                                   response=response)

            table_group_id = str(table_group_id)
            self.logger.info(
                'Successfully configured table group {id} schedule user.'.format(id=table_group_id))
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, source_id=source_id,
                                               response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to configure table group schedule user.')
            raise SourceError('Error occurred while trying to configure table group schedule user.')

    def enable_table_group_schedule(self, source_id, table_group_id, schedule_config):
        """
        Enables Schedule of a particular table group belonging to the source
        :param source_id: Source ID of which table group belongs
        :type source_id: String
        :param table_group_id: Table Group ID to fetch schedule for.
        :type table_group_id: String
        :param schedule_config: Schedule Configuration JSON of the Table Group
        :type schedule_config: JSON
        ```
        schedule_config_example = {
              "start_date": "02/22/2020",
              "end_date": "02/24/2020",
              "start_hour": 12,
              "start_min": 25,
              "end_hour": 17,
              "end_min": 30,
              "repeat_interval_measure": 2,
              "repeat_interval_unit": "{string}",
              "ends": true,
              "is_custom_job": true,
              "custom_job_details": {
                "starts_daily_at": "14:00",
                "ends_daily_at": "15:00",
                "repeat_interval_unit": "{string}",
                "repeat_interval_measure": 2
              },
              "repeat_on_last_day": "{boolean}",
              "specified_days": 1
        }
        ```
        :return: Response Dict
        """
        if source_id is None or table_group_id is None or schedule_config is None:
            self.logger.error("source_id or table_group_id or schedule_config cannot be None")
            raise Exception("source_id or table_group_id or schedule_config cannot be None")
        response = None
        try:
            response = IWUtils.ejson_deserialize(self.call_api("PUT", url_builder.get_enable_table_group_schedule_url(
                self.client_config, source_id, table_group_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']), schedule_config).content)

            result = response.get('result', None)

            if result is None:
                self.logger.error('Failed to enable schedule of the table group')
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.USER_ERROR,
                                                   error_desc='Failed to enable schedule of the table group',
                                                   response=response)

            table_group_id = str(table_group_id)
            self.logger.info(
                'Successfully enabled schedule for the table group {id}.'.format(id=table_group_id))
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, source_id=source_id,
                                               response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to enable table group schedule.')
            raise SourceError('Error occurred while trying to enable table group schedule.')

    def disable_table_group_schedule(self, source_id, table_group_id):
        """
        Disables Schedule of a particular Table Group belonging to the Source
        :param source_id: Source ID of which Table Group belongs
        :type source_id: String
        :param table_group_id: Table Group ID to fetch schedule for.
        :type table_group_id: String
        :return: Response Dict
        """
        if source_id is None or table_group_id is None:
            self.logger.error("source_id or table_group_id cannot be None")
            raise Exception("source_id or table_group_id cannot be None")
        response = None
        try:
            response = IWUtils.ejson_deserialize(self.call_api("PUT", url_builder.get_disable_table_group_schedule_url(
                self.client_config, source_id, table_group_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            result = response.get('result', None)

            if result is None:
                self.logger.error('Failed to disable schedule of the table group')
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   error_code=ErrorCode.USER_ERROR,
                                                   error_desc='Failed to disable schedule of the table group',
                                                   response=response)

            table_group_id = str(table_group_id)
            self.logger.info(
                'Successfully disabled schedule for the table group {id}.'.format(id=table_group_id))
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, source_id=source_id,
                                               response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to disable table group schedule.')
            raise SourceError('Error occurred while trying to disable table group schedule.')

    def get_table_group_schedule(self, source_id, table_group_id):
        """
        Gets Schedules of particular Table Group belonging to the Source
        :param source_id: Table Group ID of which Source belongs
        :type source_id: String
        :param table_group_id: Table Group ID to fetch schedule for.
        :type table_group_id: String
        :return: Response Dict
        """
        if source_id is None or table_group_id is None:
            self.logger.error("source_id or table_group_id cannot be None")
            raise Exception("source_id or table_group_id cannot be None")

        response = None

        try:
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.get_table_group_schedule_url(
                self.client_config, source_id, table_group_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            return SourceResponse.parse_result(status=Response.Status.SUCCESS, source_id=source_id,
                                               response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get table group schedule.')
            raise SourceError('Error occurred while trying to get table group schedule.')
