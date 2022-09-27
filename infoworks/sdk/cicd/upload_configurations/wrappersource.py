import traceback

from infoworks.sdk import url_builder
from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.cicd.upload_configurations.cdata_source import CdataSource
from infoworks.sdk.cicd.upload_configurations.csv_source import CSVSource
from infoworks.sdk.cicd.upload_configurations.rdbms_source import RDBMSSource
from infoworks.sdk.cicd.upload_configurations.salesforce_source import SalesforceSource
from infoworks.sdk.generic_response import GenericResponse
from infoworks.sdk.utils import IWUtils
from infoworks.sdk.local_configurations import Response
import os.path
import queue
import threading


class WrapperSource(BaseClient):
    def __init__(self):
        super().__init__()

    def __wrapper_get_environment_details(self, environment_id=None, params=None):
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
                result = response.get("result", [])
                if environment_id is not None:
                    env_details.extend(result)
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
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=env_details)
        except Exception as e:
            self.logger.error("Error in getting environment details")

    def __wrapper_get_storage_details(self, environment_id, storage_id=None, params=None):
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
                result = response.get("result", [])
                if storage_id is not None:
                    storage_details.extend(result)
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
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=storage_details)
        except Exception as e:
            self.logger.error("Error in getting storage details")

    def cicd_upload_source_configurations(self, configuration_file_path, override_configuration_file=None,
                                          export_lookup=False, replace_words="", read_passwords_from_secrets=False,
                                          env_tag="", secret_type=""):
        """
        Function to create and configure source using the source configuration JSON file
        :param configuration_file_path: Path of the file with source configurations to be imported
        :param override_configuration_file: Path of the file with override keys
        :param export_lookup: True/False. True if for each table in the source export, override keys have to be looked up from override_configuration_file and passwords from secrets
        :param replace_words: Pass the strings to be replaced in the configuration file. Example: DEV->PROD;dev->prod
        :param read_passwords_from_secrets: True/False. If True all the source related passwords are read from encrypted file name passed
        """
        try:
            env_id = self.client_config.get("default_environment_id", None)
            storage_id = self.client_config.get("default_storage_id", None)
            with open(configuration_file_path, 'r') as file:
                json_string = file.read()
            configuration_obj = IWUtils.ejson_deserialize(json_string)
            environment_configurations = configuration_obj["environment_configurations"]
            if env_id is None and "environment_mappings" in self.mappings:
                env_name = self.mappings["environment_mappings"].get(environment_configurations["environment_name"],
                                                                     environment_configurations["environment_name"])
                if env_name is not None:
                    result = self.__wrapper_get_environment_details(params={"filter": {"name": env_name}})
                    env_id = result["result"]["response"][0]["id"] if len(result["result"]["response"]) > 0 else None
            if storage_id is None and "storage_mappings" in self.mappings:
                storage_name = self.mappings["storage_mappings"].get(
                    environment_configurations["environment_storage_name"],
                    environment_configurations["environment_storage_name"])
                if storage_name is not None:
                    result = self.__wrapper_get_storage_details(environment_id=env_id,
                                                                params={"filter": {"name": storage_name}})
                    storage_id = result["result"]["response"][0]["id"] if len(result["result"]["response"]) > 0 else None
            if env_id is None or storage_id is None:
                raise Exception("No env id and storage id")

            source_type = configuration_obj["configuration"]["source_configs"]["type"]
            source_sub_type = configuration_obj["configuration"]["source_configs"]["sub_type"]
            if source_type == "file" and source_sub_type == "structured":
                source_obj = CSVSource(env_id, storage_id, configuration_file_path, self.secrets_config, replace_words)
                source_id = source_obj.create_csv_source(self)
                if source_id is not None:
                    # Proceed to configure the source connection details
                    if source_obj.configure_csv_source(self, source_id, self.mappings, read_passwords_from_secrets=read_passwords_from_secrets, env_tag=env_tag, secret_type=secret_type) == "SUCCESS":
                        # Proceed to configure tables and table groups
                        status = source_obj.import_source_configuration(self, source_id, self.mappings,
                                                                        override_configuration_file, export_lookup,
                                                                        read_passwords_from_secrets)
                        if status == "SUCCESS":
                            self.logger.info("Configured source successfully")
            elif source_type == "rdbms" and source_sub_type!="snowflake":
                source_obj = RDBMSSource()
                source_obj.set_variables(env_id, storage_id, configuration_file_path, self.secrets_config,
                                         replace_words)
                source_id = source_obj.create_rdbms_source(self)
                if source_id is not None:
                    # Proceed to configure the source connection details
                    if source_obj.configure_rdbms_source_connection(self, source_id, override_configuration_file,
                                                                    read_passwords_from_secrets=read_passwords_from_secrets,
                                                                    env_tag=env_tag,
                                                                    secret_type=secret_type) == "SUCCESS":
                        status = source_obj.test_source_connection(self, source_id)
                        if status == "SUCCESS":
                            status = source_obj.browse_source_tables(self, source_id)
                            if status == "SUCCESS":
                                status = source_obj.add_tables_to_source(self, source_id)
                                if status == "SUCCESS":
                                    self.logger.info("Added tables to source successfully")
                                status = source_obj.configure_tables_and_tablegroups(self, source_id,
                                                                                     override_configuration_file,
                                                                                     export_lookup, self.mappings,
                                                                                     read_passwords_from_secrets,
                                                                                     env_tag=env_tag,
                                                                                     secret_type=secret_type)
                                if status == "SUCCESS":
                                    self.logger.info("Configured source successfully")
            elif source_type == "crm" and source_sub_type == "salesforce":
                source_obj = SalesforceSource()
                source_obj.set_variables(env_id, storage_id, configuration_file_path, self.secrets_config,
                                         replace_words)
                source_id = source_obj.create_salesforce_source(self)
                if source_id is not None:
                    # Proceed to configure the source connection details
                    if source_obj.configure_salesforce_source_connection(self, source_id, override_configuration_file,
                                                                    read_passwords_from_secrets=read_passwords_from_secrets,
                                                                    env_tag=env_tag,
                                                                    secret_type=secret_type) == "SUCCESS":
                        status = source_obj.test_source_connection(self, source_id)
                        if status == "SUCCESS":
                            status = source_obj.metacrawl_source(self, source_id)
                            if status == "SUCCESS":
                                status = source_obj.configure_tables_and_tablegroups(self, source_id,
                                                                                     override_configuration_file,
                                                                                     export_lookup, self.mappings,
                                                                                     read_passwords_from_secrets,
                                                                                     env_tag=env_tag,
                                                                                     secret_type=secret_type)
                                if status == "SUCCESS":
                                    self.logger.info("Configured source successfully")
            else:    #assumes to be cdata source
                source_obj = CdataSource()
                source_obj.set_variables(env_id, storage_id, configuration_file_path, self.secrets_config,
                                         replace_words)
                source_id = source_obj.create_cdata_source(self)
                if source_id is not None:
                    # Proceed to configure the source connection details
                    if source_obj.configure_cdata_source_connection(self, source_id, override_configuration_file,
                                                                    read_passwords_from_secrets=read_passwords_from_secrets,
                                                                    env_tag=env_tag,
                                                                    secret_type=secret_type) == "SUCCESS":
                        status = source_obj.test_source_connection(self, source_id)
                        if status == "SUCCESS":
                            status = source_obj.browse_source_tables(self, source_id)
                            if status == "SUCCESS":
                                status = source_obj.add_tables_to_source(self, source_id)
                                if status == "SUCCESS":
                                    self.logger.info("Added tables to source successfully")
                                status = source_obj.configure_tables_and_tablegroups(self, source_id,
                                                                                     override_configuration_file,
                                                                                     export_lookup, self.mappings,
                                                                                     read_passwords_from_secrets,
                                                                                     env_tag=env_tag,
                                                                                     secret_type=secret_type)
                                if status == "SUCCESS":
                                    self.logger.info("Configured source successfully")
        except Exception as e:
            self.logger.error(str(e))
            traceback.print_exc()

    def __execute(self, thread_number, q):
        while True:
            try:
                print('%s: Looking for the next task ' % thread_number)
                task = q.get()
                print(f'\nThread Number {thread_number} processed {task}')
                entity_type = task["entity_type"]
                if entity_type == "source":
                    replace_words = task["replace_words"] if task["replace_words"] else ""
                    read_passwords_from_secrets = task.get("read_passwords_from_secrets", False)
                    self.cicd_upload_source_configurations(task["source_config_path"],
                                                           override_configuration_file=None,
                                                           export_lookup=True, replace_words=replace_words,
                                                           read_passwords_from_secrets=read_passwords_from_secrets)

            except Exception as e:
                print(str(e))
            q.task_done()

    def cicd_create_sourceartifacts_in_bulk(self, config_base_path, src_replace_words=None,
                                            read_passwords_from_secrets=False):
        """
        Function to configure sources in bulk
        :param config_base_path: Path with all the source configuration dumps
        :param src_replace_words: Pass the strings to be replaced in the configuration file. Example: DEV->PROD;dev->prod
        :param read_passwords_from_secrets: True/False. If True all the source related passwords are read from encrypted file name passed
        """
        num_fetch_threads = 10
        job_queue = queue.Queue(maxsize=100)
        for i in range(num_fetch_threads):
            worker = threading.Thread(target=self.__execute, args=(i, job_queue,))
            worker.setDaemon(True)
            worker.start()

        with open(os.path.join(config_base_path, "modified_files", "source.csv"), "r") as src_files_fp:
            for src_file in src_files_fp.readlines():
                src_args = {"entity_type": "source",
                            "source_config_path": os.path.join(config_base_path, "source", src_file.strip()),
                            "replace_words": src_replace_words,
                            "read_passwords_from_secrets": read_passwords_from_secrets}
                job_queue.put(src_args)
                # print(src_args)
        print('*** Main thread waiting to complete all source configuration requests ***')
        job_queue.join()
        print('*** Done with Source Configurations ***')
