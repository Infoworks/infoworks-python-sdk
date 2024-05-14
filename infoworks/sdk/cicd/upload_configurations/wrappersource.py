import json
import traceback

from infoworks.sdk import url_builder
from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.cicd.upload_configurations.cdata_source import CdataSource
from infoworks.sdk.cicd.upload_configurations.csv_source import CSVSource
from infoworks.sdk.cicd.upload_configurations.rdbms_source import RDBMSSource
from infoworks.sdk.cicd.upload_configurations.generic_jdbc_source import GenericJDBCSource
from infoworks.sdk.cicd.upload_configurations.salesforce_source import SalesforceSource
from infoworks.sdk.cicd.upload_configurations.streaming_source import StreamingSource
from infoworks.sdk.cicd.upload_configurations.file_source import FileSource
from infoworks.sdk.generic_response import GenericResponse
from infoworks.sdk.utils import IWUtils
from infoworks.sdk.local_configurations import Response
import os.path
import queue
import threading
import pandas as pd
import math

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
            print("Error in getting environment details")
            print(str(e))

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
            print("Error in getting storage details")

    def print_overall_steps_status(self,overall_steps_status):
        overall_steps_status_df = pd.DataFrame(data=overall_steps_status,
                                               columns=["STEP", "OVERALL_STATUS", "RESPONSE"])
        print(overall_steps_status_df.to_markdown())

    def cicd_upload_source_configurations(self, configuration_file_path, override_configuration_file=None,
                                          export_lookup=False, replace_words="", read_passwords_from_secrets=False,
                                          env_tag="", secret_type="",config_ini_path=None):
        """
        Function to create and configure source using the source configuration JSON file
        :param configuration_file_path: Path of the file with source configurations to be imported
        :param override_configuration_file: Path of the file with override keys
        :param export_lookup: True/False. True if for each table in the source export, override keys have to be looked up from override_configuration_file and passwords from secrets
        :param replace_words: Pass the strings to be replaced in the configuration file. Example: DEV->PROD;dev->prod
        :param read_passwords_from_secrets: True/False. If True all the source related passwords are read from encrypted file name passed
        """
        response_to_return ={}
        try:
            env_id = self.client_config.get("default_environment_id", None)
            storage_id = self.client_config.get("default_storage_id", None)
            compute_id = self.client_config.get("default_compute_id", None)
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
                    response_to_return["get_environment_entity_response"] = result
            if storage_id is None and "storage_mappings" in self.mappings:
                storage_name = self.mappings["storage_mappings"].get(
                    environment_configurations["environment_storage_name"],
                    environment_configurations["environment_storage_name"])
                if storage_name is not None:
                    result = self.__wrapper_get_storage_details(environment_id=env_id,
                                                                params={"filter": {"name": storage_name}})
                    storage_id = result["result"]["response"][0]["id"] if len(result["result"]["response"]) > 0 else None
                    response_to_return["get_storage_entity_response"] = result
            if compute_id is None and "compute_mappings" in self.mappings:
                iw_mappings = configuration_obj["configuration"]["iw_mappings"]
                compute_mappings_from_config = dict(self.mappings.get("compute_mappings"))
                for mapping in iw_mappings:
                    if mapping["entity_type"]=="environment_compute_template":
                        mapping["recommendation"]["compute_name"]=compute_mappings_from_config.get(mapping["recommendation"]["compute_name"],mapping["recommendation"]["compute_name"])
            if env_id is None or storage_id is None:
                print("No env id or storage id found")
                raise Exception("No env id or storage id found")
            old_associated_domain_names = configuration_obj["configuration"]["source_configs"].get("associated_domain_names",[])
            new_associated_domain_names = []
            domain_mappings = self.mappings.get("domain_name_mappings", {})
            if domain_mappings != {} and old_associated_domain_names:
                for domain_name in old_associated_domain_names:
                    if domain_name.lower() in domain_mappings.keys():
                        updated_domain_name=domain_mappings.get(domain_name.lower(), domain_name)
                        new_associated_domain_names.append(updated_domain_name)
                configuration_obj["configuration"]["source_configs"]["associated_domain_names"]=new_associated_domain_names
                print("Adding domains:",new_associated_domain_names)
            source_type = configuration_obj["configuration"]["source_configs"]["type"]
            source_sub_type = configuration_obj["configuration"]["source_configs"]["sub_type"]
            if source_type == "file" and (source_sub_type == "structured" or source_sub_type == "fixedwidth"):
                overall_steps_status = []
                source_obj = CSVSource(env_id, storage_id, configuration_file_path, self.secrets_config, replace_words)
                # adding below line to prevent missing out mappings done above (compute_mappings,environment_mappings etc
                source_obj.configuration_obj=configuration_obj
                source_obj.update_mappings_for_configurations(self.mappings)
                create_source_response = source_obj.create_csv_source(self)
                print("create_source_response:",create_source_response)
                source_id=create_source_response["result"]["source_id"]
                if source_id is not None:
                    overall_steps_status.append(("create_source",
                                                 create_source_response["result"][
                                                     "status"].upper(),
                                                 create_source_response.get("result", {}).get(
                                                     "response",
                                                     {})))
                    # Proceed to configure the source connection details
                    source_connection_configurations_response = source_obj.configure_csv_source(self, source_id, self.mappings,
                                                    read_passwords_from_secrets=read_passwords_from_secrets,
                                                    env_tag=env_tag, secret_type=secret_type,config_ini_path=config_ini_path,dont_skip_step=configuration_obj["steps_to_run"]["configure_rdbms_source_connection"])
                    print("source_connection_configurations_response:",source_connection_configurations_response)
                    if source_connection_configurations_response["result"]["status"].upper() in ["SUCCESS","SKIPPED"]:
                        overall_steps_status.append(("source_connection_configuration",
                                                     source_connection_configurations_response["result"][
                                                         "status"].upper(),
                                                     source_connection_configurations_response.get("result", {}).get("response",
                                                                                                           {})))
                        print("Successfully configured the connection details")
                        self.logger.info("Successfully configured the connection details")
                        # Proceed to configure tables and table groups
                        source_import_configuration_response = source_obj.import_source_configuration(self, source_id, self.mappings,
                                                                        override_configuration_file, export_lookup,
                                                                        read_passwords_from_secrets)
                        # with open("/Users/nitin.bs/Desktop/source_import_configuration_response.json","w") as f:
                        #     json.dump(source_import_configuration_response,f,indent=4)
                        if source_import_configuration_response["result"]["status"].upper() in ["SUCCESS","SKIPPED"]:
                            minimal_response = source_import_configuration_response["result"].get(
                                "response", {}).get(
                                "configuration", {}).get(
                                "iw_mappings", [])
                            minimal_response_with_error = [iw_mappings for iw_mappings in
                                                           minimal_response if
                                                           iw_mappings.get("table_upsert_status",
                                                                           {}).get("error", []) != []]
                            print(minimal_response_with_error)
                            self.logger.info(minimal_response_with_error)
                            status=source_import_configuration_response["result"]["status"]
                            overall_steps_status.append(("source_import_configuration",
                                                         status.upper(),
                                                         minimal_response_with_error))
                            self.logger.info("Configured source successfully")
                            print("Configured Source successfully!")
                            if configuration_obj.get("table_watermark_mappings", {}):
                                source_obj.update_table_watermarks_and_state_as_ready(self,source_id)

                        else:

                            self.logger.info("Failed to configure source")
                            print("Failed to configure source")
                            print(source_import_configuration_response)
                            self.logger.error("Failed to configure source")
                            minimal_response = source_import_configuration_response["result"].get(
                                "response", {}).get(
                                "configuration", {}).get(
                                "iw_mappings", [])
                            minimal_response_with_error=""
                            if minimal_response!=[]:
                                minimal_response_with_error = [iw_mappings for iw_mappings in
                                                           minimal_response if
                                                           iw_mappings.get("table_upsert_status",
                                                                           {}).get("error", []) != []]
                            else:
                                minimal_response_with_error=source_import_configuration_response.get("result",{}).get("response",{})
                            print(minimal_response_with_error)
                            self.logger.info(minimal_response_with_error)
                            status = source_import_configuration_response["result"]["status"]
                            overall_steps_status.append(("source_import_configuration",
                                                         "FAILED",
                                                         minimal_response_with_error))
                            self.print_overall_steps_status(overall_steps_status)
                            self.logger.error(source_import_configuration_response)
                            raise Exception("Failed to configure source")
                        response_to_return[
                            "source_import_configuration_response"] = source_import_configuration_response

                    else:
                        print("Failed to configure the source connection details")
                        print(source_connection_configurations_response)
                        self.print_overall_steps_status(overall_steps_status)
                        self.logger.error("Failed to configure the source connection details")
                        self.logger.error(source_connection_configurations_response)
                        raise Exception("Failed to configure the source connection details")
                    response_to_return["source_connection_configurations_response"] = source_connection_configurations_response

                else:
                    print("Failed to create source")
                    self.logger.error("Failed to create source")
                    raise Exception("Failed to create source")
                response_to_return["create_source_response"] = create_source_response
                self.print_overall_steps_status(overall_steps_status)
            elif source_type == "file":
                overall_steps_status = []
                source_obj = FileSource(env_id, storage_id, configuration_file_path, self.secrets_config, replace_words)
                # adding below line to prevent missing out mappings done above (compute_mappings,environment_mappings etc
                source_obj.configuration_obj=configuration_obj
                source_obj.update_mappings_for_configurations(self.mappings)
                create_source_response = source_obj.create_file_source(self)
                print("create_source_response:",create_source_response)
                source_id=create_source_response["result"]["source_id"]
                if source_id is not None:
                    overall_steps_status.append(("create_source",
                                                 create_source_response["result"][
                                                     "status"].upper(),
                                                 create_source_response.get("result", {}).get(
                                                     "response",
                                                     {})))
                    # Proceed to configure the source connection details
                    source_connection_configurations_response = source_obj.configure_file_source(self, source_id, self.mappings,
                                                    read_passwords_from_secrets=read_passwords_from_secrets,
                                                    env_tag=env_tag, secret_type=secret_type,config_ini_path=config_ini_path,dont_skip_step=configuration_obj["steps_to_run"]["configure_rdbms_source_connection"])
                    print("source_connection_configurations_response:",source_connection_configurations_response)
                    if source_connection_configurations_response["result"]["status"].upper() in ["SUCCESS","SKIPPED"]:
                        overall_steps_status.append(("source_connection_configuration",
                                                     source_connection_configurations_response["result"][
                                                         "status"].upper(),
                                                     source_connection_configurations_response.get("result", {}).get("response",
                                                                                                           {})))
                        print("Successfully configured the connection details")
                        self.logger.info("Successfully configured the connection details")
                        # Proceed to configure tables and table groups
                        source_import_configuration_response = source_obj.import_source_configuration(self, source_id, self.mappings,
                                                                        override_configuration_file, export_lookup,
                                                                        read_passwords_from_secrets)
                        # with open("/Users/nitin.bs/Desktop/source_import_configuration_response.json","w") as f:
                        #     json.dump(source_import_configuration_response,f,indent=4)
                        if source_import_configuration_response["result"]["status"].upper() in ["SUCCESS","SKIPPED"]:
                            minimal_response = source_import_configuration_response["result"].get(
                                "response", {}).get(
                                "configuration", {}).get(
                                "iw_mappings", [])
                            minimal_response_with_error = [iw_mappings for iw_mappings in
                                                           minimal_response if
                                                           iw_mappings.get("table_upsert_status",
                                                                           {}).get("error", []) != []]
                            print(minimal_response_with_error)
                            self.logger.info(minimal_response_with_error)
                            status=source_import_configuration_response["result"]["status"]
                            overall_steps_status.append(("source_import_configuration",
                                                         status.upper(),
                                                         minimal_response_with_error))
                            self.logger.info("Configured source successfully")
                            print("Configured Source successfully!")
                            if configuration_obj.get("table_watermark_mappings", {}):
                                source_obj.update_table_watermarks_and_state_as_ready(self,source_id)
                        else:

                            self.logger.info("Failed to configure source")
                            print("Failed to configure source")
                            print(source_import_configuration_response)
                            self.logger.error("Failed to configure source")
                            minimal_response = source_import_configuration_response["result"].get(
                                "response", {}).get(
                                "configuration", {}).get(
                                "iw_mappings", [])
                            minimal_response_with_error=""
                            if minimal_response!=[]:
                                minimal_response_with_error = [iw_mappings for iw_mappings in
                                                           minimal_response if
                                                           iw_mappings.get("table_upsert_status",
                                                                           {}).get("error", []) != []]
                            else:
                                minimal_response_with_error=source_import_configuration_response.get("result",{}).get("response",{})
                            print(minimal_response_with_error)
                            self.logger.info(minimal_response_with_error)
                            status = source_import_configuration_response["result"]["status"]
                            overall_steps_status.append(("source_import_configuration",
                                                         "FAILED",
                                                         minimal_response_with_error))
                            self.print_overall_steps_status(overall_steps_status)
                            self.logger.error(source_import_configuration_response)
                            raise Exception("Failed to configure source")
                        response_to_return[
                            "source_import_configuration_response"] = source_import_configuration_response

                    else:
                        print("Failed to configure the source connection details")
                        print(source_connection_configurations_response)
                        self.print_overall_steps_status(overall_steps_status)
                        self.logger.error("Failed to configure the source connection details")
                        self.logger.error(source_connection_configurations_response)
                        raise Exception("Failed to configure the source connection details")
                    response_to_return["source_connection_configurations_response"] = source_connection_configurations_response

                else:
                    print("Failed to create source")
                    self.logger.error("Failed to create source")
                    raise Exception("Failed to create source")
                response_to_return["create_source_response"] = create_source_response
                self.print_overall_steps_status(overall_steps_status)
            elif source_type == "streaming" and (source_sub_type == "confluent_cloud" or source_sub_type == "kafka"):
                overall_steps_status = []
                source_obj = StreamingSource(env_id, storage_id, configuration_file_path, self.secrets_config, replace_words)
                # adding below line to prevent missing out mappings done above (compute_mappings,environment_mappings etc
                source_obj.configuration_obj=configuration_obj
                source_obj.update_mappings_for_configurations(self.mappings)
                create_source_response = source_obj.create_streaming_source(self)
                print("create_source_response:",create_source_response)
                source_id=create_source_response["result"]["source_id"]
                if source_id is not None:
                    overall_steps_status.append(("create_source",
                                                 create_source_response["result"][
                                                     "status"].upper(),
                                                 create_source_response.get("result", {}).get(
                                                     "response",
                                                     {})))
                    # Proceed to configure the source connection details
                    source_connection_configurations_response = source_obj.configure_streaming_source(self, source_id, self.mappings,
                                                    read_passwords_from_secrets=read_passwords_from_secrets,
                                                    env_tag=env_tag, secret_type=secret_type,config_ini_path=config_ini_path,dont_skip_step=configuration_obj["steps_to_run"]["configure_rdbms_source_connection"])
                    print("source_connection_configurations_response:",source_connection_configurations_response)
                    if source_connection_configurations_response["result"]["status"].upper() in ["SUCCESS","SKIPPED"]:
                        overall_steps_status.append(("source_connection_configuration",
                                                     source_connection_configurations_response["result"][
                                                         "status"].upper(),
                                                     source_connection_configurations_response.get("result", {}).get("response",
                                                                                                           {})))
                        print("Successfully configured the connection details")
                        self.logger.info("Successfully configured the connection details")
                        # Proceed to configure tables and table groups
                        source_import_configuration_response = source_obj.import_source_configuration(self, source_id, self.mappings,
                                                                        override_configuration_file, export_lookup,
                                                                        read_passwords_from_secrets)
                        # with open("/Users/nitin.bs/Desktop/source_import_configuration_response.json","w") as f:
                        #     json.dump(source_import_configuration_response,f,indent=4)
                        if source_import_configuration_response["result"]["status"].upper() in ["SUCCESS","SKIPPED"]:
                            minimal_response = source_import_configuration_response["result"].get(
                                "response", {}).get(
                                "configuration", {}).get(
                                "iw_mappings", [])
                            minimal_response_with_error = [iw_mappings for iw_mappings in
                                                           minimal_response if
                                                           iw_mappings.get("table_upsert_status",
                                                                           {}).get("error", []) != []]
                            print(minimal_response_with_error)
                            self.logger.info(minimal_response_with_error)
                            status=source_import_configuration_response["result"]["status"]
                            overall_steps_status.append(("source_import_configuration",
                                                         status.upper(),
                                                         minimal_response_with_error))
                            self.logger.info("Configured source successfully")
                            print("Configured Source successfully!")
                        else:

                            self.logger.info("Failed to configure source")
                            print("Failed to configure source")
                            print(source_import_configuration_response)
                            self.logger.error("Failed to configure source")
                            minimal_response = source_import_configuration_response["result"].get(
                                "response", {}).get(
                                "configuration", {}).get(
                                "iw_mappings", [])
                            minimal_response_with_error=""
                            if minimal_response!=[]:
                                minimal_response_with_error = [iw_mappings for iw_mappings in
                                                           minimal_response if
                                                           iw_mappings.get("table_upsert_status",
                                                                           {}).get("error", []) != []]
                            else:
                                minimal_response_with_error=source_import_configuration_response.get("result",{}).get("response",{})
                            print(minimal_response_with_error)
                            self.logger.info(minimal_response_with_error)
                            status = source_import_configuration_response["result"]["status"]
                            overall_steps_status.append(("source_import_configuration",
                                                         "FAILED",
                                                         minimal_response_with_error))
                            self.print_overall_steps_status(overall_steps_status)
                            self.logger.error(source_import_configuration_response)
                            raise Exception("Failed to configure source")
                        response_to_return[
                            "source_import_configuration_response"] = source_import_configuration_response

                    else:
                        print("Failed to configure the source connection details")
                        print(source_connection_configurations_response)
                        self.print_overall_steps_status(overall_steps_status)
                        self.logger.error("Failed to configure the source connection details")
                        self.logger.error(source_connection_configurations_response)
                        raise Exception("Failed to configure the source connection details")
                    response_to_return["source_connection_configurations_response"] = source_connection_configurations_response

                else:
                    print("Failed to create source")
                    self.logger.error("Failed to create source")
                    raise Exception("Failed to create source")
                response_to_return["create_source_response"] = create_source_response
                self.print_overall_steps_status(overall_steps_status)
            elif source_type == "rdbms" and source_sub_type!="snowflake":
                overall_steps_status=[]
                source_obj = RDBMSSource()
                source_obj.set_variables(env_id, storage_id, configuration_file_path, self.secrets_config,
                                         replace_words)
                source_obj.configuration_obj = configuration_obj
                source_obj.update_mappings_for_configurations(self.mappings)
                default_section_mappings =dict(self.mappings.get("api_mappings"))
                source_obj.start_interactive_cluster(self,environment_id=source_obj.environment_id,default_section_mappings=default_section_mappings)
                source_creation_response = source_obj.create_rdbms_source(self)
                source_id = source_creation_response["result"]["source_id"]
                if source_id is not None:
                    overall_steps_status.append(("source_creation","SUCCESS", source_creation_response.get("result",{}).get("response","created/obtained existing source successfully!")))
                    # Proceed to configure the source connection details
                    source_connection_configurations_response = source_obj.configure_rdbms_source_connection(self, source_id, override_configuration_file,
                                                                    read_passwords_from_secrets=read_passwords_from_secrets,
                                                                    env_tag=env_tag,
                                                                    secret_type=secret_type,config_ini_path=config_ini_path,dont_skip_step=configuration_obj["steps_to_run"]["configure_rdbms_source_connection"])
                    if source_connection_configurations_response["result"]["status"].upper() in ["SUCCESS","SKIPPED"]:
                        print("Successfully configured the connection details")
                        self.logger.info("Successfully configured the connection details")
                        overall_steps_status.append(("source_connection_configurations",source_connection_configurations_response["result"]["status"].upper(),source_connection_configurations_response.get("result",{}).get("response",{})))
                        source_test_connection_response = source_obj.test_source_connection(self, source_id,dont_skip_step=configuration_obj["steps_to_run"]["test_source_connection"])
                        if source_test_connection_response["result"]["status"].upper() in ["SUCCESS","SKIPPED"]:
                            overall_steps_status.append(("source_test_connection",
                                                         source_test_connection_response["result"][
                                                        "status"].upper(),
                                                         source_test_connection_response.get("result",{}).get("response", {})))
                            total_tables_count = len(configuration_obj["configuration"].get("table_configs",[]))
                            batch_size=int(default_section_mappings.get("tables_batch_size",50))
                            overall_browse_add_status=[]
                            print(f"Adding the tables in batches of {batch_size}!")
                            browse_table_status="SUCCESS"
                            add_tables_status="SUCCESS"
                            update_schema_status="SUCCESS"
                            for index in range(0,total_tables_count,batch_size):
                                source_browse_source_tables_response = source_obj.browse_source_tables(self, source_id,configuration_obj["steps_to_run"]["browse_source_tables"],index=index,batch_size=batch_size)
                                if source_browse_source_tables_response["result"]["status"].upper() in ["SUCCESS","SKIPPED"]:
                                    add_tables_to_source_response = source_obj.add_tables_to_source(self, source_id,configuration_obj["steps_to_run"]["add_tables_to_source"],index=index,batch_size=batch_size)
                                    add_table_status = add_tables_to_source_response["result"]["status"]
                                    if add_table_status.upper() in ["SUCCESS","SKIPPED"]:
                                        overall_browse_add_status.append(
                                            (f"batch_{math.floor(index/batch_size)}", add_table_status.upper(), "Added tables successfully!"))
                                        self.logger.info("Added tables to source successfully")
                                        print("Added tables to source successfully")
                                    else:
                                        add_tables_status="FAILED"
                                        print("Failed to add tables to source")
                                        print(add_tables_to_source_response)
                                        self.logger.error("Failed to add tables to source")
                                        self.logger.error(add_tables_to_source_response)
                                        overall_browse_add_status.append((f"batch_{index}","FAILED",json.dumps(add_tables_to_source_response.get("result",{}).get("response",{}).get("result",{}).get("response",add_tables_to_source_response))))
                                        #raise Exception("Failed to add tables to source")
                                else:
                                    browse_table_status="FAILED"
                                    self.logger.error("Failed while browsing tables")
                                    self.logger.error(source_browse_source_tables_response)
                                    print("Failed while browsing tables")
                                    print(source_browse_source_tables_response)
                                    self.logger.error(source_browse_source_tables_response)
                                    raise Exception("Failed while browsing tables")
                            if browse_table_status =="SUCCESS":
                                overall_steps_status.append(("source_browse_source_tables",
                                                         browse_table_status,
                                                         "Browsed tables successfully!"))
                            else:
                                overall_steps_status.append(("source_browse_source_tables",
                                                             browse_table_status,
                                                             "Failed to browse tables in few batches. Refer to Add tables report"))
                            if add_tables_status == "SUCCESS":
                                overall_steps_status.append(("source_add_tables",
                                                         add_tables_status,
                                                         "Added tables successfully"))
                            else:
                                overall_steps_status.append(("source_add_tables",
                                                             add_tables_status,
                                                             "Failed to add tables in few batches.Refer to Add tables report"))
                            print("Report of add tables API calls")
                            pd.set_option("display.max_colwidth", None)
                            add_tables_report_df=pd.DataFrame(data=overall_browse_add_status,columns=["BATCH","STATUS","RESPONSE"])
                            print(add_tables_report_df.to_markdown())
                            if add_table_status.upper() in ["SUCCESS","SKIPPED"]:
                                    self.logger.info("Updated schema for tables successfully")
                                    # added below code since the config migration api doesn't support schema updatation as of 5.4.2.4
                                    # to be removed after api fix
                                    update_schema_response = source_obj.update_schema_for_tables(self, source_id=source_id,
                                                                                                 export_configuration_file=override_configuration_file,
                                                                                                 export_config_lookup=export_lookup,
                                                                                                 mappings=self.mappings,
                                                                                                 read_passwords_from_secrets=read_passwords_from_secrets,
                                                                                                 env_tag=env_tag,
                                                                                                 secret_type=secret_type,
                                                                                                 dont_skip_step=
                                                                                                 configuration_obj[
                                                                                                     "steps_to_run"][
                                                                                                     "configure_tables_and_tablegroups"])
                                    update_schema_status = update_schema_response["result"]["status"]
                                    if update_schema_status in ["SUCCESS","FAILED"]:
                                        overall_steps_status.append(("update_tables_schema",
                                                                     update_schema_status,
                                                                     update_schema_response))
                                        if update_schema_status=="FAILED":
                                            self.logger.error("Failed to update schema for tables")
                                            print("Failed to update schema for tables")
                                            print(update_schema_response)
                                            self.logger.error(update_schema_response)
                                            self.print_overall_steps_status(overall_steps_status)
                                        # print(update_schema_response)
                                        self.logger.info(update_schema_response)
                                        print("Configured source successfully")
                                        configure_tables_and_tablegroups_response = source_obj.configure_tables_and_tablegroups(
                                            self, source_id,
                                            override_configuration_file,
                                            export_lookup, self.mappings,
                                            read_passwords_from_secrets,
                                            env_tag=env_tag,
                                            secret_type=secret_type, dont_skip_step=configuration_obj["steps_to_run"][
                                                "configure_tables_and_tablegroups"])
                                        status = configure_tables_and_tablegroups_response["result"]["status"]
                                        if status == "SUCCESS":
                                            self.logger.info("Configured source successfully")
                                            minimal_response = configure_tables_and_tablegroups_response["result"].get(
                                                "response", {}).get("result", {}).get("response").get("result", {}).get(
                                                "configuration", {}).get(
                                                "iw_mappings", [])
                                            minimal_response_with_error = [iw_mappings for iw_mappings in
                                                                           minimal_response if
                                                                           iw_mappings.get("table_upsert_status",
                                                                                           {}).get("error", []) != []]
                                            print(minimal_response_with_error)
                                            self.logger.info(minimal_response_with_error)
                                            overall_steps_status.append(("configure_tables_and_table_groups",
                                                                     status.upper(),
                                                                     minimal_response_with_error))
                                            if configuration_obj.get("table_watermark_mappings", {}):
                                                source_obj.update_table_state_as_ready(self, source_id)
                                        else:
                                            self.logger.error("Failed to configure source")
                                            print("Failed to configure source")
                                            minimal_response = configure_tables_and_tablegroups_response["result"].get(
                                                "response", {}).get("result", {}).get("response").get("configuration",
                                                                                                      {}).get(
                                                "iw_mappings", [])
                                            if minimal_response != []:
                                                minimal_response_with_error = [iw_mappings for iw_mappings in
                                                                               minimal_response if
                                                                               iw_mappings.get("table_upsert_status",
                                                                                               {}).get(
                                                                                   "error", []) != []]
                                                print(minimal_response_with_error)
                                                overall_steps_status.append(("configure_tables_and_table_groups",
                                                                             status.upper(),
                                                                             minimal_response_with_error))
                                                self.logger.info(minimal_response_with_error)
                                            else:
                                                print(configure_tables_and_tablegroups_response)
                                                self.logger.error(configure_tables_and_tablegroups_response)
                                                minimal_response_with_error = configure_tables_and_tablegroups_response.get(
                                                    "result", {}).get("response", {})
                                                overall_steps_status.append(("configure_tables_and_table_groups",
                                                                             status.upper(),
                                                                             minimal_response_with_error))
                                            self.print_overall_steps_status(overall_steps_status)
                                            raise Exception("Failed to configure source")

                    else:
                        overall_steps_status.append(("source_connection_configuration", "FAILED", source_connection_configurations_response))
                        print(source_connection_configurations_response)
                        self.logger.error(source_connection_configurations_response)
                        self.print_overall_steps_status(overall_steps_status)
                        raise Exception("Failed to configure source connection details")
                else:
                    overall_steps_status.append(("source_creation", "FAILED", source_creation_response.get("result",{}).get("response",source_creation_response)))
                    print(source_creation_response)
                    self.logger.error(source_creation_response)
                    self.print_overall_steps_status(overall_steps_status)
                    raise Exception("Failed to create source")
                self.print_overall_steps_status(overall_steps_status)
            elif source_type == "crm" and source_sub_type == "salesforce":
                source_obj = SalesforceSource()
                source_obj.set_variables(env_id, storage_id, configuration_file_path, self.secrets_config,
                                         replace_words)
                source_obj.configuration_obj = configuration_obj
                source_obj.update_mappings_for_configurations(self.mappings)
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
                                    print("Configured source successfully")
                                    if configuration_obj.get("table_watermark_mappings", {}):
                                        source_obj.update_table_state_as_ready(self, source_id)
                        else:
                            self.logger.error("Failed to launch source test connection")
                            self.logger.error(status)
                            print("Failed to launch source test connection")
                            raise Exception("Failed to launch source test connection")
                else:
                    self.logger.error("Failed to create source")
                    raise Exception("Failed to create source")
            elif source_type == "generic_jdbc":
                overall_steps_status=[]
                source_obj = GenericJDBCSource()
                source_obj.set_variables(env_id, storage_id, configuration_file_path, self.secrets_config,
                                         replace_words)
                source_obj.configuration_obj = configuration_obj
                source_obj.update_mappings_for_configurations(self.mappings)
                default_section_mappings =dict(self.mappings.get("api_mappings"))
                source_obj.start_interactive_cluster(self,environment_id=source_obj.environment_id,default_section_mappings=default_section_mappings)
                source_creation_response = source_obj.create_generic_jdbc_source(self)
                source_id = source_creation_response["result"]["source_id"]
                if source_id is not None:
                    overall_steps_status.append(("source_creation","SUCCESS", source_creation_response.get("result",{}).get("response","created/obtained existing source successfully!")))
                    # Proceed to configure the source connection details
                    source_connection_configurations_response = source_obj.configure_generic_jdbc_source_connection(self, source_id, override_configuration_file,
                                                                    read_passwords_from_secrets=read_passwords_from_secrets,
                                                                    env_tag=env_tag,
                                                                    secret_type=secret_type,config_ini_path=config_ini_path,dont_skip_step=configuration_obj["steps_to_run"]["configure_rdbms_source_connection"])
                    if source_connection_configurations_response["result"]["status"].upper() in ["SUCCESS","SKIPPED"]:
                        print("Successfully configured the connection details")
                        self.logger.info("Successfully configured the connection details")
                        overall_steps_status.append(("source_connection_configurations",source_connection_configurations_response["result"]["status"].upper(),source_connection_configurations_response.get("result",{}).get("response",{})))
                        source_test_connection_response = source_obj.test_source_connection(self, source_id,dont_skip_step=configuration_obj["steps_to_run"]["test_source_connection"])
                        if source_test_connection_response["result"]["status"].upper() in ["SUCCESS","SKIPPED"]:
                            overall_steps_status.append(("source_test_connection",
                                                         source_test_connection_response["result"][
                                                        "status"].upper(),
                                                         source_test_connection_response.get("result",{}).get("response", {})))
                            total_tables_count = len(configuration_obj["configuration"].get("table_configs",[]))
                            batch_size=int(default_section_mappings.get("tables_batch_size",50))
                            overall_browse_add_status=[]
                            print(f"Adding the tables in batches of {batch_size}!")
                            browse_table_status="SUCCESS"
                            add_tables_status="SUCCESS"
                            update_schema_status="SUCCESS"
                            for index in range(0,total_tables_count,batch_size):
                                source_browse_source_tables_response = source_obj.browse_source_tables(self, source_id,configuration_obj["steps_to_run"]["browse_source_tables"],index=index,batch_size=batch_size)
                                if source_browse_source_tables_response["result"]["status"].upper() in ["SUCCESS","SKIPPED"]:
                                    add_tables_to_source_response = source_obj.add_tables_to_source(self, source_id,configuration_obj["steps_to_run"]["add_tables_to_source"],index=index,batch_size=batch_size)
                                    add_table_status = add_tables_to_source_response["result"]["status"]
                                    if add_table_status.upper() in ["SUCCESS","SKIPPED"]:
                                        overall_browse_add_status.append(
                                            (f"batch_{math.floor(index/batch_size)}", add_table_status.upper(), "Added tables successfully!"))
                                        self.logger.info("Added tables to source successfully")
                                        print("Added tables to source successfully")
                                    else:
                                        add_tables_status="FAILED"
                                        print("Failed to add tables to source")
                                        print(add_tables_to_source_response)
                                        self.logger.error("Failed to add tables to source")
                                        self.logger.error(add_tables_to_source_response)
                                        overall_browse_add_status.append((f"batch_{index}","FAILED",json.dumps(add_tables_to_source_response.get("result",{}).get("response",{}).get("result",{}).get("response",add_tables_to_source_response))))
                                        #raise Exception("Failed to add tables to source")
                                else:
                                    browse_table_status="FAILED"
                                    self.logger.error("Failed while browsing tables")
                                    self.logger.error(source_browse_source_tables_response)
                                    print("Failed while browsing tables")
                                    print(source_browse_source_tables_response)
                                    self.logger.error(source_browse_source_tables_response)
                                    raise Exception("Failed while browsing tables")
                            if browse_table_status =="SUCCESS":
                                overall_steps_status.append(("source_browse_source_tables",
                                                         browse_table_status,
                                                         "Browsed tables successfully!"))
                            else:
                                overall_steps_status.append(("source_browse_source_tables",
                                                             browse_table_status,
                                                             "Failed to browse tables in few batches. Refer to Add tables report"))
                            if add_tables_status == "SUCCESS":
                                overall_steps_status.append(("source_add_tables",
                                                         add_tables_status,
                                                         "Added tables successfully"))
                            else:
                                overall_steps_status.append(("source_add_tables",
                                                             add_tables_status,
                                                             "Failed to add tables in few batches.Refer to Add tables report"))
                            print("Report of add tables API calls")
                            pd.set_option("display.max_colwidth", None)
                            add_tables_report_df=pd.DataFrame(data=overall_browse_add_status,columns=["BATCH","STATUS","RESPONSE"])
                            print(add_tables_report_df.to_markdown())
                            if add_table_status.upper() in ["SUCCESS","SKIPPED"]:
                                    self.logger.info("Updated schema for tables successfully")
                                    # added below code since the config migration api doesn't support schema updatation as of 5.4.2.4
                                    # to be removed after api fix
                                    update_schema_response = source_obj.update_schema_for_tables(self, source_id,
                                                                                                 override_configuration_file,
                                                                                                 export_lookup,
                                                                                                 self.mappings,
                                                                                                 read_passwords_from_secrets,
                                                                                                 env_tag=env_tag,
                                                                                                 secret_type=secret_type,
                                                                                                 dont_skip_step=
                                                                                                 configuration_obj[
                                                                                                     "steps_to_run"][
                                                                                                     "configure_tables_and_tablegroups"])
                                    update_schema_status = update_schema_response["result"]["status"]
                                    if update_schema_status in ["SUCCESS","FAILED"]:
                                        overall_steps_status.append(("update_tables_schema",
                                                                     update_schema_status,
                                                                     update_schema_response))
                                        if update_schema_status=="FAILED":
                                            self.logger.error("Failed to update schema for tables")
                                            print("Failed to update schema for tables")
                                            print(update_schema_response)
                                            self.logger.error(update_schema_response)
                                            self.print_overall_steps_status(overall_steps_status)
                                        # print(update_schema_response)
                                        self.logger.info(update_schema_response)
                                        print("Configured source successfully")
                                        configure_tables_and_tablegroups_response = source_obj.configure_tables_and_tablegroups(
                                            self, source_id,
                                            override_configuration_file,
                                            export_lookup, self.mappings,
                                            read_passwords_from_secrets,
                                            env_tag=env_tag,
                                            secret_type=secret_type, dont_skip_step=configuration_obj["steps_to_run"][
                                                "configure_tables_and_tablegroups"])
                                        status = configure_tables_and_tablegroups_response["result"]["status"]
                                        if status == "SUCCESS":
                                            self.logger.info("Configured source successfully")
                                            minimal_response = configure_tables_and_tablegroups_response["result"].get(
                                                "response", {}).get("result", {}).get("response").get("result", {}).get(
                                                "configuration", {}).get(
                                                "iw_mappings", [])
                                            minimal_response_with_error = [iw_mappings for iw_mappings in
                                                                           minimal_response if
                                                                           iw_mappings.get("table_upsert_status",
                                                                                           {}).get("error", []) != []]
                                            print(minimal_response_with_error)
                                            self.logger.info(minimal_response_with_error)
                                            overall_steps_status.append(("configure_tables_and_table_groups",
                                                                     status,
                                                                     minimal_response_with_error))
                                        else:
                                            self.logger.error("Failed to configure source")
                                            print("Failed to configure source")
                                            minimal_response = configure_tables_and_tablegroups_response["result"].get(
                                                "response", {}).get("result", {}).get("response").get("configuration",
                                                                                                      {}).get(
                                                "iw_mappings", [])
                                            if minimal_response != []:
                                                minimal_response_with_error = [iw_mappings for iw_mappings in
                                                                               minimal_response if
                                                                               iw_mappings.get("table_upsert_status",
                                                                                               {}).get(
                                                                                   "error", []) != []]
                                                print(minimal_response_with_error)
                                                overall_steps_status.append(("configure_tables_and_table_groups",
                                                                             status,
                                                                             minimal_response_with_error))
                                                self.logger.info(minimal_response_with_error)
                                                if configuration_obj.get("table_watermark_mappings", {}):
                                                    source_obj.update_table_state_as_ready(self, source_id)
                                            else:
                                                print(configure_tables_and_tablegroups_response)
                                                self.logger.error(configure_tables_and_tablegroups_response)
                                            self.print_overall_steps_status(overall_steps_status)
                                            raise Exception("Failed to configure source")

                    else:
                        overall_steps_status.append(("source_connection_configuration", "FAILED", source_connection_configurations_response))
                        print(source_connection_configurations_response)
                        self.logger.error(source_connection_configurations_response)
                        self.print_overall_steps_status(overall_steps_status)
                        raise Exception("Failed to configure source connection details")
                else:
                    overall_steps_status.append(("source_creation", "FAILED", source_creation_response.get("result",{}).get("response",source_creation_response)))
                    print(source_creation_response)
                    self.logger.error(source_creation_response)
                    self.print_overall_steps_status(overall_steps_status)
                    raise Exception("Failed to create source")
                self.print_overall_steps_status(overall_steps_status)

            else:    #assumes to be cdata source
                overall_steps_status = []
                source_obj = CdataSource()
                source_obj.set_variables(env_id, storage_id, configuration_file_path, self.secrets_config,
                                         replace_words)
                source_obj.configuration_obj = configuration_obj
                source_obj.update_mappings_for_configurations(self.mappings)
                default_section_mappings = dict(self.mappings.get("api_mappings"))
                source_obj.start_interactive_cluster(self, environment_id=source_obj.environment_id,
                                                     default_section_mappings=default_section_mappings)
                source_creation_response = source_obj.create_cdata_source(self)
                source_id = source_creation_response["result"]["source_id"]
                if source_id is not None:
                    # Proceed to configure the source connection details
                    source_connection_configurations_response = source_obj.configure_cdata_source_connection(self,
                                                                                                             source_id,
                                                                                                             override_configuration_file,
                                                                                                             read_passwords_from_secrets=read_passwords_from_secrets,
                                                                                                             env_tag=env_tag,
                                                                                                             secret_type=secret_type,
                                                                                                             config_ini_path=config_ini_path,
                                                                                                             dont_skip_step=
                                                                                                             configuration_obj[
                                                                                                                 "steps_to_run"][
                                                                                                                 "configure_rdbms_source_connection"])
                    if source_connection_configurations_response["result"]["status"].upper() in ["SUCCESS", "SKIPPED"]:
                        overall_steps_status.append(("source_connection_configuration",
                                                     source_connection_configurations_response["result"][
                                                         "status"].upper(),
                                                     source_connection_configurations_response.get("result", {}).get(
                                                         "response",
                                                         {})))
                        print("Successfully configured the connection details")
                        self.logger.info("Successfully configured the connection details")
                        source_test_connection_response = source_obj.test_source_connection(self, source_id,
                                                                                            dont_skip_step=
                                                                                            configuration_obj[
                                                                                                "steps_to_run"][
                                                                                                "test_source_connection"])
                        if source_test_connection_response["result"]["status"].upper() in ["SUCCESS", "SKIPPED"]:
                            overall_steps_status.append(("source_test_connection_response",
                                                         source_test_connection_response["result"][
                                                             "status"].upper(),
                                                         source_test_connection_response.get("result",
                                                                                                       {}).get(
                                                             "response",
                                                             {})))
                            source_browse_source_tables_response = source_obj.browse_source_tables(self, source_id,
                                                                                                   configuration_obj[
                                                                                                       "steps_to_run"][
                                                                                                       "browse_source_tables"])
                            if source_browse_source_tables_response["result"]["status"].upper() in ["SUCCESS",
                                                                                                    "SKIPPED"]:
                                add_tables_to_source_response = source_obj.add_tables_to_source(self, source_id,
                                                                                                configuration_obj[
                                                                                                    "steps_to_run"][
                                                                                                    "add_tables_to_source"])
                                status = add_tables_to_source_response["result"]["status"]
                                if status.upper() in ["SUCCESS", "SKIPPED"] or add_tables_to_source_response[
                                    "result"].get("response", {}).get("result", {}).get("response", {}).get("iw_code",
                                                                                                            "") == "IW10003":
                                    self.logger.info("Added tables to source successfully")
                                    print("Added tables to source successfully")
                                else:
                                    print("Failed to add tables to source")
                                    print(add_tables_to_source_response)
                                    self.logger.error("Failed to add tables to source")
                                    self.logger.error(add_tables_to_source_response)
                                    raise Exception("Failed to add tables to source")
                                configure_tables_and_tablegroups_response = source_obj.configure_tables_and_tablegroups(
                                    self, source_id,
                                    override_configuration_file,
                                    export_lookup, self.mappings,
                                    read_passwords_from_secrets,
                                    env_tag=env_tag,
                                    secret_type=secret_type, dont_skip_step=configuration_obj["steps_to_run"][
                                        "configure_tables_and_tablegroups"])
                                status = configure_tables_and_tablegroups_response["result"]["status"]
                                if status == "SUCCESS":
                                    self.logger.info("Configured source successfully")
                                    minimal_response = configure_tables_and_tablegroups_response["result"].get(
                                        "response", {}).get("result", {}).get("response").get("result", {}).get(
                                        "configuration", {}).get(
                                        "iw_mappings", [])
                                    minimal_response_with_error = [iw_mappings for iw_mappings in minimal_response if
                                                                   iw_mappings.get("table_upsert_status", {}).get(
                                                                       "error", []) != []]
                                    print(minimal_response_with_error)
                                    self.logger.info(minimal_response_with_error)
                                    # added below code since the config migration api doesn't support schema updation as of 5.4.2.4
                                    # to be removed after api fix
                                    update_schema_response = source_obj.update_schema_for_tables(self, source_id,
                                                                                                 override_configuration_file,
                                                                                                 export_lookup,
                                                                                                 self.mappings,
                                                                                                 read_passwords_from_secrets,
                                                                                                 env_tag=env_tag,
                                                                                                 secret_type=secret_type,
                                                                                                 dont_skip_step=
                                                                                                 configuration_obj[
                                                                                                     "steps_to_run"][
                                                                                                     "configure_tables_and_tablegroups"])
                                    update_schema_status = update_schema_response["result"]["status"]
                                    if update_schema_status == "SUCCESS":
                                        # print(update_schema_response)
                                        self.logger.info(update_schema_response)
                                        print("Configured source successfully")
                                    else:
                                        self.logger.error("Failed to update schema for tables")
                                        print("Failed to update schema for tables")
                                        print(update_schema_response)
                                        self.logger.error(update_schema_response)
                                        raise Exception("Failed to update schema for tables")
                                else:
                                    self.logger.error("Failed to configure source")
                                    print("Failed to configure source")
                                    minimal_response = configure_tables_and_tablegroups_response["result"].get(
                                        "response", {}).get("result", {}).get("response").get("configuration", {}).get(
                                        "iw_mappings", [])
                                    if minimal_response != []:
                                        minimal_response_with_error = [iw_mappings for iw_mappings in minimal_response
                                                                       if
                                                                       iw_mappings.get("table_upsert_status", {}).get(
                                                                           "error", []) != []]
                                        print(minimal_response_with_error)
                                        self.logger.info(minimal_response_with_error)
                                    else:
                                        print(configure_tables_and_tablegroups_response)
                                        self.logger.error(configure_tables_and_tablegroups_response)
                                        if configuration_obj.get("table_watermark_mappings", {}):
                                            source_obj.update_table_state_as_ready(self, source_id)
                                    raise Exception("Failed to configure source")
                            else:
                                self.logger.error("Failed while browsing tables")
                                self.logger.error(source_browse_source_tables_response)
                                print("Failed while browsing tables")
                                print(source_browse_source_tables_response)
                                self.logger.error(source_browse_source_tables_response)
                                raise Exception("Failed while browsing tables")
                        else:
                            print(source_test_connection_response)
                            self.logger.error(source_test_connection_response)
                            raise Exception("Failed to launch test connection job")
                    else:
                        print(source_connection_configurations_response)
                        self.logger.error(source_connection_configurations_response)
                        raise Exception("Failed to configure source connection details")
                else:
                    print(source_creation_response)
                    self.logger.error(source_creation_response)
                    raise Exception("Failed to create source")
        except Exception as e:
            self.logger.error(str(e))
            self.logger.error(traceback.format_exc())
            print(traceback.format_exc())
            print(str(e))
            return response_to_return

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
