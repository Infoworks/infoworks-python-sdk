import json

import requests
import yaml

from infoworks.sdk.url_builder import get_source_details_url
from infoworks.sdk.utils import IWUtils
import configparser
from infoworks.sdk.cicd.upload_configurations.utils import Utils
from infoworks.sdk.cicd.upload_configurations.local_configurations import PRE_DEFINED_MAPPINGS
from infoworks.sdk.cicd.upload_configurations.update_configurations import InfoworksDynamicAccessNestedDict
from infoworks.sdk.source_response import SourceResponse

class SalesforceSource:
    def __init__(self):
        self.configuration_obj = None
        self.source_config_path = None
        self.environment_id = None
        self.storage_id = None
        self.secrets = None

    def set_variables(self, environment_id, storage_id, source_config_path, secrets=None, replace_words=""):
        self.storage_id = storage_id
        self.environment_id = environment_id
        self.source_config_path = source_config_path
        with open(self.source_config_path, 'r') as file:
            json_string = file.read()
            if replace_words != "":
                for key, value in [item.split("->") for item in replace_words.split(";")]:
                    json_string = json_string.replace(key, value)
        self.configuration_obj = IWUtils.ejson_deserialize(json_string)
        self.secrets = secrets

    def update_table_schema_and_database(self,type,mappings):
        data=self.configuration_obj
        for table in data.get("configuration",{}).get("table_configs",[]):
            if type=="target_schema":
                schema_from_config=table.get("configuration",{}).get("configuration",{}).get("target_schema_name","")
                if schema_from_config!="" and schema_from_config.lower() in mappings.keys():
                    table["configuration"]["configuration"]["target_schema_name"]=mappings.get(schema_from_config.lower())
            elif type=="stage_schema":
                schema_from_config=table.get("configuration",{}).get("configuration",{}).get("staging_schema_name","")
                if schema_from_config!="" and schema_from_config.lower() in mappings.keys():
                    table["configuration"]["configuration"]["staging_schema_name"]=mappings.get(schema_from_config.lower())
            elif type=="database":
                database_from_config=table.get("configuration",{}).get("configuration",{}).get("target_database_name","")
                if database_from_config!="" and database_from_config.lower() in mappings.keys():
                    table["configuration"]["configuration"]["target_database_name"]=mappings.get(database_from_config.lower())
            else:
                pass

    def update_mappings_for_configurations(self, mappings):
        config = configparser.ConfigParser()
        config.read_dict(mappings)
        d = InfoworksDynamicAccessNestedDict(self.configuration_obj)
        for section in config.sections():
            if section in PRE_DEFINED_MAPPINGS:
                continue
            print("section:", section)
            try:
                final = d.setval(section.split("$"), dict(config.items(section)))
            except KeyError as e:
                pass
        self.configuration_obj = d.data
        if "configuration$source_configs$data_lake_schema" in config.sections():
            self.update_table_schema_and_database("target_schema",dict(config.items("configuration$source_configs$data_lake_schema")))
        if "configuration$source_configs$staging_schema_name" in config.sections():
            self.update_table_schema_and_database("stage_schema",dict(config.items("configuration$source_configs$staging_schema_name")))
        if "configuration$source_configs$target_database_name" in config.sections():
            self.update_table_schema_and_database("database",dict(config.items("configuration$source_configs$target_database_name")))

    def create_salesforce_source(self, src_client_obj):
        data = self.configuration_obj["configuration"]["source_configs"]
        create_salesforce_source_payload = {
            "name": data["name"],
            "type": "crm",
            "sub_type": data["sub_type"],
            "data_lake_path": data["data_lake_path"],
            "data_lake_schema": data["data_lake_schema"] if "data_lake_schema" in data else "",
            "environment_id": self.environment_id,
            "storage_id": self.storage_id,
            "is_source_ingested": True
        }
        utils_obj = Utils()
        utils_obj.replace_custom_tags_names_with_mapping(create_salesforce_source_payload, src_client_obj)
        src_create_response = src_client_obj.create_source(source_config=create_salesforce_source_payload)
        if src_create_response["result"]["status"].upper() == "SUCCESS":
            source_id_created = src_create_response["result"]["source_id"]
            return source_id_created
        else:
            src_client_obj.logger.info('Cant create source {} '.format(data["name"]))
            src_client_obj.logger.info(f"Getting the existing SourceId with name {data['name']} if exists")
            filter_condition = IWUtils.ejson_serialize({"name": data['name']})
            source_detail_url = get_source_details_url(
                src_client_obj.client_config) + f"?filter={{filter_condition}}".format(
                filter_condition=filter_condition)
            response = requests.get(source_detail_url,
                                    headers={'Authorization': 'Bearer ' + src_client_obj.client_config['bearer_token'],
                                             'Content-Type': 'application/json'}, verify=False)
            if response.status_code == "406":
                headers = src_client_obj.regenerate_bearer_token_if_needed(
                    {'Authorization': 'Bearer ' + src_client_obj.client_config['bearer_token'],
                     'Content-Type': 'application/json'})
                response = requests.get(source_detail_url, headers=headers, verify=False)
            response = IWUtils.ejson_deserialize(response.content)
            if not response.get('result', None):
                src_client_obj.logger.error("Failed to make an api call to get source details")
                src_client_obj.logger.info(response)
            else:
                src_client_obj.logger.info(
                    f"Source Id with the same Source name {data['name']} : {response['result'][0]['id']}")
                return response['result'][0]['id']

    def configure_salesforce_source_connection(self, src_client_obj, source_id, override_config_file=None,
                                          read_passwords_from_secrets=False, env_tag="", secret_type=""):
        source_configs = self.configuration_obj["configuration"]["source_configs"]
        src_name = str(source_configs["name"])
        connection_object = source_configs["connection"]
        connection_object["fetch_mechanism"]=connection_object["fetch_mechanism"].lower()
        if override_config_file is not None:
            with open(override_config_file) as file:
                information = yaml.load(file, Loader=yaml.FullLoader)
            if information["source_details"].get(src_name, None) is not None:
                override_keys = information["source_details"].get(src_name).keys()
                for key in override_keys:
                    connection_object[key] = information["source_details"][src_name][key]
        response = src_client_obj.configure_source_connection(source_id, connection_object=connection_object)
        if response["result"]["status"].upper() != "SUCCESS":
            src_client_obj.logger.info(f"Failed to configure the source {source_id} connection")
            src_client_obj.logger.info(response["result"])
            return "FAILED"
        else:
            src_client_obj.logger.info(response["result"])
            return "SUCCESS"

    def test_source_connection(self, src_client_obj, source_id):
        response = src_client_obj.source_test_connection_job_poll(source_id, poll_timeout=300,
                                                                  polling_frequency=15, retries=1)
        return response["result"]["status"].upper()

    def metacrawl_source(self,src_client_obj,source_id):
        response = src_client_obj.source_metacrawl_job_poll(source_id, poll_timeout=300,
                                                                  polling_frequency=15, retries=1)
        return response["result"]["status"].upper()

    def configure_tables_and_tablegroups(self, src_client_obj, source_id, export_configuration_file=None,
                                         export_config_lookup=True, mappings=None, read_passwords_from_secrets=False,
                                         env_tag="", secret_type=""):
        if mappings is None:
            mappings = {}
        iw_mappings = self.configuration_obj["configuration"]["iw_mappings"]
        table_group_compute_mappings = mappings.get("table_group_compute_mappings", {})
        source_configs = self.configuration_obj["configuration"]["source_configs"]
        src_name = str(source_configs["name"])
        for item in iw_mappings:
            if item.get("entity_type", "") == "environment_compute_template":
                item["recommendation"]["compute_name"] = table_group_compute_mappings.get(
                    item["recommendation"]["compute_name"], item["recommendation"]["compute_name"])
        self.configuration_obj["configuration"]["iw_mappings"] = iw_mappings
        # Update the service account json file mappings if any
        if export_config_lookup and (export_configuration_file is not None or read_passwords_from_secrets):
            for table in self.configuration_obj["configuration"]["table_configs"]:
                # Check if there are any export configurations and passwords to replace
                if table.get("configuration", {}).get("export_configuration", None) is not None:
                    export_configs = table.get("configuration", {}).get("export_configuration")
                    target_type = export_configs.get("target_type", "").upper()
                    table_name = table["configuration"]["name"].upper()
                    override_keys = []
                    if export_configuration_file is not None:
                        with open(export_configuration_file) as file:
                            information = yaml.load(file, Loader=yaml.FullLoader)
                        if information["src_export_details"].get(src_name + "_" + table_name, None) is not None:
                            info_key = src_name + "_" + table_name
                            override_keys = information["src_export_details"].get(src_name + "_" + table_name,
                                                                                  {}).keys()
                        else:
                            info_key = src_name
                            override_keys = information["src_export_details"].get(src_name, {}).keys()
                        for key in override_keys:
                            table["configuration"]["export_configuration"]["connection"][key] = \
                                information["src_export_details"][info_key][key]
                    if target_type.upper() in ["SNOWFLAKE", "POSTGRES"]:
                        pass
                    elif target_type.upper() == "BIGQUERY":
                        if "server_path" not in override_keys:
                            server_path = table["configuration"]["export_configuration"].get("connection", {}).get(
                                "server_path", "")
                            if "gcp_details" in mappings:
                                server_path = mappings["gcp_details"].get("service_json_path")
                            if "service_json_mappings" in mappings:
                                server_path = mappings["service_json_mappings"].get(
                                    server_path.split("/")[-1],
                                    server_path)
                            table["configuration"]["export_configuration"]["connection"][
                                "server_path"] = server_path if server_path != "" else table["configuration"][
                                "export_configuration"].get("connection", {}).get(
                                "server_path", "")
                        table["configuration"]["export_configuration"]["connection"][
                            "upload_option"] = "serverLocation"
        # Added for Source Tags
        # source_configs = self.configuration_obj["configuration"]["source_configs"]
        # utils_obj = Utils()
        # utils_obj.replace_custom_tags_names_with_mapping(source_configs, src_client_obj)
        # # Added for Table Tags
        # table_configs = self.configuration_obj['configuration']['table_configs']
        # for table in table_configs:
        #     utils_obj.replace_custom_tags_names_with_mapping(table['configuration']['configuration'],
        #                                                      src_client_obj)
        # # Added for Table Group Tags
        # table_group_configs = self.configuration_obj['configuration']['table_group_configs']
        # for table_group in table_group_configs:
        #     utils_obj.replace_custom_tags_names_with_mapping(table_group['configuration'], src_client_obj)

        response = src_client_obj.configure_tables_and_tablegroups(source_id, configuration_obj=self.configuration_obj[
            "configuration"])
        if response["result"]["status"].upper() != "SUCCESS":
            src_client_obj.logger.error("Failed to import the source {} (source config path : {})"
                                        .format(source_id, self.source_config_path))
            src_client_obj.logger.error(response.get("message", "") + "(source config path : {})"
                                        .format(self.source_config_path))
            return "FAILED"
        else:
            src_client_obj.logger.info(f"Successfully imported source configurations to {source_id}")
            return "SUCCESS"
    def update_table_state_as_ready(self,src_client_obj, source_id):
        tables = self.configuration_obj["configuration"]["table_configs"]
        table_state_update_dict = {}
        tables_watermark_mappings = self.configuration_obj.get("table_watermark_mappings", {})
        for table in tables:
            table_name = table["configuration"]["name"]
            src_client_obj.logger.info(f"Updating the table state information for table {table_name}")
            table_update_payload = {"name": table_name, "source": source_id, "state": "ready"}
            print("table_update_payload:",table_update_payload)
            if table["configuration"].get("schema_name_at_source", "") != "":
                table_document = src_client_obj.list_tables_in_source(source_id, params={
                    "filter": {"origTableName": table["configuration"]["name"],
                               "schemaNameAtSource": table["configuration"]["schema_name_at_source"]}}).get("result",
                                                                                                            {}).get(
                    "response", {}).get("result", [])
            elif table["configuration"].get("catalog_name", ""):
                table_document = src_client_obj.list_tables_in_source(source_id, params={
                    "filter": {"origTableName": table["configuration"]["name"],
                               "catalog_name": table["configuration"]["catalog_name"]}}).get("result", {}).get(
                    "response", {}).get("result", [])
            elif table["configuration"]["configuration"].get("query",""):
                table_document = src_client_obj.list_tables_in_source(source_id, params={
                    "filter": {"table": table_name}}).get("result", {}).get(
                    "response", {}).get("result", [])
            else:
                print(f"Skipping updating state for table {table_name} as it did not match any existing table.")
            table_id = None
            if len(table_document) > 0:
                table_document = table_document[0]
                table_id = table_document["id"]
            if table_id is not None:
                response = src_client_obj.update_table_configuration(source_id=source_id, table_id=table_id,
                                                                     config_body=table_update_payload)
                if response["result"]["status"].upper() != "SUCCESS":
                    src_client_obj.logger.error(
                        "Failed to update state for table {table_name}".format(table_name=table_name))
                    src_client_obj.logger.error(response.get("result", {}).get("response", {}).get("message", ""))
                    table_state_update_dict[table_name] = (
                    "FAILED", response.get("result", {}).get("response", {}).get("message", ""))
                else:
                    src_client_obj.logger.info(
                        "Successfully updated state for table {table_name}".format(table_name=table_name))
                    table_state_update_dict[table_name] = ("SUCCESS", "")
        failed_state_update_tables = [(table_name, status[1]) for table_name, status in
                                       table_state_update_dict.items() if status[0].upper() == "FAILED"]
        overall_update_status = "FAILED" if len(failed_state_update_tables) > 0 else "SUCCESS"
        if overall_update_status == "FAILED":
            response = {f"Tables state update failed for tables:{failed_state_update_tables}"}
        else:
            response = {f"Tables state updated successfully"}
        return SourceResponse.parse_result(status=overall_update_status, source_id=source_id,
                                           response=response)