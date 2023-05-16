import copy
import json

import requests
import yaml
import configparser
from infoworks.sdk.url_builder import get_source_details_url
from infoworks.sdk.utils import IWUtils
from infoworks.sdk.cicd.upload_configurations.update_configurations import InfoworksDynamicAccessNestedDict
from infoworks.sdk.cicd.upload_configurations.local_configurations import PRE_DEFINED_MAPPINGS

class CdataSource:
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

    def create_cdata_source(self, src_client_obj):
        data = self.configuration_obj["configuration"]["source_configs"]
        create_cdata_source_payload = data.copy()
        create_cdata_source_payload["data_lake_schema"]= data["data_lake_schema"] if "data_lake_schema" in data else ""
        create_cdata_source_payload["environment_id"] = self.environment_id
        create_cdata_source_payload["storage_id"] = self.storage_id
        create_cdata_source_payload["is_source_ingested"]= True
        src_create_response = src_client_obj.create_source(source_config=create_cdata_source_payload)
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

    def configure_cdata_source_connection(self, src_client_obj, source_id, override_config_file=None,
                                          read_passwords_from_secrets=False, env_tag="", secret_type=""):
        source_configs = self.configuration_obj["configuration"]["source_configs"]
        src_name = str(source_configs["name"])
        connection_object = source_configs["connection"]
        connection_object["connection_mode"]="jdbc"
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

    def browse_source_tables(self, src_client_obj, source_id):
        filter_tables_properties = self.configuration_obj["filter_tables_properties"]
        response = src_client_obj.browse_source_tables(source_id, filter_tables_properties=filter_tables_properties,
                                                       poll_timeout=300, polling_frequency=15, retries=1)
        return response["result"]["status"].upper()

    def add_tables_to_source(self, src_client_obj, source_id):
        tables_already_added_in_source = src_client_obj.list_tables_in_source(source_id)["result"]["response"]
        tables_list = []
        tables = self.configuration_obj["configuration"]["table_configs"]
        if len(tables_already_added_in_source) > 0:
            for table in tables:
                if table["configuration"]["schema_name_at_source"] + "." + table["configuration"][
                    "name"] not in tables_already_added_in_source:
                    temp = {"table_name": table["configuration"]["name"],
                            "schema_name": table["configuration"]["schema_name_at_source"],
                            "table_type": table["entity_type"].upper(),
                            "target_table_name": table["configuration"]["configuration"]["target_table_name"],
                            "target_schema_name": table["configuration"]["configuration"]["target_schema_name"]}
                    if table["configuration"].get("catalog_name","")!="":
                        temp["catalog_name"]=table["configuration"]["catalog_name"]
                    tables_list.append(copy.deepcopy(temp))
                    src_client_obj.logger.info(
                        f"Adding table {temp['table_name']} to source {source_id} config payload")
        else:
            for table in tables:
                temp = {"table_name": table["configuration"]["name"],
                        "schema_name": table["configuration"]["schema_name_at_source"],
                        "table_type": table["entity_type"].upper(),
                        "target_table_name": table["configuration"]["configuration"]["target_table_name"],
                        "target_schema_name": table["configuration"]["configuration"]["target_schema_name"]}
                if table["configuration"].get("catalog_name", "") != "":
                    temp["catalog_name"] = table["configuration"]["catalog_name"]
                tables_list.append(copy.deepcopy(temp))
                src_client_obj.logger.info(f"Adding table {temp['table_name']} to source {source_id} config payload")
        response = src_client_obj.add_tables_to_source(source_id, tables_list)
        print(tables_list)
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
