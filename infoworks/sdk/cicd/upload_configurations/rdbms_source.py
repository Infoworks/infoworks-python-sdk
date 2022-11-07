import copy
import json

import requests
import yaml

from infoworks.sdk.url_builder import get_source_details_url
from infoworks.sdk.utils import IWUtils
from infoworks.sdk.source_response import SourceResponse
from infoworks.sdk.local_configurations import Response

class RDBMSSource:
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

    def create_rdbms_source(self, src_client_obj):
        data = self.configuration_obj["configuration"]["source_configs"]
        create_rdbms_source_payload = {
            "name": data["name"],
            "type": "rdbms",
            "sub_type": data["sub_type"],
            "data_lake_path": data["data_lake_path"],
            "data_lake_schema": data["data_lake_schema"] if "data_lake_schema" in data else "",
            "environment_id": self.environment_id,
            "storage_id": self.storage_id,
            "is_source_ingested": True
        }
        if data.get("target_database_name",""):
            create_rdbms_source_payload["target_database_name"] = data.get("target_database_name","")
        src_create_response = src_client_obj.create_source(source_config=create_rdbms_source_payload)
        if src_create_response["result"]["status"].upper() == "SUCCESS":
            return src_create_response
        else:
            src_client_obj.logger.info('Cant create source {} '.format(data["name"]))
            print('Cant create source {} '.format(data["name"]))
            print(src_create_response)
            src_client_obj.logger.info(f"Getting the existing SourceId with name {data['name']} if exists")
            print(f"Getting the existing SourceId with name {data['name']} if exists")
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
                print("Failed to make an api call to get source details")
                print(response)
                return SourceResponse.parse_result(status=Response.Status.FAILED, source_id=None,response=response)
            else:
                src_client_obj.logger.info(
                    f"Source Id with the same Source name {data['name']} : {response['result'][0]['id']}")
                print(f"Source Id with the same Source name {data['name']} : {response['result'][0]['id']}")
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, source_id=response['result'][0]['id'],response=response)

    def configure_rdbms_source_connection(self, src_client_obj, source_id, override_config_file=None,
                                          read_passwords_from_secrets=False, env_tag="", secret_type="",config_ini_path=None):
        source_configs = self.configuration_obj["configuration"]["source_configs"]
        src_name = str(source_configs["name"])
        connection_object = source_configs["connection"]
        if connection_object.get('connection_mode','')!='':
            connection_object['connection_mode'] = connection_object['connection_mode'].lower()
        else:
            connection_object['connection_mode'] = 'jdbc'
        if override_config_file is not None:
            with open(override_config_file) as file:
                information = yaml.load(file, Loader=yaml.FullLoader)
            if information["source_details"].get(src_name, None) is not None:
                override_keys = information["source_details"].get(src_name).keys()
                for key in override_keys:
                    connection_object[key] = information["source_details"][src_name][key]
        if read_passwords_from_secrets and self.secrets["custom_secrets_read"] is True:
            encrypted_key_name = f"{env_tag}-" + src_name
            decrypt_value = self.secrets.get(encrypted_key_name, "")
            if IWUtils.is_json(decrypt_value):
                decrypt_value_dict = json.loads(decrypt_value)
                for key in decrypt_value_dict.keys():
                    connection_object[key] = decrypt_value_dict[key]
        elif read_passwords_from_secrets and self.secrets["custom_secrets_read"] is False:
            encrypted_key_name = f"{env_tag}-" + src_name
            decrypt_value = src_client_obj.get_all_secrets(secret_type,keys=encrypted_key_name,ini_config_file_path=config_ini_path)
            if len(decrypt_value) > 0 and IWUtils.is_json(decrypt_value[0]):
                decrypt_value_dict = json.loads(decrypt_value[0])
                for key in decrypt_value_dict.keys():
                    connection_object[key] = decrypt_value_dict[key]

        response = src_client_obj.configure_source_connection(source_id, connection_object=connection_object)
        if response["result"]["status"].upper() != "SUCCESS":
            src_client_obj.logger.info(f"Failed to configure the source {source_id} connection")
            print(f"Failed to configure the source {source_id} connection")
            src_client_obj.logger.info(response)
            print(response)
            return SourceResponse.parse_result(status=Response.Status.FAILED, source_id=source_id,response=response)
        else:
            src_client_obj.logger.info(response)
            print(response)
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, source_id=source_id,response=response)

    def test_source_connection(self, src_client_obj, source_id):
        response = src_client_obj.source_test_connection_job_poll(source_id, poll_timeout=300,
                                                                  polling_frequency=15, retries=1)
        return SourceResponse.parse_result(status=Response.Status.SUCCESS, source_id=source_id,response=response)

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
                    if target_type.upper() in ["SNOWFLAKE", "POSTGRES"] and read_passwords_from_secrets:
                        if self.secrets["custom_secrets_read"] is True:
                            encrypted_key_name1 = f"{env_tag}-export-configuration-{src_name}-{table['configuration']['name']}"
                            encrypted_key_name2 = f"{env_tag}-export-configuration-{src_name}"
                            decrypt_value = self.secrets.get(encrypted_key_name1, "")
                            if decrypt_value == "" or decrypt_value is None:
                                decrypt_value = self.secrets.get(encrypted_key_name2, "")
                            decrypt_value_dict = json.loads(decrypt_value)
                            for key in decrypt_value_dict.keys():
                                table["configuration"]["export_configuration"]["connection"][key] = \
                                    decrypt_value_dict[key]
                        else:
                            encrypted_key_name1 = f"{env_tag}-export-configuration-{src_name}-{table['configuration']['name']}"
                            encrypted_key_name2 = f"{env_tag}-export-configuration-{src_name}"
                            decrypt_value = src_client_obj.get_all_secrets(secret_type, keys=encrypted_key_name1)
                            if len(decrypt_value) == 0:
                                decrypt_value = src_client_obj.get_all_secrets(secret_type, keys=encrypted_key_name2)
                            if len(decrypt_value) > 0 and IWUtils.is_json(decrypt_value[0]):
                                decrypt_value_dict = json.loads(decrypt_value[0])
                                for key in decrypt_value_dict.keys():
                                    table["configuration"]["export_configuration"]["connection"][key] = \
                                            decrypt_value_dict[key]
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
