import json
import requests
import yaml
from infoworks.sdk.url_builder import get_source_details_url
from infoworks.sdk.utils import IWUtils
from infoworks.sdk.source_response import SourceResponse
from infoworks.sdk.local_configurations import Response

class CSVSource:
    def __init__(self, environment_id, storage_id, source_config_path, secrets=None, replace_words=""):
        self.storage_id = storage_id
        self.environment_id = environment_id
        self.source_config_path = source_config_path
        self.secrets = secrets
        with open(self.source_config_path, 'r') as file:
            json_string = file.read()
            if replace_words != "":
                for key, value in [item.split("->") for item in replace_words.split(";")]:
                    json_string = json_string.replace(key, value)
        self.configuration_obj = IWUtils.ejson_deserialize(json_string)

    def create_csv_source(self, src_client_obj):
        data = self.configuration_obj["configuration"]["source_configs"]
        create_csv_source_payload = {
            "name": data["name"],
            "type": data["type"],
            "sub_type": data["sub_type"],
            "data_lake_path": data["data_lake_path"],
            "data_lake_schema": data["data_lake_schema"] if "data_lake_schema" in data else "",
            "environment_id": self.environment_id,
            "storage_id": self.storage_id,
            "is_source_ingested": True
        }
        if data.get("target_database_name",""):
            create_csv_source_payload["target_database_name"] = data.get("target_database_name","")
        src_create_response = src_client_obj.create_source(source_config=create_csv_source_payload)
        if src_create_response["result"]["status"].upper() == "SUCCESS":
            source_id_created = src_create_response["result"]["source_id"]
            print("Source created successfully")
            return src_create_response
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
                print("Failed to make an api call to get source details")
                src_client_obj.logger.info(response)
                print(response)
                return SourceResponse.parse_result(status=Response.Status.FAILED,
                                                   source_id=None,response=response)
            else:
                src_client_obj.logger.info(
                    f"Source Id with the same Source name {data['name']} : {response['result'][0]['id']}")
                print(f"Source Id with the same Source name {data['name']} : {response['result'][0]['id']}")
                return SourceResponse.parse_result(status=Response.Status.SUCCESS, source_id=response['result'][0]['id'],response=response)

    def configure_csv_source(self, src_client_obj, source_id, mappings, read_passwords_from_secrets=False, env_tag="",
                             secret_type="",config_ini_path=None):
        data = self.configuration_obj["configuration"]["source_configs"]["connection"]
        src_name = str(self.configuration_obj["configuration"]["source_configs"]["name"])
        storage_type = data["storage"]["storage_type"]
        cloud_type = data["storage"].get("cloud_type", None)
        if storage_type == "cloud" and cloud_type == "s3":
            access_id = data["storage"].get("access_id", "")
            secret_key = data["storage"].get("secret_key", "")
            if "source_secrets" in mappings:
                access_id = mappings.get("source_secrets").get("access_id", access_id)
                secret_key = mappings.get("source_secrets").get("secret_key", secret_key)
            if read_passwords_from_secrets and self.secrets["custom_secrets_read"] is True:
                encrypted_key_name = f"{env_tag}-" + src_name
                decrypt_value = self.secrets.get(encrypted_key_name, "")
                if IWUtils.is_json(decrypt_value):
                    decrypt_value_dict = json.loads(decrypt_value)
                    access_id = decrypt_value_dict["access_id"]
                    secret_key = decrypt_value_dict["secret_key"]
            elif read_passwords_from_secrets and self.secrets["custom_secrets_read"] is False:
                encrypted_key_name = f"{env_tag}-" + src_name
                decrypt_value = src_client_obj.get_all_secrets(secret_type, keys=encrypted_key_name,ini_config_file_path=config_ini_path)
                if len(decrypt_value) > 0 and IWUtils.is_json(decrypt_value[0]):
                    decrypt_value_dict = json.loads(decrypt_value[0])
                    access_id = decrypt_value_dict["access_id"]
                    secret_key = decrypt_value_dict["secret_key"]

            source_configure_payload = {
                "source_base_path_relative": data.get("source_base_path_relative",""),
                "source_base_path": data.get("source_base_path",""),
                "storage": {
                    "storage_type": data["storage"]["storage_type"],
                    "cloud_type": data["storage"]["cloud_type"],
                    "access_id": access_id,
                    "secret_key": secret_key,
                    "account_type": data["storage"]["account_type"],
                    "access_type": data["storage"]["access_type"]
                }
            }
        elif storage_type == "cloud" and cloud_type == "wasb":
            source_configure_payload = {}
            pass
        elif storage_type == "cloud" and "project_id" in data["storage"]:
            project_id = data["storage"]["project_id"]
            server_path = data["storage"]["server_path"]
            if "gcp_details" in mappings:
                project_id = mappings["gcp_details"].get("project_id", project_id)
                server_path = mappings["gcp_details"].get("service_json_path", server_path)
            if "gcp_project_id_mappings" in mappings:
                project_id = mappings["gcp_project_id_mappings"].get(data["storage"]["project_id"], project_id)
            if "service_json_mappings" in mappings:
                server_path = mappings["service_json_mappings"].get(data["storage"]["server_path"].split("/")[-1],
                                                                    server_path)

            source_configure_payload = {
                "source_base_path_relative": data.get("source_base_path_relative",""),
                "source_base_path": data.get("source_base_path",""),
                "storage": {
                    "cloud_type": "gs",
                    "storage_type": data["storage"]["storage_type"],
                    "project_id": project_id,
                    "access_type": data["storage"]["access_type"],
                    "server_path": server_path,
                    "upload_option": data["storage"]["upload_option"],
                    "file_details": []
                }
            }
        elif storage_type == "remote":
            # SFTP Source
            data = self.configuration_obj["configuration"]["source_configs"]["connection"]
            source_configure_payload = data
            source_configure_payload["source_base_path"]=""
        else:
            source_configure_payload = {}

        response = src_client_obj.configure_source_connection(source_id, connection_object=source_configure_payload)
        return response

    def import_source_configuration(self, src_client_obj, source_id,
                                    mappings, export_configuration_file=None, export_config_lookup=True,
                                    read_passwords_from_secrets=False):
        src_name = self.configuration_obj["configuration"]["source_configs"]["name"]
        table_group_compute_mappings = mappings.get("table_group_compute_mappings", {})
        source_import_payload = {"configuration": self.configuration_obj["configuration"]}
        modified_table_configs = self.configuration_obj["configuration"]["table_configs"]
        index = 0
        for table_config in self.configuration_obj["configuration"]["table_configs"]:
            modified_table_configs[index]["configuration"]["meta_crawl_performed"] = True
            if not table_config["configuration"].get("meta_crawl_performed", False):
                modified_table_configs[index]["configuration"]["meta_crawl_performed"] = True
                index = index + 1
            else:
                if not table_config["configuration"]["meta_crawl_performed"]:
                    modified_table_configs[index]["configuration"]["meta_crawl_performed"] = True
                index = index + 1
        source_import_payload["configuration"]["table_configs"] = modified_table_configs
        iw_mappings = source_import_payload["configuration"]["iw_mappings"]
        for item in iw_mappings:
            if item.get("entity_type", "") == "environment_compute_template":
                item["recommendation"]["compute_name"] = table_group_compute_mappings.get(
                    item["recommendation"]["compute_name"], item["recommendation"]["compute_name"])
        source_import_payload["configuration"]["iw_mappings"] = iw_mappings
        if export_config_lookup and (export_configuration_file is not None or read_passwords_from_secrets):
            for table in source_import_payload.get("configuration")["table_configs"]:
                # Check if there are any export configurations and passwords to replace
                if table.get("configuration", {}).get("export_configuration", None) is not None:
                    export_configs = table.get("configuration", {}).get("export_configuration")
                    target_type = export_configs.get("target_type", "").upper()
                    table_name = table["configuration"]["name"].upper()
                    override_keys = []
                    try:
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
                    except Exception as e:
                        src_client_obj.logger.error(
                            f"Failed to lookup the export configuration file {export_configuration_file} due to {str(e)}")

                    try:
                        if target_type.upper() in ["SNOWFLAKE", "POSTGRES"] and read_passwords_from_secrets:
                            # Read the password from KMS
                            encrypted_key_name1 = f"export-configuration-{src_name}-{table['configuration']['name']}"
                            encrypted_key_name2 = f"export-configuration-{src_name}"
                            passwd = self.secrets.get(encrypted_key_name1, "")
                            if passwd == "":
                                passwd = self.secrets.get(encrypted_key_name2, "")
                            table["configuration"]["export_configuration"]["connection"]["password"] = passwd
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
                    except Exception as e:
                        src_client_obj.logger.error(
                            f"Failed to lookup the export configuration password from secrets due to {str(e)}")
        response = src_client_obj.configure_tables_and_tablegroups(source_id,
                                                                   configuration_obj=source_import_payload.get(
                                                                       "configuration"))
        return response
        # if response["result"]["status"].upper() != "SUCCESS":
        #     src_client_obj.logger.error("Failed to import the source {} (source config path : {})"
        #                                 .format(source_id, self.source_config_path))
        #     src_client_obj.logger.error(response.get("message", "") + "(source config path : {})"
        #                                 .format(self.source_config_path))
        #     return "FAILED"
        # else:
        #     src_client_obj.logger.info(f"Successfully imported source configurations to {source_id}")
        #     return "SUCCESS"
