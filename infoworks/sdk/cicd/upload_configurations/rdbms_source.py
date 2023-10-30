import copy
import json
import time

import requests
import yaml

from infoworks.sdk.url_builder import get_source_details_url, list_secrets_url, create_domain_url, \
    get_environment_interactive_compute_details, restart_persistent_cluster_url
from infoworks.sdk.utils import IWUtils
from infoworks.sdk.source_response import SourceResponse
from infoworks.sdk.local_configurations import Response
import configparser
from infoworks.sdk.cicd.upload_configurations.update_configurations import InfoworksDynamicAccessNestedDict
from infoworks.sdk.cicd.upload_configurations.local_configurations import PRE_DEFINED_MAPPINGS


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

    def get_secret_id_from_name(self, cicd_client, secret_name):
        secret_id = None
        get_secret_details_url = list_secrets_url(cicd_client.client_config) + '?filter={"name":"' + secret_name + '"}'
        response = cicd_client.call_api("GET", get_secret_details_url,
                                        IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token']))
        parsed_response = IWUtils.ejson_deserialize(response.content)
        if response.status_code == 200 and len(parsed_response.get("result", [])) > 0:
            result = parsed_response.get("result", [])

            if len(result) > 0:
                secret_id = result[0]["id"]
                cicd_client.logger.info("Found secret id {} ".format(secret_id))
                return secret_id
            else:
                cicd_client.logger.info("Secret id is {} ".format(None))
                return None

    def poll_cluster_start_status(self, cicd_client, environment_id, compute_id):
        list_of_persistent_cluster_url = get_environment_interactive_compute_details(cicd_client.client_config,
                                                                                     env_id=environment_id,
                                                                                     compute_id=compute_id)
        response = cicd_client.call_api("GET", list_of_persistent_cluster_url,
                                        IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token']))
        parsed_response = IWUtils.ejson_deserialize(response.content)
        return parsed_response.get("result", {}).get("cluster_state", "")

    def start_interactive_cluster(self, src_client_obj, environment_id, default_section_mappings):
        retries = 1
        ttl_for_cluster_status_fetch = default_section_mappings.get("ttl_for_cluster_status_fetch", 120)
        ttl_for_cluster_status_fetch = int(ttl_for_cluster_status_fetch) if ttl_for_cluster_status_fetch != "" else 120
        # print("ttl_for_cluster_status_fetch",ttl_for_cluster_status_fetch)
        while retries != 0:
            # added this because it takes about 120 seconds for ttl to expire and refresh the cluster status
            list_of_persistent_cluster_url = get_environment_interactive_compute_details(
                config=src_client_obj.client_config,
                env_id=environment_id) + '?filter={"interactive_job_submit_enabled":true}'
            response = src_client_obj.call_api("GET", list_of_persistent_cluster_url,
                                               IWUtils.get_default_header_for_v3(
                                                   src_client_obj.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(response.content)
            time.sleep(ttl_for_cluster_status_fetch)
            retries = retries - 1
        result = parsed_response.get("result", [])
        print("result", result)
        result = result[0]
        status = result.get("cluster_state", "")
        compute_id = result.get("id", "")
        environment_id = result.get("environment_id", "")
        if status in ["TERMINATED", "STOPPED"]:
            restart_cluster_template_body = \
                {
                    "compute_template_id": compute_id,
                    "type": "persistent"
                }
            start_cluster_url = restart_persistent_cluster_url(config=src_client_obj.client_config)
            response = src_client_obj.call_api("PUT", start_cluster_url,
                                               IWUtils.get_default_header_for_v3(
                                                   src_client_obj.client_config['bearer_token']),
                                               data=restart_cluster_template_body)
            parsed_response = IWUtils.ejson_deserialize(response.content)
            print("Restart persistent cluster response:")
            print(parsed_response)
            if parsed_response.get("result", {}).get("message", "") == "Successfully submitted cluster restart request":
                while (status not in ["RUNNING", "ERROR"]):
                    time.sleep(30)
                    status = self.poll_cluster_start_status(src_client_obj, environment_id=environment_id,
                                                            compute_id=compute_id)
            else:
                print("Something went wrong while restarting the interactive cluster")
            if status == "RUNNING":
                print("Started interactive cluster successfully!")
            else:
                print("Interactive cluster start failed!")
                raise Exception(
                    "Interactive cluster start failed!Please start the cluster manually and retrigger CICD process")
        elif status == "RUNNING":
            print("Interactive Cluster is already running!")
        else:
            print(f"Unknown cluster state {status}")

    def update_table_schema_and_database(self, type, mappings):
        data = self.configuration_obj
        for table in data.get("configuration", {}).get("table_configs", []):
            if type == "target_schema":
                schema_from_config = table.get("configuration", {}).get("configuration", {}).get("target_schema_name",
                                                                                                 "")
                if schema_from_config != "" and schema_from_config.lower() in mappings.keys():
                    table["configuration"]["configuration"]["target_schema_name"] = mappings.get(
                        schema_from_config.lower())
            elif type == "stage_schema":
                schema_from_config = table.get("configuration", {}).get("configuration", {}).get("staging_schema_name",
                                                                                                 "")
                if schema_from_config != "" and schema_from_config.lower() in mappings.keys():
                    table["configuration"]["configuration"]["staging_schema_name"] = mappings.get(
                        schema_from_config.lower())
            elif type == "database":
                database_from_config = table.get("configuration", {}).get("configuration", {}).get(
                    "target_database_name", "")
                if database_from_config != "" and database_from_config.lower() in mappings.keys():
                    table["configuration"]["configuration"]["target_database_name"] = mappings.get(
                        database_from_config.lower())
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
            self.update_table_schema_and_database("target_schema",
                                                  dict(config.items("configuration$source_configs$data_lake_schema")))
        if "configuration$source_configs$staging_schema_name" in config.sections():
            self.update_table_schema_and_database("stage_schema", dict(
                config.items("configuration$source_configs$staging_schema_name")))
        if "configuration$source_configs$target_database_name" in config.sections():
            self.update_table_schema_and_database("database", dict(
                config.items("configuration$source_configs$target_database_name")))

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
        if data.get("target_database_name", ""):
            create_rdbms_source_payload["target_database_name"] = data.get("target_database_name", "")
        if data.get("staging_schema_name", ""):
            create_rdbms_source_payload["staging_schema_name"] = data.get("staging_schema_name", "")
        additional_keys_in_source_config = data.keys()
        for key in additional_keys_in_source_config:
            if key not in ["connection"] and key not in create_rdbms_source_payload.keys():
                create_rdbms_source_payload[key] = data[key]
        # adding associated domains if any
        accessible_domain_names = data.get("associated_domain_names", [])
        accessible_domain_ids = []
        for domain_name in accessible_domain_names:
            domain_response = src_client_obj.call_api("GET",
                                                      create_domain_url(
                                                          src_client_obj.client_config) + "?filter={\"name\":\"" + domain_name + "\"}",
                                                      IWUtils.get_default_header_for_v3(
                                                          src_client_obj.client_config['bearer_token']))
            domain_parsed_response = IWUtils.ejson_deserialize(domain_response.content)
            if domain_response.status_code == 200 and len(domain_parsed_response.get("result", [])) > 0:
                result = domain_parsed_response.get("result", [])
                if len(result) > 0:
                    result = result[0]
                    domain_id = result.get("id", None)
                    if domain_id is not None:
                        accessible_domain_ids.append(domain_id)
            if "associated_domain_names" in create_rdbms_source_payload.keys():
                create_rdbms_source_payload.pop("associated_domain_names", [])
                self.configuration_obj["configuration"]["source_configs"].pop("associated_domain_names", [])
            if len(accessible_domain_ids) > 0:
                create_rdbms_source_payload["associated_domains"] = accessible_domain_ids
                self.configuration_obj["configuration"]["associated_domains"] = accessible_domain_ids
        print("create_rdbms_source_payload:", create_rdbms_source_payload)
        src_create_response = src_client_obj.create_source(source_config=create_rdbms_source_payload)
        if src_create_response["result"]["status"].upper() == "SUCCESS":
            source_id = src_create_response["result"]["response"]["result"]["id"]
            # added below code to update the source due to IPD-23733
            associated_domains = create_rdbms_source_payload.get("associated_domains", [])
            if associated_domains:
                src_client_obj.update_source(source_id=source_id,
                                             update_body={"associated_domains": associated_domains})
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
                return SourceResponse.parse_result(status=Response.Status.FAILED, source_id=None, response=response)
            else:
                existing_source_id = response['result'][0]['id']
                src_client_obj.logger.info(
                    f"Source Id with the same Source name {data['name']} : {response['result'][0]['id']}")
                print(f"Source Id with the same Source name {data['name']} : {response['result'][0]['id']}")
                # added below code to update the source due to IPD-23733
                associated_domains = create_rdbms_source_payload.get("associated_domains", [])
                if associated_domains:
                    src_client_obj.update_source(source_id=existing_source_id,
                                                 update_body={"associated_domains": associated_domains})
                return SourceResponse.parse_result(status=Response.Status.SUCCESS,
                                                   source_id=response['result'][0]['id'], response=response)

    def configure_rdbms_source_connection(self, src_client_obj, source_id, override_config_file=None,
                                          read_passwords_from_secrets=False, env_tag="", secret_type="",
                                          config_ini_path=None, dont_skip_step=True):
        if not dont_skip_step:
            return SourceResponse.parse_result(status="SKIPPED", source_id=source_id)
        source_configs = self.configuration_obj["configuration"]["source_configs"]
        src_name = str(source_configs["name"])
        connection_object = source_configs["connection"]
        if connection_object.get('connection_mode', '') != '':
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
        if connection_object.get("password", {}).get("password_type", "") == "secret_store":
            # for RDBMS passwords in keyvault
            secret_name = connection_object["password"]["secret_name"]
            secret_id = self.get_secret_id_from_name(src_client_obj, secret_name)
            if secret_name:
                connection_object["password"]["secret_id"] = secret_id
                connection_object["password"].pop('secret_name', None)
        # if read_passwords_from_secrets and self.secrets["custom_secrets_read"] is True:
        #     encrypted_key_name = f"{env_tag}-" + src_name
        #     decrypt_value = self.secrets.get(encrypted_key_name, "")
        #     if IWUtils.is_json(decrypt_value):
        #         decrypt_value_dict = json.loads(decrypt_value)
        #         for key in decrypt_value_dict.keys():
        #             connection_object[key] = decrypt_value_dict[key]
        # elif read_passwords_from_secrets and self.secrets["custom_secrets_read"] is False:
        #     encrypted_key_name = f"{env_tag}-" + src_name
        #     decrypt_value = src_client_obj.get_all_secrets(secret_type,keys=encrypted_key_name,ini_config_file_path=config_ini_path)
        #     if len(decrypt_value) > 0 and IWUtils.is_json(decrypt_value[0]):
        #         decrypt_value_dict = json.loads(decrypt_value[0])
        #         for key in decrypt_value_dict.keys():
        #             connection_object[key] = decrypt_value_dict[key]
        response = src_client_obj.configure_source_connection(source_id, connection_object=connection_object)
        if response["result"]["status"].upper() != "SUCCESS":
            src_client_obj.logger.info(f"Failed to configure the source {source_id} connection")
            print(f"Failed to configure the source {source_id} connection")
            src_client_obj.logger.info(response)
            print(response)
            return SourceResponse.parse_result(status=Response.Status.FAILED, source_id=source_id, response=response)
        else:
            src_client_obj.logger.info(response)
            print(response)
            return SourceResponse.parse_result(status=Response.Status.SUCCESS, source_id=source_id, response=response)

    def test_source_connection(self, src_client_obj, source_id, dont_skip_step=True):
        if not dont_skip_step:
            return SourceResponse.parse_result(status="SKIPPED", source_id=source_id)
        response = src_client_obj.source_test_connection_job_poll(source_id, poll_timeout=300,
                                                                  polling_frequency=15, retries=1)
        return SourceResponse.parse_result(status=Response.Status.SUCCESS, source_id=source_id, response=response)

    def browse_source_tables(self, src_client_obj, source_id, dont_skip_step=True):
        if not dont_skip_step:
            return SourceResponse.parse_result(status="SKIPPED", source_id=source_id)
        filter_tables_properties = self.configuration_obj["filter_tables_properties"]
        response = src_client_obj.browse_source_tables(source_id, filter_tables_properties=filter_tables_properties,
                                                       poll_timeout=300, polling_frequency=15, retries=1)
        return SourceResponse.parse_result(status=response["result"]["status"].upper(), source_id=source_id,
                                           response=response)

    def add_tables_to_source(self, src_client_obj, source_id, dont_skip_step=True):
        if not dont_skip_step:
            return SourceResponse.parse_result(status="SKIPPED", source_id=source_id)
        tables_already_added_in_source = src_client_obj.list_tables_in_source(source_id).get("result", {}).get(
            "response", {}).get("result", [])
        tables_already_added_in_source = [table.get("schema_name_at_source",
                                                    table.get("catalog_name",
                                                              "")) + "." + table[
                                              "name"] for table in tables_already_added_in_source]
        tables_list = []
        tables = self.configuration_obj["configuration"]["table_configs"]
        if len(tables_already_added_in_source) > 0:
            for table in tables:
                if table["configuration"].get("schema_name_at_source",
                                              table["configuration"].get("catalog_name", "")) + "." + \
                        table["configuration"][
                            "name"] not in tables_already_added_in_source:
                    temp = {"table_name": table["configuration"]["name"],
                            "target_table_name": table["configuration"]["configuration"]["target_table_name"],
                            "target_schema_name": table["configuration"]["configuration"]["target_schema_name"]}
                    if table["configuration"].get("catalog_name", "") != "":
                        temp["catalog_name"] = table["configuration"]["catalog_name"]
                    if table["configuration"].get("schema_name_at_source", "") != "":
                        temp["schema_name"] = table["configuration"]["schema_name_at_source"]
                    if table["configuration"].get("configuration", {}).get("configuration", {}).get(
                            "target_database_name", "") != "":
                        temp["target_database_name"] = table["configuration"]["configuration"]["target_database_name"]
                    if table.get("table_type", "") != "":
                        temp["table_type"] = table.get("table_type", "TABLE").upper()
                    tables_list.append(copy.deepcopy(temp))
                    src_client_obj.logger.info(
                        f"Adding table {temp['table_name']} to source {source_id} config payload")
        else:
            for table in tables:
                temp = {"table_name": table["configuration"]["name"],
                        "target_table_name": table["configuration"]["configuration"]["target_table_name"],
                        "target_schema_name": table["configuration"]["configuration"]["target_schema_name"]}
                if table["configuration"].get("catalog_name", "") != "":
                    temp["catalog_name"] = table["configuration"]["catalog_name"]
                if table["configuration"].get("schema_name_at_source", "") != "":
                    temp["schema_name"] = table["configuration"]["schema_name_at_source"]
                if table["configuration"].get("configuration", {}).get("configuration", {}).get("target_database_name",
                                                                                                "") != "":
                    temp["target_database_name"] = table["configuration"]["configuration"]["target_database_name"]
                if table.get("table_type", "") != "":
                    temp["table_type"] = table.get("table_type", "TABLE").upper()
                tables_list.append(copy.deepcopy(temp))
                src_client_obj.logger.info(f"Adding table {temp['table_name']} to source {source_id} config payload")
        if len(tables_list) > 0:
            response = src_client_obj.add_tables_to_source(source_id, tables_list)
            print("Add tables API response:",response)
            return SourceResponse.parse_result(status=response["result"]["status"].upper(), source_id=source_id,
                                               response=response)
        else:
            src_client_obj.logger.info(f"No new tables found to add.")
            print(f"No new tables found to add.")
            return SourceResponse.parse_result(status="SUCCESS", source_id=source_id,
                                               response={})

    def update_schema_for_tables(self, src_client_obj, source_id, export_configuration_file=None,
                                 export_config_lookup=True, mappings=None, read_passwords_from_secrets=False,
                                 env_tag="", secret_type="", dont_skip_step=True):
        if not dont_skip_step:
            return SourceResponse.parse_result(status="SKIPPED", source_id=source_id)
        tables = self.configuration_obj["configuration"]["table_configs"]
        table_schema_update_dict = {}
        for table in tables:
            table_name = table["configuration"]["name"]
            src_client_obj.logger.info(f"Updating the schema information for table {table_name}")
            columns = table["configuration"]["columns"]
            table_update_payload = {"name": table_name, "source": source_id, "columns": columns}
            if table["configuration"].get("schema_name_at_source", "") != "":
                table_document = src_client_obj.list_tables_in_source(source_id, params={
                    "filter": {"origTableName": table["configuration"]["name"],
                               "schemaNameAtSource": table["configuration"]["schema_name_at_source"]}}).get("result",
                                                                                                            {}).get(
                    "response", {}).get("result", [])
            else:
                table_document = src_client_obj.list_tables_in_source(source_id, params={
                    "filter": {"origTableName": table["configuration"]["name"],
                               "catalog_name": table["configuration"]["catalog_name"]}}).get("result", {}).get(
                    "response", {}).get("result", [])
            table_id = None
            if len(table_document) > 0:
                table_document = table_document[0]
                table_id = table_document["id"]
            if table_id is not None:
                response = src_client_obj.update_table_configuration(source_id=source_id, table_id=table_id,
                                                                     config_body=table_update_payload)
                if response["result"]["status"].upper() != "SUCCESS":
                    src_client_obj.logger.error(
                        "Failed to update schema for table {table_name}".format(table_name=table_name))
                    src_client_obj.logger.error(response.get("result", {}).get("response", {}).get("message", ""))
                    table_schema_update_dict[table_name] = (
                    "FAILED", response.get("result", {}).get("response", {}).get("message", ""))
                else:
                    src_client_obj.logger.info(
                        "Successfully updated schema for table {table_name}".format(table_name=table_name))
                    table_schema_update_dict[table_name] = ("SUCCESS", "")
        failed_schema_update_tables = [(table_name, status[1]) for table_name, status in
                                       table_schema_update_dict.items() if status[0].upper() == "FAILED"]
        overall_update_status = "FAILED" if len(failed_schema_update_tables) > 0 else "SUCCESS"
        if overall_update_status == "FAILED":
            response = {f"Tables schema update failed for tables:{failed_schema_update_tables}"}
        else:
            response = {f"Tables schema updated successfully"}
        return SourceResponse.parse_result(status=overall_update_status, source_id=source_id,
                                           response=response)

    def configure_tables_and_tablegroups(self, src_client_obj, source_id, export_configuration_file=None,
                                         export_config_lookup=True, mappings=None, read_passwords_from_secrets=False,
                                         env_tag="", secret_type="", dont_skip_step=True):
        if not dont_skip_step:
            return SourceResponse.parse_result(status="SKIPPED", source_id=source_id)
        if mappings is None:
            mappings = {}
        source_configs = self.configuration_obj["configuration"]["source_configs"]
        src_name = str(source_configs["name"])
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
            return SourceResponse.parse_result(status="FAILED", source_id=source_id,
                                               response=response)

        else:
            src_client_obj.logger.info(f"Successfully imported source configurations to {source_id}")
            return SourceResponse.parse_result(status=response["result"]["status"].upper(), source_id=source_id,
                                               response=response)
