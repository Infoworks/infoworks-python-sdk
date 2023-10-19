import copy
import os
import traceback

import jwt

from infoworks.sdk.utils import IWUtils
from infoworks.sdk.url_builder import get_parent_entity_url, list_domains_url, configure_pipeline_url, \
    configure_workflow_url, \
    configure_source_url, get_environment_details, get_environment_storage_details, get_environment_compute_details, \
    get_environment_interactive_compute_details, get_source_configurations_url, get_pipeline_url, \
    get_data_connection, source_info, list_users_url, list_secrets_url, get_pipeline_group_base_url, list_pipelines_url, \
    create_domain_url
from infoworks.sdk.cicd.cicd_response import CICDResponse
import json


class Utils:
    def __init__(self, serviceaccountemail):
        self.serviceaccountemail = serviceaccountemail

    def get_secret_name_from_id(self, cicd_client, secret_id):
        secret_name = None
        get_secret_details_url = list_secrets_url(cicd_client.client_config) + '?filter={"_id":"' + secret_id + '"}'
        response = cicd_client.call_api("GET", get_secret_details_url,
                                        IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token']))
        parsed_response = IWUtils.ejson_deserialize(response.content)
        if response.status_code == 200 and len(parsed_response.get("result", [])) > 0:
            result = parsed_response.get("result", [])

            if len(result) > 0:
                secret_name = result[0]["name"]
                cicd_client.logger.info("Found secret name {} ".format(secret_name))
                return secret_name
            else:
                cicd_client.logger.info("Secret Name is {} ".format(None))
                return None

    def get_domain_id(self, cicd_client, json_obj):
        parent_entity_url = get_parent_entity_url(cicd_client.client_config)
        response = cicd_client.call_api("GET", parent_entity_url,
                                        IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token']),
                                        data=json_obj)
        parsed_response = IWUtils.ejson_deserialize(response.content)
        if response.status_code == 200 and len(parsed_response.get("result", [])) > 0:
            result = parsed_response.get("result", [])
            if len(result) > 0:
                domain_id = result[0]["entity_id"]
                cicd_client.logger.info("Domain ID is {} ".format(domain_id))
                return domain_id
            else:
                cicd_client.logger.info("Domain ID is {} ".format(None))
                return None

    def get_env_entities_names(self, cicd_client, environment_id, environment_compute_template_id,
                               environment_storage_id):
        env_name, storage_name, compute_name = None, None, None
        if environment_id:
            response = cicd_client.call_api("GET", get_environment_details(cicd_client.client_config, environment_id),
                                            IWUtils.get_default_header_for_v3(
                                                cicd_client.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(response.content)
            if response.status_code == 200:
                result = parsed_response.get("result", None)
                if result:
                    env_name = result["name"]
        if environment_storage_id:
            response = cicd_client.call_api("GET",
                                            get_environment_storage_details(cicd_client.client_config, environment_id,
                                                                            environment_storage_id),
                                            IWUtils.get_default_header_for_v3(
                                                cicd_client.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(response.content)
            if response.status_code == 200:
                result = parsed_response.get("result", None)
                if result:
                    storage_name = result["name"]
        if environment_compute_template_id:
            response = cicd_client.call_api("GET",
                                            get_environment_compute_details(cicd_client.client_config, environment_id,
                                                                            environment_compute_template_id),
                                            IWUtils.get_default_header_for_v3(
                                                cicd_client.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(response.content)
            if response.status_code == 200:
                result = parsed_response.get("result", None)
                if result:
                    compute_name = result["name"]
                else:
                    response = cicd_client.call_api("GET",
                                                    get_environment_interactive_compute_details(
                                                        cicd_client.client_config,
                                                        environment_id,
                                                        environment_compute_template_id),
                                                    IWUtils.get_default_header_for_v3(
                                                        cicd_client.client_config['bearer_token']))
                    parsed_response = IWUtils.ejson_deserialize(response.content)
                    if response.status_code == 200:
                        result = parsed_response.get("result", None)
                        if result:
                            compute_name = result["name"]

        return env_name, storage_name, compute_name

    def get_env_details(self, cicd_client, entity_id, entity_type, domain_id=None):
        environment_id, compute_template_id, storage_id = None, None, None
        if entity_type == "pipeline":
            get_pipeline_details_url = get_pipeline_url(cicd_client.client_config, domain_id, entity_id)
            response = cicd_client.call_api("GET", get_pipeline_details_url,
                                            IWUtils.get_default_header_for_v3(
                                                cicd_client.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(response.content)
            if response.status_code == 200 and len(parsed_response.get("result", [])) > 0:
                result = parsed_response.get("result", [])
                if len(result) > 0:
                    environment_id = result.get("environment_id", None)
                    storage_id = result.get("storage_id", None)
                    compute_template_id = result.get("compute_template_id", None)
                    cicd_client.logger.info(
                        "Environment ID is {} Storage ID is {} Compute Template Id is {}".format(environment_id,
                                                                                                 storage_id,
                                                                                                 compute_template_id))
        return environment_id, compute_template_id, storage_id

    def get_dataconnection_properties(self, sub_type, properties_obj):
        final_obj = {}
        if sub_type.lower() == "snowflake":
            for key in properties_obj.keys():
                if key == "user_name":
                    final_obj["username"] = properties_obj["user_name"]
                else:
                    final_obj[key] = properties_obj[key]
        elif sub_type.lower() == "postgres":
            for key in properties_obj.keys():
                if key == "user_name":
                    final_obj["username"] = properties_obj["user_name"]
                else:
                    final_obj[key] = properties_obj[key]
        elif sub_type.lower() == "bigquery":
            for key in properties_obj.keys():
                if key == "uploadOption":
                    final_obj["upload_option"] = properties_obj["uploadOption"]
                elif key == "serverPath":
                    final_obj["server_path"] = properties_obj["serverPath"]
                else:
                    final_obj[key] = properties_obj[key]
            if "file_details" in final_obj:
                del final_obj["file_details"]

        return final_obj

    def get_sql_pipeline_config(self, cicd_client, domain_id, pipeline_id, target_file_path):
        ip = cicd_client.client_config['ip']
        port = cicd_client.client_config['port']
        protocol = cicd_client.client_config['protocol']

        get_domain_name_url = f"{protocol}://{ip}:{port}/v3/domains/{domain_id}"
        response = cicd_client.call_api("GET", get_domain_name_url,
                                        IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token']))
        parsed_response = IWUtils.ejson_deserialize(response.content)
        domain_name = parsed_response.get("result", {}).get("name", "")

        get_active_pipeline_versions_url = f"{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/"
        response = cicd_client.call_api("GET", get_active_pipeline_versions_url, IWUtils.get_default_header_for_v3(
            cicd_client.client_config['bearer_token']))
        parsed_response = IWUtils.ejson_deserialize(response.content)
        pipeline_details = parsed_response.get("result", {})
        pipeline_name = pipeline_details.get("name", "")
        query_tag = pipeline_details.get("query_tag", "")
        custom_tags = pipeline_details.get("custom_tags", [])
        description = pipeline_details.get("description", "")
        filename = f"{domain_name}#pipeline_{pipeline_name}.json"

        active_pipeline_version = pipeline_details.get("active_version_id")

        get_pipeline_version_config_url = f"{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/versions/{active_pipeline_version}"
        response = cicd_client.call_api("GET", get_pipeline_version_config_url, IWUtils.get_default_header_for_v3(
            cicd_client.client_config[
                'bearer_token']))
        parsed_response = IWUtils.ejson_deserialize(response.content)
        pipeline_version_details = parsed_response.get("result", {})
        version_id = pipeline_version_details.get("version", 1)
        query = pipeline_version_details.get("query", "")
        pipeline_parameters = pipeline_version_details.get("pipeline_parameters", [])
        # Check for any advance configurations
        response = cicd_client.call_api("GET", get_active_pipeline_versions_url + "configurations/advance",
                                        IWUtils.get_default_header_for_v3(
                                            cicd_client.client_config[
                                                'bearer_token']))
        parsed_response = IWUtils.ejson_deserialize(response.content)
        pipeline_advance_config_details = parsed_response.get("result", [])

        environment_id, environment_compute_template_id, environment_storage_id = self.get_env_details(
            cicd_client, pipeline_id,
            "pipeline",
            domain_id)
        env_name, storage_name, compute_name = self.get_env_entities_names(
            cicd_client, environment_id,
            environment_compute_template_id,
            environment_storage_id)

        decoded_jwt = jwt.decode(cicd_client.client_config['bearer_token'], options={"verify_signature": False})
        user_email = json.loads(decoded_jwt.get("sub")).get("email")
        config_obj = {
            "configuration": {
                "entity": {
                    "entity_type": "pipeline",
                    "entity_id": pipeline_id,
                    "entity_name": pipeline_details.get("name"),
                    "subEntityName": f"V{version_id}",
                    "warehouse": pipeline_details.get("snowflake_warehouse")
                },
                "pipeline_configs": {
                    "type": "sql",
                    "query": query,
                    "pipeline_parameters": pipeline_parameters,
                    "batch_engine": "SNOWFLAKE",
                    "query_tag": query_tag,
                    "description": description
                },
                "pipeline_advance_configs": pipeline_advance_config_details
            },
            "environment_configurations": {"environment_name": env_name,
                                           "environment_compute_template_name": compute_name,
                                           "environment_storage_name": storage_name},
            "user_email": user_email
        }
        target_file_path = os.path.join(target_file_path, "pipeline", filename)
        if filename is not None and target_file_path is not None:
            cicd_client.logger.info("{} {}".format(filename, target_file_path))
            print(f"Exporting configurations file to {target_file_path}")
            with open(target_file_path, 'w') as file_ptr:
                #contents_to_write = IWUtils.ejson_serialize(config_obj)
                json.dump(config_obj,file_ptr,indent=4)
            cicd_client.logger.info("Configurations exported successfully")
            print("Configurations exported successfully")
        return filename, config_obj

    def list_pipelines(self,cicd_client, domain_id=None, params=None):
        """
        Function to list the pipelines
        :param domain_id: Entity identified for domain
        :type domain_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response list
        """

        if None in {domain_id}:
            cicd_client.logger.error("Domain ID cannot be None")
            raise Exception("Domain ID cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_pipelines = list_pipelines_url(cicd_client.client_config, domain_id) \
                                + IWUtils.get_query_params_string_from_dict(params=params)

        pipelines_list = []
        try:
            response = IWUtils.ejson_deserialize(
                cicd_client.call_api("GET", url_to_list_pipelines,
                              IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                initial_msg = response.get("message", "")
                while len(result) > 0:
                    pipelines_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=cicd_client.client_config['ip'],
                                                                      port=cicd_client.client_config['port'],
                                                                      protocol=cicd_client.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        cicd_client.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            cicd_client.client_config['bearer_token'])).content)
                    result = response.get("result", None)
                    if result is None:
                        return response
                response["result"] = pipelines_list
                response["message"] = initial_msg
            return response
        except Exception as e:
            cicd_client.logger.error("Error in listing pipelines")
            raise Exception("Error in listing pipelines" + str(e))

    def dump_to_file(self, cicd_client, entity_type, domain_id, entity_id, replace_words, target_file_path):
        response_to_return = {}
        filename = None
        environment_id, environment_compute_template_id, environment_storage_id = None, None, None
        if entity_type == "pipeline":
            url_to_config = configure_pipeline_url(cicd_client.client_config, domain_id, entity_id)
        elif entity_type == "workflow":
            url_to_config = configure_workflow_url(cicd_client.client_config, domain_id, entity_id)
        elif entity_type == "source":
            url_to_config = configure_source_url(cicd_client.client_config, entity_id)
        elif entity_type == "pipeline_group":
            url_to_config = get_pipeline_group_base_url(cicd_client.client_config, domain_id) + f"{entity_id}"
        else:
            return None
        cicd_client.logger.info(
            "URL to get configurations of entity {} of type {} is {}".format(entity_id, entity_type, url_to_config))
        response = cicd_client.call_api("GET", url_to_config, IWUtils.get_default_header_for_v3(
            cicd_client.client_config[
                'bearer_token']))

        parsed_response = IWUtils.ejson_deserialize(response.content)

        if response.status_code == 200:
            status = "SUCCESS"
        #removing below elif because sql pipeline migration is supported with config-migration api from 5.5
        #elif response.status_code == 406:
        #    return self.get_sql_pipeline_config(cicd_client, domain_id, entity_id, target_file_path)
        else:
            status = "FAILED"
            print(parsed_response)

        response_to_return["get_configuration_entity_response"] = CICDResponse.parse_result(status=status,
                                                                                            entity_id=entity_id,
                                                                                            response=parsed_response)
        configuration_obj = parsed_response.get('result', {})
        if len(configuration_obj) > 0:
            if entity_type == "source":
                get_src_details_url = source_info(cicd_client.client_config, entity_id)
                response = cicd_client.call_api("GET", get_src_details_url,
                                                IWUtils.get_default_header_for_v3(
                                                    cicd_client.client_config['bearer_token']))
                parsed_response = IWUtils.ejson_deserialize(response.content)
                if response.status_code == 200:
                    status = "SUCCESS"
                else:
                    status = "FAILED"
                    print("Get Source Info failed " + json.dumps(response))
                response_to_return["get_source_details_response"] = CICDResponse.parse_result(status=status,
                                                                                              entity_id=entity_id,
                                                                                              response=parsed_response)
                if response.status_code == 200 and len(parsed_response.get("result", [])) > 0:
                    result = parsed_response.get("result", [])
                    if len(result) > 0:
                        data_lake_path = result["data_lake_path"]
                        configuration_obj["configuration"]["source_configs"]["data_lake_path"] = data_lake_path
                        configuration_obj["filter_tables_properties"] = result.get("filter_tables_properties", {})
                        source_connection_objects = configuration_obj["configuration"]["source_configs"]["connection"]
                        if source_connection_objects.get("storage", None) is not None:
                            # for File based sources
                            if source_connection_objects.get("storage", {}).get("password", {}).get("password_type",
                                                                                                    "") == "secret_store":
                                # for SFTP password auth
                                secret_id = source_connection_objects["storage"]["password"]["secret_id"]
                                secret_name = self.get_secret_name_from_id(cicd_client, secret_id)
                                if secret_name:
                                    source_connection_objects["storage"]["password"]["secret_name"] = secret_name
                            elif source_connection_objects.get("storage", {}).get("access_key_name", {}).get(
                                    "password_type", "") == "secret_store":
                                # for adls gen2 storage account access key auth
                                secret_id = source_connection_objects["storage"]["access_key_name"]["secret_id"]
                                secret_name = self.get_secret_name_from_id(cicd_client, secret_id)
                                if secret_name:
                                    source_connection_objects["storage"]["access_key_name"]["secret_name"] = secret_name
                            elif source_connection_objects.get("storage", {}).get("service_credential", {}).get(
                                    "password_type", "") == "secret_store":
                                # for adls gen2 service credential auth
                                secret_id = source_connection_objects["storage"]["service_credential"]["secret_id"]
                                secret_name = self.get_secret_name_from_id(cicd_client, secret_id)
                                if secret_name:
                                    source_connection_objects["storage"]["service_credential"][
                                        "secret_name"] = secret_name
                            elif source_connection_objects.get("storage", {}).get("account_key", {}).get(
                                    "password_type", "") == "secret_store":
                                # for blob storage account key auth
                                secret_id = source_connection_objects["storage"]["account_key"]["secret_id"]
                                secret_name = self.get_secret_name_from_id(cicd_client, secret_id)
                                if secret_name:
                                    source_connection_objects["storage"]["account_key"]["secret_name"] = secret_name
                            else:
                                pass
                        else:
                            # for RDBMS sources
                            if source_connection_objects.get("password", {}).get("password_type", "") == "secret_store":
                                # for SFTP password auth
                                secret_id = source_connection_objects["password"]["secret_id"]
                                secret_name = self.get_secret_name_from_id(cicd_client, secret_id)
                                if secret_name:
                                    source_connection_objects["password"]["secret_name"] = secret_name
                        # handle associated domains
                        if configuration_obj.get("configuration", {}).get("source_configs", {}).get(
                                "associated_domains", None) is None:
                            associated_domains = result.get("associated_domains", [])
                            if associated_domains:
                                configuration_obj["configuration"]["source_configs"][
                                    "associated_domains"] = associated_domains
                # Check if any table has export configurations. Works for postgres/snowflake/synapse
                # Did not test for cosmos,delimited
                for table_config in configuration_obj["configuration"]["table_configs"]:
                    if len(table_config["configuration"].get("export_configuration", [])) > 0:
                        if table_config["configuration"]["export_configuration"].get("target_type", "") in ["SNOWFLAKE",
                                                                                                            "POSTGRES",
                                                                                                            "AZURE_SQL_DW"]:
                            table_config["configuration"]["export_configuration"]["connection"]["password"] = None
                        else:
                            pass  # TO_DO

                # add domain names to mapped domain ids
                accessible_domain_ids = configuration_obj["configuration"]["source_configs"].get("associated_domains",
                                                                                                 [])
                accessible_domain_names = []
                for domain_id in accessible_domain_ids:
                    domain_id_response = cicd_client.call_api("GET",
                                                              create_domain_url(
                                                                  cicd_client.client_config) + "/" + domain_id,
                                                              IWUtils.get_default_header_for_v3(
                                                                  cicd_client.client_config['bearer_token']))
                    domain_id_parsed_response = IWUtils.ejson_deserialize(domain_id_response.content)
                    # print(domain_id_parsed_response)
                    if domain_id_response.status_code == 200 and len(domain_id_parsed_response.get("result", [])) > 0:
                        result = domain_id_parsed_response.get("result", [])
                        if len(result) > 0:
                            domain_name = result.get("name", None)
                            if domain_name is not None:
                                accessible_domain_names.append(domain_name)
                    if len(accessible_domain_names) > 0:
                        configuration_obj["configuration"]["source_configs"][
                            "associated_domain_names"] = accessible_domain_names

                steps_to_run = {"configure_csv_source": True, "import_source_configuration": True,
                                "configure_rdbms_source_connection": True,
                                "test_source_connection": True, "browse_source_tables": True,
                                "add_tables_to_source": True,
                                "configure_tables_and_tablegroups": True}
                configuration_obj["steps_to_run"] = steps_to_run
                filename = "{}_".format(entity_type) + configuration_obj["configuration"]["entity"][
                    "entity_name"] + ".json"
                target_file_path = os.path.join(target_file_path, entity_type, filename)
                response = cicd_client.call_api("GET",
                                                get_source_configurations_url(cicd_client.client_config, entity_id),
                                                IWUtils.get_default_header_for_v3(
                                                    cicd_client.client_config['bearer_token']))
                parsed_response = IWUtils.ejson_deserialize(response.content)
                if response.status_code == 200:
                    status = "SUCCESS"
                    result = parsed_response.get("result", None)
                    if result:
                        environment_id = result["environment_id"]
                        environment_storage_id = result["storage_id"]
                else:
                    status = "FAILED"
                    print("Get Source Configurations failed " + json.dumps(response))
                response_to_return["get_source_configurations_response"] = CICDResponse.parse_result(status=status,
                                                                                                     entity_id=entity_id,
                                                                                                     response=parsed_response)
            else:
                entity_name = configuration_obj["configuration"]["entity"][
                    "entity_name"] if entity_type != "pipeline_group" else configuration_obj["name"]
                if entity_type == "pipeline":
                    environment_id, environment_compute_template_id, environment_storage_id = self.get_env_details(
                        cicd_client, entity_id,
                        entity_type,
                        domain_id)
                    if any([environment_id, environment_compute_template_id, environment_storage_id]):
                        status = "SUCCESS"
                    else:
                        status = "FAILED"
                        print("Get Env Details failed " + json.dumps(response))
                    response = f"Found environment details: environment_id {environment_id}, environment_compute_template_id {environment_compute_template_id}, environment_storage_id {environment_storage_id}"
                    response_to_return["get_env_details_response"] = CICDResponse.parse_result(status=status,
                                                                                               entity_id=entity_id,
                                                                                               response=response)
                    # Check if there are any data connections
                    list_of_dataconnections = [item for item in configuration_obj["configuration"]["iw_mappings"] if
                                               item["entity_type"].lower() == "data_connection"]
                    if len(list_of_dataconnections) > 0:
                        configuration_obj["dataconnection_configurations"] = []
                        for dataconn in list_of_dataconnections:
                            dataconnection_obj = {}
                            entity_id = dataconn["entity_id"]
                            get_data_connection_url = get_data_connection(cicd_client.client_config,
                                                                          entity_id)
                            response = cicd_client.call_api("GET", get_data_connection_url,
                                                            IWUtils.get_default_header_for_v3(
                                                                cicd_client.client_config['bearer_token']))
                            parsed_response = IWUtils.ejson_deserialize(response.content)
                            if response.status_code == 200:
                                status = "SUCCESS"
                            else:
                                status = "FAILED"
                                print("Get Data Connection Details failed " + json.dumps(response))
                                cicd_client.logger.error("Get Data Connection Details failed " + json.dumps(response))

                            response_to_return["get_data_connection_details_response"] = CICDResponse.parse_result(
                                status=status,
                                entity_id=entity_id,
                                response=parsed_response)
                            if response.status_code == 200 and len(parsed_response.get("result", [])) > 0:
                                result = parsed_response.get("result", [])
                                if len(result) > 0:
                                    result["properties"] = self.get_dataconnection_properties(result["sub_type"],
                                                                                              result["properties"])
                                for key in ['name', 'type', 'sub_type', 'properties']:
                                    dataconnection_obj[key] = result[key]
                                configuration_obj["dataconnection_configurations"].append(
                                    copy.deepcopy(dataconnection_obj))
                elif entity_type == "pipeline_group":
                    pipelines_under_domain_parsed_response = self.list_pipelines(cicd_client, domain_id=domain_id)
                    pipeline_name_lookup = {}
                    if pipelines_under_domain_parsed_response.get("result", "") == "":
                        cicd_client.logger.error(f"Failed to list the pipelines under domain {domain_id}")
                        cicd_client.logger.error(pipelines_under_domain_parsed_response)
                        print(f"Failed to list the pipelines under domain {domain_id}")
                        print(pipelines_under_domain_parsed_response)
                        raise Exception(f"Failed to list the pipelines under domain {domain_id}")
                    for pipeline in pipelines_under_domain_parsed_response["result"]:
                        pipeline_name_lookup[pipeline["id"]] = pipeline["name"]
                    for index, pipeline in enumerate(configuration_obj["pipelines"]):
                        pipeline["name"] = pipeline_name_lookup.get(pipeline["pipeline_id"], None)
                    environment_id = configuration_obj["environment_id"]
                domains_url_base = list_domains_url(cicd_client.client_config)
                filter_condition = IWUtils.ejson_serialize({"_id": domain_id})
                domains_url = domains_url_base + f"?filter={{filter_condition}}".format(
                    filter_condition=filter_condition)
                response = cicd_client.call_api("GET", domains_url,
                                                IWUtils.get_default_header_for_v3(
                                                    cicd_client.client_config['bearer_token']))
                parsed_response = IWUtils.ejson_deserialize(response.content)
                if response.status_code == 200:
                    status = "SUCCESS"
                    cicd_client.logger.info("Got domain details successfully")
                else:
                    status = "FAILED"
                    print("Get Domains Details failed " + json.dumps(response))
                response_to_return["get_domain_details_response"] = CICDResponse.parse_result(status=status,
                                                                                              entity_id=entity_id,
                                                                                              response=parsed_response)
                existing_domain_name = None
                if response.status_code == 200 and len(parsed_response.get("result", [])) > 0:
                    result = parsed_response.get("result", [])
                    existing_domain_name = result[0]["name"]
                    if entity_type == "workflow":
                        environment_ids = result[0]["environment_ids"]
                if existing_domain_name:
                    filename = existing_domain_name + "#{}_".format(entity_type) + \
                               entity_name + ".json"
                    target_file_path = os.path.join(target_file_path, entity_type, filename)

            if entity_type == "workflow":
                storage_name, compute_name = None, None
                environment_names = []
                if len(environment_ids) > 0:
                    for environment_id in environment_ids:
                        env_name, storage_name, compute_name = self.get_env_entities_names(cicd_client, environment_id,
                                                                                           None, None)
                        environment_names.append(env_name)
                        print(env_name, storage_name, compute_name)
                    configuration_obj["environment_configurations"] = {"environment_name": environment_names,
                                                                       "environment_compute_template_name": compute_name,
                                                                       "environment_storage_name": storage_name}
                else:
                    print("in else")
            else:
                env_name, storage_name, compute_name = self.get_env_entities_names(
                    cicd_client, environment_id,
                    environment_compute_template_id,
                    environment_storage_id)

                configuration_obj["environment_configurations"] = {"environment_name": env_name,
                                                                   "environment_compute_template_name": compute_name,
                                                                   "environment_storage_name": storage_name}

            if entity_type != "pipeline_group":
                filter_condition = IWUtils.ejson_serialize(
                    {"$or": [{"_id": configuration_obj["configuration"]["export"]["exported_by"]},
                             {"profile.name": configuration_obj["configuration"]["export"]["exported_by"]}]})
                url_to_list_users_base = list_users_url(cicd_client.client_config)
                url_to_list_users = url_to_list_users_base + f"?filter={{filter_condition}}".format(
                    filter_condition=filter_condition)
                response = cicd_client.call_api("GET", url_to_list_users,
                                                IWUtils.get_default_header_for_v3(
                                                    cicd_client.client_config['bearer_token']))
                parsed_response = IWUtils.ejson_deserialize(response.content)
                configuration_obj["user_email"] = self.serviceaccountemail
                if response.status_code == 200 and len(parsed_response.get("result", [])) > 0:
                    result = parsed_response.get("result", [])
                    configuration_obj["user_email"] = result[0]["profile"].get("email", "admin@infoworks.io")
            try:
                if filename is not None and target_file_path is not None:
                    cicd_client.logger.info("{} {}".format(filename, target_file_path))
                    print(f"Exporting configurations file to {target_file_path}")
                    with open(target_file_path, 'w') as file_ptr:
                        #contents_to_write = IWUtils.ejson_serialize(configuration_obj)
                        #if replace_words != "":
                        #    for key, value in [item.split("->") for item in replace_words.split(";")]:
                        #        contents_to_write = contents_to_write.replace(key, value)
                        #file_ptr.write(contents_to_write)
                        json.dump(configuration_obj,file_ptr,indent=4)
                    cicd_client.logger.info("Configurations exported successfully")
                    print("Configurations exported successfully")
            except Exception as e:
                cicd_client.logger.error(str(e))
                print(str(e))
        else:
            cicd_client.logger.info("Unable to export the configurations")
            print("Unable to export the configurations")
        # for item in response_to_return:
        #    print(item, json.dumps(response_to_return[item]))
        return filename, configuration_obj