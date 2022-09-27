import copy
import os
from infoworks.sdk.utils import IWUtils
from infoworks.sdk.url_builder import get_parent_entity_url, list_domains_url, configure_pipeline_url, \
    configure_workflow_url, \
    configure_source_url, get_environment_details, get_environment_storage_details, get_environment_compute_details, \
    get_environment_interactive_compute_details, get_source_configurations_url, get_pipeline_url, \
    get_data_connection, source_info, list_users_url


class Utils:
    def __init__(self, serviceaccountemail):
        self.serviceaccountemail = serviceaccountemail

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
                    environment_id = result["environment_id"]
                    storage_id = result["storage_id"]
                    compute_template_id = result["compute_template_id"]
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

    def dump_to_file(self, cicd_client, entity_type, domain_id, entity_id, replace_words, target_file_path):
        filename = None
        environment_id, environment_compute_template_id, environment_storage_id = None, None, None
        if entity_type == "pipeline":
            url_to_config = configure_pipeline_url(cicd_client.client_config, domain_id, entity_id)
        elif entity_type == "workflow":
            url_to_config = configure_workflow_url(cicd_client.client_config, domain_id, entity_id)
        elif entity_type == "source":
            url_to_config = configure_source_url(cicd_client.client_config, entity_id)
        else:
            return None
        cicd_client.logger.info(
            "URL to get configurations of entity {} of type {} is {}".format(entity_id, entity_type, url_to_config))
        response = IWUtils.ejson_deserialize(cicd_client.call_api("GET", url_to_config,
                                                                  IWUtils.get_default_header_for_v3(
                                                                      cicd_client.client_config[
                                                                          'bearer_token'])).content)
        configuration_obj = response.get('result', {})
        # print(configuration_obj)
        if len(configuration_obj) > 0:
            if entity_type == "source":
                get_src_details_url = source_info(cicd_client.client_config, entity_id)
                response = cicd_client.call_api("GET", get_src_details_url,
                                                IWUtils.get_default_header_for_v3(
                                                    cicd_client.client_config['bearer_token']))
                parsed_response = IWUtils.ejson_deserialize(response.content)
                if response.status_code == 200 and len(parsed_response.get("result", [])) > 0:
                    result = parsed_response.get("result", [])
                    if len(result) > 0:
                        data_lake_path = result["data_lake_path"]
                        configuration_obj["configuration"]["source_configs"]["data_lake_path"] = data_lake_path
                        configuration_obj["filter_tables_properties"] = result.get("filter_tables_properties", {})

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
                    result = parsed_response.get("result", None)
                    if result:
                        environment_id = result["environment_id"]
                        environment_storage_id = result["storage_id"]
            else:
                if entity_type == "pipeline":
                    environment_id, environment_compute_template_id, environment_storage_id = self.get_env_details(
                        cicd_client, entity_id,
                        entity_type,
                        domain_id)
                    # Check if there are any data connections
                    list_of_dataconnections = [item for item in configuration_obj["configuration"]["iw_mappings"] if
                                               item["entity_type"].lower() == "data_connection"]
                    if len(list_of_dataconnections) > 0:
                        configuration_obj["dataconnection_configurations"] = []
                        for dataconn in list_of_dataconnections:
                            dataconnection_obj = {}
                            entity_id = dataconn["entity_id"]
                            get_data_connection_url = get_data_connection(cicd_client.client_config, domain_id,
                                                                          entity_id)
                            response = cicd_client.call_api("GET", get_data_connection_url,
                                                            IWUtils.get_default_header_for_v3(
                                                                cicd_client.client_config['bearer_token']))
                            parsed_response = IWUtils.ejson_deserialize(response.content)
                            if response.status_code == 200 and len(parsed_response.get("result", [])) > 0:
                                result = parsed_response.get("result", [])
                                if len(result) > 0:
                                    result["properties"] = self.get_dataconnection_properties(result["sub_type"],
                                                                                              result["properties"])
                                for key in ['name', 'type', 'sub_type', 'properties']:
                                    dataconnection_obj[key] = result[key]
                                configuration_obj["dataconnection_configurations"].append(
                                    copy.deepcopy(dataconnection_obj))
                domains_url_base = list_domains_url(cicd_client.client_config)
                filter_condition = IWUtils.ejson_serialize({"_id": domain_id})
                domains_url = domains_url_base + f"?filter={{filter_condition}}".format(
                    filter_condition=filter_condition)
                response = cicd_client.call_api("GET", domains_url,
                                                IWUtils.get_default_header_for_v3(
                                                    cicd_client.client_config['bearer_token']))
                parsed_response = IWUtils.ejson_deserialize(response.content)
                existing_domain_name = None
                if response.status_code == 200 and len(parsed_response.get("result", [])) > 0:
                    result = parsed_response.get("result", [])
                    existing_domain_name = result[0]["name"]
                    if entity_type == "workflow":
                        environment_ids = result[0]["environment_ids"]
                if existing_domain_name:
                    filename = existing_domain_name + "#{}_".format(entity_type) + \
                               configuration_obj["configuration"]["entity"][
                                   "entity_name"] + ".json"
                    target_file_path = os.path.join(target_file_path, entity_type, filename)

            if entity_type == "workflow":
                storage_name, compute_name = None, None
                environment_names = []
                if len(environment_ids) > 0:
                    for environment_id in environment_ids:
                        env_name, storage_name, compute_name = self.get_env_entities_names(cicd_client, environment_id,
                                                                                           None, None)
                        environment_names.append(env_name)
                    configuration_obj["environment_configurations"] = {"environment_name": environment_names,
                                                                       "environment_compute_template_name": compute_name,
                                                                       "environment_storage_name": storage_name}
            else:
                env_name, storage_name, compute_name = self.get_env_entities_names(
                    cicd_client, environment_id,
                    environment_compute_template_id,
                    environment_storage_id)

                configuration_obj["environment_configurations"] = {"environment_name": env_name,
                                                                   "environment_compute_template_name": compute_name,
                                                                   "environment_storage_name": storage_name}

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
                    print(f"Dumping file {target_file_path}")
                    with open(target_file_path, 'w') as file_ptr:
                        contents_to_write = IWUtils.ejson_serialize(configuration_obj)
                        if replace_words != "":
                            for key, value in [item.split("->") for item in replace_words.split(";")]:
                                contents_to_write = contents_to_write.replace(key, value)
                        file_ptr.write(contents_to_write)
                    cicd_client.logger.info("Configurations have been dumped")
            except Exception as e:
                cicd_client.logger.error(str(e))
        else:
            cicd_client.logger.info("Unable to dump the configurations")

        return filename, configuration_obj
