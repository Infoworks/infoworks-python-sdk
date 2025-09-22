import copy
import os
import traceback
import json
from infoworks.sdk.utils import IWUtils
from infoworks.sdk.url_builder import get_parent_entity_url, list_domains_url, configure_pipeline_url, \
    configure_workflow_url, configure_source_url, get_environment_details, get_environment_storage_details, \
    get_environment_compute_details, get_environment_interactive_compute_details, get_source_configurations_url, \
    get_pipeline_url, get_data_connection, source_info, list_users_url, list_secrets_url, get_pipeline_group_base_url, \
    list_pipelines_url, create_domain_url, get_table_configuration, list_tables_under_source, create_table_group_url
from infoworks.sdk.cicd.cicd_response import CICDResponse

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
                cicd_client.logger.info(f"Found secret name {secret_name}")
                return secret_name
            else:
                cicd_client.logger.info("Secret Name is None")
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
                cicd_client.logger.info(f"Domain ID is {domain_id}")
                return domain_id
            else:
                cicd_client.logger.info("Domain ID is None")
                return None

    def get_env_entities_names(self, cicd_client, environment_id, environment_compute_template_id,
                               environment_storage_id):
        env_name, storage_name, compute_name = None, None, None
        if environment_id:
            response = cicd_client.call_api("GET", get_environment_details(cicd_client.client_config, environment_id),
                                           IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(response.content)
            if response.status_code == 200:
                result = parsed_response.get("result", None)
                if result:
                    env_name = result["name"]
        if environment_storage_id:
            response = cicd_client.call_api("GET",
                                           get_environment_storage_details(cicd_client.client_config, environment_id,
                                                                          environment_storage_id),
                                           IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(response.content)
            if response.status_code == 200:
                result = parsed_response.get("result", None)
                if result:
                    storage_name = result["name"]
        if environment_compute_template_id:
            response = cicd_client.call_api("GET",
                                           get_environment_compute_details(cicd_client.client_config, environment_id,
                                                                          environment_compute_template_id),
                                           IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(response.content)
            if response.status_code == 200:
                result = parsed_response.get("result", None)
                if result:
                    compute_name = result["name"]
                else:
                    response = cicd_client.call_api("GET",
                                                   get_environment_interactive_compute_details(
                                                       cicd_client.client_config, environment_id,
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
                                           IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(response.content)
            if response.status_code == 200 and len(parsed_response.get("result", [])) > 0:
                result = parsed_response.get("result", [])
                if len(result) > 0:
                    environment_id = result.get("environment_id", None)
                    storage_id = result.get("storage_id", None)
                    compute_template_id = result.get("compute_template_id", None)
                    cicd_client.logger.info(
                        f"Environment ID is {environment_id} Storage ID is {storage_id} Compute Template Id is {compute_template_id}")
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
        response = cicd_client.call_api("GET", get_active_pipeline_versions_url,
                                       IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token']))
        parsed_response = IWUtils.ejson_deserialize(response.content)
        pipeline_details = parsed_response.get("result", {})
        pipeline_name = pipeline_details.get("name", "")
        query_tag = pipeline_details.get("query_tag", "")
        custom_tags = pipeline_details.get("custom_tags", [])
        description = pipeline_details.get("description", "")
        filename = f"{domain_name}#pipeline_{pipeline_name}.json"
        active_pipeline_version = pipeline_details.get("active_version_id")
        get_pipeline_version_config_url = f"{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/versions/{active_pipeline_version}"
        response = cicd_client.call_api("GET", get_pipeline_version_config_url,
                                       IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token']))
        parsed_response = IWUtils.ejson_deserialize(response.content)
        pipeline_version_details = parsed_response.get("result", {})
        version_id = pipeline_version_details.get("version", 1)
        query = pipeline_version_details.get("query", "")
        pipeline_parameters = pipeline_version_details.get("pipeline_parameters", [])
        response = cicd_client.call_api("GET", get_active_pipeline_versions_url + "configurations/advance",
                                       IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token']))
        parsed_response = IWUtils.ejson_deserialize(response.content)
        pipeline_advance_config_details = parsed_response.get("result", [])
        environment_id, environment_compute_template_id, environment_storage_id = self.get_env_details(
            cicd_client, pipeline_id, "pipeline", domain_id)
        env_name, storage_name, compute_name = self.get_env_entities_names(
            cicd_client, environment_id, environment_compute_template_id, environment_storage_id)
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
            "environment_configurations": {
                "environment_name": env_name,
                "environment_compute_template_name": compute_name,
                "environment_storage_name": storage_name
            }
        }
        target_file_path = os.path.join(target_file_path, "pipeline", filename)
        if filename is not None and target_file_path is not None:
            cicd_client.logger.info(f"{filename} {target_file_path}")
            print(f"Exporting configurations file to {target_file_path}")
            with open(target_file_path, 'w') as file_ptr:
                json.dump(config_obj, file_ptr, indent=4)
            cicd_client.logger.info("Configurations exported successfully")
            print("Configurations exported successfully")
        return filename, config_obj

    def list_pipelines(self, cicd_client, domain_id=None, params=None):
        if None in {domain_id}:
            cicd_client.logger.error("Domain ID cannot be None")
            raise Exception("Domain ID cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_pipelines = list_pipelines_url(cicd_client.client_config, domain_id) + \
                                IWUtils.get_query_params_string_from_dict(params=params)
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
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(
                        next=response.get('links')['next'], ip=cicd_client.client_config['ip'],
                        port=cicd_client.client_config['port'], protocol=cicd_client.client_config['protocol'])
                    response = IWUtils.ejson_deserialize(
                        cicd_client.call_api("GET", nextUrl,
                                             IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token'])).content)
                    result = response.get("result", None)
                    if result is None:
                        return response
                response["result"] = pipelines_list
                response["message"] = initial_msg
            return response
        except Exception as e:
            cicd_client.logger.error(f"Error in listing pipelines: {str(e)}")
            raise Exception(f"Error in listing pipelines: {str(e)}")

    def add_secret_name_to_id(self, data, cicd_client):
        for key, value in data.items():
            if isinstance(value, dict):
                secret_name = self.add_secret_name_to_id(value, cicd_client)
                if secret_name:
                    return secret_name
            elif key == "secret_id":
                secret_name = self.get_secret_name_from_id(cicd_client=cicd_client, secret_id=value)
                if secret_name:
                    data["secret_name"] = secret_name
                return value

    def add_custom_tags_to_id(self, data, cicd_client):
        data["custom_tag_key_values"] = {}
        for key, value in data.items():
            if key == "custom_tags":
                for tag_id in value:
                    try:
                        response = cicd_client.get_custom_tag(custom_tag_id=tag_id)
                        custom_tag_key = response['result']['response']['result']['key']
                        custom_tag_value = response['result']['response']['result']['value']
                        data["custom_tag_key_values"][custom_tag_key] = custom_tag_value
                    except Exception as error:
                        cicd_client.logger.error(f"Failed to get custom tag: {error}")
                        print(f"Failed to get custom tag: {error}")

    def dump_to_file(self, cicd_client, entity_type, domain_id, entity_id, replace_words, target_file_path,
                     dump_watermarks=True, custom_tag_id=None, version_id=None):
        """
        Dump entity configuration to a file.
        :param cicd_client: Infoworks SDK client
        :param entity_type: Type of entity (e.g., 'workflow')
        :param domain_id: Domain ID
        :param entity_id: Entity ID
        :param replace_words: Words to replace in the configuration
        :param target_file_path: Path to save the configuration
        :param dump_watermarks: Whether to dump table watermarks (for sources)
        :param custom_tag_id: Custom tag ID for filtering
        :param version_id: Optional version ID for workflows
        :return: Tuple of (filename, configuration_obj)
        """
        response_to_return = {}
        filename = None
        environment_id, environment_compute_template_id, environment_storage_id = None, None, None
        try:
            if entity_type == "pipeline":
                url_to_config = configure_pipeline_url(cicd_client.client_config, domain_id, entity_id)
            elif entity_type == "workflow":
                url_to_config = configure_workflow_url(cicd_client.client_config, domain_id, entity_id)
                if version_id:
                    url_to_config += f"?version_id={version_id}"
            elif entity_type == "source":
                url_to_config = configure_source_url(cicd_client.client_config, entity_id)
            elif entity_type == "pipeline_group":
                url_to_config = get_pipeline_group_base_url(cicd_client.client_config, domain_id) + f"{entity_id}"
            else:
                error_msg = f"Invalid entity_type: {entity_type}"
                cicd_client.logger.error(error_msg)
                print(error_msg)
                raise ValueError(error_msg)

            cicd_client.logger.info(
                f"URL to get configurations of entity {entity_id} of type {entity_type} is {url_to_config}")
            response = cicd_client.call_api("GET", url_to_config, IWUtils.get_default_header_for_v3(
                cicd_client.client_config['bearer_token']))
            parsed_response = IWUtils.ejson_deserialize(response.content)
            cicd_client.logger.debug(f"API response for {entity_type} {entity_id}: {parsed_response}")

            if response.status_code != 200:
                error_msg = f"Failed to fetch {entity_type} {entity_id}: {parsed_response}"
                print(error_msg)
                cicd_client.logger.error(error_msg)
                raise Exception(error_msg)

            response_to_return["get_configuration_entity_response"] = CICDResponse.parse_result(
                status="SUCCESS", entity_id=entity_id, response=parsed_response)
            configuration_obj = parsed_response.get('result', {})
            if not configuration_obj:
                error_msg = f"No configuration found for {entity_type} {entity_id}"
                print(error_msg)
                cicd_client.logger.error(error_msg)
                raise Exception(error_msg)

            if entity_type == "source":
                # 1) Source details (normalize shape)
                get_src_details_url = source_info(cicd_client.client_config, entity_id)
                response = cicd_client.call_api(
                    "GET",
                    get_src_details_url,
                    IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token'])
                )
                parsed_response = IWUtils.ejson_deserialize(response.content)
                cicd_client.logger.debug(f"Source details response for {entity_id}: {parsed_response}")

                if response.status_code != 200:
                    error_msg = f"Get Source Info failed: {json.dumps(parsed_response)}"
                    print(error_msg)
                    cicd_client.logger.error(error_msg)
                    raise Exception(error_msg)

                src_details = parsed_response.get("result")
                if src_details is None:
                    raise Exception("Source details: missing 'result'")

                # normalize list/dict
                if isinstance(src_details, list):
                    if not src_details:
                        raise Exception("Source details: empty result list")
                    src_details = src_details[0]
                elif not isinstance(src_details, dict):
                    raise Exception(f"Source details: unexpected result type {type(src_details).__name__}")

                # 2) Safely enrich configuration_obj
                cfg = configuration_obj.setdefault("configuration", {}).setdefault("source_configs", {})

                # data_lake_path-like fields vary by tenant
                dl_path = src_details.get("data_lake_path") or src_details.get("data_lake_schema") or src_details.get(
                    "data_lake_database")
                if dl_path is not None:
                    cfg["data_lake_path"] = dl_path

                configuration_obj["filter_tables_properties"] = src_details.get("filter_tables_properties", {})

                # Connection object may be absent on some exports
                source_connection_objects = cfg.get("connection")
                if source_connection_objects:
                    self.add_secret_name_to_id(data=source_connection_objects, cicd_client=cicd_client)

                # Custom tags
                self.add_custom_tags_to_id(data=cfg, cicd_client=cicd_client)

                # Table + table group configs may be missing; guard loops
                for table in configuration_obj.get("configuration", {}).get("table_configs", []):
                    self.add_custom_tags_to_id(
                        data=table.get("configuration", {}).get("configuration", {}),
                        cicd_client=cicd_client
                    )
                for table_group in configuration_obj.get("configuration", {}).get("table_group_configs", []):
                    self.add_custom_tags_to_id(
                        data=table_group.get("configuration", {}),
                        cicd_client=cicd_client
                    )

                # Associated domains
                if cfg.get("associated_domains") is None:
                    assoc = src_details.get("associated_domains", [])
                    if assoc:
                        cfg["associated_domains"] = assoc

                # 3) Redact export targets if present
                for table_config in configuration_obj.get("configuration", {}).get("table_configs", []):
                    exp_cfg = table_config.get("configuration", {}).get("export_configuration")
                    if isinstance(exp_cfg, dict):
                        if exp_cfg.get("target_type") in ["SNOWFLAKE", "POSTGRES", "AZURE_SQL_DW"]:
                            if "connection" in exp_cfg and isinstance(exp_cfg["connection"], dict):
                                exp_cfg["connection"]["password"] = None

                # 4) Watermarks (unchanged except safer gets)
                table_watermark_mappings = {}
                if dump_watermarks:
                    for table_config in configuration_obj.get("configuration", {}).get("table_configs", []):
                        table_id = table_config.get("entity_id")
                        if entity_id and table_id:
                            get_tbl_details_url = get_table_configuration(cicd_client.client_config, entity_id,
                                                                          table_id)
                            resp_tbl = cicd_client.call_api(
                                "GET",
                                get_tbl_details_url,
                                IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token'])
                            )
                            parsed_tbl = IWUtils.ejson_deserialize(resp_tbl.content)
                            cicd_client.logger.debug(f"Table config response for {table_id}: {parsed_tbl}")
                            if resp_tbl.status_code == 200:
                                tbl_res = parsed_tbl.get("result", {}) or {}
                                table_watermark_mappings[table_id] = {
                                    "last_ingested_cdc_value": tbl_res.get("last_ingested_cdc_value"),
                                    "last_merged_watermark": tbl_res.get("last_merged_watermark"),
                                    "row_count": tbl_res.get("row_count"),
                                    "full_load_performed": tbl_res.get("full_load_performed"),
                                }
                                if tbl_res.get("max_modified_timestamp"):
                                    table_watermark_mappings[table_id]["max_modified_timestamp"] = tbl_res[
                                        "max_modified_timestamp"]
                                configuration_obj["table_watermark_mappings"] = table_watermark_mappings
                            else:
                                print(f"Get Table Config Failed: {json.dumps(parsed_tbl)}")
                                cicd_client.logger.error(f"Get Table Config Failed: {json.dumps(parsed_tbl)}")

                # 5) Custom-tag filtering (unchanged, just variable names fixed)
                if custom_tag_id:
                    filtered_table_ids = []
                    filtered_table_group_ids = []

                    url_tables = list_tables_under_source(config=cicd_client.client_config, source_id=entity_id) + \
                                 f'?filter={{"configuration.custom_tags":"{custom_tag_id}"}}'
                    resp_tables = cicd_client.call_api("GET", url_tables,
                                                       IWUtils.get_default_header_for_v3(
                                                           cicd_client.client_config['bearer_token']))
                    parsed_tbls = IWUtils.ejson_deserialize(resp_tables.content)
                    cicd_client.logger.debug(f"Custom tag tables response: {parsed_tbls}")
                    if resp_tables.status_code == 200:
                        filtered_table_ids = [t["id"] for t in parsed_tbls.get("result", [])]
                    else:
                        print(f"Failed to list tables with custom tag: {parsed_tbls}")
                        cicd_client.logger.error(f"Failed to list tables with custom tag: {parsed_tbls}")

                    url_tgs = create_table_group_url(config=cicd_client.client_config, source_id=entity_id) + \
                              f'?filter={{"configuration.custom_tags":"{custom_tag_id}"}}'
                    resp_tgs = cicd_client.call_api("GET", url_tgs,
                                                    IWUtils.get_default_header_for_v3(
                                                        cicd_client.client_config['bearer_token']))
                    parsed_tgs = IWUtils.ejson_deserialize(resp_tgs.content)
                    cicd_client.logger.debug(f"Custom tag table groups response: {parsed_tgs}")
                    if resp_tgs.status_code == 200:
                        filtered_table_group_ids = [tg["id"] for tg in parsed_tgs.get("result", [])]
                    else:
                        print(f"Failed to list table groups with custom tag: {parsed_tgs}")
                        cicd_client.logger.error(f"Failed to list table groups with custom tag: {parsed_tgs}")

                    updated_tables = [t for t in configuration_obj.get("configuration", {}).get("table_configs", [])
                                      if t.get("entity_id") in filtered_table_ids]
                    for t in configuration_obj.get("configuration", {}).get("table_configs", []):
                        if t.get("entity_id") not in filtered_table_ids:
                            print(
                                f"Removing {t.get('configuration', {}).get('name', '?').upper()} from table_configs section")
                    configuration_obj.setdefault("configuration", {})["table_configs"] = updated_tables

                    updated_tgs = [tg for tg in
                                   configuration_obj.get("configuration", {}).get("table_group_configs", [])
                                   if tg.get("entity_id") in filtered_table_group_ids]
                    for tg in configuration_obj.get("configuration", {}).get("table_group_configs", []):
                        if tg.get("entity_id") not in filtered_table_group_ids:
                            print(
                                f"Removing {tg.get('configuration', {}).get('name', '?').upper()} from table_group_configs section")
                    configuration_obj["configuration"]["table_group_configs"] = updated_tgs

                    updated_maps = []
                    for m in configuration_obj.get("configuration", {}).get("iw_mappings", []):
                        et = m.get("entity_type", "")
                        eid = m.get("entity_id", "")
                        if et == "table" and eid not in filtered_table_ids:
                            print(f"Removing table {eid} from iw_mappings section")
                            continue
                        if et == "table_group" and eid not in filtered_table_group_ids:
                            print(f"Removing table group {eid} from iw_mappings section")
                            continue
                        updated_maps.append(m)
                    configuration_obj["configuration"]["iw_mappings"] = updated_maps

                # 6) Associated domain names (fix list/dict)
                accessible_domain_ids = cfg.get("associated_domains", []) or []
                accessible_domain_names = []
                for dom_id in accessible_domain_ids:
                    dom_resp = cicd_client.call_api(
                        "GET",
                        create_domain_url(cicd_client.client_config) + "/" + dom_id,
                        IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token'])
                    )
                    dom_parsed = IWUtils.ejson_deserialize(dom_resp.content)
                    cicd_client.logger.debug(f"Domain response for {dom_id}: {dom_parsed}")
                    if dom_resp.status_code == 200:
                        dom_res = dom_parsed.get("result")
                        if isinstance(dom_res, list) and dom_res:
                            dom_name = dom_res[0].get("name")
                        elif isinstance(dom_res, dict):
                            dom_name = dom_res.get("name")
                        else:
                            dom_name = None
                        if dom_name:
                            accessible_domain_names.append(dom_name)
                if accessible_domain_names:
                    cfg["associated_domain_names"] = accessible_domain_names

                # 7) Steps + filename + write (safe name + ensure dir)
                configuration_obj["steps_to_run"] = {
                    "configure_csv_source": True,
                    "import_source_configuration": True,
                    "configure_rdbms_source_connection": True,
                    "test_source_connection": True,
                    "browse_source_tables": True,
                    "add_tables_to_source": True,
                    "configure_tables_and_tablegroups": True
                }

                def _safe_name(s):
                    return "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in s)

                raw_name = configuration_obj.get("configuration", {}).get("entity", {}).get("entity_name",
                                                                                            f"source_{entity_id}")
                filename = f"{entity_type}_{_safe_name(raw_name)}.json"
                dest_dir = os.path.join(target_file_path, entity_type)
                os.makedirs(dest_dir, exist_ok=True)
                target_file_path = os.path.join(dest_dir, filename)

                # 8) Environment bits (best-effort)
                resp_cfg = cicd_client.call_api(
                    "GET",
                    get_source_configurations_url(cicd_client.client_config, entity_id),
                    IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token'])
                )
                parsed_cfg = IWUtils.ejson_deserialize(resp_cfg.content)
                cicd_client.logger.debug(f"Source configurations response for {entity_id}: {parsed_cfg}")
                if resp_cfg.status_code == 200:
                    cfg_res = parsed_cfg.get("result") or {}
                    environment_id = cfg_res.get("environment_id")
                    environment_storage_id = cfg_res.get("storage_id")
                else:
                    error_msg = f"Get Source Configurations failed: {json.dumps(parsed_cfg)}"
                    print(error_msg)
                    cicd_client.logger.error(error_msg)
                    raise Exception(error_msg)

                # 9) Finally write
                cicd_client.logger.info(f"Exporting {filename} to {target_file_path}")
                print(f"Exporting configurations file to {target_file_path}")
                with open(target_file_path, 'w') as fh:
                    json.dump(configuration_obj, fh, indent=4)
                cicd_client.logger.info("Configurations exported successfully")
                print("Configurations exported successfully")
            else:
                entity_name = configuration_obj["configuration"]["entity"][
                    "entity_name"] if entity_type != "pipeline_group" else configuration_obj["name"]
                if entity_type == "pipeline":
                    pipeline_configs = configuration_obj['configuration']['pipeline_configs']
                    self.add_custom_tags_to_id(data=pipeline_configs, cicd_client=cicd_client)
                    environment_id, environment_compute_template_id, environment_storage_id = self.get_env_details(
                        cicd_client, entity_id, entity_type, domain_id)
                    if any([environment_id, environment_compute_template_id, environment_storage_id]):
                        status = "SUCCESS"
                    else:
                        status = "FAILED"
                        error_msg = f"Get Env Details failed: {json.dumps(response)}"
                        print(error_msg)
                        cicd_client.logger.error(error_msg)
                        raise Exception(error_msg)
                    response = f"Found environment details: environment_id {environment_id}, environment_compute_template_id {environment_compute_template_id}, environment_storage_id {environment_storage_id}"
                    response_to_return["get_env_details_response"] = CICDResponse.parse_result(
                        status=status, entity_id=entity_id, response=response)
                    list_of_dataconnections = [item for item in configuration_obj["configuration"]["iw_mappings"] if
                                              item["entity_type"].lower() == "data_connection"]
                    if len(list_of_dataconnections) > 0:
                        configuration_obj["dataconnection_configurations"] = []
                        for dataconn in list_of_dataconnections:
                            dataconnection_obj = {}
                            entity_id = dataconn["entity_id"]
                            get_data_connection_url = get_data_connection(cicd_client.client_config, entity_id)
                            response = cicd_client.call_api("GET", get_data_connection_url,
                                                           IWUtils.get_default_header_for_v3(
                                                               cicd_client.client_config['bearer_token']))
                            parsed_response = IWUtils.ejson_deserialize(response.content)
                            cicd_client.logger.debug(f"Data connection response for {entity_id}: {parsed_response}")
                            if response.status_code == 200:
                                status = "SUCCESS"
                            else:
                                status = "FAILED"
                                error_msg = f"Get Data Connection Details failed: {json.dumps(parsed_response)}"
                                print(error_msg)
                                cicd_client.logger.error(error_msg)
                                raise Exception(error_msg)
                            response_to_return["get_data_connection_details_response"] = CICDResponse.parse_result(
                                status=status, entity_id=entity_id, response=parsed_response)
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
                        error_msg = f"Failed to list the pipelines under domain {domain_id}"
                        cicd_client.logger.error(error_msg)
                        print(error_msg)
                        raise Exception(error_msg)
                    for pipeline in pipelines_under_domain_parsed_response["result"]:
                        pipeline_name_lookup[pipeline["id"]] = pipeline["name"]
                    for index, pipeline in enumerate(configuration_obj["pipelines"]):
                        pipeline["name"] = pipeline_name_lookup.get(pipeline["pipeline_id"], None)
                    environment_id = configuration_obj["environment_id"]
                    pipeline_group_configs = configuration_obj
                    self.add_custom_tags_to_id(data=pipeline_group_configs, cicd_client=cicd_client)
                domains_url_base = list_domains_url(cicd_client.client_config)
                filter_condition = IWUtils.ejson_serialize({"_id": domain_id})
                domains_url = domains_url_base + f"?filter={{filter_condition}}".format(filter_condition=filter_condition)
                response = cicd_client.call_api("GET", domains_url,
                                               IWUtils.get_default_header_for_v3(cicd_client.client_config['bearer_token']))
                parsed_response = IWUtils.ejson_deserialize(response.content)
                cicd_client.logger.debug(f"Domain details response for {domain_id}: {parsed_response}")
                if response.status_code == 200:
                    status = "SUCCESS"
                    cicd_client.logger.info("Got domain details successfully")
                else:
                    status = "FAILED"
                    error_msg = f"Get Domains Details failed: {json.dumps(parsed_response)}"
                    print(error_msg)
                    cicd_client.logger.error(error_msg)
                    raise Exception(error_msg)
                response_to_return["get_domain_details_response"] = CICDResponse.parse_result(
                    status=status, entity_id=entity_id, response=parsed_response)
                existing_domain_name = None
                if response.status_code == 200 and len(parsed_response.get("result", [])) > 0:
                    result = parsed_response.get("result", [])
                    existing_domain_name = result[0]["name"]
                    if entity_type == "workflow":
                        environment_ids = result[0]["environment_ids"]
                if existing_domain_name:
                    filename = f"{existing_domain_name}#{entity_type}_" + entity_name + ".json"
                    target_file_path = os.path.join(target_file_path, entity_type, filename)
                if entity_type == "workflow":
                    storage_name, compute_name = None, None
                    environment_names = []
                    if len(environment_ids) > 0:
                        for environment_id in environment_ids:
                            env_name, storage_name, compute_name = self.get_env_entities_names(cicd_client,
                                                                                              environment_id, None, None)
                            environment_names.append(env_name)
                            print(f"Environment: {env_name}, Storage: {storage_name}, Compute: {compute_name}")
                            cicd_client.logger.debug(
                                f"Environment details for {environment_id}: env_name={env_name}, storage_name={storage_name}, compute_name={compute_name}")
                        configuration_obj["environment_configurations"] = {
                            "environment_name": environment_names,
                            "environment_compute_template_name": compute_name,
                            "environment_storage_name": storage_name
                        }
                    workflow_configs = configuration_obj['configuration'].get('workflow_export', {}).get('workflow_model', {})
                    self.add_custom_tags_to_id(data=workflow_configs, cicd_client=cicd_client)
                else:
                    env_name, storage_name, compute_name = self.get_env_entities_names(
                        cicd_client, environment_id, environment_compute_template_id, environment_storage_id)
                    configuration_obj["environment_configurations"] = {
                        "environment_name": env_name,
                        "environment_compute_template_name": compute_name,
                        "environment_storage_name": storage_name
                    }
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
                    cicd_client.logger.debug(f"User details response: {parsed_response}")
                    configuration_obj["user_email"] = self.serviceaccountemail
                    if response.status_code == 200 and len(parsed_response.get("result", [])) > 0:
                        result = parsed_response.get("result", [])
                        configuration_obj["user_email"] = result[0]["profile"].get("email", "admin@infoworks.io")
                try:
                    if filename is not None and target_file_path is not None:
                        cicd_client.logger.info(f"Exporting {filename} to {target_file_path}")
                        print(f"Exporting configurations file to {target_file_path}")
                        with open(target_file_path, 'w') as file_ptr:
                            json.dump(configuration_obj, file_ptr, indent=4)
                        cicd_client.logger.info("Configurations exported successfully")
                        print("Configurations exported successfully")
                except Exception as e:
                    error_msg = f"Failed to write file: {str(e)}"
                    print(error_msg)
                    cicd_client.logger.error(error_msg)
                    raise Exception(error_msg)
            return filename, configuration_obj
        except Exception as e:
            error_msg = f"Failed to process {entity_type} {entity_id}: {str(e)}"
            print(error_msg)
            cicd_client.logger.error(error_msg)
            print(f"Full traceback: {traceback.format_exc()}")
            return None, None