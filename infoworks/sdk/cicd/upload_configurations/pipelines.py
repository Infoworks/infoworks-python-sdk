import json
import traceback
import configparser
import requests
import yaml
import re
import time
import base64
from infoworks.core.iw_authentication import get_bearer_token
from infoworks.sdk.utils import IWUtils
from infoworks.sdk.url_builder import list_sources_url, list_domains_url, create_pipeline_url, create_data_connection, \
    configure_pipeline_url, get_pipeline_jobs_url, pipeline_version_base_url
from infoworks.sdk.cicd.upload_configurations.domains import Domain
from infoworks.sdk.cicd.upload_configurations.update_configurations import InfoworksDynamicAccessNestedDict
from infoworks.sdk.cicd.upload_configurations.local_configurations import PRE_DEFINED_MAPPINGS
from infoworks.sdk.local_configurations import POLLING_FREQUENCY_IN_SEC

class Pipeline:
    def __init__(self, pipeline_config_path, environment_id, storage_id, interactive_id,
                 replace_words="", secrets=None):
        self.storage_id = storage_id
        self.interactive_id = interactive_id
        self.environment_id = environment_id
        self.secrets = secrets
        with open(pipeline_config_path, 'r') as file:
            json_string = file.read()
            if replace_words != "":
                for key, value in [item.split("->") for item in replace_words.split(";")]:
                    json_string = json_string.replace(key, value)
        self.configuration_obj = IWUtils.ejson_deserialize(json_string)

    def update_mappings_for_configurations(self, mappings):
        config = configparser.ConfigParser()
        config.read_dict(mappings)
        d = InfoworksDynamicAccessNestedDict(self.configuration_obj)
        for section in config.sections():
            if section in PRE_DEFINED_MAPPINGS:
                continue
            try:
                final = d.setval(section.split("$"), dict(config.items(section)))
            except KeyError as e:
                pass
        self.configuration_obj = d.data
        # handle domain name mappings
        iw_mappings = self.configuration_obj.get("configuration", {}).get("iw_mappings", [])
        try:
            if "domain_name_mappings" in config.sections():
                domain_mappings = dict(config.items("domain_name_mappings"))
                if domain_mappings != {}:
                    for mapping in iw_mappings:
                        domain_name = mapping.get("recommendation", {}).get("domain_name", "")
                        if domain_name != "" and domain_mappings != {}:
                            mapping["recommendation"]["domain_name"] = domain_mappings.get(domain_name.lower(),
                                                                                           domain_name)
                    self.configuration_obj["configuration"]["iw_mappings"] = iw_mappings
            if self.configuration_obj["configuration"].get("pipeline_configs",{}).get("type","")=="sql":
                query = self.configuration_obj["configuration"].get("pipeline_configs",{}).get("query","")
                decode_query_binary = base64.b64decode(query)
                decoded_query = decode_query_binary.decode("ascii")
                if "sql_pipeline_text_replace" in config.sections():
                    text_mappings = dict(config.items("sql_pipeline_text_replace"))
                    if text_mappings != {}:
                        for k,v in text_mappings.items():
                            decoded_query = re.sub(k, v, decoded_query, flags=re.IGNORECASE)
                            print(f"Replacing '{k}' with '{v}' in sql_query")
                        encoded_query_binary = base64.b64encode(decoded_query.encode('utf-8'))
                        encoded_query = encoded_query_binary.decode('utf-8')
                        self.configuration_obj["configuration"]["pipeline_configs"]["query"]=encoded_query
                        for item in iw_mappings:
                            if item.get("entity_type","")=="query":
                                item["recommendation"]["query"] = encoded_query
        except Exception as e:
            print("Failed while doing the domain mappings")
            print(str(e))
            print(traceback.format_exc())
        # handle any other generic name mappings like iw_mappings$recommendation$source_name
        try:
            generic_mappings = [i for i in config.sections() if i.lower().startswith("iw_mappings$")]
            for mapping in generic_mappings:
                lineage_list = mapping.split("$")
                lineage_list.remove("iw_mappings")
                if "recommendation" in lineage_list:
                    lineage_list.remove("recommendation")
                artifact_type = lineage_list[0]
                artifact_mappings = dict(config.items(f"iw_mappings$recommendation${artifact_type}"))
                if artifact_mappings != {}:
                    for mapping in iw_mappings:
                        artifact_name = mapping.get("recommendation", {}).get(artifact_type, "")
                        if artifact_name != "" and artifact_mappings != {}:
                            mapping["recommendation"][artifact_type] = artifact_mappings.get(
                                artifact_name.lower(),
                                artifact_name)
                    self.configuration_obj["configuration"]["iw_mappings"] = iw_mappings
        except Exception as e:
            print("Failed while doing the generic mappings")
            print(str(e))
            print(traceback.format_exc())

    def poll_pipeline_job(self, pipeline_client_obj, domain_id, pipeline_id):
        list_pipeline_job_url = get_pipeline_jobs_url(config=pipeline_client_obj.client_config, domain_id=domain_id,
                                                      pipeline_id=pipeline_id)
        list_pipeline_job_url = list_pipeline_job_url + "?sort_by=\"createdAt\"&limit=1&order_by=desc"
        pipeline_client_obj.logger.info(f"list pipeline url {list_pipeline_job_url}")
        response = pipeline_client_obj.call_api("GET", list_pipeline_job_url, {
            'Authorization': 'Bearer ' + pipeline_client_obj.client_config["bearer_token"],
            'Content-Type': 'application/json'}, data=None)
        parsed_response = IWUtils.ejson_deserialize(
            response.content)
        if response.status_code == 200 and "result" in parsed_response:
            result = parsed_response.get("result", [])
            if result:
                result = result[0]
                job_id = result.get("id", None)
                job_status = result.get("status", None)
                print(f"pipeline build metadata status: {job_status}")
                if job_id is not None:
                    if job_status.lower() not in ["completed", "failed", "aborted", "canceled"]:
                        time.sleep(POLLING_FREQUENCY_IN_SEC)
                        return self.poll_pipeline_job(pipeline_client_obj, domain_id, pipeline_id)
                    else:
                        return "SUCCESS" if job_status.lower() == "completed" else "FAILED"
            else:
                print("Error during polling of pipeline build metadata job")
                print(parsed_response)
                return "FAILED"
        else:
            print("Error during polling of pipeline build metadata job")
            print(parsed_response)
            return "FAILED"

    def create(self, pipeline_client_obj, domain_id, domain_name):
        pipeline_name = self.configuration_obj["configuration"]["entity"]["entity_name"]
        sources_in_pipelines = []
        for item in self.configuration_obj["configuration"]["iw_mappings"]:
            if item["entity_type"] == "table" and "source_name" in item["recommendation"]:
                sources_in_pipelines.append(item["recommendation"].get("source_name"))
        filter_condition = IWUtils.ejson_serialize({"name": {"$in": sources_in_pipelines}})
        source_list_url = list_sources_url(pipeline_client_obj.client_config)
        pipeline_client_obj.logger.info(f"Listing source url {source_list_url}")
        src_list_url = source_list_url + f"?filter={{filter_condition}}".format(filter_condition=filter_condition)
        response = pipeline_client_obj.call_api("GET", src_list_url, {
            'Authorization': 'Bearer ' + pipeline_client_obj.client_config["bearer_token"],
            'Content-Type': 'application/json'}, data=None)
        parsed_response = IWUtils.ejson_deserialize(
            response.content)
        temp_src_ids = []
        if response.status_code == 200 and "result" in parsed_response:
            result = parsed_response.get("result", {})
            for item in result:
                temp_src_ids.append(item["id"])
        sourceids_in_pipelines = list(set(temp_src_ids))
        user_email = self.configuration_obj["user_email"]
        domain_obj = Domain(self.environment_id)
        new_pipeline_id = ''
        pipeline_json_object = {
            "name": pipeline_name,
            "environment_id": self.environment_id,
            "domain_id": domain_id
        }
        # 5.2.x versions need storage id and compute id
        batch_engine = self.configuration_obj["configuration"].get("pipeline_configs", {}).get("batch_engine", "")
        storage_id = self.storage_id
        if storage_id and batch_engine not in ["SNOWFLAKE"]:
            pipeline_json_object["storage_id"] = storage_id
        if batch_engine != "":
            pipeline_json_object["batch_engine"] = batch_engine
        run_job_on_data_plane = False
        compute_name = self.configuration_obj["configuration"].get("entity", {}).get("computeName", "")
        if compute_name != "" and self.interactive_id:
            pipeline_json_object["compute_template_id"] = self.interactive_id
            run_job_on_data_plane=True
        pipeline_json_object["run_job_on_data_plane"] = run_job_on_data_plane
        snowflake_profile = self.configuration_obj["configuration"].get("pipeline_configs", {}).get("snowflake_profile", "")
        if snowflake_profile!="":
            pipeline_json_object["snowflake_profile"] = snowflake_profile
        warehouse = self.configuration_obj["configuration"].get("entity", {}).get("warehouse", "")
        if warehouse:
            pipeline_json_object["snowflake_warehouse"] = warehouse

        if domain_id is None and domain_name is None:
            pipeline_client_obj.logger.error('Either domainId or domain Name is required to create pipeline.')
            print('Either domainId or domain Name is required to create pipeline.')
            traceback.print_stack()
            raise Exception("Either domainId or domain Name is required to create pipeline.")
        if domain_name is not None and domain_id is None:
            domains_url_base = list_domains_url(pipeline_client_obj.client_config)
            filter_condition = IWUtils.ejson_serialize({"name": domain_name})
            domains_url = domains_url_base + f"?filter={{filter_condition}}".format(filter_condition=filter_condition)
            response = requests.request("GET", domains_url, headers={
                'Authorization': 'Bearer ' + pipeline_client_obj.client_config["bearer_token"],
                'Content-Type': 'application/json'}, verify=False)
            if response.status_code == 406:
                headers = pipeline_client_obj.regenerate_bearer_token_if_needed(
                    {'Authorization': 'Bearer ' + pipeline_client_obj.client_config["bearer_token"],
                     'Content-Type': 'application/json'})
                response = requests.request("GET", domains_url, headers=headers, verify=False)
            final_domain_id = None
            if response is not None:
                result = response.json().get("result", [])
                if len(result) > 0:
                    final_domain_id = result[0]["id"]
                    pipeline_json_object["domain_id"] = final_domain_id
                else:
                    pipeline_client_obj.logger.error('Can not find domain with given name {} '.format(domain_name))
                    pipeline_client_obj.logger.info('Creating a domain with given name {} '.format(domain_name))
                    print(f'Can not find domain with given name {domain_name}')
                    print(f'Creating a domain with given name {domain_name}')
                    domain_id_new = domain_obj.create(pipeline_client_obj, domain_name)
                    print('New domain id' + domain_id_new)
                    pipeline_json_object["domain_id"] = domain_id_new
                    final_domain_id = domain_id_new
            pipeline_client_obj.logger.info('domainId {}'.format(final_domain_id))
        else:
            final_domain_id = domain_id
            pipeline_json_object["domain_id"] = domain_id
        pipeline_client_obj.logger.info('Adding user {} to domain {}'.format(user_email, final_domain_id))
        print(f"Adding user {user_email} to domain {final_domain_id}")
        domain_obj.add_user_to_domain(pipeline_client_obj, final_domain_id, None, user_email)
        pipeline_client_obj.logger.info(
            'Adding sources {} to domain {}'.format(sourceids_in_pipelines, final_domain_id))
        print(f'Adding sources {sourceids_in_pipelines} to domain {final_domain_id}')
        domain_obj.add_sources_to_domain(pipeline_client_obj, final_domain_id, sourceids_in_pipelines)
        url_for_creating_pipeline = create_pipeline_url(pipeline_client_obj.client_config,
                                                        pipeline_json_object["domain_id"])
        pipeline_client_obj.logger.info('url - ' + url_for_creating_pipeline)
        json_string = IWUtils.ejson_serialize(pipeline_json_object)
        pipeline_client_obj.logger.info(json_string)
        print(json_string)
        if json_string is not None:
            try:
                response = requests.post(url_for_creating_pipeline, data=json_string, headers={
                    'Authorization': 'Bearer ' + pipeline_client_obj.client_config["bearer_token"],
                    'Content-Type': 'application/json'}, verify=False)
                new_pipeline_creation_response = response.content
                print("Pipeline creation response:",new_pipeline_creation_response)
                if response.status_code == 406:
                    headers = pipeline_client_obj.regenerate_bearer_token_if_needed(
                        {'Authorization': 'Bearer ' + pipeline_client_obj.client_config["bearer_token"],
                         'Content-Type': 'application/json'})
                    response = requests.post(url_for_creating_pipeline, data=json_string, headers=headers, verify=False)

                response = IWUtils.ejson_deserialize(response.content)
                result = response.get('result', None)
                pipeline_client_obj.logger.info("result is: " + str(result))
                if result is None:
                    pipeline_client_obj.logger.info(
                        'Cant create pipeline. {} {}'.format(response.get('message'), response.get('details')))
                    pipeline_client_obj.logger.info('Getting the existing pipelineId with given name.')
                    pipeline_base_url = create_pipeline_url(pipeline_client_obj.client_config, final_domain_id)
                    filter_condition = IWUtils.ejson_serialize({"name": pipeline_name})
                    pipeline_get_url = pipeline_base_url + f"?filter={{filter_condition}}".format(
                        filter_condition=filter_condition)
                    response = requests.request("GET", pipeline_get_url, headers={
                        'Authorization': 'Bearer ' + pipeline_client_obj.client_config["bearer_token"],
                        'Content-Type': 'application/json'}, verify=False)
                    if response.status_code == 406:
                        headers = pipeline_client_obj.regenerate_bearer_token_if_needed(
                            {'Authorization': 'Bearer ' + pipeline_client_obj.client_config["bearer_token"],
                             'Content-Type': 'application/json'})
                        response = requests.request("GET", pipeline_get_url, headers=headers, verify=False)
                    existing_pipeline_id = None
                    if response.status_code == 200 and len(response.json().get("result", [])) > 0:
                        existing_pipeline_id = response.json().get("result", [])[0]["id"]
                    if existing_pipeline_id:
                        new_pipeline_id = str(existing_pipeline_id)
                else:
                    new_pipeline_id = result.get('id')
                pipeline_client_obj.logger.info(f'Pipeline ID: {new_pipeline_id}')
                print(f'Pipeline ID: {new_pipeline_id}')
                if new_pipeline_id is None:
                    pipeline_client_obj.logger.exception('Pipeline creation response: {}'.format(str(new_pipeline_creation_response)))
            except Exception as ex:
                pipeline_client_obj.logger.exception('Response from server: {}'.format(str(ex)))
                print(f'Response from server: {str(ex)}')

        return new_pipeline_id, pipeline_json_object["domain_id"]

    def configure(self, pipeline_client_obj, pipeline_id, domain_id, override_dataconnection_config_file=None,
                  mappings=None, read_passwords_from_secrets=False, env_tag="", secret_type=""):
        if mappings is None:
            mappings = {}
        if self.configuration_obj.get("dataconnection_configurations", None):
            pipeline_client_obj.logger.info("Checking for any data connection")
            print("Checking for any data connection")
            create_data_connection_url = create_data_connection(pipeline_client_obj.client_config, domain_id)
            for item in self.configuration_obj.get("dataconnection_configurations"):
                pipeline_client_obj.logger.info("Creating a data connection {}".format(item["name"]))
                print(f"Creating a data connection {item['name']}")
                override_keys = []
                if override_dataconnection_config_file is not None:
                    with open(override_dataconnection_config_file) as file:
                        information = yaml.load(file, Loader=yaml.FullLoader)
                    if information["dataconnection_details"].get(item["name"], None) is not None:
                        override_keys = information["dataconnection_details"].get(item["name"], {}).keys()
                        for key in override_keys:
                            item["properties"][key] = information["source_details"][item["name"]][key]
                # update the bigquery service json if any for data connection
                if item.get("sub_type").upper() == "BIGQUERY":
                    if item.get("properties").get("authentication_mechanism",
                                                  "").lower() != "system" and "server_path" not in override_keys:

                        server_path = item.get("properties").get("server_path")
                        if "gcp_details" in mappings:
                            server_path = mappings["gcp_details"].get("service_json_path")
                        if "service_json_mappings" in mappings:
                            server_path = mappings["service_json_mappings"].get(
                                server_path.split("/")[-1],
                                server_path)
                        item["properties"]["server_path"] = server_path
                    if "upload_option" in item["properties"]:
                        item["properties"]["upload_option"] = "serverLocation"
                elif item.get("sub_type").upper() in ["SNOWFLAKE", "POSTGRES"]:
                    pass
                data = IWUtils.ejson_serialize(item)
                response = requests.post(create_data_connection_url,
                                         headers={'Authorization': 'Bearer ' + pipeline_client_obj.client_config[
                                             "bearer_token"],
                                                  'Content-Type': 'application/json'}, data=data, verify=False)
                if response.status_code == 406:
                    pipeline_client_obj.client_config['bearer_token'] = get_bearer_token(
                        pipeline_client_obj.client_config["protocol"],
                        pipeline_client_obj.client_config["ip"],
                        pipeline_client_obj.client_config["port"],
                        pipeline_client_obj.client_config["refresh_token"])
                    headers = IWUtils.get_default_header_for_v3(pipeline_client_obj.client_config['bearer_token'])
                    response = requests.post(create_data_connection_url, headers=headers, data=data, verify=False)
                response = IWUtils.ejson_deserialize(response.content)
                pipeline_client_obj.logger.info(response)
                print(response)

        import_configs = {
            "run_pipeline_metadata_build": True,
            "is_pipeline_version_active": True,
            "import_data_connection": True,
            "include_optional_properties": True
        }
        url_for_importing_pipeline = configure_pipeline_url(pipeline_client_obj.client_config, domain_id, pipeline_id)
        pipeline_client_obj.logger.info(f"URL to configure pipeline: {url_for_importing_pipeline}")
        print(f"URL to configure pipeline: {url_for_importing_pipeline}")
        del self.configuration_obj["environment_configurations"]
        del self.configuration_obj["user_email"]
        json_string = IWUtils.ejson_serialize(
            {"configuration": self.configuration_obj["configuration"], "import_configs": import_configs})
        response = requests.post(url_for_importing_pipeline, data=json_string,
                                 headers={
                                     'Authorization': 'Bearer ' + pipeline_client_obj.client_config["bearer_token"],
                                     'Content-Type': 'application/json'}, verify=False)
        if response.status_code == 406:
            pipeline_client_obj.client_config['bearer_token'] = get_bearer_token(
                pipeline_client_obj.client_config["protocol"],
                pipeline_client_obj.client_config["ip"], pipeline_client_obj.client_config["port"],
                pipeline_client_obj.client_config["refresh_token"])
            headers = IWUtils.get_default_header_for_v3(pipeline_client_obj.client_config['bearer_token'])
            response = requests.post(url_for_importing_pipeline, data=json_string, headers=headers, verify=False)
        response = IWUtils.ejson_deserialize(response.content)
        if response is not None:
            pipeline_client_obj.logger.info(response.get("message", "") + " Done")
            pipeline_client_obj.logger.info(response)
            print(f'{response.get("message", "")} Done')
            print(response)
            pipeline_metadata_build_status = self.poll_pipeline_job(pipeline_client_obj, domain_id, pipeline_id)
            return pipeline_metadata_build_status
        else:
            print(response)
            return "FAILED"
