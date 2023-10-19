import json
import copy
import traceback
import configparser
import requests
import yaml
from infoworks.core.iw_authentication import get_bearer_token
from infoworks.sdk.utils import IWUtils
from infoworks.sdk.url_builder import get_pipeline_group_base_url,list_domains_url,list_pipelines_url
from infoworks.sdk.cicd.upload_configurations.update_configurations import InfoworksDynamicAccessNestedDict
from infoworks.sdk.cicd.upload_configurations.local_configurations import PRE_DEFINED_MAPPINGS

class PipelineGroup:
    def __init__(self, pipeline_group_config_path, environment_id, storage_id, interactive_id,
                 replace_words="", secrets=None):
        self.storage_id = storage_id
        self.interactive_id = interactive_id
        self.environment_id = environment_id
        self.secrets = secrets
        with open(pipeline_group_config_path, 'r') as file:
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
            print("section:", section)
            try:
                final = d.setval(section.split("$"), dict(config.items(section)))
                print(f"section replacement:{d.getval(section.split('$'))}")
            except KeyError as e:
                pass
        self.configuration_obj = d.data


    def list_pipelines(self, pipeline_group_obj,domain_id=None, params=None):
        """
        Function to list the pipelines
        :param domain_id: Entity identified for domain
        :type domain_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response list
        """

        if None in {domain_id}:
            pipeline_group_obj.logger.error("Domain ID cannot be None")
            raise Exception("Domain ID cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_pipelines = list_pipelines_url(pipeline_group_obj.client_config, domain_id) \
                                + IWUtils.get_query_params_string_from_dict(params=params)

        pipelines_list = []
        headers={'Authorization': 'Bearer ' + pipeline_group_obj.client_config["bearer_token"],
                'Content-Type': 'application/json'}
        try:
            response = requests.request("GET", url_to_list_pipelines,
                                 headers=headers, verify=False)
            if response.status_code == 406:
                headers = pipeline_group_obj.regenerate_bearer_token_if_needed(
                    {'Authorization': 'Bearer ' + pipeline_group_obj.client_config["bearer_token"],
                     'Content-Type': 'application/json'})
                response = requests.request("GET", url_to_list_pipelines, headers=headers, verify=False)
            if response.status_code==200:
                response = response.json()
                result = response.get("result", [])
                initial_msg = response.get("message", "")
                while len(result) > 0:
                    pipelines_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=pipeline_group_obj.client_config['ip'],
                                                                      port=pipeline_group_obj.client_config['port'],
                                                                      protocol=pipeline_group_obj.client_config['protocol'],
                                                                      )
                    response = requests.request("GET", nextUrl, headers=headers,verify=False)
                    if response.status_code == 406:
                        headers = pipeline_group_obj.regenerate_bearer_token_if_needed(
                            {'Authorization': 'Bearer ' + pipeline_group_obj.client_config["bearer_token"],
                             'Content-Type': 'application/json'})
                        response = requests.request("GET", nextUrl, headers=headers, verify=False)
                    response = response.json()
                    result = response.get("result", None)
                    if result is None:
                        return response
                response["result"] = pipelines_list
                response["message"] = initial_msg
            return response
        except Exception as e:
            pipeline_group_obj.logger.error("Error in listing pipelines")
            raise Exception("Error in listing pipelines" + str(e))

    def create(self, pipeline_group_obj, domain_id, domain_name):

        pipeline_group_name = self.configuration_obj["name"]
        pipeline_group_id = None
        final_domain_id=domain_id
        if self.environment_id is not None:
            self.configuration_obj["environment_id"]=self.environment_id
        if domain_name is not None and domain_id is None:
            domains_url_base = list_domains_url(pipeline_group_obj.client_config)
            filter_condition = IWUtils.ejson_serialize({"name": domain_name})
            domains_url = domains_url_base + f"?filter={{filter_condition}}".format(filter_condition=filter_condition)
            response = requests.request("GET", domains_url, headers={
                'Authorization': 'Bearer ' + pipeline_group_obj.client_config["bearer_token"],
                'Content-Type': 'application/json'}, verify=False)
            if response.status_code == 406:
                headers = pipeline_group_obj.regenerate_bearer_token_if_needed(
                    {'Authorization': 'Bearer ' + pipeline_group_obj.client_config["bearer_token"],
                     'Content-Type': 'application/json'})
                response = requests.request("GET", domains_url, headers=headers, verify=False)
            if response is not None:
                result = response.json().get("result", [])
                if len(result) > 0:
                    final_domain_id = result[0]["id"]
                    self.configuration_obj["domain_id"] = final_domain_id
        pipeline_group_details_url = get_pipeline_group_base_url(pipeline_group_obj.client_config,final_domain_id)+"?filter={\"name\":\""+pipeline_group_name+"\"}"
        headers = pipeline_group_obj.regenerate_bearer_token_if_needed(
            {'Authorization': 'Bearer ' + pipeline_group_obj.client_config["bearer_token"],
             'Content-Type': 'application/json'})
        pipeline_group_details_response = requests.request("GET", pipeline_group_details_url, headers=headers, verify=False)
        pipeline_group_details_parsed_response = IWUtils.ejson_deserialize(
            pipeline_group_details_response.content)
        pipeline_name_lookup={}
        list_pipelinesparsed_response = self.list_pipelines(pipeline_group_obj=pipeline_group_obj,domain_id=final_domain_id)
        for item in list_pipelinesparsed_response["result"]:
            pipeline_name_lookup[item["name"]]=item["id"]
        for pipeline in self.configuration_obj["pipelines"]:
            pipeline["pipeline_id"]=pipeline_name_lookup[pipeline["name"]]
        if len(pipeline_group_details_parsed_response["result"])>0:
            pipeline_group_obj.logger.info(
                'Pipeline group already exists. {} {}')
            pipeline_group_obj.logger.info('Getting the existing pipeline group Id with given name.')
            pipeline_group_id = pipeline_group_details_parsed_response["result"][0]["pipeline_group_details"]["id"]
            del self.configuration_obj["id"]
            configuration_obj = copy.deepcopy(self.configuration_obj)
            for item in self.configuration_obj:
                if item in ["createdAt","createdBy","modifiedAt","modifiedBy","environment_configurations"]:
                    del configuration_obj[item]
            self.configuration_obj = configuration_obj
            for pipeline in self.configuration_obj["pipelines"]:
                if pipeline.get("name",None) is not None:
                    del pipeline["name"]

            pipeline_group_create_url = get_pipeline_group_base_url(pipeline_group_obj.client_config, final_domain_id)+pipeline_group_id
            response = pipeline_group_obj.call_api("PUT", pipeline_group_create_url, {
                'Authorization': 'Bearer ' + pipeline_group_obj.client_config["bearer_token"],
                'Content-Type': 'application/json'}, data=self.configuration_obj)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            print("parsed_response", parsed_response)
            if response.status_code == 200 and "result" in parsed_response:
                pipeline_group_obj.logger.info("Successfully updated the pipeline group")
                print("Successfully updated the pipeline group")
                pipeline_group_id = parsed_response["result"]["id"]
        else:
            del self.configuration_obj["id"]
            configuration_obj = copy.deepcopy(self.configuration_obj)
            for item in self.configuration_obj:
                if item in ["createdAt", "createdBy", "modifiedAt", "modifiedBy", "environment_configurations"]:
                    del configuration_obj[item]
            self.configuration_obj = configuration_obj
            for pipeline in self.configuration_obj["pipelines"]:
                if pipeline.get("name", None) is not None:
                    del pipeline["name"]

            pipeline_group_create_url = get_pipeline_group_base_url(pipeline_group_obj.client_config,final_domain_id)
            response = pipeline_group_obj.call_api("POST", pipeline_group_create_url, {
                'Authorization': 'Bearer ' + pipeline_group_obj.client_config["bearer_token"],
                'Content-Type': 'application/json'}, data=self.configuration_obj)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            print("parsed_response",parsed_response)
            if response.status_code == 200 and "result" in parsed_response:
                pipeline_group_obj.logger.info("Successfully created and configured the pipeline group")
                print("Successfully created and configured the pipeline group")
                pipeline_group_id=parsed_response["result"]["id"]
        return pipeline_group_id, self.configuration_obj["domain_id"]

