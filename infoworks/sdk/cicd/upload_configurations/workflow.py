from infoworks.sdk.url_builder import list_sources_url, create_workflow_url, list_domains_url, configure_workflow_url
from infoworks.sdk.utils import IWUtils
import sys
import requests
from infoworks.sdk.cicd.upload_configurations.domains import Domain
from infoworks.core.iw_authentication import get_bearer_token


class Workflow:
    def __init__(self, workflow_configuration_path, replace_words=""):
        with open(workflow_configuration_path, 'r') as file:
            json_string = file.read()
            if replace_words != "":
                for key, value in [item.split("->") for item in replace_words.split(";")]:
                    json_string = json_string.replace(key, value)
        self.configuration_obj = IWUtils.ejson_deserialize(json_string)

    def create(self, wf_client_obj, domain_id, domain_name):
        sources_in_wfs = []
        workflow_name = self.configuration_obj["configuration"]["entity"]["entity_name"]
        for item in self.configuration_obj["configuration"]["iw_mappings"]:
            if item["entity_type"] == "table_group" and "source_name" in item["recommendation"]:
                sources_in_wfs.append(item["recommendation"].get("source_name"))
        filter_condition = IWUtils.ejson_serialize({"name": {"$in": sources_in_wfs}})
        source_list_url = list_sources_url(wf_client_obj.client_config)
        wf_client_obj.logger.info(f"Listing source url {source_list_url}")
        src_list_url = source_list_url + f"?filter={{filter_condition}}".format(filter_condition=filter_condition)
        response = requests.request("GET", src_list_url,
                                    headers={'Authorization': 'Bearer ' + wf_client_obj.client_config['bearer_token'],
                                             'Content-Type': 'application/json'}, verify=False)
        if response.status_code == 406:
            wf_client_obj.client_config['bearer_token'] = get_bearer_token(wf_client_obj.client_config["protocol"],
                                                                           wf_client_obj.client_config["ip"],
                                                                           wf_client_obj.client_config["port"],
                                                                           wf_client_obj.client_config["refresh_token"])
            headers = IWUtils.get_default_header_for_v3(wf_client_obj.client_config['bearer_token'])
            response = requests.request("GET", src_list_url,
                                        headers=headers, verify=False)
        wf_client_obj.logger.info(response.json())
        temp_src_ids = []
        if response.status_code == 200 and len(response.json().get("result", [])) > 0:
            result = response.json().get("result", [])
            for item in result:
                temp_src_ids.append(item["id"])
        sourceids_in_wfs = list(set(temp_src_ids))
        user_email = self.configuration_obj["user_email"]
        domain_obj = Domain(None)
        new_workflow_id = ''
        final_domain_id = None
        if domain_id is None and domain_name is None:
            wf_client_obj.logger.error('Either domainId or domain Name is required to create workflow.')
            sys.exit(-1)
        if domain_name is not None and domain_id is None:
            domains_url_base = list_domains_url(wf_client_obj.client_config)
            filter_condition = IWUtils.ejson_serialize({"name": domain_name})
            domains_url = domains_url_base + f"?filter={{filter_condition}}".format(filter_condition=filter_condition)
            response = requests.request("GET", domains_url, headers={
                'Authorization': 'Bearer ' + wf_client_obj.client_config["bearer_token"],
                'Content-Type': 'application/json'}, verify=False)
            if response.status_code == 406:
                headers = wf_client_obj.regenerate_bearer_token_if_needed(
                    {'Authorization': 'Bearer ' + wf_client_obj.client_config["bearer_token"],
                     'Content-Type': 'application/json'})
                response = requests.request("GET", domains_url, headers=headers, verify=False)
            existing_domain_id = None
            if response is not None:
                result = response.json().get("result", [])
                if len(result) > 0:
                    existing_domain_id = result[0]["id"]
                    final_domain_id = existing_domain_id
                else:
                    wf_client_obj.logger.error('Can not find domain with given name {} '.format(domain_name))
                    wf_client_obj.logger.error('Unable to create workflow')
                    raise Exception("Unable to create workflow")
                    # wf_client_obj.logger.info('Creating a domain with given name {} '.format(domain_name))
                    # domain_id_new = domain_obj.create(wf_client_obj, domain_name)
                    # print('New domain id' + domain_id_new)
                    # final_domain_id = domain_id_new
            wf_client_obj.logger.info('domainId {}'.format(existing_domain_id))
        else:
            final_domain_id = domain_id

        wf_client_obj.logger.info('Adding user {} to domain {}'.format(user_email, final_domain_id))
        domain_obj.add_user_to_domain(wf_client_obj, final_domain_id, None, user_email)
        wf_client_obj.logger.info('Adding sources {} to domain {}'.format(sourceids_in_wfs, final_domain_id))
        domain_obj.add_sources_to_domain(wf_client_obj, final_domain_id, sourceids_in_wfs)
        url_for_creating_workflow = create_workflow_url(wf_client_obj.client_config, final_domain_id)
        workflow_json_object = {
            "name": workflow_name
        }
        wf_client_obj.logger.info('URL to create workflow: ' + url_for_creating_workflow)
        json_string = IWUtils.ejson_serialize(workflow_json_object)
        wf_client_obj.logger.debug(json_string)
        if json_string is not None:
            try:
                response = requests.post(url_for_creating_workflow, data=json_string,
                                         headers={
                                             'Authorization': 'Bearer ' + wf_client_obj.client_config['bearer_token'],
                                             'Content-Type': 'application/json'}, verify=False)
                if response.status_code == 406:
                    wf_client_obj.client_config['bearer_token'] = get_bearer_token(
                        wf_client_obj.client_config["protocol"],
                        wf_client_obj.client_config["ip"],
                        wf_client_obj.client_config["port"],
                        wf_client_obj.client_config["refresh_token"])
                    headers = IWUtils.get_default_header_for_v3(wf_client_obj.client_config['bearer_token'])
                    response = requests.post(url_for_creating_workflow, data=json_string, headers=headers, verify=False)
                response = IWUtils.ejson_deserialize(response.content)
                wf_client_obj.logger.debug(response)
                result = response.get('result', None)
                wf_client_obj.logger.info("result is: " + str(result))
                if result is None:
                    wf_client_obj.logger.info(
                        'Cant create workflow. {} {}'.format(response.get('message'), response.get('details')))
                    wf_client_obj.logger.info('Getting the existing workflow ID with given name.')
                    workflow_base_url = create_workflow_url(wf_client_obj.client_config, final_domain_id)
                    filter_condition = IWUtils.ejson_serialize({"name": workflow_name})
                    workflow_get_url = workflow_base_url + f"?filter={{filter_condition}}".format(
                        filter_condition=filter_condition)
                    response = requests.request("GET", workflow_get_url, headers={
                        'Authorization': 'Bearer ' + wf_client_obj.client_config['bearer_token'],
                        'Content-Type': 'application/json'}, verify=False)
                    if response.status_code == 406:
                        wf_client_obj.client_config['bearer_token'] = get_bearer_token(
                            wf_client_obj.client_config["protocol"],
                            wf_client_obj.client_config["ip"],
                            wf_client_obj.client_config["port"],
                            wf_client_obj.client_config["refresh_token"])
                        headers = IWUtils.get_default_header_for_v3(wf_client_obj.client_config['bearer_token'])
                        response = requests.request("GET", workflow_get_url, headers=headers, verify=False)
                    wf_client_obj.logger.debug(response)
                    existing_workflow_id = None
                    if response.status_code == 200 and len(response.json().get("result", [])) > 0:
                        existing_workflow_id = response.json().get("result", [])[0]["id"]
                        wf_client_obj.logger.info("Workflow ID found {}".format(existing_workflow_id))
                    if existing_workflow_id:
                        new_workflow_id = str(existing_workflow_id)
                else:
                    new_workflow_id = result.get('id')
            except Exception as ex:
                wf_client_obj.logger.error('Response from server: {}'.format(str(ex)))

        return new_workflow_id, final_domain_id

    def configure(self, wf_client_obj, workflow_id, domain_id):
        url_for_importing_workflow = configure_workflow_url(wf_client_obj.client_config, domain_id, workflow_id)
        json_string = IWUtils.ejson_serialize({"configuration": self.configuration_obj["configuration"]})
        response = requests.put(url_for_importing_workflow, data=json_string,
                                headers={'Authorization': 'Bearer ' + wf_client_obj.client_config['bearer_token'],
                                         'Content-Type': 'application/json'}, verify=False)
        if response.status_code == 406:
            wf_client_obj.client_config['bearer_token'] = get_bearer_token(wf_client_obj.client_config["protocol"],
                                                                           wf_client_obj.client_config["ip"],
                                                                           wf_client_obj.client_config["port"],
                                                                           wf_client_obj.client_config["refresh_token"])
            headers = IWUtils.get_default_header_for_v3(wf_client_obj.client_config['bearer_token'])
            response = requests.put(url_for_importing_workflow, data=json_string, headers=headers, verify=False)
        response = IWUtils.ejson_deserialize(response.content)
        wf_client_obj.logger.info(response)
        if response is not None:
            wf_client_obj.logger.info(response.get("message", "") + " Done")
