import requests

from infoworks.sdk import url_builder
from infoworks.sdk.url_builder import list_domains_url, list_users_url, add_user_to_domain_url, \
    add_sources_to_domain_url
from infoworks.sdk.utils import IWUtils
from infoworks.core.iw_authentication import get_bearer_token


class Domain:
    def __init__(self, environment_id):
        self.environment_id = environment_id

    def create(self, client, domain_name):
        new_domain_id = ''
        domain_json_object = {
            "name": domain_name,
            "description": "Domain {domain_name} created via API V3".format(domain_name=domain_name),
            "environment_ids": [str(self.environment_id)]
        }
        try:
            create_domain_url = url_builder.create_domain_url(client.client_config)
            response = client.call_api("POST", create_domain_url,
                                       IWUtils.get_default_header_for_v3(client.client_config['bearer_token']),
                                       domain_json_object)
            parsed_response = IWUtils.ejson_deserialize(
                response.content)
            if response.status_code != 200:
                client.logger.info('Cant create domain')
                client.logger.info('Getting the existing domain_id with given name.')
                domains_url_base = list_domains_url(client.client_config)
                filter_condition = str({"name": domain_name})
                domains_url = domains_url_base + f"?filter={{filter_condition}}".format(
                    filter_condition=filter_condition)
                response = client.call_api("GET", domains_url, headers={
                    'Authorization': 'Bearer ' + client.client_config["bearer_token"],
                    'Content-Type': 'application/json'})
                parsed_response = IWUtils.ejson_deserialize(
                    response.content)
                result = parsed_response.get("result", {})
                new_domain_id = None
                if len(result) > 0:
                    new_domain_id = result[0]["id"]
                    existing_env_ids = result[0]["environment_ids"]
                    url_to_update_domain = url_builder.create_domain_url(
                        client.client_config) + f"/{new_domain_id}"
                    config_body = {
                        "name": domain_name,
                        "description": "Domain {domain_name} created via API V3".format(domain_name=domain_name),
                        "environment_ids": existing_env_ids.append(str(self.environment_id))
                    }
                    response = client.call_api("PATCH", url_to_update_domain,
                                               IWUtils.get_default_header_for_v3(client.client_config['bearer_token']),
                                               config_body)
                    if response.status_code == 200:
                        client.logger.info(f"Domain {new_domain_id} updated")
                else:
                    client.logger.error('Can not find domain with given name {} '.format(domain_name))
                client.logger.info('domainId {}'.format(new_domain_id))
            else:
                result = parsed_response.get("result", {})
                new_domain_id = result.get('id')
        except Exception as ex:
            client.logger.error('Response from server: {}'.format(str(ex)))

        return new_domain_id

    def add_user_to_domain(self, client, domain_id, user_id, user_email):
        status_flag = "failed"
        if user_id is None and user_email is not None:
            client.logger.info('Getting userID from given user email....')
            url_for_list_users = list_users_url(client.client_config)
            filter_condition = IWUtils.ejson_serialize({"profile.email": user_email})
            get_userid_url = url_for_list_users + f"?filter={{filter_condition}}".format(
                filter_condition=filter_condition)
            if get_userid_url is not None:
                try:
                    response = requests.request("GET", get_userid_url, headers={
                        'Authorization': 'Bearer ' + client.client_config["bearer_token"],
                        'Content-Type': 'application/json'}, verify=False)
                    if response.status_code == 406:
                        client.client_config['bearer_token'] = get_bearer_token(client.client_config["protocol"],
                                                                                client.client_config["ip"],
                                                                                client.client_config["port"],
                                                                                client.client_config["refresh_token"])
                        headers = IWUtils.get_default_header_for_v3(client.client_config['bearer_token'])
                        response = requests.request("GET", get_userid_url,
                                                    headers=headers, verify=False)
                    client.logger.info(response.json())
                    if response is not None:
                        result = response.json().get("result", [])
                        if len(result) > 0:
                            user_id = result[0]["id"]
                except Exception as e:
                    client.logger.error('Couldnt get result for the user {}'.format(user_email))
                finally:
                    if user_id is None:
                        client.logger.error('Couldnt get result for the user {}'.format(user_email))

        if user_id is not None:
            url_for_adding_user_to_domain = add_user_to_domain_url(client.client_config, user_id)
            client.logger.info('url for adding user to domain - ' + url_for_adding_user_to_domain)
            if url_for_adding_user_to_domain is not None:
                client.logger.info('Adding user {user} to domain {domain}'.format(user=user_id, domain=domain_id))
                try:
                    json_string = IWUtils.ejson_serialize({"entity_ids": [str(domain_id)]})
                    response = requests.post(url_for_adding_user_to_domain, data=json_string,
                                             headers={'Authorization': 'Bearer ' + client.client_config["bearer_token"],
                                                      'Content-Type': 'application/json'},
                                             verify=False)
                    if response.status_code == 406:
                        client.client_config['bearer_token'] = get_bearer_token(client.client_config["protocol"],
                                                                                client.client_config["ip"],
                                                                                client.client_config["port"],
                                                                                client.client_config["refresh_token"])
                        headers = IWUtils.get_default_header_for_v3(client.client_config['bearer_token'])
                        response = requests.post(url_for_adding_user_to_domain, data=json_string,
                                                 headers=headers,
                                                 verify=False)
                    response = response.json()
                    if response.get('message', None) is not None:
                        client.logger.info(response.get('message'))
                        if response.get('message') == "Added Domain(s) to User":
                            status_flag = "success"
                    else:
                        client.logger.error('Error in adding user to domain - {}'.format(response))
                except Exception as e:
                    client.logger.error('Response from server while adding user to domain: {}'.format(user_id))

        return status_flag

    def add_sources_to_domain(self, client, domain_id, source_ids):
        status_flag = "failed"
        url_to_add_source_to_domain = add_sources_to_domain_url(client.client_config, domain_id)
        client.logger.info('url for adding source to domain - ' + url_to_add_source_to_domain)
        if len(source_ids) > 0:
            try:
                source_ids_existing = []
                response = requests.request("GET", url_to_add_source_to_domain,
                                            headers={'Authorization': 'Bearer ' + client.client_config["bearer_token"],
                                                     'Content-Type': 'application/json'}, verify=False)
                if response.status_code == 406:
                    client.client_config['bearer_token'] = get_bearer_token(client.client_config["protocol"],
                                                                            client.client_config["ip"],
                                                                            client.client_config["port"],
                                                                            client.client_config["refresh_token"])
                    headers = IWUtils.get_default_header_for_v3(client.client_config['bearer_token'])
                    response = requests.request("GET", url_to_add_source_to_domain,
                                                headers=headers, verify=False)
                if response is not None:
                    result = response.json().get("result", [])
                    while len(result) > 0:
                        source_ids_existing.extend([item["id"] for item in result])
                        nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.json().get('links')['next'],
                                                                          ip=client.client_config['ip'],
                                                                          port=client.client_config['port'],
                                                                          protocol=client.client_config['protocol'],
                                                                          )
                        response = self.callurl(client, nextUrl)
                        result = response.json().get("result", [])
                missing_srcs = set(source_ids) - set(source_ids_existing)
                json_string = IWUtils.ejson_serialize({"entity_ids": list(missing_srcs)})
                response = requests.post(url_to_add_source_to_domain, data=json_string,
                                         headers={'Authorization': 'Bearer ' + client.client_config["bearer_token"],
                                                  'Content-Type': 'application/json'}, verify=False)
                if response.status_code == 406:
                    client.client_config['bearer_token'] = get_bearer_token(client.client_config["protocol"],
                                                                            client.client_config["ip"],
                                                                            client.client_config["port"],
                                                                            client.client_config["refresh_token"])
                    headers = IWUtils.get_default_header_for_v3(client.client_config['bearer_token'])
                    response = requests.post(url_to_add_source_to_domain, data=json_string,
                                             headers=headers, verify=False)
                response = IWUtils.ejson_deserialize(
                    response.content)
                if response.get('message', None) is not None:
                    client.logger.info(response.get('message'))
                    if response.get('message') == "Successfully added accessible sources to the domain":
                        status_flag = "success"
                else:
                    client.logger.error('Error in adding source(s) to domain - {}'.format(response))
            except Exception as e:
                client.logger.error('Response from server while adding source(s) to domain: {}'.format(str(e)))
        return status_flag

    def callurl(self, client, url):
        try:
            print("url {url}".format(url=url))
            response = requests.request("GET", url,
                                        headers={'Authorization': 'Bearer ' + client.client_config["bearer_token"],
                                                 'Content-Type': 'application/json'})
            if response.status_code == 406:
                client.client_config['bearer_token'] = get_bearer_token(client.client_config["protocol"],
                                                                        client.client_config["ip"],
                                                                        client.client_config["port"],
                                                                        client.client_config["refresh_token"])
                headers = IWUtils.get_default_header_for_v3(client.client_config['bearer_token'])
                response = requests.request("GET", url,
                                            headers=headers, verify=False)
            if response is not None:
                return response
        except Exception as e:
            raise Exception("Unable to get response for url: {url}".format(url=url))
