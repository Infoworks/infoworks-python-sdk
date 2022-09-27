import os

from infoworks.sdk.cicd.download_configurations.get_pipeline_configuration import DownloadPipeline
from infoworks.sdk.cicd.download_configurations.get_workflow_configuration import DownloadWorkflow
from infoworks.sdk.url_builder import create_pipeline_url, create_workflow_url
from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.utils import IWUtils


class DownloadAllEntitiesFromDomain(BaseClient):
    def __init__(self):
        super(DownloadAllEntitiesFromDomain, self).__init__()

    def callurl(self, nextUrl):
        try:
            self.logger.info("url {url}".format(url=nextUrl))
            response = self.call_api("GET", nextUrl,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))

            parsed_response = IWUtils.ejson_deserialize(response.content)
            if parsed_response is not None and response.status_code == 200:
                return parsed_response
        except Exception as e:
            raise Exception("Unable to get response for url: {url}".format(url=nextUrl))

    def cicd_get_all_configuration_dumps_from_domain(self, domain_ids, config_file_dump_path, files_overwrite=True,
                                                serviceaccountemail="admin@infoworks.io",
                                                replace_words=""):
        if not os.path.exists(os.path.join(config_file_dump_path, "modified_files")):
            os.makedirs(os.path.join(config_file_dump_path, "modified_files"))
        if not os.path.exists(os.path.join(config_file_dump_path, "source")):
            os.makedirs(os.path.join(config_file_dump_path, "source"))
        if not os.path.exists(os.path.join(config_file_dump_path, "pipeline")):
            os.makedirs(os.path.join(config_file_dump_path, "pipeline"))
        if not os.path.exists(os.path.join(config_file_dump_path, "workflow")):
            os.makedirs(os.path.join(config_file_dump_path, "workflow"))
        for domain_id in domain_ids:
            get_pipeline_url = create_pipeline_url(self.client_config, str(domain_id))
            pipeline_ids = []
            response = self.call_api("GET", get_pipeline_url,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))

            parsed_response = IWUtils.ejson_deserialize(response.content)
            if response.status_code == 200:
                result = parsed_response.get("result", [])
                while len(result) > 0:
                    pipeline_ids.extend([item["id"] for item in result])
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.json().get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = self.callurl(nextUrl)
                    result = response.get("result", [])
            get_workflow_url = create_workflow_url(self.client_config, str(domain_id))
            workflow_ids = []
            response = self.call_api("GET", get_workflow_url,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token']))

            parsed_response = IWUtils.ejson_deserialize(response.content)
            if response.status_code == 200:
                result = parsed_response.get("result", [])
                while len(result) > 0:
                    workflow_ids.extend([item["id"] for item in result])
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.json().get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = self.callurl(nextUrl)
                    result = response.get("result", [])

            if len(pipeline_ids) > 0:
                pipeline_obj = DownloadPipeline()
                pipeline_obj.cicd_get_pipelineconfig_dumps(pipeline_ids, config_file_dump_path, files_overwrite,
                                                      serviceaccountemail,
                                                      replace_words)
            if len(workflow_ids) > 0:
                wf_obj = DownloadWorkflow()
                wf_obj.cicd_get_workflowconfig_dumps(workflow_ids, config_file_dump_path, files_overwrite,
                                                serviceaccountemail,
                                                replace_words)
