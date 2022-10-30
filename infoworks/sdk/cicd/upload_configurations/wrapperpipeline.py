import traceback

from infoworks.sdk import url_builder
from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.cicd.upload_configurations.pipelines import Pipeline
from pathlib import Path
from infoworks.sdk.generic_response import GenericResponse
from infoworks.sdk.utils import IWUtils
from infoworks.sdk.local_configurations import Response
import os.path
import queue
import threading
import networkx as nx


class WrapperPipeline(BaseClient):
    def __init__(self):
        super().__init__()

    def __wrapper_get_environment_details(self, environment_id=None, params=None):
        if params is None and environment_id is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_environments = url_builder.get_environment_details(
            self.client_config, environment_id) + IWUtils.get_query_params_string_from_dict(params=params)
        env_details = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_environments,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                if environment_id is not None:
                    env_details.extend(result)
                else:
                    while len(result) > 0:
                        env_details.extend(result)
                        nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                        response = IWUtils.ejson_deserialize(
                            self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                                self.client_config['bearer_token'])).content)
                        result = response.get("result", [])
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=env_details)
        except Exception as e:
            self.logger.error("Error in getting environment details")

    def __wrapper_get_storage_details(self, environment_id, storage_id=None, params=None):
        if params is None and storage_id is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_storages = url_builder.get_environment_storage_details(
            self.client_config, environment_id, storage_id) + IWUtils.get_query_params_string_from_dict(params=params)
        storage_details = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_storages,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                if storage_id is not None:
                    storage_details.extend(result)
                else:
                    while len(result) > 0:
                        storage_details.extend(result)
                        nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                        response = IWUtils.ejson_deserialize(
                            self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                                self.client_config['bearer_token'])).content)
                        result = response.get("result", [])
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=storage_details)
        except Exception as e:
            self.logger.error("Error in getting storage details")

    def __wrapper_get_compute_template_details(self, environment_id, compute_id=None, is_interactive=False,
                                               params=None):
        if params is None and compute_id is None:
            params = {"limit": 20, "offset": 0}
        if is_interactive:
            url_to_list_computes = url_builder.get_environment_interactive_compute_details(
                self.client_config, environment_id, compute_id) + IWUtils.get_query_params_string_from_dict(
                params=params)
        else:
            url_to_list_computes = url_builder.get_environment_compute_details(
                self.client_config, environment_id, compute_id) + IWUtils.get_query_params_string_from_dict(
                params=params)
        compute_details = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_computes,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None and "result" in response:
                result = response.get("result", [])
                if compute_id is not None:
                    compute_details.extend(result)
                else:
                    while len(result) > 0:
                        compute_details.extend(result)
                        nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                        response = IWUtils.ejson_deserialize(
                            self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                                self.client_config['bearer_token'])).content)
                        result = response.get("result", [])
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=compute_details)
        except Exception as e:
            self.logger.error("Error in getting compute template details")

    def cicd_create_configure_pipeline(self, configuration_file_path, domain_id=None, domain_name=None,
                                       override_configuration_file=None,
                                       replace_words="", read_passwords_from_secrets=False, env_tag="", secret_type=""):
        """
        Function to create and configure pipeline using the pipeline configuration JSON file
        Pass either domain_id or domain_name.If both are not passed the name of the domain should be first part of the file name
        :param configuration_file_path: Path of the file with pipeline configurations to be imported
        :param domain_id: Domain id to which the pipeline belongs to
        :param domain_name: Domain name to which the pipeline belongs to
        :param override_configuration_file: Path of the file with override keys for dataconnection properties
        :param replace_words: Pass the strings to be replaced in the configuration file. Example: DEV->PROD;dev->prod
        :param read_passwords_from_secrets: True/False. If True all the pipeline related passwords are read from encrypted file name passed
        """
        try:
            if domain_id is None and domain_name is None:
                domain_name = Path(configuration_file_path).name.split("#")[0]
            env_id = self.client_config.get("default_environment_id", None)
            storage_id = self.client_config.get("default_storage_id", None)
            compute_id = self.client_config.get("default_compute_id", None)
            with open(configuration_file_path, 'r') as file:
                json_string = file.read()
            configuration_obj = IWUtils.ejson_deserialize(json_string)
            environment_configurations = configuration_obj["environment_configurations"]
            if env_id is None and "environment_mappings" in self.mappings:
                env_name = self.mappings["environment_mappings"].get(environment_configurations["environment_name"],
                                                                     environment_configurations["environment_name"])
                if env_name is not None:
                    result = self.__wrapper_get_environment_details(params={"filter": {"name": env_name}})
                    env_id = result["result"]["response"][0]["id"] if len(result["result"]["response"]) > 0 else None
            if storage_id is None and "storage_mappings" in self.mappings:
                storage_name = self.mappings["storage_mappings"].get(
                    environment_configurations["environment_storage_name"],
                    environment_configurations["environment_storage_name"])
                if storage_name is not None:
                    result = self.__wrapper_get_storage_details(environment_id=env_id,
                                                                params={"filter": {"name": storage_name}})
                    storage_id = result["result"]["response"][0]["id"] if len(result["result"]["response"]) > 0 else None
            if compute_id is None and "compute_mappings" in self.mappings:
                compute_name = self.mappings["compute_mappings"].get(
                    environment_configurations["environment_compute_template_name"],
                    environment_configurations["environment_compute_template_name"])
                if compute_name is not None:
                    result = self.__wrapper_get_compute_template_details(environment_id=env_id,
                                                                         params={"filter": {"name": compute_name}})
                    if len(result["result"]["response"]) != 0:
                        compute_id = result["result"]["response"][0]["id"]
                    else:
                        result = self.__wrapper_get_compute_template_details(environment_id=env_id, is_interactive=True,
                                                                             params={"filter": {"name": compute_name}})
                        if len(result["result"]["response"]) != 0:
                            compute_id = result["result"]["response"][0]["id"]
            if env_id is None:
                print("No env id and no mapping found")
                raise Exception("No env id and no mapping found")
            pl_obj = Pipeline(configuration_file_path, env_id, storage_id, compute_id, replace_words, self.secrets_config)
            pipeline_id, domain_id = pl_obj.create(self, domain_id, domain_name)
            if pipeline_id is not None:
                status = pl_obj.configure(self, pipeline_id, domain_id, override_configuration_file, self.mappings, read_passwords_from_secrets,env_tag=env_tag, secret_type=secret_type)
        except Exception as e:
            self.logger.error(str(e))
            print(str(e))
            traceback.print_stack()

    def __execute(self, thread_number, q):
        while True:
            try:
                print('%s: Looking for the next task ' % thread_number)
                task = q.get()
                print(f'\nThread Number {thread_number} processed {task}')
                entity_type = task["entity_type"]
                if entity_type == "pipeline":
                    replace_words = task["replace_words"] if task["replace_words"] else ""
                    override_configuration_file = ""
                    read_passwords_from_secrets = task.get("read_passwords_from_secrets", False)
                    self.cicd_create_configure_pipeline(task["pipeline_config_path"], None,
                                                        task["domain_name"],
                                                        override_configuration_file, replace_words, read_passwords_from_secrets)
            except Exception as e:
                print(str(e))
            q.task_done()

    def cicd_create_pipelineartifacts_in_bulk(self, maintain_lineage, pipeline_gexf_lineage_file=None,
                                              configurations_base_path=None,
                                              pl_replace_words=None,
                                              override_configuration_file=None, read_passwords_from_secrets=False):
        """
        Function to configure pipelines in bulk
        :param maintain_lineage: True/False. If True pass the pipeline_gexf_lineage_file which has a dag of pipeline dependencies
        :param pipeline_gexf_lineage_file: File with pipeline dependencies
        :param configurations_base_path: Path with all the source configuration dumps
        :param pl_replace_words: Pass the strings to be replaced in the configuration file. Example: DEV->PROD;dev->prod
        :param override_configuration_file: Path of the file with override keys for dataconnection properties
        :param read_passwords_from_secrets: True/False. If True all the source related passwords are read from encrypted file name passed
        """
        num_fetch_threads = 10
        job_queue = queue.Queue(maxsize=100)
        for i in range(num_fetch_threads):
            worker = threading.Thread(target=self.__execute, args=(i, job_queue,))
            worker.setDaemon(True)
            worker.start()

        if maintain_lineage:
            if pipeline_gexf_lineage_file:
                path = os.path.join(configurations_base_path, "modified_files/pipeline.gexf")
            else:
                path = pipeline_gexf_lineage_file
            graph = nx.read_gexf(path)
            for item in self._topological_sort_grouping(graph):
                for i in item:
                    if i != "root":
                        pipeline_file = graph._node[i]['filename'].strip()
                        domain_name = pipeline_file.split("#")[0]
                        pipeline_args = {"entity_type": "pipeline",
                                         "pipeline_config_path": os.path.join(configurations_base_path, "pipeline",
                                                                              pipeline_file.strip()),
                                         "domain_name": domain_name, "replace_words": pl_replace_words,
                                         "override_configuration_file": override_configuration_file, "read_passwords_from_secrets": read_passwords_from_secrets}
                        job_queue.put(pipeline_args)
                print('*** Main thread waiting to complete all pending pipeline creation/configuration requests ***')
                job_queue.join()
                print('*** Done with All Tasks ***')
        else:
            with open(os.path.join(configurations_base_path, "modified_files", "pipeline.csv"),
                      "r") as pipeline_files_fp:
                for pipeline_file in pipeline_files_fp.readlines():
                    pl_name = pipeline_file.strip()
                    domain_name = pl_name.split("#")[0]
                    pipeline_args = {"entity_type": "pipeline",
                                     "pipeline_config_path": os.path.join(configurations_base_path, "pipeline",
                                                                          pipeline_file.strip()),
                                     "domain_name": domain_name, "replace_words": pl_replace_words,
                                     "override_configuration_file": override_configuration_file}
                    job_queue.put(pipeline_args)

            print('*** Main thread waiting to complete all pipeline configuration requests ***')
            job_queue.join()
            print('*** Done with Pipeline Configurations ***')
