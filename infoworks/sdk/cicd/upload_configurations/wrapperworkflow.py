import traceback

from infoworks.sdk import url_builder
from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.cicd.upload_configurations.workflow import Workflow
from pathlib import Path
from infoworks.sdk.generic_response import GenericResponse
from infoworks.sdk.utils import IWUtils
from infoworks.sdk.local_configurations import Response
import os.path
import queue
import threading
import networkx as nx


class WrapperWorkflow(BaseClient):
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

    def cicd_create_configure_workflow(self, configuration_file_path, domain_id=None, domain_name=None,
                                       replace_words=""):
        """
        Function to create and configure workflow using the workflow configuration JSON file.
        Pass either domain_id or domain_name.If both are not passed the name of the domain should be first part of the file name
        :param configuration_file_path: Path of the file with workflow configurations to be imported
        :param domain_id: Domain id to which the pipeline belongs to.
        :param domain_name: Domain name to which the pipeline belongs to
        :param replace_words: Pass the strings to be replaced in the configuration file. Example: DEV->PROD;dev->prod
        """
        try:
            if domain_id is None and domain_name is None:
                domain_name = Path(configuration_file_path).name.split("#")[0]
            wf_obj = Workflow(configuration_file_path, replace_words)
            wf_id, domain_id = wf_obj.create(self, domain_id, domain_name)
            if wf_id is not None:
                status = wf_obj.configure(self, wf_id, domain_id)
        except Exception as e:
            self.logger.error(str(e))
            traceback.print_stack()

    def __execute(self, thread_number, q):
        while True:
            try:
                print('%s: Looking for the next task ' % thread_number)
                task = q.get()
                print(f'\nThread Number {thread_number} processed {task}')
                entity_type = task["entity_type"]
                if entity_type == "workflow":
                    replace_words = task["replace_words"] if task["replace_words"] else ""
                    self.cicd_create_configure_workflow(task["workflow_config_path"], None,
                                                        task["domain_name"],
                                                        replace_words)
                else:
                    pass
            except Exception as e:
                print(str(e))
            q.task_done()

    def cicd_create_workflowartifacts_in_bulk(self, maintain_lineage, workflow_gexf_lineage_file=None,
                           configurations_base_path=None,
                           wf_replace_words=None):
        num_fetch_threads = 10
        job_queue = queue.Queue(maxsize=100)
        for i in range(num_fetch_threads):
            worker = threading.Thread(target=self.__execute, args=(i, job_queue,))
            worker.setDaemon(True)
            worker.start()
        if maintain_lineage == 'true':
            if workflow_gexf_lineage_file:
                path = os.path.join(configurations_base_path, "modified_files/workflow.gexf")
            else:
                path = workflow_gexf_lineage_file
            graph = nx.read_gexf(path)
            for item in self._topological_sort_grouping(graph):
                for i in item:
                    if i != "root":
                        wf_name = graph._node[i]['filename'].strip()
                        domain_name = wf_name.split("#")[0]
                        wf_args = {"entity_type": "workflow",
                                   "workflow_config_path": os.path.join("configurations/workflow", wf_name),
                                   "domain_name": domain_name}
                        job_queue.put(wf_args)
                print('*** Main thread waiting to complete all pending pipeline creation/configuration requests ***')
                job_queue.join()
                print('*** Done with All Tasks ***')
        else:
            with open(os.path.join("configurations/modified_files", "workflow.csv"), "r") as workflow_file_fp:
                for workflow_file in workflow_file_fp.readlines():
                    wf_name = workflow_file.strip()
                    domain_name = wf_name.split("#")[0]
                    wf_args = {"entity_type": "workflow",
                               "workflow_config_path": os.path.join("configurations/workflow", wf_name),
                               "domain_name": domain_name, "replace_words": wf_replace_words, }
                    job_queue.put(wf_args)

            print('*** Main thread waiting to complete all workflow configuration requests ***')
            job_queue.join()
            print('*** Done with Workflow Configurations  ***')