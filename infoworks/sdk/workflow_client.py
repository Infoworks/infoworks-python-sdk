import traceback

from infoworks.error import WorkflowError
from infoworks.sdk import url_builder
import infoworks.sdk.admin_client as ac
from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.local_configurations import Response
from infoworks.sdk.workflow_response import WorkflowResponse
from infoworks.sdk.utils import IWUtils
import time
from infoworks.sdk.cicd.upload_configurations.workflow import Workflow
from pathlib import Path


class WorkflowClient(BaseClient):
    def __init__(self):
        super(WorkflowClient, self).__init__()

    def get_list_of_workflows(self, domain_id=None, params=None):
        """
        Gets List of Infoworks Data workflow details for given domain id
        :param domain_id: Domain id to which the workflows belongs to, if None all workflows in all domains will be fetched
        :type domain_id: String
        :return: response dict
        """
        response = None
        try:
            if domain_id is None:
                if params is None:
                    params = {"limit": 20, "offset": 0}
                url_to_list_workflows = url_builder.get_all_workflows_url(
                    self.client_config) + IWUtils.get_query_params_string_from_dict(params=params)
                workflows_list = []
                response = IWUtils.ejson_deserialize(
                    self.call_api("GET", url_to_list_workflows,
                                  IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
                print(response)
                if response is not None:
                    result = response.get("result", [])
                    while len(result) > 0:
                        workflows_list.extend(result)
                        nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.json().get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                        response = IWUtils.ejson_deserialize(
                            self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                                self.client_config['bearer_token'])).content)
                        result = response.get("result", [])
                return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, response=workflows_list)
            else:
                workflows_list = []
                response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.create_workflow_url(
                    self.client_config, domain_id), IWUtils.get_default_header_for_v3(
                    self.client_config['bearer_token'])).content)

                result = response.get('result', {})

                if response is not None:
                    result = response.get("result", [])
                    while len(result) > 0:
                        workflows_list.extend(result)
                        nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                        response = IWUtils.ejson_deserialize(
                            self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                                self.client_config['bearer_token'])).content)
                        result = response.get("result", [])
                return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, response=workflows_list)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get workflow details.')
            raise WorkflowError('Error occurred while trying to get workflow details.')

    def create_workflow(self, domain_id=None, workflow_config=None):
        """
        Create a new Workflow
        :param domain_id: Domain id of the workflow
        :type domain_id:String
        :param workflow_config: a JSON object containing workflow configurations
        :type workflow_config: JSON Object

        workflow_config_example = {
        "name": "workflow_name"
        }
        :return: response dict
        """
        response = None
        try:
            if domain_id is None:
                self.logger.error('Please pass the mandatory domain_id as parameter.')
                raise WorkflowError('Please pass the mandatory domain_id as parameter.')

            if workflow_config is None:
                self.logger.error('Invalid workflow configuration. Cannot create a new workflow.')
                raise WorkflowError('Please pass the valid workflow_config as parameter.')
            response = IWUtils.ejson_deserialize(self.call_api("POST", url_builder.create_workflow_url(
                self.client_config, domain_id), IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                                               workflow_config).content)

            result = response.get('result', {})
            workflow_id = result.get('id', None)

            if workflow_id is None:
                self.logger.error('Workflow failed to create.')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Workflow failed to create.', response=response)

            workflow_id = str(workflow_id)
            self.logger.info(
                'Workflow {id} has been created under domain {domain_id}.'.format(id=workflow_id, domain_id=domain_id))
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, workflow_id=workflow_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to create a new workflow.')
            raise WorkflowError('Error occurred while trying to create a new workflow.')

    def get_workflow_details(self, workflow_id=None, domain_id=None):
        """
        Gets Infoworks Data workflow details for given workflow id
        :param workflow_id: id of the workflow whose details are to be fetched
        :type workflow_id: String
        :param domain_id: Domain id to which the workflow belongs to
        :type domain_id: String
        :return: response dict
        """
        response = None
        try:
            if workflow_id is None:
                self.logger.error('Please pass the mandatory workflow id as parameter.')
                raise WorkflowError('Please pass the mandatory workflow_id as parameter.')
            if domain_id is None:
                self.logger.error('Please pass the mandatory domain_id as parameter.')
                raise WorkflowError('Please pass the mandatory domain_id as parameter.')
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.create_workflow_url(
                self.client_config, domain_id) + f"/{workflow_id}", IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            result = response.get('result', {})

            if result.get('id', None) is None:
                self.logger.error('Failed to find the workflow details')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Failed to find the workflow details',
                                                     response=response)

            workflow_id = str(workflow_id)
            self.logger.info(
                'Successfully got the workflow {id} details.'.format(id=workflow_id))
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, workflow_id=workflow_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get workflow details.')
            raise WorkflowError('Error occurred while trying to get workflow details.')

    def delete_workflow(self, workflow_id=None, domain_id=None):
        """
        Deletes Infoworks Data workflow  for given workflow id
        :param workflow_id: entity id of the workflow to be deleted
        :type workflow_id: String
        :param domain_id: Domain id to which the workflow belongs to
        :type domain_id: String
        :return: response dict
        """
        response = None
        try:
            if workflow_id is None:
                self.logger.error('Please pass the mandatory workflow id as parameter.')
                raise WorkflowError('Please pass the mandatory workflow_id as parameter.')
            if domain_id is None:
                self.logger.error('Please pass the mandatory domain_id as parameter.')
                raise WorkflowError('Please pass the mandatory domain_id as parameter.')
            response = IWUtils.ejson_deserialize(self.call_api("DELETE", url_builder.create_workflow_url(
                self.client_config, domain_id) + f"/{workflow_id}", IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            result = response.get('result', {})

            if result.get('id', None) is None:
                self.logger.error(f'Failed to delete the workflow {workflow_id}')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc=f'Failed to delete the workflow {workflow_id}',
                                                     response=response)

            workflow_id = str(workflow_id)
            self.logger.info(
                'Successfully deleted the workflow {id}.'.format(id=workflow_id))
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, workflow_id=workflow_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to delete workflow.')
            raise WorkflowError('Error occurred while trying to delete workflow.')

    def update_workflow(self, workflow_id=None, domain_id=None, workflow_config=None):
        """
        Updates Infoworks Data workflow details for given workflow id
        :param workflow_id: entity id of the workflow to be updated
        :type workflow_id: String
        :param domain_id: Domain id to which the workflow belongs to
        :type domain_id: String
        :param workflow_config: a JSON object containing workflow configurations
        :type workflow_config: JSON Object

        workflow_config_example = {
         "name": "{string}",
         "description": "{string}",
         "child_workflow_ids": [
          "{array[string]...}"
         ],
         "domainId": "{string}",
         "workflow_graph": {
          "tasks": [
           {
            "task_id": "e77850ad5127a2d7dab870ff",
            "task_type": "ingest_table_group",
            "location": "-237 52",
            "title": "ingest",
            "task_properties": {},
            "run_properties": {
             "enable_exponential_backoff_for_retries": true,
             "num_retries": 5,
             "retry_delay": 200,
             "max_retry_delay": 1000,
             "trigger_rule": "{string}"
            },
            "workflow_variables": "{}"
           }
          ],
          "edges": [
           {
            "category": "{string}",
            "from_task": "{string}",
            "to_task": "{string}"
           }
          ]
         }
        }
        :return: response dict
        """
        response = None
        try:
            if workflow_config is None:
                self.logger.error('Please pass the mandatory workflow_config as parameter.')
                raise WorkflowError('Please pass valid workflow_config parameter.')
            if workflow_id is None:
                self.logger.error('Please pass the mandatory workflow id as parameter.')
                raise WorkflowError('Please pass the mandatory workflow_id as parameter.')
            if domain_id is None:
                self.logger.error('Please pass the mandatory domain_id as parameter.')
                raise WorkflowError('Please pass the mandatory domain_id as parameter.')
            if "childWorkflowIds" not in workflow_config:
                workflow_config["childWorkflowIds"] = []
            response = IWUtils.ejson_deserialize(self.call_api("PATCH", url_builder.create_workflow_url(
                self.client_config, domain_id) + f"/{workflow_id}", IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']), workflow_config).content)

            result = response.get('result', {})

            if result.get('id', None) is None:
                self.logger.error(f'Failed to update the workflow {workflow_id}')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc=f'Failed to update the workflow {workflow_id}',
                                                     response=response)

            workflow_id = str(workflow_id)
            self.logger.info(
                'Successfully updated the workflow {id}.'.format(id=workflow_id))
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, workflow_id=workflow_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to update workflow.')
            raise WorkflowError('Error occurred while trying to update workflow.')

    def trigger_workflow(self, workflow_id=None, domain_id=None):
        """
        Triggers Infoworks Data workflow for given workflow id
        :param workflow_id: entity id of the workflow to be triggered
        :type workflow_id: String
        :param domain_id: Domain id to which the workflow belongs to
        :type domain_id: String
        :return: response dict
        """
        response = None
        try:
            if workflow_id is None:
                self.logger.error('Please pass the mandatory workflow id as parameter.')
                raise WorkflowError('Please pass the mandatory workflow_id as parameter.')
            if domain_id is None:
                self.logger.error('Please pass the mandatory domain_id as parameter.')
                raise WorkflowError('Please pass the mandatory domain_id as parameter.')
            response = IWUtils.ejson_deserialize(self.call_api("POST", url_builder.trigger_workflow_url(
                self.client_config, domain_id, workflow_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            result = response.get('result', {})
            run_id = result.get('id', None)
            if result.get('id', None) is None:
                self.logger.error(f'Failed to trigger the workflow {workflow_id}')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc=f'Failed to trigger the workflow {workflow_id}',
                                                     response=response)

            workflow_id = str(workflow_id)
            self.logger.info(
                'Successfully triggered the workflow {id} with run id {run_id}.'.format(id=workflow_id, run_id=run_id))
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, workflow_id=workflow_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to trigger workflow.')
            raise WorkflowError('Error occurred while trying to trigger workflow.')

    def get_status_of_workflow(self, workflow_run_id=None, workflow_id=None):
        """
        Fetches status of Infoworks Data workflow for given workflow id and run id
        :param workflow_run_id: run id of the workflow running
        :type workflow_run_id: String
        :param workflow_id: entity id of the workflow running
        :type workflow_id: String
        :return: response dict
        """
        response = None
        try:
            if workflow_run_id is None:
                self.logger.error('Please pass the mandatory workflow_run_id as parameter.')
                raise WorkflowError('Please pass the mandatory workflow_run_id as parameter.')
            if workflow_id is None:
                self.logger.error('Please pass the mandatory workflow id as parameter.')
                raise WorkflowError('Please pass the mandatory workflow_id as parameter.')
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.get_workflow_status_url(
                self.client_config, workflow_id, workflow_run_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            result = response.get('result', {})
            run_id = result.get('id', None)
            if result.get('id', None) is None:
                self.logger.error(f'Failed to fetch status of the workflow {workflow_id}')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc=f'Failed to fetch status of the workflow {workflow_id}',
                                                     response=response)

            workflow_id = str(workflow_id)
            self.logger.info(
                'Successfully got status of the workflow {id} with run id {run_id}.'.format(id=workflow_id,
                                                                                            run_id=run_id))
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, workflow_id=workflow_id,
                                                 response=result)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to fetch status of workflow.')
            raise WorkflowError('Error occurred while trying to fetch status of workflow.')

    def poll_workflow_run_till_completion(self, workflow_run_id=None, workflow_id=None, poll_interval=30):
        """
        Polls Infoworks Data workflow for given workflow id and run id
        :param workflow_run_id: run id of the workflow running
        :type workflow_run_id: String
        :param workflow_id: entity id of the workflow running
        :type workflow_id: String
        :param poll_interval:interval in seconds between poll(default 30)
        :type poll_interval : Int
        :return: response dict
        """
        response = None
        try:
            if workflow_run_id is None:
                self.logger.error('Please pass the mandatory workflow_run_id as parameter.')
                raise WorkflowError('Please pass the mandatory workflow_run_id as parameter.')
            if workflow_id is None:
                self.logger.error('Please pass the mandatory workflow id as parameter.')
                raise WorkflowError('Please pass the mandatory workflow_id as parameter.')
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.get_workflow_status_url(
                self.client_config, workflow_id, workflow_run_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)
            result = response.get('result', {})
            workflow_status = result['workflow_status']["state"]
            while workflow_status.lower() not in ['success', 'completed', 'failed', 'aborted', 'canceled']:
                response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.get_workflow_status_url(
                    self.client_config, workflow_id, workflow_run_id), IWUtils.get_default_header_for_v3(
                    self.client_config['bearer_token'])).content)
                result = response.get('result', {})
                workflow_status = result['workflow_status']["state"]
                time.sleep(poll_interval)

            run_id = result.get('id', None)
            if result.get('id', None) is None:
                self.logger.error(f'Failed to poll status of the workflow {workflow_id}')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc=f'Failed to poll status of the workflow {workflow_id}',
                                                     response=response)

            workflow_id = str(workflow_id)
            self.logger.info(
                'Successfully polled status of the workflow {id} with run id {run_id}.'.format(id=workflow_id,
                                                                                               run_id=run_id))
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, workflow_id=workflow_id,
                                                 response=result)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to poll status of workflow.')
            raise WorkflowError('Error occurred while trying to poll status of workflow.')

    def get_workflow_configuration_json_export(self, workflow_id=None, domain_id=None):
        """
        Get exported config for workflow with workflow_id in domain with domain_id
        :param workflow_id: id of the workflow whose details is to be fetched
        :type workflow_id: String
        :param domain_id: Domain id to which the workflow belongs to
        :type domain_id: String
        :return: response dict
        """
        response = None
        try:
            if workflow_id is None:
                self.logger.error('Please pass the mandatory workflow id as parameter.')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid workflow configuration. '
                                                                'Please pass the mandatory workflow id as parameter.')
            if domain_id is None:
                self.logger.error('Please pass the mandatory domain id as parameter.')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid worlflow configuration. '
                                                                'Please pass the mandatory domain id as parameter.')
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.configure_workflow_url(
                self.client_config, domain_id, workflow_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            result = response.get('result', {})

            if result is {}:
                self.logger.error('Failed to get the configuration json of workflow')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Failed to get the configuration json of workflow',
                                                     response=response)

            workflow_id = str(workflow_id)
            self.logger.info(
                'Successfully got the workflow {id} configuration json.'.format(id=workflow_id))
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, workflow_id=workflow_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get workflow configuration json.')
            raise WorkflowError('Error occurred while trying to get workflow configuration json.')

    def update_workflow_configuration_json_import(self, workflow_id=None, domain_id=None, workflow_config=None):
        """
        Import config for workflow with workflow_id in domain with domain_id
        :param workflow_id: id of the workflow whose details are to be fetched
        :type workflow_id: String
        :param domain_id: Domain id to which the workflow belongs to
        :type domain_id: String
        :param workflow_config: configuration json of the workflow
        :type workflow_config: JSON Object
        :return: response dict
        """
        response = None
        try:
            if workflow_config is None:
                self.logger.error('Please pass the mandatory workflow_config as parameter.')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid workflow configuration. '
                                                                'Please pass the mandatory workflow_config as parameter.')
            if workflow_id is None:
                self.logger.error('Please pass the mandatory workflow id as parameter.')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid workflow configuration. '
                                                                'Please pass the mandatory workflow id as parameter.')
            if domain_id is None:
                self.logger.error('Please pass the mandatory domain id as parameter.')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid workflow configuration. '
                                                                'Please pass the mandatory domain id as parameter.')
            response = IWUtils.ejson_deserialize(self.call_api("PUT", url_builder.configure_workflow_url(
                self.client_config, domain_id, workflow_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']), workflow_config).content)

            result = response.get('result', [])

            if len(result) == 0:
                self.logger.error('Failed to update the configuration json of workflow')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Failed to update the configuration json of workflow',
                                                     response=response)

            workflow_id = str(workflow_id)
            self.logger.info(
                'Successfully updated the workflow {id} configuration json.'.format(id=workflow_id))
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, workflow_id=workflow_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to update workflow configuration json.')
            raise WorkflowError('Error occurred while trying to update workflow configuration json.')


    def get_workflow_id(self, workflow_name, domain_id=None, domain_name=None):
        """
        Function to get workflow id
        :param workflow_name: Name of the workflow
        :param domain_id: Entity identifier of the domain
        :param domain_name: Entity name of the domain
        :return: Workflow ID
        """
        if domain_id is None and domain_name is None:
            raise Exception(f"Either domain name or domain id has to be passed to get the pipeline id")
        if domain_name is not None and domain_id is None:
            # Find the domain_id
            params = {"filter": {"name": domain_name}}
            url_to_list_domains = url_builder.create_domain_url(
                self.client_config) + IWUtils.get_query_params_string_from_dict(params=params)
            try:
                response = IWUtils.ejson_deserialize(
                    self.call_api("GET", url_to_list_domains,
                                  IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
                if response is not None:
                    result = response.get("result", [])
                    domain_id = result[0]["id"]
            except Exception as e:
                self.logger.error("Error in listing domains")
                raise WorkflowError("Error in listing domains" + str(e))
        try:
            params = {"filter": {"name": workflow_name}}
            url_to_get_wf_info = url_builder.create_workflow_url(self.client_config,
                                                                       domain_id) + IWUtils.get_query_params_string_from_dict(
                params=params)
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_wf_info,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                              ).content)
            if response is not None and "result" in response:
                result = response.get("result", None)
                return result[0].get('id')
        except:
            raise WorkflowError("Unable to get pipeline NAME")

    def get_workflow_name(self, domain_id, workflow_id):
        try:
            url_to_get_wf_info = url_builder.create_workflow_url(self.client_config, domain_id)+f"/{workflow_id}"
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_wf_info,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                              ).content)
            if response is not None and "result" in response:
                result = response.get("result", None)
                return result.get('name')
        except:
            raise WorkflowError("Unable to get workflow name")