import time

from infoworks.error import WorkflowError
from infoworks.sdk import url_builder
from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.local_configurations import Response, ErrorCode
from infoworks.sdk.utils import IWUtils
from infoworks.sdk.workflow_response import WorkflowResponse


class WorkflowClient(BaseClient):
    def __init__(self):
        super(WorkflowClient, self).__init__()

    def get_list_of_workflows(self, domain_id=None, params=None):
        """
        Gets List of Infoworks Data workflow details for given domain id
        :param domain_id: Domain id to which the workflows belongs to, if None all workflows in all domains will be fetched
        :type domain_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        response = None
        initial_msg = ""
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
                if response is not None:
                    initial_msg = response.get("message", "")
                    result = response.get("result", {}).get("items", [])
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
                        result = response.get("result", {}).get("items", [])
                response["result"] = workflows_list
                response["message"] = initial_msg
                return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                workflows_list = []
                response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.create_workflow_url(
                    self.client_config, domain_id)+ IWUtils.get_query_params_string_from_dict(params=params), IWUtils.get_default_header_for_v3(
                    self.client_config['bearer_token'])).content)

                if response is not None:
                    initial_msg = response.get("message", "")
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
                response["result"] = workflows_list
                response["message"] = initial_msg
                return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, response=response)

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
        ```
        workflow_config_example = {
        "name": "workflow_name"
        }
        ```
        :return: response dict
        """
        if None in {domain_id} and workflow_config is None:
            self.logger.error("domain id or workflow_config cannot be None")
            raise Exception("domain_id id or workflow_config cannot be None")
        response = None
        try:
            response = IWUtils.ejson_deserialize(self.call_api("POST", url_builder.create_workflow_url(
                self.client_config, domain_id), IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                                               workflow_config).content)

            result = response.get('result', {})
            workflow_id = result.get('id', None)

            if workflow_id is None:
                self.logger.error('Workflow failed to create.')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
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
        if None in {domain_id, workflow_id}:
            self.logger.error("domain id or workflow_id cannot be None")
            raise Exception("domain_id id or workflow_id cannot be None")
        response = None
        try:
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.create_workflow_url(
                self.client_config, domain_id) + f"/{workflow_id}", IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            result = response.get('result', {})

            if result.get('id', None) is None:
                self.logger.error('Failed to find the workflow details')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
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
        if None in {domain_id, workflow_id}:
            self.logger.error("domain id or workflow_id cannot be None")
            raise Exception("domain_id id or workflow_id cannot be None")
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
                                                     error_code=ErrorCode.USER_ERROR,
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
        ```
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
        ```
        :return: response dict
        """
        if None in {domain_id, workflow_id} and workflow_config is None:
            self.logger.error("domain id or workflow_id or workflow_config cannot be None")
            raise Exception("domain_id or workflow_id or workflow_config cannot be None")
        response = None
        try:
            if "childWorkflowIds" not in workflow_config:
                workflow_config["child_workflow_ids"] = []
            response = IWUtils.ejson_deserialize(self.call_api("PATCH", url_builder.create_workflow_url(
                self.client_config, domain_id) + f"/{workflow_id}", IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']), workflow_config).content)

            result = response.get('result', {})

            if result.get('id', None) is None:
                self.logger.error(f'Failed to update the workflow {workflow_id}')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
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

    def trigger_workflow(self, workflow_id=None, domain_id=None, trigger_wf_body=None):
        """
        Triggers Infoworks Data workflow for given workflow id
        :param workflow_id: entity id of the workflow to be triggered
        :type workflow_id: String
        :param domain_id: Domain id to which the workflow belongs to
        :type domain_id: String
        :param trigger_wf_body: Pass the workflow parameters if any
        ```
        trigger_wf_body = {
            "workflow_parameters": [
                {
                    "key": "name",
                    "value": "WF_API_TRIGGER"
                }
            ]
        }
        ```
        :return: response dict
        """
        if None in {domain_id, workflow_id}:
            self.logger.error("domain id or workflow_id cannot be None")
            raise Exception("domain_id or workflow_id cannot be None")
        response = None
        try:
            if trigger_wf_body:
                response = IWUtils.ejson_deserialize(self.call_api("POST", url_builder.trigger_workflow_url(
                    self.client_config, domain_id, workflow_id), IWUtils.get_default_header_for_v3(
                    self.client_config['bearer_token']), data=trigger_wf_body).content)
            else:
                response = IWUtils.ejson_deserialize(self.call_api("POST", url_builder.trigger_workflow_url(
                    self.client_config, domain_id, workflow_id), IWUtils.get_default_header_for_v3(
                    self.client_config['bearer_token'])).content)
            result = response.get('result', {})
            run_id = result.get('id', None)
            if result.get('id', None) is None:
                self.logger.error(f'Failed to trigger the workflow {workflow_id}')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
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

    def restart_or_cancel_multiple_workflows(self, action_type="restart", workflow_list_body=None):
        """
        Restart/Cancel Infoworks Data workflow for given workflow id
        :param action_type: restart/cancel
        :param workflow_list_body: JSON object containing array of ids(workflow_id,run_id) to restart/cancel
        :type workflow_list_body: JSON Dict
        ```
        example: {
                  "ids": [
                    {
                      "workflow_id": "e77850ad5127a2d7dab870ff",
                      "run_id": "e77850ad5127a2d7dab870ff"
                    }
                  ]
                }
        ```
        :return: response dict
        """
        if workflow_list_body is None:
            self.logger.error("workflow_list_body cannot be None")
            raise Exception("workflow_list_body cannot be None")
        if action_type.lower() not in ["restart", "cancel"]:
            self.logger.error("action_type should be restart or cancel")
            raise Exception("action_type should be restart or cancel")
        response = None
        try:
            if action_type.lower() == "restart":
                request_url = url_builder.restart_multiple_workflows_url(self.client_config)
            else:
                request_url = url_builder.cancel_multiple_workflows_url(self.client_config)
            response = IWUtils.ejson_deserialize(self.call_api("POST", request_url, IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']), data=workflow_list_body).content)

            result = response.get('result', None)
            if result is None:
                self.logger.error(f'Failed to {action_type} the workflows')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc=f'Failed to {action_type} the workflows',
                                                     response=response)

            self.logger.info(
                f'Successfully {action_type}d the workflows.')
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception(f'Error occurred while {action_type}ing the workflows.' + str(e))
            raise WorkflowError(f'Error occurred while {action_type}ing the workflows.' + str(e))

    def cancel_multiple_workflow(self, workflow_list_body=None):
        """
        Cancels Infoworks Data workflow for given workflow id
        :param workflow_list_body: JSON object containing array of ids(workflow_id,run_id) to restart
        :type workflow_list_body: JSON dict
        ```
        example: {
                  "ids": [
                    {
                      "workflow_id": "e77850ad5127a2d7dab870ff",
                      "run_id": "e77850ad5127a2d7dab870ff"
                    }
                  ]
                }
        ```
        :return: response dict
        """
        response = None
        if workflow_list_body is None:
            self.logger.error("workflow_list_body cannot be None")
            raise Exception("workflow_list_body cannot be None")
        try:
            response = IWUtils.ejson_deserialize(self.call_api("POST", url_builder.cancel_multiple_workflows_url(
                self.client_config), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']), data=workflow_list_body).content)

            result_msg = response.get("message", None)
            if result_msg is None and result_msg == "Submitted Cancel Job for Workflow Run":
                self.logger.error(f'Failed to cancel the workflows')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc=f'Failed to cancel the workflows',
                                                     response=response)

            self.logger.info(
                'Successfully Submitted Cancel Job for Workflow Run.')
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while cancelled the workflows.' + str(e))
            raise WorkflowError('Error occurred while cancelled the workflows.' + str(e))

    def get_status_of_workflow(self, workflow_run_id=None, workflow_id=None):
        """
        Fetches status of Infoworks Data workflow for given workflow id and run id
        :param workflow_run_id: run id of the workflow running
        :type workflow_run_id: String
        :param workflow_id: entity id of the workflow running
        :type workflow_id: String
        :return: response dict
        """
        if None in {workflow_id, workflow_run_id}:
            self.logger.error("workflow_id or workflow_run_id cannot be None")
            raise Exception("workflow_id or workflow_run_id cannot be None")
        response = None
        try:
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.get_workflow_status_url(
                self.client_config, workflow_id, workflow_run_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            result = response.get('result', None)
            if result is None:
                self.logger.error('Failed to get the status of workflow')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc='Failed to get the configuration json of workflow',
                                                     response=response)
            run_id = result.get('id', None)
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
        if None in {workflow_id, workflow_run_id}:
            self.logger.error("workflow_id or workflow_run_id cannot be None")
            raise Exception("workflow_id or workflow_run_id cannot be None")
        try:
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.get_workflow_status_url(
                self.client_config, workflow_id, workflow_run_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)
            result = response.get('result', {})
            if result:
                workflow_status = result['workflow_status']["state"]
                while workflow_status.lower() not in ['success', 'completed', 'failed', 'aborted', 'canceled']:
                    response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.get_workflow_status_url(
                        self.client_config, workflow_id, workflow_run_id), IWUtils.get_default_header_for_v3(
                        self.client_config['bearer_token'])).content)
                    result = response.get('result', {})
                    workflow_status = result['workflow_status']["state"]
                    if workflow_status.lower() in ['success', 'completed', 'failed', 'aborted', 'canceled']:
                        break
                    print(f"workflow_status : {workflow_status}.Sleeping for {poll_interval} seconds")
                    time.sleep(poll_interval)

                run_id = result.get('id', None)
                if result.get('id', None) is None:
                    self.logger.error(f'Failed to poll status of the workflow {workflow_id}')
                    return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                         error_code=ErrorCode.USER_ERROR,
                                                         error_desc=f'Failed to poll status of the workflow {workflow_id}',
                                                         response=response)

                workflow_id = str(workflow_id)
                self.logger.info(
                    'Successfully polled status of the workflow {id} with run id {run_id}.'.format(id=workflow_id,
                                                                                                   run_id=run_id))
                return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, workflow_id=workflow_id,
                                                     response=response)
            else:
                print(response)
                raise Exception("Returned API Result is None")

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
        if None in {workflow_id, domain_id}:
            self.logger.error("workflow_id or domain_id cannot be None")
            raise Exception("workflow_id or domain_id cannot be None")
        try:
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.configure_workflow_url(
                self.client_config, domain_id, workflow_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            result = response.get('result', None)

            if result is None:
                self.logger.error('Failed to get the configuration json of workflow')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
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
        if None in {workflow_id, domain_id} and workflow_config is None:
            self.logger.error("workflow_id or domain_id or workflow_config cannot be None")
            raise Exception("workflow_id or domain_id or workflow_config cannot be None")
        response = None
        try:
            response = IWUtils.ejson_deserialize(self.call_api("PUT", url_builder.configure_workflow_url(
                self.client_config, domain_id, workflow_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']), workflow_config).content)

            result = response.get('result', None)

            if result is None:
                self.logger.error('Failed to update the configuration json of workflow')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
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

    def get_workflow_id(self, workflow_name=None, domain_id=None, domain_name=None):
        """
        Function to get workflow id
        :param workflow_name: Name of the workflow
        :param domain_id: Entity identifier of the domain
        :param domain_name: Entity name of the domain
        :return: Workflow ID
        ```
        example response : {"id": "884236e85b9b1a69b2907e4c"}
        ```
        """
        if None in {workflow_name}:
            self.logger.error("workflow_name cannot be None")
            raise Exception("workflow_name cannot be None")
        if domain_id is None and domain_name is None:
            raise Exception(f"Either domain name or domain id has to be passed")
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
                if result is not None:
                    return WorkflowResponse.parse_result(status=Response.Status.SUCCESS,
                                                         response={"id": result[0].get('id')})
        except:
            raise WorkflowError("Unable to get workflow id from name")

    def get_workflow_name(self, domain_id=None, workflow_id=None):
        """
        Function to get workflow name from ID
        :param workflow_id: Id of the workflow
        :type workflow_id: String
        :param domain_id: Entity identifier of the domain
        :type domain_id: String
        :return: Workflow name
        ```
        example response : {"name": "sample_workflow"}
        ```
        """
        if None in {workflow_id, domain_id}:
            self.logger.error("workflow_id or domain_id cannot be None")
            raise Exception("workflow_id or domain_id cannot be None")
        try:
            url_to_get_wf_info = url_builder.create_workflow_url(self.client_config, domain_id) + f"/{workflow_id}"
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_wf_info,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                              ).content)
            if response is not None and "result" in response:
                result = response.get("result", None)
                if result is not None:
                    return WorkflowResponse.parse_result(status=Response.Status.SUCCESS,
                                                         response={"name": result.get("name")})
        except:
            raise WorkflowError("Unable to get workflow name")

    def get_list_of_workflow_runs(self, domain_id=None, workflow_id=None, params=None, api_body_for_filter={}):
        """
        Gets List of Infoworks Data workflow runs details for given domain id and workflow id
        :param domain_id: Domain id to which the workflows belongs to, if None all workflows in all domains will be fetched
        :type domain_id: String
        :param workflow_id: Workflow id,if None all workflow runs in all domains will be fetched
        :type workflow_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :param api_body_for_filter: dict of API body
        :type api_body_for_filter: JSON dict
        ```
        example:
        {'limit': 10000, 'date_range': {'type': 'last', 'unit': 'day', 'value': 1}, 'offset': 0}
        ```
        :return: response List
        """
        response = None
        initial_msg = ""
        try:
            if domain_id is None:
                url_to_list_workflow_runs = url_builder.get_all_workflows_runs_url(
                    self.client_config) + IWUtils.get_query_params_string_from_dict(params=params)
                if not api_body_for_filter.get("limit", None):
                    api_body_for_filter["limit"] = 10000

                if not api_body_for_filter.get("date_range", None):
                    api_body_for_filter["date_range"] = {"type": "last", "unit": "day", "value": 1}
                api_body_for_filter["offset"] = 0
                workflow_runs_list = []
                response = IWUtils.ejson_deserialize(
                    self.call_api("POST", url_to_list_workflow_runs,
                                  IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                  data=api_body_for_filter).content)
                if response is not None:
                    initial_msg = response.get("message", "")
                    result = response.get("result", [])
                    if len(result) == 0:
                        self.logger.error('Failed to update the configuration json of workflow')
                        return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                             error_code=ErrorCode.USER_ERROR,
                                                             error_desc='Failed to get the workflow run id jobs',
                                                             response=response)
                    while len(result) > 0:
                        workflow_runs_list.extend(result)
                        api_body_for_filter["offset"] = api_body_for_filter.get("limit")
                        response = IWUtils.ejson_deserialize(
                            self.call_api("POST", url_to_list_workflow_runs, IWUtils.get_default_header_for_v3(
                                self.client_config['bearer_token']), data=api_body_for_filter).content)
                        result = response.get("result", [])
                response["result"] = workflow_runs_list
                response["message"] = initial_msg
                return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                workflow_runs_list = []
                response = IWUtils.ejson_deserialize(
                    self.call_api("GET", url_builder.get_all_workflows_runs_url_with_domain_id(
                        self.client_config, domain_id, workflow_id), IWUtils.get_default_header_for_v3(
                        self.client_config['bearer_token'])).content)

                result = response.get('result', None)
                if result is None:
                    self.logger.error('Failed to get the list of workflow runs')
                    return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                         error_code=ErrorCode.USER_ERROR,
                                                         error_desc='Failed to get the workflow run id jobs',
                                                         response=response)
                if response is not None:
                    result = response.get("result", [])
                    initial_msg = response.get("message", "")
                    while len(result) > 0:
                        workflow_runs_list.extend(result)
                        nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                        response = IWUtils.ejson_deserialize(
                            self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                                self.client_config['bearer_token'])).content)
                        result = response.get("result", [])
                    response["result"] = workflow_runs_list
                    response["message"] = initial_msg
                return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get workflow details.')
            raise WorkflowError('Error occurred while trying to get workflow details.')

    def get_list_of_workflow_runs_jobs(self, run_id=None, params=None):
        """
         Gets List of Infoworks Data workflow runs jobs details
        :param run_id: Run id to which the workflows belongs to, if None all workflows
        :type run_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        if None in {run_id}:
            self.logger.error("run_id cannot be None")
            raise Exception("run_id cannot be None")
        response = None
        try:
            workflow_run_jobs_list = []
            get_all_workflow_run_jobs_url = url_builder.get_all_workflow_run_jobs_url(self.client_config, run_id)
            if params is None:
                get_all_workflow_run_jobs_url = get_all_workflow_run_jobs_url + "?fetch_summary=true&recursive_job_search=true"
            else:
                get_all_workflow_run_jobs_url = get_all_workflow_run_jobs_url + \
                                                IWUtils.get_query_params_string_from_dict(params=params) + \
                                                "&fetch_summary=true&recursive_job_search=true"
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", get_all_workflow_run_jobs_url, IWUtils.get_default_header_for_v3(
                    self.client_config['bearer_token'])).content)

            result = response.get('result', None)
            initial_msg = ""
            if result is None:
                self.logger.error('Failed to get list of workflow runs jobs')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc='Failed to get the workflow run id jobs',
                                                     response=response)

            if response is not None:
                result = response.get("result", [])
                initial_msg = response.get("message", "")
                while len(result) > 0:
                    workflow_run_jobs_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", [])
            response["result"] = workflow_run_jobs_list
            response["message"] = initial_msg
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get jobs under workflow run.' + str(e))
            raise WorkflowError('Error occurred while trying to get jobs under workflow run.' + str(e))

    def get_list_of_domain_workflow_schedules(self, domain_id, params=None):
        """
        Gets List of Schedules of all Workflows belonging to the Domain
        :param domain_id: Domain Id to list the schedules configured
        :type domain_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type params: JSON Dict
        :return: Response Dict
        """
        if domain_id is None:
            self.logger.error("domain_id cannot be None")
            raise Exception("domain_id cannot be None")

        if params is None:
            params = {"limit": 20, "offset": 0}

        domain_schedules = []
        response = None
        initial_msg = ''
        try:
            domain_schedules_url = url_builder.get_domain_workflow_schedules_url(self.client_config, domain_id) + \
                                   IWUtils.get_query_params_string_from_dict(params=params)

            response = IWUtils.ejson_deserialize(self.call_api("GET", domain_schedules_url,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token'])).content)
            if response:
                initial_msg = response.get("message", '')
                result = response.get("result", [])

                while len(result) > 0:
                    domain_schedules.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", [])
            response["result"] = domain_schedules
            response["message"] = initial_msg
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get domain schedules .')
            raise WorkflowError('Error occurred while trying to get domain schedules.')

    def get_workflow_schedule(self, domain_id, workflow_id):
        """
        Gets Schedules of particular Workflow belonging to the Domain
        :param domain_id: Domain ID of which Workflow belongs
        :type domain_id: String
        :param workflow_id: Workflow ID to fetch schedule for.
        :type workflow_id: String
        :return: Response Dict
        """
        if domain_id is None or workflow_id is None:
            self.logger.error("domain_id or workflow_id cannot be None")
            raise Exception("domain_id or workflow_id cannot be None")

        response = None

        try:
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.get_workflow_schedule_url(
                self.client_config, domain_id, workflow_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, workflow_id=workflow_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get workflow schedule.')
            raise WorkflowError('Error occurred while trying to get workflow schedule.')

    def enable_workflow_schedule(self, domain_id, workflow_id, schedule_config):
        """
        Enables Schedule of a particular Workflow belonging to the Domain
        :param domain_id: Domain ID of which Workflow belongs
        :type domain_id: String
        :param workflow_id: Workflow ID to fetch schedule for.
        :type workflow_id: String
        :param schedule_config: Schedule Configuration JSON of the Workflow
        :type schedule_config: JSON
        ```
        schedule_config_example = {
              "start_date": "02/22/2020",
              "end_date": "02/24/2020",
              "start_hour": 12,
              "start_min": 25,
              "end_hour": 17,
              "end_min": 30,
              "repeat_interval_measure": 2,
              "repeat_interval_unit": "{string}",
              "ends": true,
              "is_custom_job": true,
              "custom_job_details": {
                "starts_daily_at": "14:00",
                "ends_daily_at": "15:00",
                "repeat_interval_unit": "{string}",
                "repeat_interval_measure": 2
              },
              "repeat_on_last_day": "{boolean}",
              "specified_days": 1
        }
        ```
        :return: Response Dict
        """
        if domain_id is None or workflow_id is None or schedule_config is None:
            self.logger.error("domain_id or workflow_id or schedule_config cannot be None")
            raise Exception("domain_id or workflow_id or schedule_config cannot be None")
        response = None
        try:
            response = IWUtils.ejson_deserialize(self.call_api("PUT", url_builder.get_enable_workflow_schedule_url(
                self.client_config, domain_id, workflow_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']), schedule_config).content)

            result = response.get('result', None)

            if result is None:
                self.logger.error('Failed to enable schedule of the workflow')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc='Failed to enable schedule of the workflow',
                                                     response=response)

            workflow_id = str(workflow_id)
            self.logger.info(
                'Successfully enabled schedule for the workflow {id}.'.format(id=workflow_id))
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, workflow_id=workflow_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to enable workflow schedule.')
            raise WorkflowError('Error occurred while trying to enable workflow schedule.')

    def disable_workflow_schedule(self, domain_id, workflow_id):
        """
        Disables Schedule of a particular Workflow belonging to the Domain
        :param domain_id: Domain ID of which Workflow belongs
        :type domain_id: String
        :param workflow_id: Workflow ID to fetch schedule for.
        :type workflow_id: String
        :return: Response Dict
        """
        if domain_id is None or workflow_id is None:
            self.logger.error("domain_id or workflow_id cannot be None")
            raise Exception("domain_id or workflow_id cannot be None")
        response = None
        try:
            response = IWUtils.ejson_deserialize(self.call_api("PUT", url_builder.get_disable_workflow_schedule_url(
                self.client_config, domain_id, workflow_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            result = response.get('result', None)

            if result is None:
                self.logger.error('Failed to disable schedule of the workflow')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc='Failed to disable schedule of the workflow',
                                                     response=response)

            workflow_id = str(workflow_id)
            self.logger.info(
                'Successfully disabled schedule for the workflow {id}.'.format(id=workflow_id))
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, workflow_id=workflow_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to disable workflow schedule.')
            raise WorkflowError('Error occurred while trying to disable workflow schedule.')

    def update_workflow_schedule_user(self, domain_id, workflow_id):
        """
        Changes Workflow Schedule User of a particular Workflow belonging to the Domain
        :param domain_id: Domain ID of which Workflow belongs
        :type domain_id: String
        :param workflow_id: Workflow ID to fetch schedule for.
        :type workflow_id: String
        :return: Response Dict
        """
        if domain_id is None or workflow_id is None:
            self.logger.error("domain_id or workflow_id cannot be None")
            raise Exception("domain_id or workflow_id cannot be None")
        response = None
        try:
            response = IWUtils.ejson_deserialize(self.call_api("PUT", url_builder.update_workflow_schedule_user_url(
                self.client_config, domain_id, workflow_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            result = response.get('result', None)

            if result is None:
                self.logger.error('Failed to update workflow schedule user')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc='Failed to update workflow schedule user',
                                                     response=response)

            workflow_id = str(workflow_id)
            self.logger.info(
                'Successfully updated workflow {id} schedule user.'.format(id=workflow_id))
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, workflow_id=workflow_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to update workflow schedule user.')
            raise WorkflowError('Error occurred while trying to update workflow schedule user.')



    def pause_or_resume_multiple_workflows(self, action_type="pause", workflow_list_body=None):
        """
        Pause/resume Infoworks Data workflow for given workflow ids
        :param action_type: pause/resume
        :param workflow_list_body: JSON object containing array of workflow ids
        :type workflow_list_body: JSON Dict
        workflow_list_body_example: {
        "workflow_ids": ["c265e25b886a1b5e09896885"]
        }
        :return: response dict
        """
        if workflow_list_body is None:
            self.logger.error("workflow_list_body cannot be None")
            raise Exception("workflow_list_body cannot be None")
        if action_type.lower() not in ["pause", "resume"]:
            self.logger.error(f"action_type cannot be {action_type}. Supported resume/pause")
            raise Exception(f"action_type cannot be {action_type}. Supported resume/pause")
        response = None
        try:
            if action_type.lower() == "pause":
                request_url = url_builder.pause_multiple_workflows_url(self.client_config)
            else:
                request_url = url_builder.resume_multiple_workflows_url(self.client_config)
            response = IWUtils.ejson_deserialize(self.call_api("POST", request_url, IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']), data=workflow_list_body).content)
            result = response.get('result', None)
            if result is None:
                self.logger.error(f'Failed to {action_type} the workflows')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc=f'Failed to {action_type} the workflows',
                                                     response=response)

            self.logger.info(
                f'Successfully {action_type}d the workflows.')
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception(f'Error occurred while {action_type}d the workflows.' + str(e))
            raise WorkflowError(f'Error occurred while {action_type}d the workflows.' + str(e))

    def get_global_list_of_workflow_runs(self, params=None):
        """
        Gets List of all the Infoworks Data workflows. Need admin access
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        response = None
        initial_msg = ""
        try:
            if params is None:
                params = {"limit": 20, "offset": 0}
            url_to_list_workflow_runs = url_builder.get_workflow_runs_url(
                self.client_config) + IWUtils.get_query_params_string_from_dict(params=params)
            workflow_run_list = []
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_workflow_runs,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                initial_msg = response.get("message", "")
                result = response.get("result", {}).get("items", [])
                while len(result) > 0:
                    workflow_run_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", {}).get("items", [])
            response["result"] = workflow_run_list
            response["message"] = initial_msg
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get workflow run details.')
            raise WorkflowError('Error occurred while trying to get workflow run details.')

    def get_list_of_workflow_tasks(self, params=None):
        """
        Gets List of all the Infoworks Data workflow tasks
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response dict
        """
        response = None
        initial_msg = ""
        try:
            if params is None:
                params = {"limit": 20, "offset": 0}
            url_to_list_workflow_run_tasks = url_builder.get_workflow_tasks_url(
                self.client_config) + IWUtils.get_query_params_string_from_dict(params=params)
            workflow_run_tasks_list = []
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_workflow_run_tasks,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                initial_msg = response.get("message", "")
                result = response.get("result", {}).get("items", [])
                while len(result) > 0:
                    workflow_run_tasks_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", {}).get("items", [])
            response["result"] = workflow_run_tasks_list
            response["message"] = initial_msg
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get workflow run task details.')
            raise WorkflowError('Error occurred while trying to get workflow run task details.')

    def get_workflow_run_details(self, workflow_id=None, run_id=None):
        """
        Gets Infoworks Data workflow run details for given workflow id
        :param workflow_id: id of the workflow whose details are to be fetched
        :type workflow_id: String
        :param run_id: Workflow run id
        :type run_id: String
        :return: response dict
        """
        if None in {run_id, workflow_id}:
            self.logger.error("run_id id or workflow_id cannot be None")
            raise Exception("run_id id or workflow_id cannot be None")
        response = None
        try:
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.get_workflow_run_id_details_url(self.client_config, workflow_id, run_id)
                                                               , IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            result = response.get('result', {})
            if result.get('id', None) is None:
                self.logger.error('Failed to find the workflow run details')
                return WorkflowResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc='Failed to find the workflow run details',
                                                     response=response)

            workflow_id = str(workflow_id)
            self.logger.info(
                'Successfully got the workflow run details.')
            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, workflow_id=workflow_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get workflow run details.')
            raise WorkflowError('Error occurred while trying to get workflow run details.')

    def get_workflow_lineage(self, domain_id, workflow_id):
        """
        Gets workflow lineage
        :param domain_id: Domain ID of which Workflow belongs
        :type domain_id: String
        :param workflow_id: Entity id of the Workflow
        :type workflow_id: String
        :return: Response Dict
        """
        if domain_id is None or workflow_id is None:
            self.logger.error("domain_id or workflow_id cannot be None")
            raise Exception("domain_id or workflow_id cannot be None")

        response = None

        try:
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.workflow_lineage_url(
                self.client_config, domain_id, workflow_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            return WorkflowResponse.parse_result(status=Response.Status.SUCCESS, workflow_id=workflow_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get workflow lineage.')
            raise WorkflowError('Error occurred while trying to get workflow lineage.')