import time

from infoworks.error import PipelineError
from infoworks.sdk import url_builder, local_configurations
from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.generic_response import GenericResponse
from infoworks.sdk.local_configurations import Response, ErrorCode
from infoworks.sdk.utils import IWUtils


class PipelineGroupClient(BaseClient):
    def __init__(self):
        super(PipelineGroupClient, self).__init__()

    def poll_pipeline_group_job(self, pipeline_group_id=None, job_id=None,
                                poll_timeout=local_configurations.POLLING_TIMEOUT,
                                polling_frequency=local_configurations.POLLING_FREQUENCY_IN_SEC,
                                retries=local_configurations.NUM_POLLING_RETRIES):
        """
        Function to poll the pipeline group job
        :param pipeline_group_id: ID of the pipeline group
        :type pipeline_group_id: String
        :param job_id: job_id of the pipeline group job
        :type job_id: String
        :param poll_timeout: Polling timeout(default is 1200 seconds).If -1 then till the job completes polling will be done.
        :type poll_timeout: Integer
        :param polling_frequency: Frequency of the polling(default is 15seconds)
        :type polling_frequency: Integer
        :param retries: Number of retries in case of failure(default 3)
        :type retries: Integer
        :return: response dict
        """
        failed_count = 0
        response = {}
        if poll_timeout != -1:
            timeout = time.time() + poll_timeout
        else:
            # 2,592,000 is 30 days assuming it to be max time a job can run
            timeout = time.time() + 2592000
        while True:
            if time.time() > timeout:
                break
            try:
                self.logger.info(f"Failed poll job status count: {failed_count}")
                job_monitor_url = url_builder.get_job_status_url(self.client_config, job_id)
                response = IWUtils.ejson_deserialize(self.call_api("GET", job_monitor_url,
                                                                   IWUtils.get_default_header_for_v3(
                                                                       self.client_config['bearer_token'])).content)
                result = response.get('result', {})
                if len(result) != 0:
                    job_status = result["status"]
                    print(f"pipeline_group_status : {job_status}.Sleeping for {polling_frequency} seconds")
                    self.logger.info(
                        "Job poll status : " + result["status"] + "Job completion percentage: " + str(result.get(
                            "percentage", 0)))
                    if job_status.lower() in ["completed", "failed", "aborted", "canceled"]:
                        return GenericResponse.parse_result(status=Response.Status.SUCCESS, entity_id=pipeline_group_id,
                                                            job_id=job_id, response=response)
                else:
                    self.logger.error(f"Error occurred during job {job_id} status poll")
                    if failed_count >= retries - 1:
                        return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                            error_code=ErrorCode.GENERIC_ERROR,
                                                            error_desc=f"Error occurred during job {job_id} status poll",
                                                            response=response, job_id=job_id,
                                                            entity_id=pipeline_group_id)
                    failed_count = failed_count + 1
            except Exception as e:
                self.logger.exception("Error occurred during job status poll")
                self.logger.info(str(e))
                if failed_count >= retries - 1:
                    # traceback.print_stack()
                    print(response)
                    raise PipelineError(response.get("message", "Error occurred during job status poll"))
                failed_count = failed_count + 1
            time.sleep(polling_frequency)

    def list_jobs_under_pipeline_group(self, domain_id, pipeline_group_id, params=None):
        """
            Function to list the jobs under pipeline group
            :param domain_id: Entity identifier for domain
            :type domain_id: String
            :param pipeline_group_id: Entity identifier of the pipeline group
            :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
            :type: JSON dict
            :return: response dict
        """
        if None in {domain_id, pipeline_group_id}:
            self.logger.error("domain_id or pipeline_group_id cannot be None")
            raise Exception("domain_id or pipeline_group_id cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_pipeline_grp_jobs = url_builder.get_pipeline_group_jobs_base_url(self.client_config, domain_id,
                                                                                     pipeline_group_id) + IWUtils.get_query_params_string_from_dict(
            params=params)
        pipeline_groups_jobs_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_pipeline_grp_jobs,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                initial_msg = response.get("message", "")
                while len(result) > 0:
                    pipeline_groups_jobs_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", None)
                    if result is None:
                        return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                            error_code=ErrorCode.GENERIC_ERROR,
                                                            error_desc="Error in listing pipeline group jobs",
                                                            response=response
                                                            )

                response["result"] = pipeline_groups_jobs_list
                response["message"] = initial_msg
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing pipeline group jobs")
            raise PipelineError("Error in listing pipeline group jobs" + str(e))

    def trigger_pipeline_group_build(self, domain_id, pipeline_group_id, poll=True):
        """
            Function to trigger the pipeline group build
            :param domain_id: Entity identifier for domain
            :type domain_id: String
            :param pipeline_group_id: Entity identifier of the pipeline group
            :param poll: Poll job until its completion
            :type poll: Boolean
            :return: response dict
        """
        if None in {domain_id, pipeline_group_id}:
            self.logger.error("domain_id or pipeline_group_id cannot be None")
            raise Exception("domain_id or pipeline_group_id cannot be None")
        url_for_pipeline_grp_build = url_builder.get_pipeline_group_jobs_base_url(self.client_config, domain_id,
                                                                                  pipeline_group_id)
        request_body = {
            "job_type": "pipeline_group_build"
        }
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", url_for_pipeline_grp_build,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              request_body).content)
            result = response.get('result', {})
        except Exception as e:
            self.logger.error(e)
            raise PipelineError(f"Failed to submit pipeline group build job for {pipeline_group_id} " + str(e))
        if len(result) != 0 and "id" in result:
            job_id = result["id"]
            if not poll:
                self.logger.info(f"Pipeline group build job has been submitted for {pipeline_group_id} with {job_id}")
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, job_id=job_id,
                                                    entity_id=pipeline_group_id, response=response)
            else:
                return self.poll_pipeline_group_job(pipeline_group_id=pipeline_group_id, job_id=job_id)
        else:
            self.logger.error(response)
            raise PipelineError(f"Failed to submit pipeline group build job for {pipeline_group_id} ")

    def get_pipeline_group_job_details(self, domain_id, pipeline_group_id, job_id):
        """
        Gets Infoworks pipeline group job details for given pipeline group id and job id
        :param pipeline_group_id: id of the pipeline group whose details are to be fetched
        :type pipeline_group_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :param job_id: id of the pipeline group job
        :return: response dict
        """
        try:
            if None in {pipeline_group_id, domain_id, job_id}:
                raise Exception(f"pipeline_group_id, domain_id, job_id cannot be None")
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.get_pipeline_group_jobs_base_url(
                self.client_config, domain_id, pipeline_group_id) + f"/{job_id}", IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)
            result = response.get('result', {})
            if result.get('id', None) is None:
                self.logger.error('Failed to find the pipeline group job details')
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    error_desc='Failed to find the pipeline group job details',
                                                    response=response)

            job_id = str(job_id)
            self.logger.info(
                'Successfully got the pipeline group job {id} details.'.format(id=job_id))
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, entity_id=job_id,
                                                response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to get pipeline group details.')
            raise PipelineError('Error occurred while trying to get pipeline group details.')

    def list_pipeline_job_details_in_pipeline_group_job(self, domain_id, pipeline_group_id, job_id, params=None):
        """
            Function to list the pipeline jobs in a pipeline group job
            :param domain_id: Entity identifier for domain
            :type domain_id: String
            :param pipeline_group_id: Entity identifier of the pipeline group
            :param job_id: Entity identifier of the pipeline group job
            :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
            :type: JSON dict
            :return: response dict
        """
        if None in {pipeline_group_id, domain_id, job_id}:
            raise Exception(f"pipeline_group_id, domain_id, job_id cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_pipeline_grp_jobs = url_builder.get_pipeline_group_jobs_base_url(self.client_config, domain_id,
                                                                                     pipeline_group_id) + f"/{job_id}/pipeline-jobs" + IWUtils.get_query_params_string_from_dict(
            params=params)
        pipelines_in_pipeline_groups_jobs_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_pipeline_grp_jobs,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                initial_msg = response.get("message", "")
                while len(result) > 0:
                    pipelines_in_pipeline_groups_jobs_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", None)
                    if result is None:
                        return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                            error_code=ErrorCode.GENERIC_ERROR,
                                                            error_desc="Error in listing pipeline jobs in pipeline group job",
                                                            response=response
                                                            )

                response["result"] = pipelines_in_pipeline_groups_jobs_list
                response["message"] = initial_msg
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing pipeline jobs in pipeline group job")
            raise PipelineError("Error in listing pipeline jobs in pipeline group job" + str(e))

    def list_cluster_job_runs_in_pipeline_group_job(self, domain_id, pipeline_group_id, job_id, params=None):
        """
            Function to list the cluster jobs in a pipeline group job
            :param domain_id: Entity identifier for domain
            :type domain_id: String
            :param pipeline_group_id: Entity identifier of the pipeline group
            :param job_id: Entity identifier of the pipeline group job
            :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
            :type: JSON dict
            :return: response dict
        """
        if None in {pipeline_group_id, domain_id, job_id}:
            raise Exception(f"pipeline_group_id, domain_id, job_id cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_pipeline_grp_job_runs = url_builder.get_pipeline_group_jobs_base_url(self.client_config, domain_id,
                                                                                         pipeline_group_id) + f"/{job_id}/runs" + IWUtils.get_query_params_string_from_dict(
            params=params)
        cluster_jobs_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_pipeline_grp_job_runs,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                initial_msg = response.get("message", "")
                while len(result) > 0:
                    cluster_jobs_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", None)
                    if result is None:
                        return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                            error_code=ErrorCode.GENERIC_ERROR,
                                                            error_desc="Error in listing cluster jobs in pipeline group job",
                                                            response=response
                                                            )

                response["result"] = cluster_jobs_list
                response["message"] = initial_msg
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing cluster jobs in pipeline group job")
            raise PipelineError("Error in listing cluster jobs in pipeline group job" + str(e))

    def get_pipeline_group_job_log_as_text(self, domain_id, pipeline_group_id, job_id):
        """
            Gets Infoworks pipeline group job logs
            :param pipeline_group_id: id of the pipeline group whose logs are to be fetched
            :type pipeline_group_id: String
            :param domain_id: Domain id to which the pipeline belongs to
            :type domain_id: String
            :param job_id: id of the pipeline group job
            :return: response dict
        """
        if None in {pipeline_group_id, domain_id, job_id}:
            raise Exception(f"pipeline_group_id, domain_id, job_id cannot be None")
        try:
            if None in {pipeline_group_id, domain_id, job_id}:
                raise Exception(f"pipeline_group_id, domain_id, job_id cannot be None")
            response = self.call_api("GET", url_builder.get_pipeline_group_jobs_base_url(
                self.client_config, domain_id, pipeline_group_id) + f"/{job_id}/logs",
                                     IWUtils.get_default_header_for_v3(
                                         self.client_config['bearer_token'])).content
            if response is None:
                self.logger.error('Failed to find the pipeline group job logs')
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    error_desc='Failed to find the pipeline group job logs',
                                                    response=response)
            else:
                self.logger.info(
                    'Successfully got the pipeline group job {id} logs.'.format(id=job_id))
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, entity_id=job_id,
                                                    response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to get pipeline group job logs.')
            raise PipelineError('Error occurred while trying to get pipeline group job logs.')

    def create_pipeline_group(self, pipeline_group_config):
        """
            Function to create the pipeline group
            :param pipeline_group_config: a JSON object containing pipeline group configurations
            :type pipeline_group_config: JSON Object

        pipeline_group_config_example = {
                    "name": "test_pg1",
                    "description": "",
                    "environment_id": "a801283f2e8077120d000b49",
                    "domain_id": "576a8508b25540b4b180927f",
                    "batch_engine": "SNOWFLAKE",
                    "run_job_on_data_plane": false,
                    "pipelines": [
                        {
                            "pipeline_id": "270a56ca0ae71c79039d79e0",
                            "version": 3,
                            "execution_order": 1,
                            "run_active_version": true
                        },
                        {
                            "pipeline_id": "266c068325500cdc8ccd6d69",
                            "version": 1,
                            "execution_order": 2,
                            "run_active_version": true
                        }
                    ],
                    "custom_tags": [],
                    "query_tag": "",
                    "snowflake_warehouse": "DEMO_WH"
                }
            :return: response dict
        """

        try:
            if pipeline_group_config is None or "domain_id" not in pipeline_group_config:
                raise Exception(
                    f"pipeline_config cannot be None or domain_id should be present in pipeline_group_config")
            domain_id = pipeline_group_config["domain_id"]
            response = IWUtils.ejson_deserialize(self.call_api("POST", url_builder.get_pipeline_group_base_url(
                self.client_config, domain_id), IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                                               pipeline_group_config).content)
            result = response.get('result', {})
            pipeline_grp_id = result.get('id', None)
            if pipeline_grp_id is None:
                self.logger.error('Failed to create pipeline group.')
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    error_desc='Failed to create Pipeline Group',
                                                    response=response)

            pipeline_grp_id = str(pipeline_grp_id)
            self.logger.info(
                'Pipeline Group {id} has been created under domain {domain_id}.'.format(id=pipeline_grp_id,
                                                                                        domain_id=domain_id))
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, entity_id=pipeline_grp_id,
                                                response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to create a new pipeline group.')
            raise PipelineError('Error occurred while trying to create a new pipeline group.')

    def list_pipeline_groups_under_domain(self, domain_id, params=None):
        """
        Function to list the pipeline groups under domain
        :param domain_id: Entity identifier for domain
        :type domain_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response list
        """
        if None in {domain_id}:
            raise Exception(f"domain_id cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_pipeline_grp = url_builder.get_pipeline_group_base_url(self.client_config,
                                                                           domain_id) + IWUtils.get_query_params_string_from_dict(
            params=params)
        pipeline_groups_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_pipeline_grp,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                initial_msg = response.get("message", "")
                while len(result) > 0:
                    pipeline_groups_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", None)
                    if result is None:
                        return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                            error_code=ErrorCode.GENERIC_ERROR,
                                                            error_desc="Error in listing pipeline groups",
                                                            response=response
                                                            )

                response["result"] = pipeline_groups_list
                response["message"] = initial_msg
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing pipeline groups")
            raise PipelineError("Error in listing pipeline groups" + str(e))

    def get_pipeline_group_details(self, domain_id, pipeline_group_id):
        """
        Gets Infoworks pipeline group details for given pipeline group id
        :param pipeline_group_id: id of the pipeline group whose details are to be fetched
        :type pipeline_group_id: String
        :param domain_id: Domain id to which the pipeline  group belongs to
        :type domain_id: String
        :return: response dict
        """
        try:
            if None in {pipeline_group_id, domain_id}:
                raise Exception(f"pipeline_group_id, domain_id cannot be None")
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.get_pipeline_group_base_url(
                self.client_config, domain_id) + f"{pipeline_group_id}", IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)
            result = response.get('result', {})
            if result.get('id', None) is None:
                self.logger.error('Failed to find the pipeline group details')
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    error_desc='Failed to find the pipeline group details',
                                                    response=response)

            pipeline_group_id = str(pipeline_group_id)
            self.logger.info(
                'Successfully got the pipeline group {id} details.'.format(id=pipeline_group_id))
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, entity_id=pipeline_group_id,
                                                response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to get pipeline group details.')
            raise PipelineError('Error occurred while trying to get pipeline group details.')

    def delete_pipeline_group(self, domain_id, pipeline_group_id):
        """
        Function to delete the pipeline group
        :param pipeline_group_id: id of the pipeline group that has to be deleted
        :type pipeline_group_id: String
        :param domain_id: Domain id to which the pipeline group  belongs to
        :type domain_id: String
        :return: response dict
        """
        try:
            if None in {pipeline_group_id, domain_id}:
                raise Exception(f"pipeline_group_id, domain_id cannot be None")
            response = IWUtils.ejson_deserialize(self.call_api("DELETE", url_builder.get_pipeline_group_base_url(
                self.client_config, domain_id) + f"{pipeline_group_id}", IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)
            result = response.get('result', {})
            if result.get('id', None) is None:
                self.logger.error(f'Failed to delete the pipeline group {pipeline_group_id}')
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    error_desc=f'Failed to delete the pipeline group {pipeline_group_id}',
                                                    response=response)

            pipeline_group_id = str(pipeline_group_id)
            self.logger.info(
                'Successfully deleted the pipeline group {id}.'.format(id=pipeline_group_id))
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, entity_id=pipeline_group_id,
                                                response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to delete the pipeline group.')
            raise PipelineError('Error occurred while trying to delete the pipeline group.')

    def update_pipeline_group(self, domain_id, pipeline_group_id, pipeline_group_config):
        """
            Function to update the pipeline group
            :param pipeline_group_id: id of the pipeline group that has to be deleted
            :type pipeline_group_id: String
            :param domain_id: Domain id to which the pipeline group belongs to
            :type domain_id: String
            :param pipeline_group_config: a JSON object containing pipeline group configurations
            :type pipeline_group_config: JSON Object
        pipeline_group_config_example = {
                    "name": "test_pg1",
                    "description": "",
                    "environment_id": "a801283f2e8077120d000b49",
                    "domain_id": "576a8508b25540b4b180927f",
                    "batch_engine": "SNOWFLAKE",
                    "run_job_on_data_plane": false,
                    "pipelines": [
                        {
                            "pipeline_id": "270a56ca0ae71c79039d79e0",
                            "version": 3,
                            "execution_order": 1,
                            "run_active_version": true
                        },
                        {
                            "pipeline_id": "266c068325500cdc8ccd6d69",
                            "version": 1,
                            "execution_order": 2,
                            "run_active_version": true
                        }
                    ],
                    "custom_tags": [],
                    "query_tag": "",
                    "snowflake_warehouse": "DEMO_WH"
                }
            :return: response dict
        """
        try:
            if pipeline_group_config is None or None in {domain_id, pipeline_group_id}:
                raise Exception(
                    f"pipeline_config cannot be None or domain_id or pipeline_group_id cannot be null")
            domain_id = pipeline_group_config["domain_id"]
            response = IWUtils.ejson_deserialize(self.call_api("PUT", url_builder.get_pipeline_group_base_url(
                self.client_config, domain_id) + f"{pipeline_group_id}", IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']),
                                                               pipeline_group_config).content)
            result = response.get('result', {})
            pipeline_grp_id = result.get('id', None)
            if pipeline_grp_id is None:
                self.logger.error('Failed to update pipeline group.')
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    error_desc='Failed to update Pipeline Group',
                                                    response=response)

            pipeline_grp_id = str(pipeline_grp_id)
            self.logger.info(
                'Pipeline Group {id} has been updated under domain {domain_id}.'.format(id=pipeline_grp_id,
                                                                                        domain_id=domain_id))
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, entity_id=pipeline_grp_id,
                                                response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to update the pipeline group.')
            raise PipelineError('Error occurred while trying to update the pipeline group.')

    def get_accessible_pipeline_groups(self, domain_id, params=None):
        """
            Function to get the accessible pipeline group
            :param domain_id: Domain id to which the pipeline  group  belongs to
            :type domain_id: String
            :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
            :type: JSON dict
            :return: response dict
        """
        if domain_id is None:
            raise Exception(f"domain_id cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_get_accessible_pl_grp = url_builder.get_accessible_pipeline_groups_url(self.client_config,
                                                                                      domain_id) + IWUtils.get_query_params_string_from_dict(
            params=params)
        pipeline_groups_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_accessible_pl_grp,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                initial_msg = response.get("message", "")
                while len(result) > 0:
                    print(result)
                    print(response)
                    pipeline_groups_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", None)
                    if result is None:
                        return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                            error_code=ErrorCode.GENERIC_ERROR,
                                                            error_desc="Error in listing accessible pipeline groups",
                                                            response=response
                                                            )

                response["result"] = pipeline_groups_list
                response["message"] = initial_msg
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing accessible pipeline groups")
            raise PipelineError("Error in listing accessible pipeline groups" + str(e))

    def get_list_of_advanced_config_of_pipeline_groups(self, domain_id, pipeline_group_id, params=None):
        """
            Function to get list of Advance Config pipeline group
            :param domain_id: Domain id to which the pipeline  group belongs to
            :type domain_id: String
            param pipeline_group_id: Entity identifier of pipeline group
            :type pipeline_group_id: String
            :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
            :type: JSON dict
            :return: response dict
        """
        if None in {pipeline_group_id, domain_id}:
            raise Exception(f"pipeline_group_id, domain_id cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_get_adv_config_pl_grp = url_builder.advance_config_under_pipeline_groups_base_url(self.client_config,
                                                                                                 domain_id,
                                                                                                 pipeline_group_id) + IWUtils.get_query_params_string_from_dict(
            params=params)
        adv_config_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_adv_config_pl_grp,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                initial_msg = response.get("message", "")
                while len(result) > 0:
                    adv_config_list.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(
                        self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                            self.client_config['bearer_token'])).content)
                    result = response.get("result", None)
                    if result is None:
                        return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                            error_code=ErrorCode.GENERIC_ERROR,
                                                            error_desc="Error in listing adv config of pipeline groups",
                                                            response=response
                                                            )

                response["result"] = adv_config_list
                response["message"] = initial_msg
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing adv config of pipeline groups")
            raise PipelineError("Error in listing adv config of pipeline groups" + str(e))

    def modify_advanced_config_for_pipeline_group(self, domain_id, pipeline_group_id, adv_config_body,
                                                  action_type="update", key=None):
        """
        Function to add/update the adv config for the pipeline group
        :param pipeline_group_id: id of the pipeline group whose details are to be fetched
        :type pipeline_group_id: String
        :param domain_id: Domain id to which the pipeline group belongs to
        :type domain_id: String
        :param action_type: values can be either create/update. default update
        :type action_type: String
        :param adv_config_body: JSON dict
        adv_config_body_example = {
            "key" : "",
            "value": "",
            "description": "",
            "is_active": True
            }
        :param key: In case of update, name of the adv config to update
        :return: response dict
        """
        if None in {pipeline_group_id, domain_id} or adv_config_body is None:
            raise Exception(f"pipeline_group_id, domain_id and adv_config_body cannot be None")
        try:
            if action_type.lower() == "create":
                request_type = "POST"
                request_url = url_builder.advance_config_under_pipeline_groups_base_url(self.client_config, domain_id,
                                                                                        pipeline_group_id)
            else:
                request_type = "PUT"
                request_url = url_builder.advance_config_under_pipeline_groups_base_url(self.client_config, domain_id,
                                                                                        pipeline_group_id) + f"{key}"
            response = IWUtils.ejson_deserialize(
                self.call_api(request_type, request_url,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              adv_config_body).content)
            result = response.get('result', {})
            message = response.get('message', "")
            adv_config_id = result.get('id', None)
            if adv_config_id is not None:
                adv_config_id = str(adv_config_id)
                self.logger.info(
                    'Advanced Config has been created {id}.'.format(id=adv_config_id))
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, entity_id=adv_config_id,
                                                    response=response)
            elif message == "Successfully updated Advance configuration":
                self.logger.info('Advanced Config has been updated')
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
            else:
                self.logger.error(f'Failed to {action_type} advanced config for pipeline group.')
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    error_desc=f'Failed to {action_type} advanced config for pipeline group',
                                                    response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to add/update adv config pipeline group.')
            raise PipelineError('Error occurred while trying to add/update adv config pipeline group.')

    def get_or_delete_advance_config_details_of_pipeline_group(self, domain_id, pipeline_group_id, key,
                                                               action_type="get"):
        """
        Gets/Deletes advance configuration of pipeline group
        :param pipeline_group_id: id of the pipeline group whose details are to be fetched
        :type pipeline_group_id: String
        :param domain_id: Domain id to which the pipeline group belongs to
        :type domain_id: String
        :param key: name of the advanced configurations
        :param action_type: values can be get/delete
        :return: response dict
        """
        try:
            if None in {pipeline_group_id, domain_id, key}:
                raise Exception(f"pipeline_group_id, domain_id, key cannot be None")
            request_type = "GET" if action_type.lower() == "get" else "DELETE"
            response = IWUtils.ejson_deserialize(
                self.call_api(request_type, url_builder.advance_config_under_pipeline_groups_base_url(
                    self.client_config, domain_id, pipeline_group_id) + f"{key}", IWUtils.get_default_header_for_v3(
                    self.client_config['bearer_token'])).content)
            result = response.get('result', {})
            message = response.get("message", "")
            if action_type.lower() == "delete" and message == "Successfully removed Advance configuration":
                self.logger.info(
                    'Successfully deleted the advanced config')
                return GenericResponse.parse_result(status=Response.Status.SUCCESS,
                                                    response=response)

            if result.get('entity_id', None) is None:
                self.logger.error('Failed to find the advance config details')
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    error_desc='Failed to find the advance config details',
                                                    response=response)

            self.logger.info(
                'Successfully got the advanced config details.')
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, entity_id=result.get('entity_id', ''),
                                                response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to get/delete adv config details.')
            raise PipelineError('Error occurred while trying to get/delete adv config details.')
