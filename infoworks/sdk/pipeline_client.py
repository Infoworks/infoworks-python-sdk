import base64
import time

from infoworks.error import PipelineError
from infoworks.sdk import url_builder, local_configurations
from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.generic_response import GenericResponse
from infoworks.sdk.local_configurations import Response, ErrorCode
from infoworks.sdk.pipeline_response import PipelineResponse
from infoworks.sdk.utils import IWUtils


class PipelineClient(BaseClient):
    def __init__(self):
        super(PipelineClient, self).__init__()

    def poll_pipeline_job(self, pipeline_id=None, job_id=None, poll_timeout=local_configurations.POLLING_TIMEOUT,
                          polling_frequency=local_configurations.POLLING_FREQUENCY_IN_SEC,
                          retries=local_configurations.NUM_POLLING_RETRIES):
        """
        Function to poll the pipeline job
        :param pipeline_id: ID of the pipeline
        :type pipeline_id: String
        :param job_id: job_id of the pipeline
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
                    print(f"pipeline_status : {job_status}.Sleeping for {polling_frequency} seconds")
                    self.logger.info(
                        "Job poll status : " + result["status"] + "Job completion percentage: " + str(result.get(
                            "percentage", 0)))
                    if job_status in ["completed", "failed", "aborted", "canceled"]:
                        return PipelineResponse.parse_result(status=Response.Status.SUCCESS,
                                                             response=response,
                                                             pipeline_id=pipeline_id,
                                                             job_id=job_id)
                else:
                    self.logger.error(f"Error occurred during job {job_id} status poll")
                    if failed_count >= retries - 1:
                        return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                             error_code=ErrorCode.GENERIC_ERROR,
                                                             error_desc=f"Error occurred during job {job_id} status poll",
                                                             response=response, job_id=job_id,
                                                             pipeline_id=pipeline_id)
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

    def list_pipelines(self, domain_id=None, params=None):
        """
        Function to list the pipelines
        :param domain_id: Entity identified for domain
        :type domain_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response list
        """

        if None in {domain_id}:
            self.logger.error("Domain ID cannot be None")
            raise Exception("Domain ID cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_pipelines = url_builder.list_pipelines_url(self.client_config, domain_id) \
                                + IWUtils.get_query_params_string_from_dict(params=params)

        pipelines_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_pipelines,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                initial_msg = response.get("message", "")
                while len(result) > 0:
                    pipelines_list.extend(result)
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
                        return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                             error_code=ErrorCode.GENERIC_ERROR,
                                                             error_desc="Error in listing pipelines",
                                                             response=response
                                                             )

                response["result"] = pipelines_list
                response["message"] = initial_msg
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing pipelines")
            raise PipelineError("Error in listing pipelines" + str(e))

    def create_pipeline(self, pipeline_config=None):
        """
        Create a new Pipeline.
        :param pipeline_config: a JSON object containing pipeline configurations
        :type pipeline_config: JSON Object
        ```
        pipeline_config_example = {
        "name": "pipeline_name",
        "batch_engine": "spark",
        "domain_id": "domain_id",
        "environment_id":"environment_id. Needed from 5.2 onwards"
        "storage_id": "environment_storage_id",
        "compute_template_id": "environment_compute_id",
        "ml_engine": "SparkML"
        }
        ```
        :return: response dict
        """
        try:
            if pipeline_config is None:
                raise Exception(f"pipeline_config cannot be None")
            domain_id = pipeline_config["domain_id"]
            response = IWUtils.ejson_deserialize(self.call_api("POST", url_builder.create_pipeline_url(
                self.client_config, domain_id), IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                                               pipeline_config).content)
            result = response.get('result', {})
            pipeline_id = result.get('id', None)
            if pipeline_id is None:
                self.logger.error('Pipeline failed to create.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc='Pipeline failed to create.',
                                                     response=response)

            pipeline_id = str(pipeline_id)
            self.logger.info(
                'Pipeline {id} has been created under domain {domain_id}.'.format(id=pipeline_id, domain_id=domain_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to create a new pipeline.')
            raise PipelineError('Error occurred while trying to create a new pipeline.')

    def get_pipeline(self, pipeline_id=None, domain_id=None):
        """
        Gets Infoworks Data pipeline details for given pipeline id
        :param pipeline_id: id of the pipeline whose details are to be fetched
        :type pipeline_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :return: response dict
        """
        try:
            if None in {pipeline_id, domain_id}:
                raise Exception(f"pipeline_id, domain_id cannot be None")
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.get_pipeline_url(
                self.client_config, domain_id, pipeline_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)
            result = response.get('result', {})
            # print(response)
            if result.get('id', None) is None:
                self.logger.error('Failed to find the pipeline details')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc='Failed to find the pipeline details',
                                                     response=response)

            pipeline_id = str(pipeline_id)
            self.logger.info(
                'Successfully got the pipeline {id} details.'.format(id=pipeline_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to get pipeline details.')
            raise PipelineError('Error occurred while trying to get pipeline details.')

    def delete_pipeline(self, pipeline_id=None, domain_id=None):
        """
        Deletes Infoworks Data pipeline  for given pipeline id
        :param pipeline_id: entity id of the pipeline to be deleted
        :type pipeline_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :return: response dict
        """
        try:
            if None in {pipeline_id, domain_id}:
                raise Exception(f"pipeline_id, domain_id cannot be None")
            response = IWUtils.ejson_deserialize(self.call_api("DELETE", url_builder.delete_pipeline_url(
                self.client_config, domain_id, pipeline_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            result = response.get('result', {})

            if result.get('id', None) is None:
                self.logger.error(f'Failed to delete the pipeline {pipeline_id}')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc=f'Failed to delete the pipeline {pipeline_id}',
                                                     response=response)

            pipeline_id = str(pipeline_id)
            self.logger.info(
                'Successfully deleted the pipeline {id}.'.format(id=pipeline_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to delete pipeline.')
            raise PipelineError('Error occurred while trying to delete pipeline.')

    def update_pipeline(self, pipeline_id=None, domain_id=None, pipeline_config=None):
        """
        Updates Infoworks Data pipeline details for given pipeline id
        :param pipeline_id: entity id of the pipeline to be updated
        :type pipeline_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :param pipeline_config: a JSON object containing pipeline configurations
        :type pipeline_config: JSON Object
        ```
        pipeline_config_example = {
        "name": "pipeline_name",
        "compute_template_id": "environment_compute_id",
        "active_version_id": "pipeline_version_id"
        }
        ```
        :return: response dict
        """
        try:
            if None in {pipeline_id, domain_id} or pipeline_config is None:
                raise Exception(f"pipeline_config, pipeline_id, domain_id cannot be None")
            response = IWUtils.ejson_deserialize(self.call_api("PUT", url_builder.update_pipeline_url(
                self.client_config, domain_id, pipeline_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']), pipeline_config).content)
            result = response.get('result', {})
            if result.get('id', None) is None:
                self.logger.error(f'Failed to update the pipeline {pipeline_id}')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc=f'Failed to update the pipeline {pipeline_id}',
                                                     response=response)
            pipeline_id = str(pipeline_id)
            self.logger.info(
                'Successfully updated the pipeline {id}.'.format(id=pipeline_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to update pipeline.')
            raise PipelineError('Error occurred while trying to update pipeline.')

    def sql_import_into_pipeline(self, pipeline_id=None, domain_id=None, import_configs=None, sql_query=None):
        """
        Import SQL into given pipeline id. You could either pass the config body as shown in example or simply pass the sql_query
        :param pipeline_id: Id of the pipeline
        :type pipeline_id: String
        :param domain_id: Domain Id to which the pipeline belongs to
        :type domain_id: String
        :param import_configs: a JSON object containing configurations to import the SQL
        :type import_configs: JSON Object
        :param sql_query: SQL query
        :type sql_query: String
        ```
        import_configs_example = {
        "dry_run": "{boolean}",
         "sql": "select * from employee",
         "sql_import_configuration": {
          "quoted_identifier": "DOUBLE_QUOTE",
          "sql_dialect": "LENIENT"
        }
        ```
        :return: response dict
        """
        try:
            if None in {pipeline_id, domain_id}:
                raise Exception(f"pipeline_id, domain_id cannot be None")
            if import_configs is None and sql_query is None:
                raise Exception(f"Either import_configs or sql_query has to be passed")
            if sql_query is not None:
                import_configs = {
                    "dry_run": False,
                    "sql": sql_query,
                    "sql_import_configuration": {
                        "quoted_identifier": "DOUBLE_QUOTE",
                        "sql_dialect": "LENIENT"
                    }
                }
            response = IWUtils.ejson_deserialize(self.call_api("POST", url_builder.pipeline_sql_import_url(
                self.client_config, domain_id, pipeline_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']), import_configs).content)
            result = response.get('result', {})
            if result.get('status', 'failed') != 'success':
                self.logger.error(f'Failed to update the pipeline {pipeline_id} with the given SQL')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc=f'Failed to update the pipeline {pipeline_id}',
                                                     response=response)
            self.logger.info(
                'Successfully updated the pipeline {id} with the given SQL.'.format(id=pipeline_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to update pipeline with the given sql.')
            raise PipelineError('Error occurred while trying to update pipeline with the given sql.')

    def get_pipeline_version_details(self, pipeline_id=None, domain_id=None, pipeline_version_id=None):
        """
        Gets a pipelineVersion details for a pipeline version
        :param pipeline_id: id of the pipeline whose details are to be fetched
        :type pipeline_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :param pipeline_version_id: id of the pipeline version whose details are to be fetched
        :type pipeline_version_id: String
        :return: response dict
        """
        try:
            if None in {pipeline_id, pipeline_version_id, domain_id}:
                raise Exception(f"pipeline_id, pipeline_version_id, domain_id cannot be None")
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.get_pipeline_version_url(
                self.client_config, domain_id, pipeline_id, pipeline_version_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)
            result = response.get('result', {})
            if result.get('id', None) is None:
                self.logger.error('Failed to find the pipeline version details')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc='Failed to find the pipeline version details',
                                                     response=response)
            pipeline_version_id = str(pipeline_version_id)
            self.logger.info(
                'Successfully got the pipeline version {pipeline_version_id} details.'.format(
                    pipeline_version_id=pipeline_version_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_version_id,
                                                 response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to get pipeline version details.')
            raise PipelineError('Error occurred while trying to get pipeline version details.')

    def delete_pipeline_version(self, pipeline_id=None, domain_id=None, pipeline_version_id=None):
        """
        Deletes a pipelineVersion
        :param pipeline_id: id of the pipeline
        :type pipeline_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :param pipeline_version_id: id of the pipeline version that has to be deleted
        :type pipeline_version_id: String
        :return: response dict
        """
        try:
            if None in {pipeline_id, pipeline_version_id, domain_id}:
                raise Exception(f"pipeline_id, pipeline_version_id, domain_id cannot be None")
            response = IWUtils.ejson_deserialize(self.call_api("DELETE", url_builder.delete_pipeline_version_url(
                self.client_config, domain_id, pipeline_id, pipeline_version_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)
            if response.get("message","")!="Successfully removed Pipeline Version":
                self.logger.error('Failed to delete the pipeline version')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc='Failed to delete the pipeline version',
                                                     response=response)
            pipeline_version_id = str(pipeline_version_id)
            self.logger.info(
                'Successfully deleted the pipeline version {pipeline_version_id} details.'.format(
                    pipeline_version_id=pipeline_version_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to delete pipeline version details.')
            raise PipelineError('Error occurred while trying to delete pipeline version details.')

    def update_pipeline_version_details(self, pipeline_id=None, domain_id=None, pipeline_version_id=None,
                                        pipeline_version_config=None):
        """
        Update a pipelineVersion with pipeline version config
        :param pipeline_id: id of the pipeline
        :type pipeline_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :param pipeline_version_id: id of the pipeline version to be updated
        :type pipeline_version_id: String
        :param pipeline_version_config: configurations of the pipeline version to be updated
        :type pipeline_version_config:JSON Object
        :return: response dict
        """
        try:
            if None in {pipeline_id, pipeline_version_id, domain_id} or pipeline_version_config is None:
                raise Exception(f"pipeline_version_config, pipeline_id, pipeline_version_id, domain_id cannot be None")
            response = IWUtils.ejson_deserialize(self.call_api("PUT", url_builder.update_pipeline_version_url(
                self.client_config, domain_id, pipeline_id, pipeline_version_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']), pipeline_version_config).content)
            result = response.get('result', {})
            if result.get('id', None) is None:
                self.logger.error('Failed to update the pipeline version details')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc='Failed to update the pipeline version details',
                                                     response=response)
            pipeline_version_id = str(pipeline_version_id)
            self.logger.info(
                'Successfully updated the pipeline version {pipeline_version_id} details.'.format(
                    pipeline_version_id=pipeline_version_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_version_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to update pipeline version details.')
            raise PipelineError('Error occurred while trying to update pipeline version details.')

    def export_pipeline_configurations(self, pipeline_id=None, domain_id=None):
        """
        Get exported config for pipeline
        :param pipeline_id: id of the pipeline whose details are to be fetched
        :type pipeline_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :return: response dict
        """
        try:
            if None in {pipeline_id, domain_id}:
                raise Exception(f"pipeline_id, domain_id cannot be None")
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.configure_pipeline_url(
                self.client_config, domain_id, pipeline_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)
            result = response.get('result', {})
            if result is {}:
                self.logger.error('Failed to get the configuration json of pipeline')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc='Failed to get the configuration json of pipeline',
                                                     response=response)
            pipeline_id = str(pipeline_id)
            self.logger.info(
                'Successfully got the pipeline {id} configuration json.'.format(id=pipeline_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to get pipeline configuration json.')
            raise PipelineError('Error occurred while trying to get pipeline configuration json.')

    def import_pipeline_configurations(self, pipeline_id=None, domain_id=None, pipeline_config=None):
        """
        Import configurations for the pipeline
        :param pipeline_id: id of the pipeline
        :type pipeline_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :param pipeline_config: configuration json of the pipeline
        :type pipeline_config: JSON Object
        :return: response dict
        """
        try:
            if None in {pipeline_id, domain_id} or pipeline_config is None:
                raise Exception(f"pipeline_config, pipeline_id, domain_id cannot be None")
            response = IWUtils.ejson_deserialize(self.call_api("POST", url_builder.configure_pipeline_url(
                self.client_config, domain_id, pipeline_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']), pipeline_config).content)
            result = response.get('result', [])
            if len(result) == 0:
                self.logger.error('Failed to update the configuration json of pipeline')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc='Failed to update the configuration json of pipeline',
                                                     response=response)

            pipeline_id = str(pipeline_id)
            self.logger.info(
                'Successfully updated the pipeline {id} configuration json.'.format(id=pipeline_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to update pipeline configuration json.')
            raise PipelineError('Error occurred while trying to update pipeline configuration json.')

    def trigger_pipeline_job(self, domain_id=None, pipeline_id=None, pipeline_version_id=None, poll=False):
        """
           Trigger the pipeline build job
           :param pipeline_id: entity id of the pipeline
           :type pipeline_id: String
           :param pipeline_version_id: entity id  of the pipeline version
           :type pipeline_version_id: String
           :param domain_id: Domain id to which the pipeline belongs to
           :type domain_id: String
           :param poll: Poll job until its completion
           :type poll: Boolean
        """
        if domain_id is None or pipeline_id is None:
            self.logger.error('Please pass non-null values to mandatory parameters, domain_id and pipeline_id')
            raise Exception('Please pass non-null values to mandatory parameters, domain_id and pipeline_id')
        url_for_pipeline_build = url_builder.trigger_pipeline_build_url(self.client_config, domain_id, pipeline_id)
        if pipeline_version_id is not None:
            request_body = {
                "job_type": "pipeline_build",
                "version_id": str(pipeline_version_id)
            }
        else:
            request_body = {
                "job_type": "pipeline_build"
            }
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", url_for_pipeline_build,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                              request_body).content)
            result = response.get('result', {})
        except Exception as e:
            self.logger.error(e)
            raise PipelineError(f"Failed to submit pipeline build job for {pipeline_id} " + str(e))
        if len(result) != 0 and "id" in result:
            job_id = result["id"]
            if not poll:
                self.logger.info(f"Pipeline build job has been submitted for {pipeline_id} with {job_id}")
                return PipelineResponse.parse_result(status=Response.Status.SUCCESS, job_id=job_id,
                                                     pipeline_id=pipeline_id, response=response)
            else:
                return self.poll_pipeline_job(pipeline_id=pipeline_id, job_id=job_id)
        else:
            self.logger.error(response)
            raise PipelineError(f"Failed to submit pipeline build job for {pipeline_id} ")

    def get_pipeline_id(self, pipeline_name, domain_id=None, domain_name=None):
        """
        Function to get pipeline id
        :param pipeline_name: Name of the pipeline
        :type pipeline_name: String
        :param domain_id: Entity identifier of the domain
        :type domain_id: String
        :param domain_name: Entity name of the domain
        :type domain_name: String
        :return: response Dict
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
                self.logger.error("Error in listing domains " + str(e))
                raise PipelineError("Error in listing domains" + str(e))
        try:
            params = {"filter": {"name": pipeline_name}}
            url_to_get_pipeline_info = url_builder.create_pipeline_url(self.client_config,
                                                                       domain_id) + IWUtils.get_query_params_string_from_dict(
                params=params)
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_pipeline_info,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                              ).content)
            if response is not None and "result" in response:
                result = response.get("result", None)
                if len(result) > 0:
                    return PipelineResponse.parse_result(status=Response.Status.SUCCESS,
                                                         pipeline_id=result[0].get('id'), response=response)
                else:
                    return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                         pipeline_id=None, response=response)
        except Exception as e:
            self.logger.error("Unable to get pipeline name " + str(e))
            raise PipelineError("Unable to get pipeline name " + str(e))

    def get_pipeline_lineage(self, domain_id, pipeline_id, pipeline_version_id, column_name, node):
        """
        Function to get the lineage of column in pipeline
        :param pipeline_id: Entity identifier for pipeline
        :type pipeline_id: String
        :param pipeline_version_id: Entity identifier for pipeline version
        :type pipeline_version_id: String
        :param domain_id: Entity identifier of the domain
        :type domain_id: String
        :param column_name: Name of the column for which lineage needs to be tracked
        :type column_name: String
        :param node: Node name from which the column lineage needs to be tracked
        :type node: String
        :return: response dict
        """
        url_to_get_pipeline_lineage = url_builder.get_pipeline_lineage_url(self.client_config,
                                                                           domain_id,
                                                                           pipeline_id,
                                                                           pipeline_version_id,
                                                                           column_name,
                                                                           node)
        try:
            if None in {pipeline_id, pipeline_version_id, domain_id}:
                raise Exception(f"pipeline_id, pipeline_version_id, domain_id cannot be None")
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_pipeline_lineage,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                return PipelineResponse.parse_result(status=Response.Status.SUCCESS,
                                                     pipeline_id=pipeline_id, response=response)
            else:
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     response=response)
        except Exception as e:
            self.logger.error(
                f"Failed to get pipeline lineage for pipeline {pipeline_id} and column {column_name} " + str(e))
            raise PipelineError(
                f"Failed to get pipeline lineage for pipeline {pipeline_id} and column {column_name} " + str(e))

    def create_pipeline_version(self, domain_id=None, pipeline_id=None, body=None):
        """
        Create a new Pipeline Version
        :param body: a JSON object containing pipeline version
        :type body: JSON Object
        :param domain_id: Entity identifier of domain
        :param pipeline_id: Entity identifier of pipeline_id
        :return: response dict
        """
        try:
            if None in {pipeline_id, domain_id}:
                raise Exception(f"pipeline_id, domain_id cannot be None")
            response = IWUtils.ejson_deserialize(self.call_api("POST", url_builder.pipeline_version_base_url(
                self.client_config, domain_id, pipeline_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']),
                                                               body).content)
            result = response.get('result', {})
            pipeline_version_id = result.get('id', None)
            if pipeline_version_id is None:
                self.logger.error('Pipeline version failed to create.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc='Pipeline version failed to create.',
                                                     response=response)

            pipeline_version_id = str(pipeline_version_id)
            self.logger.info(
                'Pipeline version {id} has been created under domain {domain_id}.'.format(id=pipeline_version_id,
                                                                                          domain_id=domain_id))
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, entity_id=pipeline_version_id,
                                                response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to create a new pipeline version.')
            raise PipelineError('Error occurred while trying to create a new pipeline version.')

    def update_pipeline_version(self, domain_id=None, pipeline_id=None, pipeline_version_id=None, body=None):
        """
        Updates the Pipeline Version
        :param body: a JSON object containing pipeline version
        :type body: JSON Object
        :param domain_id: Entity identifier of domain
        :param pipeline_id: Entity identifier of pipeline_id
        :param pipeline_version_id: Entity identifier of pipeline_version_id
        :return: response dict
        """
        try:
            if None in {pipeline_id, pipeline_version_id, domain_id}:
                raise Exception(f"pipeline_id, pipeline_version_id, domain_id cannot be None")
            response = IWUtils.ejson_deserialize(self.call_api("PATCH", url_builder.pipeline_version_base_url(
                self.client_config, domain_id, pipeline_id) + f"/{pipeline_version_id}",
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']), body).content)
            message = response.get('message', "")
            if message != "Pipeline Version Updated Successfully":
                self.logger.error('Pipeline version failed to update.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc='Pipeline version failed to update.',
                                                     response=response)

            else:
                self.logger.info(
                    'Pipeline version {id} has been updated.'.format(id=pipeline_version_id))
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, entity_id=pipeline_version_id,
                                                    response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to update a pipeline version.')
            raise PipelineError('Error occurred while trying to update a pipeline version.')

    def set_pipeline_version_as_active(self, domain_id=None, pipeline_id=None, pipeline_version_id=None):
        """
        Sets the pipeline version as active
        :param domain_id: Entity identifier of domain
        :param pipeline_id: Entity identifier of pipeline_id
        :param pipeline_version_id: Entity identifier of pipeline_version_id
        :return: response dict
        """
        try:
            if None in {pipeline_id, pipeline_version_id, domain_id}:
                raise Exception(f"pipeline_id, pipeline_version_id, domain_id cannot be None")
            response = IWUtils.ejson_deserialize(self.call_api("POST", url_builder.pipeline_version_base_url(
                self.client_config, domain_id, pipeline_id) + f"/{pipeline_version_id}/set-active",
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token'])).content)
            message = response.get('message', "")
            if message != "Successfully set active version":
                self.logger.error('Pipeline version failed to be set active.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc='Pipeline version failed to be set active.',
                                                     response=response)

            else:
                self.logger.info(
                    'Pipeline version {id} has been set as active.'.format(id=pipeline_version_id))
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, entity_id=pipeline_version_id,
                                                    response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to set a pipeline version as active.')
            raise PipelineError('Error occurred while trying to set a pipeline version as active.')

    def modify_advanced_config_for_pipeline(self, domain_id=None, pipeline_id=None, adv_config_body=None,
                                            action_type="update", key=None):
        """
        Function to add/update the adv config for the pipeline
        :param pipeline_id: id of the pipeline
        :type pipeline_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :param action_type: values can be either create/update. default update
        :type action_type: String
        :param adv_config_body: JSON dict
        ```
        adv_config_body_example = {
            "key" : "",
            "value": "",
            "description": "",
            "is_active": True
            }
        ```
        :param key: In case of update, name of the adv config to update
        :return: response dict
        """
        if None in {pipeline_id, domain_id} or adv_config_body is None:
            raise Exception(f"pipeline_id, domain_id and adv_config_body cannot be None")
        try:
            if action_type.lower() == "create":
                request_type = "POST"
                request_url = url_builder.advanced_config_pipeline_url(self.client_config, domain_id,
                                                                       pipeline_id)
            else:
                request_type = "PUT"
                request_url = url_builder.advanced_config_pipeline_url(self.client_config, domain_id,
                                                                       pipeline_id) + f"{key}"
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
                self.logger.error(f'Failed to {action_type} advanced config for pipeline.')
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                    error_code=ErrorCode.USER_ERROR,
                                                    error_desc=f'Failed to {action_type} advanced config for pipeline',
                                                    response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to add/update adv config pipeline.')
            raise PipelineError('Error occurred while trying to add/update adv config pipeline.')

    def list_pipeline_versions(self, domain_id=None, pipeline_id=None, params=None):
        """
        Function to list the pipeline versions
        :param domain_id: Entity identified for domain
        :type domain_id: String
        :param pipeline_id: Entity identified for pipeline
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response list
        """
        if None in {domain_id, pipeline_id}:
            self.logger.error("domain_id and pipeline_id cannot be None")
            raise Exception("omain_id and pipeline_id cannot be None")
        if params is None:
            params = {"limit": 20, "offset": 0}
        url_to_list_pipeline_versions = url_builder.pipeline_version_base_url(self.client_config, domain_id,
                                                                              pipeline_id) \
                                        + IWUtils.get_query_params_string_from_dict(params=params)

        pipelines_list = []
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_pipeline_versions,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                initial_msg = response.get("message", "")
                while len(result) > 0:
                    pipelines_list.extend(result)
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
                        return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                             error_code=ErrorCode.GENERIC_ERROR,
                                                             error_desc="Error in listing pipeline version",
                                                             response=response
                                                             )

                response["result"] = pipelines_list
                response["message"] = initial_msg
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            self.logger.error("Error in listing pipeline version")
            raise PipelineError("Error in listing pipeline version" + str(e))

    def validate_pipeline(self, domain_id, pipeline_id, pipeline_version_id):
        """
        Validates the pipeline
        :param domain_id: Domain ID of which pipeline belongs
        :type domain_id: String
        :param pipeline_id: Entity id of the pipeline
        :type pipeline_id: String
        :param pipeline_version_id: Entity id of the pipeline version
        :type pipeline_version_id: String
        :return: Response Dict
        """
        if domain_id is None or pipeline_id is None or pipeline_version_id is None:
            self.logger.error("domain_id or pipeline_id or pipeline_version_id cannot be None")
            raise Exception("domain_id or pipeline_id or pipeline_version_id cannot be None")

        response = None

        try:
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.validate_pipeline_url(
                self.client_config, domain_id, pipeline_id, pipeline_version_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to validate pipeline')
            raise PipelineError('Error occurred while trying to validate pipeline')

    def refresh_sample_data_of_pipeline(self, domain_id, pipeline_id, pipeline_version_id):
        """
        Function to refresh the sample data of pipeline
        :param domain_id: Domain ID of which pipeline belongs
        :type domain_id: String
        :param pipeline_id: Entity id of the pipeline
        :type pipeline_id: String
        :param pipeline_version_id: Entity id of the pipeline version
        :type pipeline_version_id: String
        :return: Response Dict
        """
        if domain_id is None or pipeline_id is None or pipeline_version_id is None:
            self.logger.error("domain_id or pipeline_id or pipeline_version_id cannot be None")
            raise Exception("domain_id or pipeline_id or pipeline_version_id cannot be None")

        response = None

        try:
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.validate_pipeline_url(
                self.client_config, domain_id, pipeline_id, pipeline_version_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to refresh sample data pipeline')
            raise PipelineError('Error occurred while trying to refresh sample data pipeline')

    def get_details_of_pipeline_node(self, domain_id, pipeline_id, pipeline_version_id, node_id, action_type="schema"):
        """
        Function to get the schema, data, query and sql-inspection of pipeline node
        :param domain_id: Domain ID of which pipeline belongs
        :type domain_id: String
        :param pipeline_id: Entity id of the pipeline
        :type pipeline_id: String
        :param pipeline_version_id: Entity id of the pipeline version
        :type pipeline_version_id: String
        :param node_id: Node id of the node in pipeline
        :param action_type: Can be one of schema, data, query or sql-inspection
        :return: Response Dict
        """
        if None in {domain_id, pipeline_id, pipeline_version_id, node_id}:
            self.logger.error("domain_id or pipeline_id or pipeline_version_id or node_id cannot be None")
            raise Exception("domain_id or pipeline_id or pipeline_version_id or node_id cannot be None")

        response = None

        try:
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.url_to_get_pipeline_node_details(
                self.client_config, domain_id, pipeline_id, pipeline_version_id, node_id) + action_type,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token'])).content)

            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get node details requested')
            raise PipelineError('Error occurred while trying to get node details requested')

    def get_pipeline_version_audits(self, domain_id, pipeline_id, pipeline_version_id):
        """
        Function to get the audit logs of the pipeline version
        :param domain_id: Domain ID of which pipeline belongs
        :type domain_id: String
        :param pipeline_id: Entity id of the pipeline
        :type pipeline_id: String
        :param pipeline_version_id: Entity id of the pipeline version
        :type pipeline_version_id: String
        :return: Response Dict
        """
        if domain_id is None or pipeline_id is None or pipeline_version_id is None:
            self.logger.error("domain_id or pipeline_id or pipeline_version_id cannot be None")
            raise Exception("domain_id or pipeline_id or pipeline_version_id cannot be None")

        response = None

        try:
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.url_for_pipeline_version_audits(
                self.client_config, domain_id, pipeline_id, pipeline_version_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get pipeline version audit logs')
            raise PipelineError('Error occurred while trying to get pipeline version audit logs')

    def get_pipeline_audits(self, domain_id, pipeline_id):
        """
        Function to get the audit logs of the pipeline version
        :param domain_id: Domain ID of which pipeline belongs
        :type domain_id: String
        :param pipeline_id: Entity id of the pipeline
        :type pipeline_id: String
        :return: Response Dict
        """
        if domain_id is None or pipeline_id is None:
            self.logger.error("domain_id or pipeline_id cannot be None")
            raise Exception("domain_id or pipeline_id cannot be None")

        response = None

        try:
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.url_for_pipeline_audits(
                self.client_config, domain_id, pipeline_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get pipeline audit logs')
            raise PipelineError('Error occurred while trying to get pipeline audit logs')

    def import_sql_mappings(self, pipeline_id=None, domain_id=None, sql_mapping=None):
        """
        Function to import sql mappings for Infoworks Data pipeline
        :param pipeline_id: entity id of the pipeline to be updated
        :type pipeline_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :param sql_mapping: a JSON object containing SQL mappings
        :type sql_mapping: JSON Object
        ```
           sql_mapping_example = {
          "dry_run": false,
          "sql": null,
          "sql_import_configuration": {
            "sql_dialect": "LENIENT",
            "quoted_identifier": "DOUBLE_QUOTE"
          },
          "import_json": {
            "mappings": [
              {
                "entity_type": "table",
                "to_entity_id": "2f1bf594bc37fdd7edbda06c",
                "recommendation": {
                  "original_table_name": "based_table"
                }
              }
            ]
          }
        }

        ```
        :return: response dict
        """
        try:
            if None in {pipeline_id, domain_id} or sql_mapping is None:
                raise Exception(f"pipeline_config, pipeline_id, domain_id cannot be None")
            response = IWUtils.ejson_deserialize(self.call_api("PUT", url_builder.import_sql_mappings_url(
                self.client_config, domain_id, pipeline_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']), sql_mapping).content)
            result = response.get('result', {})
            if result.get('id', None) is None:
                self.logger.error(f'Failed to import the sql mapping {pipeline_id}')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_code=ErrorCode.USER_ERROR,
                                                     error_desc=f'Failed to update the pipeline {pipeline_id} sql mapping',
                                                     response=response)
            pipeline_id = str(pipeline_id)
            self.logger.info(
                'Successfully updated the pipeline {id} with sql mappings.'.format(id=pipeline_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=response)
        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to import sql mapping.')
            raise PipelineError('Error occurred while trying to import sql mapping.')

    def create_sql_pipeline(self, pipeline_config=None, pipeline_id=None, domain_id=None, sql_query="",
                            pipeline_parameters=None):
        """
        Create a new SQL Pipeline version. You can pass pipeline id if you need to create sql version in existing pipeline
        :param pipeline_config: a JSON object containing pipeline configurations
        :type pipeline_config: JSON Object
        :param pipeline_id: Entity identifier of pipeline
        :param domain_id: Entity identifier of domain
        :param sql_query: A SQL query that is part of pipeline
        :param pipeline_parameters: Array of key-value pair. Example: [{"key": param_key, "value": "value1"}]
        :type pipeline_parameters: List
        ```
        pipeline_config_example = {
            "name": pipeline_name,
            "description": '',
            "environment_id": env_id,
            "domain_id": domain_id,
            "custom_tags": [],
            "batch_engine": 'SNOWFLAKE',
            "snowflake_profile": snowflake_profile,
            "snowflake_warehouse": warehouse,
            "run_job_on_data_plane": False,
            "query_tag": ""
            }
        ```
        :return: response dict
        """
        if pipeline_parameters is None:
            pipeline_parameters = []
        try:
            if pipeline_config is None and pipeline_id is None:
                raise Exception(f"pipeline_config and pipeline_id both cannot be None")
            if pipeline_config is None:
                if None in {pipeline_id, domain_id}:
                    raise Exception(f"As pipeline_config is None, please pass pipeline_id and domain_id.")
            if pipeline_config is not None:
                domain_id = pipeline_config["domain_id"]
                response = IWUtils.ejson_deserialize(self.call_api("POST", url_builder.create_pipeline_url(
                    self.client_config, domain_id), IWUtils.get_default_header_for_v3(
                    self.client_config['bearer_token']),
                                                                   pipeline_config).content)
                result = response.get('result', {})
                pipeline_id = result.get('id', None)
                if pipeline_id is None:
                    self.logger.error('Failed to create pipeline')
                    return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                         error_code=ErrorCode.USER_ERROR,
                                                         error_desc='Failed to create pipeline.',
                                                         response=response)
            else:
                pipeline_id = str(pipeline_id)
                self.logger.info(
                    'Pipeline {id} has been created/found under domain {domain_id}.'.format(id=pipeline_id,
                                                                                            domain_id=domain_id))

                sample_string_bytes = sql_query.encode("ascii")
                base64_bytes = base64.b64encode(sample_string_bytes)
                base64_string = base64_bytes.decode("ascii")
                pv_response = self.create_pipeline_version(domain_id, pipeline_id, body={
                    "pipeline_id": pipeline_id,
                    "type": "sql",
                    "query": base64_string,
                    "pipeline_parameters": pipeline_parameters
                })
                if pv_response["result"].get("status", "") == "success":
                    pipeline_version_id = pv_response["result"]["entity_id"]
                    self.logger.info(f"Pipeline version {pipeline_version_id} created")
                    pv_details_response = self.get_pipeline_version_details(pipeline_id, domain_id,
                                                                            pipeline_version_id)
                    # Make the new created version as active
                    pv_active_response = self.set_pipeline_version_as_active(domain_id, pipeline_id,
                                                                             pipeline_version_id)
                    if pv_active_response["result"].get("status", "") != "success":
                        self.logger.error(f"Unable to set the pipeline version {pipeline_version_id} as active")
                        self.logger.error(str(pv_active_response))
                    else:
                        self.logger.info(f"Pipeline version {pipeline_version_id} set as active")
                else:
                    self.logger.error(f"Unable to create the new pipeline version")
                    self.logger.error(str(pv_response))

                return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                     response=pv_response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(e))
            self.logger.exception('Error occurred while trying to create a new sql pipeline.')
            raise PipelineError('Error occurred while trying to create a new sql pipeline.')
