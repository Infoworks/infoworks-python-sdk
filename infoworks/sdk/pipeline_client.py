import time

from infoworks.error import PipelineError
from infoworks.sdk import url_builder, local_configurations
from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.local_configurations import Response, ErrorCode
from infoworks.sdk.pipeline_response import PipelineResponse
from infoworks.sdk.utils import IWUtils


class PipelineClient(BaseClient):
    def __init__(self):
        super(PipelineClient, self).__init__()

    def poll_job(self, pipeline_id=None, job_id=None, poll_timeout=local_configurations.POLLING_TIMEOUT,
                 polling_frequency=local_configurations.POLLING_FREQUENCY_IN_SEC,
                 retries=local_configurations.NUM_POLLING_RETRIES):
        failed_count = 0
        response = {}
        timeout = time.time() + poll_timeout
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
                    self.logger.info(
                        "Job poll status : " + result["status"] + "Job completion percentage: " + str(result.get(
                            "percentage", 0)))
                    if job_status in ["completed", "failed", "aborted"]:
                        if job_status == "completed":
                            return PipelineResponse.parse_result(status=Response.Status.SUCCESS,
                                                                 pipeline_id=pipeline_id,
                                                                 job_id=job_id)
                        else:
                            return PipelineResponse.parse_result(status=job_status, pipeline_id=pipeline_id,
                                                                 job_id=job_id)
                else:
                    self.logger.error(f"Error occurred during job {job_id} status poll")
                    if failed_count >= retries - 1:
                        return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                             error_code=ErrorCode.GENERIC_ERROR,
                                                             error_desc=response, job_id=job_id,
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

    def create_pipeline(self, pipeline_config=None):
        """
        Create a new Pipeline.
        :param pipeline_config: a JSON object containing pipeline configurations
        :type pipeline_config: JSON Object

        pipeline_config_example = {
        "name": "pipeline_name",
        "batch_engine": "spark",
        "domain_id": "domain_id",
        "environment_id":"environment_id. Needed from 5.2 onwards"
        "storage_id": "environment_storage_id",
        "compute_template_id": "environment_compute_id",
        "ml_engine": "SparkML"
        }
        :return: response dict
        """
        response = None
        try:
            if pipeline_config is None:
                self.logger.error('Invalid pipeline configuration. Cannot create a new pipeline.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Cannot create a new pipeline.')
            domain_id = pipeline_config["domain_id"]
            response = IWUtils.ejson_deserialize(self.call_api("POST", url_builder.create_pipeline_url(
                self.client_config, domain_id), IWUtils.get_default_header_for_v3(self.client_config['bearer_token']),
                                                               pipeline_config).content)

            result = response.get('result', {})
            pipeline_id = result.get('id', None)

            if pipeline_id is None:
                self.logger.error('Pipeline failed to create.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Pipeline failed to create.')

            pipeline_id = str(pipeline_id)
            self.logger.info(
                'Pipeline {id} has been created under domain {domain_id}.'.format(id=pipeline_id, domain_id=domain_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=result)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to create a new pipeline.')
            raise PipelineError('Error occurred while trying to create a new pipeline.')

    def get_pipeline(self, pipeline_id, domain_id):
        """
        Gets Infoworks Data pipeline details for given pipeline id
        :param pipeline_id: id of the pipeline whose details is to be fetched
        :type pipeline_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :return: response dict
        """
        response = None
        try:
            if pipeline_id is None:
                self.logger.error('Please pass the mandatory pipeline id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory pipeline id as parameter.')
            if domain_id is None:
                self.logger.error('Please pass the mandatory domain id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory domain id as parameter.')
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.get_pipeline_url(
                self.client_config, domain_id, pipeline_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            result = response.get('result', {})
            # print(response)
            if result.get('id', None) is None:
                self.logger.error('Failed to find the pipeline details')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Failed to find the pipeline details')

            pipeline_id = str(pipeline_id)
            self.logger.info(
                'Successfully got the pipeline {id} details.'.format(id=pipeline_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=result)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get pipeline details.')
            raise PipelineError('Error occurred while trying to get pipeline details.')

    def delete_pipeline(self, pipeline_id, domain_id):
        """
        Deletes Infoworks Data pipeline  for given pipeline id
        :param pipeline_id: entity id of the pipeline to be deleted
        :type pipeline_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :return: response dict
        """
        response = None
        try:
            if pipeline_id is None:
                self.logger.error('Please pass the mandatory pipeline id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory pipeline id as parameter.')
            if domain_id is None:
                self.logger.error('Please pass the mandatory domain id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory domain id as parameter.')
            response = IWUtils.ejson_deserialize(self.call_api("DELETE", url_builder.delete_pipeline_url(
                self.client_config, domain_id, pipeline_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            result = response.get('result', {})

            if result.get('id', None) is None:
                self.logger.error(f'Failed to delete the pipeline {pipeline_id}')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc=f'Failed to delete the pipeline {pipeline_id}')

            pipeline_id = str(pipeline_id)
            self.logger.info(
                'Successfully deleted the pipeline {id}.'.format(id=pipeline_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to delete pipeline.')
            raise PipelineError('Error occurred while trying to delete pipeline.')

    def update_pipeline(self, pipeline_id, domain_id, pipeline_config):
        """
        Updates Infoworks Data pipeline details for given pipeline id
        :param pipeline_id: entity id of the pipeline to be updated
        :type pipeline_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :param pipeline_config: a JSON object containing pipeline configurations
        :type pipeline_config: JSON Object

        pipeline_config_example = {
        "name": "pipeline_name",
        "compute_template_id": "environment_compute_id",
        "active_version_id": "pipeline_version_id"
        }
        :return: response dict
        """
        response = None
        try:
            if pipeline_config is None:
                self.logger.error('Please pass the mandatory pipeline_config as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory pipeline_config as parameter.')
            if pipeline_id is None:
                self.logger.error('Please pass the mandatory pipeline id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory pipeline id as parameter.')
            if domain_id is None:
                self.logger.error('Please pass the mandatory domain id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory domain id as parameter.')
            response = IWUtils.ejson_deserialize(self.call_api("PUT", url_builder.update_pipeline_url(
                self.client_config, domain_id, pipeline_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']), pipeline_config).content)

            result = response.get('result', {})

            if result.get('id', None) is None:
                self.logger.error(f'Failed to update the pipeline {pipeline_id}')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc=f'Failed to update the pipeline {pipeline_id}',
                                                     response=response)

            pipeline_id = str(pipeline_id)
            self.logger.info(
                'Successfully updated the pipeline {id}.'.format(id=pipeline_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to update pipeline.')
            raise PipelineError('Error occurred while trying to update pipeline.')

    def sql_import_into_pipeline(self, pipeline_id, domain_id, pipeline_config):
        """
        Import SQL into given pipeline id
        :param pipeline_id Id of the pipeline to be updated
        :type Id as string
        :param domain_id Domain Id to which the pipeline belongs to
        :type Id as string
        :param pipeline_config: a JSON object containing pipeline configurations
        :type pipeline_config: JSON Object

        pipeline_config_example = {
        "dry_run": "{boolean}",
         "sql": "select * from employee",
         "sql_import_configuration": {
          "quoted_identifier": "DOUBLE_QUOTE",
          "sql_dialect": "LENIENT"
        }
        :return: response dict
        """
        response = None
        try:
            if pipeline_config is None:
                self.logger.error('Please pass the mandatory pipeline_config as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory pipeline_config as parameter.')
            if pipeline_id is None:
                self.logger.error('Please pass the mandatory pipeline id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory pipeline id as parameter.')
            if domain_id is None:
                self.logger.error('Please pass the mandatory domain id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory domain id as parameter.')
            response = IWUtils.ejson_deserialize(self.call_api("POST", url_builder.pipeline_sql_import_url(
                self.client_config, domain_id, pipeline_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']), pipeline_config).content)

            result = response.get('result', {})

            if result.get('status', 'failed') != 'success':
                self.logger.error(f'Failed to update the pipeline {pipeline_id} with the given SQL')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc=f'Failed to update the pipeline {pipeline_id}',
                                                     response=response)

            self.logger.info(
                'Successfully updated the pipeline {id} with the given SQL.'.format(id=pipeline_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to update pipeline with the given sql.')
            raise PipelineError('Error occurred while trying to update pipeline with the given sql.')

    def get_pipeline_version_details(self, pipeline_id, domain_id, pipeline_version_id):
        """
        Gets a pipelineVersion with pipeline_version_id of pipeline with {pipeline_id} in domain with {domain_id}
        :param pipeline_id: id of the pipeline whose details is to be fetched
        :type pipeline_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :param pipeline_version_id: id of the pipeline_version_id whose details are to be fetched
        :type pipeline_version_id: String
        :return: response dict
        """
        response = None
        try:
            if pipeline_version_id is None:
                self.logger.error('Please pass the mandatory pipeline_version_id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline_version_id configuration. '
                                                                'Please pass the mandatory pipeline_version_id as parameter.')
            if pipeline_id is None:
                self.logger.error('Please pass the mandatory pipeline id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory pipeline id as parameter.')
            if domain_id is None:
                self.logger.error('Please pass the mandatory domain id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory domain id as parameter.')
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.get_pipeline_version_url(
                self.client_config, domain_id, pipeline_id, pipeline_version_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            result = response.get('result', {})

            if result.get('id', None) is None:
                self.logger.error('Failed to find the pipeline version details')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Failed to find the pipeline version details',
                                                     response=response)

            pipeline_version_id = str(pipeline_version_id)
            self.logger.info(
                'Successfully got the pipeline version {pipeline_version_id} details.'.format(
                    pipeline_version_id=pipeline_version_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_version_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get pipeline version details.')
            raise PipelineError('Error occurred while trying to get pipeline version details.')

    def delete_pipeline_version(self, pipeline_id, domain_id, pipeline_version_id):
        """
        Deletes a pipelineVersion with pipeline_version_id of pipeline with {pipeline_id} in domain with {domain_id}
        :param pipeline_id: id of the pipeline whose details is to be fetched
        :type pipeline_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :param pipeline_version_id: id of the pipeline_version_id whose details is to be fetched
        :type pipeline_version_id: String
        :return: response dict

        """
        response = None
        try:
            if pipeline_version_id is None:
                self.logger.error('Please pass the mandatory pipeline_version_id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline_version_id configuration. '
                                                                'Please pass the mandatory pipeline_version_id as parameter.')
            if pipeline_id is None:
                self.logger.error('Please pass the mandatory pipeline id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory pipeline id as parameter.')
            if domain_id is None:
                self.logger.error('Please pass the mandatory domain id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory domain id as parameter.')
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.delete_pipeline_version_url(
                self.client_config, domain_id, pipeline_id, pipeline_version_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            result = response.get('result', {})

            if result.get('id', None) is None:
                self.logger.error('Failed to delete the pipeline version')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Failed to delete the pipeline version',
                                                     response=response)

            pipeline_version_id = str(pipeline_version_id)
            self.logger.info(
                'Successfully deleted the pipeline version {pipeline_version_id} details.'.format(
                    pipeline_version_id=pipeline_version_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_version_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to delete pipeline version details.')
            raise PipelineError('Error occurred while trying to delete pipeline version details.')

    def update_pipeline_version_details(self, pipeline_id, domain_id, pipeline_version_id,
                                        pipeline_version_config):
        """
        Update a pipelineVersion with pipeline_version_id of pipeline with {pipeline_id} in domain with {domain_id}
        :param pipeline_id: id of the pipeline whose details is to be fetched
        :type pipeline_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :param pipeline_version_id: id of the pipeline_version_id whose details is to be fetched
        :type pipeline_version_id: String
        :param pipeline_version_config: configurations of the pipeline version to be updated
        :type pipeline_version_config:JSON Object

        pipeline_version_config example:
        {
         "id": "e77850ad5127a2d7dab870ff",
         "created_at": "2019-11-21T12:54:05.875Z",
         "created_by": "6RkfybTRQQByEey3v",
         "modified_at": "2019-11-21T12:54:05.875Z",
         "modified_by": "6RkfybTRQQByEey3v"
        }
        :return: response dict

        """
        response = None
        try:
            if pipeline_version_config is None:
                self.logger.error('Please pass the mandatory pipeline_version_config as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline_version_config configuration. '
                                                                'Please pass the mandatory pipeline_version_config as parameter.')
            if pipeline_version_id is None:
                self.logger.error('Please pass the mandatory pipeline_version_id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline_version_id configuration. '
                                                                'Please pass the mandatory pipeline_version_id as parameter.')
            if pipeline_id is None:
                self.logger.error('Please pass the mandatory pipeline id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory pipeline id as parameter.')
            if domain_id is None:
                self.logger.error('Please pass the mandatory domain id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory domain id as parameter.')
            response = IWUtils.ejson_deserialize(self.call_api("PUT", url_builder.update_pipeline_version_url(
                self.client_config, domain_id, pipeline_id, pipeline_version_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']), pipeline_version_config).content)

            result = response.get('result', {})

            if result.get('id', None) is None:
                self.logger.error('Failed to update the pipeline version details')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Failed to update the pipeline version details',
                                                     response=response)

            pipeline_version_id = str(pipeline_version_id)
            self.logger.info(
                'Successfully updated the pipeline version {pipeline_version_id} details.'.format(
                    pipeline_version_id=pipeline_version_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_version_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to update pipeline version details.')
            raise PipelineError('Error occurred while trying to update pipeline version details.')

    def get_pipeline_configuration_json_export(self, pipeline_id, domain_id):
        """
        Get exported config for pipeline with pipeline_id in domain with domain_id
        :param pipeline_id: id of the pipeline whose details is to be fetched
        :type pipeline_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :return: response dict
        """
        response = None
        try:
            if pipeline_id is None:
                self.logger.error('Please pass the mandatory pipeline id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory pipeline id as parameter.')
            if domain_id is None:
                self.logger.error('Please pass the mandatory domain id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory domain id as parameter.')
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_builder.configure_pipeline_url(
                self.client_config, domain_id, pipeline_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token'])).content)

            result = response.get('result', {})

            if result is {}:
                self.logger.error('Failed to get the configuration json of pipeline')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Failed to get the configuration json of pipeline',
                                                     response=response)

            pipeline_id = str(pipeline_id)
            self.logger.info(
                'Successfully got the pipeline {id} configuration json.'.format(id=pipeline_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to get pipeline configuration json.')
            raise PipelineError('Error occurred while trying to get pipeline configuration json.')

    def update_pipeline_configuration_json_export(self, pipeline_id, domain_id, pipeline_config):
        """
        Import config for pipeline with pipeline_id in domain with domain_id
        :param pipeline_id: id of the pipeline whose details is to be fetched
        :type pipeline_id: String
        :param domain_id: Domain id to which the pipeline belongs to
        :type domain_id: String
        :param pipeline_config: configuration json of the pipeline
        :type pipeline_config: JSON Object
        :return: response dict
        """
        response = None
        try:
            if pipeline_config is None:
                self.logger.error('Please pass the mandatory pipeline_config as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory pipeline_config as parameter.')
            if pipeline_id is None:
                self.logger.error('Please pass the mandatory pipeline id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory pipeline id as parameter.')
            if domain_id is None:
                self.logger.error('Please pass the mandatory domain id as parameter.')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Invalid pipeline configuration. '
                                                                'Please pass the mandatory domain id as parameter.')
            response = IWUtils.ejson_deserialize(self.call_api("POST", url_builder.configure_pipeline_url(
                self.client_config, domain_id, pipeline_id), IWUtils.get_default_header_for_v3(
                self.client_config['bearer_token']), pipeline_config).content)

            result = response.get('result', [])

            if len(result) == 0:
                self.logger.error('Failed to update the configuration json of pipeline')
                return PipelineResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc='Failed to update the configuration json of pipeline',
                                                     response=response)

            pipeline_id = str(pipeline_id)
            self.logger.info(
                'Successfully updated the pipeline {id} configuration json.'.format(id=pipeline_id))
            return PipelineResponse.parse_result(status=Response.Status.SUCCESS, pipeline_id=pipeline_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to update pipeline configuration json.')
            raise PipelineError('Error occurred while trying to update pipeline configuration json.')

    def trigger_pipeline_job(self, domain_id, pipeline_id, pipeline_version_id=None, poll=False):
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
            raise PipelineError(f"Failed to submit pipeline build job for {pipeline_id} " + str(e))
        if len(result) != 0 and "id" in result:
            job_id = result["id"]
            if not poll:
                self.logger.info(f"Pipeline build job has been submitted for {pipeline_id} with {job_id}")
                return PipelineResponse.parse_result(status=Response.Status.SUCCESS)
            else:
                return self.poll_job(pipeline_id=pipeline_id, job_id=job_id)
        else:
            raise PipelineError(f"Failed to submit pipeline build job for {pipeline_id} ")

    def get_pipeline_id(self, pipeline_name, domain_id=None, domain_name=None):
        """
        Function to get pipeline id
        :param pipeline_name: Name of the pipeline
        :param domain_id: Entity identifier of the domain
        :param domain_name: Entity name of the domain
        :return: Pipeline ID
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
                return result[0].get('id')
        except:
            raise PipelineError("Unable to get pipeline NAME")
