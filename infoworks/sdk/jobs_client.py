import traceback

from infoworks.error import JobsError
from infoworks.sdk import url_builder, local_configurations
from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.local_configurations import Response, ErrorCode
from infoworks.sdk.generic_response import GenericResponse
from infoworks.sdk.utils import IWUtils


class JobsClient(BaseClient):

    def __init__(self):
        super(JobsClient, self).__init__()

    def get_job_details(self, job_id=None, params=None):
        """
        Function to get the job details
        :param job_id: entity identifier for job
        :type: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response list of dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}

        url_to_list_jobs = url_builder.get_jobs_url(self.client_config)
        if job_id is not None:
            url_to_list_jobs = url_to_list_jobs + f"/{job_id}"
        url_to_list_jobs = url_to_list_jobs + IWUtils.get_query_params_string_from_dict(params=params)
        job_details = []

        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_jobs,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                if job_id is not None:
                    job_details.extend([result])
                else:
                    while len(result) > 0:
                        job_details.extend(result)
                        nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                        response = IWUtils.ejson_deserialize(
                            self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                                self.client_config['bearer_token'])).content)
                        result = response.get("result", [])
            return GenericResponse.parse_result(job_id=job_id, status=Response.Status.SUCCESS, response=job_details)
        except Exception as e:
            self.logger.error("Error in getting job details")
            raise JobsError("Error in getting job details" + str(e))

    def resubmit_failed_tables_for_ingestion(self, job_id=None, params=None):
        """
        Resubmit the failed tables for Ingestion
        :param job_id: job id to resubmit the failed tables for ingestion
        :type job_id: String
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}
        if job_id is None:
            self.logger.error("Pass the mandatory parameter job_id for this method")
            raise JobsError("Pass the mandatory parameter job_id for this method")
        url_to_resubmit_job_for_failed_tables = url_builder.get_jobs_url(self.client_config)+ f"/{job_id}/resubmit"

        response = None
        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("POST", url_to_resubmit_job_for_failed_tables,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)

            result = response.get('result', {})
            new_job_id = result.get('id', None)
            if result.get('id', None) is None:
                self.logger.error(f'Failed to resubmit the job {job_id}')
                return GenericResponse.parse_result(status=Response.Status.FAILED,
                                                     error_desc=f'Failed to resubmit the job {job_id}',
                                                     response=response)

            self.logger.info(
                'Successfully resubmitted the job {job_id} with new job id {new_job_id}.'.format(job_id=job_id,new_job_id=new_job_id))
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, job_id=new_job_id,
                                                 response=response)

        except Exception as e:
            self.logger.error('Response from server: ' + str(response))
            self.logger.exception('Error occurred while trying to trigger workflow.')
            raise JobsError('Error occurred while trying to trigger workflow.')

    def get_job_logs_as_text_stream(self, job_id=None, params=None):
        """
        Get the job logs as text stream
        :param job_id: job id to get logs
        :type job_id: String
        :return: response dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}
        if job_id is None:
            self.logger.error("Pass the mandatory parameter job_id for this method")
            raise JobsError("Pass the mandatory parameter job_id for this method")
        url_to_get_job_logs_text = url_builder.get_jobs_url(self.client_config)+ f"/{job_id}/logs"
        response = None
        try:
            headers=IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
            headers["Content-Type"]= "application/octet-stream"

            response = self.call_api("GET", url_to_get_job_logs_text,
                              headers)
            result = response.content
            if response.status_code != 200:
                self.logger.error(f'Unable to get logs for job {job_id}')
                return {"status":Response.Status.FAILED,
                                                     "error_desc":f'Unable to get logs for the job {job_id}',
                                                     "response":result}

            self.logger.info(
                'Successfully got the logs for the job {job_id}.'.format(job_id=job_id))
            return {"status":Response.Status.SUCCESS, "job_id":job_id,
                                                 "response":result}

        except Exception as e:
            self.logger.error('Response from server: ' + str(response.content))
            self.logger.exception('Error occurred while trying to get logs for job.')
            raise JobsError('Error occurred while trying to get logs for job.')

    def get_cluster_job_details(self, job_id=None, run_id=None, params=None):
        """
        Function to get cluster job logs for iw_job using job_id
        :param job_id: job_id for the job
        :type job_id: String
        :param run_id: run_id for the job
        :type run_id: String
        :return: response dict
        """
        try:
            if params is None:
                params = {"limit": 20, "offset": 0}
            if job_id is None:
                self.logger.error("Pass the mandatory parameter job_id for this method")
                raise JobsError("Pass the mandatory parameter job_id for this method")
            url_to_get_cluster_job_details = url_builder.get_jobs_url(self.client_config) + f"/{job_id}/runs"
            if run_id:
                url_to_get_cluster_job_details=url_to_get_cluster_job_details+f"/{run_id}"
            response = None
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_to_get_cluster_job_details,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get('result', {})
            if not result:
                self.logger.error(f"Failed to get the cluster job details for {job_id}.")
                return GenericResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=response, job_id=job_id)
            else:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=result)
        except Exception as e:
            raise JobsError(f"Failed to get the cluster job details for {job_id}." + str(e))

    def get_admin_job_details(self, params=None):
        """
        Function to get the admin job details
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response list of dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}

        url_to_list_jobs = url_builder.get_admin_jobs_url(self.client_config)
        url_to_list_jobs = url_to_list_jobs + IWUtils.get_query_params_string_from_dict(params=params)
        job_details = []

        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_jobs,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                while len(result) > 0:
                    job_details.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                    response = IWUtils.ejson_deserialize(
                    self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                    self.client_config['bearer_token'])).content)
                    result = response.get("result", [])
            return GenericResponse.parse_result(job_id=None, status=Response.Status.SUCCESS, response=job_details)
        except Exception as e:
            self.logger.error("Error in getting job details")
            raise JobsError("Error in getting job details" + str(e))

    def get_all_jobs_for_source(self, source_id=None, params=None):
        """
        Function to get all jobs for a particular source
        :param source_id: entity identifier for which the jobs are to be fetched
        :type: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response list of dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}

        url_to_list_jobs = url_builder.get_source_details_url(self.client_config)
        if source_id is not None:
            url_to_list_jobs = url_to_list_jobs + f"/{source_id}/jobs"
        else:
            self.logger.error("Pass the mandatory parameter source_id for this method")
            raise JobsError("Pass the mandatory parameter source_id for this method")
        url_to_list_jobs = url_to_list_jobs + IWUtils.get_query_params_string_from_dict(params=params)
        job_details = []

        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_jobs,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                if source_id is not None:
                    job_details.extend([result])
                else:
                    while len(result) > 0:
                        job_details.extend(result)
                        nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                        response = IWUtils.ejson_deserialize(
                            self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                                self.client_config['bearer_token'])).content)
                        result = response.get("result", [])
            return GenericResponse.parse_result( status=Response.Status.SUCCESS, response=job_details)
        except Exception as e:
            self.logger.error("Error in getting job details")
            raise JobsError("Error in getting job details" + str(e))

    def submit_source_job(self, source_id=None, job_type=None, purge_metadata=None, params=None):
        """
        Function to initiate a source job for given source id
        :param source_id: source_id for the source
        :type source_id: String
        :param job_type: type of job to run on the given source
        :type job_type: String
        :param purge_metadata: purge_metadata True/False
        :type purge_metadata: Boolean
        :return: response dict
        """
        try:
            if params is None:
                params = {"limit": 20, "offset": 0}
            if source_id is None:
                self.logger.error("Pass the mandatory parameter source_id for this method")
                raise JobsError("Pass the mandatory parameter source_id for this method")
            url_to_initiate_source_job = url_builder.get_source_details_url(self.client_config)+f"/{source_id}/jobs"
            response = None
            api_payload = {}
            if job_type:
                api_payload["job_type"]=job_type
            if purge_metadata:
                api_payload["purge_metadata"] = purge_metadata
            response = IWUtils.ejson_deserialize(self.call_api("POST", url_to_initiate_source_job,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),data=api_payload
                                                               ).content)
            result = response.get('result', {})
            if not result:
                self.logger.error(f"Failed to initiate {job_type} job for source {source_id}.")
                return GenericResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=result)
        except Exception as e:
            raise JobsError(f"Failed to initiate {job_type} job for source {source_id}." + str(e))

    def get_crawl_job_summary(self, job_id=None, source_id=None,params=None):
        """
        Function to get job summary for given job_id
        :param source_id: source_id for the job
        :type source_id: String
        :param job_id: job_id for the job
        :type job_id: String
        :return: response dict
        """
        try:
            if params is None:
                params = {"limit": 20, "offset": 0}
            if job_id is None:
                self.logger.error("Pass the mandatory parameter job_id for this method")
                raise JobsError("Pass the mandatory parameter job_id for this method")
            if source_id is None:
                self.logger.error("Pass the mandatory parameter source_id for this method")
                raise JobsError("Pass the mandatory parameter source_id for this method")
            url_to_get_cluster_job_details = url_builder.get_source_details_url(self.client_config) + f"/{source_id}/jobs/{job_id}/summary"
            response = None
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_to_get_cluster_job_details,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),
                                                               ).content)
            result = response.get('result', {})
            if not result:
                self.logger.error(f"Failed to get the crawl job summary for job_id {job_id}.")
                return GenericResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=response, job_id=job_id)
            else:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=result)
        except Exception as e:
            raise JobsError(f"Failed to get the crawl job summary for job_id {job_id}." + str(e))

    def get_interactive_jobs_list(self, source_id=None, job_id=None, params=None):
        """
        Function to get all interactive jobs
        :param source_id: source_id for the interactive jobs
        :type source_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response list of dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}

        url_to_list_jobs = url_builder.get_interactive_jobs_url(self.client_config,source_id)
        if not source_id:
            self.logger.error("Pass the mandatory parameter source_id for this method")
            raise JobsError("Pass the mandatory parameter source_id for this method")
        if job_id:
            url_to_list_jobs = url_to_list_jobs + f"/{job_id}"
        url_to_list_jobs = url_to_list_jobs + IWUtils.get_query_params_string_from_dict(params=params)
        job_details = []

        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_jobs,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                if job_id is not None:
                    job_details.extend([result])
                else:
                    while len(result) > 0:
                        job_details.extend(result)
                        nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                        response = IWUtils.ejson_deserialize(
                            self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                                self.client_config['bearer_token'])).content)
                        result = response.get("result", [])
            return GenericResponse.parse_result( status=Response.Status.SUCCESS, response=job_details)
        except Exception as e:
            self.logger.error("Error in getting job details")
            raise JobsError("Error in getting job details" + str(e))

    def get_list_of_pipeline_jobs(self, domain_id=None, pipeline_id=None, params=None):
        """
        Function to get all jobs for a particular pipeline
        :param domain_id: entity identifier for domain
        :type domain_id: String
        :param pipeline_id: entity identifier for pipeline
        :type pipeline_id: String
        :param params: Pass the parameters like limit, filter, offset, sort_by, order_by as a dictionary
        :type: JSON dict
        :return: response list of dict
        """
        if params is None:
            params = {"limit": 20, "offset": 0}

        if domain_id is None or pipeline_id is None:
            self.logger.error("Domain Id or Pipeline Id cannot be None for this method")
            raise JobsError("Domain Id or Pipeline Id cannot be None for this method")
        url_to_list_jobs = url_builder.get_pipeline_jobs_url(self.client_config,domain_id=domain_id,pipeline_id=pipeline_id)
        url_to_list_jobs = url_to_list_jobs + IWUtils.get_query_params_string_from_dict(params=params)
        job_details = []

        try:
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_list_jobs,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])).content)
            if response is not None:
                result = response.get("result", [])
                while len(result) > 0:
                    job_details.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                          ip=self.client_config['ip'],
                                                                          port=self.client_config['port'],
                                                                          protocol=self.client_config['protocol'],
                                                                          )
                    response = IWUtils.ejson_deserialize(
                    self.call_api("GET", nextUrl, IWUtils.get_default_header_for_v3(
                    self.client_config['bearer_token'])).content)
                    result = response.get("result", [])
            return GenericResponse.parse_result( status=Response.Status.SUCCESS, response=job_details)
        except Exception as e:
            self.logger.error("Error in getting job details")
            raise JobsError("Error in getting job details" + str(e))

    def submit_pipeline_job(self, domain_id=None, pipeline_id=None, version_id=None, job_type=None, updated_pipeline_parameters=[], params=None):
        """
        Function to initiate a pipeline job for given pipeline id
        :param domain_id: domain_id for the pipeline
        :type domain_id: String
        :param pipeline_id: Id of the pipeline
        :type pipeline_id: String
        :param version_id: version_id of the pipeline
        :type version_id: String
        :param updated_pipeline_parameters: list of pipeline parameters(Key Value pairs)
        :type updated_pipeline_parameters: Array
        :param job_type: type of job to run on the given source
        :type job_type: String
        :return: response dict
        """
        try:
            if params is None:
                params = {"limit": 20, "offset": 0}
            if domain_id is None or pipeline_id is None:
                self.logger.error("Domain Id or Pipeline Id cannot be None for this method")
                raise JobsError("Domain Id or Pipeline Id cannot be None for this method")
            url_to_initiate_pipeline_job = url_builder.get_pipeline_jobs_url(self.client_config,domain_id=domain_id,pipeline_id=pipeline_id)
            response = None
            api_payload = {}
            if version_id:
                api_payload["version_id"]=version_id
            if job_type:
                api_payload["job_type"]=job_type
            if updated_pipeline_parameters:
                api_payload["updated_pipeline_parameters"] = updated_pipeline_parameters
            response = IWUtils.ejson_deserialize(self.call_api("POST", url_to_initiate_pipeline_job,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token']),data=api_payload
                                                               ).content)
            result = response.get('result', {})
            if not result:
                self.logger.error(f"Failed to initiate {job_type} job for pipeline {pipeline_id}.")
                return GenericResponse.parse_result(status=Response.Status.FAILED, error_code=ErrorCode.USER_ERROR,
                                                   error_desc=response)
            else:
                return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=result)
        except Exception as e:
            raise JobsError(f"Failed to initiate {job_type} job for pipeline {pipeline_id}." + str(e))

    def cancel_job(self, job_id=None, params=None):
        """
        Function to cancel an Infoworks Job
        :param job_id: job_id in Infoworks
        :type job_id: String
        :return: response dict
        """
        try:
            if params is None:
                params = {"limit": 20, "offset": 0}
            if job_id is None:
                self.logger.error("Pass the mandatory parameter job_id for this method")
                raise JobsError("Pass the mandatory parameter job_id for this method")
            url_to_cancel_job = url_builder.get_cancel_job_url(self.client_config,job_id=job_id)
            response = None
            response = IWUtils.ejson_deserialize(self.call_api("GET", url_to_cancel_job,
                                                               IWUtils.get_default_header_for_v3(
                                                                   self.client_config['bearer_token'])
                                                               ).content)
            return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
        except Exception as e:
            raise JobsError(f"Failed to cancel job for job_id {job_id}." + str(e))

