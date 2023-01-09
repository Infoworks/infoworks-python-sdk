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
