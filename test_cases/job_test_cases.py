# Before running this test suite ensure to have a valid source_id,domain_id,pipeline_id,failed_tables_ingestion_job_id
# in ValueStorage of config.ini
import configparser
import pytest
from infoworks.sdk.client import InfoworksClientSDK
from infoworks.error import *
from test_cases.conftest import ValueStorage

config = configparser.ConfigParser()
config.read('config.ini')
refresh_token = ""
iwx_client = InfoworksClientSDK()
iwx_client.initialize_client_with_defaults("https", "aks-qa-600.infoworks.technology", "443", refresh_token)


class TestJobsSDKFlow:

    @pytest.mark.dependency()
    def test_get_all_jobs_for_source(self):
        try:
            response = iwx_client.get_all_jobs_for_source(source_id=ValueStorage.source_id,
                                                          params={"filter": {"jobType": {
                                                              "$in": ["source_crawl", "source_structured_cdc_merge",
                                                                      "source_structured_crawl"]}}})
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
            if len(response["result"]["response"]["result"]) > 0:
                job_0 = response["result"]["response"]["result"][0]
                pytest.job_id = job_0["id"]
            else:
                pytest.job_id = ValueStorage.job_id
        except JobsError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_get_admin_job_details(self):
        try:
            response = iwx_client.get_admin_job_details()
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except JobsError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestJobsSDKFlow::test_get_all_jobs_for_source"])
    def test_get_job_details(self):
        try:
            response = iwx_client.get_job_details(job_id=pytest.job_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except JobsError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_get_interactive_jobs_list(self):
        try:
            response = iwx_client.get_interactive_jobs_list(source_id=ValueStorage.source_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except JobsError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestJobsSDKFlow::test_get_all_jobs_for_source"])
    def test_get_cluster_job_details(self):
        try:
            response = iwx_client.get_cluster_job_details(job_id=pytest.job_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except JobsError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestJobsSDKFlow::test_get_all_jobs_for_source"])
    def test_get_source_crawl_job_summary(self):
        try:
            response = iwx_client.get_source_crawl_job_summary(job_id=pytest.job_id, source_id=ValueStorage.source_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except JobsError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_get_list_of_pipeline_jobs(self):
        try:
            response = iwx_client.get_list_of_pipeline_jobs(domain_id=ValueStorage.domain_id,
                                                            pipeline_id=ValueStorage.pipeline_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except JobsError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_submit_pipeline_job(self):
        try:
            response = iwx_client.submit_pipeline_job(domain_id=ValueStorage.domain_id,
                                                      pipeline_id=ValueStorage.pipeline_id,
                                                      job_type="pipeline_metadata")
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
            pytest.pipeline_metadata_job_id = response["result"]["response"]["result"]["id"]
        except JobsError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestJobsSDKFlow::test_submit_pipeline_job"])
    def test_cancel_job(self):
        try:
            response = iwx_client.cancel_job(job_id=pytest.pipeline_metadata_job_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except JobsError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_resubmit_failed_tables_for_ingestion(self):
        try:
            response = iwx_client.resubmit_failed_tables_for_ingestion(
                job_id=ValueStorage.failed_tables_ingestion_job_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except JobsError as e:
            print(str(e))
            assert False
