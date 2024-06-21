import logging
import os
import pytest
import sys
#sys.path.insert(0,"/Users/nitin.bs/PycharmProjects/infoworks-python-sdk")
from infoworks.error import *
from infoworks.sdk.client import InfoworksClientSDK

refresh_token = ""
iwx_client = InfoworksClientSDK()
iwx_client.initialize_client_with_defaults("https", "", "443", refresh_token)
cwd = os.getcwd()

pytest.environment_id = "662f30700730090008d92fc5"
pytest.domain_id = "6628e2eebfaf2d06816b6fdd"
pytest.pipelines = ["66585c71adc7773251932ba8", "66585c72adc7773251932bae"]
pytest.pipeline_groups = {"pipeline_group_api_unit_test_1":"642d8abdc2c96c5a634ae02b","pipeline_group_api_unit_test_2":"642d8abdc2c96c5a634ae02d"}
pytest.job_id = "642d97a7c2c96c5a634ae030"

logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s - %(module)s - %(pathname)s - %(lineno)d - %(levelname)s - %(message)s')
fh = logging.FileHandler('/tmp/sdk_pipeline_group.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)


class TestPipelineGroupFlow():
    def get_domain_id(self):
        response = iwx_client.get_domain_id("CE_Domain")
        domain_id = response["result"]["response"].get("domain_id", "")
        if domain_id == "":
            print("Unable to get the domain iwx_sdk_domain1")
            sys.exit(0)
        return domain_id


    @pytest.mark.dependency()
    def test_create_pipeline_group(self):
        pytest.domain_id = self.get_domain_id()
        try:
            pipeline_group_config_example_1 = {
                "name": "pipeline_group_api_unit_test_1",
                "description": "Created by SDK",
                "environment_id": pytest.environment_id,
                "domain_id": pytest.domain_id,
                "snowflake_profile":"Default",
                "batch_engine": "SNOWFLAKE",
                "run_job_on_data_plane": False,
                "pipelines": [
                    {
                        "pipeline_id": pytest.pipelines[0],
                        "version": 2,
                        "execution_order": 1,
                        "run_active_version": False
                    },
                    {
                        "pipeline_id": pytest.pipelines[1],
                        "version": 2,
                        "execution_order": 2,
                        "run_active_version": True
                    }
                ],
                "custom_tags": [],
                "query_tag": "",
                "snowflake_warehouse": "DEMO_WH"
            }
            pipeline_group_config_example_2 = {
                "name": "pipeline_group_api_unit_test_2",
                "description": "Created by SDK",
                "environment_id": pytest.environment_id,
                "domain_id": pytest.domain_id,
                "snowflake_profile": "Default",
                "batch_engine": "SNOWFLAKE",
                "run_job_on_data_plane": False,
                "pipelines": [
                    {
                        "pipeline_id": pytest.pipelines[0],
                        "version": 1,
                        "execution_order": 1,
                        "run_active_version": False
                    },
                    {
                        "pipeline_id": pytest.pipelines[1],
                        "version": 1,
                        "execution_order": 2,
                        "run_active_version": True
                    }
                ],
                "custom_tags": [],
                "query_tag": "",
                "snowflake_warehouse": "DEMO_WH"
            }
            response_1 = iwx_client.create_pipeline_group(pipeline_group_config=pipeline_group_config_example_1)
            response_2 = iwx_client.create_pipeline_group(pipeline_group_config=pipeline_group_config_example_2)
            print("response_1:",response_1)
            print("response_2:",response_2)
            assert response_1["result"]["status"].upper() == "SUCCESS" and response_2["result"][
                "status"].upper() == "SUCCESS"
            pytest.pipeline_groups["pipeline_group_api_unit_test_1"] = response_1["result"]["entity_id"]
            pytest.pipeline_groups["pipeline_group_api_unit_test_2"] = response_2["result"]["entity_id"]
        except PipelineError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestPipelineGroupFlow::test_create_pipeline_group"])
    def test_list_pipeline_groups_under_domain(self):
        try:
            response = iwx_client.list_pipeline_groups_under_domain(domain_id=pytest.domain_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except PipelineError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestPipelineGroupFlow::test_create_pipeline_group"])
    def test_get_pipeline_group_details(self):
        try:
            response = iwx_client.get_pipeline_group_details(domain_id=pytest.domain_id,
                                                             pipeline_group_id=pytest.pipeline_groups[
                                                                 "pipeline_group_api_unit_test_1"])
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except PipelineError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestPipelineGroupFlow::test_create_pipeline_group"])
    def test_delete_pipeline_group(self):
        try:
            response = iwx_client.delete_pipeline_group(domain_id=pytest.domain_id,
                                                        pipeline_group_id=pytest.pipeline_groups[
                                                            "pipeline_group_api_unit_test_2"])
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except PipelineError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestPipelineGroupFlow::test_delete_pipeline_group"])
    def test_update_pipeline_group(self):
        try:
            pipeline_group_config_example_1 = {
                "name": "pipeline_group_api_unit_test_1_updated",
                "description": "Created by SDK",
                "environment_id": pytest.environment_id,
                "domain_id": pytest.domain_id,
                "batch_engine": "SNOWFLAKE",
                "run_job_on_data_plane": False,
                "pipelines": [
                    {
                        "pipeline_id": pytest.pipelines[0],
                        "version": 1,
                        "execution_order": 1,
                        "run_active_version": False
                    },
                    {
                        "pipeline_id": pytest.pipelines[1],
                        "version": 1,
                        "execution_order": 2,
                        "run_active_version": True
                    }
                ],
                "custom_tags": [],
                "query_tag": "",
                "snowflake_warehouse": "DEMO_WH"
            }
            response_1 = iwx_client.update_pipeline_group(pytest.domain_id,
                                                          pytest.pipeline_groups["pipeline_group_api_unit_test_1"],
                                                          pipeline_group_config=pipeline_group_config_example_1)
            print(response_1)
            assert response_1["result"]["status"].upper() == "SUCCESS"
        except PipelineError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestPipelineGroupFlow::test_create_pipeline_group"])
    def test_get_accessible_pipeline_groups(self):
        try:
            response = iwx_client.get_accessible_pipeline_groups(domain_id=pytest.domain_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except PipelineError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestPipelineGroupFlow::test_update_pipeline_group"])
    def test_modify_advanced_config_for_pipeline_group(self):
        try:
            adv_config_body_example = {
                "key": "abc",
                "value": "test",
                "is_active": True
            }
            adv_config_body_update = {
                "key": "abc",
                "value": "test_updated",
                "is_active": True
            }
            response_create = iwx_client.modify_advanced_config_for_pipeline_group(pytest.domain_id, pytest.pipeline_groups[
                "pipeline_group_api_unit_test_1"], adv_config_body_example,
                                                                                   action_type="create")

            assert response_create["result"]["status"].upper() == "SUCCESS"
            response_update = iwx_client.modify_advanced_config_for_pipeline_group(pytest.domain_id, pytest.pipeline_groups[
                "pipeline_group_api_unit_test_1"], adv_config_body_update, action_type="update", key="abc")
            assert response_update["result"]["status"].upper() == "SUCCESS"
        except PipelineError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestPipelineGroupFlow::test_modify_advanced_config_for_pipeline_group"])
    def test_get_list_of_advanced_config_of_pipeline_groups(self):
        try:
            response = iwx_client.get_list_of_advanced_config_of_pipeline_groups(domain_id=pytest.domain_id,
                                                                                 pipeline_group_id=pytest.pipeline_groups[
                                                                                     "pipeline_group_api_unit_test_1"])
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except PipelineError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestPipelineGroupFlow::test_get_list_of_advanced_config_of_pipeline_groups"])
    def test_get_or_delete_advance_config_details_of_pipeline_group(self):
        try:
            response_get = iwx_client.get_or_delete_advance_config_details_of_pipeline_group(domain_id=pytest.domain_id,
                                                                                             pipeline_group_id=
                                                                                             pytest.pipeline_groups[
                                                                                                 "pipeline_group_api_unit_test_1"],
                                                                                             key="abc", action_type="get")
            assert response_get["result"]["status"].upper() == "SUCCESS"
            response_delete = iwx_client.get_or_delete_advance_config_details_of_pipeline_group(domain_id=pytest.domain_id,
                                                                                                pipeline_group_id=
                                                                                                pytest.pipeline_groups[
                                                                                                    "pipeline_group_api_unit_test_1"],
                                                                                                key="abc",
                                                                                                action_type="delete")
            assert response_delete["result"]["status"].upper() == "SUCCESS"
        except PipelineError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestPipelineGroupFlow::test_get_or_delete_advance_config_details_of_pipeline_group"])
    def test_trigger_pipeline_group_build(self):
        try:
            response = iwx_client.trigger_pipeline_group_build(domain_id=pytest.domain_id,
                                                               pipeline_group_id=pytest.pipeline_groups[
                                                                   "pipeline_group_api_unit_test_1"])
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
            pytest.job_id = response["result"]["job_id"]
        except PipelineError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestPipelineGroupFlow::test_trigger_pipeline_group_build"])
    def test_get_pipeline_group_job_details(self):
        try:
            response = iwx_client.get_pipeline_group_job_details(domain_id=pytest.domain_id,
                                                                 pipeline_group_id=pytest.pipeline_groups[
                                                                     "pipeline_group_api_unit_test_1"],
                                                                 job_id=pytest.job_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except PipelineError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestPipelineGroupFlow::test_trigger_pipeline_group_build"])
    def test_list_pipeline_job_details_in_pipeline_group_job(self):
        try:
            response = iwx_client.list_pipeline_job_details_in_pipeline_group_job(domain_id=pytest.domain_id,
                                                                                  pipeline_group_id=pytest.pipeline_groups[
                                                                                      "pipeline_group_api_unit_test_1"],
                                                                                  job_id=pytest.job_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except PipelineError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestPipelineGroupFlow::test_trigger_pipeline_group_build"])
    def test_get_pipeline_group_job_log_as_text(self):
        try:
            response = iwx_client.get_pipeline_group_job_log_as_text(domain_id=pytest.domain_id,
                                                                     pipeline_group_id=pytest.pipeline_groups[
                                                                         "pipeline_group_api_unit_test_1"],
                                                                     job_id=pytest.job_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except PipelineError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestPipelineGroupFlow::test_trigger_pipeline_group_build"])
    def test_list_cluster_job_runs_in_pipeline_group_job(self):
        try:
            response = iwx_client.list_cluster_job_runs_in_pipeline_group_job(domain_id=pytest.domain_id,
                                                                              pipeline_group_id=pytest.pipeline_groups[
                                                                                  "pipeline_group_api_unit_test_1"],
                                                                              job_id=pytest.job_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except PipelineError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestPipelineGroupFlow::test_trigger_pipeline_group_build"])
    def test_list_jobs_under_pipeline_group(self):
        try:
            response = iwx_client.list_jobs_under_pipeline_group(domain_id=pytest.domain_id,
                                                                 pipeline_group_id=pytest.pipeline_groups[
                                                                     "pipeline_group_api_unit_test_1"],
                                                                 )
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except PipelineError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestPipelineGroupFlow::test_trigger_pipeline_group_build"])
    def test_get_pipeline_group_job_summary(self):
        try:
            response = iwx_client.get_pipeline_group_job_summary(domain_id=pytest.domain_id,
                                                                 pipeline_group_id=pytest.pipeline_groups[
                                                                     "pipeline_group_api_unit_test_1"],
                                                                 job_id=pytest.job_id
                                                                 )
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except PipelineError as e:
            print(str(e))
            assert False