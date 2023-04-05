import logging
import os
import sys
import pytest
from infoworks.error import *
from infoworks.sdk.client import InfoworksClientSDK

refresh_token = "zThziQ7MoJJPYAha+U/+PBSTZG944F+SHBDs+m/z2qn8+m/ax8Prpzla1MHzQ5EBLzB2Bw8a+Qs9r6En5BEN2DsmUVJ6sKFb2yI2"
iwx_client = InfoworksClientSDK()
iwx_client.initialize_client_with_defaults("https", "att-iwx-pri.infoworks.technology", "443", refresh_token)
cwd = os.getcwd()

pytest.environment_id = "a801283f2e8077120d000b49"
pytest.domain_id = "f1b68b17e40c11b07fc62128"
pytest.pipelines = ["a058a234b55d6b47493ed4f1", "ec35b790067afc400b277438"]
pytest.pipeline_groups = {"pipeline_group_api_unit_test_1":"642d8abdc2c96c5a634ae02b","pipeline_group_api_unit_test_2":"642d8abdc2c96c5a634ae02d"}
pytest.job_id = "642d97a7c2c96c5a634ae030"

logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s - %(module)s - %(pathname)s - %(lineno)d - %(levelname)s - %(message)s')
fh = logging.FileHandler('/tmp/sdk_pipeline_group.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)


def get_domain_id():
    response = iwx_client.get_domain_id("iwx_sdk_domain1")
    domain_id = response["result"]["response"].get("domain_id", "")
    if domain_id == "":
        print("Unable to get the domain iwx_sdk_domain1")
        sys.exit(0)
    return domain_id


@pytest.mark.dependency()
def test_create_pipeline_group():
    pytest.domain_id = get_domain_id()
    try:
        pipeline_group_config_example_1 = {
            "name": "pipeline_group_api_unit_test_1",
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
        pipeline_group_config_example_2 = {
            "name": "pipeline_group_api_unit_test_2",
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
        response_1 = iwx_client.create_pipeline_group(pipeline_group_config=pipeline_group_config_example_1)
        response_2 = iwx_client.create_pipeline_group(pipeline_group_config=pipeline_group_config_example_2)
        assert response_1["result"]["status"].upper() == "SUCCESS" and response_2["result"][
            "status"].upper() == "SUCCESS"
        pytest.pipeline_groups["pipeline_group_api_unit_test_1"] = response_1["result"]["entity_id"]
        pytest.pipeline_groups["pipeline_group_api_unit_test_2"] = response_2["result"]["entity_id"]
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_pipeline_group"])
def test_list_pipeline_groups_under_domain():
    try:
        response = iwx_client.list_pipeline_groups_under_domain(domain_id=pytest.domain_id)
        print(response)
        assert response["result"]["status"].upper() == "SUCCESS"
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_pipeline_group"])
def test_get_pipeline_group_details():
    try:
        response = iwx_client.get_pipeline_group_details(domain_id=pytest.domain_id,
                                                         pipeline_group_id=pytest.pipeline_groups[
                                                             "pipeline_group_api_unit_test_1"])
        print(response)
        assert response["result"]["status"].upper() == "SUCCESS"
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_pipeline_group"])
def test_delete_pipeline_group():
    try:
        response = iwx_client.delete_pipeline_group(domain_id=pytest.domain_id,
                                                    pipeline_group_id=pytest.pipeline_groups[
                                                        "pipeline_group_api_unit_test_2"])
        print(response)
        assert response["result"]["status"].upper() == "SUCCESS"
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_delete_pipeline_group"])
def test_update_pipeline_group():
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


@pytest.mark.dependency(depends=["test_create_pipeline_group"])
def test_get_accessible_pipeline_groups():
    try:
        response = iwx_client.get_accessible_pipeline_groups(domain_id=pytest.domain_id)
        print(response)
        assert response["result"]["status"].upper() == "SUCCESS"
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_update_pipeline_group"])
def test_modify_advanced_config_for_pipeline_group():
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


@pytest.mark.dependency(depends=["test_modify_advanced_config_for_pipeline_group"])
def test_get_list_of_advanced_config_of_pipeline_groups():
    try:
        response = iwx_client.get_list_of_advanced_config_of_pipeline_groups(domain_id=pytest.domain_id,
                                                                             pipeline_group_id=pytest.pipeline_groups[
                                                                                 "pipeline_group_api_unit_test_1"])
        print(response)
        assert response["result"]["status"].upper() == "SUCCESS"
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_get_list_of_advanced_config_of_pipeline_groups"])
def test_get_or_delete_advance_config_details_of_pipeline_group():
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


@pytest.mark.dependency(depends=["test_get_or_delete_advance_config_details_of_pipeline_group"])
def test_trigger_pipeline_group_build():
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


@pytest.mark.dependency(depends=["test_trigger_pipeline_group_build"])
def test_get_pipeline_group_job_details():
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


# FAILS. Bug in API
# @pytest.mark.dependency(depends=["test_trigger_pipeline_group_build"])
# def test_list_pipeline_job_details_in_pipeline_group_job():
#     try:
#         response = iwx_client.list_pipeline_job_details_in_pipeline_group_job(domain_id=pytest.domain_id,
#                                                                               pipeline_group_id=pytest.pipeline_groups[
#                                                                                   "pipeline_group_api_unit_test_1"],
#                                                                               job_id=pytest.job_id)
#         print(response)
#         assert response["result"]["status"].upper() == "SUCCESS"
#     except PipelineError as e:
#         print(str(e))
#         assert False


@pytest.mark.dependency(depends=["test_trigger_pipeline_group_build"])
def test_get_pipeline_group_job_log_as_text():
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


@pytest.mark.dependency(depends=["test_trigger_pipeline_group_build"])
def test_list_cluster_job_runs_in_pipeline_group_job():
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


@pytest.mark.dependency(depends=["test_trigger_pipeline_group_build"])
def test_list_jobs_under_pipeline_group():
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
