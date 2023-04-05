import json
import logging
import os
import sys
import time
import pytest

from infoworks.error import *
from infoworks.sdk.client import InfoworksClientSDK

refresh_token = ""
iwx_client = InfoworksClientSDK()
iwx_client.initialize_client_with_defaults("https", "host_name", "443", refresh_token)
cwd = os.getcwd()

pytest.environment_id = "884236e85b9b1a69b2907e4c"
pytest.domain_id = "63ce8f639e231b03aee3d11d"
pytest.workflows = {}
pytest.workflow_run_id = ""

logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s - %(module)s - %(pathname)s - %(lineno)d - %(levelname)s - %(message)s')
fh = logging.FileHandler('/tmp/sdk_workflow_test.log')
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
def test_create_workflow():
    pytest.domain_id = get_domain_id()
    try:
        workflow1_create_response = iwx_client.create_workflow(pytest.domain_id,
                                                               {"name": "workflow_api_test_1"})
        workflow2_create_response = iwx_client.create_workflow(pytest.domain_id,
                                                               {"name": "workflow_api_test_2"})
        workflow3_create_response = iwx_client.create_workflow(pytest.domain_id,
                                                               {"name": "workflow_api_test_3"})
        assert workflow1_create_response["result"]["status"].upper() == "SUCCESS" and \
               workflow2_create_response["result"]["status"].upper() == "SUCCESS" and \
               workflow3_create_response["result"]["status"].upper() == "SUCCESS"
        pytest.workflows["workflow_api_test_1"] = workflow1_create_response["result"]["workflow_id"]
        pytest.workflows["workflow_api_test_2"] = workflow2_create_response["result"]["workflow_id"]
        pytest.workflows["workflow_api_test_3"] = workflow3_create_response["result"]["workflow_id"]
    except WorkflowError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_workflow"])
def test_delete_workflow():
    try:
        workflow_get_response = iwx_client.delete_workflow(pytest.workflows["workflow_api_test_3"], pytest.domain_id)
        assert workflow_get_response["result"]["status"].upper() == "SUCCESS"
    except WorkflowError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_delete_workflow"])
def test_list_of_workflows():
    try:
        workflow_in_domain_get_response = iwx_client.get_list_of_workflows(pytest.domain_id)
        workflow_all_get_response = iwx_client.get_list_of_workflows()
        assert workflow_in_domain_get_response["result"]["status"].upper() == "SUCCESS" and \
               workflow_all_get_response["result"]["status"].upper() == "SUCCESS"
    except WorkflowError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_workflow"])
def test_get_workflow_details():
    try:
        workflow_get_response = iwx_client.get_workflow_details(pytest.workflows["workflow_api_test_1"],
                                                                pytest.domain_id)
        assert workflow_get_response["result"]["status"].upper() == "SUCCESS"
    except WorkflowError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_workflow"])
def test_get_workflow_id():
    try:
        workflow_get_response = iwx_client.get_workflow_id("workflow_api_test_1", pytest.domain_id)
        assert workflow_get_response["result"]["status"].upper() == "SUCCESS"
    except WorkflowError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_workflow"])
def test_get_workflow_name():
    try:
        workflow_get_response = iwx_client.get_workflow_name(pytest.domain_id, pytest.workflows["workflow_api_test_1"])
        assert workflow_get_response["result"]["status"].upper() == "SUCCESS"
    except WorkflowError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_get_workflow_details"])
def test_import_workflow_configuration_json():
    try:
        with open(f"{cwd}/config_jsons/workflow_configurations.json", "r") as f:
            workflow_config = json.load(f)
        put_workflow_json_response = iwx_client.update_workflow_configuration_json_import(
            pytest.workflows["workflow_api_test_1"],
            pytest.domain_id,
            workflow_config)
        assert put_workflow_json_response["result"]["status"].upper() == "SUCCESS"
    except WorkflowError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_import_workflow_configuration_json"])
def test_export_workflow_configuration_json():
    try:
        workflow_get_json_response = iwx_client.get_workflow_configuration_json_export(
            pytest.workflows["workflow_api_test_1"],
            pytest.domain_id)
        print("workflow_get_json_response", workflow_get_json_response)
        if workflow_get_json_response["result"]["status"].upper() == "SUCCESS":
            with open(f"{cwd}/config_jsons/workflow_export_configurations.json", "w") as f:
                json.dump(workflow_get_json_response["result"]["response"]["result"], f)
        assert workflow_get_json_response["result"]["status"].upper() == "SUCCESS"
    except WorkflowError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_export_workflow_configuration_json"])
def test_update_workflow():  # Fails
    try:
        workflow_update_response = iwx_client.update_workflow(pytest.workflows["workflow_api_test_2"], pytest.domain_id,
                                                              {
                                                                  "name": "workflow_api_unit_test_updated"
                                                              })
        print(workflow_update_response)
        assert workflow_update_response["result"]["status"].upper() == "SUCCESS"
    except WorkflowError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_export_workflow_configuration_json"])
def test_trigger_workflow():
    try:
        workflow_get_response = iwx_client.trigger_workflow(pytest.workflows["workflow_api_test_1"], pytest.domain_id)
        pytest.workflow_run_id = workflow_get_response["result"]["response"].get('result', {}).get('id', None)
        if pytest.workflow_run_id is None:
            assert False
        else:
            assert workflow_get_response["result"]["status"].upper() == "SUCCESS"
    except WorkflowError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_trigger_workflow"])
def test_get_status_workflow():
    try:
        workflow_run_status_response = iwx_client.get_status_of_workflow(pytest.workflow_run_id,
                                                                         pytest.workflows["workflow_api_test_1"])
        assert workflow_run_status_response["result"]["status"].upper() == "SUCCESS"
    except WorkflowError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_get_status_workflow"])
def test_get_list_of_workflow_runs():
    try:
        workflow_run_status_response = iwx_client.get_list_of_workflow_runs(pytest.domain_id,
                                                                            pytest.workflows["workflow_api_test_1"])
        workflow_runs_all_status_response = iwx_client.get_list_of_workflow_runs()
        assert workflow_run_status_response["result"]["status"].upper() == "SUCCESS" and \
               workflow_runs_all_status_response["result"]["status"].upper() == "SUCCESS"
    except WorkflowError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_get_status_workflow"])
def test_get_list_of_workflow_runs_jobs():
    try:
        workflow_run_status_response = iwx_client.get_list_of_workflow_runs_jobs(pytest.workflow_run_id)
        assert workflow_run_status_response["result"]["status"].upper() == "SUCCESS"
    except WorkflowError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_get_status_workflow"])
def test_poll_workflow():
    try:
        workflow_run_status_response = iwx_client.poll_workflow_run_till_completion(pytest.workflow_run_id,
                                                                                    pytest.workflows[
                                                                                        "workflow_api_test_1"])
        assert workflow_run_status_response["result"]["status"].upper() == "SUCCESS"
    except WorkflowError as e:
        print(str(e))
        assert False


# @pytest.mark.dependency(depends=["test_poll_workflow"])
def test_cancel_restart_multiple_workflow():
    # Restart Multiple Workflows (This operation is only for operations_analyst user)
    try:
        workflow_get_response = iwx_client.trigger_workflow("63d1297548ef1c73e0fa875b", pytest.domain_id)
        pytest.workflow_run_id = workflow_get_response["result"]["response"].get('result', {}).get('id', None)
        workflow_cancel_response = iwx_client.cancel_multiple_workflow(
            {"ids": [{"workflow_id": "63d1297548ef1c73e0fa875b", "run_id": pytest.workflow_run_id}]})
        time.sleep(10)
        workflow_restart_response = iwx_client.restart_multiple_workflow({"ids": [{"workflow_id": "63d1297548ef1c73e0fa875b", "run_id": pytest.workflow_run_id}]})
        assert workflow_cancel_response["result"]["status"].upper() == "SUCCESS" and workflow_restart_response["result"]["status"].upper() == "SUCCESS"
    except WorkflowError as e:
        print(str(e))
        assert False
