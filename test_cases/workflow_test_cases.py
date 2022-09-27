from infoworks.sdk.client import InfoworksClientSDK
from infoworks.error import *
from test_cases.conftest import ValueStorage
import json
import os
import uuid

refresh_token = "zThziQ7MoJJPYAha+U/+PBSTZG944F+SHBDs+m/z2qn8+m/ax8Prpzla1MHzQ5EBLzB2Bw8a+Qs9r6En5BEN2DsmUVJ6sKFb2yI2"
iwx_client = InfoworksClientSDK()
iwx_client.initialize_client_with_defaults("http", "10.18.1.28", "3001", refresh_token)
uuid_val = uuid.uuid4()
cwd = os.getcwd()


def test_create_workflow():
    try:
        workflow_create_response = iwx_client.create_workflow(ValueStorage.domain_id,
                                                              {"name": "workflow_unit_test_" + str(uuid_val)})
        assert workflow_create_response["result"]["status"].upper() == "SUCCESS"
        # ValueStorage.workflow_id = workflow_create_response["result"]["workflow_id"]
    except WorkflowError as e:
        print(str(e))
        assert False


def test_get_workflow():
    try:
        workflow_get_response = iwx_client.get_workflow_details(ValueStorage.workflow_id, ValueStorage.domain_id)
        assert workflow_get_response["result"]["status"].upper() == "SUCCESS"
    except WorkflowError as e:
        print(str(e))
        assert False


def test_update_workflow():  # Fails
    try:
        workflow_update_response = iwx_client.update_workflow(ValueStorage.workflow_id, ValueStorage.domain_id, {
            "name": "workflow_api_unit_test_updated"
        })
        print(workflow_update_response)
        assert workflow_update_response["result"]["status"].upper() == "SUCCESS"
    except WorkflowError as e:
        print(str(e))
        assert False


def test_get_workflow_configuration_json():
    try:
        workflow_get_json_response = iwx_client.get_workflow_configuration_json_export(ValueStorage.workflow_id,
                                                                                       ValueStorage.domain_id)
        print("workflow_get_json_response", workflow_get_json_response)
        if workflow_get_json_response["result"]["status"].upper() == "SUCCESS":
            with open(f"{cwd}/test_cases/config_jsons/workflow_configurations.json", "w") as f:
                json.dump(workflow_get_json_response["result"]["response"]["result"], f)
        assert workflow_get_json_response["result"]["status"].upper() == "SUCCESS"

    except WorkflowError as e:
        print(str(e))
        assert False


def test_put_workflow_configuration_json():
    try:
        with open(f"{cwd}/test_cases/config_jsons/workflow_configurations.json", "r") as f:
            workflow_config = json.load(f)
        put_workflow_json_response = iwx_client.update_workflow_configuration_json_export(ValueStorage.workflow_id,
                                                                                          ValueStorage.domain_id,
                                                                                          workflow_config)
        assert put_workflow_json_response["result"]["status"].upper() == "SUCCESS"
    except WorkflowError as e:
        print(str(e))
        assert False
