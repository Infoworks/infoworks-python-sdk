import datetime
import json
import logging
import os
import time
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
pytest.workflows = {}
pytest.workflow_run_id = ""
def configure_logger():
    """
    Wrapper function to configure a logger to write to console and file.
    """
    file_console_logger = logging.getLogger('IWXFileWatcher')
    file_console_logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s — [ %(levelname)s ] - %(message)s')
    console_handler.setFormatter(formatter)
    file_console_logger.addHandler(console_handler)
    return file_console_logger
logger = configure_logger()

class TestWorkflowFlow():
    def get_domain_id(self):
        response = iwx_client.get_domain_id("CE_Domain")
        domain_id = response["result"]["response"].get("domain_id", "")
        if domain_id == "":
            print("Unable to get the domain CE_Domain")
            sys.exit(0)
        return domain_id

    @pytest.mark.dependency()
    def test_create_workflow(self):
        pytest.domain_id = self.get_domain_id()
        try:
            workflow1_create_response = iwx_client.create_workflow(pytest.domain_id,
                                                                   {"name": "workflow_api_test_1"})
            print("workflow1_create_response:",workflow1_create_response)
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

    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_create_workflow"])
    def test_update_workflow_schedule_user(self):
        try:
            update_workflow1_user_response = iwx_client.update_workflow_schedule_user(pytest.domain_id,
                                                                                      pytest.workflows[
                                                                                          "workflow_api_test_1"])
            update_workflow2_user_response = iwx_client.update_workflow_schedule_user(pytest.domain_id,
                                                                                      pytest.workflows[
                                                                                          "workflow_api_test_2"])
            update_workflow3_user_response = iwx_client.update_workflow_schedule_user(pytest.domain_id,
                                                                                      pytest.workflows[
                                                                                          "workflow_api_test_3"])
            assert update_workflow1_user_response["result"].get("response", {}).get("result") is not None and \
                   update_workflow2_user_response["result"].get("response", {}).get("result") is not None and \
                   update_workflow3_user_response["result"].get("response", {}).get("result") is not None
        except WorkflowError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_create_workflow"])
    def test_create_workflow_version(self):
        try:
            workflow_id = pytest.workflows["workflow_api_test_1"]
            workflow_version_config = {
                "description": "Test version",
                "is_active": True,
                "workflow_graph": {
                    "tasks": [
                        {
                            "is_group": False,
                            "task_type": "bash_command_run",
                            "task_id": "BS_3UWO",
                            "location": "-175 -155",
                            "title": "Bash Script",
                            "description": "",
                            "task_properties": {
                                "is_script_uploaded": False,
                                "bash_command": "echo \"Hello Workflow\""
                            },
                            "run_properties": {
                                "trigger_rule": "all_success",
                                "num_retries": 0
                            }
                        }
                    ],
                    "edges": []
                }
            }
            response = iwx_client.create_workflow_version(
                domain_id=pytest.domain_id,
                workflow_id=workflow_id,
                workflow_version_config=workflow_version_config
            )
            assert response["result"]["status"].upper() == "SUCCESS"
            pytest.workflow_version_id = response["result"]["workflow_version_id"]
            workflow_id = pytest.workflows["workflow_api_test_1"]
            workflow_version_config['is_active'] = False
            workflow_version_config['description'] = 'version for delete test'
            response = iwx_client.create_workflow_version(
                domain_id=pytest.domain_id,
                workflow_id=workflow_id,
                workflow_version_config=workflow_version_config
            )
            assert response["result"]["status"].upper() == "SUCCESS"
            pytest.workflow_version_id_for_delete = response["result"]["workflow_version_id"]
            assert response["result"]["status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_create_workflow_version"])
    def test_update_workflow_version(self):
        try:
            workflow_id = pytest.workflows["workflow_api_test_1"]
            workflow_version_id = pytest.workflow_version_id
            workflow_version_config = {
                "description": "Updated version",
                "is_active": True
            }
            response = iwx_client.update_workflow_version(
                workflow_version_id=workflow_version_id,
                workflow_id=workflow_id,
                domain_id=pytest.domain_id,
                workflow_version_config=workflow_version_config
            )
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_create_workflow_version"])
    def test_get_list_of_workflow_versions(self):
        try:
            workflow_id = pytest.workflows["workflow_api_test_1"]
            workflow_versions_in_workflow_get_response = iwx_client.get_list_of_workflow_versions(
                domain_id=pytest.domain_id,
                workflow_id=workflow_id
            )
            workflow_versions_all_get_response = iwx_client.get_list_of_workflow_versions()
            assert workflow_versions_in_workflow_get_response["result"]["status"].upper() == "SUCCESS" and \
                   workflow_versions_all_get_response["result"]["status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_create_workflow_version"])
    def test_delete_workflow_version(self):
        try:
            workflow_id = pytest.workflows["workflow_api_test_1"]
            workflow_version_id = pytest.workflow_version_id_for_delete
            response = iwx_client.delete_workflow_version(
                workflow_version_id=workflow_version_id,
                workflow_id=workflow_id,
                domain_id=pytest.domain_id
            )
            assert response["result"]["status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_update_workflow_schedule_user"])
    def test_enable_workflow_schedule(self):
        try:
            current_date = datetime.datetime.today()
            end_date = current_date + datetime.timedelta(days=1)
            current_date = current_date.strftime("%m/%d/%Y")
            end_date = end_date.strftime("%m/%d/%Y")
            schedule_config = {
                "start_date": current_date,
                "end_date": end_date,
                "start_hour": 10,
                "start_min": 0,
                "end_hour": 23,
                "end_min": 59,
                "repeat_interval_measure": 1,
                "repeat_interval_unit": "DAY",
                "ends": True,
                "is_custom_job": False,
                "repeat_on_last_day": False,
                "specified_days": [
                    8
                ]
            }
            print(pytest.domain_id, pytest.workflows["workflow_api_test_1"], pytest.workflows["workflow_api_test_2"])
            enable_workflow_1 = iwx_client.enable_workflow_schedule(
                pytest.domain_id, pytest.workflows["workflow_api_test_1"], schedule_config)
            print(enable_workflow_1)
            enable_workflow_2 = iwx_client.enable_workflow_schedule(
                pytest.domain_id, pytest.workflows["workflow_api_test_2"], schedule_config)
            print(enable_workflow_2)
            assert enable_workflow_1.get("result", {}).get("response", {}).get("result") is not None and \
                   enable_workflow_2.get("result", {}).get("response", {}).get("result") is not None
        except WorkflowError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_enable_workflow_schedule"])
    def test_get_list_of_domain_workflow_schedules(self):
        try:
            workflow_get_response = iwx_client.get_list_of_domain_workflow_schedules(pytest.domain_id)
            assert (workflow_get_response["result"].get("response", {}).get("result")) is not None
        except WorkflowError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_enable_workflow_schedule"])
    def test_get_workflow_schedule(self):
        try:
            workflow_get_response = iwx_client.get_workflow_schedule(pytest.domain_id,
                                                                     pytest.workflows["workflow_api_test_1"])
            assert (workflow_get_response["result"].get("response", {}).get("result")) is not None
        except WorkflowError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_get_list_of_domain_workflow_schedules","TestWorkflowFlow::test_get_workflow_schedule"])
    def test_disable_workflow_schedule(self):
        try:
            workflow_get_response = iwx_client.disable_workflow_schedule(pytest.domain_id,
                                                                         pytest.workflows["workflow_api_test_1"])
            assert workflow_get_response["result"].get("response", {}).get("result") is not None
        except WorkflowError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_create_workflow","TestWorkflowFlow::test_disable_workflow_schedule"])
    def test_delete_workflow(self):
        try:
            workflow_get_response = iwx_client.delete_workflow(pytest.workflows["workflow_api_test_3"], pytest.domain_id)
            assert workflow_get_response["result"]["status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_delete_workflow"])
    def test_list_of_workflows(self):
        try:
            workflow_in_domain_get_response = iwx_client.get_list_of_workflows(pytest.domain_id)
            workflow_all_get_response = iwx_client.get_list_of_workflows()
            assert workflow_in_domain_get_response["result"]["status"].upper() == "SUCCESS" and \
                   workflow_all_get_response["result"]["status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_create_workflow"])
    def test_get_workflow_details(self):
        try:
            workflow_get_response = iwx_client.get_workflow_details(pytest.workflows["workflow_api_test_1"],
                                                                    pytest.domain_id)
            assert workflow_get_response["result"]["status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_create_workflow"])
    def test_get_workflow_id(self):
        try:
            workflow_get_response = iwx_client.get_workflow_id("workflow_api_test_1", pytest.domain_id)
            assert workflow_get_response["result"]["status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_create_workflow"])
    def test_get_workflow_name(self):
        try:
            workflow_get_response = iwx_client.get_workflow_name(pytest.domain_id, pytest.workflows["workflow_api_test_1"])
            assert workflow_get_response["result"]["status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_get_workflow_details"])
    def test_import_workflow_configuration_json(self):
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


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_import_workflow_configuration_json"])
    def test_export_workflow_configuration_json(self):
        try:
            workflow_get_json_response = iwx_client.get_workflow_configuration_json_export(
                workflow_id=pytest.workflows["workflow_api_test_1"],
                domain_id=pytest.domain_id)
            print("workflow_get_json_response", workflow_get_json_response)
            if workflow_get_json_response["result"]["status"].upper() == "SUCCESS":
                with open(f"{cwd}/config_jsons/workflow_export_configurations.json", "w") as f:
                    json.dump(workflow_get_json_response["result"]["response"]["result"], f)
            assert workflow_get_json_response["result"]["status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_export_workflow_configuration_json"])
    def test_update_workflow(self):  # Fails
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


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_create_workflow_version"])
    def test_trigger_workflow(self):
        try:
            workflow_get_response = iwx_client.trigger_workflow(
                workflow_version_id=pytest.workflow_version_id,
                workflow_id=pytest.workflows["workflow_api_test_1"],
                domain_id=pytest.domain_id)
            print(workflow_get_response)
            pytest.workflow_run_id = workflow_get_response["result"]["response"].get('result', {}).get('id', None)
            if pytest.workflow_run_id is None:
                assert False
            else:
                assert workflow_get_response["result"]["status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_trigger_workflow"])
    def test_get_status_workflow(self):
        try:
            time.sleep(5)
            workflow_run_status_response = iwx_client.get_status_of_workflow(pytest.workflow_run_id,
                                                                             pytest.workflows["workflow_api_test_1"])
            print("workflow_run_status_response",workflow_run_status_response)
            assert workflow_run_status_response["result"]["status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_get_status_workflow"])
    def test_get_list_of_workflow_runs(self):
        try:
            workflow_run_status_response = iwx_client.get_list_of_workflow_runs(pytest.domain_id,
                                                                                pytest.workflows["workflow_api_test_1"])
            workflow_runs_all_status_response = iwx_client.get_list_of_workflow_runs()
            assert workflow_run_status_response["result"]["status"].upper() == "SUCCESS" and \
                   workflow_runs_all_status_response["result"]["status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_get_status_workflow"])
    def test_get_list_of_workflow_runs_jobs(self):
        try:
            workflow_run_status_response = iwx_client.get_list_of_workflow_runs_jobs(pytest.workflow_run_id)
            assert workflow_run_status_response["result"]["status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_get_status_workflow"])
    def test_poll_workflow(self):
        try:
            workflow_run_status_response = iwx_client.poll_workflow_run_till_completion(pytest.workflow_run_id,
                                                                                        pytest.workflows[
                                                                                            "workflow_api_test_1"])
            assert workflow_run_status_response["result"]["status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_poll_workflow"])
    def test_restart_or_cancel_multiple_workflows(self):
        # Restart Multiple Workflows (This operation is only for operations_analyst user)
        try:
            workflow_get_response = iwx_client.trigger_workflow(
                workflow_id=pytest.workflows['workflow_api_test_1'],
                workflow_version_id=pytest.workflow_version_id,
                domain_id=pytest.domain_id)
            pytest.workflow_run_id = workflow_get_response["result"]["response"].get('result', {}).get('id', None)
            workflow_cancel_response = iwx_client.cancel_multiple_workflow(
                {"ids": [
                    {"workflow_id": pytest.workflows['workflow_api_test_1'], "run_id": pytest.workflow_run_id}]})
            time.sleep(10)
            workflow_restart_response = iwx_client.restart_or_cancel_multiple_workflows(
                workflow_list_body={"ids": [
                    {"workflow_id": pytest.workflows['workflow_api_test_1'], "run_id": pytest.workflow_run_id}]})
            assert workflow_cancel_response["result"]["status"].upper() == "SUCCESS" and \
                   workflow_restart_response["result"]["status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_restart_or_cancel_multiple_workflows"])
    def test_pause_or_resume_multiple_workflows(self):
        try:
            workflow_pause_response = iwx_client.pause_or_resume_multiple_workflows(workflow_list_body={
                "workflows": [{
                     'workflow_id': pytest.workflows['workflow_api_test_1'],
                     'workflow_version_id': pytest.workflow_version_id
                 }]
            })
            print(workflow_pause_response)
            time.sleep(30)
            workflow_resume_response = iwx_client.pause_or_resume_multiple_workflows(action_type="resume",
                                                                                     workflow_list_body={
                                                                                         "workflows": [{
                                                                                             'workflow_id': pytest.workflows['workflow_api_test_1'],
                                                                                             'workflow_version_id': pytest.workflow_version_id
                                                                                         }]
                                                                                     })
            print(workflow_resume_response)
            assert workflow_pause_response["result"]["status"].upper() == "SUCCESS" and workflow_resume_response["result"][
                "status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_pause_or_resume_multiple_workflows"])
    def test_get_global_list_of_workflow_runs(self):
        try:
            workflow_run_status_response = iwx_client.get_global_list_of_workflow_runs()
            # print(workflow_run_status_response)
            assert workflow_run_status_response["result"]["status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestWorkflowFlow::test_get_global_list_of_workflow_runs"])
    def test_get_workflow_run_details(self):
        try:
            workflow_run_details_response = iwx_client.get_workflow_run_details(
                workflow_id=pytest.workflows[next(iter(pytest.workflows))], run_id=pytest.workflow_run_id)
            # print(workflow_run_status_response)
            assert workflow_run_details_response["result"]["status"].upper() == "SUCCESS"
        except WorkflowError as e:
            print(str(e))
            assert False

