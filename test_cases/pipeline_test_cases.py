import json
import logging
import os
import sys
import pytest
from infoworks.error import *
from infoworks.sdk.client import InfoworksClientSDK

refresh_token = "zThziQ7MoJJPYAha+U/+PBSTZG944F+SHBDs+m/z2qn8+m/ax8Prpzla1MHzQ5EBLzB2Bw8a+Qs9r6En5BEN2DsmUVJ6sKFb2yI2"
iwx_client = InfoworksClientSDK()
iwx_client.initialize_client_with_defaults("https", "att-iwx-ci-cd.infoworks.technology", "443", refresh_token)
cwd = os.getcwd()

pytest.environment_id = "884236e85b9b1a69b2907e4c"
pytest.domain_id = ""
pytest.pipelines = {}
pytest.active_version_id = ""
pytest.old_version_id = ""
pytest.env_type = "snowflake"
pytest.default_sf_warehouse = "DEMO_WH"

logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s - %(module)s - %(pathname)s - %(lineno)d - %(levelname)s - %(message)s')
fh = logging.FileHandler('/tmp/sdk_pipeline_test.log')
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
def test_list_pipelines():
    pytest.domain_id = get_domain_id()
    try:
        response = iwx_client.list_pipelines(domain_id=pytest.domain_id,params={"filter":{"name":"pipeline_api_unit_test_1"}})
        print(response)
        assert response["result"]["status"].upper() == "SUCCESS"
    except PipelineError as e:
        print(str(e))
        assert False

@pytest.mark.dependency()
def test_create_pipeline():
    pytest.domain_id = get_domain_id()
    try:
        if pytest.env_type == "snowflake":
            pipeline1_create_response = iwx_client.create_pipeline(
                pipeline_config={"name": "pipeline_api_unit_test_1",
                                 "batch_engine": "SNOWFLAKE",
                                 "snowflake_warehouse": pytest.default_sf_warehouse,
                                 "environment_id": pytest.environment_id,
                                 "domain_id": pytest.domain_id,
                                 "run_job_on_data_plane": False,
                                 })
            pipeline2_create_response = iwx_client.create_pipeline(
                pipeline_config={"name": "pipeline_api_unit_test_2",
                                 "batch_engine": "SNOWFLAKE",
                                 "snowflake_warehouse": pytest.default_sf_warehouse,
                                 "environment_id": pytest.environment_id,
                                 "domain_id": pytest.domain_id,
                                 "run_job_on_data_plane": False,
                                 })
            pipeline3_create_response = iwx_client.create_pipeline(
                pipeline_config={"name": "pipeline_api_unit_test_3",
                                 "batch_engine": "SNOWFLAKE",
                                 "snowflake_warehouse": pytest.default_sf_warehouse,
                                 "environment_id": pytest.environment_id,
                                 "domain_id": pytest.domain_id,
                                 "run_job_on_data_plane": False,
                                 })
        elif pytest.env_type == "spark":
            pipeline1_create_response = iwx_client.create_pipeline(
                pipeline_config={"name": "pipeline_api_unit_test_1",
                                 "batch_engine": "SPARK",
                                 "domain_id": pytest.domain_id,
                                 "environment_id": pytest.environment_id,
                                 "storage_id": pytest.storage_id,
                                 "compute_template_id": pytest.cluster_id,
                                 "ml_engine": "SPARK",
                                 "run_job_on_data_plane": True})
            pipeline2_create_response = iwx_client.create_pipeline(
                pipeline_config={"name": "pipeline_api_unit_test_2",
                                 "batch_engine": "SPARK",
                                 "domain_id": pytest.domain_id,
                                 "environment_id": pytest.environment_id,
                                 "storage_id": pytest.storage_id,
                                 "compute_template_id": pytest.cluster_id,
                                 "ml_engine": "SPARK",
                                 "run_job_on_data_plane": True})
            pipeline3_create_response = iwx_client.create_pipeline(
                pipeline_config={"name": "pipeline_api_unit_test_3",
                                 "batch_engine": "SPARK",
                                 "domain_id": pytest.domain_id,
                                 "environment_id": pytest.environment_id,
                                 "storage_id": pytest.storage_id,
                                 "compute_template_id": pytest.cluster_id,
                                 "ml_engine": "SPARK",
                                 "run_job_on_data_plane": True})
        elif pytest.env_type == "bigquery":
            pipeline1_create_response = iwx_client.create_pipeline(
                pipeline_config={"name": "pipeline_api_unit_test_1",
                                 "batch_engine": str("BIGQUERY"), "domain_id": pytest.domain_id,
                                 "environment_id": pytest.environment_id, "run_job_on_data_plane": False})
            pipeline2_create_response = iwx_client.create_pipeline(
                pipeline_config={"name": "pipeline_api_unit_test_2",
                                 "batch_engine": str("BIGQUERY"), "domain_id": pytest.domain_id,
                                 "environment_id": pytest.environment_id, "run_job_on_data_plane": False})
            pipeline3_create_response = iwx_client.create_pipeline(
                pipeline_config={"name": "pipeline_api_unit_test_3",
                                 "batch_engine": str("BIGQUERY"), "domain_id": pytest.domain_id,
                                 "environment_id": pytest.environment_id, "run_job_on_data_plane": False})

        assert pipeline1_create_response["result"]["status"].upper() == "SUCCESS" and \
               pipeline2_create_response["result"]["status"].upper() == "SUCCESS" and \
               pipeline3_create_response["result"]["status"].upper() == "SUCCESS"
        pytest.pipelines["pipeline_api_unit_test_1"] = pipeline1_create_response["result"]["pipeline_id"]
        pytest.pipelines["pipeline_api_unit_test_2"] = pipeline2_create_response["result"]["pipeline_id"]
        pytest.pipelines["pipeline_api_unit_test_3"] = pipeline3_create_response["result"]["pipeline_id"]
        logger.info(pipeline1_create_response)
        logger.info(pipeline2_create_response)
        logger.info(pipeline3_create_response)
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_pipeline"])
def test_get_old_pipeline_version():
    try:
        pipeline_get_response = iwx_client.get_pipeline(pytest.pipelines["pipeline_api_unit_test_1"], pytest.domain_id)
        assert pipeline_get_response["result"]["status"].upper() == "SUCCESS"
        pytest.old_version_id = pipeline_get_response["result"]["response"]["result"].get("active_version_id")
        logger.info(pipeline_get_response)
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_pipeline"])
def test_delete_pipeline():
    try:
        pipeline_delete_response = iwx_client.delete_pipeline(pytest.pipelines["pipeline_api_unit_test_3"],
                                                              pytest.domain_id)
        assert pipeline_delete_response["result"]["status"].upper() == "SUCCESS"
        logger.info(pipeline_delete_response)
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_pipeline"])
def test_get_pipeline_id():
    try:
        pipeline_get_response = iwx_client.get_pipeline_id("pipeline_api_unit_test_1", pytest.domain_id)
        assert pipeline_get_response["result"]["status"].upper() == "SUCCESS"
        logger.info(pipeline_get_response)
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_get_old_pipeline_version"])
def test_sql_import_into_pipeline():
    try:
        pipeline_sql_import_response = iwx_client.sql_import_into_pipeline(pytest.pipelines["pipeline_api_unit_test_1"],
                                                                           pytest.domain_id, {
                                                                               "sql": "select * from CUSTOMER",
                                                                               "sql_import_configuration": {
                                                                                   "quoted_identifier": "DOUBLE_QUOTE",
                                                                                   "sql_dialect": "LENIENT"
                                                                               }})
        logger.info(pipeline_sql_import_response)
        assert pipeline_sql_import_response["result"]["status"].upper() == "SUCCESS"
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_sql_import_into_pipeline"])
def test_get_pipeline():
    try:
        pipeline_get_response = iwx_client.get_pipeline(pytest.pipelines["pipeline_api_unit_test_1"], pytest.domain_id)
        assert pipeline_get_response["result"]["status"].upper() == "SUCCESS"
        pytest.active_version_id = pipeline_get_response["result"]["response"]["result"].get("active_version_id")
        logger.info(pipeline_get_response)
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_sql_import_into_pipeline"])
def test_get_pipeline_configuration_json():
    try:
        pipeline_get_pipeline_json_response = iwx_client.export_pipeline_configurations(
            pytest.pipelines["pipeline_api_unit_test_1"],
            pytest.domain_id)
        if pipeline_get_pipeline_json_response["result"]["status"].upper() == "SUCCESS":
            with open(f"{cwd}/config_jsons/pipeline_configurations.json", "w+") as f:
                json.dump(pipeline_get_pipeline_json_response["result"]["response"]["result"], f)
        assert pipeline_get_pipeline_json_response["result"]["status"].upper() == "SUCCESS"
        logger.info(pipeline_get_pipeline_json_response)
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_get_pipeline_configuration_json"])
def test_import_pipeline_configuration_json():
    try:
        with open(f"{cwd}/config_jsons/pipeline_configurations.json", "r") as f:
            pipeline_config = json.loads(f.read())
        pipeline_put_pipeline_json_response = iwx_client.import_pipeline_configurations(
            pytest.pipelines["pipeline_api_unit_test_2"],
            pytest.domain_id,
            pipeline_config)
        assert pipeline_put_pipeline_json_response["result"]["status"].upper() == "SUCCESS"
        logger.info(pipeline_put_pipeline_json_response)
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_get_pipeline_configuration_json"])
def test_get_pipeline_lineage():
    try:
        with open(f"{cwd}/config_jsons/pipeline_configurations.json", "r") as f:
            pipeline_config = json.loads(f.read())
            node_keys = pipeline_config["configuration"]["pipeline_configs"]["model"]["nodes"].keys()
            for item in node_keys:
                if item.startswith("TARGET") or item.startswith("SNOWFLAKE_TARGET"):
                    target_node = item
                    column_name = \
                        pipeline_config["configuration"]["pipeline_configs"]["model"]["nodes"][item]["output_entities"][
                            0].get("name")
                    break

        pipeline_get_response = iwx_client.get_pipeline_lineage(pytest.domain_id,
                                                                pytest.pipelines["pipeline_api_unit_test_1"],
                                                                pytest.active_version_id, column_name, target_node)
        assert pipeline_get_response["result"]["status"].upper() == "SUCCESS"
        logger.info(pipeline_get_response)
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_get_pipeline"])
def test_get_pipeline_version_details():
    try:
        pipeline_get_version_response = iwx_client.get_pipeline_version_details(
            pytest.pipelines["pipeline_api_unit_test_1"],
            pytest.domain_id,
            pytest.active_version_id)
        assert pipeline_get_version_response["result"]["status"].upper() == "SUCCESS"
        logger.info(pipeline_get_version_response)
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_get_pipeline_version_details"])
def test_delete_pipeline_version():
    try:
        pipeline_get_version_response = iwx_client.delete_pipeline_version(
            pytest.pipelines["pipeline_api_unit_test_1"],
            pytest.domain_id,
            pytest.old_version_id)
        assert pipeline_get_version_response["result"]["status"].upper() == "SUCCESS"
        logger.info(pipeline_get_version_response)
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_pipeline"])
def test_parent_entity_id():
    try:
        parent_entity_id_response = iwx_client.get_parent_entity_id(
            {"entity_id": pytest.pipelines["pipeline_api_unit_test_1"], "entity_type": "pipeline"})
        assert parent_entity_id_response["result"]["status"].upper() == "SUCCESS"
        logger.info(parent_entity_id_response)
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_sql_import_into_pipeline"])
def test_trigger_pipeline_job():
    try:
        pipeline_trigger_response = iwx_client.trigger_pipeline_job(
            pytest.domain_id,
            pytest.pipelines["pipeline_api_unit_test_1"],
            poll=False
        )
        assert pipeline_trigger_response["result"]["status"].upper() == "SUCCESS"
        logger.info(pipeline_trigger_response)
    except PipelineError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_import_pipeline_configuration_json"])
def test_update_pipeline():
    try:
        pipeline_update_response = iwx_client.update_pipeline(pytest.pipelines["pipeline_api_unit_test_2"],
                                                              pytest.domain_id, {
                                                                  "name": "pipeline_api_unit_test_2_updated"
                                                              })
        assert pipeline_update_response["result"]["status"].upper() == "SUCCESS"
    except PipelineError as e:
        print(str(e))
        assert False

# @pytest.mark.dependency(depends=[""])
# def test_update_pipeline_version_details():
#     try:
#         pipeline_update_response = iwx_client.update_pipeline_version_details(pytest.pipelines["pipeline_api_unit_test_2"], pytest.domain_id, {
#             "name": "pipeline_api_unit_test_2_updated"
#         })
#         assert pipeline_update_response["result"]["status"].upper() == "SUCCESS"
#     except PipelineError as e:
#         print(str(e))
#         assert False
