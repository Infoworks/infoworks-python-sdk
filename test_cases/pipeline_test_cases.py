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


def test_create_pipeline():
    try:
        pipeline_create_response = iwx_client.create_pipeline(
            pipeline_config={"name": "pipeline_api_unit_test_" + str(uuid_val),
                             "batch_engine": "spark",
                             "domain_id": ValueStorage.domain_id,
                             "storage_id": ValueStorage.storage_id,
                             "compute_template_id": ValueStorage.cluster_id,
                             "ml_engine": "SparkML"})
        assert pipeline_create_response["result"]["status"].upper() == "SUCCESS"
        ValueStorage.pipeline_id = pipeline_create_response["result"]["pipeline_id"]
    except PipelineError as e:
        print(str(e))
        assert False


def test_get_pipeline():
    try:
        pipeline_get_response = iwx_client.get_pipeline(ValueStorage.pipeline_id, ValueStorage.domain_id)
        assert pipeline_get_response["result"]["status"].upper() == "SUCCESS"
    except PipelineError as e:
        print(str(e))
        assert False


def test_update_pipeline():
    try:
        pipeline_update_response = iwx_client.update_pipeline(ValueStorage.pipeline_id, ValueStorage.domain_id, {
            "name": "pipeline_api_unit_test_updated",
            "compute_template_id": ValueStorage.cluster_id
        })
        assert pipeline_update_response["result"]["status"].upper() == "SUCCESS"
    except PipelineError as e:
        print(str(e))
        assert False


def test_sql_import_into_pipeline():
    try:
        pipeline_sql_import_response = iwx_client.sql_import_into_pipeline(ValueStorage.pipeline_id,
                                                                           ValueStorage.domain_id, {
                                                                               "sql": "select * from DIMACCOUNTtest",
                                                                               "sql_import_configuration": {
                                                                                   "quoted_identifier": "DOUBLE_QUOTE",
                                                                                   "sql_dialect": "LENIENT"
                                                                               }})
        print(pipeline_sql_import_response)
        assert pipeline_sql_import_response["result"]["status"].upper() == "SUCCESS"
    except PipelineError as e:
        print(str(e))
        assert False


def test_get_pipeline_version_details():
    try:
        pipeline_get_version_response = iwx_client.get_pipeline_version_details(ValueStorage.pipeline_id,
                                                                                ValueStorage.domain_id,
                                                                                ValueStorage.pipeline_version_id)
        assert pipeline_get_version_response["result"]["status"].upper() == "SUCCESS"
    except PipelineError as e:
        print(str(e))
        assert False


def test_get_pipeline_configuration_json():
    pipeline_get_pipeline_json_response = iwx_client.get_pipeline_configuration_json_export(ValueStorage.pipeline_id,
                                                                                            ValueStorage.domain_id)
    if pipeline_get_pipeline_json_response["result"]["status"].upper() == "SUCCESS":
        with open(f"{cwd}/test_cases/config_jsons/pipeline_configurations.json", "w+") as f:
            json.dump(pipeline_get_pipeline_json_response["result"]["response"]["result"], f)
    assert pipeline_get_pipeline_json_response["result"]["status"].upper() == "SUCCESS"


def test_put_pipeline_configuration_json():
    try:
        with open(f"{cwd}/test_cases/config_jsons/pipeline_configurations.json", "r") as f:
            pipeline_config = json.loads(f.read())
        pipeline_put_pipeline_json_response = iwx_client.update_pipeline_configuration_json_export(
            ValueStorage.pipeline_id,
            ValueStorage.domain_id,
            pipeline_config)
        assert pipeline_put_pipeline_json_response["result"]["status"].upper() == "SUCCESS"
    except PipelineError as e:
        print(str(e))
        assert False
