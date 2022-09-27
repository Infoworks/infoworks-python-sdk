import pytest

from infoworks.sdk.client import InfoworksClientSDK
from infoworks.error import *
from test_cases.conftest import ValueStorage
import json

refresh_token = "zThziQ7MoJJPYAha+U/+PBSTZG944F+SHBDs+m/z2qn8+m/ax8Prpzla1MHzQ5EBLzB2Bw8a+Qs9r6En5BEN2DsmUVJ6sKFb2yI2"
iwx_client = InfoworksClientSDK()
iwx_client.initialize_client_with_defaults("http", "10.18.1.28", "3001", refresh_token)


@pytest.mark.dependency()
def test_create_source():
    try:
        src_create_response = iwx_client.create_source(source_config={
            "name": "iwx_sdk_srcname",
            "type": "rdbms",
            "sub_type": "oracle",
            "data_lake_path": "/iw/sources/iwx_sdk_srcname",
            "environment_id": ValueStorage.env_id,
            "storage_id": ValueStorage.storage_id,
            "is_source_ingested": True
        })
        assert src_create_response["result"]["status"].upper() == "SUCCESS"
        ValueStorage.source_id = src_create_response["result"]["source_id"]
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_source"])
def test_configure_source_connection():
    try:
        response = iwx_client.configure_source_connection(ValueStorage.source_id, {
            "driver_name": "oracle.jdbc.driver.OracleDriver",
            "driver_version": "v2",
            "connection_url": "jdbc:oracle:thin:@52.73.246.109:1521:xe",
            "username": "automation_db",
            "password": "eEBcRuPkw0zh9oIPvKnal+1BNKmFH5TfdI1ieDinruUv47Z5+f/oPjb+uyqUmfcQusM2DjoHc3OM",
            # can be iwx encrypted password or plain text password
            "connection_mode": "jdbc",
            "database": "ORACLE"
        })
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_configure_source_connection"])
def test_source_test_connection_job():
    try:
        response = iwx_client.source_test_connection_job_poll(ValueStorage.source_id, poll_timeout=300,
                                                              polling_frequency=15, retries=1)
        assert response["result"]["status"].upper() == "SUCCESS" and response["result"]["job_id"] not in ["", None]
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_source_test_connection_job"])
def test_browse_source_job():
    try:
        response = iwx_client.browse_source_tables(ValueStorage.source_id, filter_tables_properties={
            "schemas_filter": "AUTOMATION_DB",
            "catalogs_filter": "%",
            "tables_filter": "CUSTOMERS,ORDERS",
            "is_data_sync_with_filter": True,
            "is_filter_enabled": True
        }, poll_timeout=300, polling_frequency=15, retries=1)
        assert response["result"]["status"].upper() == "SUCCESS" and response["result"]["job_id"] not in ["", None]
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_browse_source_job"])
def test_add_tables_to_source():
    try:
        response = iwx_client.add_tables_to_source(ValueStorage.source_id, tables_to_add_config=[{
            "table_name": "CUSTOMERS",
            "schema_name": "AUTOMATION_DB",
            "table_type": "TABLE",
            "target_table_name": "CUSTOMERS",
            "target_schema_name": "IWX_SDK_TEST_SCHEMA"
        }, {
            "table_name": "ORDERS",
            "schema_name": "AUTOMATION_DB",
            "table_type": "TABLE",
            "target_table_name": "ORDERS",
            "target_schema_name": "IWX_SDK_TEST_SCHEMA"
        }], poll_timeout=300, polling_frequency=15, retries=1)
        assert response["result"]["status"].upper() == "SUCCESS" and response["result"]["job_id"] not in ["", None]
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_add_tables_to_source"])
def test_configure_tables_and_tablegroups():
    try:
        f = open("config_jsons/rdbms_source_configure.json")
        data = json.load(f)
        f.close()
        response = iwx_client.configure_tables_and_tablegroups(ValueStorage.source_id,
                                                               configuration_obj=data["configuration"])
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_add_tables_to_source"])
def test_list_of_tables():
    try:
        response = iwx_client.list_tables_in_source(ValueStorage.source_id)
        assert response["result"]["status"].upper() == "SUCCESS"
        tables = response["result"]["response"]
        for item in tables:
            ValueStorage.table_ids.append(item["id"])
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_list_of_tables"])
def test_create_table_group():
    try:
        response = iwx_client.create_table_group(ValueStorage.source_id,
                                                 table_group_obj={
                                                     "environment_compute_template": {
                                                         "environment_compute_template_id": "03e08fb415447fd56df9d888"},
                                                     "name": "tg_via_api",
                                                     "max_connections": 1,
                                                     "max_parallel_entities": 1,
                                                     "tables": [
                                                         {"table_id": ValueStorage.table_ids[0],
                                                          "connection_quota": 100}
                                                     ]
                                                 })
        assert response["result"]["status"].upper() == "SUCCESS"

    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_table_group"])
def test_update_table_group():
    try:
        response = iwx_client.update_table_group(ValueStorage.source_id, ValueStorage.table_group_id,
                                                 table_group_obj={
                                                     "environment_compute_template": {
                                                         "environment_compute_template_id": "03e08fb415447fd56df9d888"},
                                                     "name": "tg_via_api",
                                                     "max_connections": 1,
                                                     "max_parallel_entities": 1,
                                                     "tables": [
                                                         {"table_id": ValueStorage.table_ids[0],
                                                          "connection_quota": 100}
                                                     ]
                                                 })
        assert response["result"]["status"].upper() == "SUCCESS"

    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_table_group", "test_configure_tables_and_tablegroups"])
def test_list_of_tablegroups():
    try:
        response = iwx_client.get_list_of_table_groups(ValueStorage.source_id)
        assert response["result"]["status"].upper() == "SUCCESS" and len(response["result"]["response"]) > 0
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_table_group", "test_configure_tables_and_tablegroups"])
def test_delete_tablegroup():  # Fails
    try:
        response = iwx_client.delete_table_group(ValueStorage.source_id, ValueStorage.table_group_id)
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_table_group", "test_configure_tables_and_tablegroups"])
def test_ingestion_job():
    try:
        response = iwx_client.submit_source_job(ValueStorage.source_id, body={
            "job_type": "truncate_reload",
            "table_group_id": ValueStorage.table_group_id
        }, poll_timeout=300, polling_frequency=15, retries=1)
        assert response["result"]["status"].upper() == "SUCCESS" and response["result"]["job_id"] not in ["", None]
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency()
def test_list_of_sources():
    try:
        response = iwx_client.get_list_of_sources()
        assert response["result"]["status"].upper() == "SUCCESS" and len(response["result"]["response"]) > 0
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_configure_source_connection"])
def test_get_source_connection_details():
    try:
        response = iwx_client.get_source_connection_details(ValueStorage.source_id)
        print(response)
        assert response["result"]["status"].upper() == "SUCCESS" and len(response["result"]["response"]) > 0
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_source"])
def test_add_source_advanced_configuration():
    try:
        response = iwx_client.add_source_advanced_configuration(ValueStorage.source_id,
                                                                config_body={"key": "file_preview_row_count",
                                                                             "value": "10", "is_active": True})
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_add_source_advanced_configuration"])
def test_update_source_advanced_configuration():
    try:
        response = iwx_client.update_source_advanced_configuration(ValueStorage.source_id,
                                                                   "file_preview_row_count",
                                                                   config_body={"key": "file_preview_row_count",
                                                                                "value": "20", "is_active": True})
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_source"])
def test_list_advanced_configuration_of_sources():
    try:
        response_all = iwx_client.get_advanced_configuration_of_sources(ValueStorage.source_id)
        response_one = iwx_client.get_advanced_configuration_of_sources(ValueStorage.source_id,
                                                                        key="file_preview_row_count")
        assert response_all["result"]["status"].upper() == "SUCCESS" and response_one["result"][
            "status"].upper() == "SUCCESS" and len(response_all["result"]["response"]) > 0
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_add_source_advanced_configuration"])
def test_delete_source_advanced_configuration():
    try:
        response = iwx_client.delete_source_advanced_configuration(ValueStorage.source_id, key="file_preview_row_count")
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_source", "test_configure_tables_and_tablegroups"])
def test_export_import_source_json():
    try:
        response = iwx_client.get_source_configurations_json_export(ValueStorage.source_id)
        assert response["result"]["status"].upper() == "SUCCESS" and len(response["result"]["response"]) > 0
        response = iwx_client.post_source_configurations_json_import(ValueStorage.source_id,
                                                                     response["result"]["response"])
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_source", "test_configure_tables_and_tablegroups"])
def test_get_table_configs():
    try:
        response = iwx_client.get_table_configurations(ValueStorage.source_id, ValueStorage.table_ids[0],
                                                       ingestion_config_only=False)
        print(response)
        assert response["result"]["status"].upper() == "SUCCESS" and len(response["result"]["response"]) > 0
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_source", "test_configure_tables_and_tablegroups"])
def test_update_table_configuration():
    try:
        response = iwx_client.update_table_configuration(ValueStorage.source_id,
                                                         ValueStorage.table_ids[0],
                                                         config_body={
                                                             "generate_history_view": True
                                                         })
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_source", "test_configure_tables_and_tablegroups"])
def test_add_table_advanced_configuration():
    try:
        response = iwx_client.add_table_advanced_configuration(ValueStorage.source_id,
                                                               ValueStorage.table_ids[0],
                                                               config_body={"key": "should_run_dedupe",
                                                                            "value": "false", "is_active": True})
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_add_table_advanced_configuration"])
def test_update_table_advanced_configuration():
    try:
        response = iwx_client.update_table_advanced_configuration(ValueStorage.source_id,
                                                                  ValueStorage.table_ids[0],
                                                                  "should_run_dedupe",
                                                                  config_body={"key": "should_run_dedupe",
                                                                               "value": "true", "is_active": True})
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_add_table_advanced_configuration"])
def test_delete_table_advanced_configuration():
    try:
        response = iwx_client.delete_table_advanced_configuration(ValueStorage.source_id,
                                                                  ValueStorage.table_ids[0],
                                                                  "should_run_dedupe")
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_configure_tables_and_tablegroups"])
def test_get_table_export_configurations():  # Fails
    try:
        response = iwx_client.get_table_export_configurations(ValueStorage.source_id, ValueStorage.table_ids[0],
                                                              connection_only=False)
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_create_source", "test_configure_tables_and_tablegroups"])
def test_get_table_ingestion_metrics():
    try:
        response = iwx_client.get_table_ingestion_metrics(ValueStorage.source_id, ValueStorage.table_ids[0])
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency()
def test_add_table_and_file_mappings_for_csv():
    try:
        data = {
            "configuration": {
                "source_file_properties": {
                    "column_enclosed_by": "\"",
                    "column_separator": ",",
                    "encoding": "UTF-8",
                    "escape_character": "\\",
                    "header_rows_count": 1
                },
                "target_relative_path": "/table_path",
                "deltaLake_table_name": "table_name",
                "source_file_type": "csv",
                "ingest_subdirectories": True,
                "source_relative_path": "/table_path",
                "exclude_filename_regex": "",
                "include_filename_regex": "",
                "is_archive_enabled": False,
                "target_schema_name": "target_schema_name",
                "target_table_name": "target_table_name"}, "source": "3eeeb65bedc0b75fbbcaf730",
            "name": "table_name"}
        response = iwx_client.add_table_and_file_mappings_for_csv(ValueStorage.source_id, file_mappings_config=data)
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False
