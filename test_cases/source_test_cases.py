import os
import configparser
import pytest

from infoworks.sdk.client import InfoworksClientSDK
from infoworks.error import *
from test_cases.conftest import ValueStorage
import json

config = configparser.ConfigParser()
config.read('config.ini')
refresh_token = "zThziQ7MoJJPYAha+U/+PBSTZG944F+SHBDs+m/z2qn8+m/ax8Prpzla1MHzQ5EBLzB2Bw8a+Qs9r6En5BEN2DsmUVJ6sKFb2yI2"
iwx_client = InfoworksClientSDK()
iwx_client.initialize_client_with_defaults("https", "att-iwx-ci-cd.infoworks.technology", "443", refresh_token)


class TestRDBMSSDKFlow:
    @pytest.mark.dependency()
    def test_create_source(self):
        pytest.env_type = "snowflake"
        try:
            src_create_response = {}
            if pytest.env_type == "snowflake":
                src_create_response = iwx_client.create_source(source_config={
                    "name": "iwx_sdk_srcname_snowflake",
                    "type": "rdbms",
                    "sub_type": "sqlserver",
                    "data_lake_path": "/iw/sources/iwx_sdk_srcname_snowflake",
                    "environment_id": ValueStorage.env_id,
                    "storage_id": ValueStorage.storage_id,
                    "data_lake_schema": "PUBLIC",
                    "target_database_name": "PUBLIC",
                    "is_database_case_sensitive": False,
                    "is_schema_case_sensitive": False,
                    "staging_schema_name": None,
                    "is_staging_schema_case_sensitive": None,
                    "is_source_ingested": True})
            else:
                src_create_response = iwx_client.create_source(source_config={
                    "name": "iwx_sdk_srcname_non_snowflake",
                    "type": "rdbms",
                    "sub_type": "sqlserver",
                    "data_lake_schema": "PUBLIC",
                    "data_lake_path": "/iw/sources/iwx_sdk_srcname_non_snowflake/",
                    "environment_id": ValueStorage.env_id,
                    "storage_id": ValueStorage.storage_id,
                    "is_source_ingested": True
                })
            print(src_create_response)
            assert src_create_response["result"]["status"].upper() == "SUCCESS"
            ValueStorage.source_id = src_create_response["result"]["source_id"]
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_create_source"])
    def test_configure_source_connection(self):
        try:
            response = iwx_client.configure_source_connection(ValueStorage.source_id, {
                "driver_name": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                "driver_version": "v2",
                "connection_url": "jdbc:sqlserver://10.18.2.3:1433;databaseName=customer_360",
                "username": "presalesuser",
                "password": "r4wfGwUd3s4XfEbGzkLmB1w5ATbAcaXhNtmAfVI7FZ4IutHR/YpSgeLnCFyvcYn1hW8vFg==",
                # can be iwx encrypted password or plain text password
                "connection_mode": "jdbc",
                "database": "SQL Server"
            })
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_configure_source_connection"])
    def test_get_source_connection_details(self):
        try:
            response = iwx_client.get_source_connection_details(ValueStorage.source_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS" and len(response["result"]["response"]) > 0
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_configure_source_connection"])
    def test_source_test_connection_job(self):
        try:
            response = iwx_client.source_test_connection_job_poll(ValueStorage.source_id, poll_timeout=300,
                                                                  polling_frequency=15, retries=1)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS" and response["result"]["job_id"] not in ["", None]
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_source_test_connection_job"])
    def test_browse_source_job(self):
        try:
            response = iwx_client.browse_source_tables(ValueStorage.source_id, filter_tables_properties={
                "schemas_filter": "%",
                "catalogs_filter": "%",
                "tables_filter": "%",
                "is_data_sync_with_filter": True,
                "is_filter_enabled": True
            }, poll_timeout=300, polling_frequency=15, retries=1)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS" and response["result"]["job_id"] not in ["", None]
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_browse_source_job"])
    def test_add_tables_to_source(self):
        try:
            response = iwx_client.add_tables_to_source(ValueStorage.source_id, tables_to_add_config=[{
                "table_name": "customer",
                "schema_name": "lineage_demo",
                "catalog_name": "customer_360",
                "table_type": "TABLE",
                "target_table_name": "customer",
                "target_schema_name": "IWX_SDK_TEST_SCHEMA"
            }, {
                "table_name": "sales",
                "schema_name": "lineage_demo",
                "catalog_name": "customer_360",
                "table_type": "TABLE",
                "target_table_name": "sales",
                "target_schema_name": "IWX_SDK_TEST_SCHEMA"
            }], poll_timeout=300, polling_frequency=15, retries=1)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS" and response["result"]["job_id"] not in ["", None]
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_add_tables_to_source"])
    def test_configure_tables_and_tablegroups(self):
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

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_add_tables_to_source"])
    def test_list_of_tables(self):
        try:
            response = iwx_client.list_tables_in_source(ValueStorage.source_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
            tables = response["result"]["response"]["result"]
            for item in tables:
                print(item["id"])
                ValueStorage.table_ids.append(item["id"])
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_list_of_tables"])
    def test_create_table_group(self):
        try:
            response = iwx_client.create_table_group(ValueStorage.source_id,
                                                     table_group_obj={
                                                         "environment_compute_template": {
                                                             "environment_compute_template_id": ValueStorage.compute_id},
                                                         "name": "tg_via_api",
                                                         "max_connections": 1,
                                                         "max_parallel_entities": 1,
                                                         "tables": [
                                                             {"table_id": ValueStorage.table_ids[0],
                                                              "connection_quota": 50}, {
                                                                 "table_id": ValueStorage.table_ids[1],
                                                                 "connection_quota": 50
                                                             }
                                                         ]
                                                     })
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"

        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_create_table_group"])
    def test_update_table_group(self):
        try:
            response = iwx_client.update_table_group(ValueStorage.source_id, ValueStorage.table_group_id,
                                                     table_group_obj={
                                                         "environment_compute_template": {
                                                             "environment_compute_template_id": ValueStorage.compute_id},
                                                         "name": "tg_via_api",
                                                         "max_connections": 1,
                                                         "max_parallel_entities": 1,
                                                         "tables": [
                                                             {"table_id": ValueStorage.table_ids[0],
                                                              "connection_quota": 100}
                                                         ]
                                                     })
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"

        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_create_table_group",
                                     "TestRDBMSSDKFlow::test_configure_tables_and_tablegroups"])
    def test_list_of_tablegroups(self):
        try:
            response = iwx_client.get_list_of_table_groups(ValueStorage.source_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS" and len(response["result"]["response"]) > 0
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_create_table_group",
                                     "TestRDBMSSDKFlow::test_configure_tables_and_tablegroups"])
    def test_ingestion_job(self):
        try:
            response = iwx_client.submit_source_job(ValueStorage.source_id, body={
                "job_type": "truncate_reload",
                "table_group_id": ValueStorage.table_group_id
            }, poll_timeout=300, polling_frequency=15, retries=1)
            assert response["result"]["status"].upper() == "SUCCESS" and response["result"]["job_id"] not in ["", None]
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_create_table_group",
                                     "TestRDBMSSDKFlow::test_configure_tables_and_tablegroups"])
    def test_delete_tablegroup(self):  # Fails
        try:
            response = iwx_client.delete_table_group(ValueStorage.source_id, ValueStorage.table_group_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_list_of_sources(self):
        try:
            response = iwx_client.get_list_of_sources()
            assert response["result"]["status"].upper() == "SUCCESS" and len(response["result"]["response"]) > 0
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_create_source"])
    def test_add_source_advanced_configuration(self):
        try:
            response = iwx_client.add_source_advanced_configuration(ValueStorage.source_id,
                                                                    config_body={"key": "file_preview_row_count",
                                                                                 "value": "10", "is_active": True})
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_add_source_advanced_configuration"])
    def test_update_source_advanced_configuration(self):
        try:
            response = iwx_client.update_source_advanced_configuration(ValueStorage.source_id,
                                                                       "file_preview_row_count",
                                                                       config_body={"key": "file_preview_row_count",
                                                                                    "value": "20", "is_active": True})
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_update_source_advanced_configuration"])
    def test_list_advanced_configuration_of_sources(self):
        try:
            response_all = iwx_client.get_advanced_configuration_of_sources(ValueStorage.source_id)
            response_one = iwx_client.get_advanced_configuration_of_sources(ValueStorage.source_id,
                                                                            key="file_preview_row_count")
            print(response_all)
            assert response_all["result"]["status"].upper() == "SUCCESS" and response_one["result"][
                "status"].upper() == "SUCCESS" and len(response_all["result"]["response"]) > 0
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_add_source_advanced_configuration"])
    def test_delete_source_advanced_configuration(self):
        try:
            response = iwx_client.delete_source_advanced_configuration(ValueStorage.source_id,
                                                                       key="file_preview_row_count")
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(
        depends=["TestRDBMSSDKFlow::test_create_source", "TestRDBMSSDKFlow::test_configure_tables_and_tablegroups"])
    def test_export_import_source_json(self):
        try:
            response = iwx_client.get_source_configurations_json_export(ValueStorage.source_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS" and len(response["result"]["response"]) > 0
            response = iwx_client.post_source_configurations_json_import(ValueStorage.source_id,
                                                                         response["result"]["response"]["result"])
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(
        depends=["TestRDBMSSDKFlow::test_create_source", "TestRDBMSSDKFlow::test_configure_tables_and_tablegroups"])
    def test_get_table_configs(self):
        try:
            response = iwx_client.get_table_configurations(ValueStorage.source_id, ValueStorage.table_ids[0],
                                                           ingestion_config_only=False)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS" and len(
                response["result"]["response"]["result"]) > 0
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(
        depends=["TestRDBMSSDKFlow::test_create_source", "TestRDBMSSDKFlow::test_configure_tables_and_tablegroups"])
    def test_update_table_configuration(self):
        try:
            response = iwx_client.update_table_configuration(ValueStorage.source_id,
                                                             ValueStorage.table_ids[0],
                                                             config_body={
                                                                 "name": "customer",
                                                                 "source": ValueStorage.source_id,
                                                                 "generate_history_view": True
                                                             })
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(
        depends=["TestRDBMSSDKFlow::test_create_source", "TestRDBMSSDKFlow::test_configure_tables_and_tablegroups"])
    def test_add_table_advanced_configuration(self):
        try:
            response = iwx_client.add_table_advanced_configuration(ValueStorage.source_id,
                                                                   ValueStorage.table_ids[0],
                                                                   config_body={"key": "should_run_dedupe",
                                                                                "value": "false", "is_active": True})
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_add_table_advanced_configuration"])
    def test_update_table_advanced_configuration(self):
        try:
            response = iwx_client.update_table_advanced_configuration(ValueStorage.source_id,
                                                                      ValueStorage.table_ids[0],
                                                                      "should_run_dedupe",
                                                                      config_body={"key": "should_run_dedupe",
                                                                                   "value": "true", "is_active": True})
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_update_table_advanced_configuration"])
    def test_delete_table_advanced_configuration(self):
        try:
            response = iwx_client.delete_table_advanced_configuration(ValueStorage.source_id,
                                                                      ValueStorage.table_ids[0],
                                                                      "should_run_dedupe")
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestRDBMSSDKFlow::test_configure_tables_and_tablegroups"])
    def test_get_table_export_configurations(self):  # Fails
        try:
            response = iwx_client.get_table_export_configurations(ValueStorage.source_id, ValueStorage.table_ids[0],
                                                                  connection_only=False)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(
        depends=["TestRDBMSSDKFlow::test_create_source", "TestRDBMSSDKFlow::test_configure_tables_and_tablegroups"])
    def test_get_table_ingestion_metrics(self):
        try:
            response = iwx_client.get_table_ingestion_metrics(ValueStorage.source_id, ValueStorage.table_ids[0])
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(
        depends=["TestRDBMSSDKFlow::test_create_source", "TestRDBMSSDKFlow::test_configure_tables_and_tablegroups"])
    def test_get_table_columns_details(self):
        try:
            response = iwx_client.get_table_columns_details(ValueStorage.source_id, "sales", "lineage_demo",
                                                            "customer_360")
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False


class TestFileSDKFlow:
    pytest.file_subtype = "csv"
    pytest.file_source = "sftp"

    @pytest.mark.dependency()
    def test_create_source(self):
        pytest.env_type = "snowflake"
        try:
            src_create_response = {}
            if pytest.env_type == "snowflake":
                src_create_response = iwx_client.create_source(source_config={
                    "name": f"iwx_sdk_{pytest.file_subtype}_srcname_snowflake_from_{pytest.file_source}",
                    "type": "file",
                    "sub_type": "structured",
                    "data_lake_path": f"/iw/sources/iwx_sdk_csv_srcname_snowflake_from_{pytest.file_source}",
                    "environment_id": ValueStorage.env_id,
                    "storage_id": ValueStorage.storage_id,
                    "data_lake_schema": "PUBLIC",
                    "target_database_name": "PUBLIC",
                    "is_database_case_sensitive": False,
                    "is_schema_case_sensitive": False,
                    "staging_schema_name": None,
                    "is_staging_schema_case_sensitive": None,
                    "is_source_ingested": True})
            else:
                src_create_response = iwx_client.create_source(source_config={
                    "name": "iwx_sdk_srcname_csv_non_snowflake",
                    "type": "file",
                    "sub_type": "structured",
                    "data_lake_schema": "PUBLIC",
                    "data_lake_path": f"/iw/sources/iwx_sdk_srcname_csv_non_snowflake_from_{pytest.file_source}/",
                    "environment_id": ValueStorage.env_id,
                    "storage_id": ValueStorage.storage_id,
                    "is_source_ingested": True
                })
            print(src_create_response)
            assert src_create_response["result"]["status"].upper() == "SUCCESS"
            ValueStorage.source_id = src_create_response["result"]["source_id"]
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestFileSDKFlow::test_create_source"])
    def test_configure_source_connection(self):
        try:
            src_connection_details_body = {}
            if pytest.file_source == "s3":
                access_key = config["aws_secrets"]["access_id"]
                secret_key = config["aws_secrets"]["secret_key"]
                if access_key == "" or secret_key == "":
                    assert False
                src_connection_details_body = {
                    "source_base_path": "s3a://",
                    "storage": {
                        "storage_type": "cloud",
                        "cloud_type": "s3",
                        "access_id": access_key,
                        "secret_key": secret_key,
                        "access_type": "use_access_credentials",
                        "support_gov_cloud": False,
                        "endpoint": ""
                    },
                    "source_base_path": "",
                    "source_base_path_relative": "field-testing-cs"
                }
            elif pytest.file_source == "sftp":
                src_connection_details_body = {
                    "source_base_path": "/home/infoworks/",
                    "storage": {
                        "storage_type": "remote",
                        "sftp_host": "10.38.10.133",
                        "sftp_port": 22,
                        "username": "infoworks",
                        "auth_type": "private_key",
                        "credential": {
                            "type": "path",
                            "private_key_path": "/opt/infoworks/uploads/ingestion/634d4aa304c66a10724290b6/133_instance",
                            "private_key_file_details": {
                                "file_name": "133_instance",
                                "file_size": 2459,
                                "uploaded_by": "6RkfybTRQQByEey3v",
                                "file_path": "/opt/infoworks/uploads/ingestion/634d4aa304c66a10724290b6/133_instance"
                            }
                        }
                    },
                    "source_base_path_relative": "/"
                }
            response = iwx_client.configure_source_connection(ValueStorage.source_id, src_connection_details_body)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestFileSDKFlow::test_configure_source_connection"])
    def test_get_source_connection_details(self):
        try:
            response = iwx_client.get_source_connection_details(ValueStorage.source_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS" and len(response["result"]["response"]) > 0
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestFileSDKFlow::test_configure_source_connection"])
    def test_add_table_and_file_mappings_for_csv(self):
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
                    "target_relative_path": "/customers",
                    "deltaLake_table_name": "customers",
                    "source_file_type": "csv",
                    "ingest_subdirectories": True,
                    "source_relative_path": "/customers",
                    "exclude_filename_regex": "",
                    "include_filename_regex": "",
                    "is_archive_enabled": False,
                    "target_schema_name": "iwx_sdk_srcname_csv_non_snowflake",
                    "target_table_name": "customers"},
                "source": ValueStorage.source_id,
                "name": "customers"}
            response = iwx_client.add_table_and_file_mappings_for_csv(source_id=ValueStorage.source_id,
                                                                      file_mappings_config=data)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestFileSDKFlow::test_add_table_and_file_mappings_for_csv"])
    def test_source_metacrawl_job_poll(self):
        try:
            response = iwx_client.source_metacrawl_job_poll(ValueStorage.source_id, poll_timeout=300,
                                                            polling_frequency=15, retries=1)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS" and response["result"]["job_id"] not in ["", None]
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestFileSDKFlow::test_source_metacrawl_job_poll"])
    def test_configure_tables_and_tablegroups(self):
        try:
            f = open("config_jsons/csv_source_configuration.json")
            data = json.load(f)
            f.close()
            for item in data["configuration"]["table_configs"]:
                item["configuration"]["configuration"]["target_schema_name"] = item["configuration"]["configuration"][
                                                                                   "target_schema_name"] \
                                                                               + f"_from_{pytest.file_source}"
            response = iwx_client.configure_tables_and_tablegroups(ValueStorage.source_id,
                                                                   configuration_obj=data["configuration"])
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestFileSDKFlow::test_add_tables_to_source"])
    def test_list_of_tables(self):
        try:
            response = iwx_client.list_tables_in_source(ValueStorage.source_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
            tables = response["result"]["response"]["result"]
            for item in tables:
                print(item["id"])
                ValueStorage.table_ids.append(item["id"])
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestFileSDKFlow::test_list_of_tables"])
    def test_create_table_group(self):
        try:
            response = iwx_client.create_table_group(ValueStorage.source_id,
                                                     table_group_obj={
                                                         "environment_compute_template": {
                                                             "environment_compute_template_id": ValueStorage.compute_id},
                                                         "name": "tg_via_api",
                                                         "max_connections": 1,
                                                         "max_parallel_entities": 1,
                                                         "tables": [
                                                             {"table_id": ValueStorage.table_ids[0],
                                                              "connection_quota": 100}

                                                         ]
                                                     })
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"

        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestFileSDKFlow::test_create_table_group"])
    def test_update_table_group(self):
        try:
            response = iwx_client.update_table_group(ValueStorage.source_id, ValueStorage.table_group_id,
                                                     table_group_obj={
                                                         "environment_compute_template": {
                                                             "environment_compute_template_id": ValueStorage.compute_id},
                                                         "name": "tg_via_api_updated",
                                                         "max_connections": 1,
                                                         "max_parallel_entities": 1,
                                                         "tables": [
                                                             {"table_id": ValueStorage.table_ids[0],
                                                              "connection_quota": 100}
                                                         ]
                                                     })
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"

        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(
        depends=["TestFileSDKFlow::test_create_table_group", "TestFileSDKFlow::test_configure_tables_and_tablegroups"])
    def test_list_of_tablegroups(self):
        try:
            response = iwx_client.get_list_of_table_groups(ValueStorage.source_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS" and len(response["result"]["response"]) > 0
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(
        depends=["TestFileSDKFlow::test_create_table_group", "TestFileSDKFlow::test_configure_tables_and_tablegroups"])
    def test_ingestion_job(self):
        try:
            response = iwx_client.submit_source_job(ValueStorage.source_id, body={
                "job_type": "truncate_reload",
                "table_group_id": ValueStorage.table_group_id
            }, poll_timeout=300, polling_frequency=15, retries=1)
            assert response["result"]["status"].upper() == "SUCCESS" and response["result"]["job_id"] not in ["", None]
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(
        depends=["TestFileSDKFlow::test_create_table_group", "TestFileSDKFlow::test_configure_tables_and_tablegroups"])
    def test_delete_tablegroup(self):  # Fails
        try:
            response = iwx_client.delete_table_group(ValueStorage.source_id, ValueStorage.table_group_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_list_of_sources(self):
        try:
            response = iwx_client.get_list_of_sources()
            assert response["result"]["status"].upper() == "SUCCESS" and len(response["result"]["response"]) > 0
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestFileSDKFlow::test_create_source", "TestFileSDKFlow::test_list_of_tables"])
    def test_add_source_advanced_configuration(self):
        try:
            response = iwx_client.add_source_advanced_configuration(ValueStorage.source_id,
                                                                    config_body={"key": "file_preview_row_count",
                                                                                 "value": "10", "is_active": True})
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(
        depends=["TestFileSDKFlow::test_add_source_advanced_configuration", "TestFileSDKFlow::test_list_of_tables"])
    def test_update_source_advanced_configuration(self):
        try:
            response = iwx_client.update_source_advanced_configuration(ValueStorage.source_id,
                                                                       "file_preview_row_count",
                                                                       config_body={"key": "file_preview_row_count",
                                                                                    "value": "20", "is_active": True})
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestFileSDKFlow::test_update_source_advanced_configuration"])
    def test_list_advanced_configuration_of_sources(self):
        try:
            response_all = iwx_client.get_advanced_configuration_of_sources(ValueStorage.source_id)
            response_one = iwx_client.get_advanced_configuration_of_sources(ValueStorage.source_id,
                                                                            key="file_preview_row_count")
            print(response_all)
            assert response_all["result"]["status"].upper() == "SUCCESS" and response_one["result"][
                "status"].upper() == "SUCCESS" and len(response_all["result"]["response"]) > 0
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestFileSDKFlow::test_add_source_advanced_configuration"])
    def test_delete_source_advanced_configuration(self):
        try:
            response = iwx_client.delete_source_advanced_configuration(ValueStorage.source_id,
                                                                       key="file_preview_row_count")
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(
        depends=["TestFileSDKFlow::test_create_source", "TestFileSDKFlow::test_configure_tables_and_tablegroups"])
    def test_export_import_source_json(self):
        try:
            response = iwx_client.get_source_configurations_json_export(ValueStorage.source_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS" and len(response["result"]["response"]) > 0
            response = iwx_client.post_source_configurations_json_import(ValueStorage.source_id,
                                                                         response["result"]["response"]["result"])
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(
        depends=["TestFileSDKFlow::test_configure_tables_and_tablegroups", "TestFileSDKFlow::test_list_of_tables"])
    def test_get_table_configs(self):
        try:
            response = iwx_client.get_table_configurations(ValueStorage.source_id, ValueStorage.table_ids[0],
                                                           ingestion_config_only=False)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS" and len(
                response["result"]["response"]["result"]) > 0
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(
        depends=["TestFileSDKFlow::test_get_table_configs", "TestFileSDKFlow::test_configure_tables_and_tablegroups"])
    def test_update_table_configuration(self):
        try:
            response = iwx_client.update_table_configuration(ValueStorage.source_id,
                                                             ValueStorage.table_ids[0],
                                                             config_body={
                                                                 "name": "customer",
                                                                 "source": ValueStorage.source_id,
                                                                 "generate_history_view": True
                                                             })
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(
        depends=["TestFileSDKFlow::test_get_table_configs", "TestFileSDKFlow::test_configure_tables_and_tablegroups"])
    def test_add_table_advanced_configuration(self):
        try:
            response = iwx_client.add_table_advanced_configuration(ValueStorage.source_id,
                                                                   ValueStorage.table_ids[0],
                                                                   config_body={"key": "should_run_dedupe",
                                                                                "value": "false", "is_active": True})
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestFileSDKFlow::test_add_table_advanced_configuration",
                                     "TestFileSDKFlow::test_add_table_advanced_configuration"])
    def test_update_table_advanced_configuration(self):
        try:
            response = iwx_client.update_table_advanced_configuration(ValueStorage.source_id,
                                                                      ValueStorage.table_ids[0],
                                                                      "should_run_dedupe",
                                                                      config_body={"key": "should_run_dedupe",
                                                                                   "value": "true", "is_active": True})
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestFileSDKFlow::test_update_table_advanced_configuration"])
    def test_delete_table_advanced_configuration(self):
        try:
            response = iwx_client.delete_table_advanced_configuration(ValueStorage.source_id,
                                                                      ValueStorage.table_ids[0],
                                                                      "should_run_dedupe")
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestFileSDKFlow::test_configure_tables_and_tablegroups"])
    def test_get_table_export_configurations(self):
        try:
            response = iwx_client.get_table_export_configurations(ValueStorage.source_id, ValueStorage.table_ids[0],
                                                                  connection_only=False)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(
        depends=["TestFileSDKFlow::test_create_source", "TestFileSDKFlow::test_configure_tables_and_tablegroups"])
    def test_get_table_ingestion_metrics(self):
        try:
            response = iwx_client.get_table_ingestion_metrics(ValueStorage.source_id, ValueStorage.table_ids[0])
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False
