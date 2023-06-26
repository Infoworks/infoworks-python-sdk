import configparser
import pytest

from infoworks.sdk.client import InfoworksClientSDK
from test_cases.conftest import ValueStorage

# Infoworks Replicator Environment Details
replicator_host = ""
protocol = ""
port = ""
refresh_token = ""

config = configparser.ConfigParser()
config.read('config.ini')

# Initialising SDKClient
iwx_client = InfoworksClientSDK()
iwx_client.initialize_client_with_defaults(protocol, replicator_host, port, refresh_token)


class TestReplicationFlow:
    @pytest.mark.dependency()
    def test_create_source(self):
        try:
            source_config = {
                "name": "SDK_Replicator_Test_Source",
                "type": "hive",
                "properties": {
                    "hadoop_version": "2.x",
                    "hdfs_root": "gs://infoworksxxx/",
                    "hive_metastore_url": "thrift://10.17.2.85:9083",
                    "network_throttling": 4000,
                    "temp_directory": "gs://infoworksxxx//iwxr/temp",
                    "output_directory": "gs://infoworksxxx//iwxr/output"
                },
                "advanced_configurations": [
                    {
                        "key": "SECURE_CLUSTER_PROPERTIES",
                        "value": "hive.metastore.sasl.enabled=false"
                    }
                ]
            }
            create_source_response = iwx_client.create_replicator_source(source_config)
            print(f"create_replicator_source() - Response: {create_source_response}")
            if create_source_response['result']['status'].lower() == "success":
                source_id = create_source_response['result']['response'].get('result', None)
                ValueStorage.replicator_source_id = source_id
                assert True
            else:
                raise Exception(f"Status Returned as Failed")
        except Exception as create_source_error:
            print(f"Failed to create Source: {create_source_error}")
            assert False

    @pytest.mark.dependency()
    def test_create_destination(self):
        try:
            destination_config = {
                "name": "SDK_Replicator_Test_Destination",
                "type": "hive",
                "properties": {
                    "hadoop_version": "2.x",
                    "hdfs_root": "wasb://hdfsstorage.blob.core.windows.net/",
                    "hive_metastore_url": "thrift://microsoft.com:9083",
                    "temp_directory": "/root/iwxr/temp",
                    "output_directory": "/root/iwxr/output"
                },
                "advanced_configurations": [
                    {
                        "key": "DATABASE_FILTER",
                        "value": ".*"
                    }
                ]
            }
            create_destination_response = iwx_client.create_replicator_destination(destination_config)
            print(f"create_destination_response() - Response: {create_destination_response}")
            if create_destination_response['result']['status'].lower() == "success":
                destination_id = create_destination_response['result']['response'].get('result', None)
                ValueStorage.replicator_destination_id = destination_id
                assert True
            else:
                raise Exception(f"Status Returned as Failed")
        except Exception as create_destination_error:
            print(f"Failed to create Source: {create_destination_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_create_source"])
    def test_list_of_replicator_sources(self):
        try:
            list_source_response = iwx_client.get_list_of_replicator_sources()
            print(f"get_list_of_replicator_sources() - Response: {list_source_response}")
            assert list_source_response['result']['status'].lower() == "success" and \
                   list_source_response['result']['response']['result']
        except Exception as list_sources_error:
            print(f"Failed to list Sources: {list_sources_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_create_source"])
    def test_get_replicator_source(self):
        try:
            get_source_response = iwx_client.get_replicator_source(ValueStorage.replicator_source_id)
            print(f"get_source_response() - Response: {get_source_response}")

            assert get_source_response['result']['status'].lower() == "success" and \
                   get_source_response['result']['response']['result']
        except Exception as get_source_error:
            print(f"Failed to get source: {get_source_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_create_source"])
    def test_update_replicator_source(self):
        try:
            updated_source_config = {
                "name": "SDK_Replicator_Test_Source_Updated",
                "type": "hive",
                "properties": {
                    "hadoop_version": "2.x",
                    "hdfs_root": "gs://infoworksxxx/",
                    "hive_metastore_url": "thrift://10.17.2.85:9083",
                    "network_throttling": 4000,
                    "temp_directory": "gs://infoworksxxx//iwxr/temp",
                    "output_directory": "gs://infoworksxxx//iwxr/output"
                },
                "advanced_configurations": [
                    {
                        "key": "SECURE_CLUSTER_PROPERTIES",
                        "value": "hive.metastore.sasl.enabled=false"
                    }
                ]
            }
            update_source_response = iwx_client.update_replicator_source(ValueStorage.replicator_source_id,
                                                                         updated_source_config)
            print(f"update_replicator_source() - Response: {update_source_response}")

            assert update_source_response['result']['status'].lower() == "success"
        except Exception as update_source_error:
            print(f"Failed to update source: {update_source_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_create_destination"])
    def test_list_of_replicator_destinations(self):
        try:
            list_destination_response = iwx_client.get_list_of_replicator_destinations()
            print(f"get_list_of_replicator_destinations() - Response: {list_destination_response}")
            assert list_destination_response['result']['status'].lower() == "success" and \
                   list_destination_response['result']['response']['result']
        except Exception as list_destination_error:
            print(f"Failed to list Destinations: {list_destination_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_create_destination"])
    def test_get_replicator_destination(self):
        try:
            get_destination_response = iwx_client.get_replicator_destination(ValueStorage.replicator_destination_id)
            print(f"get_replicator_destination() - Response: {get_destination_response}")

            assert get_destination_response['result']['status'].lower() == "success" and \
                   get_destination_response['result']['response']['result']
        except Exception as get_destination_error:
            print(f"Failed to get destination: {get_destination_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_create_destination"])
    def test_update_replicator_destination(self):
        try:
            updated_destination_config = {
                "name": "SDK_Replicator_Test_Destination_Updated",
                "type": "hive",
                "properties": {
                    "hadoop_version": "2.x",
                    "hdfs_root": "wasb://hdfsstorage.blob.core.windows.net/",
                    "hive_metastore_url": "thrift://microsoft.com:9083",
                    "temp_directory": "/root/iwxr/temp",
                    "output_directory": "/root/iwxr/output"
                },
                "advanced_configurations": [
                    {
                        "key": "DATABASE_FILTER",
                        "value": ".*"
                    }
                ]
            }
            update_destination_response = iwx_client.update_replicator_destination(
                ValueStorage.replicator_destination_id, updated_destination_config)
            print(f"update_replicator_destination() - Response: {update_destination_response}")

            assert update_destination_response['result']['status'].lower() == "success"
        except Exception as update_destination_error:
            print(f"Failed to update destination: {update_destination_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_create_source"])
    def test_add_replicator_sources_to_domain(self):
        try:
            source_access_config = {"entity_details": [{"entity_id": ValueStorage.replicator_source_id,
                                                        "schema_filter": ".*"}]}
            source_access_response = iwx_client.add_replicator_sources_to_domain(
                domain_id=ValueStorage.replicator_domain_id, config=source_access_config)
            print(f"add_replicator_sources_to_domain() - Response: {source_access_response}")
            assert source_access_response['result']['status'].lower() == "success"
        except Exception as add_source_to_domain_error:
            print(f"Failed to add source to domain: {add_source_to_domain_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_create_destination"])
    def test_add_replicator_destinations_to_domain(self):
        try:
            destination_access_config = {"entity_details": [{"entity_id": ValueStorage.replicator_destination_id,
                                                             "schema_filter": ".*"}]}
            destination_access_response = iwx_client.add_replicator_destinations_to_domain(
                domain_id=ValueStorage.replicator_domain_id, config=destination_access_config)
            print(f"add_replicator_destinations_to_domain() - Response: {destination_access_response}")
            assert destination_access_response['result']['status'].lower() == "success"
        except Exception as add_destination_to_domain_error:
            print(f"Failed to add destination to domain: {add_destination_to_domain_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_create_source",
                                     "TestReplicationFlow::test_create_destination"])
    def test_create_replication_definition(self):
        try:
            definition_config = {
                "name": "SDK_Replication_Test_Definition",
                "domain_id": ValueStorage.replicator_domain_id,
                "replicator_source_name": "SDK_Replicator_Test_Source ",
                "replicator_destination_name": "SDK_Replicator_Test_Destination",
                "replicator_source_id": ValueStorage.replicator_source_id,
                "replicator_destination_id": ValueStorage.replicator_destination_id,
                "job_bandwidth_mb": 1000,
                "replication_type": "batch",
                "copy_parallelism_factor": 4,
                "metastore_parallelism_factor": 4,
                "advanced_configurations": [
                    {
                        "key": "DATABASE_FILTER",
                        "value": ".*"
                    }
                ]
            }
            create_definition_response = iwx_client.create_replicator_definition(definition_config)
            print(f"create_replicator_definition() - Response: {create_definition_response}")

            if create_definition_response['result']['status'] == "success":
                definition_id = create_definition_response['result']['response'].get('result', None)
                ValueStorage.replicator_definition_id = definition_id
                assert True
            else:
                raise Exception(f"Status Returned as Failed")
        except Exception as create_definition_error:
            print(f"Failed to create replication definition : {create_definition_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_create_replication_definition"])
    def test_list_of_replicator_definitions(self):
        try:
            list_definitions_response = iwx_client.get_list_of_replicator_definitions(ValueStorage.replicator_domain_id)
            print(f"get_list_of_replicator_definitions() - Response: {list_definitions_response}")

            assert list_definitions_response['result']['status'].lower() == "success" and \
                   list_definitions_response['result']['response']['result']
        except Exception as list_definition_error:
            print(f"Failed to list Definitions: {list_definition_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_create_replication_definition"])
    def test_get_replicator_definition(self):
        try:
            get_definition_response = iwx_client.get_replicator_definition(
                ValueStorage.replicator_domain_id, ValueStorage.replicator_definition_id)
            print(f"get_replicator_definition() - Response: {get_definition_response}")

            assert get_definition_response['result']['status'].lower() == "success" and \
                   get_definition_response['result']['response']['result']
        except Exception as get_definition_error:
            print(f"Failed to get source: {get_definition_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_create_replication_definition"])
    def test_update_replicator_definition(self):
        try:
            updated_definition_config = {
                "name": "SDK_Replication_Test_Definition_Updated",
                "domain_id": ValueStorage.replicator_domain_id,
                "replicator_source_name": "SDK_Replicator_Test_Source_Updated",
                "replicator_destination_name": "SDK_Replicator_Test_Destination_Updated",
                "replicator_source_id": ValueStorage.replicator_source_id,
                "replicator_destination_id": ValueStorage.replicator_destination_id,
                "job_bandwidth_mb": 1000,
                "replication_type": "batch",
                "copy_parallelism_factor": 4,
                "metastore_parallelism_factor": 4,
                "advanced_configurations": [
                    {
                        "key": "DATABASE_FILTER",
                        "value": ".*"
                    }
                ]
            }
            update_definition_response = iwx_client.update_replicator_definition(
                ValueStorage.replicator_domain_id, ValueStorage.replicator_definition_id, updated_definition_config)
            print(f"update_replicator_definition() - Response: {update_definition_response}")

            assert update_definition_response['result']['status'].lower() == "success"
        except Exception as update_definition_error:
            print(f"Failed to update definition: {update_definition_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_create_replication_definition"])
    def test_submit_replication_meta_crawl_job(self):
        try:
            crawl_metadata_response = iwx_client.submit_replication_meta_crawl_job(ValueStorage.replicator_domain_id,
                                                                                   ValueStorage.replicator_source_id)
            print(f"submit_replication_meta_crawl_job() - Response: {crawl_metadata_response}")
            metadata_crawl_job_status = crawl_metadata_response['result']['response'].get('result', {}).get('status')
            assert crawl_metadata_response['result']['status'].lower() == 'success' and \
                   metadata_crawl_job_status == "completed"
        except Exception as crawl_error:
            print(f"Failed to Crawl Source Metadata: {crawl_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_submit_replication_meta_crawl_job"])
    def test_get_replicator_source_table_id_from_name(self):
        try:
            schema_name = 'default'
            table_name = 'student'
            table_response = iwx_client.get_replicator_source_table_id_from_name(ValueStorage.replicator_source_id,
                                                                                 schema_name, table_name)
            print(f"get_replicator_source_table_id_from_name() - Response: {table_response}")
            table_id = table_response['result']['response'].get('id')
            if table_id:
                ValueStorage.replicator_table_ids.append(table_id)
            assert table_response['result']['status'].lower() == "success" and table_id
        except Exception as table_error:
            print(f"Failed to get table id from name: {table_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_get_replicator_source_table_id_from_name"])
    def test_add_tables_to_replicator_definition(self):
        try:
            schema_name = 'default'
            table_name = 'student'
            add_tables_config = {
                "selected_objects": [
                    {"id": ValueStorage.replicator_table_ids[0],
                     "table_name": table_name,
                     "schema_name": schema_name}
                ]
            }
            add_tables_response = iwx_client.add_tables_to_replicator_definition(
                ValueStorage.replicator_domain_id, ValueStorage.replicator_definition_id, add_tables_config)
            print(f"add_tables_to_replicator_definition() - Response: {add_tables_response}")
            assert add_tables_response['result']['status'].lower() == "success"
        except Exception as add_tables_error:
            print(f"Failed to add tables to replicator definition : {add_tables_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_add_tables_to_replicator_definition"])
    def test_create_replication_schedule(self):
        try:
            schedule_config = {
                "entity_type": "replicate_definition",
                "properties": {
                    "schedule_status": "enabled",
                    "repeat_interval": "day",
                    "starts_on": "08/30/2022",
                    "time_hour": 7,
                    "time_min": 20,
                    "ends": "off",
                    "repeat_every": "1"
                },
                "scheduler_username": "admin@infoworks.io",
                "scheduler_auth_token": "1TGf748u1I0mvZj3efuwr8y"
            }
            schedule_response = iwx_client.create_replication_schedule(
                ValueStorage.replicator_domain_id, ValueStorage.replicator_definition_id, schedule_config)
            print(f"create_replication_schedule() - Response: {schedule_response}")
            schedule_id = schedule_response['result']['response'].get('result')
            ValueStorage.replicator_definition_schedule_id = schedule_id
            assert schedule_response['result']['status'].lower() == "success" and schedule_id
        except Exception as schedule_error:
            print(f"Failed to configure schedule: {schedule_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_create_replication_schedule"])
    def test_get_replication_schedule(self):
        try:
            get_schedule_response = iwx_client.get_replication_schedule(
                ValueStorage.replicator_domain_id, ValueStorage.replicator_definition_id,
                ValueStorage.replicator_definition_schedule_id)
            print(f"get_replication_schedule() - Response: {get_schedule_response}")

            assert get_schedule_response['result']['status'].lower() == "success" and \
                   get_schedule_response['result']['response']['result']
        except Exception as get_schedule_error:
            print(f"Failed to get schedule: {get_schedule_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_create_replication_schedule"])
    def test_update_replication_schedule(self):
        try:
            updated_schedule_config = {
                "entity_type": "replicate_definition",
                "properties": {
                    "schedule_status": "enabled",
                    "repeat_interval": "day",
                    "starts_on": "08/30/2022",
                    "time_hour": 6,
                    "time_min": 20,
                    "ends": "off",
                    "repeat_every": "1"
                },
                "scheduler_username": "admin@infoworks.io",
                "scheduler_auth_token": "1TGf748u1I0mvZj3efuwr8y"
            }
            update_schedule_response = iwx_client.update_replication_schedule(
                ValueStorage.replicator_domain_id, ValueStorage.replicator_definition_id,
                ValueStorage.replicator_definition_schedule_id, updated_schedule_config)
            print(f"update_replication_schedule() - Response: {update_schedule_response}")

            assert update_schedule_response['result']['status'].lower() == "success"
        except Exception as update_schedule_error:
            print(f"Failed to update schedule: {update_schedule_error}")
            assert False

    '''
    @pytest.mark.dependency(depends=["TestReplicationFlow::test_add_tables_to_replicator_definition"])
    def test_submit_replication_data_job(self):
        try:
            data_replication_job_response = iwx_client.submit_replication_data_job(
                ValueStorage.replicator_domain_id, ValueStorage.replicator_definition_id)
            print(f"submit_replication_data_job() - Response: {data_replication_job_response}")
            job_id = data_replication_job_response['result']['response'].get('result', {}).get('id')
            assert data_replication_job_response['result']['status'].lower() == "success" and job_id
        except Exception as replication_error:
            print(f"Failed to trigger/monitor Data Replication Job: {replication_error}")
            assert False
    '''

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_update_replication_schedule"])
    def test_delete_replication_schedule(self):
        try:
            delete_schedule_response = iwx_client.delete_replication_schedule(
                ValueStorage.replicator_domain_id, ValueStorage.replicator_definition_id,
                ValueStorage.replicator_definition_schedule_id)
            print(f"delete_replication_schedule() - Response: {delete_schedule_response}")

            assert delete_schedule_response['result']['status'].lower() == "success"
        except Exception as delete_schedule_error:
            print(f"Failed to delete schedule: {delete_schedule_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_delete_replication_schedule"])
    def test_delete_replicator_definition(self):
        try:
            delete_definition_response = iwx_client.delete_replicator_definition(ValueStorage.replicator_domain_id,
                                                                                 ValueStorage.replicator_definition_id)
            print(f"delete_replicator_definition() - Response: {delete_definition_response}")

            assert delete_definition_response['result']['status'].lower() == "success"
        except Exception as delete_definition_error:
            print(f"Failed to delete definition: {delete_definition_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_delete_replicator_definition"])
    def test_delete_replicator_source(self):
        try:
            delete_source_response = iwx_client.delete_replicator_source(ValueStorage.replicator_source_id)
            print(f"delete_replicator_source() - Response: {delete_source_response}")

            assert delete_source_response['result']['status'].lower() == "success"
        except Exception as delete_source_error:
            print(f"Failed to delete source: {delete_source_error}")
            assert False

    @pytest.mark.dependency(depends=["TestReplicationFlow::test_delete_replicator_definition"])
    def test_delete_replicator_destination(self):
        try:
            delete_destination_response = iwx_client.delete_replicator_destination(ValueStorage.replicator_destination_id)
            print(f"delete_replicator_destination() - Response: {delete_destination_response}")

            assert delete_destination_response['result']['status'].lower() == "success"
        except Exception as delete_destination_error:
            print(f"Failed to delete destination: {delete_destination_error}")
            assert False
