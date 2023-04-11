import pytest
import configparser
from infoworks.sdk.client import InfoworksClientSDK
from infoworks.error import *
from test_cases.conftest import ValueStorage

config = configparser.ConfigParser()
config.read('config.ini')
refresh_token = ""
iwx_client = InfoworksClientSDK()
iwx_client.initialize_client_with_defaults("https", "host_name", "443", refresh_token)


class TestAdminClientSDK:

    @pytest.mark.dependency()
    def test_list_users(self):
        try:
            response = iwx_client.list_users()
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except AdminError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_create_new_user(self):
        try:
            response = iwx_client.create_new_user({
                "profile": {
                    "name": "iwx_sdk_user",
                    "email": "iwx_sdk_user@infoworks.io",
                    "needs_password_reset": False
                },
                "roles": ["modeller"],
                "password": "ABCDabcd1234##"
            })
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except AdminError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_create_new_user"])
    def test_get_user_details(self):
        try:
            response = iwx_client.get_user_details(params={"filter": {"profile.email": "iwx_sdk_user@infoworks.io"}})
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS" and len(
                response["result"]["response"]["result"]) > 0
            pytest.user_id = response["result"]["response"]["result"][0]["id"]
        except AdminError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_create_new_user"])
    def test_update_the_user(self):
        try:
            response = iwx_client.update_the_user(user_id=pytest.user_id, data={
                "profile": {
                    "name": "iwx_sdk_user_update",
                }})
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except AdminError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_create_new_user"])
    def test_add_domains_to_user(self):
        try:
            response = iwx_client.add_domains_to_user(domain_id=ValueStorage.domain_id, user_id=pytest.user_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except AdminError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_create_new_user",
                                     "TestAdminClientSDK::test_update_the_user",
                                     "TestAdminClientSDK::test_get_user_details",
                                     "TestAdminClientSDK::test_add_domains_to_user"])
    def test_delete_the_user(self):
        try:
            response = iwx_client.delete_the_user(user_id=pytest.user_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except AdminError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_get_environment_details(self):
        try:
            response = iwx_client.get_environment_details()
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS" and len(
                response["result"]["response"]["result"]) > 0
        except AdminError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_get_storage_details(self):
        try:
            response = iwx_client.get_storage_details(ValueStorage.env_id)
            assert response["result"]["status"].upper() == "SUCCESS" and len(
                response["result"]["response"]["result"]) > 0
        except AdminError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_get_compute_template_details(self):
        try:
            response = iwx_client.get_compute_template_details(ValueStorage.env_id)
            assert response["result"]["status"].upper() == "SUCCESS" and len(
                response["result"]["response"]["result"]) > 0
        except AdminError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_get_interactive_compute_template_details(self):
        try:
            response = iwx_client.get_compute_template_details(ValueStorage.env_id, is_interactive=True)
            assert response["result"]["status"].upper() == "SUCCESS" and len(
                response["result"]["response"]["result"]) > 0
        except AdminError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_get_abc_metrics(self):
        try:
            result = iwx_client.get_abc_job_metrics(time_range_for_jobs_in_mins=1000)
            assert result is not None
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_get_environment_id_from_name(self):
        try:
            response = iwx_client.get_environment_id_from_name(environment_name=ValueStorage.environment_name)
            print(response)
            assert response["result"]["response"]["environment_id"] is not None
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_get_environment_default_warehouse(self):
        try:
            response = iwx_client.get_environment_default_warehouse(environment_id=ValueStorage.env_id)
            print(response)
            assert response["result"]["response"]["default_warehouse"] is not None
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_get_compute_id_from_name(self):
        try:
            response = iwx_client.get_compute_id_from_name(environment_id=ValueStorage.env_id,
                                                           compute_name=ValueStorage.compute_name)
            print(response)
            assert response["result"]["response"]["compute_id"] is not None
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_get_storage_id_from_name(self):
        try:
            response = iwx_client.get_storage_id_from_name(environment_id=ValueStorage.env_id,
                                                           storage_name=ValueStorage.storage_name)
            print(response)
            assert response["result"]["response"]["storage_id"] is not None
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_create_source_extension(self):
        try:
            response = iwx_client.create_source_extension(data={"name": "iwx_sdk_test", 'extension_type': 'Hive_UDF',
                                                                'ingestion_extension_type': 'source_extension',
                                                                'registered_udf': True, 'transformations': [
                    {'alias': 'test', 'additional_params_count': 0, 'params_details': []},
                    {'alias': 'upper', 'additional_params_count': 0, 'params_details': []},
                    {'alias': 'nvl2', 'additional_params_count': 2,
                     'params_details': [{'value': 'case when 1=1 then 1 else 0 end'}, {'value': 'null'}]},
                    {'alias': 'nvl', 'additional_params_count': 1, 'params_details': [{'value': ''}]}]})
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
            pytest.source_extension_id = response["result"]["response"]["result"]["id"]
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_create_source_extension"])
    def test_get_source_extension(self):
        try:
            response = iwx_client.get_source_extension(extension_id=pytest.source_extension_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_get_source_extension"])
    def test_update_source_extension(self):
        try:
            response = iwx_client.update_source_extension(extension_id=pytest.source_extension_id,
                                                          data={"name": "iwx_sdk_test_updated"})
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_update_source_extension"])
    def test_delete_source_extension(self):
        try:
            response = iwx_client.delete_source_extension(extension_id=pytest.source_extension_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_create_data_connection(self):
        try:
            config_body = {
                "name": "Snowflake-connection_via_api",
                "type": "TARGET",
                "sub_type": "SNOWFLAKE",
                "properties": {
                    "url": "",
                    "account": "",
                    "username": "",
                    "password": "",
                    "additional_params": "",
                    "warehouse": "DEMO_WH"
                }
            }
            response = iwx_client.create_data_connection(config_body)
            print(response)
            pytest.data_connection_id = response["result"]["response"]["result"]["id"]
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_create_data_connection"])
    def test_update_data_connection(self):
        try:
            config_body = {
                "name": "Snowflake-connection_via_api_updated",
                "type": "TARGET",
                "sub_type": "SNOWFLAKE",
                "properties": {
                    "url": "",
                    "account": "",
                    "username": "",
                    "password": "",
                    "additional_params": "",
                    "warehouse": "DEMO_WH"
                }
            }
            response = iwx_client.update_data_connection(pytest.data_connection_id, config_body)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_create_data_connection"])
    def test_get_data_connections(self):
        try:
            response = iwx_client.get_data_connection()
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_get_data_connections"])
    def test_delete_data_connection(self):
        try:
            response = iwx_client.delete_data_connection(data_connection_id=pytest.data_connection_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_list_service_authentication(self):
        try:
            response = iwx_client.list_service_authentication()
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_create_service_authentication(self):
        try:
            response = iwx_client.create_service_authentication(data={
                "name": "iw-azure-cs-db-cluster-sp-iwx",
                "service_type": "azure",
                "authentication_type": "service_principal",
                "authentication_properties": {
                    "subscription_id": "xxx",
                    "client_id": "xxx",
                    "client_secret": "xxx",
                    "object_id": "xxx",
                    "tenant_id": "xxx"
                }
            })
            print(response)
            pytest.service_auth_id = response["result"]["response"]["result"]["id"]
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_create_service_authentication"])
    def test_get_service_authentication_details(self):
        try:
            response = iwx_client.get_service_authentication_details(service_auth_id=pytest.service_auth_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_get_service_authentication_details"])
    def test_update_service_authentication_details(self):
        try:
            response = iwx_client.update_service_authentication_details(service_auth_id=pytest.service_auth_id, data={
                "description": "description updated"
            })
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_list_secret_stores(self):
        try:
            response = iwx_client.list_secret_stores()
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_create_secret_store(self):
        try:
            response = iwx_client.create_secret_store(data={
                "name": "iwx-sp-secret-store-iwx",
                "service_type": "azure",
                "key_vault_uri": "https://iwazurecsdbkey.vault.azure.net/",
                "service_authentication": "64182fac35298f0d05a8b689",
            })
            print(response)
            pytest.secret_store_id = response["result"]["response"]["result"]["id"]
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_create_secret_store"])
    def test_get_secret_store_details(self):
        try:
            response = iwx_client.get_secret_store_details(secret_store_id=pytest.secret_store_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_get_secret_store_details"])
    def test_update_secret_store_details(self):
        try:
            response = iwx_client.update_secret_store_details(secret_store_id=pytest.secret_store_id, data={
                "description": "description updated"
            })
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_update_secret_store_details"])
    def test_delete_secret_store(self):
        try:
            response = iwx_client.delete_secret_store(secret_store_id=pytest.secret_store_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_list_secrets(self):
        try:
            response = iwx_client.list_secrets()
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_create_secret(self):
        try:
            response = iwx_client.create_secret(data={
                "name": "mongo-pg-pwd-iwx",
                "secret_store": "64182fd27eeb1c40de2b6007",
                "secret_name": "mongo-pg-pwd-iwx"
            })
            print(response)
            pytest.secret_id = response["result"]["response"]["result"]["id"]
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_create_secret"])
    def test_get_secret_details(self):
        try:
            response = iwx_client.get_secret_details(secret_id=pytest.secret_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_get_secret_details"])
    def test_update_secret_details(self):
        try:
            response = iwx_client.update_secret_details(secret_id=pytest.secret_id, data={
                "description": "description updated"
            })
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_update_secret_details"])
    def test_delete_secret(self):
        try:
            response = iwx_client.delete_secret(secret_id=pytest.secret_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestAdminClientSDK::test_delete_secret"])
    def test_delete_service_authentication_details(self):
        try:
            response = iwx_client.delete_service_authentication(service_auth_id=pytest.service_auth_id)
            print(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except Exception as e:
            print(str(e))
            assert False
