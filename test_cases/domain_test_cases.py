import sys
#sys.path.insert(0,"/Users/nitin.bs/PycharmProjects/infoworks-python-sdk")
from infoworks.sdk.client import InfoworksClientSDK
from infoworks.error import *
from test_cases.conftest import ValueStorage
import pytest
import logging
global logger
refresh_token = ""
iwx_client = InfoworksClientSDK()
iwx_client.initialize_client_with_defaults("https", "", "443", refresh_token)
pytest.environment_id = "662f30700730090008d92fc5"
pytest.domains = {}
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

class TestDomainFlow():
    @pytest.mark.dependency()
    def test_create_domain(self):
        try:
            response_1 = iwx_client.create_domain({
                "name": "iwx_sdk_domain1",
                "description": "Example Domain",
                "environment_ids": [pytest.environment_id]
            })
            logger.info(response_1)
            response_2 = iwx_client.create_domain({
                "name": "iwx_sdk_domain2",
                "description": "Example Domain",
                "environment_ids": [pytest.environment_id]
            })
            logger.info(response_2)
            assert response_1["result"]["status"].upper() == "SUCCESS" and response_2["result"][
                "status"].upper() == "SUCCESS"
            assert True
            pytest.domains["iwx_sdk_domain1"] = response_1["result"]["response"]["result"]["id"]
            pytest.domains["iwx_sdk_domain2"] = response_2["result"]["response"]["result"]["id"]
        except DomainError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestDomainFlow::test_create_domain"])
    def test_add_domains_to_user(self):
        try:
            response_1 = iwx_client.add_domains_to_user(pytest.domains.get("iwx_sdk_domain1"),
                                                        user_email="admin@infoworks.io")

            logger.info(response_1)
            response_2 = iwx_client.add_domains_to_user(pytest.domains.get("iwx_sdk_domain2"),
                                                        user_email="admin@infoworks.io")
            logger.info(response_2)
            assert response_1["result"]["status"].upper() == "SUCCESS" and response_2["result"][
                "status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestDomainFlow::test_add_domains_to_user"])
    def test_list_domains(self):
        try:
            response_1 = iwx_client.list_domains()
            logger.info(response_1)
            response_2 = iwx_client.list_domains(params={"filter": {"name": "iwx_sdk_domain2"}})
            logger.info(response_2)
            assert response_1["result"]["status"].upper() == "SUCCESS" and len(response_1["result"]["response"]) > 0 and \
                   response_2["result"]["status"].upper() == "SUCCESS"
        except DomainError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestDomainFlow::test_add_domains_to_user"])
    def test_get_domain_details(self):
        try:
            response = iwx_client.get_domain_details(pytest.domains.get("iwx_sdk_domain1"))
            logger.info(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except DomainError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestDomainFlow::test_get_domain_details"])
    def test_update_domain(self):
        try:
            response = iwx_client.update_domain(pytest.domains.get("iwx_sdk_domain2"), {
                "name": "iwx_sdk_domain2_updated_to_delete",
                "description": "Example Update Domain",
                "environment_ids": [pytest.environment_id]
            })
            logger.info(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestDomainFlow::test_add_domains_to_user"])
    def test_get_domain_id(self):
        try:
            response = iwx_client.get_domain_id("iwx_sdk_domain1")
            logger.info(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except DomainError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestDomainFlow::test_add_domains_to_user"])
    def test_add_source_to_domain(self):
        try:
            response = iwx_client.add_source_to_domain(pytest.domains.get("iwx_sdk_domain1"), {
                "entity_ids": [ValueStorage.source_id]
            })
            logger.info(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestDomainFlow::test_add_source_to_domain"])
    def test_get_sources_associated_with_domain(self):
        try:
            response = iwx_client.get_sources_associated_with_domain(pytest.domains.get("iwx_sdk_domain1"))
            logger.info(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestDomainFlow::test_get_sources_associated_with_domain"])
    def test_remove_source_from_domain(self):
        try:
            sources_response = iwx_client.get_sources_associated_with_domain(pytest.domains.get("iwx_sdk_domain1"))
            logger.info(sources_response)
            sources = sources_response["result"]["response"]["result"]
            sources = [source["id"] for source in sources]
            config_body={
                "entity_ids": sources
            }
            print("config_body",config_body)
            response = iwx_client.remove_source_from_domain(domain_id=pytest.domains.get("iwx_sdk_domain1"),config_body=config_body)
            logger.info(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency()
    def test_get_parent_entity_id(self):
        try:
            response = iwx_client.get_parent_entity_id(json_obj={"entity_id": ValueStorage.workflow_id, "entity_type": "workflow"})
            logger.info(response)
            assert response["result"]["status"].upper() == "SUCCESS" and response["result"]["response"]["parent_entity_id"]==ValueStorage.domain_id
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestDomainFlow::test_update_domain"])
    def test_get_domain_id(self):
        try:
            response = iwx_client.get_domain_id(domain_name="iwx_sdk_domain1")
            logger.info(response)
            assert response["result"]["status"].upper() == "SUCCESS" and response["result"]["response"]["domain_id"]==pytest.domains.get("iwx_sdk_domain1")
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestDomainFlow::test_update_domain"])
    def test_add_update_pipeline_extensions_to_domain(self):
        try:
            create_response = iwx_client.add_update_pipeline_extensions_to_domain(domain_id=pytest.domains.get("iwx_sdk_domain1"),config_body={
                 "entity_ids": [ValueStorage.pipeline_extension_id]
                },action_type="create")
            logger.info(create_response)
            update_response = iwx_client.add_update_pipeline_extensions_to_domain(domain_id=pytest.domains.get("iwx_sdk_domain1"), config_body={
                "entity_ids": [ValueStorage.pipeline_extension_id]
            }, action_type="update")
            logger.info(update_response)
            assert create_response["result"]["status"].upper() == "SUCCESS" and update_response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestDomainFlow::test_add_update_pipeline_extensions_to_domain"])
    def test_get_pipeline_extensions_associated_with_domain(self):
        try:
            response = iwx_client.get_pipeline_extensions_associated_with_domain(domain_id=pytest.domains.get("iwx_sdk_domain1"))
            logger.info(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestDomainFlow::test_get_pipeline_extensions_associated_with_domain"])
    def test_delete_pipeline_extensions_from_domain(self):
        try:
            response = iwx_client.delete_pipeline_extensions_from_domain(domain_id=pytest.domains.get("iwx_sdk_domain1"),config_body = {
        "entity_ids": [ValueStorage.pipeline_extension_id]
    })
            logger.info(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestDomainFlow::test_get_pipeline_extensions_associated_with_domain"])
    def test_modify_advanced_config_for_domain(self):
        try:
            adv_config_body_example = {
                "key": "test_key",
                "value": "test_value",
                "description": "",
                "is_active": True
            }
            create_response = iwx_client.modify_advanced_config_for_domain(domain_id=pytest.domains.get("iwx_sdk_domain1"),
                                                                                  action_type="create",
                                                                                  adv_config_body=adv_config_body_example)
            logger.info(create_response)
            adv_config_body_example["value"]="test_value_updated"
            update_response = iwx_client.modify_advanced_config_for_domain(domain_id=pytest.domains.get("iwx_sdk_domain1"),
                                                                                         action_type="update",
                                                                                         key = "test_key",
                                                                                         adv_config_body=adv_config_body_example)
            logger.info(update_response)
            assert create_response["result"]["status"].upper() == "SUCCESS" and update_response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False


    @pytest.mark.dependency(depends=["TestDomainFlow::test_modify_advanced_config_for_domain"])
    def test_get_or_delete_advance_config_details_for_domain(self):
        try:
            get_response = iwx_client.get_or_delete_advance_config_details_for_domain(domain_id=pytest.domains.get("iwx_sdk_domain1"),
                                                                                      action_type="get",
                                                                                      key = "test_key")
            logger.info(get_response)
            delete_response = iwx_client.get_or_delete_advance_config_details_for_domain(domain_id=pytest.domains.get("iwx_sdk_domain1"),
                                                                                      action_type="delete",
                                                                                      key = "test_key")
            logger.info(delete_response)
            assert get_response["result"]["status"].upper() == "SUCCESS" and delete_response["result"]["status"].upper() == "SUCCESS"
        except SourceError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestDomainFlow::test_update_domain"])
    def test_get_accessible_pipelines_under_domain(self):
        try:
            response = iwx_client.get_accessible_pipelines_under_domain(ValueStorage.domain_id)
            logger.info(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except DomainError as e:
            print(str(e))
            assert False

    @pytest.mark.dependency(depends=["TestDomainFlow::test_update_domain"])
    def test_get_accessible_workflows_under_domain(self):
        try:
            response = iwx_client.get_accessible_workflows_under_domain(ValueStorage.domain_id)
            logger.info(response)
            assert response["result"]["status"].upper() == "SUCCESS"
        except DomainError as e:
            print(str(e))
            assert False

    # @pytest.mark.dependency(depends=["TestDomainFlow::test_update_domain"])
    # def test_delete_domain(self):
    #     try:
    #         iwx_sdk_domain1_response = iwx_client.delete_domain(pytest.domains.get("iwx_sdk_domain1"))
    #         logger.info(iwx_sdk_domain1_response)
    #         iwx_sdk_domain2_response = iwx_client.delete_domain(pytest.domains.get("iwx_sdk_domain2"))
    #         logger.info(iwx_sdk_domain2_response)
    #         assert iwx_sdk_domain1_response["result"]["status"].upper() == "SUCCESS" and iwx_sdk_domain2_response["result"]["status"].upper()
    #     except DomainError as e:
    #         print(str(e))
    #         assert False

    # def test_create_data_connection():
    #     try:
    #         response = iwx_client.create_data_connection(ValueStorage.domain_id, {
    #             "name": "IWX_Snowflake_API",
    #             "type": "TARGET",
    #             "sub_type": "SNOWFLAKE",
    #             "properties": {
    #                 "url": "",
    #                 "account": "",
    #                 "username": "",
    #                 "password": "",
    #                 "warehouse": "DEMO_WH",
    #                 "additional_params": ""
    #             }
    #         })
    #         assert response["result"]["status"].upper() == "SUCCESS"
    #         ValueStorage.dataconnection_id = response["result"]["response"]["result"]["id"]
    #     except SourceError as e:
    #         print(str(e))
    #         assert False
