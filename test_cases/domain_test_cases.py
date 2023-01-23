from infoworks.sdk.client import InfoworksClientSDK
from infoworks.error import *
from test_cases.conftest import ValueStorage
import pytest
import logging

refresh_token = "zThziQ7MoJJPYAha+U/+PBSTZG944F+SHBDs+m/z2qn8+m/ax8Prpzla1MHzQ5EBLzB2Bw8a+Qs9r6En5BEN2DsmUVJ6sKFb2yI2"
iwx_client = InfoworksClientSDK()
iwx_client.initialize_client_with_defaults("https", "att-iwx-ci-cd.infoworks.technology", "443", refresh_token)
pytest.environment_id = "884236e85b9b1a69b2907e4c"
pytest.domains = {}
logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s - %(module)s - %(pathname)s - %(lineno)d - %(levelname)s - %(message)s')
fh = logging.FileHandler('/tmp/sdk_domain_test.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)


@pytest.mark.dependency()
def test_create_domain():
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


@pytest.mark.dependency(depends=["test_create_domain"])
def test_add_domains_to_user():
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


@pytest.mark.dependency(depends=["test_add_domains_to_user"])
def test_list_domains():
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


@pytest.mark.dependency(depends=["test_add_domains_to_user"])
def test_get_domain_details():
    try:
        response = iwx_client.get_domain_details(pytest.domains.get("iwx_sdk_domain1"))
        logger.info(response)
        assert response["result"]["status"].upper() == "SUCCESS"
    except DomainError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_add_domains_to_user"])
def test_get_domain_id():
    try:
        response = iwx_client.get_domain_id("iwx_sdk_domain1")
        logger.info(response)
        assert response["result"]["status"].upper() == "SUCCESS"
    except DomainError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_list_domains"])
def test_update_domain():
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


@pytest.mark.dependency(depends=["test_update_domain"])
def test_delete_domain():
    try:
        response = iwx_client.delete_domain(pytest.domains.get("iwx_sdk_domain2"))
        logger.info(response)
        assert response["result"]["status"].upper() == "SUCCESS"
    except DomainError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_add_domains_to_user"])
def test_add_source_to_domain():
    try:
        response = iwx_client.add_source_to_domain(pytest.domains.get("iwx_sdk_domain1"), {
            "entity_ids": [ValueStorage.source_id]
        })
        logger.info(response)
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False


@pytest.mark.dependency(depends=["test_add_source_to_domain"])
def test_get_sources_associated_with_domain():
    try:
        response = iwx_client.get_sources_associated_with_domain(pytest.domains.get("iwx_sdk_domain1"))
        logger.info(response)
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False

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
