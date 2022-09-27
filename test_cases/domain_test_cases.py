from infoworks.sdk.client import InfoworksClientSDK
from infoworks.error import *
from test_cases.conftest import ValueStorage

refresh_token = "zThziQ7MoJJPYAha+U/+PBSTZG944F+SHBDs+m/z2qn8+m/ax8Prpzla1MHzQ5EBLzB2Bw8a+Qs9r6En5BEN2DsmUVJ6sKFb2yI2"
iwx_client = InfoworksClientSDK()
iwx_client.initialize_client_with_defaults("http", "10.18.1.28", "3001", refresh_token)


def test_create_domain():
    try:
        response = iwx_client.create_domain({
            "name": "iwx_sdk_domain",
            "description": "Example Domain",
            "environment_id": "09b14e517f9d22faf593b982"
        })
        assert response["result"]["status"].upper() == "SUCCESS"
        ValueStorage.domain_id = response["result"]["response"]["result"]["id"]
    except SourceError as e:
        print(str(e))
        assert False


def test_add_domains_to_user():
    try:
        response = iwx_client.add_domains_to_user(ValueStorage.domain_id, user_email="admin@infoworks.io")
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False


def test_list_domains():
    try:
        response_all = iwx_client.list_domains()
        response_one = iwx_client.get_domain_details(ValueStorage.domain_id)
        assert response_all["result"]["status"].upper() == "SUCCESS" and response_one["result"][
            "status"].upper() == "SUCCESS" and len(response_all["result"]["response"]) > 0
    except SourceError as e:
        print(str(e))
        assert False


def test_update_domain():
    try:
        response = iwx_client.update_domain(ValueStorage.domain_id, {
            "name": "iwx_sdk_domain",
            "description": "Example Update Domain",
            "environment_id": "09b14e517f9d22faf593b982"
        })
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False


def test_get_sources_associated_with_domain():
    try:
        response = iwx_client.get_sources_associated_with_domain(ValueStorage.domain_id)
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False


def test_add_source_to_domain():
    try:
        response = iwx_client.add_source_to_domain(ValueStorage.domain_id, {
            "entity_ids": [ValueStorage.source_id]
        })
        assert response["result"]["status"].upper() == "SUCCESS"
    except SourceError as e:
        print(str(e))
        assert False


def test_create_data_connection():
    try:
        response = iwx_client.create_data_connection(ValueStorage.domain_id, {
            "name": "IWX_Snowflake_API",
            "type": "TARGET",
            "sub_type": "SNOWFLAKE",
            "properties": {
                "url": "",
                "account": "",
                "username": "",
                "password": "",
                "warehouse": "DEMO_WH",
                "additional_params": ""
            }
        })
        assert response["result"]["status"].upper() == "SUCCESS"
        ValueStorage.dataconnection_id = response["result"]["response"]["result"]["id"]
    except SourceError as e:
        print(str(e))
        assert False
