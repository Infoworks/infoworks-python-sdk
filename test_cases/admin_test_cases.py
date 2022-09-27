from infoworks.sdk.client import InfoworksClientSDK
from infoworks.error import *
from test_cases.conftest import ValueStorage

refresh_token = "zThziQ7MoJJPYAha+U/+PBSTZG944F+SHBDs+m/z2qn8+m/ax8Prpzla1MHzQ5EBLzB2Bw8a+Qs9r6En5BEN2DsmUVJ6sKFb2yI2"
iwx_client = InfoworksClientSDK()
iwx_client.initialize_client_with_defaults("http", "10.18.1.28", "3001", refresh_token)


def test_create_new_user():
    try:
        response = iwx_client.create_new_user({
            "profile": {
                "name": "iwx_sdk_user",
                "email": "iwx_sdk_user@infoworks.io",
                "needs_password_reset": False
            },
            "roles": ["modeller"],
            "password": ""
        })
        assert response["result"]["status"].upper() == "SUCCESS"
    except AdminError as e:
        print(str(e))
        assert False


def test_get_user_details():
    try:
        response = iwx_client.get_user_details()
        assert response["result"]["status"].upper() == "SUCCESS" and len(response["result"]["response"]) > 0
    except AdminError as e:
        print(str(e))
        assert False


def test_get_environment_details():
    try:
        response = iwx_client.get_environment_details()
        assert response["result"]["status"].upper() == "SUCCESS" and len(response["result"]["response"]) > 0
    except AdminError as e:
        print(str(e))
        assert False


def test_get_storage_details():
    try:
        response = iwx_client.get_storage_details(ValueStorage.env_id)
        assert response["result"]["status"].upper() == "SUCCESS" and len(response["result"]["response"]) > 0
    except AdminError as e:
        print(str(e))
        assert False


def test_get_compute_template_details():
    try:
        response = iwx_client.get_compute_template_details(ValueStorage.env_id)
        assert response["result"]["status"].upper() == "SUCCESS" and len(response["result"]["response"]) > 0
    except AdminError as e:
        print(str(e))
        assert False


def test_get_abc_metrics():
    try:
        result = iwx_client.get_abc_job_metrics(time_range_for_jobs_in_mins=10)
        assert result is not None
    except Exception as e:
        print(str(e))
        assert False

