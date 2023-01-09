from infoworks.sdk.client import InfoworksClientSDK

# Connect to Dev environment and get the source dump
# refresh_token = "zThziQ7MoJJPYAha+U/+PBSTZG944F+SHBDs+m/z2qn8+m/ax8Prpzla1MHzQ5EBLzB2Bw8a+Qs9r6En5BEN2DsmUVJ6sKFb2yI2"
# iwx_client_dev = InfoworksClientSDK()
# iwx_client_dev.initialize_client_with_defaults("http", "10.28.1.163", "3001", refresh_token)
# iwx_client_dev.cicd_get_sourceconfig_dumps(source_ids=["1d6fae5b9c1d6359d4f0cf1e"],
#                                            config_file_dump_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps")

# iwx_client_dev.cicd_get_pipelineconfig_dumps(pipeline_ids=["83c15fb2157347a1e4ff030b"],
#                                              config_file_dump_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps")
#
# iwx_client_dev.cicd_get_workflowconfig_dumps(workflow_ids=["77f68f74d3ec09a4cff076f2"],config_file_dump_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps")
# # Connect to Prod environment
refresh_token = "zThziQ7MoJJPYAha+U/+PBSTZG944F+SHBDs+m/z2qn8+m/ax8Prpzla1MHzQ5EBLzB2Bw8a+Qs9r6En5BEN2DsmUVJ6sKFb2yI2"
iwx_client_prd = InfoworksClientSDK()
iwx_client_prd.initialize_client_with_defaults("http", "10.18.1.32", "3001", refresh_token)
# print(iwx_client_prd.get_pipeline("7fb2fef4efb0a2c06cb322fc","c5a8c2256c825dd6c605497c"))
# print(iwx_client_prd.get_pipeline_version_details("7fb2fef4efb0a2c06cb322fc","c5a8c2256c825dd6c605497c","6298cb771086072f3633bdd9"))
# print(iwx_client_prd.get_table_info("5053d4f29fd1730ec43a3c75","55712e35a92a2284acbe30d7"))
print(iwx_client_prd.get_source_configurations("5053d4f29fd1730ec43a3c75"))

# get_table_configurations
# iwx_client_prd.cicd_get_sourceconfig_dumps(source_ids=["4ff2a9c972418ff809ef0e6c"],
#                                            config_file_dump_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps",
#                                            files_overwrite=True)
# iwx_client_prd.get_environment_details("cfc879153b26323297758bf1")
# col_details = iwx_client_prd.get_table_columns_details("6208d41191d7193702fc2e39", "T86102_ACCESS_USAGE_STATISTICS", "PUBLIC", "ABHI_DATABASE").get("result")["response"]
# print(col_details)

# iwx_client_prd.get_mappings_from_config_file(ini_config_file_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/test_cases/config.ini")
# iwx_client_prd.cicd_upload_source_configurations(
#     configuration_file_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps/source/source_DEMO_ORACLE.json",
#     read_passwords_from_secrets=True,
#     env_tag="PRD", secret_type="aws_secrets")

# iwx_client_prd.cicd_create_configure_workflow(configuration_file_path="",)
# iwx_client_prd.cicd_create_configure_pipeline(
#     configuration_file_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps/pipeline/Alation_Demo#pipeline_Raw_To_Staging_Customer.json",
#     read_passwords_from_secrets=True,
#     env_tag="PRD", secret_type="aws_secrets")

# iwx_client_prd.cicd_create_configure_workflow(
#     configuration_file_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps/workflow/Alation_Demo#workflow_Demo.json")
# iwx_client.get_all_secrets("aws_secrets", ini_config_file_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/test_cases/config.ini")

#
# iwx_client.get_all_secrets("azure_keyvault", ini_config_file_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/test_cases/config.ini")
# obj = {
#             "driver_name": "oracle.jdbc.driver.OracleDriver",
#             "driver_version": "v2",
#             "connection_url": "jdbc:oracle:thin:@52.73.246.109:1521:xe",
#             "username": "automation_db",
#             "connection_mode": "jdbc",
#             "database": "ORACLE"
#         }
# iwx_client.configure_source_connection(source_id="62a20c841275ce1c0c5d91d1",connection_object=obj,read_passwords_from_secrets=True)
# iwx_client.cicd_get_sourceconfig_dumps(source_ids=["629e11b01275ce1c0c5c18d8"],
#                                      config_file_dump_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps",
#                                      files_overwrite=True)
# iwx_client.cicd_download_pipeline_configurations(pipeline_ids=["629ecbe31275ce1c0c5c516a"],
#                                      path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps",
#                                      files_overwrite=True)

# iwx_client.cicd_upload_source_configurations(configuration_file_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps/source/source_iwx_sdk_srcname.json")
#
# from infoworks.sdk.cicd.cicd_client import InfoworksClientCICD
# import logging
#
# client_config = {'protocol': "http",
#                  'ip': "10.18.1.110",
#                  'port': "3001",
#                  'refresh_token': "zThziQ7MoJJPYAha+U/+PBSTZG944F+SHBDs+m/z2qn8+m/ax8Prpzla1MHzQ5EBLzB2Bw8a+Qs9r6En5BEN2DsmUVJ6sKFb2yI2",
#                  'bearer_token': "eyJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE2NTQ3NTE5MDYsInN1YiI6IntcbiAgXCJlbWFpbFwiIDogXCJhZG1pbkBpbmZvd29ya3MuaW9cIixcbiAgXCJzY29wZVwiIDoge1xuICAgIFwiYWN0aW9uXCIgOiBcInJlZnJlc2hfdG9rZW5cIlxuICB9XG59IiwianRpIjoiYTgxZTE4NTYtNDU4OC00MzM4LTg2M2QtMDJkMGE3NDkyYWYxIiwiZXhwIjoxNjU0NzUyODA2fQ.yFZUlI3hWz6_eZF5wAP1UwMNufK0u2oUd3NH-pDAcGI"}
#
# logging.basicConfig(filename="/tmp/iwx_sdk.log", filemode='w',
#                     format='%(asctime)s - %(module)s - %(pathname)s - %(lineno)d - %(levelname)s - %(message)s',
#                     level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
# logger = logging.getLogger('infoworks_sdk_logs')
# cicd_client = InfoworksClientCICD(logger, client_config)
#
# # cicd_client.get_workflowconfig_dumps(workflow_ids=["19246c1126c680ad73d65651"],
# #                                      config_file_dump_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps",
# #                                      files_overwrite=True)
#
# # cicd_client.get_pipelineconfig_dumps(pipeline_ids=["629ecbe31275ce1c0c5c516a"],
# #                                      config_file_dump_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps",
# #                                      files_overwrite=True)
#
# cicd_client.get_all_configuration_dumps_from_domain(domain_ids=["8b54662f856ad024b2bb22c4"],
#                                      config_file_dump_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps",
#                                      files_overwrite=True)
