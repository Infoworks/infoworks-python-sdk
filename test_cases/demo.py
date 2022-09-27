# pip install infoworkssdk
from infoworks.sdk.client import InfoworksClientSDK

# Initialisation of client
refresh_token = "zThziQ7MoJJPYAha+U/+PBSTZG944F+SHBDs+m/z2qn8+m/ax8Prpzla1MHzQ5EBLzB2Bw8a+Qs9r6En5BEN2DsmUVJ6sKFb2yI2"
iwx_client = InfoworksClientSDK()
iwx_client.initialize_client_with_defaults("http", "10.28.1.163", "3001", refresh_token,
                                           default_environment_id="bda4006276b137668187579a",
                                           default_storage_id="b7965cc813dbc2da1e7d2724",
                                           default_compute_id="fecd2b5e930570fb485ecfc5")

# Step to create an RDBMS source
args = {"src_name": "sdk_demo_oracle_source".upper(), "src_type": "oracle",
        "data_lake_path": "/iw/sources/sdk_demo_oracle_source",
        "connection_url": "jdbc:oracle:thin:@52.73.246.109:1521:xe", "username": "automation_db",
        "password": "eEBcRuPkw0zh9oIPvKnal+1BNKmFH5TfdI1ieDinruUv47Z5+f/oPjb+uyqUmfcQusM2DjoHc3OM",
        "schemas_filter": "AUTOMATION_DB",
        "tables_filter": "CUSTOMERS,ORDERS"}
'''
This function 
    1. Creates source
    2. Configures the source connection details
    3. Does source test connection and polls for job completion
    4. Browses the source and polls the interactive job submitted
'''
src_id = iwx_client.create_onboard_rdbms_source(**args)
# Add tables to onboard + crawls metadata for onboarded tables
response = iwx_client.add_tables_to_source(src_id, tables_to_add_config=[{
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
}])

# Configure tables

# iwx_client.configure_table(source_id="1d6fae5b9c1d6359d4f0cf1e", table_id="2aae2bdba2c44240be6afb7f",
#                            natural_keys=["EMPLOYEE_ID"], sync_type="full-load", split_by_key="REPORTS_TO",
#                            storage_format="delta")

# Submit Ingestion Job
iwx_client.submit_source_job(source_id="1d6fae5b9c1d6359d4f0cf1e", body={
    "job_type": "truncate_reload",
    "job_name": "sdk_demo_job",
    "interactive_cluster_id": "fecd2b5e930570fb485ecfc5",
    "table_ids": [
        "8d85fb45df871a1d9aaf307b"
    ]
})

# Get ingestion metrics for an Ingestion Job
ingestion_metrics_results = iwx_client.get_ingestion_metrics(job_id="31768d07e4ed4f9a43e9846b",
                                                             source_id="1d6fae5b9c1d6359d4f0cf1e")
print(ingestion_metrics_results)

# Get source name and source id
source_name = iwx_client.get_sourcename_from_id(src_id="1d6fae5b9c1d6359d4f0cf1e")
print(f"Source Name is {source_name}")
source_id = iwx_client.get_sourceid_from_name(source_name="DEMO_ORACLE")
print(f"Source ID is {source_id}")

# Get domain id from name
domainid = iwx_client.get_domain_id("DEMO_DOMAIN")
print(f"Domain ID is {domainid}")

# Get pipeline name from id and get pipeline id from name
pl_name = iwx_client.get_pipeline_name(domain_id="e47495c6c31e989d870910ae", pipeline_id="83c15fb2157347a1e4ff030b")
print(f"Pipeline Name is {pl_name}")
pl_id = iwx_client.get_pipeline_id(pipeline_name="DEMO_EXPORTS", domain_name="DEMO_DOMAIN")
print(f"Pipeline ID is {pl_id}")

# Get workflow name from id and workflow id from name
wf_name = iwx_client.get_workflow_name(domain_id="e47495c6c31e989d870910ae", workflow_id="77f68f74d3ec09a4cff076f2")
print(f"Workflow Name is {wf_name}")
wf_id = iwx_client.get_workflow_id(domain_id="e47495c6c31e989d870910ae", workflow_name="DEMO_WORKFLOW")
print(f"Workflow ID is {wf_id}")

# List environment details
environment_details_response = iwx_client.get_environment_details()
print(iwx_client.get_environment_details(params={"filter": {"name": "PRE_PROD_NOVARTIS"}})["result"]["response"])

# Get compute id and storage id from name
compute_id = iwx_client.get_compute_id_from_name(environment_id="bda4006276b137668187579a",
                                                 compute_name="PRE_PROD_DEFAULT_PERSISTENT_COMPUTE")
storage_id = iwx_client.get_storage_id_from_name(environment_id="bda4006276b137668187579a",
                                                 storage_name="PRE_PROD_DEFAULT_STORAGE")
print(f"Compute {compute_id} Storage {storage_id}")

# Get the domain ids for the pipelines or workflows using entity ids
domain_id = iwx_client.get_parent_entity_id(
    json_obj={"entity_id": "83c15fb2157347a1e4ff030b", "entity_type": "pipeline"})

# Trigger workflow
wf_run_id = \
    iwx_client.trigger_workflow(workflow_id="23b50e0d1a74e7e66a0eab09", domain_id="e47495c6c31e989d870910ae")["result"][
        "response"]["result"]["id"]
iwx_client.get_status_of_workflow(workflow_id="23b50e0d1a74e7e66a0eab09", workflow_run_id=wf_run_id)
iwx_client.poll_workflow_run_till_completion(wf_run_id, workflow_id="23b50e0d1a74e7e66a0eab09")

# Capture ABC metrics
abc_metrics_results = iwx_client.get_abc_job_metrics(time_range_for_jobs_in_mins=10)

# Get lineage information
# iwx_client.alation_compatible_lineage_for_pipeline(domain_id="", pipeline_id="", pipeline_version_id="")

# Download entity configuration JSON exports

iwx_client.cicd_get_sourceconfig_dumps(source_ids=["1d6fae5b9c1d6359d4f0cf1e"],
                                       config_file_dump_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps",
                                       files_overwrite=True)
# iwx_client_dev.cicd_get_pipelineconfig_dumps(pipeline_ids=["7fb2fef4efb0a2c06cb322fc"], config_file_dump_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps", files_overwrite=True)
# iwx_client_dev.cicd_get_workflowconfig_dumps(workflow_ids=["8b20fa7c9f9856190b3ff0cc"], config_file_dump_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps", files_overwrite=True)
# iwx_client_dev.cicd_get_dumps_withlineage(workflows=["8b20fa7c9f9856190b3ff0cc"], config_file_dump_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps", files_overwrite=True)
# iwx_client_dev.cicd_get_all_configuration_dumps_from_domain(domain_ids=["c5a8c2256c825dd6c605497c"], config_file_dump_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps", files_overwrite=True)
'''
iwx_client_preprod = InfoworksClientSDK()
iwx_client_preprod = iwx_client_preprod.initialize_client_with_defaults("http", "10.18.1.28", "3001", refresh_token,
                                                                    default_environment_id="09b14e517f9d22faf593b982",
                                                                    default_storage_id="0e42b346a101a81e37155fbe",
                                                                    default_compute_id="03e08fb415447fd56df9d888")

iwx_client_dev.cicd_download_source_configurations(source_ids=["627a523580cccd05ac200159"],
                                                   path="/Users/infoworks/Downloads/GitHub/infoworks-public/dumps",
                                                   files_overwrite=True)
iwx_client_preprod.get_mappings_from_config_file(ini_config_file_path="")
iwx_client_preprod.cicd_upload_source_configurations(configuration_file_path="")
'''
# iwx_client_dev.cicd_download_source_configurations(source_ids=["6298a57f1086072f3633bce9"],
#                                                   path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps",
#                                                    files_overwrite=True)
# iwx_client_preprod.get_mappings_from_config_file(ini_config_file_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/test_cases/config.ini")
# iwx_client_preprod.cicd_upload_source_configurations(configuration_file_path="/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps/source/source_iwx_sdk_srcname.json")
# iwx_client_preprod.cicd_create_configure_workflow(configuration_file_path="/Users/infoworks/Downloads/GitHub/infoworks-public/dumps/workflow/Alation_Demo#workflow_Demo_Workflow.json")
# iwx_client_preprod.cicd_create_configure_pipeline(configuration_file_path="/Users/infoworks/Downloads/GitHub/infoworks-public/dumps/pipeline/Alation_Demo#pipeline_Raw_To_Staging_Customer.json")
# iwx_client_preprod.cicd_create_sourceartifacts_in_bulk("/Users/infoworks/Downloads/GitHub/infoworks_sdk/dumps")
