from infoworks.sdk.cicd.download_configurations.get_source_configuration import DownloadSource
from infoworks.sdk.cicd.download_configurations.get_pipeline_configuration import DownloadPipeline
from infoworks.sdk.cicd.download_configurations.get_workflow_configuration import DownloadWorkflow
from infoworks.sdk.cicd.download_configurations.get_entity_configuration_lineage import DownloadEntityWithLineage
from infoworks.sdk.cicd.download_configurations.get_all_configs_from_domain import DownloadAllEntitiesFromDomain
from infoworks.sdk.cicd.upload_configurations.wrappersource import WrapperSource
from infoworks.sdk.cicd.upload_configurations.wrapperpipeline import WrapperPipeline
from infoworks.sdk.cicd.upload_configurations.wrapperworkflow import WrapperWorkflow
from infoworks.sdk.source_client import SourceClient
from infoworks.sdk.pipeline_client import PipelineClient
from infoworks.sdk.workflow_client import WorkflowClient
from infoworks.sdk.domain_client import DomainClient
from infoworks.sdk.jobmetrics import JobMetricsClient
from infoworks.sdk.admin_client import AdminClient


class InfoworksClientSDK(SourceClient, PipelineClient, WorkflowClient, DomainClient, AdminClient, JobMetricsClient,
                         DownloadSource, DownloadPipeline, DownloadWorkflow, DownloadEntityWithLineage,
                         DownloadAllEntitiesFromDomain, WrapperSource, WrapperPipeline, WrapperWorkflow):
    def __init__(self):
        super().__init__()
