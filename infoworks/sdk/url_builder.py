def get_bearer_token_url(config):
    """
    returns URL to get bearer token  using v3 api
    :param config: client configurations
    :type config: dict
    :return: url to get the bearer_token
    """
    request = '{protocol}://{ip}:{port}/v3/security/token/access'.format(ip=config['ip'], port=config['port'],
                                                                         protocol=config['protocol'])
    return request


def validate_bearer_token_url(config):
    """
    returns URL to validate bearer token using v3 api
    :param config: client configurations
    :type config: dict
    :return: url to validate bearer token
    """
    request = '{protocol}://{ip}:{port}/v3/security/token/validate'.format(ip=config['ip'], port=config['port'],
                                                                           protocol=config['protocol'])
    return request


def list_domains_url(config):
    """
    returns URL to list domains using v3 api
    :param config: client configurations
    :type config: dict
    :return: url to list domains
    """
    request = '{protocol}://{ip}:{port}/v3/domains'.format(ip=config['ip'], port=config['port'],
                                                           protocol=config['protocol'])
    return request


def list_pipelines_url(config, domain_id):
    """
    returns URL to list pipelines under given domain using v3 api
    :param config: client configurations
    :type config: dict
    :return: url to list pipelines
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines'.format(ip=config['ip'], port=config['port'],
                                                                                 protocol=config['protocol'],
                                                                                 domain_id=domain_id)
    return request


def list_sources_url(config):
    """
    returns URL to list sources using v3 api
    :param config: client configurations
    :type config: dict
    :return: url to list sources
    """
    request = '{protocol}://{ip}:{port}/v3/sources'.format(ip=config['ip'], port=config['port'],
                                                           protocol=config['protocol'])
    return request


def list_tables_under_source(config, source_id):
    """
    returns URL to list all tables in source using v3 api
    :param config: client configurations
    :type config: dict
    :param source_id: source entity identifier
    :type source_id: string
    :return: url to list all tables in source
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/tables'.format(ip=config['ip'], port=config['port'],
                                                                              protocol=config['protocol'],
                                                                              source_id=source_id)
    return request


def get_general_configs_url(config):
    """
    returns URL to create source using v3 api
    :param config: client configurations
    :type config: dict
    :return: url to get admin configurations
    """
    request = '{protocol}://{ip}:{port}/v3/admin/configuration/general'.format(ip=config['ip'], port=config['port'],
                                                                               protocol=config['protocol'])
    return request


def get_source_details_url(config):
    """
    returns URL to list sources using v3 api
    :param config: client configurations
    :type config: dict
    :param filter: filter criteria
    :type: filter: string
    :return: source details matching filter criteria
    """

    request = '{protocol}://{ip}:{port}/v3/sources'.format(ip=config['ip'], port=config['port'],
                                                           protocol=config['protocol'])

    return request


def source_info(config, source_id):
    """
    returns URL to get source info using v3 api
    :param config: client configurations
    :type config: dict
    :param source_id: source id
    :type: source_id: string
    :return: URL to get source information
    """

    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}'.format(ip=config['ip'], port=config['port'],
                                                                       protocol=config['protocol'], source_id=source_id)

    return request


def create_source_url(config):
    """
    returns URL to create source using v3 api
    :param config: client configurations
    :type config: dict
    :return: url for the source creation
    """
    request = '{protocol}://{ip}:{port}/v3/sources'.format(ip=config['ip'], port=config['port'],
                                                           protocol=config['protocol'])
    return request


def get_source_configurations_url(config, source_id):
    """
    returns URL to get source configuration using v3 api
    :param config: client configurations
    :type config: dict
    :param source_id: source entity id
    :type source_id: string
    :return: url to get source configuration
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/configurations'.format(ip=config['ip'],
                                                                                      port=config['port'],
                                                                                      protocol=config['protocol'],
                                                                                      source_id=source_id)
    return request


def get_source_connection_details_url(config, source_id):
    """
    returns URL to get or update source connection details using v3 api
    :param config: client configurations
    :type config: dict
    :param source_id: source entity id
    :type source_id: string
    :return: url to get/update source connection details
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/configurations/connection'.format(ip=config['ip'],
                                                                                                 port=config['port'],
                                                                                                 protocol=config[
                                                                                                     'protocol'],
                                                                                                 source_id=source_id)
    return request


def interactive_job_poll_url(config, source_id, job_id):
    """
    returns URL to import poll interactive jobs using v3 api
    :param config: client configurations
    :type config: dict
    :param source_id: source entity id
    :type source_id: string
    :param job_id: job id of interactive job
    :type config: string
    :return: url to poll interactive job
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/interactive-jobs/{interactive_job_id}'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        source_id=source_id,
        interactive_job_id=job_id)

    return request


def browse_source_tables_url(config, source_id):
    """
    returns URL to browse source tables using v3 api
    :param config: client configurations
    :type config: dict
    :param source_id: source entity id
    :type source_id: string
    :return: url to browse source tables details
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/source_tables'.format(ip=config['ip'],
                                                                                     port=config['port'],
                                                                                     protocol=config['protocol'],
                                                                                     source_id=source_id)
    return request


def add_tables_to_source_url(config, source_id):
    """
    returns URL to browse source tables using v3 api
    :param config: client configurations
    :type config: dict
    :param source_id: source entity id
    :type source_id: string
    :return: url to browse source tables details
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/tables/source_tables'.format(ip=config['ip'],
                                                                                            port=config['port'],
                                                                                            protocol=config['protocol'],
                                                                                            source_id=source_id)
    return request


def configure_tables_and_tablegroups_url(config, source_id):
    """
    returns URL to configure source tables using v3 api
    :param config: client configurations
    :type config: dict
    :param source_id: source entity id
    :type source_id: string
    :return: url to configure source tables details
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/config-migration'.format(ip=config['ip'],
                                                                                        port=config['port'],
                                                                                        protocol=config['protocol'],
                                                                                        source_id=source_id)
    return request


def create_table_group_url(config, source_id):
    """
        returns URL to create table groups using v3 api
        :param config: client configurations
        :type config: dict
        :param source_id: source entity id
        :type source_id: string
        :return: url to create table groups using v3 api
        """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/table-groups'.format(ip=config['ip'],
                                                                                    port=config['port'],
                                                                                    protocol=config['protocol'],
                                                                                    source_id=source_id)
    return request


def get_compute_template_details_url(config, environment_id):
    """
    returns URL to get compute template details using v3 api
    :param config: client configurations
    :type config: dict
    :param environment_id: environment id
    :type environment_id: string
    :return: url to get compute template details
    """
    request = '{protocol}://{ip}:{port}/v3/admin/environment/{environment_id}/environment-interactive-clusters'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        environment_id=environment_id)
    return request


def get_test_connection_url(config, source_id):
    """
    returns URL to test rdbms source connection using v3 api
    :param config: client configurations
    :type config: dict
    :param source_id: source entity id
    :type source_id: string
    :return: url to test rdbms source connection
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/jobs'.format(ip=config['ip'], port=config['port'],
                                                                            protocol=config['protocol'],
                                                                            source_id=source_id)
    return request


def get_job_status_url(config, job_id=None):
    """
    returns URL for checking job progress

    :param config: client configurations
    :type config: dict
    :param job_id: Identifier for Job
    :type job_id: string
    :return: url for the job status
    """
    if job_id is None:
        request = '{protocol}://{ip}:{port}/v3/admin/jobs'.format(ip=config['ip'],
                                                                  port=config['port'],
                                                                  protocol=config['protocol'])
    else:
        request = '{protocol}://{ip}:{port}/v3/admin/jobs/{job_id}'.format(ip=config['ip'],
                                                                           port=config['port'],
                                                                           job_id=job_id,
                                                                           protocol=config['protocol'])
    return request


def configure_source_url(config, source_id):
    """
    returns URL to configure the source using v3 rest apis
    :param config: client configurations
    :type config: dict
    :param source_id: source entity id
    :type source_id: string
    :return: url to configure the source
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/config-migration'.format(ip=config['ip'],
                                                                                        port=config['port'],
                                                                                        protocol=config['protocol'],
                                                                                        source_id=source_id)
    return request


def create_domain_url(config):
    """
    returns URL for creating domain

    :param config: client configurations
    :type config: dict
    :return: url for the domain creation
    """
    request = '{protocol}://{ip}:{port}/v3/domains'.format(ip=config['ip'],
                                                           port=config['port'],
                                                           protocol=config['protocol'])
    return request


def list_users_url(config):
    """
    returns URL to list users using v3 api
    :param config: client configurations
    :type config: dict
    :return: url to list users
    """
    request = '{protocol}://{ip}:{port}/v3/admin/users'.format(ip=config['ip'], port=config['port'],
                                                               protocol=config['protocol'])
    return request


def add_user_to_domain_url(config, user_id):
    """
    returns URL to add user to domains

    :param config: client configurations
    :type config: dict
    :param user_id: Identifier for user
    :type user_id: string
    :return: url for the add user to domain
    """
    request = '{protocol}://{ip}:{port}/v3/admin/users/{user_id}/accessible-domains'.format(ip=config['ip'],
                                                                                            port=config['port'],
                                                                                            user_id=user_id,
                                                                                            protocol=config['protocol'])
    return request


def add_sources_to_domain_url(config, domain_id):
    """
    returns URL to add sources to domains

    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :return: url to add sources to domains
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/accessible-sources'.format(ip=config['ip'],
                                                                                          port=config['port'],
                                                                                          domain_id=domain_id,
                                                                                          protocol=config['protocol'])
    return request


def create_pipeline_url(config, domain_id):
    """
    returns URL to create pipeline using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :return: url for the pipeline creation
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines'.format(ip=config['ip'], port=config['port'],
                                                                                 protocol=config['protocol'],
                                                                                 domain_id=domain_id)
    return request


def get_pipeline_url(config, domain_id, pipeline_id):
    """
    returns URL to get pipeline details using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :param pipeline_id: Identifier for pipeline
    :type pipeline_id: string
    :return: url get pipeline details
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}'.format(ip=config['ip'],
                                                                                               port=config['port'],
                                                                                               protocol=config[
                                                                                                   'protocol'],
                                                                                               domain_id=domain_id,
                                                                                               pipeline_id=pipeline_id)
    return request


def delete_pipeline_url(config, domain_id, pipeline_id):
    """
    returns URL to delete pipeline using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :param pipeline_id: Identifier for pipeline
    :type pipeline_id: string
    :return: url to delete pipeline details
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}'.format(ip=config['ip'],
                                                                                               port=config['port'],
                                                                                               protocol=config[
                                                                                                   'protocol'],
                                                                                               domain_id=domain_id,
                                                                                               pipeline_id=pipeline_id)
    return request


def update_pipeline_url(config, domain_id, pipeline_id):
    """
    returns URL to update pipeline using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :param pipeline_id: Identifier for pipeline
    :type pipeline_id: string
    :return: url to update pipeline details
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}'.format(ip=config['ip'],
                                                                                               port=config['port'],
                                                                                               protocol=config[
                                                                                                   'protocol'],
                                                                                               domain_id=domain_id,
                                                                                               pipeline_id=pipeline_id)
    return request


def configure_pipeline_url(config, domain_id, pipeline_id):
    """
    returns URL to configure pipeline using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :param pipeline_id: Identifier for pipeline
    :type pipeline_id: string
    :return: url to configure pipeline
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/config-migration'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'],
        domain_id=domain_id,
        pipeline_id=pipeline_id)
    return request


def list_pipeline_versions_url(config, domain_id, pipeline_id):
    """
    returns URL to list pipeline versions using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :param pipeline_id: Identifier for pipeline
    :type pipeline_id: string
    :return: url to list pipeline versions
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/versions'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'],
        domain_id=domain_id,
        pipeline_id=pipeline_id)
    return request


def make_pipeline_version_active_url(config, domain_id, pipeline_id, version_id):
    """
    returns URL to make pipeline version active
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :param pipeline_id: Identifier for pipeline
    :type pipeline_id: string
    :param version_id: Identifier for pipeline
    :type version_id: string
    :return: url to make pipeline version active
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/versions/{version_id}/set-active'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'],
        domain_id=domain_id,
        pipeline_id=pipeline_id,
        version_id=version_id)
    return request


def create_workflow_url(config, domain_id):
    """
    returns URL to create workflow using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :return: url for the workflow creation
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/workflows'.format(ip=config['ip'], port=config['port'],
                                                                                 protocol=config['protocol'],
                                                                                 domain_id=domain_id)
    return request


def configure_workflow_url(config, domain_id, workflow_id):
    """
    returns URL to configure workflow using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :param workflow_id: Identifier for workflow
    :type workflow_id: string
    :return: url to configure workflow
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/workflows/{workflow_id}/config-migration'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'],
        domain_id=domain_id,
        workflow_id=workflow_id)
    return request


def trigger_workflow_url(config, domain_id, workflow_id):
    """
    returns URL to trigger a workflow using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :param workflow_id: Identifier for workflow
    :type workflow_id: string
    :return: url to trigger workflow
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/workflows/{workflow_id}/start'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'],
        domain_id=domain_id,
        workflow_id=workflow_id)
    return request


def get_workflow_status_url(config, workflow_id, workflow_run_id):
    """
    returns URL to fetch status of workflow using v3 api
    :param config: client configurations
    :type config: dict
    :param workflow_id: Identifier for workflow id
    :type workflow_id: string
    :param workflow_run_id: Identifier for workflow run id
    :type workflow_run_id: string
    :return: url to fetch status of workflow using v3 api
    """
    request = '{protocol}://{ip}:{port}/v3/admin/workflows/{workflow_id}/runs/{workflow_run_id}'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'],
        workflow_id=workflow_id,
        workflow_run_id=workflow_run_id)
    return request


def get_all_workflows_url(config):
    """
    returns URL to fetch all workflows in Infoworks using v3 api
    :param config: client configurations
    :type config: dict
    :return: url fetch all workflows in Infoworks using v3 api
    """
    request = '{protocol}://{ip}:{port}/v3/admin/workflows'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_all_workflows_runs_url(config):
    """
    returns URL to fetch all workflows runs in Infoworks using v3 api
    :param config: client configurations
    :type config: dict
    :return: url fetch all workflows in Infoworks using v3 api
    """
    request = '{protocol}://{ip}:{port}/v3/prodops/workflow_runs'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_all_workflow_run_jobs_url(config, run_id):
    """
    returns URL to fetch all workflows run jobs in Infoworks using v3 api
    :param config: client configurations
    :type config: dict
    :param run_id: Workflow run id to get the jobs under it
    :type run_id: String
    :return: url fetch all workflows in Infoworks using v3 api
    """
    request = '{protocol}://{ip}:{port}/v3/prodops/workflow_runs/{run_id}/jobs'.format(
        run_id=run_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def restart_multiple_workflows_url(config):
    """
    returns URL to restart multiple workflows using Infoworks using v3 api
    :param config: client configurations
    :type config: dict
    :return: url restart multiple workflows using Infoworks using v3 api
    """
    request = '{protocol}://{ip}:{port}/v3/prodops/workflows/restart'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def cancel_multiple_workflows_url(config):
    """
    returns URL to cancel multiple workflows using Infoworks using v3 api
    :param config: client configurations
    :type config: dict
    :return: url cancel multiple workflows using Infoworks using v3 api
    """
    request = '{protocol}://{ip}:{port}/v3/prodops/workflows/cancel'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_all_workflows_runs_url_with_domain_id(config, domain_id, workflow_id):
    """
    returns URL to fetch all workflows runs in Infoworks using v3 api
    :param config: client configurations
    :type config: dict
    :return: url fetch all workflows in Infoworks using v3 api
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/workflows/{workflow_id}/runs'.format(
        domain_id=domain_id,
        workflow_id=workflow_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def create_data_connection(config):
    """
    returns URL to create data connection using v3 api
    :param config: client configurations
    :type config: dict
    :return: url for data connection creation
    """
    request = '{protocol}://{ip}:{port}/v3/admin/data-connections'.format(ip=config['ip'],
                                                                          port=config['port'],
                                                                          protocol=config['protocol']
                                                                          )
    return request


def get_data_connection(config, dataconnection_id=None):
    """
    returns URL to get data connection details using v3 api
    :param config: client configurations
    :type config: dict
    :param dataconnection_id: Identifier for data connection
    :type dataconnection_id: string
    :return: url to get data connection details
    """
    if dataconnection_id is None:
        request = '{protocol}://{ip}:{port}/v3/admin/data-connections'.format(
            ip=config['ip'],
            port=config['port'],
            protocol=config['protocol'])
    else:
        request = '{protocol}://{ip}:{port}/v3/admin/data-connections/{dataconnection_id}'.format(
            ip=config['ip'],
            port=config['port'],
            protocol=config['protocol'],
            dataconnection_id=dataconnection_id)
    return request


def get_parent_entity_url(config):
    """
    returns URL to get parent entity
    :param config: client configurations
    :type config: dict
    :return: url to get parent entity
    """
    request = '{protocol}://{ip}:{port}/v3/admin/entity'.format(ip=config['ip'],
                                                                port=config['port'],
                                                                protocol=config['protocol']
                                                                )
    return request


def get_pipeline_details(config, domain_id, pipeline_id):
    """
        returns URL to get pipeline details using v3 api
        :param config: client configurations
        :type config: dict
        :param domain_id: Identifier for domain_id
        :type domain_id: string
        :param pipeline_id: Identifier for pipeline id
        :type pipeline_id: string
        :return: url to get pipeline details
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'],
        domain_id=domain_id,
        pipeline_id=pipeline_id)
    return request


def get_environment_details(config, env_id=None):
    """
    returns URL to get environment details using v3 api
    :param config: client configurations
    :type config: dict
    :param env_id: Identifier for environment_id
    :type env_id: string
    :return: url to get environment details
    """
    if env_id is not None:
        request = '{protocol}://{ip}:{port}/v3/admin/environment/{env_id}/'.format(
            ip=config['ip'], port=config['port'],
            protocol=config['protocol'],
            env_id=env_id)
    else:
        request = '{protocol}://{ip}:{port}/v3/admin/environment'.format(
            ip=config['ip'], port=config['port'],
            protocol=config['protocol'])
    return request


def get_environment_storage_details(config, env_id, storage_id=None):
    """
    returns URL to get environment storage details using v3 api
    :param config: client configurations
    :type config: dict
    :param env_id: Identifier for environment_id
    :type env_id: string
    :param storage_id: Identifier for storage_id
    :type storage_id: string
    :return: url to get environment storage details
    """
    if storage_id is None:
        request = '{protocol}://{ip}:{port}/v3/admin/environment/{env_id}/environment-storage'.format(
            ip=config['ip'], port=config['port'],
            protocol=config['protocol'],
            env_id=env_id)
    else:
        request = '{protocol}://{ip}:{port}/v3/admin/environment/{env_id}/environment-storage/{storage_id}'.format(
            ip=config['ip'], port=config['port'],
            protocol=config['protocol'],
            env_id=env_id,
            storage_id=storage_id)
    return request


def get_environment_compute_details(config, env_id, compute_id=None):
    """
    returns URL to get environment compute details using v3 api
    :param config: client configurations
    :type config: dict
    :param env_id: Identifier for environment_id
    :type env_id: string
    :param compute_id: Identifier for compute_id
    :type compute_id: string
    :return: url to get environment compute details
    """
    if compute_id is None:
        request = '{protocol}://{ip}:{port}/v3/admin/environment/{env_id}/environment-compute-template'.format(
            ip=config['ip'], port=config['port'],
            protocol=config['protocol'],
            env_id=env_id)
    else:
        request = '{protocol}://{ip}:{port}/v3/admin/environment/{env_id}/environment-compute-template/{compute_id}'.format(
            ip=config['ip'], port=config['port'],
            protocol=config['protocol'],
            env_id=env_id,
            compute_id=compute_id)
    return request


def get_environment_interactive_compute_details(config, env_id, compute_id=None):
    """
    returns URL to get environment interactive compute details using v3 api
    :param config: client configurations
    :type config: dict
    :param env_id: Identifier for environment_id
    :type env_id: string
    :param compute_id: Identifier for compute_id
    :type compute_id: string
    :return: url to get environment interactive compute details
    """
    if compute_id is None:
        request = '{protocol}://{ip}:{port}/v3/admin/environment/{env_id}/environment-interactive-clusters'.format(
            ip=config['ip'], port=config['port'],
            protocol=config['protocol'],
            env_id=env_id)
    else:
        request = '{protocol}://{ip}:{port}/v3/admin/environment/{env_id}/environment-interactive-clusters/{compute_id}'.format(
            ip=config['ip'], port=config['port'],
            protocol=config['protocol'],
            env_id=env_id,
            compute_id=compute_id)
    return request


def submit_source_job(config, source_id):
    """
        returns URL to submit source jobs using v3 api
        :param config: client configurations
        :type config: dict
        :param source_id: Identifier for source
        :type source_id: string
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/jobs'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'],
        source_id=source_id)
    return request


def resubmit_job_url(config, job_id):
    """
        returns URL to submit source jobs using v3 api
        :param config: client configurations
        :type config: dict
        :param job_id: Identifier for job
        :type job_id: string
    """
    request = '{protocol}://{ip}:{port}/v3/admin/jobs/{job_id}/resubmit'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'],
        job_id=job_id)
    return request


def get_advanced_config_url(config, source_id, table_id=None):
    """
        returns URL to list advanced configurations of source using v3 api
        :param config: client configurations
        :type config: dict
        :param source_id: source identifier
        :type: source_id: string
        :param table_id: table identifier
        :type: table_id: string
    """
    if table_id is None:
        request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/configurations/advance'.format(ip=config['ip'],
                                                                                                  port=config['port'],
                                                                                                  protocol=config[
                                                                                                      'protocol'],
                                                                                                  source_id=source_id)
    else:
        request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/tables/{table_id}/configurations/advance'.format(
            ip=config['ip'],
            port=config['port'],
            protocol=config[
                'protocol'],
            source_id=source_id, table_id=table_id)

    return request


def get_pipeline_version_url(config, domain_id, pipeline_id, pipeline_version_id):
    """
    returns URL to get pipeline details using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain_id
    :type domain_id: string
    :param pipeline_id: Identifier for pipeline_id
    :type pipeline_id: string
    :return: url get pipeline details
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/versions/{pipeline_version_id}'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config[
            'protocol'],
        domain_id=domain_id,
        pipeline_id=pipeline_id,
        pipeline_version_id=pipeline_version_id)
    return request


def delete_pipeline_version_url(config, domain_id, pipeline_id, pipeline_version_id):
    """
    returns URL to delete pipeline using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain_id
    :type domain_id: string
    :param pipeline_id: Identifier for pipeline_id
    :type pipeline_id: string
    :return: url delete pipeline
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/versions/{pipeline_version_id}'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config[
            'protocol'],
        domain_id=domain_id,
        pipeline_id=pipeline_id,
        pipeline_version_id=pipeline_version_id)
    return request


def update_pipeline_version_url(config, domain_id, pipeline_id, pipeline_version_id):
    """
    returns URL to update pipeline using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain_id
    :type domain_id: string
    :param pipeline_id: Identifier for pipeline_id
    :type pipeline_id: string
    :param pipeline_version_id: Identifier for pipeline version id
    :type pipeline_version_id: string
    :return: url update pipeline details
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/versions/{pipeline_version_id}'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config[
            'protocol'],
        domain_id=domain_id,
        pipeline_id=pipeline_id,
        pipeline_version_id=pipeline_version_id)
    return request


def pipeline_sql_import_url(config, domain_id, pipeline_id):
    """
    returns URL to import sql into pipeline using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain_id
    :type domain_id: string
    :param pipeline_id: Identifier for pipeline_id
    :type pipeline_id: string
    :return: url import sql into pipeline
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/sql-import'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config[
            'protocol'],
        domain_id=domain_id,
        pipeline_id=pipeline_id)
    return request


def get_table_configuration(config, source_id, table_id):
    """
    returns URL to configure the get the table configurations using v3 rest apis
    :param config: client configurations
    :type config: dict
    :param source_id: source entity id
    :type source_id: string
    :param table_id:
    :return: url to configure the source
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/tables/{table_id}'.format(ip=config['ip'],
                                                                                         port=config['port'],
                                                                                         protocol=config['protocol'],
                                                                                         source_id=source_id,
                                                                                         table_id=table_id)
    return request


def update_table_ingestion_config_url(config, source_id, table_id):
    """
    returns URL to update the table configurations using v3 rest apis
    :param config: client configurations
    :type config: dict
    :param source_id: source entity id
    :type source_id: string
    :param table_id: table entity id
    :type table_id: string
    :return: url to update the table configurations
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/tables/{table_id}/configurations/ingestion'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        source_id=source_id,
        table_id=table_id)
    return request


def table_export_config_url(config, source_id, table_id):
    """
    returns URL to get/update the table export configurations using v3 rest apis
    :param config: client configurations
    :type config: dict
    :param source_id: source entity id
    :type source_id: string
    :param table_id: table entity id
    :type table_id: string
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/tables/{table_id}/configurations/export'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        source_id=source_id,
        table_id=table_id)
    return request


def get_ingestion_metrics_source_url(config, source_id):
    """
    returns URL to get ingestion metrics using v3 rest apis
    :param config: client configurations
    :type config: dict
    :param source_id: source entity id
    :type source_id: string
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/reports/table-metrics'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        source_id=source_id)
    return request


def get_ingestion_metrics_table_url(config, source_id, table_id):
    """
    returns URL to get ingestion metrics using v3 rest apis
    :param config: client configurations
    :type config: dict
    :param source_id: source entity id
    :type source_id: string
    :param table_id: table entity id
    :type table_id: string
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/tables/{table_id}/reports/ingestion-metrics'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        source_id=source_id,
        table_id=table_id)
    return request


def get_job_metrics_url(config, source_id, job_id):
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/jobs/{job_id}/reports/job-metrics'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        job_id=job_id,
        source_id=source_id
    )
    return request


def get_ingestion_metrics_admin_url(config, job_id):
    request = '{protocol}://{ip}:{port}/v3/admin/jobs/{job_id}/reports/ingestion-metrics'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        job_id=job_id,
    )
    return request


def get_export_metrics_source_url(config, source_id, job_id):
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/jobs/{job_id}/reports/export-metrics'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        job_id=job_id,
        source_id=source_id
    )
    return request


def get_source_file_paths_url(config, source_id, table_id, job_id):
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/tables/{table_id}/jobs/{job_id}/reports/sourceFilesPath'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        table_id=table_id,
        job_id=job_id,
        source_id=source_id
    )
    return request


def get_export_metrics_url(config, job_id):
    request = '{protocol}://{ip}:{port}/v3/admin/jobs/{job_id}/reports/export-metrics'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        job_id=job_id
    )
    return request


def get_pipeline_build_metrics_url(config, job_id):
    request = '{protocol}://{ip}:{port}/v3/admin/jobs/{job_id}/reports/pipeline-metrics'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        job_id=job_id
    )
    return request


def get_pipeline_lineage_url(config, domain_id, pipeline_id, pipeline_version_id, column_name, node_id):
    """
        returns URL to get pipeline lineage information using v3 api
        :param config: client configurations
        :type config: dict
        :param domain_id: Identifier for domain_id
        :type domain_id: string
        :param pipeline_id: Identifier for pipeline id
        :type pipeline_id: string
        :param pipeline_version_id: Identifier for pipeline id
        :type pipeline_version_id: string
        :param column_name: Identifier for column name
        :type column_name: string
        :param node_id: Identifier for node id
        :type node_id: string
        :return: url to get pipeline lineage information
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/versions/{pipeline_version_id}/lineage?port_type=OUTPUT&column_name={column_name}&node_id={node_id}'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'],
        domain_id=domain_id,
        pipeline_id=pipeline_id,
        pipeline_version_id=pipeline_version_id,
        column_name=column_name,
        node_id=node_id
    )
    return request


def unlock_locked_entities(config):
    """
    returns URL to unlock locked entities
    :param config: client configurations
    :type config: dict
    """
    request = '{protocol}://{ip}:{port}/v3/admin/unlock-entities'.format(ip=config['ip'],
                                                                         port=config['port'],
                                                                         protocol=config['protocol'])
    return request


def trigger_pipeline_build_url(config, domain_id, pipeline_id):
    """
    returns URL to trigger pipeline
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain_id
    :type domain_id: string
    :param pipeline_id: Identifier for pipeline
    :type pipeline_id: string
    :return: url to trigger pipeline
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/jobs'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'],
        domain_id=domain_id,
        pipeline_id=pipeline_id)
    return request


def create_source_extension_url(config):
    """
    returns URL to create source extension
    :param config: client configurations
    :type config: dict
    :return: url to create source extension
    """
    request = '{protocol}://{ip}:{port}/v3/admin/source-extensions'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_jobs_url(config):
    """
    returns URL to get jobs details
    :param config: client configurations
    :type config: dict
    :return: URL to get jobs details
    """
    request = '{protocol}://{ip}:{port}/v3/admin/jobs'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_interactive_jobs_url(config, source_id):
    """
    returns URL to get interactive jobs details
    :param config: client configurations
    :type config: dict
    :param source_id: Id of the source
    :type source_id: String
    :return: URL to get jobs details
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/interactive-jobs'.format(
        source_id=source_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_admin_jobs_url(config):
    """
    returns URL to get jobs details
    :param config: client configurations
    :type config: dict
    :return: URL to get jobs details
    """
    request = '{protocol}://{ip}:{port}/v3/admin/admin-jobs'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_pipeline_jobs_url(config, domain_id, pipeline_id):
    """
    returns URL to get jobs details
    :param config: client configurations
    :type config: dict
    :param domain_id: Id of the domain for given pipeline
    :type domain_id: String
    :param pipeline_id: Id of the pipeline
    :type pipeline_id: String
    :return: URL to get jobs details
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/jobs'.format(
        domain_id=domain_id,
        pipeline_id=pipeline_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_cancel_job_url(config, job_id):
    """
    returns URL to get jobs details
    :param config: client configurations
    :type config: dict
    :param job_id: Id of job to cancel
    :type job_id: String
    :return: URL to get jobs details
    """
    request = '{protocol}://{ip}:{port}/v3/prodops/jobs/{job_id}/cancel'.format(
        job_id=job_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def list_service_authentication_url(config):
    """
    returns URL to list service authentication using v3 api
    :param config: client configurations
    :type config: dict
    :return: url to list service authentication
    """
    request = '{protocol}://{ip}:{port}/v3/admin/manage-secrets/service-auth'.format(ip=config['ip'],
                                                                                     port=config['port'],
                                                                                     protocol=config['protocol'])
    return request


def list_secret_stores_url(config):
    """
    returns URL to list secret stores using v3 api
    :param config: client configurations
    :type config: dict
    :return: url to list secret stores
    """
    request = '{protocol}://{ip}:{port}/v3/admin/manage-secrets/secret-store'.format(ip=config['ip'],
                                                                                     port=config['port'],
                                                                                     protocol=config['protocol'])
    return request


def get_query_as_table_url(config, source_id):
    """
        returns URL to create query as table
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/tables/query_tables'.format(
        source_id=source_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_pipeline_group_jobs_base_url(config, domain_id, pipeline_group_id):
    """
        returns URL to get list of pipeline group jobs
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipeline-groups/{pipeline_group_id}/jobs'.format(
        domain_id=domain_id,
        pipeline_group_id=pipeline_group_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_pipeline_group_base_url(config, domain_id):
    """
        returns URL to get details of pipeline group
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipeline-groups/'.format(
        domain_id=domain_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_accessible_pipeline_groups_url(config, domain_id):
    """
    returns URL to get accessible pipeline group
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/accessible-pipeline-groups'.format(
        domain_id=domain_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def advance_config_under_pipeline_groups_base_url(config, domain_id, pipeline_group_id):
    """
    returns base URL to get the advance config under pipeline group.
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipeline-groups/{pipeline_group_id}/configurations/advance/'.format(
        domain_id=domain_id, pipeline_group_id=pipeline_group_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_domain_workflow_schedules_url(config, domain_id):
    """
    Returns URL to get Schedules of Workflows belonging to the Domain.
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/schedules'.format(
        protocol=config['protocol'], ip=config['ip'], port=config['port'], domain_id=domain_id
    )
    return request


def get_workflow_schedule_url(config, domain_id, workflow_id):
    """
    Returns URL to get Schedules of particular Workflow belonging to the Domain.
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/workflows/{workflow_id}/schedules'.format(
        protocol=config['protocol'], ip=config['ip'], port=config['port'], domain_id=domain_id, workflow_id=workflow_id
    )
    return request


def get_enable_workflow_schedule_url(config, domain_id, workflow_id):
    """
    Returns URL to Enable Workflow Schedule.
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/workflows/{workflow_id}/schedules/enable'.format(
        protocol=config['protocol'], ip=config['ip'], port=config['port'], domain_id=domain_id, workflow_id=workflow_id
    )
    return request


def get_disable_workflow_schedule_url(config, domain_id, workflow_id):
    """
    Returns URL to Disable Workflow Schedule.
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/workflows/{workflow_id}/schedules/disable'.format(
        protocol=config['protocol'], ip=config['ip'], port=config['port'], domain_id=domain_id, workflow_id=workflow_id
    )
    return request


def update_workflow_schedule_user_url(config, domain_id, workflow_id):
    """
    Return URL to Change Workflow Schedule User
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/workflows/{workflow_id}/schedules/user'.format(
        protocol=config['protocol'], ip=config['ip'], port=config['port'], domain_id=domain_id, workflow_id=workflow_id
    )
    return request


def pause_multiple_workflows_url(config):
    """
    returns url to pause multiple workflows
    """
    request = '{protocol}://{ip}:{port}/v3/admin/workflows/pause'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def resume_multiple_workflows_url(config):
    """
    returns url to resume workflow
    """
    request = '{protocol}://{ip}:{port}/v3/admin/workflows/resume'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_workflow_runs_url(config):
    """
    returns url to get workflow runs
    """
    request = '{protocol}://{ip}:{port}/v3/admin/workflows/runs'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_workflow_tasks_url(config):
    """
    returns url to get workflow tasks
    """
    request = '{protocol}://{ip}:{port}/v3/admin/workflows/tasks'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_workflow_run_id_details_url(config, workflow_id, run_id):
    """
    returns url to get workflow run id details
    """
    request = '{protocol}://{ip}:{port}/v3/admin/workflows/{workflow_id}/runs/{run_id}'.format(
        workflow_id=workflow_id, run_id=run_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def source_table_schema_url(config, source_id, table_id):
    """
    returns url to get table schema details
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/tables/{table_id}/configurations/schema'.format(
        source_id=source_id,
        table_id=table_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_file_mappings_for_json_source_url(config, source_id):
    """
    returns url to get table schema details
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/file-mappings'.format(
        source_id=source_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_source_audit_logs_url(config, source_id):
    """
    returns url to get source audit logs
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/audit-logs'.format(
        source_id=source_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_table_audit_logs_url(config, source_id, table_id):
    """
    returns url to get table audit logs
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}//tables/table_id/audit-logs'.format(
        source_id=source_id,
        table_id=table_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_table_group_audit_logs_url(config, source_id, table_group_id):
    """
    returns url to get table group audit logs
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/table-groups/table_group_id/audit-logs'.format(
        source_id=source_id,
        table_group_id=table_group_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def accessible_pipeline_extensions_url(config, domain_id):
    """
    returns url to get all the Pipeline Extensions associated with this Domain
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/accessible-pipeline-extensions'.format(
        domain_id=domain_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def advanced_config_domain_url(config, domain_id):
    """
    returns url to work with adv config of the domain
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/configurations/advance'.format(
        domain_id=domain_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def advanced_config_pipeline_url(config, domain_id, pipeline_id):
    """
    returns url to work with adv config of the pipeline
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/configurations/advance'.format(
        domain_id=domain_id, pipeline_id=pipeline_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def table_advanced_base_url(config, source_id, table_id):
    """
    returns url to work with table level advanced configuration
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/tables/{table_id}/configurations/advance'.format(
        source_id=source_id,
        table_id=table_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def table_segmentation_base_url(config, source_id, table_id):
    """
        returns url to work with table segmentation
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/tables/{table_id}/segmentation'.format(
        source_id=source_id,
        table_id=table_id,
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_source_job_url(config, source_id, job_id):
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/jobs/{job_id}'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        job_id=job_id,
        source_id=source_id
    )
    return request


def get_jobs_prodops_url(config):
    """
    returns URL to get jobs details
    :param config: client configurations
    :type config: dict
    :return: URL to get jobs details
    """
    request = '{protocol}://{ip}:{port}/v3/prodops/jobs'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def get_metrics_prodops_url(config):
    request = '{protocol}://{ip}:{port}/v3/prodops/metrics'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
    )


def list_secrets_url(config):
    """
        returns url to get secret details in Infoworks
        """
    request = '{protocol}://{ip}:{port}/v3/admin/manage-secrets/secrets'.format(
        ip=config['ip'], port=config['port'],
        protocol=config['protocol'])
    return request


def list_replicator_sources_url(config):
    """
    returns URL to list replicator sources using v3 api
    :param config: client configurations
    :type config: dict
    :return: url to list sources
    """
    request = '{protocol}://{ip}:{port}/v3/replicator/sources'.format(ip=config['ip'], port=config['port'],
                                                                      protocol=config['protocol'])
    return request


def get_replicator_source_url(config, source_id):
    """
    returns URL to the details of a specific replicator source using v3 api
    :param config: client configurations
    :type config: dict
    :param source_id: Identifier of Source
    :type source_id: str
    :return: url to the details of a specific replicator source
    """
    request = '{protocol}://{ip}:{port}/v3/replicator/sources/{source_id}'.format(
        ip=config['ip'], port=config['port'], protocol=config['protocol'], source_id=source_id)
    return request


def create_replicator_source_url(config):
    """
    returns URL to create a replicator source using v3 api
    :param config: client configurations
    :type config: dict
    :return: url for the replicator source creation
    """
    request = '{protocol}://{ip}:{port}/v3/replicator/sources'.format(ip=config['ip'], port=config['port'],
                                                                      protocol=config['protocol'])
    return request


def list_replicator_destinations_url(config):
    """
    returns URL to list replicator destinations using v3 api
    :param config: client configurations
    :type config: dict
    :return: url to list destinations
    """
    request = '{protocol}://{ip}:{port}/v3/replicator/destinations'.format(ip=config['ip'], port=config['port'],
                                                                           protocol=config['protocol'])
    return request


def get_replicator_destination_url(config, destination_id):
    """
    returns URL to the details of a specific replicator destination using v3 api
    :param config: client configurations
    :type config: dict
    :param destination_id: Identifier of Destination
    :type destination_id: str
    :return: url to the details of a specific replicator destination
    """
    request = '{protocol}://{ip}:{port}/v3/replicator/destinations/{destination_id}'.format(
        ip=config['ip'], port=config['port'], protocol=config['protocol'], destination_id=destination_id)
    return request


def create_replicator_destination_url(config):
    """
    returns URL to create a replicator destination using v3 api
    :param config: client configurations
    :type config: dict
    :return: url for the replicator destination creation
    """
    request = '{protocol}://{ip}:{port}/v3/replicator/destinations'.format(ip=config['ip'], port=config['port'],
                                                                           protocol=config['protocol'])
    return request


def list_replicator_definitions_url(config, domain_id):
    """
    returns URL to list replicator definitions using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier of Domain
    :type domain_id: String
    :return: url to list definitions
    """

    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/definitions' \
        .format(ip=config['ip'], port=config['port'], domain_id=domain_id,
                protocol=config['protocol'])
    return request


def get_replicator_definition_url(config, domain_id, definition_id):
    """
    returns URL to get replicator definition using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier of Domain
    :type domain_id: String
    :param definition_id: Identifier of Definition
    :type definition_id: String
    :return: url to list definitions
    """

    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/definitions/{definition_id}' \
        .format(ip=config['ip'], port=config['port'], domain_id=domain_id, definition_id=definition_id,
                protocol=config['protocol'])
    return request


def create_replicator_definition_url(config, domain_id):
    """
    returns URL to create a replicator definition using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :return: url for the replicator definition creation
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/definitions'.format(ip=config['ip'],
                                                                                   port=config['port'],
                                                                                   protocol=config['protocol'],
                                                                                   domain_id=domain_id)
    return request


def list_replicator_domains_url(config):
    """
    returns URL to list replicator domains using v3 api
    :param config: client configurations
    :type config: dict
    :return: url to list domains
    """
    request = '{protocol}://{ip}:{port}/v3/domains'.format(ip=config['ip'], port=config['port'],
                                                           protocol=config['protocol'])
    return request


def add_replicator_definition_tables_url(config, domain_id, definition_id):
    """
    returns URL to add tables to replicator definition using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :param definition_id: Identifier for Replicator definition
    :type definition_id: string
    :return: url to add tables to replicator definition
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/definitions/{definition_id}/tables' \
        .format(ip=config['ip'], port=config['port'], protocol=config['protocol'], domain_id=domain_id,
                definition_id=definition_id)

    return request


def submit_replication_metacrawl_job_url(config, domain_id, replicator_source_id):
    """
    returns URL to add tables to replicator definition using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :param replicator_source_id: Identifier for Source
    :type replicator_source_id: string
    :return: url to submit replication metacrawl job
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/replicator-sources/{replicator_source_id}/jobs' \
        .format(ip=config['ip'], port=config['port'], protocol=config['protocol'], domain_id=domain_id,
                replicator_source_id=replicator_source_id)

    return request


def submit_replication_data_job_url(config, domain_id, definition_id):
    """
    returns URL to add tables to replicator definition using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :param definition_id: Identifier for Definition
    :type definition_id: string
    :return: url to submit replication data job
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/definitions/{definition_id}/jobs' \
        .format(ip=config['ip'], port=config['port'], protocol=config['protocol'], domain_id=domain_id,
                definition_id=definition_id)

    return request


def get_replicator_job_summary_url(config, domain_id, definition_id, job_id):
    """
    returns URL to add tables to replicator definition using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :param definition_id: Identifier for Definition
    :type definition_id: string
    :param job_id: Identifier of Job
    :type job_id: string
    :return: url to submit replication data job
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/definitions/{definition_id}/jobs/{job_id}/summary' \
        .format(ip=config['ip'], port=config['port'], protocol=config['protocol'], domain_id=domain_id,
                definition_id=definition_id, job_id=job_id)
    return request


def add_replicator_sources_to_domain_url(config, domain_id):
    """
    returns URL to add replicator sources to domains

    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :return: url to add replicator sources to domains
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/accessible-replicator-sources' \
        .format(ip=config['ip'], port=config['port'], domain_id=domain_id, protocol=config['protocol'])
    return request


def add_replicator_destinations_to_domain_url(config, domain_id):
    """
    returns URL to add replicator destinations to domains

    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :return: url to add replicator destinations to domains
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/accessible-replicator-destinations' \
        .format(ip=config['ip'], port=config['port'], domain_id=domain_id, protocol=config['protocol'])
    return request


def list_replication_schedules_url(config, domain_id, definition_id):
    """
    returns replication schedules URL

    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :param definition_id : Identifier for Replicator definition
    :type definition_id: string
    :return: replication schedules URL
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/definitions/{definition_id}/schedules' \
        .format(ip=config['ip'], port=config['port'], domain_id=domain_id, protocol=config['protocol'],
                definition_id=definition_id)
    return request


def get_replication_schedule_url(config, domain_id, definition_id, schedule_id):
    """
    returns replication schedule URL

    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :param definition_id : Identifier for Replicator definition
    :type definition_id: string
    :param schedule_id : Identifier of schedule
    :type schedule_id: string
    :return: replication schedules URL
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/definitions/{definition_id}/schedules/{schedule_id}' \
        .format(ip=config['ip'], port=config['port'], domain_id=domain_id, protocol=config['protocol'],
                definition_id=definition_id, schedule_id=schedule_id)
    return request


def get_replicator_source_tables_url(config, source_id):
    """
    returns replicator source tables URL

    :param config: client configurations
    :type config: dict
    :param source_id: Identifier for replicator source
    :type source_id: string
    :return: replicator source tables url
    """
    request = '{protocol}://{ip}:{port}/v3//replicator/sources/{source_id}/tables' \
        .format(ip=config['ip'], port=config['port'], source_id=source_id, protocol=config['protocol'])
    return request


def pipeline_version_base_url(config, domain_id, pipeline_id):
    """
    returns URL to work on pipeline versions using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :param pipeline_id: Identifier for pipeline
    :return: url to work on pipeline version
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/versions'.format(ip=config['ip'],
                                                                                                        port=config[
                                                                                                            'port'],
                                                                                                        protocol=config[
                                                                                                            'protocol'],
                                                                                                        domain_id=domain_id,
                                                                                                        pipeline_id=pipeline_id)
    return request


def workflow_lineage_url(config, domain_id, workflow_id):
    """
    returns URL for workflow lineage
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain
    :type domain_id: string
    :param workflow_id: Identifier for workflow
    :type workflow_id: string
    :return: url for workflow lineage
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/workflows/{workflow_id}/lineage'.format(ip=config['ip'],
                                                                                                       port=config[
                                                                                                           'port'],
                                                                                                       protocol=config[
                                                                                                           'protocol'],
                                                                                                       domain_id=domain_id,
                                                                                                       workflow_id=workflow_id)
    return request


def validate_pipeline_url(config, domain_id, pipeline_id, pipeline_version_id):
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/versions/{pipeline_version_id}/validate'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        domain_id=domain_id, pipeline_id=pipeline_id, pipeline_version_id=pipeline_version_id)
    return request


def url_to_refresh_sample_data_of_pipeline(config, domain_id, pipeline_id, pipeline_version_id):
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/versions/{pipeline_version_id}/refresh-sample-data'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        domain_id=domain_id, pipeline_id=pipeline_id, pipeline_version_id=pipeline_version_id)
    return request


def url_to_get_pipeline_node_details(config, domain_id, pipeline_id, pipeline_version_id, node_id):
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/versions/{pipeline_version_id}/nodes/{node_id}/'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        domain_id=domain_id, pipeline_id=pipeline_id, pipeline_version_id=pipeline_version_id, node_id=node_id)
    return request


def url_for_pipeline_version_audits(config, domain_id, pipeline_id, version_id):
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/versions/{version_id}/pipeline-version-audits'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        domain_id=domain_id, pipeline_id=pipeline_id, version_id=version_id)
    return request


def url_for_pipeline_audits(config, domain_id, pipeline_id):
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/pipeline-audits'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        domain_id=domain_id, pipeline_id=pipeline_id)
    return request


def import_sql_mappings_url(config, domain_id, pipeline_id):
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/import-sql-mappings'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        domain_id=domain_id, pipeline_id=pipeline_id)
    return request


def url_to_get_accessible_pipelines(config, domain_id):
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/accessible-pipelines'.format(ip=config['ip'],
                                                                                            port=config['port'],
                                                                                            protocol=config[
                                                                                                'protocol'],
                                                                                            domain_id=domain_id
                                                                                            )
    return request


def url_to_get_accessible_workflows(config, domain_id):
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/accessible-workflows'.format(ip=config['ip'],
                                                                                            port=config['port'],
                                                                                            protocol=config[
                                                                                                'protocol'],
                                                                                            domain_id=domain_id
                                                                                            )
    return request


def list_domains_admin_url(config):
    """
    returns Admin URL to list domains using v3 api
    :param config: client configurations
    :type config: dict
    :return: url to list domains
    """
    request = '{protocol}://{ip}:{port}/v3/admin/domains'.format(ip=config['ip'], port=config['port'],
                                                                  protocol=config['protocol'])
    return request


def stop_interactive_cluster_url(config):
    request = '{protocol}://{ip}:{port}/v3/admin/environment/cluster/service/stop'.format(ip=config['ip'],
                                                                                          port=config['port'],
                                                                                          protocol=config[
                                                                                              'protocol'],
                                                                                          )
    return request


def terminate_persistent_cluster_aysnc(config):
    request = '{protocol}://{ip}:{port}/v3/admin/environment/cluster/terminate-async'.format(ip=config['ip'],
                                                                                             port=config['port'],
                                                                                             protocol=config[
                                                                                                 'protocol'],
                                                                                             )
    return request


def restart_persistent_cluster_url(config):
    request = '{protocol}://{ip}:{port}/v3/admin/environment/cluster/restart'.format(ip=config['ip'],
                                                                                     port=config['port'],
                                                                                     protocol=config['protocol'],
                                                                                     )
    return request


def stop_persistent_cluster_url(config):
    request = '{protocol}://{ip}:{port}/v3/admin/environment/cluster/stop'.format(ip=config['ip'],
                                                                                  port=config['port'],
                                                                                  protocol=config['protocol'],
                                                                                  )
    return request


def url_to_list_accessible_sources(config, domain_id):
    request = '{protocol}://{ip}:{port}/v3/admin/domains/{domain_id}/accessible-sources-to-add'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        domain_id=domain_id)
    return request


def url_to_list_accessible_pipeline_extensions(config, domain_id):
    request = '{protocol}://{ip}:{port}/v3/admin/domains/{domain_id}/accessible-pipeline-extensions-to-add'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        domain_id=domain_id)
    return request


def url_to_list_accessible_domains(config, domain_id):
    request = '{protocol}://{ip}:{port}/v3/admin/domains/{domain_id}/accessible-domains-to-add'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        domain_id=domain_id)
    return request


def delete_user_schedules_url(config, user_id):
    request = '{protocol}://{ip}:{port}/v3/admin/users/{user_id}/delete-schedules'.format(ip=config['ip'],
                                                                                          port=config['port'],
                                                                                          protocol=config[
                                                                                              'protocol'],
                                                                                          user_id=user_id)
    return request


def list_all_schedules_url(config):
    request = '{protocol}://{ip}:{port}/v3/admin/schedules'.format(ip=config['ip'],
                                                                   port=config['port'],
                                                                   protocol=config['protocol'],
                                                                   )
    return request


def get_crawl_job_summary_url(config, source_id, job_id):
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/jobs/{job_id}/logs'.format(ip=config['ip'],
                                                                                          port=config['port'],
                                                                                          protocol=config['protocol'],
                                                                                          source_id=source_id,
                                                                                          job_id=job_id
                                                                                          )
    return request


def get_table_group_schedule_url(config, source_id, table_group_id):
    """
    Returns URL to get Table Group Schedule
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/table-groups/{table_group_id}/schedules'.format(
        protocol=config['protocol'], ip=config['ip'], port=config['port'], source_id=source_id,
        table_group_id=table_group_id)
    return request


def get_enable_table_group_schedule_url(config, source_id, table_group_id):
    """
    Returns URL to Enable Table Group Schedule
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/table-groups/{table_group_id}/schedules/enable'.format(
        protocol=config['protocol'], ip=config['ip'], port=config['port'], source_id=source_id,
        table_group_id=table_group_id)
    return request


def get_disable_table_group_schedule_url(config, source_id, table_group_id):
    """
    Returns URL to Disable Table Group Schedule
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/table-groups/{table_group_id}/schedules/disable'.format(
        protocol=config['protocol'], ip=config['ip'], port=config['port'], source_id=source_id,
        table_group_id=table_group_id)
    return request


def configure_table_group_schedule_user_url(config, source_id, table_group_id):
    """
    Return URL to configure Table Group schedule user
    """
    request = '{protocol}://{ip}:{port}/v3/sources/{source_id}/table-groups/{table_group_id}/schedules/user'.format(
        protocol=config['protocol'], ip=config['ip'], port=config['port'], source_id=source_id,
        table_group_id=table_group_id)
    return request