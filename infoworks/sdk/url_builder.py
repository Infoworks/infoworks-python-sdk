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


def create_data_connection(config, domain_id):
    """
    returns URL to create data connection using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain_id
    :type domain_id: string
    :return: url for data connection creation
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/data-connections'.format(ip=config['ip'],
                                                                                        port=config['port'],
                                                                                        protocol=config['protocol'],
                                                                                        domain_id=domain_id)
    return request


def get_data_connection(config, domain_id, dataconnection_id):
    """
    returns URL to get data connection details using v3 api
    :param config: client configurations
    :type config: dict
    :param domain_id: Identifier for domain_id
    :type domain_id: string
    :param dataconnection_id: Identifier for data connection
    :type dataconnection_id: string
    :return: url to get data connection details
    """
    request = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/data-connections/{dataconnection_id}'.format(
        ip=config['ip'],
        port=config['port'],
        protocol=config['protocol'],
        domain_id=domain_id, dataconnection_id=dataconnection_id)
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
