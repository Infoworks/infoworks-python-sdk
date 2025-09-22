import os
from infoworks.sdk.cicd.download_configurations.utils import Utils
from infoworks.sdk.base_client import BaseClient

class DownloadWorkflow(BaseClient):
    def __init__(self):
        super(DownloadWorkflow, self).__init__()

    def cicd_get_workflowconfig_export(self, workflow_ids, config_file_export_path, files_overwrite=True,
                                      serviceaccountemail="admin@infoworks.io",
                                      replace_words="", workflow_version_map=None):
        """
        Export workflow configurations with optional version specification.
        :param workflow_ids: List of workflow IDs to export
        :param config_file_export_path: Path to save the exported configurations
        :param files_overwrite: Whether to overwrite existing files
        :param serviceaccountemail: Service account email
        :param replace_words: Words to replace in the configuration
        :param workflow_version_map: Dictionary mapping workflow IDs to version IDs
        """
        self.cicd_get_workflowconfig_dumps(workflow_ids, config_file_export_path, files_overwrite,
                                          serviceaccountemail, replace_words, workflow_version_map or {})

    def cicd_get_workflowconfig_dumps(self, workflow_ids, config_file_dump_path, files_overwrite=True,
                                     serviceaccountemail="admin@infoworks.io",
                                     replace_words="", workflow_version_map=None):
        """
        Dump workflow configurations to files.
        :param workflow_ids: List of workflow IDs
        :param config_file_dump_path: Path to save configurations
        :param files_overwrite: Whether to overwrite existing files
        :param serviceaccountemail: Service account email
        :param replace_words: Words to replace in the configuration
        :param workflow_version_map: Dictionary mapping workflow IDs to version IDs
        """
        if not os.path.exists(os.path.join(config_file_dump_path, "modified_files")):
            os.makedirs(os.path.join(config_file_dump_path, "modified_files"))
        if not os.path.exists(os.path.join(config_file_dump_path, "workflow")):
            os.makedirs(os.path.join(config_file_dump_path, "workflow"))
        utils_obj = Utils(serviceaccountemail)
        target_file_path = os.path.join(os.path.join(config_file_dump_path, "modified_files"), "workflow.csv")
        if files_overwrite:
            open(target_file_path, 'w').close()
            mode = "a"
        else:
            mode = "a"
        f = open(target_file_path, mode)
        workflow_version_map = workflow_version_map or {}
        for workflow_id in workflow_ids:
            try:
                json_obj = {"entity_id": workflow_id, "entity_type": "workflow"}
                domain_id = utils_obj.get_domain_id(self, json_obj)
                if domain_id:
                    # Pass version_id to dump_to_file if available
                    version_id = workflow_version_map.get(workflow_id)
                    filename, configuration_obj = utils_obj.dump_to_file(
                        self, "workflow", domain_id, workflow_id, replace_words,
                        config_file_dump_path, version_id=version_id
                    )
                    if filename is not None:
                        f.write(filename)
                        f.write("\n")
            except Exception as e:
                self.logger.error(f"Unable to export configurations for workflow {workflow_id} due to {str(e)}")
                print(f"Unable to export configurations for workflow {workflow_id} due to {str(e)}")
        f.close()