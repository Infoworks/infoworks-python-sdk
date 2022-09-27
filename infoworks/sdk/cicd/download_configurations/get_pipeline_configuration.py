import os

from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.cicd.download_configurations.utils import Utils


class DownloadPipeline(BaseClient):
    def __init__(self):
        super(DownloadPipeline, self).__init__()

    def cicd_get_pipelineconfig_dumps(self, pipeline_ids, config_file_dump_path, files_overwrite=True, serviceaccountemail="admin@infoworks.io", replace_words=""):
        if not os.path.exists(os.path.join(config_file_dump_path, "modified_files")):
            os.makedirs(os.path.join(config_file_dump_path, "modified_files"))
        if not os.path.exists(os.path.join(config_file_dump_path, "pipeline")):
            os.makedirs(os.path.join(config_file_dump_path, "pipeline"))
        utils_obj = Utils(serviceaccountemail)
        target_file_path = os.path.join(os.path.join(config_file_dump_path, "modified_files"), "pipeline.csv")
        if files_overwrite:
            open(target_file_path, 'w').close()
            mode = "a"
        else:
            mode = "a"
        f = open(target_file_path, mode)
        for pipeline_id in pipeline_ids:
            try:
                json_obj = {"entity_id": pipeline_id, "entity_type": "pipeline"}
                domain_id = utils_obj.get_domain_id(self, json_obj)
                if domain_id:
                    filename, configuration_obj = utils_obj.dump_to_file(self, "pipeline", domain_id, pipeline_id, replace_words, config_file_dump_path)
                    if filename is not None:
                        f.write(filename)
                        f.write("\n")
            except Exception as e:
                self.logger.error(f"Unable to dump configurations for pipeline {pipeline_id} due to {str(e)}")
        f.close()
