import os

from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.cicd.download_configurations.utils import Utils


class DownloadPipeline(BaseClient):
    def __init__(self):
        super(DownloadPipeline, self).__init__()

    def cicd_get_pipelineconfig_export(self, pipeline_ids, config_file_export_path, files_overwrite=True,
                                       serviceaccountemail="admin@infoworks.io", replace_words="",
                                       pipeline_grp_config=None):
        self.cicd_get_pipelineconfig_dumps(pipeline_ids, config_file_export_path, files_overwrite, serviceaccountemail,
                                           replace_words, pipeline_grp_config)

    def cicd_get_pipelineconfig_dumps(self, pipeline_ids, config_file_dump_path, files_overwrite=True,
                                      serviceaccountemail="admin@infoworks.io", replace_words="",
                                      pipeline_grp_config=None):
        if not os.path.exists(os.path.join(config_file_dump_path, "modified_files")):
            os.makedirs(os.path.join(config_file_dump_path, "modified_files"))
        if not os.path.exists(os.path.join(config_file_dump_path, "pipeline")):
            os.makedirs(os.path.join(config_file_dump_path, "pipeline"))
        if not os.path.exists(os.path.join(config_file_dump_path, "pipeline_group")):
            os.makedirs(os.path.join(config_file_dump_path, "pipeline_group"))
        utils_obj = Utils(serviceaccountemail)
        target_file_path = os.path.join(os.path.join(config_file_dump_path, "modified_files"), "pipeline.csv")
        target_file_path_pipeline_groups = os.path.join(os.path.join(config_file_dump_path, "modified_files"),
                                                        "pipeline_group.csv")
        if files_overwrite:
            open(target_file_path, 'w').close()
            open(target_file_path_pipeline_groups, 'a').close()
            mode = "a"
        else:
            mode = "a"
        f = open(target_file_path, mode)
        f_pipeline_group = open(target_file_path_pipeline_groups, mode)
        for pipeline_id in pipeline_ids:
            try:
                json_obj = {"entity_id": pipeline_id, "entity_type": "pipeline"}
                domain_id = utils_obj.get_domain_id(self, json_obj)
                if domain_id:
                    filename, configuration_obj = utils_obj.dump_to_file(self, "pipeline", domain_id, pipeline_id,
                                                                         replace_words, config_file_dump_path)
                    if filename is not None:
                        f.write(filename)
                        f.write("\n")
            except Exception as e:
                self.logger.error(f"Unable to export configurations for pipeline {pipeline_id} due to {str(e)}")
                print(f"Unable to export configurations for pipeline {pipeline_id} due to {str(e)}")
        if pipeline_grp_config is not None:
            pipeline_group_id = pipeline_grp_config["id"]
            json_obj = {"entity_id": pipeline_group_id, "entity_type": "pipeline_group"}
            domain_id = utils_obj.get_domain_id(self, json_obj)
            if domain_id:
                filename, configuration_obj = utils_obj.dump_to_file(self, "pipeline_group", domain_id,
                                                                     pipeline_group_id,
                                                                     replace_words, config_file_dump_path)
                if filename is not None:
                    f_pipeline_group.write(filename)
                    f_pipeline_group.write("\n")
        f.close()
        f_pipeline_group.close()

if __name__ == "__main__":
    d_client = DownloadPipeline()