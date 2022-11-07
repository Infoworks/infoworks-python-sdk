import os

from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.cicd.download_configurations.utils import Utils


class DownloadSource(BaseClient):
    def __init__(self):
        super(DownloadSource, self).__init__()

    def cicd_get_sourceconfig_dumps(self, source_ids, config_file_dump_path, files_overwrite=True,
                                    serviceaccountemail="admin@infoworks.io",
                                    replace_words=""):
        # replace_words = "DEV->PROD;dev->prod"
        utils_obj = Utils(serviceaccountemail)
        if not os.path.exists(os.path.join(config_file_dump_path, "modified_files")):
            os.makedirs(os.path.join(config_file_dump_path, "modified_files"))
        if not os.path.exists(os.path.join(config_file_dump_path, "source")):
            os.makedirs(os.path.join(config_file_dump_path, "source"))
        target_file_path = os.path.join(os.path.join(config_file_dump_path, "modified_files"), "source.csv")
        if files_overwrite:
            open(target_file_path, 'w').close()
            mode = "a"
        else:
            mode = "a"
        f = open(target_file_path, mode)
        for source_id in source_ids:
            try:
                filename, configuration_obj = utils_obj.dump_to_file(self, "source", None,
                                                                                         source_id, replace_words,
                                                                                         config_file_dump_path)
                if filename is not None:
                    f.write(filename)
                    f.write("\n")
            except Exception as e:
                self.logger.error(f"Unable to dump configurations for source {source_id} due to {str(e)}")
                print(f"Unable to dump configurations for source {source_id} due to {str(e)}")
        f.close()
