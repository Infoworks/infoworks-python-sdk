from file_based_iwx_source import FileBasedIWXSource
from infoworks.sdk.client import InfoworksClientSDK

class CsvIwxSource(FileBasedIWXSource):
    def __init__(self):
        self.refresh_token = ""
        self.proxy_host = ""
        self.proxy_port = ""

    def create_source(self):
        pass

    def configure_source_connection(self):
        pass

    def configure_file_mappings(self):
        pass

    def configure_tables(self):
        pass