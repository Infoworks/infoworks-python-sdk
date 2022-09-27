from abc import ABC, abstractmethod


class FileBasedIWXSource(ABC):

    @abstractmethod
    def create_source(self):
        pass

    @abstractmethod
    def configure_source_connection(self):
        pass

    @abstractmethod
    def configure_file_mappings(self):
        pass

    @abstractmethod
    def configure_tables(self):
        pass