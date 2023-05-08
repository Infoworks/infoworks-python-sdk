from infoworks.sdk.base_client import BaseClient
from infoworks.sdk.generic_response import GenericResponse
from infoworks.sdk.local_configurations import Response


class ReplicatorClient(BaseClient):
    def __init__(self):
        super(ReplicatorClient, self).__init__()

    def sample_replicator_function(self, msg):
        print(msg)
        response = msg
        self.logger.info(response)
        return GenericResponse.parse_result(status=Response.Status.SUCCESS, response=response)
