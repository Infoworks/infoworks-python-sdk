POLLING_FREQUENCY_IN_SEC = 15
NUM_POLLING_RETRIES = 3
POLLING_TIMEOUT = 1200
NUM_FAILURE_RETRIES = 5
RUNNING_JOB_LOGGING_COUNTER = 10
REQUEST_TIMEOUT_IN_SEC = 60
MAX_RETRIES = 1
LOG_LOCATION = "/tmp/iwx_sdk.log"


class Response(object):
    class Code(object):
        OK = 200

    class Status(object):
        FAILED = 'failed'
        SUCCESS = 'success'
        ERROR = 'error'
        PENDING = 'pending'
        RUNNING = 'running'


class EntityType(object):
    TABLE = 'table'
    SOURCE = 'source'
    PIPELINE = 'pipeline'
    PIPELINE_TARGET = 'pipeline_target'
    JOB = 'job'
    CONFIG = 'config'
    TABLE_GROUP = 'table_group'
    WORKFLOW = 'workflow'


class Job(object):
    class Status(object):
        PENDING = 'pending'
        RUNNING = 'running'
        BLOCKED = 'blocked'
        FAILED = 'failed'
        COMPLETED = 'completed'
        CANCELED = 'canceled'
        ABORTED = 'aborted'
        SUCCESS = 'success'


class ErrorCode(object):
    ENTITY_NOT_FOUND = 1400
    DEPENDENT_ENTITY_NOT_FOUND = 1401
    ENTITY_STATE_INCONSISTENT = 1402
    DEPENDENT_ENTITY_STATE_INCONSISTENT = 1403
    ACTION_LOCKED_FOR_ENTITY = 1404
    MANDATORY_PARAMETER_MISSING = 1405
    ENTITY_LOCKED = 1500
    DEPENDENT_ENTITY_LOCKED = 1501
    COMMUNICATION_FAILURE = 1600
    FSM_ERROR = 1601
    INTERNAL_SERVER_ERROR = 500
    RESOURCE_NOT_FOUND = 404
    USER_ERROR = 1406
    GENERIC_ERROR = 1407


class SourceMappings(object):
    oracle = {"sub_type": "oracle", "driver_name": "oracle.jdbc.driver.OracleDriver", "driver_version": "v2",
              "database": "ORACLE"}
    teradata = {"sub_type": "teradata", "driver_name": "com.teradata.jdbc.TeraDriver", "driver_version": "v2",
                "database": "TERADATA"}
    netezza = {"sub_type": "netezza", "driver_name": "org.netezza.Driver", "driver_version": "v2",
               "database": "NETEZZA"}
