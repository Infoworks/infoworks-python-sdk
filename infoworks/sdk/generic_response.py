class GenericResponse:
    def __init__(self):
        pass

    @staticmethod
    def parse_result(status=None, error_code=None, error_desc=None, job_id=None, response=None, entity_id=None):
        result = {
            'error': {
                'error_code': error_code,
                'error_desc': error_desc
            },
            'result': {
                'entity_id': entity_id,
                'job_id': job_id,
                'status': status,
                'response': response
            }
        }
        return result
