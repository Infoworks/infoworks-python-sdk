class SourceResponse:
    def __init__(self):
        pass

    @staticmethod
    def parse_result(status=None, error_code=None, error_desc=None, job_id=None, source_id=None, response=None):
        result = {
            'error': {
                'error_code': error_code,
                'error_desc': error_desc
            },
            'result': {
                'job_id': job_id,
                'status': status,
                'source_id': str(source_id) if source_id is not None else "",
                'response': response
            }
        }
        return result
