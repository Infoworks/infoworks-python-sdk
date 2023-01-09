class JobResponse:
    def __init__(self):
        pass

    @staticmethod
    def parse_result(status=None, error_code=None, error_desc=None, job_id=None, response=None):
        result = {
            'error': {
                'error_code': error_code,
                'error_desc': error_desc
            },
            'result': {
                'job_id': job_id,
                'status': status,
                'response': response
            }
        }
        return result
