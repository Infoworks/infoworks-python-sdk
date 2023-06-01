class AuthenticationError(Exception):
    def __init__(self, message):
        self.message = message
        super(AuthenticationError, self).__init__(self.message)


class SourceError(Exception):
    def __init__(self, message):
        self.message = message
        super(SourceError, self).__init__(self.message)


class PipelineError(Exception):
    def __init__(self, message):
        self.message = message
        super(PipelineError, self).__init__(self.message)


class WorkflowError(Exception):
    def __init__(self, message):
        self.message = message
        super(WorkflowError, self).__init__(self.message)


class DomainError(Exception):
    def __init__(self, message):
        self.message = message
        super(DomainError, self).__init__(self.message)


class AdminError(Exception):
    def __init__(self, message):
        self.message = message
        super(AdminError, self).__init__(self.message)


class JobsError(Exception):
    def __init__(self, message):
        self.message = message
        super(JobsError, self).__init__(self.message)


class GenericError(Exception):
    def __init__(self, message):
        self.message = message
        super(GenericError, self).__init__(self.message)
