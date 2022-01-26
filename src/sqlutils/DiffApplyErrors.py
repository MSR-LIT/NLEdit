class DiffParseError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

class InvalidSQLError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

class RedundantOpError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message