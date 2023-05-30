from logging_repository import LoggingRepository

class LoggingService:
    def __init__(self, logging_repository: LoggingRepository):
        self.logging_repository = logging_repository

    def add_message(self, msg):
        self.logging_repository.add_message(msg)

    def get_logs(self):
        return self.logging_repository.get_logs()