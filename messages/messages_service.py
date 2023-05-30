from messages_repository import MessagesRepository

class MessagesService:
    def __init__(self, messaging_repository: MessagesRepository):
        self.messaging_repository = messaging_repository

    def add_message(self, msg):
        self.messaging_repository.add_message(msg)

    def get_messages(self):
        return self.messaging_repository.get_messages()