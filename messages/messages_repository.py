import hazelcast

class MessagesRepository:
    def __init__(self):
        self.messages = []

    def add_message(self, msg):
        self.messages.append(msg)

    def get_messages(self):
        return ", ".join(self.messages)