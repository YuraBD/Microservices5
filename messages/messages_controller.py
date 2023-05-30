from fastapi import FastAPI, APIRouter
import uvicorn
import argparse
from messages_repository import MessagesRepository
from messages_service import MessagesService
import json
import asyncio
from aiokafka import AIOKafkaConsumer
from consul import Consul

localhost = "127.0.0.1"

class MessagesController:
    def __init__(self, messages_service):
        self.router = APIRouter()
        self.router.add_api_route("/", self.get_req, methods=["GET"])
        self.messages_service = messages_service

    async def consume_messages(self):
        consul = Consul()
        index, data = consul.kv.get('kafka/settings')
        kafka_settings = json.loads(data['Value'].decode())

        consumer = AIOKafkaConsumer(
            kafka_settings['topics'][str(id(self))],
            bootstrap_servers=kafka_settings['bootstrap_servers'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        await consumer.start()
        try:
            async for msg in consumer:
                print("Got message: ", msg.value['msg'])
                self.messages_service.add_message(msg.value['msg'])
        finally:
            await consumer.stop()

    async def post_req(self):
        message = message.value
        print(f'Received message: {message}')

    async def get_req(self):
        print("Getting messages...")
        msg_list = self.messages_service.get_messages()
        return msg_list


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Accept a port number as argument.")
    parser.add_argument("--port", type=int, required=True, help="Port number (8006 or 8007)")
    args = parser.parse_args()
    messages_port = args.port
    if messages_port in [8006, 8007]:
        print(f"Using port {messages_port}")
    else:
        print("Invalid port number. Please use 8004, 8005, or 8006.")
        exit()

    messages_repository = MessagesRepository()
    messages_service = MessagesService(messages_repository)
    app = FastAPI()
    messages_controller = MessagesController(messages_service)
    app.include_router(messages_controller.router) 

    @app.on_event("startup")
    async def startup_event():
        asyncio.create_task(messages_controller.consume_messages())

    consul = Consul()
    consul.agent.service.register(
        name="messages-service",
        service_id=str(id(messages_controller)),
        address=localhost,
        port=messages_port
    )

    if messages_port == 8006:
        topic = 'service1'
    else:
        topic = 'service2'
    index, data = consul.kv.get('kafka/settings')
    if data is not None:
        kafka_settings = json.loads(data['Value'].decode())
        kafka_settings['topics'][str(id(messages_controller))] = topic
    else:
        kafka_settings = {
            'topics' : { str(id(messages_controller)): topic },
            'bootstrap_servers': ['localhost:9092']
        }
    consul.kv.put('kafka/settings', json.dumps(kafka_settings))

    uvicorn.run(app, host=localhost, port=messages_port)
