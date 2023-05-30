import uvicorn
from fastapi import FastAPI, APIRouter
from facade_service import FacadeService
import random
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from message import Message
from requests.exceptions import ConnectionError
from urllib3.exceptions import MaxRetryError
from consul import Consul

localhost = "127.0.0.1"
facade_port = 8001

class FacadeController:
    def __init__(self, facade_service: FacadeService):
        self.router = APIRouter()
        self.router.add_api_route("/", self.get_req, methods=["GET"])
        self.router.add_api_route("/", self.post_req, methods=["POST"])
        self.facade_service = facade_service
        self.consul = Consul()

    def get_random_service_addr(self, service_name: str):
        index, services = self.consul.health.service(service_name)
        if not services:
            raise Exception(f"No instances of service {service_name} found.")
        service = random.choice(services)
        service_address = service['Service']['Address']
        service_port = service['Service']['Port']
        return service_address, service_port

    async def get_req(self):
        success = False
        print("Logging: Connecting to random port...")
        logging_address, logging_port = self.get_random_service_addr('logging-service')
        while not success:
            try:
                logging_url = f"http://{logging_address}:{logging_port}"
                logging_response = self.facade_service.get_logged_messages(logging_url)
                success = True
            except (ConnectionError, MaxRetryError) as e:
                pass

        print("Getting messages from messages service")
        index, messages_services = self.consul.health.service('messages-service')
        messages_resposes = ""
        for messages_service in messages_services:
            messages_address = messages_service['Service']['Address']
            messages_port = messages_service['Service']['Port']
            messages_url = f"http://{messages_address}:{messages_port}"
            messages_response = self.facade_service.get_messages(messages_url)
            messages_resposes += ', ' + messages_response.json()

        text = "Logged messages: " +  logging_response.json() + " | " + "Messages service response: "  + messages_resposes
        return text

    async def post_req(self, msg: Message):
        if not msg.msg:
            return "Empty message"

        success = False
        print("Logging: Connecting to random port...")
        logging_address, logging_port = self.get_random_service_addr('logging-service')
        while not success:
           try:
               logging_url = f"http://{logging_address}:{logging_port}"
               logging_response = self.facade_service.log_message(msg, logging_url)
               success = True
           except (ConnectionError, MaxRetryError) as e:
               pass
        
        if logging_response.json() == "Empty message":
           return "Empty message"

        print("Sending message")
        self.facade_service.send_message(msg.msg)

        return f"Message: '{msg.msg}' sent"


if __name__ == "__main__":
    facade_service = FacadeService()
    app = FastAPI() 
    facade_controller = FacadeController(facade_service) 
    app.include_router(facade_controller.router) 
    uvicorn.run(app, host=localhost, port=facade_port)