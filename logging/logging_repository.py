import hazelcast
from consul import Consul
import json

class LoggingRepository:
    def __init__(self, ):
        self.map = dict()
        self.client = hazelcast.HazelcastClient()
        
        consul = Consul()
        index, data = consul.kv.get('hazelcast/settings')
        hazelcast_settings = json.loads(data['Value'].decode())
        self.map = self.client.get_map(hazelcast_settings['map']).blocking()

    def add_message(self, msg):
        self.map.put(msg.uuid, msg.msg)

    def get_logs(self):
        return ", ".join(self.map.values())