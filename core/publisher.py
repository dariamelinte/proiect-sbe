import threading
import time
import json
import uuid
from typing import Dict, Any
import redis

from .generator_configs import Configs
from .generator_pub_sub import GeneratorPubSub


class Publisher:
    """
    The Publisher generates publications and pushes them to a central Redis queue.
    """
    def __init__(self, configs: Configs, redis_client: redis.Redis):
        self.configs = configs
        self.redis_client = redis_client
        self.publication_queue_name = 'publications_queue'
        self.is_running = False
        self.thread = None
        # The publisher creates its own generator instance
        self.generator = GeneratorPubSub(configs)

    def _generate_publications_loop(self):
        """
        Continuously generates publications and pushes them to the Redis queue.
        """
        while self.is_running:
            publication_data = self.generator.generate_pub()
            if not publication_data:
                continue

            publication_data['id'] = str(uuid.uuid4())
            publication_data['timestamp'] = time.time()
            
            self.redis_client.lpush(self.publication_queue_name, json.dumps(publication_data))
            
            # Use a fixed interval for publishing
            time.sleep(0.1)

    def start(self):
        """
        Starts the publisher in a background thread.
        """
        if self.is_running:
            print("Publisher is already running.")
            return
            
        self.is_running = True
        self.thread = threading.Thread(target=self._generate_publications_loop)
        self.thread.start()
        print("Publisher started and is pushing to Redis.")

    def stop(self):
        """
        Stops the publisher and waits for its thread to finish.
        """
        if self.is_running:
            self.is_running = False
            if self.thread:
                self.thread.join()
            print("Publisher stopped.")
