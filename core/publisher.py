import time
import json
import threading
from typing import List, Dict, Any
from queue import Queue
from datetime import datetime

from .generator_pub_sub import GeneratorPubSub
from .generator_configs import Configs

class Publisher:
    def __init__(self, configs: Configs):
        self.configs = configs
        self.generator = GeneratorPubSub(configs)
        self.publication_queue = Queue()
        self.is_running = False
        self.publication_thread = None

    def generate_publications(self):
        """Generate publications using the GeneratorPubSub and add them to the queue"""
        while self.is_running:
            # Generate a single publication
            publication = self.generator.generate_pub()
            if publication:
                # Add timestamp to the publication
                publication['timestamp'] = datetime.now().isoformat()
                self.publication_queue.put(publication)
            time.sleep(0.1)  # Small delay to control generation rate

    def start(self):
        """Start the publisher"""
        self.is_running = True
        self.publication_thread = threading.Thread(target=self.generate_publications)
        self.publication_thread.start()
        print("Publisher started")

    def stop(self):
        """Stop the publisher"""
        self.is_running = False
        if self.publication_thread:
            self.publication_thread.join()
        print("Publisher stopped")

    def get_publication(self) -> Dict[str, Any]:
        """Get the next publication from the queue"""
        return self.publication_queue.get()
