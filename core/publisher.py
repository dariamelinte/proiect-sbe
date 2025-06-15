import threading
import time
import queue
import random
import logging
from datetime import datetime
from typing import Dict, Any

from .proto_utils import create_broker_message
from protos.message_pb2 import BrokerMessage

class Publisher:
    def __init__(self, configs):
        self.configs = configs
        self.publication_queue = queue.Queue()
        self.running = False
        self.thread = None
        self.logger = logging.getLogger('publisher')
        
    def start(self):
        """Start the publisher thread."""
        self.running = True
        self.thread = threading.Thread(target=self._generate_publications)
        self.thread.daemon = True
        self.thread.start()
        self.logger.info("Publisher started")
        
    def stop(self):
        """Stop the publisher thread."""
        self.running = False
        if self.thread:
            self.thread.join()
        self.logger.info("Publisher stopped")
        
    def _generate_publications(self):
        """Generate publications in a separate thread."""
        while self.running:
            try:
                # Generate a random publication
                publication = self._create_random_publication()
                
                # Add to queue
                self.publication_queue.put(publication)
                
                # Log the publication
                self.logger.info(f"Generated publication: {publication}")
                
                # Sleep for the configured interval
                time.sleep(self.configs.publication_interval)
                
            except Exception as e:
                self.logger.error(f"Error generating publication: {str(e)}")
                
    def _create_random_publication(self) -> Dict[str, Any]:
        """Create a random publication based on configuration."""
        # Choose a random field from the schema
        field = random.choice(self.configs.fields)
        
        # Generate value based on field type
        if field in self.configs.numeric_fields:
            range_info = self.configs.get_field_range(field)
            value = random.uniform(range_info['min'], range_info['max'])
        elif field in self.configs.string_fields:
            choices = self.configs.get_field_choices(field)
            value = random.choice(choices)
        else:  # date field
            value = datetime.now().isoformat()
        
        return {
            'type': field,
            'value': value,
            'timestamp': datetime.now().isoformat()
        }
        
    def get_publication(self) -> Dict[str, Any]:
        """Get the next publication from the queue."""
        try:
            return self.publication_queue.get_nowait()
        except queue.Empty:
            return None
