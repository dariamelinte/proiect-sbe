import time
import threading
from typing import Dict, Any
from queue import Queue
from datetime import datetime
from core.proto import publication_pb2 as pb

from .generator_pub_sub import GeneratorPubSub
from .generator_configs import Configs
from .utils import log_event


class Publisher:
    def __init__(self, configs: Configs):
        self.configs = configs
        self.generator = GeneratorPubSub(configs)
        self.publication_queue = Queue()
        self.is_running = False
        self.publication_thread = None
        self.threads = []
        self.generated_publications = 0

    def generate_publications_proto(self, batch_size=5):
        """Generate multiple publications per iteration using GeneratorPubSub and add them to the queue"""
        while self.is_running:
            for _ in range(batch_size):
                data = self.generator.generate_pub()
                if data:
                    from datetime import datetime, timezone
                    data['timestamp'] = datetime.now(timezone.utc).isoformat()

                    pub_msg = pb.Publication(
                        station_id=data['station_id'],
                        city=data['city'],
                        direction=data['direction'],
                        temperature=data['temperature'],
                        rain=data['rain'],
                        wind=data['wind'],
                        created_at=data['created_at'],
                        timestamp=data['timestamp'],
                    )

                    # Serializăm mesajul într-un bytes
                    serialized_pub = pub_msg.SerializeToString()

                    # Adăugăm bytes în coadă (transmiterea binară)
                    self.publication_queue.put(serialized_pub)
                    self.generated_publications += 1

            time.sleep(0.1)

    def generate_publications(self, batch_size=20):
        """Generate multiple publications per iteration using GeneratorPubSub and add them to the queue"""
        while self.is_running:
            for _ in range(batch_size):
                publication = self.generator.generate_pub()
                if publication:
                    from datetime import datetime, timezone
                    publication['timestamp'] = datetime.now()
                    self.publication_queue.put(publication)

    def start(self, num_threads=4):
        """Start the publisher with multiple threads generating publications"""
        self.is_running = True
        self.threads = []
        for _ in range(num_threads):
            t = threading.Thread(target=self.generate_publications_proto)
            t.start()
            self.threads.append(t)
        log_event(self.configs.logger, 'publisher_started', {
            'message': 'Publisher started',
            'num_threads': num_threads
        })

    def stop(self):
        """Stop the publisher and wait for threads to finish"""
        self.is_running = False
        for t in self.threads:
            t.join()
        log_event(self.configs.logger, 'publisher_stopped', {
            'message': 'Publisher stopped',
            'generated_publications': self.generated_publications
        })

    def get_publication(self) -> Dict[str, Any]:
        """Get the next publication from the queue"""
        return self.publication_queue.get()
