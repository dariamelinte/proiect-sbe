import threading
import logging
from typing import Dict, Any, List, Optional, TYPE_CHECKING
from queue import Queue

from .subscription import Subscription
from .proto_utils import parse_broker_message, create_broker_message, serialize_publication, deserialize_publication
from protos.message_pb2 import BrokerMessage

if TYPE_CHECKING:
    from .broker_network import BrokerNetwork

class Broker:
    def __init__(self, name: str, window_size: int, logger: Optional[logging.Logger] = None):
        self.name = name
        self.window_size = window_size
        self.logger = logger or logging.getLogger(__name__)
        self.subscriptions: List[Subscription] = []
        self.publication_queue = Queue()
        self.running = False
        self.thread = None
        self.network: Optional['BrokerNetwork'] = None
        
    def start(self):
        """Start the broker's processing thread."""
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._process_publications)
            self.thread.daemon = True
            self.thread.start()
            self.logger.info(f"Broker {self.name} started")
        
    def stop(self):
        """Stop the broker's processing thread."""
        self.running = False
        if self.thread:
            self.thread.join()
            self.logger.info(f"Broker {self.name} stopped")
        
    def add_subscription(self, subscription: Subscription):
        """Add a subscription to this broker."""
        self.subscriptions.append(subscription)
        self.logger.info(f"Added subscription to broker {self.name}")
        
    def publish(self, publication: Dict[str, Any]):
        """Add a publication to the broker's queue."""
        self.publication_queue.put(publication)
        
    def _process_publications(self):
        """Process publications from the queue."""
        while self.running:
            try:
                publication = self.publication_queue.get(timeout=1)
                self._match_publication(publication)
            except Queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Error processing publication: {e}")
        
    def _match_publication(self, publication: Dict[str, Any]):
        """Match a publication against all subscriptions."""
        for subscription in self.subscriptions:
            if subscription.matches(publication):
                self.logger.info(f"Publication matched subscription in broker {self.name}")
                # Notify the network about the match
                if self.network:
                    self.network.notify_match(subscription, publication)
        
    def get_subscriptions(self) -> List[Subscription]:
        """Get all subscriptions managed by this broker."""
        return self.subscriptions 