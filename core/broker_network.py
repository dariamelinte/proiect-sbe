import threading
import time
from typing import Dict, List, Any, Set, Tuple
from queue import Queue
from datetime import datetime
import json
import logging
from .broker import Broker
from .subscription import Subscription
from .utils import log_event

class BrokerNetwork:
    def __init__(self, num_brokers: int = 3, window_size: int = 10, logger: logging.Logger = None):
        self.brokers = [Broker(f"broker_{i}", window_size, logger) for i in range(num_brokers)]
        self.current_broker_index = 0
        self.logger = logger or logging.getLogger('pubsub_system')
        log_event(self.logger, 'broker_network_created', {
            'num_brokers': num_brokers,
            'window_size': window_size
        })

    def start(self):
        """Start all brokers in the network"""
        log_event(self.logger, 'broker_network_starting', {
            'num_brokers': len(self.brokers)
        })
        for broker in self.brokers:
            broker.start()

    def stop(self):
        """Stop all brokers in the network"""
        log_event(self.logger, 'broker_network_stopping', {
            'num_brokers': len(self.brokers)
        })
        for broker in self.brokers:
            broker.stop()

    def add_subscription(self, subscription: Subscription) -> str:
        """Add a subscription to a broker using round-robin distribution"""
        broker = self.brokers[self.current_broker_index]
        self.current_broker_index = (self.current_broker_index + 1) % len(self.brokers)
        subscription_id = broker.add_subscription(subscription)
        log_event(self.logger, 'subscription_distributed', {
            'broker_id': broker.broker_id,
            'subscription_id': subscription_id,
            'current_broker_index': self.current_broker_index
        })
        return subscription_id

    def publish(self, publication: Dict[str, Any]):
        """Publish to all brokers in the network"""
        log_event(self.logger, 'publication_distributed', {
            'num_brokers': len(self.brokers),
            'publication': publication
        })
        for broker in self.brokers:
            broker.publication_queue.put(publication)