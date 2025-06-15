import threading
import time
from typing import Dict, List, Any, Set, Tuple
from queue import Queue
from datetime import datetime
import json
import logging
from .broker import Broker
from .subscription import Subscription
from .subscriber import Subscriber

class BrokerNetwork:
    def __init__(self, num_brokers: int = 3, window_size: int = 10, logger: logging.Logger = None):
        self.brokers = [Broker(f"broker_{i}", window_size, logger) for i in range(num_brokers)]
        self.current_broker_index = 0
        self.logger = logger or logging.getLogger(__name__)
        self.logger.info(f"Created broker network with {num_brokers} brokers and window size {window_size}")

    def start(self):
        """Start all brokers in the network"""
        self.logger.info(f"Starting {len(self.brokers)} brokers")
        for broker in self.brokers:
            broker.start()

    def stop(self):
        """Stop all brokers in the network"""
        self.logger.info(f"Stopping {len(self.brokers)} brokers")
        for broker in self.brokers:
            broker.stop()

    def add_subscriber(self, subscriber: Subscriber):
        """Add a subscriber to the network and distribute its subscriptions."""
        self.logger.info(f"Adding subscriber {subscriber.name}")
        for subscription in subscriber.subscriptions:
            self.add_subscription(subscription)

    def add_subscription(self, subscription: Subscription):
        """Add a subscription to a broker using round-robin distribution"""
        broker = self.brokers[self.current_broker_index]
        self.current_broker_index = (self.current_broker_index + 1) % len(self.brokers)
        broker.add_subscription(subscription)
        self.logger.info(f"Added subscription to broker {broker.name}")

    def publish(self, publication: Dict[str, Any]):
        """Publish to all brokers in the network"""
        self.logger.info(f"Publishing to {len(self.brokers)} brokers: {publication}")
        for broker in self.brokers:
            broker.publish(publication)

    def notify_match(self, subscription: Subscription, publication: Dict[str, Any]):
        """Notify when a publication matches a subscription."""
        self.logger.info(f"Match found: {publication} matches subscription")
        # The subscriber will handle the notification through its process_matches method 