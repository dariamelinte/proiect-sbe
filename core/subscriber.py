import random
import uuid
from queue import Queue
from typing import Dict, Any, List, Callable
import logging
from datetime import datetime

from .subscription import Subscription
from .utils import log_event
from .generator_pub_sub import GeneratorPubSub

import threading
import time

class Subscriber:
    def __init__(self, subscriber_id: str, logger: logging.Logger = None, configs: Any = None):
        self.subscriber_id = subscriber_id
        self.subscriptions: Dict[str, Subscription] = {}
        self.logger = logger or logging.getLogger('pubsub_system')
        self.configs = configs
        self.generator = GeneratorPubSub(configs) if configs else None
        self.received_messages: List[Dict[str, Any]] = []
        self.latencies: List[float] = []
        self.is_running = False
        self.sub_thread = None
        self.message_queue = Queue()

    def start(self):
        self.is_running = True
        self.sub_thread = threading.Thread(target=self.run)
        self.sub_thread.start()
        print(f"{self.subscriber_id} started subscription loop")

    def stop(self):
        self.is_running = False
        if self.sub_thread:
            self.sub_thread.join()
        print(f"{self.subscriber_id} stopped")

    def run(self):
        print(f"{self.subscriber_id} thread started")
        while self.is_running:
            try:
                # Try to get a message with timeout so thread stays responsive
                message = self.message_queue.get(timeout=1)
                self.process_message(message)  # You need to implement this to handle incoming publications
            except queue.Empty:
                # No message received, time to add subscriptions?
                # Could add new subscriptions here every N seconds using a timer logic

                current_time = time.time()
                if not hasattr(self, "_last_sub_time"):
                    self._last_sub_time = 0
                if current_time - self._last_sub_time > 60:
                    simple_cond = generate_random_subscription(self.generator)
                    self.create_simple_subscription(simple_cond)
                    print(f"{self.subscriber_id} added new simple subscription")

                    if random.random() < 0.3:
                        window_cond = generate_random_window_subscription(self.generator)
                        self.create_window_subscription(window_cond)
                        print(f"{self.subscriber_id} added new window subscription")

                    self._last_sub_time = current_time

        print(f"{self.subscriber_id} thread exiting")

    def create_simple_subscription(self, conditions) -> Subscription:
        """Create a simple subscription with specified conditions"""
        subscription = Subscription(conditions=conditions, subscriber=self)
        self.subscriptions[subscription.id] = subscription
        log_conditions = [
            {
                'field': condition[0],
                'operator': condition[1],
                'value': str(condition[2])
            }
            for condition in conditions
        ]
        log_event(self.logger, 'simple_subscription_created', {
            'subscriber_id': self.subscriber_id,
            'subscription_id': subscription.id,
            'conditions': log_conditions
        })
        return subscription

    def create_window_subscription(self, conditions) -> Subscription:
        """Create a window-based subscription with specified conditions"""
        subscription = Subscription(conditions=conditions, window_size=10, subscriber=self)
        self.subscriptions[subscription.id] = subscription
        # Convert conditions to a serializable format
        log_conditions = [
            {
                'field': condition[0],
                'operator': condition[1],
                'value': str(condition[2])
            }
            for condition in subscription.conditions
        ]
        log_event(self.logger, 'window_subscription_created', {
            'subscriber_id': self.subscriber_id,
            'subscription_id': subscription.id,
            'conditions': log_conditions,
        })
        return subscription

    def receive_message(self, message: Dict[str, Any]):
        receive_time = time.time()

        # Transformă timestamp-ul din mesaj (ISO) în epoch
        pub_timestamp_str = message.get('timestamp')
        pub_timestamp = datetime.fromisoformat(pub_timestamp_str).timestamp() if pub_timestamp_str else receive_time

        latency = receive_time - pub_timestamp
        # Salvează latenta undeva, ex:
        if not hasattr(self, 'latencies'):
            self.latencies = []
        self.latencies.append(latency)

        self.received_messages.append(message)
        log_event(self.logger, 'message_received', {
            'subscriber_id': self.subscriber_id,
            'message': message,
            'latency': latency
        })

    def average_latency(self):
        if hasattr(self, 'latencies') and self.latencies:
            return sum(self.latencies) / len(self.latencies)
        return 0.0

    def get_received_messages(self) -> List[Dict[str, Any]]:
        """Get all received messages"""
        return self.received_messages

    def clear_messages(self):
        """Clear received messages"""
        self.received_messages = []

def generate_random_subscription(generator: GeneratorPubSub):
    """Generate a random subscription using GeneratorPubSub"""
    import threading  # Add this import at the top of the file if not already present
    event = threading.Event()  # Create an event object
    sub = generator.generate_single_sub()
    event.set()  # Signal that the operation is complete
    event.wait()  # Wait until the event is set
    return sub

def generate_random_window_description(generator: GeneratorPubSub):
    """Generate a random window-based subscription description"""
    import threading  # Add this import at the top of the file if not already present
    event = threading.Event()  # Create an event object
    sub = generator.generate_single_window_sub()
    event.set()  # Signal that the operation is complete
    event.wait()  # Wait until the event is set
    return sub

def generate_random_window_subscription(generator: GeneratorPubSub):
    """Generate a random window-based subscription with random conditions and window size"""
    conditions = generate_random_window_description(generator)
    return conditions