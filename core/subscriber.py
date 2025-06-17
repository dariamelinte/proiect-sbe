import random
import queue
from queue import Queue
from typing import Dict, Any, List
import logging
from datetime import datetime
from dateutil import parser
from .subscription import Subscription
from .utils import log_event
from .generator_pub_sub import GeneratorPubSub

import threading
import time

class Subscriber:
    def __init__(self, subscriber_id: str, logger: logging.Logger = None, configs: Any = None, pass_generation: bool = False):
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
        self.pass_generation = pass_generation  # Flag to control subscription generation

    def start(self):
        """Start the subscriber thread"""
        self.is_running = True
        self.sub_thread = threading.Thread(target=self.run)
        self.sub_thread.start()
        print(f"{self.subscriber_id} started subscription loop")

    def stop(self):
        """Stop the subscriber thread"""
        self.is_running = False
        if self.sub_thread:
            self.sub_thread.join()
        print(f"{self.subscriber_id} stopped")

    def run(self, pass_generation = False):
        """Run the subscriber thread to process messages and manage subscriptions"""
        print(f"{self.subscriber_id} thread started")
        while self.is_running:
            try:
                # Try to get a message with timeout so thread stays responsive
                message = self.message_queue.get(timeout=1)
                self.process_message(message)
            except queue.Empty:
                # No message received, time to add subscriptions
                if self.pass_generation:
                    continue
                current_time = time.time()
                if not hasattr(self, "_last_sub_time"):
                    self._last_sub_time = 0
                if int(current_time) - self._last_sub_time > 60:
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
        """Receive a message and calculate latency"""
        self.received_messages.append(message)

        try:
            timestamp_str = message.get('timestamp')
            latency_ms = None

            if timestamp_str:
                sent_time = parser.isoparse(timestamp_str)
                # Get receive time as timezone-aware UTC
                receive_time = datetime.now()

                latency_ms = (receive_time - sent_time).total_seconds() * 1000
                if latency_ms >= 0:
                    self.latencies.append(latency_ms)

            log_event(self.logger, 'message_received', {
                'subscriber_id': self.subscriber_id,
                'message_id': message.get('id', repr(message)),
                'latency_ms': latency_ms
            })

        except Exception as e:
            self.logger.error(f"Error calculating latency: {e}")

    def average_latency(self):
        """Calculate the average latency of received messages"""
        if hasattr(self, 'latencies') and self.latencies:
            return sum(self.latencies) / len(self.latencies)
        return 0.0

    def get_received_messages(self) -> List[Dict[str, Any]]:
        """Get all received messages"""
        return self.received_messages

    def clear_messages(self):
        """Clear received messages"""
        self.received_messages = []

    def process_message(self, message):
        pass


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
