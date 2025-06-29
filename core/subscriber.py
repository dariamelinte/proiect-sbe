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
    def __init__(self, subscriber_id: str, logger: logging.Logger = None, configs: Any = None, flag_pass_generation: bool = False, flag_generate_window: bool = False):
        self._last_sub_time = time.time()
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
        self.flag_pass_generation = flag_pass_generation # If True, subscriber will not generate subscriptions automatically
        self.flag_generate_window = flag_generate_window # If True, subscriber will generate window subscriptions with a probability of 0.6

    def start(self):
        self.is_running = True
        self.sub_thread = threading.Thread(target=self.run)
        self.sub_thread.start()
        log_event(self.logger, 'subscriber_started', {
            'subscriber_id': self.subscriber_id,
            'message': 'Subscription loop started'
        })

    def stop(self):
        self.is_running = False
        if self.sub_thread:
            self.sub_thread.join()
        log_event(self.logger, 'subscriber_stopped', {
            'subscriber_id': self.subscriber_id,
            'message': 'Subscription loop stopped'
        })

    def run(self):
        """Run the subscriber thread to generate subscriptions periodically"""
        log_event(self.logger, 'subscriber_run_started', {
            'subscriber_id': self.subscriber_id,
            'flag_pass_generation': self.flag_pass_generation
        })

        while self.is_running:
            if self.flag_pass_generation:
                time.sleep(1)
                continue

            current_time = time.time()
            if not hasattr(self, "_last_sub_time"):
                self._last_sub_time = 0

            if int(current_time) - self._last_sub_time > 30:
                simple_cond = generate_random_subscription(self.generator)
                self.create_simple_subscription(simple_cond)

                if random.random() < 0.6 and self.flag_generate_window:
                    window_cond = generate_random_window_subscription(self.generator)
                    self.create_window_subscription(window_cond)

                self._last_sub_time = current_time

            time.sleep(1)

        log_event(self.logger, 'subscriber_run_ended', {
            'subscriber_id': self.subscriber_id
        })

    def create_simple_subscription(self, conditions) -> Subscription:
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
        subscription = Subscription(conditions=conditions, window_size=10, subscriber=self)
        self.subscriptions[subscription.id] = subscription
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
            'conditions': log_conditions
        })
        return subscription

    def receive_message(self, message: Dict[str, Any]):
        self.received_messages.append(message)
        try:

            timestamp_str = message.get('timestamp')
            latency_ms = None
            sent_time = None
            receive_time = None
            if timestamp_str:
                sent_time = parser.isoparse(timestamp_str)
                receive_time = datetime.now(sent_time.tzinfo)  # Ensure same tzinfo
                latency_ms = (receive_time - sent_time).total_seconds() * 1000
                if latency_ms >= 0:
                    self.latencies.append(latency_ms)

            log_event(self.logger, 'message_received', {
                'subscriber_id': self.subscriber_id,
                'message_id': message.get('id', repr(message)),
                'latency_ms': latency_ms,
                'received_at': receive_time.isoformat() if receive_time else None,
                'sent_at': sent_time.isoformat() if sent_time else None
            })


        except Exception as e:
            self.logger.error(f"Error calculating latency: {e}")

    def average_latency(self):
        if hasattr(self, 'latencies') and self.latencies:
            return sum(self.latencies) / len(self.latencies)
        return 0.0

    def get_received_messages(self) -> List[Dict[str, Any]]:
        return self.received_messages

    def clear_messages(self):
        self.received_messages = []

    def process_message(self, message):
        pass

def generate_random_subscription(generator: GeneratorPubSub):
    import threading
    event = threading.Event()
    sub = generator.generate_single_sub()
    event.set()
    event.wait()
    return sub

def generate_random_window_description(generator: GeneratorPubSub):
    import threading
    event = threading.Event()
    sub = generator.generate_single_window_sub()
    event.set()
    event.wait()
    return sub

def generate_random_window_subscription(generator: GeneratorPubSub):
    conditions = generate_random_window_description(generator)
    return conditions
