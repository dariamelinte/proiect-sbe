import random
import uuid
from typing import Dict, Any, List, Callable
import logging
from datetime import datetime

from .subscription import Subscription
from .utils import log_event

class Subscriber:
    def __init__(self, name: str, logger: logging.Logger = None):
        self.name = name
        self.logger = logger or logging.getLogger(__name__)
        self.subscriptions: List[Subscription] = []
        self.received_messages: List[Dict[str, Any]] = []

    def add_subscription(self, conditions: Dict[str, Callable]):
        """Add a simple subscription with given conditions."""
        subscription = Subscription(conditions=conditions)
        self.subscriptions.append(subscription)
        self.logger.info(f"Added simple subscription to {self.name}")

    def add_window_subscription(self, field: str, condition: Callable, window_size: int):
        """Add a window-based subscription."""
        conditions = {field: condition}
        subscription = Subscription(conditions=conditions, window_size=window_size)
        self.subscriptions.append(subscription)
        self.logger.info(f"Added window subscription to {self.name}")

    def process_matches(self):
        """Process any matches in window-based subscriptions."""
        for subscription in self.subscriptions:
            if subscription.window_size is not None:
                meta_pub = subscription.process_window()
                if meta_pub:
                    self.receive_message(meta_pub)

    def receive_message(self, message: Dict[str, Any]):
        """Receive and store a message."""
        self.received_messages.append(message)
        self.logger.info(f"Subscriber {self.name} received message: {message}")

    def clear_messages(self):
        """Clear received messages."""
        self.received_messages = []

def generate_random_subscription() -> Dict[str, Any]:
    """Generate a random subscription with random conditions"""
    types = ['temperature', 'humidity', 'pressure']
    selected_type = random.choice(types)
    
    if selected_type == 'temperature':
        threshold = random.uniform(20, 30)
        condition = lambda x: x > threshold
    elif selected_type == 'humidity':
        threshold = random.uniform(30, 70)
        condition = lambda x: x < threshold
    else:  # pressure
        threshold = random.uniform(990, 1010)
        condition = lambda x: x > threshold

    return {
        'type': selected_type,
        'value': condition
    }

def generate_random_window_subscription() -> tuple[Dict[str, Any], int]:
    """Generate a random window-based subscription with random conditions and window size"""
    conditions = generate_random_subscription()
    window_size = random.randint(3, 8)
    return conditions, window_size 