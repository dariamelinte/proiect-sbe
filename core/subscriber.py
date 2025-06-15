import random
import uuid
from typing import Dict, Any, List, Callable
import logging
from datetime import datetime

from .subscription import Subscription
from .utils import log_event

class Subscriber:
    def __init__(self, subscriber_id: str, logger: logging.Logger = None):
        self.subscriber_id = subscriber_id
        self.subscriptions: Dict[str, Subscription] = {}
        self.logger = logger or logging.getLogger('pubsub_system')
        self.received_messages: List[Dict[str, Any]] = []

    def create_simple_subscription(self, conditions: Dict[str, Any]) -> Subscription:
        """Create a simple subscription with given conditions"""
        subscription = Subscription(conditions=conditions)
        self.subscriptions[subscription.id] = subscription
        # Convert conditions to a serializable format
        log_conditions = {
            'type': conditions['type'],
            'threshold': str(conditions['value'].__code__.co_consts[0])  # Extract threshold from lambda
        }
        log_event(self.logger, 'simple_subscription_created', {
            'subscriber_id': self.subscriber_id,
            'subscription_id': subscription.id,
            'conditions': log_conditions
        })
        return subscription

    def create_window_subscription(self, conditions: Dict[str, Any], window_size: int) -> Subscription:
        """Create a window-based subscription with given conditions and window size"""
        subscription = Subscription(conditions=conditions, window_size=window_size)
        self.subscriptions[subscription.id] = subscription
        # Convert conditions to a serializable format
        log_conditions = {
            'type': conditions['type'],
            'threshold': str(conditions['value'].__code__.co_consts[0])  # Extract threshold from lambda
        }
        log_event(self.logger, 'window_subscription_created', {
            'subscriber_id': self.subscriber_id,
            'subscription_id': subscription.id,
            'conditions': log_conditions,
            'window_size': window_size
        })
        return subscription

    def receive_message(self, message: Dict[str, Any]):
        """Receive and store a message"""
        self.received_messages.append(message)
        log_event(self.logger, 'message_received', {
            'subscriber_id': self.subscriber_id,
            'message': message
        })

    def get_received_messages(self) -> List[Dict[str, Any]]:
        """Get all received messages"""
        return self.received_messages

    def clear_messages(self):
        """Clear received messages"""
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