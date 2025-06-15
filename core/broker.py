import threading
import time
from typing import Dict, List, Any, Set, Tuple, TYPE_CHECKING
from queue import Queue
from datetime import datetime
import json
import logging

from .subscription import Subscription
from .utils import log_event

class Broker:
    def __init__(self, broker_id: str, window_size: int = 10, logger: logging.Logger = None):
        self.broker_id = broker_id
        self.window_size = window_size
        self.subscriptions: Dict[str, Subscription] = {}
        self.publication_queue = Queue()
        self.is_running = False
        self.processing_thread = None
        self.lock = threading.Lock()
        self.logger = logger or logging.getLogger('pubsub_system')

    def add_subscription(self, subscription: Subscription) -> str:
        """Add a new subscription and return its ID"""
        with self.lock:
            self.subscriptions[subscription.id] = subscription
            log_event(self.logger, 'subscription_added', {
                'broker_id': self.broker_id,
                'subscription_id': subscription.id,
                'conditions': subscription.conditions
            })
            return subscription.id

    def remove_subscription(self, subscription_id: str):
        """Remove a subscription by ID"""
        with self.lock:
            if subscription_id in self.subscriptions:
                del self.subscriptions[subscription_id]
                log_event(self.logger, 'subscription_removed', {
                    'broker_id': self.broker_id,
                    'subscription_id': subscription_id
                })

    def process_publication(self, publication: Dict[str, Any]):
        """Process a publication and notify matching subscribers"""
        with self.lock:
            log_event(self.logger, 'publication_received', {
                'broker_id': self.broker_id,
                'publication': publication
            })
            
            # Process simple subscriptions
            for sub_id, subscription in self.subscriptions.items():
                if subscription.window_size is None:
                    if subscription.matches(publication):
                        log_event(self.logger, 'match_found', {
                            'broker_id': self.broker_id,
                            'subscription_id': sub_id,
                            'publication': publication
                        })
                        self.notify_subscriber(sub_id, publication)
                else:
                    # Add to window buffer
                    subscription.window_buffer.append(publication)
                    log_event(self.logger, 'window_buffer_updated', {
                        'broker_id': self.broker_id,
                        'subscription_id': sub_id,
                        'buffer_size': len(subscription.window_buffer),
                        'window_size': subscription.window_size
                    })
                    
                    # Process window if full
                    if len(subscription.window_buffer) >= subscription.window_size:
                        meta_pub = subscription.process_window()
                        if meta_pub:
                            log_event(self.logger, 'window_processed', {
                                'broker_id': self.broker_id,
                                'subscription_id': sub_id,
                                'meta_publication': meta_pub
                            })
                            self.notify_subscriber(sub_id, meta_pub)
                        # Clear buffer for next window
                        subscription.window_buffer = []

    def notify_subscriber(self, subscription_id: str, publication: Dict[str, Any]):
        """Notify a subscriber about a matching publication"""
        log_event(self.logger, 'subscriber_notified', {
            'broker_id': self.broker_id,
            'subscription_id': subscription_id,
            'publication': publication
        })
        print(f"Broker {self.broker_id} notifying subscription {subscription_id}:")
        print(json.dumps(publication, indent=2))

    def start(self):
        """Start the broker's processing thread"""
        self.is_running = True
        self.processing_thread = threading.Thread(target=self._process_loop)
        self.processing_thread.start()
        log_event(self.logger, 'broker_started', {
            'broker_id': self.broker_id
        })
        print(f"Broker {self.broker_id} started")

    def stop(self):
        """Stop the broker's processing thread"""
        self.is_running = False
        if self.processing_thread:
            self.processing_thread.join()
        log_event(self.logger, 'broker_stopped', {
            'broker_id': self.broker_id
        })
        print(f"Broker {self.broker_id} stopped")

    def _process_loop(self):
        """Main processing loop for publications"""
        while self.is_running:
            try:
                publication = self.publication_queue.get(timeout=1)
                self.process_publication(publication)
            except:
                continue 