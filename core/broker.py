import threading
from datetime import datetime
from typing import Dict, Any
from queue import Queue
from .proto import publication_pb2 as pb
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
        self.received_publications = 0
        self.sent_to_subscribers = 0
        self.matching_attempts = 0
        self.matches_found = 0

    def add_subscription(self, subscription: Subscription) -> str:
        """Add a new subscription and return its ID"""
        with self.lock:
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
            log_event(self.logger, 'subscription_added', {
                'broker_id': self.broker_id,
                'subscription_id': subscription.id,
                'conditions': log_conditions
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
        """Process a publication and notify subscribers if conditions match"""
        with self.lock:
            self.received_publications += 1

            log_event(self.logger, 'publication_received', {
                'broker_id': self.broker_id,
                'publication': publication,
            })

            notified_subscribers = set()

            for sub_id, subscription in self.subscriptions.items():
                self.matching_attempts += 1

                matched = False
                if subscription.window_size is None:
                    matched = subscription.matches(publication)
                else:
                    matched = self._process_window_subscription(sub_id, subscription, publication)

                if matched:
                    self.matches_found += 1

                    # Only notify subscriber once, even if multiple subs match
                    if subscription.subscriber_id not in notified_subscribers:
                        subscription.subscriber.receive_message(publication)
                        self.sent_to_subscribers += 1
                        notified_subscribers.add(subscription.subscriber_id)

    def _process_window_subscription(self, sub_id, subscription, publication):
        """Process a window-based subscription"""
        subscription.window_buffer.append(publication)
        log_event(self.logger, 'window_buffer_updated', {
            'broker_id': self.broker_id,
            'subscription_id': sub_id,
            'buffer_size': len(subscription.window_buffer),
            'window_size': subscription.window_size
        })
        if len(subscription.window_buffer) >= subscription.window_size:
            log_event(self.logger, 'window_size_reached', {
                'broker_id': self.broker_id,
                'subscription_id': sub_id,
                'window_size': subscription.window_size
            })
            meta_pub = subscription.process_window()
            if meta_pub:
                self.notify_subscriber(sub_id, meta_pub)
                log_event(self.logger, 'window_subscription_generated', {
                    'broker_id': self.broker_id,
                    'subscription_id': sub_id,
                    'publication': meta_pub
                })
                subscription.window_buffer = []
                return True
            subscription.window_buffer = []
        return False

    def _process_simple_subscription(self, sub_id, subscription, publication):
        """Process a simple subscription"""
        if subscription.matches(publication):
            self.notify_subscriber(sub_id, publication)

    def notify_subscriber(self, subscription_id: str, publication: Dict[str, Any]):
        """Notify the subscriber of a matched publication"""
        subscription = self.subscriptions.get(subscription_id)
        if subscription and subscription.subscriber:
            # Adăugăm un ID unic pentru publicație pentru a evita duplicatele
            publication['unique_id'] = f"{publication['id']}_{self.broker_id}"
            subscription.subscriber.receive_message(publication)
            log_event(self.logger, 'subscriber_notified', {
                'broker_id': self.broker_id,
                'subscription_id': subscription_id,
                'publication': publication,
                'subscriber_id': subscription.subscriber.subscriber_id
            })
        else:
            log_event(self.logger, 'subscriber_notify_failed', {
                'broker_id': self.broker_id,
                'subscription_id': subscription_id
            })

    def start(self):
        """Start the broker's processing thread"""
        self.is_running = True
        self.processing_thread = threading.Thread(target=self._process_loop_proto)
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

    def get_stats(self):
        """Get statistics about the broker's operations"""
        return {
            "broker_id": self.broker_id,
            "received_publications": self.received_publications,
            "sent_to_subscribers": self.sent_to_subscribers,
            "matching_attempts": self.matching_attempts,
            "matches_found": self.matches_found
        }

    def _process_loop_proto(self):
        """Main processing loop for publications using Protobuf serialization"""
        while self.is_running:
            try:
                serialized_pub = self.publication_queue.get(timeout=1)

                # Deserializăm din bytes în mesaj Protobuf
                pub_msg = pb.Publication()
                pub_msg.ParseFromString(serialized_pub)

                # Convertim în dict pentru logare și procesare
                publication_dict = {
                    'station_id': pub_msg.station_id,
                    'city': pub_msg.city,
                    'direction': pub_msg.direction,
                    'temperature': pub_msg.temperature,
                    'rain': pub_msg.rain,
                    'wind': pub_msg.wind,
                    'created_at': pub_msg.created_at,
                    'timestamp': pub_msg.timestamp,
                }

                self.process_publication(publication_dict)
            except Exception:
                continue

    def publish(self, publication: Dict[str, Any]):
        """Publish a message to all brokers to ensure all subscriptions are checked"""
        for broker in self.brokers:
            broker.publication_queue.put(publication)
        log_event(self.logger, 'publication_broadcasted', {
            'publication': publication,
            'num_brokers': len(self.brokers)
        })