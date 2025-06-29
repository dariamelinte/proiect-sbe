import threading
from datetime import datetime
from typing import Dict, Any, List
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
        self.neighbor_brokers: List["Broker"] = []  # legături cu alți brokeri
        self.routing_table: Dict[str, List[Subscription]] = {} # tabel de rutare pentru abonații din brokeri

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

    def administer(self, source_broker_id: str, subscription: Subscription, visited: set = None):
        """
        Administer a subscription by adding it to the routing table and forwarding it to neighbor brokers.
        """
        if visited is None:
            visited = set()
        if self.broker_id in visited:
            # Already visited this broker, avoid loops
            return
        visited.add(self.broker_id)

        # Add the subscription to this broker's routing table
        if source_broker_id not in self.routing_table:
            self.routing_table[source_broker_id] = []
        self.routing_table[source_broker_id].append(subscription)

        for neighbor in self.neighbor_brokers:
            # Avoid sending back to the source broker
            if neighbor.broker_id == source_broker_id:
                continue

            # Check if the subscription is already sent to this neighbor
            already_sent = self.routing_table.get(neighbor.broker_id, [])
            if any(sub.id == subscription.id for sub in already_sent):
                # Subscription already sent to this neighbor, skip forwarding
                continue

            # Check if the subscription is covered by any existing subscription in the neighbor's routing table
            is_covered = any(self.covers(existing, subscription) for existing in already_sent)
            if not is_covered:
                log_event(self.logger, 'subscription_forwarded', {
                    'source_broker_id': self.broker_id,
                    'destination_broker_id': neighbor.broker_id,
                    'subscription_id': subscription.id
                })
                neighbor.administer(self.broker_id, subscription, visited)

    def covers(self, sub1: Subscription, sub2: Subscription) -> bool:
        """Check if subscription sub1 covers subscription sub2"""
        log_event(self.logger, 'checking_subscription_cover', {
            'broker_id': self.broker_id,
            'covering_subscription_id': sub1.id,
            'covered_subscription_id': sub2.id
        })
        conds1 = {field: (op, val) for field, op, val in sub1.conditions}
        conds2 = {field: (op, val) for field, op, val in sub2.conditions}

        for field in conds2:
            if field not in conds1:
                log_event(self.logger, 'subscription_not_covered', {
                    'broker_id': self.broker_id,
                    'covering_subscription_id': sub1.id,
                    'covered_subscription_id': sub2.id,
                    'field': field
                })
                return False

            op1, val1 = conds1[field]
            op2, val2 = conds2[field]

            # Handle window condition prefixes
            if field.startswith(('avg_', 'min_', 'max_')):
                log_event(self.logger, 'checking_window_condition', {
                    'broker_id': self.broker_id,
                    'field': field,
                    'op1': op1, 'val1': val1,
                    'op2': op2, 'val2': val2
                })
                if not self._window_condition_covers(field, op1, val1, op2, val2):
                    log_event(self.logger, 'subscription_not_covered', {
                        'broker_id': self.broker_id,
                        'covering_subscription_id': sub1.id,
                        'covered_subscription_id': sub2.id,
                        'field': field,
                        'op1': op1, 'val1': val1,
                        'op2': op2, 'val2': val2
                    })
                    return False
            else:
                if not self._condition_covers(op1, val1, op2, val2):
                    log_event(self.logger, 'subscription_not_covered', {
                        'broker_id': self.broker_id,
                        'covering_subscription_id': sub1.id,
                        'covered_subscription_id': sub2.id,
                        'field': field,
                        'op1': op1, 'val1': val1,
                        'op2': op2, 'val2': val2
                    })
                    return False

        # If we reach here, sub1 covers sub2
        log_event(self.logger, 'subscription_covered', {
            'broker_id': self.broker_id,
            'covering_subscription_id': sub1.id,
            'covered_subscription_id': sub2.id
        })
        return True

    def _condition_covers(self, op1, val1, op2, val2):
        """Check if condition op1, val1 covers op2, val2"""
        try:
            if op1 == "=": return op2 == "=" and val1 == val2
            if op1 == "!=": return op2 == "!=" and val1 == val2
            if op1 in [">", ">="] and op2 in [">",
                                              ">="]: return val1 <= val2 if op2 == ">" else val1 < val2 if op1 == ">" else val1 <= val2
            if op1 in ["<", "<="] and op2 in ["<",
                                              "<="]: return val1 >= val2 if op2 == "<" else val1 > val2 if op1 == "<" else val1 >= val2
            return False
        except:
            return False

    def _window_condition_covers(self, field, op1, val1, op2, val2):
        """Check if window condition op1, val1 covers op2, val2"""
        try:
            if op1 == "=": return op2 == "=" and val1 == val2
            if op1 == "!=": return op2 == "!=" and val1 == val2
            if op1 in [">", ">="] and op2 in [">",
                                              ">="]: return val1 <= val2 if op2 == ">" else val1 < val2 if op1 == ">" else val1 <= val2
            if op1 in ["<", "<="] and op2 in ["<",
                                              "<="]: return val1 >= val2 if op2 == "<" else val1 > val2 if op1 == "<" else val1 >= val2
            return False
        except:
            return False

    def route_publication(self, publication: Any, visited: set):
        if self.broker_id in visited:
            return
        visited.add(self.broker_id)

        # If publication is a Protobuf message, convert it to a dict
        if isinstance(publication, bytes):
            pub_msg = pb.Publication()
            pub_msg.ParseFromString(publication)
            publication = {
                'station_id': pub_msg.station_id,
                'city': pub_msg.city,
                'direction': pub_msg.direction,
                'temperature': pub_msg.temperature,
                'rain': pub_msg.rain,
                'wind': pub_msg.wind,
                'created_at': pub_msg.created_at,
                'timestamp': pub_msg.timestamp,
            }

        self.process_publication(publication)

        for neighbor in self.neighbor_brokers:
            dest_subs = self.routing_table.get(neighbor.broker_id, [])
            if any(sub.matches(publication) for sub in dest_subs):
                neighbor.route_publication(publication, visited)

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

                subscription.total_checked += 1
                if matched:
                    # Increment match count and notify subscriber
                    subscription.matched += 1
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

    def stop(self):
        """Stop the broker's processing thread"""
        self.is_running = False
        if self.processing_thread:
            self.processing_thread.join()
        log_event(self.logger, 'broker_stopped', {
            'broker_id': self.broker_id
        })

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

    def log_routing_table(self):
        """Log the routing table for this broker"""
        log_event(self.logger, 'routing_table', {
            'broker_id': self.broker_id,
            'routing_table': {
                k: [sub.id for sub in v] for k, v in self.routing_table.items()
            }
        })