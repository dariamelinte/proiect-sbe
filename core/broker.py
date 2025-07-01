import threading
import json
import logging
from queue import Queue, Empty
from typing import Dict, Any
import redis

from .subscription import Subscription
from .utils import log_event

class Broker:
    """
    A Broker processes publications against subscriptions.
    It's designed to be fault-tolerant by recovering its state from Redis.
    """
    def __init__(self, broker_id: str, window_size: int = 10, logger: logging.Logger = None, redis_client: redis.Redis = None):
        self.broker_id = broker_id
        self.window_size = window_size  # Default, can be overridden by subscription
        self.subscriptions: Dict[str, Subscription] = {}
        self.publication_queue = Queue()
        self.is_running = False
        self.processing_thread = None
        self.lock = threading.Lock()
        self.logger = logger or logging.getLogger('pubsub_system')
        self.redis_client = redis_client
        
        # A map to reconstruct subscriber references during recovery
        # This gets populated as subscriptions are added.
        self.subscriber_map: Dict[str, Any] = {}

    def _recover_state(self):
        """Recovers subscriptions and unprocessed messages from Redis."""
        log_event(self.logger, 'broker_recovering_state', {'broker_id': self.broker_id})
        print(f"Broker {self.broker_id} starting recovery...")

        # 1. Recover subscriptions
        sub_key = f"subscriptions:{self.broker_id}"
        stored_subs = self.redis_client.hgetall(sub_key)
        for sub_id, sub_data_json in stored_subs.items():
            sub_data = json.loads(sub_data_json)
            # We need the subscriber map to be populated before we can recover.
            # This is a chicken-and-egg problem if subscribers are not known at startup.
            # In this simulation, subscribers are created first, so we add them as they subscribe.
            subscription = Subscription.from_dict(sub_data, self.subscriber_map)
            self.subscriptions[sub_id] = subscription
            log_event(self.logger, 'recovered_subscription', {'broker_id': self.broker_id, 'subscription_id': sub_id})

        # 2. Recover unprocessed publications
        unprocessed_key = f"unprocessed_pubs:{self.broker_id}"
        pub_ids_to_recover = self.redis_client.smembers(unprocessed_key)
        
        if pub_ids_to_recover:
            pub_keys = [f"publication:{pub_id}" for pub_id in pub_ids_to_recover]
            recovered_pubs_json = self.redis_client.mget(pub_keys)

            recovered_count = 0
            for pub_json in recovered_pubs_json:
                if pub_json:
                    publication = json.loads(pub_json)
                    self.publication_queue.put(publication)
                    recovered_count += 1
            log_event(self.logger, 'recovered_publications', {'broker_id': self.broker_id, 'count': recovered_count})
        
        print(f"Broker {self.broker_id} recovery complete. Found {len(self.subscriptions)} subscriptions and {len(pub_ids_to_recover)} unprocessed publications.")

        # Dump recovered subscriptions
        if self.subscriptions:
            print(f"Recovered subscriptions for {self.broker_id}:")
            for sub_id, sub in self.subscriptions.items():
                try:
                    sub_info = sub.to_dict() if hasattr(sub, 'to_dict') else str(sub)
                except Exception as e:
                    sub_info = f"(error serializing: {e})"
                print(f"  - ID: {sub_id}, Info: {sub_info}")
        else:
            print(f"No subscriptions recovered for {self.broker_id}.")

    def add_subscription(self, subscription: Subscription) -> str:
        """Add a new subscription locally and save it to Redis."""
        with self.lock:
            sub_id = subscription.id
            self.subscriptions[sub_id] = subscription
            if subscription.subscriber:
                self.subscriber_map[subscription.subscriber_id] = subscription.subscriber

            sub_key = f"subscriptions:{self.broker_id}"
            sub_data = json.dumps(subscription.to_dict())
            self.redis_client.hset(sub_key, sub_id, sub_data)
            self.redis_client.expire(sub_key, 3600) # Expire after 1h

            log_event(self.logger, 'subscription_added', {'broker_id': self.broker_id, 'subscription_id': sub_id})
            return sub_id

    def remove_subscription(self, subscription_id: str):
        """Remove a subscription locally and from Redis."""
        with self.lock:
            if subscription_id in self.subscriptions:
                del self.subscriptions[subscription_id]
                
                sub_key = f"subscriptions:{self.broker_id}"
                self.redis_client.hdel(sub_key, subscription_id)
                
                window_buffer_key = f"window_buffer:{subscription_id}"
                self.redis_client.delete(window_buffer_key)
                
                log_event(self.logger, 'subscription_removed', {'broker_id': self.broker_id, 'subscription_id': subscription_id})

    def process_publication(self, publication: Dict[str, Any]):
        """Process a publication against all subscriptions and then mark it as processed in Redis."""
        pub_id = publication.get('id')
        if not pub_id:
            log_event(self.logger, 'publication_missing_id', {'broker_id': self.broker_id})
            return

        with self.lock:
            notified_subscribers = set()
            for sub_id, subscription in self.subscriptions.items():
                if subscription.window_size is None: # Simple subscription
                    if subscription.matches(publication):
                        if subscription.subscriber and subscription.subscriber_id not in notified_subscribers:
                            subscription.subscriber.receive_message(publication)
                            notified_subscribers.add(subscription.subscriber_id)
                else: # Window-based subscription
                    self._process_window_subscription(subscription, publication)

        unprocessed_key = f"unprocessed_pubs:{self.broker_id}"
        self.redis_client.srem(unprocessed_key, pub_id)
        log_event(self.logger, 'publication_processed', {'broker_id': self.broker_id, 'publication_id': pub_id})

    def _process_window_subscription(self, subscription: Subscription, publication: Dict[str, Any]):
        """Process a window-based subscription using Redis for state."""
        buffer_key = f"window_buffer:{subscription.id}"
        
        self.redis_client.rpush(buffer_key, json.dumps(publication))
        self.redis_client.expire(buffer_key, 3600)

        if self.redis_client.llen(buffer_key) >= subscription.window_size:
            window_data_json = self.redis_client.lrange(buffer_key, 0, -1)
            subscription.window_buffer = [json.loads(p) for p in window_data_json]
            
            meta_pub = subscription.process_window()
            if meta_pub and subscription.subscriber:
                subscription.subscriber.receive_message(meta_pub)
                log_event(self.logger, 'window_match', {'broker_id': self.broker_id, 'sub_id': subscription.id})

            self.redis_client.delete(buffer_key) # Tumbling window

    def start(self):
        """Start the broker's processing thread after recovering state."""
        self._recover_state()
        self.is_running = True
        self.processing_thread = threading.Thread(target=self._process_loop)
        self.processing_thread.start()
        log_event(self.logger, 'broker_started', {'broker_id': self.broker_id})

    def stop(self):
        """Stop the broker's processing thread."""
        self.is_running = False
        if self.processing_thread:
            self.processing_thread.join()
        log_event(self.logger, 'broker_stopped', {'broker_id': self.broker_id})

    def is_alive(self):
        """Check if the processing thread is alive."""
        return self.processing_thread and self.processing_thread.is_alive()

    def _process_loop(self):
        """Main processing loop for publications from the internal queue."""
        while self.is_running:
            try:
                publication = self.publication_queue.get(timeout=1)

                if publication.get('__crash__'):
                    raise Exception("Simulating a broker crash via poison pill")

                self.process_publication(publication)
            except Empty:
                # This is normal, just means the queue was empty for the timeout period.
                continue
            except Exception as e:
                # Any other exception is considered a crash of the processing loop.
                log_event(self.logger, 'broker_process_loop_crash', {'broker_id': self.broker_id, 'error': str(e)})
                # The loop will terminate, and the health checker should restart it.
                break

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