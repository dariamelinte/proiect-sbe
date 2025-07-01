import threading
import time
import json
import logging
import redis
from typing import Dict, Any, List

from .broker import Broker
from .subscription import Subscription
from .utils import log_event

class BrokerNetwork:
    def __init__(self, num_brokers: int = 3, window_size: int = 10, logger: logging.Logger = None, redis_client: redis.Redis = None):
        self.num_brokers = num_brokers
        self.broker_ids = [f"broker_{i}" for i in range(num_brokers)]
        self.brokers: Dict[str, Broker] = {}
        self.window_size = window_size
        self.logger = logger or logging.getLogger('pubsub_system')
        self.redis_client = redis_client
        self.is_running = False
        self.health_check_thread = None
        self.current_broker_index = 0  # For round-robin subscription assignment

        log_event(self.logger, 'broker_network_created', {
            'num_brokers': num_brokers,
            'window_size': window_size
        })

    def _health_check_loop(self):
        """Periodically checks the health of brokers and restarts them if they fail."""
        while self.is_running:
            for broker_id in self.broker_ids:
                broker_instance = self.brokers.get(broker_id)
                # If broker doesn't exist or its thread is dead, restart it
                if broker_instance is None or not broker_instance.is_alive():
                    if broker_instance is not None:
                        log_event(self.logger, 'broker_failed', {'broker_id': broker_id})
                        print(f"Broker {broker_id} failed. Restarting...")
                    
                    self._start_broker(broker_id)
            time.sleep(5) # Check every 5 seconds

    def _start_broker(self, broker_id: str):
        """Starts a new broker instance with the given ID."""
        log_event(self.logger, 'broker_starting', {'broker_id': broker_id})
        broker = Broker(
            broker_id=broker_id,
            window_size=self.window_size,
            logger=self.logger,
            redis_client=self.redis_client
        )
        self.brokers[broker_id] = broker
        broker.start()

    def start(self):
        """Start all brokers and the health checker."""
        self.is_running = True
        log_event(self.logger, 'broker_network_starting', {'num_brokers': len(self.brokers)})
        
        # Start initial brokers
        for broker_id in self.broker_ids:
            self._start_broker(broker_id)

        # Start the health checker
        self.health_check_thread = threading.Thread(target=self._health_check_loop)
        self.health_check_thread.start()
        print("BrokerNetwork health checker started.")

    def stop(self):
        """Stop all brokers and the health checker."""
        self.is_running = False
        log_event(self.logger, 'broker_network_stopping', {'num_brokers': len(self.brokers)})
        
        if self.health_check_thread:
            self.health_check_thread.join()

        for broker in self.brokers.values():
            broker.stop()
        
        print("BrokerNetwork stopped.")

    def add_subscription(self, subscription: Subscription) -> str:
        """Add a subscription to a broker using round-robin distribution."""
        # Ensure we distribute across the defined broker IDs, not the current live ones
        broker_id_to_assign = self.broker_ids[self.current_broker_index]
        self.current_broker_index = (self.current_broker_index + 1) % len(self.broker_ids)
        
        broker = self.brokers.get(broker_id_to_assign)
        if broker and broker.is_alive():
            subscription_id = broker.add_subscription(subscription)
            log_event(self.logger, 'subscription_distributed', {
                'broker_id': broker.broker_id,
                'subscription_id': subscription_id
            })
            return subscription_id
        else:
            # If the broker is down, we still log the subscription to Redis for recovery
            log_event(self.logger, 'subscription_added_to_redis_for_dead_broker', {'broker_id': broker_id_to_assign})
            # The subscription logic in the Broker class will handle the Redis part
            # This is a temporary solution until the broker comes back up.
            # A more robust solution might use a temporary queue.
            # For now, we rely on the broker recovering its state.
            
            # We need to manually save the subscription to redis if the broker is down
            sub_id = subscription.id
            sub_key = f"subscriptions:{broker_id_to_assign}"
            # Let's assume a to_dict method on subscription
            sub_data = json.dumps(subscription.to_dict()) 
            self.redis_client.hset(sub_key, sub_id, sub_data)
            return sub_id


    def publish(self, publication: Dict[str, Any]):
        """Broadcast a message to all live brokers and log it to Redis for recovery."""
        pub_id = publication['id']
        pub_key = f"publication:{pub_id}"

        # Expire keys after 1 hour (3600 seconds)
        # Using a pipeline for atomicity
        pipe = self.redis_client.pipeline()
        pipe.set(pub_key, json.dumps(publication), ex=3600)

        # Log this publication as "unprocessed" for ALL brokers
        for broker_id in self.broker_ids:
            unprocessed_key = f"unprocessed_pubs:{broker_id}"
            pipe.sadd(unprocessed_key, pub_id)
            pipe.expire(unprocessed_key, 3600)
        
        pipe.execute()

        log_event(self.logger, 'publication_logged_to_redis', {'publication_id': pub_id})

        # Broadcast to all currently LIVE brokers
        for broker in self.brokers.values():
            if broker.is_alive():
                broker.publication_queue.put(publication)
        
    def get_all_broker_stats(self):
        """Get statistics from all brokers in the network"""
        stats = []
        for broker in self.brokers.values():
            stat = broker.get_stats()
            stats.append(stat)
        return stats
