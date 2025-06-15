import logging
import time
import random
from typing import Dict, Any, List

from core import Configs, BrokerNetwork, Subscriber
from core.publisher import Publisher

def print_subscriber_messages(subscriber: Subscriber):
    """Print messages received by a subscriber."""
    print(f"\nMessages for Subscriber {subscriber.name}:")
    for msg in subscriber.received_messages:
        print(f"  - {msg}")

def print_conditions(subscriber: Subscriber):
    """Print subscription conditions in a readable format."""
    print(f"\nSubscriptions for Subscriber {subscriber.name}:")
    for i, sub in enumerate(subscriber.subscriptions, 1):
        print(f"\nSubscription {i}:")
        for field, condition in sub.conditions.items():
            if callable(condition):
                print(f"  {field}: {condition.__name__}")
            else:
                print(f"  {field}: {condition}")

def main():
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Initialize configurations
    configs = Configs(config_path='generator_configs.json')
    
    # Create broker network
    broker_network = BrokerNetwork(
        num_brokers=configs.broker_count,
        window_size=configs.subscription_window_size,
        logger=logger
    )
    
    # Start the publisher
    publisher = Publisher(configs)
    publisher.start()
    
    # Create subscribers
    subscribers = []
    for i in range(configs.subscriber_count):
        subscriber = Subscriber(f"subscriber_{i}", logger)
        
        # Add some random subscriptions
        for j in range(2):  # Add 2 simple subscriptions
            field = random.choice(configs.fields)
            if field in configs.numeric_fields:
                range_info = configs.get_field_range(field)
                value = random.randint(range_info['min'], range_info['max'])
                subscriber.add_subscription({field: lambda x, v=value: x == v})
            elif field in configs.string_fields:
                choices = configs.get_field_choices(field)
                value = random.choice(choices)
                subscriber.add_subscription({field: lambda x, v=value: x == v})
        
        # Add one window-based subscription
        if configs.numeric_fields:
            field = random.choice(configs.numeric_fields)
            range_info = configs.get_field_range(field)
            value = random.randint(range_info['min'], range_info['max'])
            subscriber.add_window_subscription(
                field,
                lambda x, v=value: x > v,
                configs.subscription_window_size
            )
        
        subscribers.append(subscriber)
        broker_network.add_subscriber(subscriber)
    
    # Print initial subscriptions
    for subscriber in subscribers:
        print_conditions(subscriber)
    
    try:
        # Run for 30 seconds
        start_time = time.time()
        while time.time() - start_time < 30:
            # Get publication from publisher
            if not publisher.publication_queue.empty():
                publication = publisher.get_publication()
                if publication:
                    # Publish to all brokers
                    broker_network.publish(publication)
            
            # Process any matches
            for subscriber in subscribers:
                subscriber.process_matches()
            
            time.sleep(0.1)  # Small delay to prevent CPU overuse
            
    except KeyboardInterrupt:
        print("\nStopping simulation...")
    finally:
        # Cleanup
        publisher.stop()
        broker_network.stop()
        
        # Print final results
        for subscriber in subscribers:
            print_subscriber_messages(subscriber)

if __name__ == "__main__":
    main()
