import time
import json
from core import Publisher, Configs, BrokerNetwork, Subscription
from core.subscriber import Subscriber, generate_random_subscription, generate_random_window_subscription
from core.utils import setup_logging

def print_subscriber_messages(subscriber: Subscriber):
    """Print received messages for a subscriber"""
    print(f"\nMessages received by {subscriber.subscriber_id}:")
    for msg in subscriber.get_received_messages():
        print(json.dumps(msg, indent=2))

def print_conditions(conditions: dict):
    """Print conditions in a readable format"""
    # Extract the lambda function's code object
    lambda_code = conditions['value'].__code__
    # Get the constants tuple and find the threshold value
    constants = lambda_code.co_consts
    threshold = next((c for c in constants if isinstance(c, (int, float))), None)
    
    if threshold is None:
        return f"{conditions['type']} (threshold not found)"
    
    # Determine the operator from the lambda function's code
    operator = '>' if '>' in str(conditions['value']) else '<'
    return f"{conditions['type']} {operator} {threshold:.2f}"

def main():
    # Setup logging
    logger = setup_logging()
    
    # Initialize configurations
    configs = Configs(config_path='generator_configs.json')
    
    # Create broker network
    broker_network = BrokerNetwork(num_brokers=3, window_size=10, logger=logger)
    broker_network.start()

    # Create publisher with configurations
    publisher = Publisher(configs)

    # Create 3 subscribers
    subscribers = [
        Subscriber(f"subscriber_{i}", logger) for i in range(3)
    ]

    # Create random subscriptions for each subscriber
    for subscriber in subscribers:
        # Create 2 simple subscriptions
        for _ in range(2):
            conditions = generate_random_subscription()
            subscription = subscriber.create_simple_subscription(conditions)
            broker_network.add_subscription(subscription)
            print(f"\nCreated simple subscription for {subscriber.subscriber_id}:")
            print(f"Conditions: {print_conditions(conditions)}")

        # Create 1 window-based subscription
        conditions, window_size = generate_random_window_subscription()
        subscription = subscriber.create_window_subscription(conditions, window_size)
        broker_network.add_subscription(subscription)
        print(f"\nCreated window subscription for {subscriber.subscriber_id}:")
        print(f"Conditions: {print_conditions(conditions)}")
        print(f"Window size: {window_size}")

    try:
        # Generate and publish messages for 30 seconds
        start_time = time.time()
        while time.time() - start_time < 30:
            if not publisher.publication_queue.empty():
                publication = publisher.get_publication()
                broker_network.publish(publication)
                
                # Simulate message delivery to subscribers
                for subscriber in subscribers:
                    for subscription in subscriber.subscriptions.values():
                        if subscription.matches(publication):
                            subscriber.receive_message(publication)
                        elif subscription.window_size is not None:
                            subscription.window_buffer.append(publication)
                            if len(subscription.window_buffer) >= subscription.window_size:
                                meta_pub = subscription.process_window()
                                if meta_pub:
                                    subscriber.receive_message(meta_pub)
                                subscription.window_buffer = []
            
            time.sleep(0.1)

    finally:
        # Print received messages for each subscriber
        for subscriber in subscribers:
            print_subscriber_messages(subscriber)
        
        # Stop the broker network
        broker_network.stop()
        publisher.stop()

if __name__ == "__main__":
    main()
