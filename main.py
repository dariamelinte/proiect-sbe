import time
import json
from core.publisher import Publisher
from core.broker_network import BrokerNetwork
from core.generator_configs import Configs
from core.subscription import Subscription
from core.subscriber import Subscriber, generate_random_subscription, generate_random_window_subscription
from core.utils import setup_logging
import threading

def save_subscriber_messages(subscriber: Subscriber, filename: str):
    """Save received messages for a subscriber to a file"""
    with open(filename, 'w') as file:
        for msg in subscriber.received_messages:
            file.write(json.dumps(msg, indent=2) + '\n')


def print_conditions(field_name: str, condition_func):
    """Print conditions in a readable format"""
    try:
        # Extract the lambda function's code object
        lambda_code = condition_func.__code__
        # Get the constants tuple and find the threshold value
        constants = lambda_code.co_consts
        threshold = None

        # Look for numeric constants (threshold values)
        for const in constants:
            if isinstance(const, (int, float)) and const not in (0, 1):  # Exclude common non-threshold values
                threshold = const
                break

        if threshold is None:
            # Try to get from closure variables if it's a closure
            if condition_func.__closure__:
                for cell in condition_func.__closure__:
                    if isinstance(cell.cell_contents, (int, float)):
                        threshold = cell.cell_contents
                        break

        if threshold is None:
            return f"{field_name} (custom condition)"

        # Try to determine operator by testing the function
        try:
            if condition_func(threshold + 1):
                operator = '>='
            elif condition_func(threshold - 1):
                operator = '<='
            elif condition_func(threshold):
                operator = '=='
            else:
                operator = '!='
        except:
            operator = '?'

        return f"{field_name} {operator} {threshold}"
    except Exception as e:
        return f"{field_name} (condition analysis error: {e})"


def main():
    # Setup logging
    logger = setup_logging()

    # Initialize configurations
    configs = Configs(config_path='generator_configs.json')
    print(f"Configurations loaded: {configs.__dict__}")

    # Create broker network
    broker_network = BrokerNetwork(num_brokers=3, window_size=10, logger=logger)
    broker_network.start()

    # Create publisher with configurations
    publisher = Publisher(configs)
    publisher.start()

    # Create 3 subscribers
    subscribers = [
        Subscriber(f"subscriber_{i}", logger,configs) for i in range(3)
    ]

    for subscriber in subscribers:
        subscriber.start()

    event = threading.Event()
    for subscriber in subscribers:
        for _ in range(2):
            subscription_data = generate_random_subscription(subscriber.generator)
            subscription = subscriber.create_simple_subscription(subscription_data)
            broker_network.add_subscription(subscription)
            print(f"Created subscription data: {subscription_data}")
        subscription_data = generate_random_window_subscription(subscriber.generator)
        subscription = subscriber.create_window_subscription(subscription_data)
        broker_network.add_subscription(subscription)
        print(f"\nCreated window subscription for {subscriber.subscriber_id}:")
    event.set()  # Signal that the operation is complete
    event.wait()  # Wait until the event is set

    try:
        # Generate and publish messages for 30 seconds
        start_time = time.time()
        while time.time() - start_time < 60:
            if not publisher.publication_queue.empty():
                publication = publisher.get_publication()
                broker_network.publish(publication)
            time.sleep(0.01)

    finally:
        # Print received messages for each subscriber
        for subscriber in subscribers:
            save_subscriber_messages(subscriber, f"{subscriber.subscriber_id}_main_messages.json")

        # In finally block:
        for subscriber in subscribers:
            subscriber.stop()

        # Stop the broker network
        broker_network.stop()
        publisher.stop()

if __name__ == "__main__":
    main()
