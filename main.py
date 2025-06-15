import time
import json
from core import Publisher, Configs, BrokerNetwork, Subscription
from core.utils import setup_logging

def main():
    # Setup logging
    logger = setup_logging()
    
    # Initialize configurations
    configs = Configs(config_path='generator_configs.json')
    
    # Create broker network with 3 brokers and window size of 10
    broker_network = BrokerNetwork(num_brokers=3, window_size=10, logger=logger)
    broker_network.start()
    
    # Create and start publisher
    publisher = Publisher(configs)
    publisher.start()

    try:
        # Example subscriptions
        # Simple subscription: city=Bucharest, temp>=10, wind<11
        simple_sub = Subscription([
            ("city", "=", "Bucharest"),
            ("temp", ">=", 10),
            ("wind", "<", 11)
        ])
        broker_network.add_subscription(simple_sub)

        # Complex subscription with window-based processing
        # avg_temp>8.5, avg_wind<=13 for city=Bucharest
        complex_sub = Subscription([
            ("city", "=", "Bucharest"),
            ("avg_temp", ">", 8.5),
            ("avg_wind", "<=", 13)
        ], window_size=10)
        broker_network.add_subscription(complex_sub)

        # Process publications for 30 seconds
        start_time = time.time()
        while time.time() - start_time < 30:
            if not publisher.publication_queue.empty():
                publication = publisher.get_publication()
                # Publish to the broker network
                broker_network.publish(publication)
            time.sleep(0.4)
    finally:
        publisher.stop()
        broker_network.stop()

if __name__ == "__main__":
    main()
