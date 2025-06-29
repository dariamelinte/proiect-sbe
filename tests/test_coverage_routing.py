import time
from core.broker_network import BrokerNetwork
from core.subscriber import Subscriber
from core.subscription import Subscription
from core.generator_configs import Configs
from core.generator_pub_sub import GeneratorPubSub
from core.publisher import Publisher
from core.utils import setup_logging

def test_multiple_subscription_coverage():
    logger = setup_logging()

    # Load config and generator
    config = Configs("./generator_configs.json", logger=logger)
    generator = GeneratorPubSub(config)

    # Setup broker network and publisher
    network = BrokerNetwork(num_brokers=3, window_size=10, logger=logger)
    network.print_topology()
    network.start()

    publisher = Publisher(config)
    publisher.start()

    # Create subscribers (with controlled subscriptions)
    subscribers = [
        Subscriber(f"subscriber_{i}", logger, config, flag_pass_generation=True)
        for i in range(3)
    ]
    for sub in subscribers:
        sub.start()

    # -- MANUAL SUBSCRIPTIONS --

    # General subscription (will cover specific)
    sub1 = subscribers[0].create_simple_subscription([
        ("temperature", ">", 10),
        ("rain", "<", 90)
    ])
    network.add_subscription(sub1)

    # Specific version (covered by sub1)
    sub2 = subscribers[1].create_simple_subscription([
        ("temperature", ">", 20),
        ("rain", "<", 50)
    ])
    network.add_subscription(sub2)

    # Another general subscription
    sub3 = subscribers[2].create_simple_subscription([
        ("wind", "<", 80)
    ])
    network.add_subscription(sub3)

    # Covered by sub3
    sub4 = subscribers[1].create_simple_subscription([
        ("wind", "<", 60)
    ])
    network.add_subscription(sub4)

    # Uncovered: different field
    sub5 = subscribers[0].create_simple_subscription([
        ("city", "=", "Cluj")
    ])
    network.add_subscription(sub5)

    # Window-based subscription: cannot be covered by any above
    sub6 = subscribers[2].create_window_subscription([
        ("avg_temperature", ">=", 25),
        ("max_rain", "<=", 70)
    ])
    network.add_subscription(sub6)

    # Run system briefly to allow subscriptions to settle
    time.sleep(5)

    # -- PUBLISH A FEW MESSAGES --
    for _ in range(10):
        if not publisher.publication_queue.empty():
            pub = publisher.get_publication()
            network.publish(pub)
        time.sleep(0.3)

    # Dump routing tables
    print("\n Final Routing Tables:")
    for broker in network.brokers:
        print(f"\n🔗 {broker.broker_id}:")
        for src, subs in broker.routing_table.items():
            ids = [s.id for s in subs]
            print(f"  from {src}: {ids}")

    # Shut down cleanly
    for sub in subscribers:
        sub.stop()
    network.stop()
    publisher.stop()

if __name__ == "__main__":
    test_multiple_subscription_coverage()
