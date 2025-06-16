import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import time
import json
import csv
import threading
from datetime import datetime
from core.publisher import Publisher
from core.broker_network import BrokerNetwork
from core.generator_configs import Configs
from core.subscription import Subscription
from core.subscriber import Subscriber, generate_random_subscription, generate_random_window_subscription
from core.utils import setup_logging

def print_subscriber_messages(subscriber: Subscriber):
    """Print received messages for a subscriber"""
    print(f"\nMessages received by {subscriber.subscriber_id}:")
    for msg in subscriber.received_messages:
        print(json.dumps(msg, indent=2))

def run_experiment(config_path: str, label: str):
    logger = setup_logging()

    print(f"\nRunning experiment with config: {label} ({config_path})")

    configs = Configs(config_path=config_path)
    broker_network = BrokerNetwork(num_brokers=3, window_size=10, logger=logger)
    broker_network.start()

    publisher = Publisher(configs)
    publisher.start()

    subscribers = [Subscriber(f"subscriber_{i}", logger, configs) for i in range(3)]
    for subscriber in subscribers:
        subscriber.start()

    # Create 10,000 simple subscriptions (approx)
    total_subscriptions = 10000
    subs_per_subscriber = total_subscriptions // len(subscribers)

    print(f"Creating ~{subs_per_subscriber} simple subscriptions per subscriber...")

    for subscriber in subscribers:
        for _ in range(subs_per_subscriber):
            sub_data = generate_random_subscription(subscriber.generator)
            subscription = subscriber.create_simple_subscription(sub_data)
            broker_network.add_subscription(subscription)

    print("Subscriptions created, starting publication processing...")

    delivered_messages = 0
    latencies = []

    start_time = time.time()
    run_duration = 180  # 3 minutes = 180 seconds

    try:
        while time.time() - start_time < run_duration:
            if not publisher.publication_queue.empty():
                publication = publisher.get_publication()
                broker_network.publish(publication)
                delivered_messages += 1
            time.sleep(0.01)

    finally:
        # Stop everything cleanly
        for subscriber in subscribers:
            print_subscriber_messages(subscriber)

        for subscriber in subscribers:
            subscriber.stop()
        broker_network.stop()
        publisher.stop()

    avg_latency_ms = sum(latencies) / len(latencies) if latencies else 0

    broker_stats = broker_network.get_all_broker_stats()
    total_matches = sum(b["matches_found"] for b in broker_stats)
    total_attempts = sum(b["matching_attempts"] for b in broker_stats)
    match_rate = (total_matches / total_attempts * 100) if total_attempts > 0 else 0

    # Medie latențe
    avg_latency_ms = 0
    if broker_stats:
        total_latency = sum(b["average_latency_ms"] for b in broker_stats)
        avg_latency_ms = total_latency / len(broker_stats)

    # Save broker stats CSV
    broker_csv_file = f"broker_stats_{label.replace('%','')}.csv"
    with open(broker_csv_file, mode='w', newline='') as csvfile:
        fieldnames = [
            "broker_id", "received_publications", "sent_to_subscribers",
            "matching_attempts", "matches_found", "timestamp", "average_latency_ms"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        timestamp = datetime.now().isoformat()
        for stat in broker_stats:
            stat["timestamp"] = timestamp
            writer.writerow(stat)

    print(f"Broker stats saved to {broker_csv_file}")

    return {
        "config": label,
        "delivered": delivered_messages,
        "avg_latency_ms": avg_latency_ms,
        "match_rate_percent": match_rate,
    }


def write_summary_csv(results, filename="evaluation_summary.csv"):
    fieldnames = ["config_label", "delivered_messages", "avg_latency_ms", "match_rate_percent", "timestamp"]
    with open(filename, mode='w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        timestamp = datetime.now().isoformat()
        for res in results:
            writer.writerow({
                "config_label": res["config"],
                "delivered_messages": res["delivered"],
                "avg_latency_ms": f"{res['avg_latency_ms']:.2f}",
                "match_rate_percent": f"{res['match_rate_percent']:.2f}",
                "timestamp": timestamp
            })
    print(f"Summary results saved to {filename}")


def main():
    configs = [
        ("generator_config_25.json", "25%"),
        ("generator_config_100.json", "100%"),
    ]

    all_results = []

    for config_path, label in configs:
        result = run_experiment(config_path, label)
        print(f"\n=== Results for config {label} ===")
        print(f"Delivered messages: {result['delivered']}")
        print(f"Average latency (ms): {result['avg_latency_ms']:.2f}")
        print(f"Match rate (%): {result['match_rate_percent']:.2f}")
        all_results.append(result)

    write_summary_csv(all_results)


if __name__ == "__main__":
    main()
