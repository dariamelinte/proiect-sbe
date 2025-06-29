import sys
import os
from core.generator_pub_sub import GeneratorPubSub
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import time
import json
import csv
from datetime import datetime
from core.publisher import Publisher
from core.broker_network import BrokerNetwork
from core.generator_configs import Configs
from core.subscriber import Subscriber
from core.utils import setup_logging, log_event


def print_subscriber_messages(subscriber: Subscriber, file_path=None):
    """Print received messages for a subscriber"""
    with open(file_path, 'w') as file:
        file.write(f"Messages received by {subscriber.subscriber_id}:\n")
        for msg in subscriber.received_messages:
            file.write(json.dumps(msg, indent=2) + '\n')

def run_experiment(config_path: str, label: str):
    """Run the experiment with the given configuration path and label."""
    logger = setup_logging()
    print(f"\nRunning experiment with config: {label} ({config_path})")
    configs = Configs(config_path=config_path, logger=logger)

    # Initialize GeneratorPubSub
    generator = GeneratorPubSub(configs)
    print(f"Generator initialized with config: {configs.__dict__}")
    _, all_subs, *_ = generator.generate(iteration=0, thread_num=4)  # Only need subs
    rain_count = 0
    rain_eq_count = 0

    for sub in all_subs:
        if 'rain' in sub:
            rain_count += 1
            if sub['rain'][0] == "=":
                rain_eq_count += 1

    if rain_count > 0:
        rain_eq_percentage = rain_eq_count / rain_count
    else:
        rain_eq_percentage = 0
    print(f"Total subscriptions with 'rain': {rain_count}")
    print(f"Total subscriptions with '=' operator on rain: {rain_eq_count}")
    print (f"Percentage of subscriptions with 'rain': {rain_eq_count / rain_count:.2%}")
    time.sleep(20)
    broker_network = BrokerNetwork(num_brokers=3, window_size=10, logger=logger)
    broker_network.start()

    print(f"Percentage of '=' operator on rain: {rain_eq_percentage:.2%}")
    subscribers = [Subscriber(f"subscriber_{i}", logger, configs, True) for i in range(3)]
    for subscriber in subscribers:
        subscriber.start()

    print(f"Creating {len(all_subs)} subscriptions across {len(subscribers)} subscribers...")

    for i, sub_dict in enumerate(all_subs):
        sub_list = [(field, op_val[0], op_val[1]) for field, op_val in sub_dict.items()]
        subscriber = subscribers[i % len(subscribers)]
        subscription = subscriber.create_simple_subscription(sub_list)
        broker_network.add_subscription(subscription)
    delivered_messages = 0
    latencies = []

    publisher = Publisher(configs)
    publisher.start()

    start_time = time.time()
    run_duration = 10  # Run for 30 seconds

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
            print_subscriber_messages(subscriber, f"{subscriber.subscriber_id}_messages.json")

        for subscriber in subscribers:
            subscriber.stop()
        broker_network.stop()
        publisher.stop()

    print(f"\n=== Experiment Results for {label} ===")

    log_event(logger, 'experiment_completed', {
              'config_label': label,
              'delivered_messages': delivered_messages,
              'generated_publications': publisher.generated_publications
              })

    print(f"\nPublications generated: {publisher.generated_publications}")
    pubs = publisher.generated_publications
    for subscriber in subscribers:
        latencies.extend(subscriber.latencies)
    avg_latency_ms = sum(latencies) / len(latencies) if latencies else 0
    broker_stats = broker_network.get_all_broker_stats()
    total_matches = sum(b["matches_found"] for b in broker_stats)
    total_attempts = sum(b["matching_attempts"] for b in broker_stats)
    match_rate = (total_matches / total_attempts * 100) if total_attempts > 0 else 0

    # Save broker stats CSV
    broker_csv_file = f"broker_stats_{label.replace('%','')}.csv"
    with open(broker_csv_file, mode='w', newline='') as csvfile:
        fieldnames = [
            "broker_id", "received_publications","sent_to_subscribers",
            "matching_attempts", "matches_found", "timestamp", "average_latency_ms"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        timestamp = datetime.now().isoformat()
        for stat in broker_stats:
            stat["timestamp"] = timestamp
            stat["average_latency_ms"] = avg_latency_ms
            writer.writerow(stat)

    log_event(logger, 'system_match_rate', {
        'config_label': label,
        'total_matches': total_matches,
        'total_attempts': total_attempts,
        'match_rate_percent': round(match_rate, 2)
    })

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
        time.sleep(60)

    write_summary_csv(all_results)


if __name__ == "__main__":
    main()
