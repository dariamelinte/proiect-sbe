# Content-Based Publish/Subscribe System

A distributed content-based publish/subscribe system implemented in Python, featuring multiple brokers, window-based subscriptions, and comprehensive logging.

## Project Progress Report

### Completed Tasks (25/35 points)

1. ✅ Publisher Node (5 points)
   - Implemented random publication generation
   - Supports multiple message types (temperature, humidity, pressure)
   - Configurable generation intervals
   - Timestamp addition to publications

2. ✅ Broker Network (10 points)
   - Implemented 3-broker overlay network
   - Content-based filtering
   - Window-based publication processing
   - Round-robin subscription distribution
   - Thread-safe operations

3. ✅ Subscriber Nodes (5 points)
   - Implemented 3 subscriber nodes
   - Support for both simple and window-based subscriptions
   - Random subscription generation
   - Message history tracking
   - Comprehensive logging

4. ✅ Binary Serialization (5 points)
   - Implement Google Protocol Buffers or Thrift
   - Define message schemas
   - Modify publisher-broker communication
   - Update broker-subscriber communication

5. ✅ System Evaluation (10 points)
   - Implement performance measurement tools
   - Run 10,000 simple subscription test
   - Measure delivery statistics:
     - Successful deliveries in 3-minute window
     - Average delivery latency
     - Matching rate comparison (100% vs 25% equality)
   - Generate evaluation report
      - [PDF Report](/docs/Raport.pdf)
      - [HTML Report](https://dariamelinte.github.io/proiect-sbe/pubsub_performance_report.html)

## System Architecture

### Core Components

1. **Publisher**
   - Generates random publications (messages) with different types and values
   - Configurable message generation interval
   - Supports multiple message types: temperature, humidity, pressure

2. **BrokerNetwork**
   - Manages multiple brokers (default: 3)
   - Distributes publications across brokers
   - Handles subscription distribution using round-robin strategy

3. **Broker**
   - Processes publications against subscriptions
   - Manages subscription lifecycle
   - Handles window-based processing
   - Thread-safe operations

4. **Subscription**
   - Defines matching conditions for publications
   - Supports both simple and window-based subscriptions
   - Configurable window sizes for aggregation

5. **Subscriber**
   - Manages multiple subscriptions (simple and window-based)
   - Receives and stores matching messages
   - Supports random subscription generation
   - Tracks message history

### System Flow

```
Publisher -> BrokerNetwork -> Brokers -> Subscriptions -> Subscribers
```

1. Publisher generates messages at configurable intervals
2. Messages are distributed to all brokers in the network
3. Each broker processes messages against its subscriptions
4. When matches occur, subscribers are notified
5. Subscribers store received messages for analysis

## Features

### Message Types
- Temperature: Random values between 15 and 35
- Humidity: Random values between 20 and 80
- Pressure: Random values between 980 and 1020

### Subscription Types

1. **Simple Subscriptions**
   ```python
   subscription = Subscription(
       conditions={
           "type": "temperature",
           "value": lambda x: x > 25
       }
   )
   ```

2. **Window-Based Subscriptions**
   ```python
   subscription = Subscription(
       conditions={
           "type": "humidity",
           "value": lambda x: x < 30
       },
       window_size=5
   )
   ```

### Subscriber Simulation

The system includes a simulation of 3 subscriber nodes, each with:
- 2 simple subscriptions with random conditions
- 1 window-based subscription with random window size
- Message history tracking
- Comprehensive logging

Example subscriber setup:
```python
# Create subscribers
subscribers = [Subscriber(f"subscriber_{i}") for i in range(3)]

# Create random subscriptions
for subscriber in subscribers:
    # Simple subscriptions
    conditions = generate_random_subscription()
    subscription = subscriber.create_simple_subscription(conditions)
    
    # Window-based subscription
    conditions, window_size = generate_random_window_subscription()
    subscription = subscriber.create_window_subscription(conditions, window_size)
```

### Logging System

- Comprehensive JSON-formatted logging
- Logs stored in `logs` directory with timestamps
- Tracks:
  - Broker network operations
  - Publication processing
  - Subscription management
  - Window operations
  - Match notifications
  - Subscriber activities

Example log entry:
```json
{
    "timestamp": "2024-03-21T14:30:22.123456",
    "type": "publication_received",
    "data": {
        "broker_id": "broker_0",
        "publication": {
            "type": "temperature",
            "value": 26.5,
            "timestamp": "2024-03-21T14:30:22.123456"
        }
    }
}
```

## Configuration

### Default Settings
- Number of brokers: 3
- Number of subscribers: 3
- Window size: 10
- Message generation interval: 0.4 seconds
- Logging: Enabled

### Error Handling
- Graceful shutdown of publishers and brokers
- Thread-safe operations using locks
- Exception handling in processing loops

## Project Structure

```
proiect-sbe/
├── core/
│   ├── __init__.py
│   ├── broker.py
│   ├── broker_network.py
│   ├── publisher.py
│   ├── subscription.py
│   ├── subscriber.py
│   └── utils.py
├── logs/
│   └── pubsub_*.log
├── main.py
└── README.md
```

## Running the System

1. Ensure Python 3.x is installed
2. Run the main script:
   ```bash
   python main.py
   ```

The simulation will:
1. Create 3 subscribers
2. Generate random subscriptions for each subscriber
3. Run for 30 seconds, publishing messages
4. Display received messages for each subscriber
5. Generate comprehensive logs

## Design Principles

The system is designed to be:
- **Scalable**: Multiple brokers can handle different subscriptions
- **Extensible**: New subscription types and conditions can be added
- **Observable**: Comprehensive logging for monitoring and debugging
- **Reliable**: Thread-safe operations and proper resource management

## Future Improvements

1. Add support for more message types
2. Implement persistent storage for publications
3. Add network communication between brokers
4. Implement load balancing strategies
5. Add monitoring and metrics collection
6. Support for dynamic broker addition/removal
7. Add more complex subscription patterns
8. Implement subscriber authentication and authorization
