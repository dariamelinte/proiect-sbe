# Content-Based Publish/Subscribe System

A distributed content-based publish/subscribe system implemented in Python, featuring multiple brokers, window-based subscriptions, and comprehensive logging.

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

### System Flow

```
Publisher -> BrokerNetwork -> Brokers -> Subscriptions
```

1. Publisher generates messages at configurable intervals
2. Messages are distributed to all brokers in the network
3. Each broker processes messages against its subscriptions
4. When matches occur, subscribers are notified

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

### Logging System

- Comprehensive JSON-formatted logging
- Logs stored in `logs` directory with timestamps
- Tracks:
  - Broker network operations
  - Publication processing
  - Subscription management
  - Window operations
  - Match notifications

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
