import logging
from typing import Dict, Any, List, Optional
from queue import Queue

from .proto_utils import parse_broker_message

class Subscription:
    def __init__(self, conditions: Dict[str, Any], window_size: Optional[int] = None):
        self.conditions = conditions
        self.window_size = window_size
        self.window_buffer: List[Dict[str, Any]] = []
        self.notification_queue = Queue()
        self.logger = logging.getLogger(__name__)
        
    def matches(self, publication: Dict[str, Any]) -> bool:
        """Check if a publication matches the subscription conditions."""
        try:
            if self.window_size is None:
                # Simple subscription matching
                for field, condition in self.conditions.items():
                    if field in publication and not condition(publication[field]):
                        return False
                return True
            else:
                # Window-based subscription
                self.window_buffer.append(publication)
                if len(self.window_buffer) >= self.window_size:
                    return self.process_window() is not None
                return False
        except Exception as e:
            self.logger.error(f"Error matching publication: {str(e)}")
            return False
            
    def process_window(self) -> Optional[Dict[str, Any]]:
        """Process a window of publications and return a meta-publication if conditions are met."""
        try:
            if not self.window_buffer or len(self.window_buffer) < self.window_size:
                return None
                
            # Get the field we're monitoring
            field = next(iter(self.conditions.keys()))
            
            # Calculate average value for the window
            values = [pub[field] for pub in self.window_buffer if field in pub]
            if not values:
                return None
                
            avg_value = sum(values) / len(values)
            
            # Check if average matches condition
            if self.conditions[field](avg_value):
                # Create meta-publication
                meta_pub = {
                    'type': field,
                    'value': avg_value,
                    'timestamp': self.window_buffer[-1]['timestamp'],
                    'window_size': self.window_size
                }
                
                # Clear buffer for next window
                self.window_buffer = []
                return meta_pub
                
            # Clear buffer for next window
            self.window_buffer = []
            return None
            
        except Exception as e:
            self.logger.error(f"Error processing window: {str(e)}")
            self.window_buffer = []
            return None
            
    def notify(self, serialized_notification: bytes):
        """Add a notification to the queue."""
        self.notification_queue.put(serialized_notification)
        
    def get_notification(self) -> Optional[Dict[str, Any]]:
        """Get the next notification from the queue."""
        try:
            serialized = self.notification_queue.get_nowait()
            _, content = parse_broker_message(serialized)
            return content
        except:
            return None 