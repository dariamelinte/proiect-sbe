import time
import uuid
from typing import Dict, List, Any, Tuple


class Subscription:
    def __init__(self, conditions, window_size=None, subscriber=None):
        self.conditions = conditions
        self.window_size = window_size
        self.window_buffer = []
        self.id = str(uuid.uuid4())
        self.subscriber = subscriber  # Reference to subscriber

    @property
    def subscriber_id(self):
        return self.subscriber.subscriber_id if self.subscriber else None

    def matches(self, publication) -> bool:
        """Check if a publication matches the subscription conditions"""
        # print(f"Checking publication against subscription {self.id} with conditions: {self.conditions.__repr__()}")
        for field, operator, value in self.conditions:
            if field not in publication:
                return False

            pub_value = publication[field]

            if operator == "=":
                if pub_value != value:
                    return False
            elif operator == ">":
                if not pub_value > value:
                    return False
            elif operator == ">=":
                if not pub_value >= value:
                    return False
            elif operator == "<":
                if not pub_value < value:
                    return False
            elif operator == "<=":
                if not pub_value <= value:
                    return False
            elif operator == "!=":
                if not pub_value != value:
                    return False

        return True

    def process_window(self) -> Dict[str, Any]:
        """Process the window buffer and return a meta-publication if conditions are met"""
        if not self.window_buffer or len(self.window_buffer) < self.window_size:
            return None
        # Example: Calculate average, minimum, and maximum for fields starting with 'avg_', 'min_', or 'max_'
        aggregated_fields = {}
        for field, operator, _ in self.conditions:
            if field.startswith(('avg_', 'min_', 'max_')):
                prefix, base_field = field.split('_', 1)  # Split prefix and base field
                values = [pub[base_field] for pub in self.window_buffer if base_field in pub]
                if values:
                    if prefix == 'avg':
                        aggregated_fields[field] = sum(values) / len(values)
                    elif prefix == 'min':
                        aggregated_fields[field] = min(values)
                    elif prefix == 'max':
                        aggregated_fields[field] = max(values)

        # Check if window conditions are met
        for field, operator, value in self.conditions:
            if field.startswith(('avg_', 'min_', 'max_')):
                if field not in aggregated_fields:
                    return None
                agg_value = aggregated_fields[field]
                if operator == ">" and not agg_value > value:
                    return None
                elif operator == ">=" and not agg_value >= value:
                    return None
                elif operator == "<" and not agg_value < value:
                    return None
                elif operator == "<=" and not agg_value <= value:
                    return None
                elif operator == "=" and not agg_value == value:
                    return None
        # Create a meta-publication with aggregated fields
        meta_publication = {
            'id': f"meta_{self.id}_{int(time.time() * 1000)}",
            'timestamp': int(time.time()),
            'aggregated_fields': aggregated_fields
        }
        return meta_publication
