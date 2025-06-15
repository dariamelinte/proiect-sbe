import time
from typing import Dict, List, Any, Tuple

class Subscription:
    def __init__(self, conditions: List[Tuple[str, str, Any]], window_size: int = None):
        self.conditions = conditions  # List of (field, operator, value) tuples
        self.window_size = window_size  # None for simple subscriptions, int for window-based
        self.window_buffer = [] if window_size else None
        self.id = f"sub_{int(time.time() * 1000)}"

    def matches(self, publication: Dict[str, Any]) -> bool:
        """Check if a publication matches the subscription conditions"""
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

        # Example: Calculate average for fields starting with 'avg_'
        avg_fields = {}
        for field, operator, _ in self.conditions:
            if field.startswith('avg_'):
                base_field = field[4:]  # Remove 'avg_' prefix
                values = [pub[base_field] for pub in self.window_buffer if base_field in pub]
                if values:
                    avg_fields[field] = sum(values) / len(values)

        # Check if window conditions are met
        for field, operator, value in self.conditions:
            if field.startswith('avg_'):
                if field not in avg_fields:
                    return None
                
                avg_value = avg_fields[field]
                if operator == ">" and not avg_value > value:
                    return None
                elif operator == ">=" and not avg_value >= value:
                    return None
                elif operator == "<" and not avg_value < value:
                    return None
                elif operator == "<=" and not avg_value <= value:
                    return None

        # Create meta-publication
        meta_pub = {}
        for field, operator, value in self.conditions:
            if not field.startswith('avg_'):
                meta_pub[field] = value

        meta_pub['conditions'] = True
        return meta_pub 