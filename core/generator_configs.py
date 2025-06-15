import json
import traceback
import os
import random
from typing import Dict, List, Any
from datetime import datetime

from .utils import (
    generate_field_freq,
    generate_operator_freq,
    validate_schema,
    create_dir
)

class Configs:
    def __init__(self, config_path):
        self.config_path: str = config_path

        self.pubs: int = random.randint(1000, 1500)
        self.subs: int = random.randint(1000, 1500)

        self.threads: List[int] = [1]
        self.results = 'results'
        
        self.schema: List[Dict[Any]] = {}
        self.fields: List[str] = []

        self.freq_fields: Dict[Any] = {}
        self.freq_equality: Dict[Any] = {}
        self.min_freq_eq_percentage: float = round(random.uniform(0, 1), 2)

        # System configuration defaults
        self._interval: float = 0.4
        self._window_size: int = 10
        self._num_brokers: int = 3
        self._num_subscribers: int = 3

        self.error = True
        self.get_configs_from_file()

    def get_configs_from_file(self):
        try:
            with open(self.config_path, 'r') as file:
                content = json.load(file)

                for key, value in content.items():
                    if hasattr(self, key):
                        setattr(self, key, value)

                    if key == 'schema':
                        if not validate_schema(self.schema):
                            return

                        self.fields = [item['name'] for item in self.schema]
                        self.error = False
                
                if self.error:
                    return

                if not content.get('freq_fields'):
                    self.freq_fields = generate_field_freq(self.fields)
    
                if not content.get('freq_equality'):
                    self.freq_equality = generate_operator_freq(list(self.freq_fields.keys()), self.min_freq_eq_percentage)

                create_dir(self.results)
        except Exception as e:
            print(f"[ERROR] {e}\n\n{traceback.format_exc()}")
            self.error = True

    @property
    def numeric_fields(self) -> List[str]:
        """Get list of numeric fields (int and float) from schema."""
        return [
            field['name'] for field in self.schema 
            if field['type'] in ['int', 'float']
        ]
        
    @property
    def string_fields(self) -> List[str]:
        """Get list of string fields from schema."""
        return [
            field['name'] for field in self.schema 
            if field['type'] == 'string'
        ]
        
    @property
    def date_fields(self) -> List[str]:
        """Get list of date fields from schema."""
        return [
            field['name'] for field in self.schema 
            if field['type'] == 'date'
        ]
        
    def get_field_range(self, field_name: str) -> Dict[str, Any]:
        """Get the range (min/max) for a numeric field."""
        for field in self.schema:
            if field['name'] == field_name and field['type'] in ['int', 'float']:
                return {
                    'min': field['min'],
                    'max': field['max']
                }
        return None
        
    def get_field_choices(self, field_name: str) -> List[str]:
        """Get the possible choices for a string field."""
        for field in self.schema:
            if field['name'] == field_name and field['type'] == 'string':
                return field['choices']
        return None
        
    def get_date_format(self, field_name: str) -> str:
        """Get the date format for a date field."""
        for field in self.schema:
            if field['name'] == field_name and field['type'] == 'date':
                return field['format']
        return None

    @property
    def publication_interval(self) -> float:
        """Get the publication generation interval."""
        return self._interval
        
    @property
    def subscription_window_size(self) -> int:
        """Get the window size for window-based subscriptions."""
        return self._window_size
        
    @property
    def broker_count(self) -> int:
        """Get the number of brokers in the network."""
        return self._num_brokers
        
    @property
    def subscriber_count(self) -> int:
        """Get the number of subscribers to simulate."""
        return self._num_subscribers
