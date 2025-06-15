import json
import traceback
import os
import random
from typing import Dict, List, Any

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
