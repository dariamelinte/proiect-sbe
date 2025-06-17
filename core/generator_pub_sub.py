import random
import threading
import time
import json
import traceback
import os

from datetime import datetime, timedelta
from math import ceil

from .generator_configs import Configs
from .utils import create_dir


class GeneratorPubSub:
    def __init__(self, configs: Configs):
        self.configs = configs
        self.operators = ["=", ">", ">=", "<", "<=", "!="]
        self.lock = threading.Lock()
        self.subs_generate = 0
        self.pubs_generate = 0

    def generate_random_value(self, field):
        """Generate a random value based on the field type and constraints"""
        if field['type'] == 'int':
            return random.randint(field['min'], field['max'])

        if field['type'] == 'float':
            return round(random.uniform(field['min'], field['max']), 2)

        if field['type'] == 'string':
            return random.choice(field['choices'])

        if field['type'] == 'date':
            min_date = datetime.strptime(field['min'], field['format'])
            max_date = datetime.strptime(field['max'], field['format'])
            delta = max_date - min_date
            random_days = random.randint(0, delta.days)
            random_date = min_date + timedelta(days=random_days)
            return random_date.strftime(field['format'])

    def generate_pub(self):
        """Generate a single publication with random values for each field in the schema"""
        try:
            publication = {}
            for field in self.configs.schema:
                publication[field['name']] = self.generate_random_value(field)
            return publication
        except Exception as e:
            print(f"[ERROR] - {e}\n\n{traceback.format_exc()}")
            return None

    def generate_pubs(self, nr_pubs, result, index, timing):
        """Generate a specified number of publications in a separate thread"""
        start_time = time.time()
        result[index] = [self.generate_pub() for _ in range(nr_pubs)]
        end_time = time.time()
        timing[f'pubs_thread_{index + 1}'] = {
            'start': start_time,
            'end': end_time,
            'duration': end_time - start_time
        }

    def generate_subs(self, nr_subs, result, index, thread_num, timing):
        """Generate a specified number of subscriptions in a separate thread"""
        start_time = time.time()
        subs = [{} for _ in range(nr_subs)]

        for field, freq in self.configs.freq_fields.items():
            nr_field = int(nr_subs * freq)

            fractional_part = nr_subs * freq - nr_field
            number_threads = int(fractional_part * thread_num)

            if number_threads > 0:
                if index < number_threads:
                    nr_field += 1
            for i in range(nr_field):
                if field in self.configs.freq_equality:
                    nr_equality = int(
                        ceil(nr_field * self.configs.freq_equality[field]))
                    if i < nr_equality:
                        operator = "="
                    else:
                        not_eq_ops = [op for op in self.operators if op != "="]
                        operator_index = i % len(not_eq_ops)
                        operator = not_eq_ops[operator_index]
                else:
                    operator_index = i % len(self.operators)
                    operator = self.operators[operator_index]

                valoare = self.generate_random_value(
                    next((item for item in self.configs.schema if item['name'] == field), None))
                target_idx = min(range(nr_subs), key=lambda j: len(subs[j]))
                subs[target_idx][field] = (operator, valoare)

        result[index] = subs
        end_time = time.time()
        timing[f'subs_thread_{index + 1}'] = {
            'start': start_time,
            'end': end_time,
            'duration': end_time - start_time
        }

    def generate_single_sub(self) -> list[tuple]:
        """Generate a single subscription list of tuples without threading"""
        sub = []
        print("Generating single subscription...")
        for field, freq in self.configs.freq_fields.items():
            if random.random() <= freq:
                # Determine operator
                if field in self.configs.freq_equality:
                    eq_ratio = self.configs.freq_equality[field]
                    operator = "=" if random.random() < eq_ratio else random.choice(
                        [op for op in self.operators if op != "="]
                    )
                else:
                    operator = random.choice(self.operators)
                # Get schema info
                field_schema = next((item for item in self.configs.schema if item['name'] == field), None)
                if field_schema is None:
                    continue
                # Generate value
                value = self.generate_random_value(field_schema)
                # Add to subscription
                sub.append((field, operator, value))
        return sub

    def generate_single_window_sub(self) -> list[tuple]:
        """Generate a single window-based subscription with random processing criteria and dynamic field selection"""
        sub = []
        print("Generating single window subscription...")
        for field, freq in self.configs.freq_fields.items():
            if random.random() <= freq:
                # Determine operator
                if field in self.configs.freq_equality:
                    eq_ratio = self.configs.freq_equality[field]
                    operator = "=" if random.random() < eq_ratio else random.choice(
                        [op for op in self.operators if op != "="]
                    )
                else:
                    operator = random.choice(self.operators)
                # Get schema info
                field_schema = next((item for item in self.configs.schema if item['name'] == field), None)
                if field_schema is None:
                    continue
                value = self.generate_random_value(field_schema)
                sub.append((field, operator, value))

        # Window criteria
        num_fields = random.choice([1, 2])
        selected_fields = random.sample(
            [field for field in self.configs.schema if field['type'] in ['int', 'float']], num_fields
        )
        for field in selected_fields:
            processing_criteria = random.choice(["avg", "max", "min"])  # Randomly select avg, max, or min
            dynamic_field_name = f"{processing_criteria}_{field['name']}"
            operator = random.choice(self.operators)
            # Generate value for dynamic field
            value = self.generate_random_value(field)
            # Add to subscription
            sub.append((dynamic_field_name, operator, value))
        print(f"Generated window subscription with {len(sub)} conditions {sub}")
        return sub

    def generate_dataset(self, thread_num=1):
        """Generate the dataset of publications and subscriptions"""
        start_time = time.time()
        sum_freqs = sum(self.configs.freq_fields.values())

        if sum_freqs < 1.0:
            print(
                f"Atenție: sum frecvențelor ({sum_freqs}) este mai mică decât 1.0.")

        pubs, subs = [], []
        timing = {}

        if thread_num <= 1:
            pubs = [self.generate_pub() for _ in range(self.configs.pubs)]

            result_subs = [None]
            self.generate_subs(self.configs.subs, result_subs, 0, 1, timing)
            subs = result_subs[0]
        else:
            pubs_per_worker = [
                self.configs.pubs // thread_num +
                (1 if i < self.configs.pubs % thread_num else 0)
                for i in range(thread_num)]

            subs_per_worker = [
                self.configs.subs // thread_num +
                (1 if i < self.configs.subs % thread_num else 0)
                for i in range(thread_num)]

            pubs_results = [None] * thread_num
            subs_results = [None] * thread_num

            threads = []

            for i in range(thread_num):
                if pubs_per_worker[i] > 0:
                    thread = threading.Thread(
                        target=self.generate_pubs,
                        args=(pubs_per_worker[i], pubs_results, i, timing)
                    )
                    threads.append(thread)
                    thread.start()

            for i in range(thread_num):
                if subs_per_worker[i] > 0:
                    thread = threading.Thread(
                        target=self.generate_subs,
                        args=(subs_per_worker[i], subs_results, i, thread_num, timing)
                    )
                    threads.append(thread)
                    thread.start()

            for thread in threads:
                thread.join()

            for res in pubs_results:
                if res is not None:
                    pubs.extend(res)

            for res in subs_results:
                if res is not None:
                    subs.extend(res)

        end_time = time.time()

        return pubs, subs, sum_freqs, start_time, end_time, timing

    def dump_data(self, pubs, subs, stats, dump_path):
        """Dump the generated publications, subscriptions, and stats to JSON files"""
        with open(os.path.join(dump_path, "pubs.json"), "w", encoding="utf-8") as f:
            json.dump(pubs, f, indent=2)

        with open(os.path.join(dump_path, "subs.json"), "w", encoding="utf-8") as f:
            json.dump(subs, f, indent=2)

        with open(os.path.join(dump_path, "stats.json"), "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2)

    def generate(self, iteration, thread_num):
        """Generate the dataset for a specific iteration and thread number"""
        pubs, subs, sum_freqs, start_time, end_time, timing = self.generate_dataset(
            thread_num)

        stats = {
            "configs": {
                "thread_num": thread_num,
                "pubs": self.configs.pubs,
                "subs": self.configs.subs,
                "freq_fields": self.configs.freq_fields,
                "freq_eq": self.configs.freq_equality,
                "sum_freqs": sum_freqs
            },
            "time": {
                "start": start_time,
                "end": end_time,
                "duration": end_time - start_time
            },
            "thread_time": timing,
            "sub_stats": {
                "freq_fields": {},
                "freq_eq": {}
            }
        }

        for field in self.configs.fields:
            count = sum(1 for sub in subs if sub and field in sub)

            stats['sub_stats']['freq_fields'][field] = {
                "count": count,
                "percentage": count / len(subs),
                "expected": self.configs.subs * self.configs.freq_fields[field] \
                    if field in self.configs.freq_fields else 0
            }

        for field in self.configs.fields:
            field_count = sum(1 for sub in subs if sub and field in sub)
            if field_count > 0:
                eq_count = sum(
                    1 for sub in subs if sub and field in sub and sub[field][0] == "=")
                stats['sub_stats']['freq_eq'][field] = {
                    "eq_count": eq_count,
                    "field_count": field_count,
                    "percentage": eq_count / field_count
                }
        dump_path = os.path.join(self.configs.results, str(iteration), str(thread_num))
        create_dir(dump_path)
        self.dump_data(pubs, subs, stats, dump_path)
        return pubs, subs, stats
