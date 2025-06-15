import time
import json
import threading
from typing import List, Dict, Any
from queue import Queue
from datetime import datetime

from publisher import Publisher
from publisher.generator_configs import Configs


def main():
    # Initialize configurations
    configs = Configs(config_path='generator_configs.json')
    
    # Create and start publisher
    publisher = Publisher(configs)
    publisher.start()

    try:
        while True:
            if not publisher.publication_queue.empty():
                publication = publisher.get_publication()
                print(f"Received publication: {json.dumps(publication, indent=2)}")
            time.sleep(1)
    finally:
        publisher.stop()

if __name__ == "__main__":
    main()
