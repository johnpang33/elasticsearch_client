from elasticsearch import Elasticsearch
import eland as ed
import pandas as pd
import yaml
import os
from pathlib import Path

import warnings
from urllib3.exceptions import InsecureRequestWarning
warnings.filterwarnings("ignore", message=".*Connecting to.*TLS.*verify_certs=False.*") 
warnings.filterwarnings("ignore", category=InsecureRequestWarning)  # Suppress InsecureRequestWarnings

try:
    # Attempt to get the directory of the current file
    CURRENT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(CURRENT_DIRECTORY, 'config.yaml')
    # CURRENT_DIRECTORY = Path(__file__).resolve().parent
    # config_path = CURRENT_DIRECTORY / 'config.yaml'
    print("Config file path constructed via os.")
except NameError as e:
    # If NameError occurs (likely due to __file__ being undefined), use Path.cwd()
    # For use in jupyter notebook
    print('__file__ is undefined, switching to pathlib:', e)
    CURRENT_DIRECTORY = Path.cwd()
    config_path = CURRENT_DIRECTORY / 'config.yaml'
    print("Config file path constructed via pathlib.")
else:
    # This block is executed only if no exception occurred in the 'try' block
    print("Successfully determined the config file path.")
# Now that we have a valid config_path, let's load the file
try:
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    print("Config file loaded successfully.")
except Exception as e:
    print(f"Error loading config from {config_path}: {e}")
finally:
    print("Exited loading config function.")
    # You can work with the 'config' here if it's successfully loaded


es_config = config.get("elasticsearch", {}).get('localhost', {})
es_client = Elasticsearch([{'host': es_config['host'], 'port': es_config['port'], 'scheme': 'https'}],
              basic_auth=(es_config['username'], es_config['password']),
              verify_certs=False
)
print("Connected to Elasticsearch...")

df = ed.DataFrame(es_client, 'expenses_2023')

print(df.info())