import os
import socket
import yaml
from elasticsearch import Elasticsearch
import random
import string
from datetime import datetime, timedelta
import warnings
from urllib3.exceptions import InsecureRequestWarning
warnings.filterwarnings("ignore", message=".*Connecting to.*TLS.*verify_certs=False.*") 
warnings.filterwarnings("ignore", category=InsecureRequestWarning)  # Suppress InsecureRequestWarnings

with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

current_host = 'localhost'
es_config = config.get("elasticsearch", {}).get(current_host, {})
es_hostname = es_config.get("hostname")
es_host = es_config.get("host")
es_port = es_config.get("port")
es_username = es_config.get("username")
es_password = es_config.get("password")


# es = Elasticsearch(
#     # hosts=[f"https://{es_host}:{es_port}"],
#     hosts=[es_hostname],
#     basic_auth=(es_username, es_password),  # Use credentials from YAML
#     verify_certs=False
# )

# current_host = socket.gethostname()
# if current_host == "DESKTOP-9B67VE1":
#     current_host = 'localhost'
#     es_config = config.get("elasticsearch1", {}).get(current_host, {})
print(es_config)

# if es.ping():
#     print("Successfully connected to Elasticsearch!")
# else:
#     print("Failed to connect to Elasticsearch.")

# # Example: Retrieve cluster information
# info = es.info()
# print("Elasticsearch Cluster Info:", info)



# # Define the index name
# index_name = "my_index"

# # Create the index with a simple mapping (optional)
# index_settings = {
#     "settings": {
#         "number_of_shards": 1,
#         "number_of_replicas": 0
#     },
#     "mappings": {
#         "properties": {
#             "name": {"type": "text"},
#             "age": {"type": "integer"},
#             "date": {"type": "date"}
#         }
#     }
# }

# # Create the index
# if not es.indices.exists(index=index_name):
#     es.indices.create(index=index_name, body=index_settings)
#     print(f"Index '{index_name}' created successfully!")
# else:
#     print(f"Index '{index_name}' already exists.")

#     if not es.indices.exists(index=index_name):
#         es.indices.create(index=index_name)
#         print(f"Index '{index_name}' created successfully!")


# def random_datetime(start, end):
#     delta = end - start
#     random_days = random.randint(0, delta.days)
#     random_seconds = random.randint(0, 86400)  # Random seconds in the day
#     random_date = start + timedelta(days=random_days, seconds=random_seconds)
#     return random_date

# start_date = datetime(2020, 1, 1)
# end_date = datetime(2025, 1, 1)

# # Generate and insert 62 dummy entries into the index
# for i in range(1, 63):
#     # Dummy data generation
#     doc = {
#         "name": ''.join(random.choices(string.ascii_uppercase, k=5)),  # Random 5 letter name
#         "age": random.randint(18, 70),  # Random age between 18 and 70
#         "date": random_datetime(start_date, end_date).strftime("%Y-%m-%dT%H:%M:%S")  # Random datetime
#     }

#     # Insert the document into Elasticsearch
#     es.index(index=index_name, id=i, body=doc)
#     print(f"Inserted document {i}: {doc}")

# print("62 dummy entries have been inserted successfully!")