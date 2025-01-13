import yaml
import socket
from elasticsearch import Elasticsearch
from pathlib import Path

class ESClient(Elasticsearch):
    def __init__(self):
        # Initialize the parent Elasticsearch class
        print("ESClient initialized!")
        # Load configuration from the YAML file
        # CURRENT_DIRECTORY = Path.cwd()
        CURRENT_DIRECTORY = Path(__file__).parent
        config_path = Path(CURRENT_DIRECTORY, 'config.yaml')
        print(config_path)
        # CURRENT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
        # config_path = os.path.join(CURRENT_DIRECTORY, 'config.yaml')
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        current_host = socket.gethostname()

        if current_host == "DESKTOP-9B67VE1":
            current_host = 'localhost'
            es_config = config.get("elasticsearch", {}).get(current_host, {})
        
        # Fallback to localhost if hostname not found in config
        if not es_config:
            print(f"Hostname '{current_host}' not found in config. Using default 'localhost'.")
            es_config = config.get("elasticsearch", {}).get("localhost", {})
        
        self.host = es_config.get("host", "localhost")  # Default to localhost if not found
        self.port = es_config.get("port", 9200)  # Default to 9200 if not found
        self.username = es_config.get("username", "")  # Default to empty if not found
        self.password = es_config.get("password", "")  # Default to empty if not found
        self.scheme = es_config.get('scheme', 'https') # Default to https if not found

        # Initialize the parent Elasticsearch class
        super().__init__(
            [{'host': self.host, 'port': self.port, 'scheme': self.scheme}],
            basic_auth=(self.username, self.password),
            verify_certs=False  # Set to True in production if using SSL/TLS certificates
        )

    def smart_search(self, index_name, query= {"match_all": {}}, scroll="2m", size=10000):
        """
        Perform a search that dynamically switches between simple search and scroll search 
        based on the total number of matching records.

        Args:
            index_name (str): The name of the Elasticsearch index.
            query (dict): The query to execute in Elasticsearch Query DSL format.
            scroll (str): The time Elasticsearch should keep the scroll context alive. Default is "2m".
            size (int): The number of documents to retrieve per scroll batch. Default is 1000.

        Returns:
            list: A list of all retrieved documents.
        """
        try:
            # Get the count of matching documents
            count_response = self.count(index=index_name, body={"query": query})
            total_count = count_response["count"]

            print(f"Total matching documents: {total_count}")

            if total_count <= 10000:
                # Perform a simple search
                response = self.search(index=index_name, body={"query": query, "size": total_count})
                documents = [hit["_source"] for hit in response["hits"]["hits"]]
                print(f"Retrieved {len(documents)} documents using simple search.")
                return {
                    "total_hits": total_count,
                    "documents": documents
                }
            else:
                # Perform a scroll search for large datasets
                print("Using scroll search for large dataset.")
                documents = []
                response = self.search(index=index_name, body={"query": query}, scroll=scroll, size=size)
                scroll_id = response["_scroll_id"]
                hits = response["hits"]["hits"]
                documents.extend([hit["_source"] for hit in hits])

                while hits:
                    response = self.scroll(scroll_id=scroll_id, scroll=scroll)
                    scroll_id = response["_scroll_id"]
                    hits = response["hits"]["hits"]
                    documents.extend([hit["_source"] for hit in hits])

                # Clear the scroll context
                self.client.clear_scroll(scroll_id=scroll_id)
                print(f"Retrieved {len(documents)} documents using scroll search.")
                return {
                    "total_hits": total_count,
                    "documents": documents
                }

        except Exception as e:
            print(f"Error during smart search: {e}")
            return {"error": str(e)}







