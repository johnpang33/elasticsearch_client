import os
import socket
import yaml
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, ConnectionError
from elasticsearch.helpers import bulk
import warnings
from urllib3.exceptions import InsecureRequestWarning
warnings.filterwarnings("ignore", message=".*Connecting to.*TLS.*verify_certs=False.*") 
warnings.filterwarnings("ignore", category=InsecureRequestWarning)  # Suppress InsecureRequestWarnings

class ElasticSearchClient:
    def __init__(self, config_file='config.yaml'):
        # Load configuration from the YAML file
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
        
        # Get the machine's hostname
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
        self.scheme = es_config.get('scheme', 'https')

        # Initialize the Elasticsearch client with basic authentication
        if self.username and self.password:
            self.client = Elasticsearch(
                [{'host': self.host, 'port': self.port, 'scheme': self.scheme}],
                basic_auth=(self.username, self.password),
                verify_certs=False
            )
        else:
            # If no credentials are provided, use without authentication
            self.client = Elasticsearch([{'host': self.host, 'port': self.port, 'scheme': self.scheme}])

    def ping(self):
        # Ping the Elasticsearch server to check if it's up
        try:
            return self.client.ping()
        except ConnectionError:
            print("Unable to connect to Elasticsearch server.")
            return False
        except Exception as e:
            print(f"Error: {e}")
            raise
        
    def create_index(self, index_name, settings=None):
        # Create an index with optional settings and mappings
        if not self.client.indices.exists(index=index_name):
            self.client.indices.create(index=index_name, body=settings)
            print(f"Index {index_name} created.")
        else:
            print(f"Index {index_name} already exists.")

    def delete_index(self, index_name):
        # Delete an index
        try:
            self.client.indices.delete(index=index_name)
            print(f"Index {index_name} deleted.")
        except NotFoundError:
            print(f"Index {index_name} does not exist.")

    def index_document(self, index_name, doc_id=None, document=None):
        ''' Index a document into the specified index.
            - If doc_id is provided, check if the document exists.
            - If it exists, update it; otherwise, index with the given ID.
            - If doc_id is None, index the document without specifying an ID (auto-generate ID).
            
            Args:
                index_name (str): Name of the index.
                doc_id (str, optional): Document ID. Defaults to None.
                document (dict): The document data to index.

            Returns:
                dict: Response from Elasticsearch.
        '''
        try:
            if doc_id:
                # Check if the document exists
                response = self.client.index(index=index_name, id=doc_id, document=document)
                print(f"Document indexed with ID {doc_id}: {response['result']}")
                return {"status": "indexed", "doc_id": doc_id, "response": response}
            else:
                # Index without ID (Elasticsearch auto-generates ID)
                response = self.client.index(index=index_name, document=document)
                auto_generated_id = response["_id"]
                print(f"Document indexed with auto-generated ID {auto_generated_id}: {response['result']}")
                return {"status": "indexed", "doc_id": auto_generated_id, "response": response}
        except Exception as e:
            print(f"Error indexing document: {e}")
            return {"status": "error", "error": str(e)}
         
    def update_document(self, index_name, doc_id, updates):
        """
        Update a document in Elasticsearch.

        Args:
            index_name (str): The name of the index containing the document.
            doc_id (str): The ID of the document to update.
            updates (dict): The fields to update and their new values.

        Returns:
            dict: The response from Elasticsearch.
        """
        try:
            if self.client.exists(index=index_name, id=doc_id):
                # Perform the update
                response = self.client.update(
                    index=index_name,
                    id=doc_id,
                    body={"doc": updates}
                )
                print(f"Document with ID {doc_id} updated successfully.")
                return {"status": "success", "response": response}
            else:
                print(f"Document with ID {doc_id} does not exist.")
                return {"status": "error", "message": f"Document with ID {doc_id} not found."}
        except Exception as e:
            print(f"Error updating document: {e}")
            return {"status": "error", "error": str(e)}
    
    def upsert_document(self, index_name, updates, doc_id=None):
        """
        Update a document in Elasticsearch.

        Args:
            index_name (str): The name of the index containing the document.
            doc_id (str): The ID of the document to update.
            updates (dict): The fields to update and their new values.

        Returns:
            dict: The response from Elasticsearch.
        """
        try:
            if doc_id:
                # Perform the update
                response = self.client.update(
                    index=index_name,
                    id=doc_id,
                    body={
                        "doc": updates,
                        "doc_as_upsert": True
                    }
                )
                print(f"Document with ID {doc_id} updated successfully.")
                return {"status": "success", "response": response}
            else:
                response = self.client.index(index=index_name, body=updates)
                auto_generated_id = response["_id"]
                print(f"Document with ID {auto_generated_id} updated successfully.")
                return {"status": "success", "response": response}
        except Exception as e:
            print(f"Error updating document: {e}")
            return {"status": "error", "error": str(e)}

    def get_document(self, index_name, doc_id):
        # Retrieve a document by ID
        try:
            response = self.client.get(index=index_name, id=doc_id)
            return response['_source']
        except NotFoundError:
            print(f"Document with ID {doc_id} not found.")
            return None

    def search(self, index_name, query):
        # Search for documents based on a query
        response = self.client.search(index=index_name, body=query)
        return response['hits']['hits']

    def update_mapping(self, index_name, new_mappings):
        """
        Update the mappings of an existing index to add new fields.

        Args:
            index_name (str): Name of the index.
            new_mappings (dict): New mappings to be added.

        Returns:
            dict: Response from Elasticsearch.
        """
        try:
            response = self.client.indices.put_mapping(index=index_name, body=new_mappings)
            print(f"Mapping updated for index '{index_name}'.")
            return {"status": "success", "response": response}
        except Exception as e:
            print(f"Error updating mapping: {e}")
            return {"status": "error", "error": str(e)}

    def delete_document_by_id(self, index_name, doc_id):
        """
        Delete a document from an Elasticsearch index by its ID.

        Args:
            index_name (str): The name of the index.
            doc_id (str): The ID of the document to delete.

        Returns:
            dict: The response from Elasticsearch.
        """
        try:
            response = self.client.delete(index=index_name, id=doc_id)
            print(f"Document with ID {doc_id} deleted: {response['result']}")
            return response
        except Exception as e:
            print(f"Error deleting document with ID {doc_id}: {e}")
            return {"error": str(e)}

    def delete_documents_by_query(self, index_name, query):
        """
        Delete documents from an Elasticsearch index based on a query.

        Args:
            index_name (str): The name of the index.
            query (dict): The Elasticsearch query to match documents to delete.

        Returns:
            dict: The response from Elasticsearch.
        """
        try:
            response = self.client.delete_by_query(index=index_name, body=query)
            print(f"Documents matching query deleted: {response['deleted']} documents.")
            return response
        except Exception as e:
            print(f"Error deleting documents by query in index '{index_name}': {e}")
            return {"error": str(e)}    

    def delete_all_documents(self, index_name):
        """
        Delete all documents from an Elasticsearch index.

        Args:
            index_name (str): The name of the index.

        Returns:
            dict: The response from Elasticsearch.
        """
        try:
            query = {"query": {"match_all": {}}}
            response = self.client.delete_by_query(index=index_name, body=query)
            print(f"All documents in index '{index_name}' deleted: {response['deleted']} documents.")
            return response
        except Exception as e:
            print(f"Error deleting all documents in index '{index_name}': {e}")
            return {"error": str(e)}

    def bulk_index(self, index_name, documents):
        """
        Bulk index multiple documents into Elasticsearch.

        Args:
            index_name (str): The name of the Elasticsearch index.
            documents (list[dict]): A list of dictionaries, each representing a document.
                                    Each document should optionally include an '_id' field.

        Returns:
            dict: A summary of the bulk operation results.
        """
        try:
            # Prepare actions for bulk indexing
            actions = []
            for doc in documents:
                # Extract _id if it exists, and exclude it from the document body
                doc_id = doc.pop("_id", None)
                action = {
                    "_index": index_name,
                    "_source": doc
                }
                if doc_id:
                    action["_id"] = doc_id  # Add _id only as a metadata field
                actions.append(action)

            # Execute bulk indexing
            success, failed = bulk(self.client, actions, raise_on_error=False, stats_only=False)
            print(f"Successfully indexed {success} documents.")
            if failed:
                print(f"Failed to index {len(failed)} documents. Errors: {failed}")
            return {"success": success, "failed": failed}
        except Exception as e:
            print(f"Error during bulk indexing: {e}")
            return {"error": str(e)}