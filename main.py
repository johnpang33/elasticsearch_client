from datetime import datetime, timedelta
import random
import string
from elastic_client import ElasticSearchClient

def main():
    index_name="my_index"
    es = ElasticSearchClient()

    if es.ping():
        print("Connected to Elasticsearch!")


    # index_settings = {
    #     "settings": {
    #         "number_of_shards":1,
    #         "number_of_replicas": 1
    #     },
    #     "mappings": {
    #         "properties": {
    #             "name": {"type": "text"},
    #             "age": {"type": "integer"},
    #             "date": {"type": "date"}
    #         }
    #     }
    # }

    # es.create_index(index_name=index_name, settings=index_settings)

    def random_datetime(start, end):
        delta = end - start
        random_days = random.randint(0, delta.days)
        random_seconds = random.randint(0, 86400)  # Random seconds in the day
        random_date = start + timedelta(days=random_days, seconds=random_seconds)
        return random_date

    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 1, 1)

    # Generate and insert 62 dummy entries into the index
    # for i in range(1, 63):
    #     # Dummy data generation
    #     doc = {
    #         "name": ''.join(random.choices(string.ascii_uppercase, k=5)),  # Random 5 letter name
    #         "age": random.randint(18, 70),  # Random age between 18 and 70
    #         "date": random_datetime(start_date, end_date).strftime("%Y-%m-%dT%H:%M:%S")  # Random datetime
    #     }

    #     # Insert the document into Elasticsearch
    #     es.index(index=index_name, body=doc)
    #     print(f"Inserted document {doc}")


    # Index some data
    # document = {
    #     'name': 'John Doe',
    #     'age': 30,
    #     'occupation': 'Engineer'
    # }
    # es.index_document(index_name=index_name, document=document)

    # doc_id = 'VBk4NpQB5Z9JS3cDWg4z_'
    # document = {
    #     'name': 'John Doe',
    #     'age': 25,
    #     'occupation': 'Slave Engineer'
    # }

    # es.index_document(index_name=index_name, doc_id=doc_id, document=document)


    # updates = {
    #     'name': 'John Do',
    #     'age': 27,
    #     'occupation': 'Engineer'
    # }
    # es.update_document(index_name=index_name, doc_id=doc_id, updates=updates)


    # updates = {
    #     # 'name': 'Jhvfoe Not Done',
    #     'age': 40,
    #     'occupation': 'Slave',
    # }
    # # es.upsert_document(index_name=index_name, updates=updates)
    # es.upsert_document(index_name=index_name, doc_id=doc_id, updates=updates)


    # new_mappings = {
    #     "properties": {
    #         "category": {"type": "keyword"},
    #         "updated": {"type": "date"}
    #     }
    # }

    # # Update the mapping
    # response = es.update_mapping(index_name="my_index", new_mappings=new_mappings)
    # print("Update Mapping Response:", response)

    # # Get the document
    # print(es.get_document("my_index", 1))

    # # Search for data
    # query = {
    #     "query": {
    #         "match": {
    #             "occupation": "Engineer"
    #         }
    #     }
    # }
    # results = es.search("my_index", query)
    # print("Search results:", results)

    # # Delete the index
    # es.delete_index("my_index")

    # query = {
    #     "query": {
    #         "match": {"title": "Document 3"}  # Delete documents where the title matches
    #     }
    # }
    # es.delete_documents_by_query(index_name=index_name, query=query)

    # es.delete_document_by_id(index_name=index_name, doc_id='bxnBN5QB5Z9JS3cDtA4u')
    documents = [
        {"_id": "1", "title": "Document 1", "content": "This is the first document."},
        {"_id": "2", "title": "Document 2", "content": "This is the second document."},
        {"title": "Document 3", "content": "This document does not have an ID."},
    ]

    # Bulk index the documents
    # result = es.bulk_index(index_name=index_name, documents=documents)
    # print("Bulk Indexing Result:", result)

    result = es.bulk_index(index_name=index_name, documents=documents)
    



if __name__ == "__main__":
    main()
