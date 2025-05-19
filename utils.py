from elasticsearch import Elasticsearch, helpers


def upload_to_elasticsearch(documents, index_name, es_host="http://localhost:9200"):

    es = Elasticsearch(es_host)
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
    

    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
    
    actions = [
        {
            "_op_type": "index",  # Action type (use "create" for new records only)
            "_index": index_name,  # Index name
            "_source": doc         # Document source
        }
        for doc in documents
    ]

    success, failed = helpers.bulk(es, actions)
    
    if success:
        print (f"Successfully uploaded {success} documents to the index '{index_name}'.")
    else:
        print (f"Failed to upload documents. Error: {failed}")   
