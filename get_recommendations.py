from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from elasticsearch import Elasticsearch

app = FastAPI()
es = Elasticsearch("http://localhost:9200")

es_index_name = "hotels" 

class EmailRequest(BaseModel):
    email: str

@app.post("/get_recommendations")
def get_recommendations_for_email(req: EmailRequest):
    query = {
        "query": {
            "match": {
                "email": req.email
            }
        }
    }

    response = es.search(index=es_index_name, body=query)

    if response['hits']['total']['value'] > 0:
        recommendations = response['hits']['hits'][0]['_source']['recommendations']
        return {
            "email": req.email,
            "recommendations": recommendations
        }
    else:
        raise HTTPException(status_code=404, detail="No recommendations found for this email")