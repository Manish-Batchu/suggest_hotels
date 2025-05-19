from fastapi import FastAPI, HTTPException, Query
from elasticsearch import Elasticsearch

app = FastAPI()
es = Elasticsearch("http://localhost:9200")

es_index_name = "hotels"

@app.get("/get_recommendations")
def get_recommendations_for_email(email: str = Query(..., description="User's email address")):
    query = {
        "query": {
            "match": {
                "email": email
            }
        }
    }

    response = es.search(index=es_index_name, body=query)

    if response['hits']['total']['value'] > 0:
        recommendations = response['hits']['hits'][0]['_source']['recommendations']
        return {
            "email": email,
            "recommendations": recommendations
        }
    else:
        raise HTTPException(status_code=404, detail="No recommendations found for this email")
    


@app.get("/get_similar_hotels")
def h2h(hotelname: str = Query(..., description="hotel's name")):
    query = {"query": {
        "term": {
            "hotel_name.keyword": hotelname
        }
    }
    }

    response = es.search(index="h2h", body=query)

    if response['hits']['total']['value'] > 0:
        similar_hotels = response['hits']['hits'][0]['_source']['similar_hotels']
        return {
            "hotel_name": hotelname,
            "similar_hotels": similar_hotels
        }
    else:
        raise HTTPException(status_code=404, detail="No similar hotels found for this hotel")    
    

@app.get("/related_hotels")
def h2h(hotelname: str = Query(..., description="hotel's name")):
    query = {"query": {
        "term": {
            "hotel_name.keyword": hotelname
        }
    }
    }

    response = es.search(index="related_hotels", body=query)

    if response['hits']['total']['value'] > 0:
        similar_hotels = response['hits']['hits'][0]['_source']['related_hotels']
        return {
            "hotel_name": hotelname,
            "similar_hotels": similar_hotels
        }
    else:
        raise HTTPException(status_code=404, detail="No related hotels found for this hotel")      
