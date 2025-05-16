import pandas as pd
import json
import numpy as np
from scipy.sparse import coo_matrix,csr_matrix
from implicit.als import AlternatingLeastSquares
from elasticsearch import Elasticsearch, helpers


df = pd.read_csv("./data/hotel_bookings.csv")


def convert_to_als_format():
    df = pd.read_csv("./data/hotel_bookings.csv")
    #  & (df['brandName'] == 'ginger')
    filtered_df = df[(df['paymentStatus'] == 'CHARGED') & (df['brandName'] == 'ginger')]
    booking_counts_df = filtered_df.groupby(['customerEmail', 'hotelId']).size().reset_index(name='booking_count')
    pivot_df = booking_counts_df.pivot_table(index='customerEmail', columns='hotelId', values='booking_count', aggfunc='sum', fill_value=0)
    pivot_df.reset_index(inplace=True)
    output_path = './data/emails_with_hotel_booking_counts.csv'
    pivot_df.to_csv(output_path, index=False)

convert_to_als_format()


# get similar hotels
df = pd.read_csv('./data/emails_with_hotel_booking_counts.csv')
hotels_df = pd.read_csv('./data/hotel_bookings.csv')
catalog_df = pd.read_csv('./data/catalog.csv')




user_item_matrix = df.set_index('customerEmail').values
user_item_matrix = coo_matrix(user_item_matrix)  # Convert to COO format
user_item_matrix_csr = user_item_matrix.tocsr()  # Convert to CSR format


model = AlternatingLeastSquares(factors=50, regularization=0.5, iterations=100)

model.fit(user_item_matrix_csr)

user_id = 0
recommendations = model.recommend(user_id, user_item_matrix_csr[user_id], N=5)

hotel_indices, scores = recommendations

emails_List = df['customerEmail'].unique().tolist()
hotel_ids_List  = df.columns[1:].tolist()
hotel_dict = dict(zip(hotels_df['hotelId'], hotels_df['hotelName']))
# print(hotel_dict)

def convert_tuple_to_dict(result_tuple):

    result_list = [{"hotelName": hotel_dict.get(hotel_ids_List[int(hotel)]), "score": round(float(score), 3)} for hotel, score in zip(result_tuple[0], result_tuple[1])]

    return result_list

recommendations_list = []

for i in range(len(emails_List)):
    recommendations = model.recommend(i, user_item_matrix_csr[i], N=5)
    recommendations_list.append({
        "email": emails_List[i],
        "recommendations": convert_tuple_to_dict(recommendations)
    })


file_path = "./data/output.json"

# Write the list of dictionaries to a JSON file
with open(file_path, 'w') as json_file:
    json.dump(recommendations_list, json_file, indent=4) 







def upload_to_elasticsearch(documents, index_name, es_host="http://localhost:9200"):

    es = Elasticsearch(es_host)

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


upload_to_elasticsearch(recommendations_list, "final_recommendations2")


def get_recommendations_for_email(index_name,email):
    es = Elasticsearch("http://localhost:9200")
    # Elasticsearch query to match the email
    query = {
        "query": {
            "match": {
                "email": email
            }
        }
    }

    response = es.search(index=index_name, body=query)

    if response['hits']['total']['value'] > 0:
        # Get the recommendations from the first hit (assuming only one email match)
        recommendations = response['hits']['hits'][0]['_source']['recommendations']
        return recommendations
    else:
        return "No recommendations found for this email."



print(get_recommendations_for_email("final_recommendations2","vivek.chadha@valorganics.com"))



  

































