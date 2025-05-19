import pandas as pd
import json
import numpy as np
from scipy.sparse import coo_matrix,csr_matrix
from implicit.als import AlternatingLeastSquares
from elasticsearch import Elasticsearch, helpers
from utils import upload_to_elasticsearch



df = pd.read_csv("./data/hotel_bookings.csv")


def convert_to_als_format():
    df = pd.read_csv("./data/hotel_bookings.csv")
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


model = AlternatingLeastSquares(factors=10, regularization=0.05, iterations=100)

model.fit(user_item_matrix_csr)

user_id = 0
recommendations = model.recommend(user_id, user_item_matrix_csr[user_id], N=5)

hotel_indices, scores = recommendations

emails_List = df['customerEmail'].unique().tolist()
hotel_ids_List  = df.columns[1:].tolist()
hotel_dict = dict(zip(hotels_df['hotelId'], hotels_df['hotelName']))

recommendations_list = []

for i in range(len(emails_List)):
    recommendations = model.recommend(i, user_item_matrix_csr[i], N=5)
    recommendations_list.append({
        "email": emails_List[i],
        "recommendations":  [{"hotelName": hotel_dict.get(hotel_ids_List[int(hotel)]), "score": float(score)} for hotel, score in zip(recommendations[0], recommendations[1])]
    })


upload_to_elasticsearch(recommendations_list, "recommended_for_you")





  

































