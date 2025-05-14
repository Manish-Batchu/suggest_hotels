import pandas as pd
import json
import numpy as np
from scipy.sparse import coo_matrix,csr_matrix
from implicit.als import AlternatingLeastSquares

# df = pd.read_csv("./data/user_bookings.csv")

# first_column = df.columns[0]

# counts = df[first_column].value_counts()

# # Convert to dictionary and then to JSON
# counts_json = counts.to_dict()
# json_output = json.dumps(counts_json, indent=4)

# print(json_output)




# get the column names of the hotel_bookings.csv file
df = pd.read_csv("./data/hotel_bookings.csv")
# column_names = df.columns.tolist()
# for column in column_names:
#     print(column)






# filtered_df = df[df['paymentStatus'] == 'CHARGED']

# # Count the occurrences of each email for each hotel
# final_df = filtered_df.groupby(['customerEmail', 'hotelId', 'hotelName']).size().reset_index(name='booking_count')

# # Save to a CSV file
# output_path = './data/emails_with_booking_count.csv'
# final_df.to_csv(output_path, index=False)











# # Converts to ALS format
# filtered_df = df[df['paymentStatus'] == 'CHARGED']

# # Step 2: Group by customerEmail and hotelId, and count the number of occurrences (bookings)
# booking_counts_df = filtered_df.groupby(['customerEmail', 'hotelId']).size().reset_index(name='booking_count')

# # Step 3: Pivot the table to create a wide-format table where each hotel is a column
# pivot_df = booking_counts_df.pivot_table(index='customerEmail', columns='hotelId', values='booking_count', aggfunc='sum', fill_value=0)

# # Step 4: Reset the index to have customerEmail as a column
# pivot_df.reset_index(inplace=True)

# # Step 5: Save the result to a CSV file
# output_path = './data/emails_with_hotel_booking_counts.csv'
# pivot_df.to_csv(output_path, index=False)




df = pd.read_csv('./data/emails_with_hotel_booking_counts.csv')


user_item_matrix = df.set_index('customerEmail').values
user_item_matrix = coo_matrix(user_item_matrix)  # Convert to COO format
user_item_matrix_csr = user_item_matrix.tocsr()  # Convert to CSR format


# print(user_item_matrix_csr.shape)
# print("Sparsity:", user_item_matrix_csr.nnz / float(user_item_matrix_csr.shape[0] * user_item_matrix_csr.shape[1]))

model = AlternatingLeastSquares(factors=50, regularization=0.1, iterations=100)
model.fit(user_item_matrix_csr)


user_id = 0  # For user1@email (index starts from 0)
recommendations = model.recommend(user_id, user_item_matrix_csr[user_id], N=2)

hotel_indices, scores = recommendations

for hotel_index, score in zip(hotel_indices, scores):
    hotel_name = df.columns[hotel_index + 1]  # Get the hotel name from the column index (hotel columns start from 1)
    print(f"Recommended Hotel: {hotel_name} with a score of {score:.2f}")

































