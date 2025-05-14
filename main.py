import pandas as pd
import json

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






filtered_df = df[df['paymentStatus'] == 'CHARGED']

# Count the occurrences of each email for each hotel
final_df = filtered_df.groupby(['customerEmail', 'hotelId', 'hotelName']).size().reset_index(name='booking_count')

# Save to a CSV file
output_path = './data/emails_with_booking_count.csv'
final_df.to_csv(output_path, index=False)






