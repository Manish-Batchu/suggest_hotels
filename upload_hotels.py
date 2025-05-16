import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import json
import pandas as pd
import json
from elasticsearch import Elasticsearch, helpers

# Load data
df = pd.read_csv('./data/catalog.csv', encoding='ISO-8859-1')

# Specify the attributes you want to combine
attributes_to_combine = ['highlights', 'hotelstate', 'region']

# Check that all specified attributes exist in the DataFrame
missing_attrs = [attr for attr in attributes_to_combine if attr not in df.columns]
if missing_attrs:
    raise ValueError(f"Missing attributes in the DataFrame: {missing_attrs}")

# Combine specified attributes
def combine_attributes(row):
    combined_text = []
    for attr in attributes_to_combine:
        value = row[attr]
        combined_text.append(value if pd.notna(value) else '')
    return ' '.join(combined_text)

df['combined'] = df.apply(combine_attributes, axis=1)

# TF-IDF vectorization
vectorizer = TfidfVectorizer(stop_words='english')
tfidf_matrix = vectorizer.fit_transform(df['combined'])

# Use dot product similarity (not cosine normalized)
similarity_matrix = tfidf_matrix @ tfidf_matrix.T  # Matrix multiplication gives dot product similarity

# Ensure hotel names are unique
df['hotel_name_unique'] = df['hotel_name'] + ' - ' + df.index.astype(str)
hotel_names = df['hotel_name_unique'].values

# Create similarity matrix DataFrame
similarity_df = pd.DataFrame(similarity_matrix.toarray(), index=hotel_names, columns=hotel_names)

# Choose target hotel (match with unique names)
target_hotel_name = "Ginger Thane"
target_row = df[df['hotel_name'] == target_hotel_name]

if target_row.empty:
    print(f"Hotel '{target_hotel_name}' not found in dataset.")
else:
    target_unique_name = target_row['hotel_name_unique'].values[0]
    
    # Extract and filter similar hotels
    similarities = similarity_df[target_unique_name].copy()
    filtered_similarities = similarities[similarities >= 0.4].sort_values(ascending=False)

    # Convert to DataFrame
result_df = filtered_similarities.reset_index()
result_df.columns = ['hotel_name_unique', 'similarity_score']
result_df['hotel_name'] = result_df['hotel_name_unique'].apply(lambda x: x.split(' - ')[0])

# Drop duplicates by hotel_name and similarity_score
result_df = result_df[['hotel_name', 'similarity_score']].drop_duplicates()

# Display results
# print(f"Hotels similar to '{target_hotel_name}' with similarity >= 0.7:")
print(result_df)

hotel_to_hotel=[]
hotels = df['hotel_name'].unique().tolist()

for hotel in hotels:
    hotel_to_hotel.append({
        "hotel_name": hotel,
        "similar_hotels":  result_df.to_dict(orient="records")
    })


file_path = "./data/hotel_to_hotel.json"

# Write the list of dictionaries to a JSON file
with open(file_path, 'w') as json_file:
    json.dump(hotel_to_hotel, json_file, indent=4) 




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

upload_to_elasticsearch(hotel_to_hotel, "h2h")         