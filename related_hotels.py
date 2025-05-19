import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from utils import upload_to_elasticsearch

df = pd.read_csv('./data/catalog.csv', encoding='ISO-8859-1')

# Specify the attributes you want to combine
attributes_to_combine = ['hotelstate','hotelcity']

missing_attrs = [attr for attr in attributes_to_combine if attr not in df.columns]
if missing_attrs:
    raise ValueError(f"Missing attributes in the DataFrame: {missing_attrs}")

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

similarity_matrix = tfidf_matrix @ tfidf_matrix.T  

df['hotel_name_unique'] = df['hotel_name'] + ' - ' + df.index.astype(str)
hotel_names = df['hotel_name_unique'].values

# Create similarity matrix DataFrame
similarity_df = pd.DataFrame(similarity_matrix.toarray(), index=hotel_names, columns=hotel_names)

def get_related_hotels(target_hotel_name:str):
    target_row = df[df['hotel_name'] == target_hotel_name]

    if target_row.empty:
        print(f"Hotel '{target_hotel_name}' not found in dataset.")
    else:
        target_unique_name = target_row['hotel_name_unique'].values[0]
    
    similarities = similarity_df[target_unique_name].copy()
    filtered_similarities = similarities[similarities>0.2].sort_values(ascending=False)

    result_df = filtered_similarities.reset_index()
    result_df.columns = ['hotel_name_unique', 'similarity_score']
    result_df['hotel_name'] = result_df['hotel_name_unique'].apply(lambda x: x.split(' - ')[0])
    result_df = result_df[['hotel_name', 'similarity_score']].drop_duplicates()
    return result_df



# Looping through all hotel names and adding to a list to store in an ES index.
related_hotels=[]
hotels = df['hotel_name'].unique().tolist()

for hotel in hotels:
    related = get_related_hotels(hotel)
    filtered_related = related[related["hotel_name"] != hotel] 
    related_hotels.append({
        "hotel_name": hotel,
        "related_hotels": filtered_related.to_dict(orient="records")
    })
  


upload_to_elasticsearch(related_hotels,"related_hotels")



