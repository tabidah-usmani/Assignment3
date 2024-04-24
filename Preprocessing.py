#!/usr/bin/env python
# coding: utf-8

# # Pre-Processing

# In[1]:


import pandas as pd
get_ipython().system('pip install mlxtend')


# In[5]:


import json

def parse(path):
    with open(path, 'r') as file:
        for line in file:
            yield json.loads(line)

json_file = 'Sampledfile.json'

try:
    for data in parse(json_file):
        print(data)
except FileNotFoundError:
    print("File not found.")
except Exception as e:
    print("An unexpected error occurred:", str(e))


# In[21]:


import json

def parse_json(json_file):
    with open(json_file, 'r') as file:
        for line in file:
            data = json.loads(line)
            # Extract relevant information based on the provided descriptions
            review_info = {
                "reviewerID": data.get("reviewerID", ""),
                "asin": data.get("asin", ""),
                "reviewerName": data.get("reviewerName", ""),
                "vote": data.get("vote", ""),
                "style": data.get("style", {}),
                "reviewText": data.get("reviewText", ""),
                "overall": data.get("overall", ""),
                "summary": data.get("summary", ""),
                "unixReviewTime": data.get("unixReviewTime", ""),
                "reviewTime": data.get("reviewTime", ""),
                "image": data.get("image", "")
            }
            yield review_info

json_file = 'Sampledfile.json'

try:
    for review_data in parse_json(json_file):
        print(review_data)
except FileNotFoundError:
    print("File not found.")
except Exception as e:
    print("An unexpected error occurred:", str(e))


# In[22]:


import pandas as pd
import json
import string
from nltk.tokenize import word_tokenize
import nltk
from mlxtend.preprocessing import TransactionEncoder


# In[23]:


nltk.download('punkt')


# In[24]:


def parse(path):
    with open(path, 'r') as file:
        for line in file:
            yield json.loads(line)


# In[25]:


def clean_data(data):
    # Remove duplicates based on 'asin'
    unique_asin_data = {}
    for product in data:
        asin = product['asin']
        if asin not in unique_asin_data:
            unique_asin_data[asin] = product

    # Convert unique dictionary back to list of products
    cleaned_data = list(unique_asin_data.values())
    return cleaned_data


# In[26]:


def extract_relevant_info(data):
    # Extract relevant fields
    relevant_fields = ['asin', 'title', 'feature', 'description', 'price', 'brand', 'categories']
    extracted_data = [{field: product.get(field) for field in relevant_fields} for product in data]
    return extracted_data


# In[27]:


def handle_missing_values(data):
    # Handle missing values (e.g., remove rows with missing values, impute values, etc.)
    # For simplicity, let's remove rows with missing 'price' values
    cleaned_data = [product for product in data if product.get('price') is not None]
    return cleaned_data


# In[28]:


def convert_data_types(data):
    # Convert data types as needed
    for product in data:
        # Convert 'price' to float
        if 'price' in product:
            try:
                product['price'] = float(product['price'])
            except ValueError:
                # Handle cases where 'price' cannot be converted to float
                product['price'] = None  # or any other appropriate handling strategy

        # Convert other fields to their respective data types as needed

    return data


# In[29]:


def normalize_text(text):
    # Lowercase the text
    text = text.lower()
    # Remove punctuation
    text = text.translate(str.maketrans('', '', string.punctuation))
    # Remove extra whitespaces
    text = ' '.join(text.split())
    return text

def normalize_data(data):
    # Normalize text data
    for product in data:
        product['title'] = normalize_text(product['title'])
        product['feature'] = normalize_text(product['feature'])
        product['description'] = normalize_text(product['description'])
        product['categories'] = [normalize_text(category) for category in product['categories']]

    return data


# In[30]:


def tokenize_text(text):
    # Tokenize the text
    tokens = word_tokenize(text)
    return tokens

def tokenize_data(data):
    # Tokenize text data
    for product in data:
        product['title_tokens'] = tokenize_text(product['title'])
        product['feature_tokens'] = tokenize_text(product['feature'])
        product['description_tokens'] = tokenize_text(product['description'])
        product['category_tokens'] = [tokenize_text(category) for category in product['categories']]

    return data


# In[31]:


# Function to handle categorical data
def handle_categorical_data(data):
    # Extract 'brand' data
    brands = [product['brand'] for product in data]
    
    # Initialize OneHotEncoder
    encoder = OneHotEncoder(sparse=False)
    
    # Fit and transform the 'brand' data
    brand_encoded = encoder.fit_transform(np.array(brands).reshape(-1, 1))

    # Add encoded brand columns to the dataset
    for i, brand in enumerate(encoder.categories_[0]):
        for product, encoded_value in zip(data, brand_encoded[:, i]):
            product[brand] = encoded_value

    return data


# In[32]:


def prepare_data_for_frequent_itemset_mining(data):
    transactions = []
    for product in data:
        # Concatenate title, feature, description, and categories as items in a transaction
        transaction = product['title_tokens'] + product['feature_tokens'] + \
                      product['description_tokens']
        for category in product['category_tokens']:
            transaction += category
        transactions.append(transaction)

    return transactions


# In[40]:


json_file = 'Sampledfile.json'
output_file = 'preprocessed_data.json'

try:
    # Load and clean the data
    data = [d for d in parse(json_file)]
    cleaned_data = clean_data(data)

    # Extract relevant information
    relevant_info = extract_relevant_info(cleaned_data)

    # Handle missing values
    data_without_missing_values = handle_missing_values(relevant_info)

    # Convert data types
    data_with_correct_types = convert_data_types(data_without_missing_values)

   # Normalize data
    normalized_data = normalize_data(data_with_correct_types)

    # Tokenize data
    tokenized_data = tokenize_data(data_with_correct_types)

    # Handle categorical data
    data_with_categorical = handle_categorical_data(tokenized_data)
    
    # Prepare data for frequent itemset mining
    transactions = prepare_data_for_frequent_itemset_mining(data_with_categorical)

    # Convert transactions into a format suitable for MLxtend's Apriori algorithm
    te = TransactionEncoder()
    te_ary = te.fit(transactions).transform(transactions)
    df = pd.DataFrame(te_ary, columns=te.columns_)

    # Print the first few rows of the transformed dataframe
    print(df.head())
    
    with open(output_file, 'w') as outfile:
        json.dump(transactions, outfile)

    print("Preprocessed data saved to:", output_file)
    
except FileNotFoundError:
    print("File not found.")
except Exception as e:
    print("An unexpected error occurred:", str(e))


# In[ ]:




