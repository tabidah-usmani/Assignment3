from kafka import KafkaConsumer
import json
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
from pymongo import MongoClient

# Download VADER lexicon
nltk.download('vader_lexicon')

bootstrap_servers = ['localhost:9092']
topic = 'recommendation_engine_topic'

products_list = []

consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Initialize sentiment analyzer
sid = SentimentIntensityAnalyzer()

# Function to analyze sentiment of product description
def analyze_sentiment(description):
    scores = sid.polarity_scores(description)
    if scores['compound'] >= 0.03:
        return 'Positive'
    elif scores['compound'] <= 0.02:
        return 'Negative'
    else:
        return 'Neutral'

# MongoDB setup
client = MongoClient('localhost', 27017)
db = client['sentiment_analysis_db']
collection = db['product_sentiments']

# Main loop to consume messages, perform sentiment analysis, and save results to MongoDB
for message in consumer:
    product_info = message.value
    products_list.append(product_info)

    if len(products_list) == 5:
        for data in products_list:
            description = data.get('description', '')
            sentiment = analyze_sentiment(description)
            sentiment_data = {
                'title': data['title'],
                'sentiment': sentiment
            }
            # Save sentiment analysis result to MongoDB
            collection.insert_one(sentiment_data)
            print(f"Sentiment analysis for {data['title']}: {sentiment}")

        products_list = []  # Clear the list after processing
