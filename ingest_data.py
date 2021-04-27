import json
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

nltk.download('vader_lexicon')
sid = SentimentIntensityAnalyzer()
  
#load api key
f = open("/Users/tin/Documents/secrets/es_creds.json")

es_creds = json.load(f)


es = Elasticsearch(
    cloud_id=es_creds["cloud"]["id"],
    api_key=(es_creds["auth"]["apiKey"]["id"], es_creds["auth"]["apiKey"]["api_key"]),
)

tweet_file_obj = open("trumptweets.json")

tweets = json.load(tweet_file_obj)

for key in tweets:
    print("There are {} tweets to ingest".format(len(tweets[key])))
    
    for tweet in tweets[key]:
        tweet['date'] = datetime.strptime(tweet['date'], '%Y-%m-%d %H:%M:%S')
        tweet["sentiment"] = sid.polarity_scores(tweet["content"])
        tweet["retweets"] = int(tweet["retweets"])
        tweet["favorites"] = int(tweet["favorites"])
        tweet["tokens"] = nltk.word_tokenize(tweet["content"])
        
     
        if(tweet["sentiment"]["compound"] > 0):
            tweet["sentiment"]["sentiment"] = "Positive"
        else:
            tweet["sentiment"]["sentiment"] = "Negative"
        
        
        
def gendata(tweets):
    for tweet in tweets["tweets"]:
        yield {
            "_index" : "trumptweetsentiment",
            "_source" : tweet
        }
        

def bulk_ingest(tweets_to_ingest):
    try:
        response = helpers.bulk(es, gendata(tweets_to_ingest))
        print("[+] Succesfully ingested {} tweets".format(response[0]))
    except Exception as e:
        print("[-] Error in bulk ingest: {}".format(e))

bulk_ingest(tweets)