import json
from elasticsearch import Elasticsearch, helpers

  
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

def gendata(tweets):
    for tweet in tweets["tweets"]:
        yield {
            "_index" : "trumptweets",
            "_source" : tweet
        }
        

def bulk_ingest(tweets):
    try:
        response = helpers.bulk(es, gendata(buckets))
        print("[+] Succesfully ingested {} tweets".format(response[0]))
    except Exception as e:
        print("[-] Error in bulk ingest: {}".format(e))