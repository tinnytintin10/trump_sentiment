import json
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import twitter_samples
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.tag import pos_tag
from nltk.stem.wordnet import WordNetLemmatizer

nltk.download('wordnet')
stop_words = stopwords.words('english')

nltk.download('vader_lexicon')
sid = SentimentIntensityAnalyzer()

...

import re, string

def remove_noise(tweet_tokens, stop_words = ()):

    cleaned_tokens = []

    for token, tag in pos_tag(tweet_tokens):
        token = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]|'\
                       '(?:%[0-9a-fA-F][0-9a-fA-F]))+','', token)
        token = re.sub("(@[A-Za-z0-9_]+)","", token)

        if tag.startswith("NN"):
            pos = 'n'
        elif tag.startswith('VB'):
            pos = 'v'
        else:
            pos = 'a'

        lemmatizer = WordNetLemmatizer()
        token = lemmatizer.lemmatize(token, pos)

        if len(token) > 0 and token not in string.punctuation and token.lower() not in stop_words:
            cleaned_tokens.append(token.lower())
    return cleaned_tokens

#load api key
f = open("/Users/tin/Documents/secrets/es_creds.json")

es_creds = json.load(f)


es = Elasticsearch(
    cloud_id=es_creds["cloud"]["id"],
    api_key=(es_creds["auth"]["apiKey"]["id"], es_creds["auth"]["apiKey"]["api_key"]),
)

tweet_file_obj = open("data/trumptweets_clean.json")

tweets = json.load(tweet_file_obj)

for key in tweets:
    print("There are {} tweets to ingest".format(len(tweets[key])))
    
    for tweet in tweets[key]:
        
        tweet.pop("", None)
        tweet['date'] = datetime.strptime(tweet['date'], '%Y-%m-%d %H:%M:%S')
        tweet["sentiment"] = sid.polarity_scores(tweet["clean content"])
        tweet["retweets"] = int(tweet["retweets"])
        tweet["favorites"] = int(tweet["favorites"])
        space_tokenizer = RegexpTokenizer("\s+", gaps=True)
        tweet_tokens = []
        tweet_tokens.append(space_tokenizer.tokenize(tweet["clean content"]))
        tweet["tokens"] = tweet_tokens[0]
        tweet["token_count"] = len(tweet["tokens"])
        tweet["tokens_no_noise"] = remove_noise(tweet["tokens"], stop_words)
        tweet["mentions_ns"] = []
        for tk in tweet["tokens"]:
            if(tk[0] == "@"):
                tweet["mentions_ns"].append(tk)
            
        
        if(tweet["sentiment"]["compound"] > 0):
            tweet["sentiment"]["sentiment"] = "Positive"
        else:
            tweet["sentiment"]["sentiment"] = "Negative"
        

# with open("trumptweets_clean_v2.json", 'w', encoding='utf-8') as jsonf:
#     for key in tweets:
    
#         for tweet in tweets[key]:
#             tweet['date'] = str(tweet['date'])
    
    
    
#     jsonf.write(json.dumps(tweets, indent=4))
        
        
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