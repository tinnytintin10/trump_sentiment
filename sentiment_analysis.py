import json 
import nltk
nltk.download(["names","stopwords","state_union","twitter_samples","movie_reviews","averaged_perceptron_tagger","vader_lexicon","punkt", ])
tweet_file_obj = open("trumptweets.json")

tweets = json.load(tweet_file_obj)





words = "tinsae is at it again"

a = nltk.word_tokenize(words)

print(a)