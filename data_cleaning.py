import re, os
import pandas as pd
import numpy as np
from collections import Counter
from operator import itemgetter

word_pattern = re.compile("\w[\w\-\'’]*\w|\w")

tweets_df = pd.read_csv("trumptweets.csv")

dev_count = 0

def is_photo_tweet(lowered_tweet):
    url = "pic.twitter.com"
    photo_tweet = 0
    if len(lowered_tweet) >= 5 and url[0:4] == lowered_tweet[0:4]:
        photo_tweet = 1
    return photo_tweet

def analyze_link(link):

    embeded_tweet = False
    embeded_tweet_author = ""

    twitter_url2 = "https://twitter.com/"
    if link[0:20] == twitter_url2:
        embeded_tweet = True
    if embeded_tweet == True:
        for c in link[20:]:
            if c != "/":
                embeded_tweet_author += c
            else:
                break
    if embeded_tweet == False:
        return None
    else:
        if len(embeded_tweet_author) >= 2:
            return embeded_tweet_author

def process_links(lowered_tweet):

    clean_tweet = ""

    mid_link = False

    link = ""

    for i in range(len(lowered_tweet)):

        curr_char = lowered_tweet[i]

        if mid_link == False and (lowered_tweet[i:i+4] == "http" or "twitter" in lowered_tweet[i:i+15]):
            mid_link = True

        if mid_link == False:
            clean_tweet += lowered_tweet[i]

        if mid_link == True and curr_char == " ":
            mid_link = False

        if mid_link == True:
            link += curr_char

    embeded_tweet_author = analyze_link(link)
    return clean_tweet, embeded_tweet_author

def is_mention_a_time(mention):

    for i in range(len(mention)):
        curr_char = mention[i]
        if i != 0 and curr_char == "m":
            prev_char = mention[i-1]
            if prev_char == "a" or prev_char == "p":
                return True
    return False

def clean_mentions(mentions):
    final_mentions = []

    for mention in mentions:

        if len(mention) <= 4:
            final_mentions.append(mention)
            continue

        last_char_is_not_alphanum = False

        nums = ["0","1","2","3","4","5","6","7","8","9","_"]

        last_3_characters = mention[len(mention)-3:len(mention)]

        #print(mention)

        #print(last_3_characters)
        
        for c in last_3_characters:
            if c not in nums:
                if c < "a" or c > "z":
                    last_char_is_not_alphanum = True
                    #print("\n")
                    #print("Bad Mention: ", mention)
                    #for char in mention:
                        #print("CHAR: ", char)

        if last_char_is_not_alphanum == False:
            final_mentions.append(mention)

        temp_mention = ""
        k = 1
        while last_char_is_not_alphanum:

            if temp_mention == "":
                temp_mention = mention[0:len(mention)-k]
            else:
                temp_mention = temp_mention[0:len(temp_mention)-k]
                #print("Trying ", temp_mention)

            last_3_characters_temp = temp_mention[len(temp_mention)-3:len(temp_mention)]
            for c in last_3_characters_temp:
                if c in nums or (c >= "a" and c <= "z"):
                    last_char_is_not_alphanum = False
                else:
                    #print("No good! Trying Again")
                    last_char_is_not_alphanum = True
                    break

            if last_char_is_not_alphanum == False:
                #print("Fixed! New Mention: ", temp_mention)
                final_mentions.append(temp_mention)
            k += 1

    
    if len(final_mentions) >= 1:
        return final_mentions
    else:
        return "None"

def find_mentions(lowered_tweet):

    mentions = []

    in_mention = False
    at_username = ""

    for i in range(len(lowered_tweet)):

        curr_char = lowered_tweet[i]

        if in_mention and (curr_char == " ") and len(at_username) > 2:
            in_mention = False
            if not is_mention_a_time(at_username):
                mentions.append(at_username)
            at_username = ""

        if curr_char == "@":
            #print("FOUND @")
            #print(lowered_tweet)
            #print(lowered_tweet[i:i+10])
            in_mention = True
        
        if in_mention and curr_char != " ":
            at_username += curr_char
            #print(at_username)

    if in_mention:
        if not is_mention_a_time(at_username):
            mentions.append(at_username)

    
    final_mentions = clean_mentions(mentions)

    
    #if len(final_mentions) >= 1:
    return final_mentions
    #else:
    #    return "None"

clean_content = []
cleaned_mentions = []
is_tweet_embeded = []
embeded_authors = []
is_tweet_only_picture = []

for i, row in tweets_df.iterrows():

    if i % 1000 == 0:
        total = len( tweets_df )
        percent = str(i/total)
        print("Progess: " + str(i) + "/" + str(total) + " -- " + percent)

    # Lowercases the tweet itself

    tweet_content = row["content"].lower()

    # Checks to see if the tweet is only a photograph

    photo_tweet = is_photo_tweet(tweet_content)

    if photo_tweet:
        is_tweet_only_picture.append(1)
    else:
        is_tweet_only_picture.append(0)

    # removes irrelvant links, and finds embeded tweets if present

    tweet_content, embeded_tweet_author = process_links(tweet_content)

    clean_content.append(tweet_content)

    # finds a list of handles mentioned in the tweet

    tweet_mentions = find_mentions(tweet_content)

    # Adding mentions to list to be added to data frame

    cleaned_mentions.append(tweet_mentions)

    # creating lists for whether there is an embeded tweet, and the author of the tweet if so

    if embeded_tweet_author == None:
        is_tweet_embeded.append(0)
        embeded_authors.append("None")
    else:
        is_tweet_embeded.append(1)
        embeded_authors.append(embeded_tweet_author)



    tokens = word_pattern.findall(tweet_content)

    #print(tokens)

    if dev_count == 40:
        break
    #dev_count += 1

tweets_df["clean content"] = clean_content
tweets_df["pic only"] = is_tweet_only_picture

tweets_df.to_csv("tweets_clean.csv")
