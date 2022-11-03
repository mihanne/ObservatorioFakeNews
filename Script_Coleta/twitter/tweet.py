
from datetime import datetime
import csv
import logging
import os

class Tweet(object):

    def __init__(self, data, includes, meta):

        self.created_at = data['created_at']
        self.referenced_type = data['referenced_tweets'][0]['type'] if 'referenced_tweets' in data else ''
        self.referenced_id = data['referenced_tweets'][0]['id'] if 'referenced_tweets' in data else ''
        self.source = data['source']
        self.id = data['id']
        self.reply_settings = data['reply_settings']
        self.lang = data['lang']
        self.author_id = data['author_id']
        self.text = data['text']
        self.geo_id = data['geo']['place_id'] if 'geo' in data and 'place_id' in data['geo'] else ''
        self.retweet_count = data['public_metrics']['retweet_count']
        self.reply_count = data['public_metrics']['reply_count']
        self.like_count = data['public_metrics']['like_count']
        self.quote_count = data['public_metrics']['quote_count']
        self.conversation_id = data['conversation_id']
        self.api_date = datetime.now()

        user = [x for x in includes['users'] if x['id'] == self.author_id][0]
        self.username = user['username']                           
        self.user_description = user['description']                         
        #self.id = user['id']                                   
        self.user_verified = user['verified']                             
        self.user_created_at = user['created_at']                           
        self.user_name = user['name']                                 
        self.user_followers_count = user['public_metrics']['followers_count']                      
        self.user_following_count = user['public_metrics']['following_count']                      
        self.user_tweet_count = user['public_metrics']['tweet_count']                          
        self.user_listed_count = user['public_metrics']['listed_count']

        place = [x for x in includes['places'] if x['id'] == self.geo_id] if 'places' in includes else []
        self.place_type = place[0]['place_type'] if len(place) > 0 else 'none'
        #self.geo_id = place[0]['id']
        self.country = place[0]['country'] if len(place) > 0 else 'none'
        self.full_name = place[0]['full_name'] if len(place) > 0 else 'none'
        self.country_code = place[0]['country_code'] if len(place) > 0 else 'none'
        self.name = place[0]['name'] if len(place) > 0 else 'none'

        self.result_count = meta['result_count']
        self.next_token = meta['next_token'] if 'next_token' in meta else 0


    def __str__(self) -> str:
        return f'\nCreated_at: {self.created_at}\nReferenced_type: {self.referenced_type}\nReferenced_id: {self.referenced_id}\nSource: {self.source}\nID: {self.id}\nReply_settings: {self.reply_settings}\nLang: {self.lang}\nAuthor_id: {self.author_id}\nText: {self.text}\nRetweet_count: {self.retweet_count}\nReply_count: {self.reply_count}\nLike_count: {self.like_count}\nQuote_count: {self.quote_count}\nConversation_id: {self.conversation_id}\nApi_date: {self.api_date}'        

    def __iter__(self):
        return iter([self.created_at,self.referenced_type,self.referenced_id,self.source,self.id,self.reply_settings,self.lang,self.author_id,self.text,self.geo_id,self.retweet_count,self.reply_count,self.like_count,self.quote_count,self.conversation_id,self.api_date,self.username,self.user_description,self.user_verified,self.user_created_at,self.user_name,self.user_followers_count,self.user_following_count,self.user_tweet_count,self.user_listed_count,self.place_type,self.country,self.full_name,self.country_code,self.name,self.result_count,self.next_token])

    @staticmethod
    def save_as_csv(tweet_list):
        if len(tweet_list) > 0:
            header = ['created_at','referenced_type','referenced_id','source','id','reply_settings','lang','author_id','text','geo_id','retweet_count','reply_count','like_count','quote_count','conversation_id','api_date','username','description','user_verified','user_created_at','user_name','user_followers_count','user_following_count','user_tweet_count','user_listed_count','place_type','country','full_name','country_code','name','result_count','next_token']
            now = datetime.now().strftime("%Y%m%d")
            file_name = f'data/tweetdata{now}.csv'
            file_exists = os.path.isfile(file_name)
            with open(file_name, 'a', encoding='UTF-8', newline='',) as f:
                writer = csv.writer(f, delimiter=';')
                if not file_exists:
                    writer.writerow(header)
                writer.writerows(tweet_list)
            #logging.info(f'{datetime.now()} - Incrementado .csv da data {now} com {tweet_list[0].result_count} registros')
            return tweet_list[0].result_count
        return 0
