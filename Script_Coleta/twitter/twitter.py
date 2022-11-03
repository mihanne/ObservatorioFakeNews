from ast import keyword
import base64
import datetime
import logging
from urllib.parse import urlencode
import json
from datetime import datetime
import requests
from twitter.tweet import Tweet
import os
from dotenv import load_dotenv
import time as ttime
import params.exec_params as Params


#curl -u lZGpVLBLq2uv479QLbyocav6Y:bIFbELuh3D5F2Rk7hmbY3RYKh9kj0VqBOGJwd0xC2lKSqLCzCE --data grant_type=client_credentials https://api.twitter.com/oauth2/token

class TwitterAPI(object):
    access_token = None
    access_token_expires = datetime.now()
    access_token_did_expire = True
    base_url = "https://api.twitter.com/2"

    def __init__(self,*args, **kwargs):
        super().__init__(*args, **kwargs)
        load_dotenv()
        self.bearer_token = os.getenv('BEARER_TOKEN')
        self.next_token = {}

    def __get_token_headers(self):
        
        return {
            "Authorization": f"Bearer {self.bearer_token}"
        }
    
    def get_twetts(self,**kwargs):
        self.next_token = {}
        req_seq = kwargs.get('req_seq') if 'req_seq' in kwargs else []
        keywords = kwargs.get('keywords') if 'keywords' in kwargs else ''
        start_date = kwargs.get('start_date') if 'start_date' in kwargs else 0
        end_date = kwargs.get('end_date') if 'end_date' in kwargs else 0
        max_results = kwargs.get('max_results') if 'max_results' in kwargs else 0
        count = 0
        for r in req_seq:
            max = r['num']
            key = keywords[r['type']]
            for _ in range(0,max):
                resp = self.__req_tweets(key, start_date, end_date, max_results,'all',use_next=True)
                count += Tweet.save_as_csv(resp)
                if len(resp) == 0 or resp[0].next_token == 0:
                    break
                ttime.sleep(2) #1 request/1s 
            ttime.sleep(Params.SLEEP_TIME) #300 requests/15 mins
            if  len(resp) == 0 or resp[0].next_token == 0: #Double Loop break
                logging.info(f'{datetime.now()} - Fim inesperado coleta de {str(start_date)} a {str(end_date)}, nenhum Tweet encontrado.')                            
                break
        return count

    def __req_tweets(self,keyword, start_date, end_date, max_results = 10,endpoint = 'recent', use_next=False):
        url = f'{self.base_url}/tweets/search/{endpoint}'
        next_token = self.next_token if use_next else {}
        query_params = {'query': keyword,
                        'start_time': start_date.strftime("%Y-%m-%dT%H:%M:%S.00Z"),
                        'end_time': end_date.strftime("%Y-%m-%dT%H:%M:%S.00Z"),
                        'max_results': max_results,
                        'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                        'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                        'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                        'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                        'next_token': next_token}

        resp = self.__get_resource(url,query_params)
        if resp == {}:
            return []
        self.save_json(resp)
        self.next_token = {resp['meta']['next_token']} if 'next_token' in resp['meta'] else {}
        tweet_list = [Tweet(x,resp['includes'],resp['meta']) for x in resp['data']]
        return tweet_list

    def old_json_convert(self,json):
        file_name = 'json/get_tweets.json'
        try:
            with open(file_name, 'r') as f:
                file_s = f.read()
            file_s.replace('\n','').replace('}{','},{')
            js = json.loads(file_s)
            file_name = 'json/backup_tweets_2.json'
            with open(file_name, 'w') as f:
                json.dump(js, f, indent=4,separators=(',',': '))
        except json.decoder.JSONDecodeError:
            logging.error(f'{datetime.now()} - Erro ao ler json de backup {file_name} não encontrado.')
            raise Exception("File not found")
        
        
    def __req_users(self):
        raise NotImplementedError
    
    def __req_bookmarks(self):
        raise NotImplementedError
    
    def __req_lists(self):
        raise NotImplementedError

    def save_json(self,resp):
        try:
            file_name = 'json/backup_tweets.json' #Necessário arquivo estar dentro da pasta e conter como conteudo {} na primeira execução
            js = []

            if os.path.isfile(file_name) is False:
                logging.error(f'{datetime.now()} - Erro ao salvar json de backup, arquivo {file_name} não encontrado.')
                raise Exception(f"Arquivo {file_name} nao encontrado")

            with open(file_name, 'r') as f:
                js = json.load(f)

            js.update({str(datetime.now()):resp})

            with open(file_name, 'w') as f:
                json.dump(js, f, indent=4,separators=(',',': '))
        except Exception as e: #Filtrar excessões
            logging.error(f'{datetime.now()} - Nao foi possível salvar backup. {e}.')
            pass

    def __get_resource(self, url,query_params, version='v2'):
        headers = self.__get_token_headers()
        r = requests.get(url, headers=headers,params=query_params)
        if r.status_code not in range(200, 299):
            print(f'Endpoint Response Code:{str(r.status_code)} {r.text}')
            logging.warning(f'{datetime.now()} - Erro na chamada API Twitter: {str(r.status_code)} {r.text}')
            return {}
        return r.json()
