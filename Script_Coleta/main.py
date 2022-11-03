from datetime import datetime, timedelta
from twitter.tweet import Tweet
from twitter.twitter import TwitterAPI
import params.exec_params as Params
from util import Util
import threading
import time as ttime
import logging
import json


api = TwitterAPI()
ENV = Params.EXEC_ENV
max_results = Params.MAX_RESULTS
req_seq = Params.REQUISIONS_PER_EXEC

logging.basicConfig(filename=f'execution_{ENV}.log', level=logging.INFO)
#QUERY BUILDER https://developer.twitter.com/en/docs/twitter-api/tweets/counts/integrate/build-a-query

def loop():
    start_date = datetime.now()#.replace(hour=00, minute=00, second=00) 
    start_date -= timedelta(days=1)
    while True:
        keywords = Util.get_keywords()
        file = open('params/retroac.txt', 'r',encoding="utf-8-sig")
        all_lines = file.readlines()
        """
        Arquivo para processamento retroativo, para cada reprocessamento inserir uma linha no arquivo params/retroac.txt no padrão abaixo
        DataProcessamento: Data base para busca dos tweets, formato YYYY-MM-DD
        NumeroRequisições: Numero de requisições a ser feita para a data, formato 00-
        TipoKeyword: Tipo de Keyword para busca 0 - keywords 1 - contas_twitter, formato 0

        Exemplo:DataProcessamento;NumeroRequisições-TipoKeyword
            YYYY-MM-DD;10-1,99-0,2-0
            2022-05-01;299-0,299-0,47-1
            
        """
        for l in all_lines:
            if l.startswith("#"):
                continue
            try:
                p_date,p_params = l.split(';')
                p_date = datetime.strptime(p_date,"%Y-%m-%d").replace(hour=00, minute=00, second=00) 
                end_date = p_date.replace(hour=23, minute=59, second=59)
                p_params = [{'num':int(i.split('-')[0]),'type':int(i.split('-')[1])} for i in p_params.split(',')] if p_params != '' else req_seq
                logging.info(f'{datetime.now()} - Inciando coleta retroativa de {str(p_date)} a {str(end_date)}')
                #print(p_params,p_date,end_date,max_results)
                count = api.get_twetts(req_seq=p_params,keywords=keywords,start_date=p_date,end_date=end_date,max_results=max_results)
                logging.info(f'{datetime.now()} - Finalizada a coleta reatroativa de {str(p_date)} a {str(end_date)}. {count} registros incluidos.')
                all_lines.remove(l)
            except Exception as e:
                logging.error(f'{datetime.now()} - Não foi possível fazer o reprocessamento, verifique os parâmetros incluidos no arquivo.\n{e} {e.args}')
                pass
        file.close()
        file = open('params/retroac.txt', 'w',encoding="utf8")
        file.writelines(all_lines)
        file.close()               

        try:
            if start_date.day != datetime.now().day:
                ttime.sleep(11) # 'end_time' must be a minimum of 10 seconds prior to the request time.
                end_date = start_date.replace(hour=23, minute=59, second=59)
                logging.info(f'{datetime.now()} - Inciando coleta de {str(start_date)} a {str(end_date)}')
                count = api.get_twetts(req_seq=req_seq,keywords=keywords,start_date=start_date,end_date=end_date,max_results=max_results)
                logging.info(f'{datetime.now()} - Finalizada a coleta de {str(start_date)} a {str(end_date)}. {count} registros incluidos.')
                start_date += timedelta(days=1)
        except Exception as e:
            print(f'Erro ás {datetime.now()}')
            print(e)
            logging.error(f'{datetime.now()} - Erro na chamada da API Twitter: {e} {e.args}')

    
if ENV == 'PROD':    
    x = threading.Thread(target=loop)
    x.start()
    print("[Thread started]")
else:
    max_results = 10
    req_seq = [{'num':1,'type':0},{'num':1,'type':1}]
    start_date = datetime.now().replace(hour=00, minute=00, second=00)
    end_date = start_date.replace(hour=23, minute=59, second=59)
    print(f'Execução única - {datetime.now()}')
    keywords = Util.get_keywords()
    count = api.get_twetts(req_seq=req_seq,keywords=keywords,start_date=start_date,end_date=end_date,max_results=max_results)
    print(f'Finalizada, {count} foram coletados. - {datetime.now()}')
