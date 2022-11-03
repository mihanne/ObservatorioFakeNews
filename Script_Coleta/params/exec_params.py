'''
Com a limitação de 10.000.000 Tweets por mês e máximo de 
300 requisições a cada 15min e apenas 500 tweets a cada
requisição, foi estipulado a regra:

10M/31 = 322.580 tweets por dia
322.580/500 = 645 requisições por dia
300 requisições ás X
300 requisições ás X + 30min
045 requisições ás X + 60min

'''
EXEC_ENV =  'PROD' #PROD DEV
MAX_RESULTS = 500
REQUISIONS_PER_EXEC = [{'num':299,'type':0},{'num':299,'type':0},{'num':47,'type':1}] #n: numero de requisições, t: tipo, onde 0 é keywords e 1 contas_twitter
SLEEP_TIME = 900 #15min