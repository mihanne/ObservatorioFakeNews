from libs import sum_sentimentos, limpa_stopword, filtro_lula, filtro_bolso
from libs import gera_wordcloud, gera_bar_plot

# manipulação de dados
from pandas import DataFrame, read_csv, concat, read_excel
from unidecode import unidecode
import os
import re
import string

# preprocessamento NLP
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.probability import FreqDist
from nltk.sentiment import SentimentIntensityAnalyzer

# Multi Processamento
from multiprocessing import Pool

caminho: list
dfs: list
df: DataFrame
carac: list
stopword: set
lista: list
fdist: FreqDist


senti = read_csv('save/sentinet.csv')

lexico = []

for row in range(0, senti.shape[0]):
    lexico.append(
        (unidecode(str(senti['CONCEPT'][row])),
         senti['POLARITY INTENSITY'][row]))
    lexico.append(
        (unidecode(str(senti['SEMANTICS1'][row])),
         senti['POLARITY INTENSITY'][row]))
    lexico.append(
        (unidecode(str(senti['SEMANTICS2'][row])),
         senti['POLARITY INTENSITY'][row]))
    lexico.append(
        (unidecode(str(senti['SEMANTICS3'][row])),
         senti['POLARITY INTENSITY'][row]))
    lexico.append(
        (unidecode(str(senti['SEMANTICS4'][row])),
         senti['POLARITY INTENSITY'][row]))
    lexico.append(
        (unidecode(str(senti['SEMANTICS5'][row])),
         senti['POLARITY INTENSITY'][row]))

carac = ["'", '"', '@', ':', ';', '.', '!', '?', '#', '$', '%', '&', '*', '(',
         ')', '-', '_', '+', '=', '{', '}', '[', ']', '|', '\\', '<', '>', ',',
         'https', 'http', '²', 'rt', 'i', '0', '1', '2', '3', '4', '5', ' 6',
         'é', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'do', 'da', 'de', 'di',
         'dos', 'das', 'das', 'das', 'das', 'das', 'das', 'lo', 'o', 'ó',
         '...', '``', '', '…', '“', '”', '’', '..', 'el', 'la', 'en', 'nt',
         'los', 'del', 'like', 'amp', 'get', 'con', 'pra', 'podemos', 'las',
         'si', 'le', 'gt', 'ya', '‘', '10', 'tá', "''", '7', '8', '9', 'su',
         'do', 'das', '"s"', '!', 'un', 'es', 'q']

stopword = stopwords.words('portuguese') + carac
stopword = set(stopword)

lula_lista = read_excel('data/Lista_Lula_Bolsonaro.xlsx', sheet_name=1)
bolso_lista = read_excel('data/Lista_Lula_Bolsonaro.xlsx', sheet_name=2)

lula_lista = lula_lista.iloc[:, 0].to_list()
bolso_lista = bolso_lista.iloc[:, 0].to_list()

caminhos = [os.path.join(nome) for nome in os.listdir('./data')
            if 'csv' in nome]

for caminho in caminhos:
    df = read_csv(f'./data/{caminho}', sep=';', encoding='utf-8')
    df = df.loc[df['lang'] == 'pt', 'text']
    df = df.apply(lambda x: x.lower())
    df = DataFrame(data={'text': df})

    # tokeniza
    df['token'] = df.apply(lambda x: word_tokenize(x['text']), axis=1)

    # limpando
    url_regex = re.compile(
        '((https://|www|\[)[a-z0-9_.-]+]?)', re.IGNORECASE)
    chars = '[%s]+' % re.escape(string.punctuation)
    df['text'] = df.apply(lambda x: unidecode(str(x['text'])), axis=1)
    df['text'] = df.apply(
        lambda x: url_regex.sub('URL', x['text']), axis=1)
    df['text'] = df.apply(lambda x: re.sub(chars, '', x['text']), axis=1)

    # retira stopwords
    df['token_clean'] = df.apply(
        lambda x: limpa_stopword(x['token'], stopword), axis=1)

    # Classifica sentimentos nltk
    df['nota_nltk'] = ''

    sid = SentimentIntensityAnalyzer()

    for x in range(0, df.shape[0]):

        senti = sid.polarity_scores(df['text'][x])

        if (senti['pos'] > senti['neg']) and (senti['pos'] > senti['neu']):
            df['sentiment_nltk'][x] = 'positivo'

        elif (senti['neg'] > senti['pos']) and (senti['neg'] > senti['neu']):
            df['sentiment_nltk'][x] = 'negativo'

        elif (senti['neu'] > senti['neg']) and (senti['neu'] > senti['pos']):
            df['sentiment_nltk'][x] = 'neutro'

        else:
            df['sentiment_nltk'][x] = 'sem_classificacao'

    if __name__ == '__main__':
        with Pool(processes=8) as pool:
            df['nota_senticnet'] = pool.map(sum_sentimentos, df['text'])

    if __name__ == '__main__':
        with Pool(processes=8) as pool:
            df['is_lula'] = pool.map(filtro_lula, df['text'])

    if __name__ == '__main__':
        with Pool(processes=8) as pool:
            df['is_bolso'] = pool.map(filtro_bolso, df['text'])

    minimo = df["nota_senticnet"].min()
    maximo = df["nota_senticnet"].max()

    def scalar(x):
        if x < 0:
            return x / -minimo
        else:
            return x / maximo

    df["nota_senticnet_scaler"] = df["nota_senticnet"].apply(
        lambda x: scalar(x))
    
    df["nota_senticnet_scaler"] = data["nota_senticnet"]\
        .apply(lambda x: scalar(x))
    df['senti'] = pd.qcut(x = data['nota_senticnet_scaler'],
                            q=[0,.375,.625,1],
                            labels=['negativo', 'neutro','positivo'] )

    df.to_csv(f'./temp/{caminho}')
    print(f'{caminho}')
    
caminhos = [os.path.join(nome)
            for nome in os.listdir(pasta) if 'csv' in nome]

dfs = []

for caminho in caminhos:
    print(caminho)
    df = read_csv(f'temp/{caminho}', lineterminator='\n')
    dfs.append(df)

data_complet = read_csv('complet.csv').reset_index(drop=True, inplace=True)

data = concat([dfs, data_complet], ignore_index=True)

data.drop_duplicates(inplace=True)

data.to_csv('complet.csv')

data.sort_values(by='nota_senticnet', ascending=True)[['text','nota_senticnet_scaler']].head(500).to_csv('negativo.csv')

data.sort_values(by='nota_senticnet', ascending=False)[['text','nota_senticnet_scaler']].head(500).to_csv('positivo.csv')