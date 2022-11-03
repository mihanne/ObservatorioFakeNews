# manipulação de dados
from pandas import DataFrame, read_csv, concat

# manipulação de arquivos
import os
import re

# visualização de dados
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import plotly.express as px


def sum_sentimentos(string: str):
    return sum([lexico[lrow][1] if lexico[lrow][0] in string else 0
                for lrow in range(0, len(lexico))])


def limpa_stopword(token, stopword: list):
    return [w for w in token if w not in stopword]


def filtro_lula(string: str):
    for x in range(len(lula_lista)):
        padrao = re.search(lula_lista[x], string)
        if padrao is not None:
            return 'lula'
        else:
            return 'nop'


def filtro_bolso(string: str):
    for x in range(len(bolso_lista)):
        padrao = re.search(bolso_lista[x], string)
        if padrao is not None:
            return 'bolso'
        else:
            return 'nop'


def concatena_limpa_salva(pasta: str) -> DataFrame:
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
    return data


# Gera os plots

def gera_wordcloud(freq: dict) -> None:
    wc = WordCloud(background_color="white", max_words=500,
                   max_font_size=256, random_state=42, width=500, height=500)
    wc.generate_from_frequencies(freq)
    plt.figure(figsize=(20, 20))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis('off')
    plt.show()


def gera_bar_plot(freq: DataFrame) -> None:
    fig = px.bar(freq, x=0, y=1, text_auto='.2s',
                 title='As 50 palavras mais frequentes')
    fig.update_layout(barmode='group', xaxis_tickangle=-45,
                      title={'xanchor': 'center', 'yanchor': 'top', 'x': .5})
    fig.show()
