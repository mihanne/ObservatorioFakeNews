
class Util(object):

    @staticmethod
    def get_keywords():
        keyword = accounts = ''
        with open('params/keywords.txt', 'r',encoding="utf8") as file:
            keyword = file.read().replace('\n', ' OR ')
        with open('params/contas_twitter.txt', 'r',encoding="utf8") as file:
            accounts = file.read().replace('\n', ' OR ') #from: jairbolsonaro OR lulaoficial 
        return [keyword, f'from: {accounts}']