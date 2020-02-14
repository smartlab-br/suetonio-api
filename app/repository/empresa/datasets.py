''' Repository para recuperar informações de uma empresa '''
from repository.base import RedisRepository

#pylint: disable=R0903
class DatasetsRepository(RedisRepository):
    ''' Definição do repo '''
    REDIS_KEY = 'rx:ds'
    DATASETS = {
        'rais': '2017',
        'rfb': '2018',
        'sisben': '2018',
        'catweb': '2017,2018',
        'auto': '2018',
        'caged': '2019',
        'rfbsocios': '2018',
        'rfbparticipacaosocietaria': '2018',
        'renavam': '2018',
        'aeronaves': '2018'
    }

    def retrieve(self):
        ''' Localiza o dicionário de datasources no REDIS '''
        return self.retrieve_hashset(self.REDIS_KEY)

    def store(self):
        ''' Inclui/atualiza dicionário de competências e datasources no REDIS '''
        self.get_dao().hmset(self.REDIS_KEY, self.DATASETS)
        return self.DATASETS
