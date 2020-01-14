''' Repository para recuperar informações de uma empresa '''
from datetime import datetime
from repository.base import RedisRepository
import json

#pylint: disable=R0903
class PessoaDatasetsRepository(RedisRepository):
    ''' Definição do repo '''
    REDIS_BASE = 'rx:{}:{}:{}'
    
    def retrieve(self, id_pfpj, ds, pfpj = 'pj'):
        ''' Obtém o hashset de status de carregamento dco REDIS '''
        return self.get_dao().hgetall(self.REDIS_BASE.format(pfpj, id_pfpj, ds))

    def store_status(self, id_pfpj, ds, competencias, pfpj="pj"):
        ''' Armazena um registro de status enviado para o kafka '''
        dict_status = {cmp: 'INGESTING' for cmp in competencias}
        dict_status['when'] = f"{datetime.now():%Y-%m-%d}"
        self.get_dao().hmset(self.REDIS_BASE.format(pfpj, id_pfpj, ds), dict_status)
