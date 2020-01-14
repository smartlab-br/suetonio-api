''' Repository para recuperar informações de uma empresa '''
from repository.base import RedisRepository
import json

#pylint: disable=R0903
class PessoaDatasetsRepository(RedisRepository):
    ''' Definição do repo '''
    REDIS_BASE = 'rx:{}:{}:{}'
    
    def retrieve(self, id_pfpj, ds, pfpj = 'pj'):
        ''' Obtém o hashset de status de carregamento do REDIS '''
        return self.get_dao().hgetall(self.REDIS_BASE.format(pfpj, id_pfpj, ds))
