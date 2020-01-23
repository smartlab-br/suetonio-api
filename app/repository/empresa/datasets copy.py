''' Repository para recuperar informações de uma empresa '''
from repository.base import RedisRepository
import json

#pylint: disable=R0903
class ReportRepository(RedisRepository):
    ''' Definição do repo '''
    REDIS_KEY = 'rmd:{}:{}'
    
    def find_report(self, cnpj):
        ''' Localiza o report no REDIS '''
        return self.get_dao().get(self.REDIS_KEY.format(cnpj_raiz))
