''' Repository para recuperar informações da CEE '''
from model.base import BaseModel
from repository.empresa.empresa import EmpresaRepository

#pylint: disable=R0903
class Empresa(BaseModel):
    ''' Definição do repo '''
    def __init__(self):
        ''' Construtor '''
        self.repo = EmpresaRepository()

    def get_repo(self):
        ''' Garantia de que o repo estará carregado '''
        if self.repo is None:
            self.repo = EmpresaRepository()
        return self.repo

    def find_datasets(self, cnpj_raiz, column_family=None, column=None, cnpj=None, id_pf=None, only_meta=False, simplified=False, perspective=None):
        ''' Localiza um todos os datasets de uma empresa pelo CNPJ Raiz '''
        (dataset, metadata) = self.get_repo().find_datasets(cnpj_raiz, column_family, column, cnpj, id_pf, simplified=simplified, perspective=perspective)
        if (only_meta):
            return { 'metadata': metadata, 'dataset': [] }
        return { 'metadata': metadata, 'dataset': dataset }
