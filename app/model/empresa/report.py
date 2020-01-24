''' Repository para recuperar informações da CEE '''
from model.base import BaseModel
from repository.empresa.report import ReportRepository

#pylint: disable=R0903
class Report(BaseModel):
    ''' Definição do repo '''
    TOPICS = [
        'rais', 'rfb', 'sisben', 'catweb', 'auto', 'caged', 'rfbsocios',
        'rfbparticipacaosocietaria', 'aeronaves', 'renavam'
    ]

    def __init__(self):
        ''' Construtor '''
        self.repo = ReportRepository()

    def get_repo(self):
        ''' Garantia de que o repo estará carregado '''
        if self.repo is None:
            self.repo = ReportRepository()
        return self.repo

    def find_report(self, cnpj_raiz):
        ''' Localiza report pelo CNPJ Raiz '''
        return self.get_repo().find_report(cnpj_raiz)

    def generate(self, cnpj_raiz):
        ''' Inclui/atualiza dicionário de competências e datasources no REDIS '''
        self.get_repo().store(cnpj_raiz)
