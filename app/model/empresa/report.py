''' Repository para recuperar informações da CEE '''
from model.empresa.empresa import Empresa
from repository.empresa.report import ReportRepository

#pylint: disable=R0903
class Report(Empresa):
    ''' Definição do repo '''
    def __init__(self):
        ''' Construtor '''
        self.repo = None
        self.__set_repo()

    def get_repo(self):
        ''' Garantia de que o repo estará carregado '''
        if self.repo is None:
            self.repo = ReportRepository()
        return self.repo

    def __set_repo(self):
        ''' Setter invoked in Construtor '''
        self.repo = ReportRepository()

    def find_report(self, cnpj_raiz):
        ''' Localiza report pelo CNPJ Raiz '''
        return self.get_repo().find_report(cnpj_raiz)

    def generate(self, cnpj_raiz):
        ''' Inclui/atualiza dicionário de competências e datasources no REDIS '''
        self.get_repo().store(cnpj_raiz)
