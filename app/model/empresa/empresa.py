''' Repository para recuperar informações da CEE '''
from model.base import BaseModel
from repository.empresa.empresa import EmpresaRepository
from kafka import KafkaProducer
from flask import current_app

#pylint: disable=R0903
class Empresa(BaseModel):
    ''' Definição do repo '''
    TOPICS = ['rais', 'catweb', 'autos']
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

    def produce(self, cnpj_raiz):
        kafka_server = f'{current_app.config["KAFKA_HOST"]}:{current_app.config["KAFKA_PORT"]}'
        # TODO Check namespace for connection
        # TODO check broker version
        print(kafka_server)
        producer = KafkaProducer(bootstrap_servers=kafka_server)

        for t in self.TOPICS:
            t_name = f'{current_app.config["KAFKA_TOPIC_PREFIX"]}-{t}'
            print(t_name)
            producer.send(t_name, cnpj_raiz)
