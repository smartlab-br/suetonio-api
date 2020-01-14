''' Repository para recuperar informações da CEE '''
from model.base import BaseModel
from repository.empresa.empresa import EmpresaRepository
from repository.empresa.pessoadatasets import PessoaDatasetsRepository
from repository.empresa.datasets import DatasetsRepository
from kafka import KafkaProducer
from flask import current_app
from datetime import datetime

#pylint: disable=R0903
class Empresa(BaseModel):
    ''' Definição do repo '''
    TOPICS = ['rais', 'rfb', 'sisben', 'catweb', 'auto', 'delphos', 'mni', 'caged', 'rfbsocios', 'rfbparticipacaosocietaria']

    def __init__(self):
        ''' Construtor '''
        self.repo = EmpresaRepository()

    def get_repo(self):
        ''' Garantia de que o repo estará carregado '''
        if self.repo is None:
            self.repo = EmpresaRepository()
        return self.repo

    def find_datasets(self, options):
        ''' Localiza um todos os datasets de uma empresa pelo CNPJ Raiz '''
        (loading_entry, loading_entry_is_valid, column_status) = self.get_loading_entry(options['cnpj_raiz'], options)
        result = {'status': loading_entry}
        if loading_entry_is_valid:
            (dataset, metadata) = self.get_repo().find_datasets(options)
            result['metadata'] = metadata
            if 'only_meta' in options and options['only_meta']:
                result['dataset'] = []
            else: 
                result['dataset'] = dataset
        if 'column' in options:
            result['status_competencia'] = column_status
        return result

    def produce(self, cnpj_raiz):
        ''' Gera uma entrada na fila para ingestão de dados da empresa '''
        (loading_entry, loading_entry_is_valid) = self.get_loading_entry(cnpj_raiz)
        if not loading_entry_is_valid:
            kafka_server = f'{current_app.config["KAFKA_HOST"]}:{current_app.config["KAFKA_PORT"]}'
            msg = bytes(cnpj_raiz, 'utf-8')
            producer = KafkaProducer(bootstrap_servers=[kafka_server])
            for t in self.TOPICS:
                t_name = f'{current_app.config["KAFKA_TOPIC_PREFIX"]}-{t}'
                producer.send(t_name, msg)
            producer.close()
        return {'status': loading_entry}

    def get_loading_entry(self, cnpj_raiz, options={}):
        ''' Verifica se há uma entrada ainda válida para ingestão de dados da empresa '''
        rules_dao = DatasetsRepository()
        loading_status_dao = PessoaDatasetsRepository()
        is_valid = True
        loading_entry = {}
        column_status = 'INGESTED'
        for ds, slot_list in rules_dao.DATASOURCES:
            columns_available = loading_status_dao.retrieve(cnpj_raiz, ds)

            # Aquela entrada já existe no REDIS (foi carregada)?
            # A entrada é compatível com o rol de datasources?
            # A entrada tem menos de 1 mês?
            if (columns_available is None or
                any([slot not in columns_available.keys() for slot in slot_list.split(',')]) or
                abs((datetime.now() - datetime.strptime(columns_available['when'], "%Y-%m-%d")).days) > 30):
                is_valid = False
            loading_entry[ds] = columns_available

            if 'column' in options:
                column_status = self.assess_column_status(
                    slot_list.split(','),
                    columns_available,
                    options['column']
                )
        
        return (loading_entry, is_valid, column_status)

    @staticmethod
    def assess_column_status(slot_list, columns_available, column):
        ''' Checks the status of a defined column '''
        if column in slot_list:
            if column in columns_available.keys():
                return columns_available[column]
            else:
                return 'MISSING'
        if (column in columns_available.keys() and
            if columns_available[column] == 'INGESTED'):
                return 'DEPRECATED'
        return 'UNAVAILABLE'