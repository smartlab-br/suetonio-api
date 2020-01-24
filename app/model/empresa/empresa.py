''' Repository para recuperar informações da CEE '''
import requests
from datetime import datetime
from kafka import KafkaProducer
from model.base import BaseModel
from repository.empresa.empresa import EmpresaRepository
from repository.empresa.pessoadatasets import PessoaDatasetsRepository
from model.empresa.datasets import DatasetsRepository
from flask import current_app

#pylint: disable=R0903
class Empresa(BaseModel):
    ''' Definição do repo '''
    TOPICS = ['rais', 'rfb', 'sisben', 'catweb', 'auto', 'caged', 'rfbsocios', 'rfbparticipacaosocietaria', 'aeronaves', 'renavam']

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
        try:
            (dataset, metadata) = self.get_repo().find_datasets(options)
            result['metadata'] = metadata
            if 'only_meta' in options and options['only_meta']:
                result['dataset'] = []
            else:
                result['dataset'] = dataset
        except requests.exceptions.HTTPError:
            loading_entry_is_valid = False
        if not loading_entry_is_valid:
            result['invalid'] = True
        if 'column' in options:
            result['status_competencia'] = column_status
        return result

    def produce(self, cnpj_raiz):
        ''' Gera uma entrada na fila para ingestão de dados da empresa '''
        (loading_entry, loading_entry_is_valid, column_status) = self.get_loading_entry(cnpj_raiz)
        kafka_server = f'{current_app.config["KAFKA_HOST"]}:{current_app.config["KAFKA_PORT"]}'
        msg = bytes(cnpj_raiz, 'utf-8')
        producer = KafkaProducer(bootstrap_servers=[kafka_server])
        redis_dao = PessoaDatasetsRepository()
        ds_dict = DatasetsRepository().DATASETS
        for t in self.TOPICS:
            # First, updates status on REDIS
            redis_dao.store_status(cnpj_raiz, t, ds_dict[t].split(','))
            # Then publishes to Kafka
            t_name = f'{current_app.config["KAFKA_TOPIC_PREFIX"]}-{t}'
            producer.send(t_name, msg)
        producer.close()

    def get_loading_entry(self, cnpj_raiz, options={}):
        ''' Verifica se há uma entrada ainda válida para ingestão de dados da empresa '''
        rules_dao = DatasetsRepository()
        loading_status_dao = PessoaDatasetsRepository()
        is_valid = True
        loading_entry = {}
        column_status = 'INGESTED'
        column_status_specific = None
        for ds, slot_list in rules_dao.DATASETS.items():
            columns_available = loading_status_dao.retrieve(cnpj_raiz, ds)

            # Aquela entrada já existe no REDIS (foi carregada)?
            # A entrada é compatível com o rol de datasources?
            # A entrada tem menos de 1 mês?
            if (columns_available is None or
                    any([slot not in columns_available.keys() for slot in slot_list.split(',')]) or
                    ('when' in columns_available and (datetime.strptime(columns_available['when'], "%Y-%m-%d") - datetime.now()).days > 30)):
                is_valid = False
            loading_entry[ds] = columns_available

            if 'column' in options:
                column_status = self.assess_column_status(
                    slot_list.split(','),
                    columns_available,
                    options['column']
                )
                if options['column_family'] == ds:
                    column_status_specific = column_status

        # Overrides if there's a specific column status
        if column_status_specific is not None:
            column_status = column_status_specific

        return (loading_entry, is_valid, column_status)

    @staticmethod
    def assess_column_status(slot_list, columns_available, column):
        ''' Checks the status of a defined column '''
        if column in slot_list:
            if column in columns_available.keys():
                return columns_available[column]
            return 'MISSING'
        if (column in columns_available.keys() and
                columns_available[column] == 'INGESTED'):
            return 'DEPRECATED'
        return 'UNAVAILABLE'
