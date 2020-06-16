''' Repository para recuperar informações da CEE '''
from datetime import datetime
import requests
import json
from kafka import KafkaProducer
from flask import current_app
from model.thematic import Thematic
from model.base import BaseModel
from model.empresa.datasets import DatasetsRepository
from repository.empresa.empresa import EmpresaRepository
from repository.empresa.pessoadatasets import PessoaDatasetsRepository

#pylint: disable=R0903
class Empresa(BaseModel):
    ''' Definição do repo '''
    TOPICS = [
        'rais', 'rfb', 'sisben', 'catweb', 'auto', 'caged', 'rfbsocios',
        'rfbparticipacaosocietaria', 'aeronaves', 'renavam', 'cagedsaldo',
        'cagedtrabalhador', 'cagedtrabalhadorano'
    ]
    
    def __init__(self):
        ''' Construtor '''
        self.repo = None
        self.__set_repo()

    def get_repo(self):
        ''' Garantia de que o repo estará carregado '''
        if self.repo is None:
            self.repo = EmpresaRepository()
        return self.repo

    def __set_repo(self):
        ''' Setter invoked in Construtor '''
        self.repo = EmpresaRepository()

    def find_datasets(self, options):
        ''' Localiza um todos os datasets de uma empresa pelo CNPJ Raiz '''
        (loading_entry, loading_entry_is_valid, column_status) = self.get_loading_entry(
            options['cnpj_raiz'],
            options
        )
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
            self.produce(
                options['cnpj_raiz'],
                options.get('column_family'),
                options.get('column')
            )
        if not loading_entry_is_valid:
            result['invalid'] = True
        if 'column' in options:
            result['status_competencia'] = column_status
        return result

    def produce(self, cnpj_raiz, column_family, column):
        ''' Gera uma entrada na fila para ingestão de dados da empresa '''
        kafka_server = f'{current_app.config["KAFKA_HOST"]}:{current_app.config["KAFKA_PORT"]}'
        producer = KafkaProducer(bootstrap_servers=[kafka_server])
        redis_dao = PessoaDatasetsRepository()
        ds_dict = DatasetsRepository().DATASETS

        if column_family is None:
            for topic in self.TOPICS:
                # First, updates status on REDIS
                redis_dao.store_status(cnpj_raiz, topic, ds_dict[topic].split(','))
                # Then publishes to Kafka
                for comp in ds_dict[topic].split(','):
                    t_name = f'{current_app.config["KAFKA_TOPIC_PREFIX"]}-{topic}'
                    msg = bytes(f'{cnpj_raiz}:{comp}', 'utf-8')
                    producer.send(t_name, msg)
        else:
            if column is None:
                # First, updates status on REDIS
                redis_dao.store_status(cnpj_raiz, column_family, ds_dict[column_family].split(','))
                # Then publishes to Kafka
                for comp in ds_dict[column_family].split(','):
                    t_name = f'{current_app.config["KAFKA_TOPIC_PREFIX"]}-{column_family}'
                    msg = bytes(f'{cnpj_raiz}:{comp}', 'utf-8')
                    producer.send(t_name, msg)
            else:
                # First, updates status on REDIS
                redis_dao.store_status(cnpj_raiz, column_family, [column])
                t_name = f'{current_app.config["KAFKA_TOPIC_PREFIX"]}-{column_family}'
                msg = bytes(f'{cnpj_raiz}:{column}', 'utf-8')
                producer.send(t_name, msg)
        producer.close()

    def get_loading_entry(self, cnpj_raiz, options=None):
        ''' Verifica se há uma entrada ainda válida para ingestão de dados da empresa '''
        rules_dao = DatasetsRepository()
        if (not options.get('column_family') or
                not rules_dao.DATASETS.get((options.get('column_family')))):
            raise ValueError('Dataset inválido')
        if (options.get('column') and 
                options.get('column') not in rules_dao.DATASETS.get((options.get('column_family'))).split(',')):
            raise ValueError('Competência inválida para o dataset informado')
        loading_status_dao = PessoaDatasetsRepository()
        is_valid = True
        loading_entry = {}
        column_status = 'INGESTED'
        column_status_specific = None
        for dataframe, slot_list in rules_dao.DATASETS.items():
            columns_available = loading_status_dao.retrieve(cnpj_raiz, dataframe)
            if options.get('column_family', dataframe) == dataframe:
                # Aquela entrada já existe no REDIS (foi carregada)?
                # A entrada é compatível com o rol de datasources?
                # A entrada tem menos de 1 mês?
                if (columns_available is None or
                        any([slot not in columns_available.keys() for slot in slot_list.split(',')])):
                    is_valid = False
                else:
                    for col_key, col_val in columns_available.items():
                        if (options.get('column', col_key) == col_key and
                                'INGESTED' in col_val and
                                len(col_val.split('|')) > 1 and
                                (datetime.strptime(col_val.split('|')[1], "%Y-%m-%d") - datetime.now()).days > 30):
                            is_valid = False
                
                if 'column' in options:
                    column_status = self.assess_column_status(
                        slot_list.split(','),
                        columns_available,
                        options.get('column')
                    )
                    if options.get('column_family') == dataframe:
                        column_status_specific = column_status
            if columns_available:
                    loading_entry[dataframe] = columns_available

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
                'INGESTED' in columns_available[column]):
            return 'DEPRECATED'
        return 'UNAVAILABLE'
    
    def get_statistics(self, options):
        ''' Gets statistics for a company using impala '''
        print(options)
        if options.get('column_family'):
            dataframes = [options.get('column_family')]
        else:
            dataframes = self.TOPICS # TODO 1 - Check if tables and topics names match

        # TODO 99 - Add threads to run impala queries
        result = {}
        thematic_handler = Thematic()
        for df in dataframes:
            # Get statistics for dataset
            cols = thematic_handler.get_column_defs(df)
            subset_rules = [f"eq-{cols.get('cnpj_raiz')}-{options.get('cnpj_raiz')}"]
            if options.get('cnpj'): # Add cnpj filter
                subset_rules.append("and")
                subset_rules.append(f"eq-{cols.get('cnpj')}-{options.get('cnpj')}")
            if options.get('id_pf'): # Add pf filter
                subset_rules.append("and")
                subset_rules.append(f"eq-{cols.get('pf')}-{options.get('id_pf')}")
            if options.get('column'): # Add timeframe filter
                subset_rules.append("and")
                subset_rules.append(f"eq-{cols.get('compet')}-{options.get('column')}")
            
            local_options = {
                "categorias": [cols.get('cnpj_raiz')],
                "agregacao": ['count'],
                "where": subset_rules,
                "theme": df
            }
            base_stats = json.loads(thematic_handler.find_dataset(local_options))
            result[df] = {
                "metadata": base_stats.get('metadata')
            }
            if base_stats.get('dataset',[]):
                result[df]["stats"] = base_stats.get('dataset')[0]

            local_options['as_pandas'] = True
            local_options['no_wrap'] = True

            result[df] = {**result[df], **self.get_grouped_stats(thematic_handler, local_options, cols)}
            if options.get('perspective') and thematic_handler.PERSP_VALUES.get(df):
                local_result = {}
                for each_persp_key, each_persp_value in thematic_handler.PERSP_VALUES.get(df):
                    local_options["where"].append(f"and")
                    local_options["where"].append(f"eq-{thematic_handler.PERSP_COLUMNS.get(df)}-{each_persp_value}")
                    local_result[each_persp] = self.get_grouped_stats(thematic_handler, local_options, cols) 
                result[df][f"stats_{each_persp}"] = {**result[df], **local_result}
        return result
        
    @staticmethod
    def get_grouped_stats(thematic_handler, options, cols):
        ''' Get stats for dataframe partitions '''
        result = {}

        # Get statistics partitioning by timeframe
        options["categorias"] = [cols.get('compet')]
        options["ordenacao"] = [f"-{cols.get('compet')}"]
        result["stats_compet"] = json.loads(
            thematic_handler.find_dataset(options).set_index(cols.get('compet')).to_json(orient="index")
        )
        
        # Get statistics partitioning by unit
        options["categorias"] = [cols.get('cnpj')]
        options["ordenacao"] = [cols.get('cnpj')]
        result["stats_estab"] = json.loads(
            thematic_handler.find_dataset(options).set_index(cols.get('cnpj')).to_json(orient="index")
        )

        # Get statistics partitioning by unit and timeframe
        options["categorias"] = [cols.get('cnpj'), cols.get('compet')]
        options["ordenacao"] = [f"-{cols.get('compet')}"]
        df_local_result = thematic_handler.find_dataset(options)
        df_local_result['idx'] = df_local_result[cols.get('compet')].apply(str) + \
            '_' + df_local_result[cols.get('cnpj')].apply(str)
        result["stats_estab_compet"] = json.loads(
            df_local_result.set_index('idx').to_json(orient="index")
        )
    
        ## RETIRADO pois a granularidade torna imviável a performance
        # metadata['stats_pf'] = dataframe[
        #     [col_pf_name, 'col_compet']
        # ].groupby(col_pf_name).describe(include='all')

        ## RETIRADO pois a granularidade torna inviável a performance
        # metadata['stats_pf_compet'] = dataframe[
        #     [col_pf_name, 'col_compet']
        # ].groupby(
        #     ['col_compet', col_cnpj_name]
        # ).describe(include='all')
        
        return result