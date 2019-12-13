''' Repository para recuperar informações de uma empresa '''
from repository.base import HBaseRepository
import json
# import pandas as pd

#pylint: disable=R0903
class EmpresaRepository(HBaseRepository):
    ''' Definição do repo '''
    TABLE = 'sue'
    CNPJ_COLUMNS = {
        'auto': 'nrinscricao',
        'caged': 'cnpj_cei',
        'rais': 'nu_cnpj_cei',
        'rfb': 'nu_cnpj'
    } # Dados que possuem nomes diferentes para a coluna de cnpj
    PF_COLUMNS = {
        'catweb': 'nu_nit',
        'rais': 'nu_cpf',
        'rfb': 'nu_cpf_responsavel',
        'rfbsocios': 'cnpj_cpf_socio',
        'rfbparticipacaosocietaria': 'cnpj_cpf_socio',
    } # Dados que possuem nomes diferentes para a coluna de identificação da Pessoa Física
    PERSP_COLUMNS = { # Colunas que indicam diferentes perspectivas em um mesmo dataset
        'catweb': 'origem'
    } 
    SIMPLE_COLUMNS = {}

    def find_datasets(self, cnpj_raiz, column_family, column, cnpj=None, id_pf=None, simplified=False, perspective=None):
        ''' Localiza um município pelo código do IBGE '''
        # Performance evaluation timestamp baseline
        # from datetime import datetime
        # import dateutil.relativedelta
        # ts_init = datetime.now()

        if cnpj_raiz is not None:
            # return self.find_row(self.TABLE, cnpj_raiz)
            result = self.find_row('empresa', cnpj_raiz, column_family, column)
            metadata = {}

            for ds_key in result:
                col_cnpj_name = 'cnpj'
                if ds_key in self.CNPJ_COLUMNS:
                    col_cnpj_name = self.CNPJ_COLUMNS[ds_key]
                col_pf_name = None
                if ds_key in self.PF_COLUMNS:
                    col_pf_name = self.PF_COLUMNS[ds_key]

                if not result[ds_key].empty:
                    # Filtrar cnpj e id_pf nos datasets pandas
                    if cnpj is not None and id_pf is not None and col_pf_name is not None:
                        if result[ds_key][col_cnpj_name].dtype == 'int64':
                            cnpj = int(cnpj)
                        if result[ds_key][col_pf_name].dtype == 'int64':
                            id_pf = int(id_pf)
                        result[ds_key] = result[ds_key][(result[ds_key][col_cnpj_name] == cnpj) & (result[ds_key][col_pf_name] == id_pf)]
                    # Filtrar apenas cnpj nos datasets pandas
                    elif cnpj is not None:
                        if result[ds_key][col_cnpj_name].dtype == 'int64':
                            cnpj = int(cnpj)
                        result[ds_key] = result[ds_key][result[ds_key][col_cnpj_name] == cnpj]
                    # Filtrar apenas id_pf nos datasets pandas
                    elif id_pf is not None and col_pf_name is not None:
                        if result[ds_key][col_pf_name].dtype == 'int64':
                            id_pf = int(id_pf)
                        result[ds_key] = result[ds_key][result[ds_key][col_pf_name] == id_pf]

                    if perspective is not None and ds_key in self.PERSP_COLUMNS:
                        result[ds_key] = result[ds_key][result[ds_key][self.PERSP_COLUMNS[ds_key]] == perspective]

                    if not result[ds_key].empty: # Not empty after filters
                        # Redução de dimensionalidade (simplified)
                        if simplified:
                            list_dimred = ['nu_cnpj_cei', 'nu_cpf', 'col_compet']
                            if ds_key in self.SIMPLE_COLUMNS:
                                list_dimred = self.SIMPLE_COLUMNS[ds_key]
                                # Garantir que col_compet sempre estará na lista
                                if 'col_compet' not in list_dimred:
                                    list_dimred.append('col_compet') 
                            result[ds_key] = result[ds_key][list_dimred]

                        # Captura de metadados
                        metadata[ds_key] = {}
                        metadata[ds_key]['stats'] = json.loads(result[ds_key].describe(include='all').to_json())

                        stats_estab = result[ds_key].groupby(col_cnpj_name).describe(include='all')
                        stats_estab.columns = ["_".join(col).strip() for col in stats_estab.columns.values]
                        metadata[ds_key]['stats_estab'] = json.loads(stats_estab.reset_index().to_json())
                        ## RETIRADO pois a granularidade torna imviável a performance
                        # metadata['stats_pf'] = result[ds_key][[col_pf_name, 'col_compet']].groupby(col_pf_name).describe(include='all')
                        stats_estab_compet = result[ds_key].groupby(['col_compet', col_cnpj_name]).describe(include='all')
                        stats_estab_compet.columns = ["_".join(col).strip() for col in stats_estab_compet.columns.values]
                        metadata[ds_key]['stats_estab_compet'] = json.loads(stats_estab_compet.reset_index().to_json())
                        ## RETIRADO pois a granularidade torna imviável a performance
                        # metadata['stats_pf_compet'] = result[ds_key][[col_pf_name, 'col_compet']].groupby(['col_compet', col_cnpj_name]).describe(include='all')
                    else: # Empty after filters
                        # Captura de metadados
                        metadata[ds_key] = {}
                        metadata[ds_key]['stats'] = json.loads(result[ds_key].describe(include='all').to_json())    
                else:
                    # Captura de metadados
                    metadata[ds_key] = {}
                    metadata[ds_key]['stats'] = json.loads(result[ds_key].describe(include='all').to_json())

                # Conversão dos datasets em json
                result[ds_key] = json.loads(result[ds_key].to_json(orient="records"))

            # ts_finish = datetime.now()
            # rd = dateutil.relativedelta.relativedelta (ts_finish, ts_init)
            # print("%d years, %d months, %d days, %d hours, %d minutes and %d seconds" % (rd.years, rd.months, rd.days, rd.hours, rd.minutes, rd.seconds))

            return (result, metadata)
