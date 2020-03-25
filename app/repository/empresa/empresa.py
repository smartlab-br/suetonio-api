''' Repository para recuperar informações de uma empresa '''
import json
from repository.base import HBaseRepository

#pylint: disable=R0903
class EmpresaRepository(HBaseRepository):
    ''' Definição do repo '''
    TABLE = 'sue'
    CNPJ_COLUMNS = {
        'aeronaves': 'proprietario_cpfcnpj',
        'auto': 'nrinscricao',
        'caged': 'cnpj_cei',
        'cagedsaldo': 'cnpj_cei',
        'rais': 'nu_cnpj_cei',
        'renavam': 'proprietario_cpfcnpj',
        'rfb': 'nu_cnpj',
        'sisben': 'nu_cnpj'
    } # Dados que possuem nomes diferentes para a coluna de cnpj
    PF_COLUMNS = {
        'aeronaves': 'proprietario_cpfcnpj',
        'catweb': 'nu_nit',
        'rais': 'nu_cpf',
        'renavam': 'proprietario_cpfcnpj',
        'rfb': 'nu_cpf_responsavel',
        'rfbsocios': 'cnpj_cpf_socio',
        'rfbparticipacaosocietaria': 'cnpj_cpf_socio'
    } # Dados que possuem nomes diferentes para a coluna de identificação da Pessoa Física
    PERSP_COLUMNS = { # Colunas que indicam diferentes perspectivas em um mesmo dataset
        'catweb': 'origem_busca'
    }
    PERSP_VALUES = {
        'catweb': {
            'empregador': 'Empregador',
            'tomador': 'Tomador',
            'empregador_concessao': 'Empregador Concessão',
            'empregador_aeps': 'Empregador AEPS'
        }
    }
    SIMPLE_COLUMNS = {}

    def find_datasets(self, options):
        ''' Localiza um município pelo código do IBGE '''
        if 'cnpj_raiz' in options and options['cnpj_raiz'] is not None:
            result = self.find_row(
                'empresa',
                options['cnpj_raiz'],
                options.get('column_family'),
                options.get('column')
            )
            metadata = {}

            # Result splitting according to perspectives
            nu_results = {}
            for ds_key in result:
                if not result[ds_key].empty and ds_key in self.PERSP_COLUMNS:
                    for nu_persp_key, nu_persp_val in self.PERSP_VALUES[ds_key].items():
                        if options.get('perspective', nu_persp_key) == nu_persp_key:
                            nu_key = ds_key + "_" + nu_persp_key
                            nu_results[nu_key] = result[ds_key][
                                result[ds_key][self.PERSP_COLUMNS[ds_key]] == nu_persp_val
                            ]
            result = {**result, **nu_results}

            for ds_key in result:
                col_cnpj_name = 'cnpj'
                if ds_key in self.CNPJ_COLUMNS:
                    col_cnpj_name = self.CNPJ_COLUMNS[ds_key]
                col_pf_name = None
                if ds_key in self.PF_COLUMNS:
                    col_pf_name = self.PF_COLUMNS[ds_key]

                if not result[ds_key].empty:
                    result[ds_key] = self.filter_by_person(
                        result[ds_key], options, col_cnpj_name, col_pf_name
                    )

                # Redução de dimensionalidade (simplified)
                if not result[ds_key].empty and options.get('simplified'):
                    list_dimred = ['nu_cnpj_cei', 'nu_cpf', 'col_compet']
                    if ds_key in self.SIMPLE_COLUMNS:
                        list_dimred = self.SIMPLE_COLUMNS[ds_key]
                        # Garantir que col_compet sempre estará na lista
                        if 'col_compet' not in list_dimred:
                            list_dimred.append('col_compet')
                    result[ds_key] = result[ds_key][list_dimred]

                # Captura de metadados
                metadata[ds_key] = self.get_metadata(result[ds_key], col_cnpj_name)

                # Conversão dos datasets em json
                result[ds_key] = json.loads(result[ds_key].to_json(orient="records"))

            return (result, metadata)
        return (None, None)

    @staticmethod
    def filter_by_person(dataframe, options, col_cnpj_name, col_pf_name):
        ''' Filter dataframe by person identification, according to options data '''
        cnpj = options.get('cnpj')
        id_pf = options.get('id_pf')
        # Filtrar cnpj e id_pf nos datasets pandas
        if cnpj is not None and id_pf is not None and col_pf_name is not None:
            if dataframe[col_cnpj_name].dtype == 'int64':
                cnpj = int(cnpj)
            if dataframe[col_pf_name].dtype == 'int64':
                id_pf = int(id_pf)
            dataframe = dataframe[
                (dataframe[col_cnpj_name] == cnpj) & (dataframe[col_pf_name] == id_pf)
            ]
        # Filtrar apenas cnpj nos datasets pandas
        elif cnpj is not None:
            if dataframe[col_cnpj_name].dtype == 'int64':
                cnpj = int(cnpj)
            dataframe = dataframe[dataframe[col_cnpj_name] == cnpj]
        # Filtrar apenas id_pf nos datasets pandas
        elif (id_pf is not None and col_pf_name is not None):
            if dataframe[col_pf_name].dtype == 'int64':
                id_pf = int(id_pf)
            dataframe = dataframe[dataframe[col_pf_name] == id_pf]
        return dataframe

    @staticmethod
    def get_metadata(dataframe, col_cnpj_name):
        ''' Captura metadados de um dataframe '''
        if dataframe is None:
            return None

        result = {
            'stats': json.loads(dataframe.describe(include='all').to_json(orient="index"))
        }

        if dataframe.empty:
            return result

        stats_estab = dataframe.groupby(col_cnpj_name).describe(include='all')
        stats_estab.columns = [
            "_".join(col).strip()
            for
            col
            in
            stats_estab.columns.values
        ]
        result['stats_estab'] = json.loads(
            stats_estab.to_json(orient="index")
        )

        stats_compet = dataframe.groupby('col_compet').describe(include='all')
        stats_compet.columns = [
            "_".join(col).strip() for col in stats_compet.columns.values
        ]
        result['stats_compet'] = json.loads(
            stats_compet.to_json(orient="index")
        )

        ## RETIRADO pois a granularidade torna imviável a performance
        # metadata['stats_pf'] = dataframe[
        #     [col_pf_name, 'col_compet']
        # ].groupby(col_pf_name).describe(include='all')

        stats_estab_compet = dataframe.groupby(
            ['col_compet', col_cnpj_name]
        ).describe(include='all')
        stats_estab_compet.columns = [
            "_".join(col).strip() for col in stats_estab_compet.columns.values
        ]
        stats_estab_compet = stats_estab_compet.reset_index()
        stats_estab_compet['idx'] = stats_estab_compet['col_compet'].apply(str) + \
            '_' + stats_estab_compet[col_cnpj_name].apply(str)
        stats_estab_compet = stats_estab_compet.set_index('idx')
        result['stats_estab_compet'] = json.loads(
            stats_estab_compet.to_json(orient="index")
        )

        ## RETIRADO pois a granularidade torna inviável a performance
        # metadata['stats_pf_compet'] = dataframe[
        #     [col_pf_name, 'col_compet']
        # ].groupby(
        #     ['col_compet', col_cnpj_name]
        # ).describe(include='all')

        return result
