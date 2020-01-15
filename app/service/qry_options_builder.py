''' Service para conversão de atributos de query para os objetos do fetch '''
class QueryOptionsBuilder():
    ''' Classe de serviço '''
    @classmethod
    def build_options(cls, r_args):
        ''' Constrói as opções da pesquisa '''
        categorias = cls.extract_qry_param(r_args, 'categorias')
        if categorias is None:
            raise ValueError('Categories required')

        filtros = None
        if r_args.get('filtros') is not None and r_args.get('filtros'):
            filtros = r_args.get('filtros').replace('\\,', '|')
            filtros = filtros.split(',')
            filtros = [f.replace('|', ',') for f in filtros]

        return {
            "categorias": categorias,
            "valor": cls.extract_qry_param(r_args, 'valor'),
            "agregacao": cls.extract_qry_param(r_args, 'agregacao'),
            "ordenacao": cls.extract_qry_param(r_args, 'ordenacao'),
            "where": filtros,
            "pivot": cls.extract_qry_param(r_args, 'pivot'),
            "limit": r_args.get('limit'),
            "offset": r_args.get('offset'),
            "calcs": cls.extract_qry_param(r_args, 'calcs'),
            "partition": cls.extract_qry_param(r_args, 'partition')
        }

    @staticmethod
    def extract_qry_param(params, param_name):
        ''' Extracts the given query param as an array of values '''
        if (param_name in params.keys() and
                params.get(param_name) is not None and
                params.get(param_name)):
            return params.get(param_name).split(',')
        return None

    @classmethod
    def build_person_options(cls, r_args, mod='empresa'):
        ''' Constrói as opções da pesquisa '''
        options = {}

        if 'dados' in r_args:
            options['column_family'] = r_args['dados']
        if 'competencia' in r_args:
            options['column'] = r_args['competencia']
        if 'id_pf' in r_args:
            options['id_pf'] = r_args['id_pf']
        if 'perspectiva' in r_args:
            options['perspective'] = r_args['perspectiva']
        if 'only_meta' in r_args and r_args['only_meta'] == 'S':
            options['only_meta'] = True
        if 'reduzido' in r_args and r_args['reduzido'] == 'S':
            options['reduzido'] = True

        if mod == 'estabelecimento':
            options['cnpj_raiz'] = r_args['id_inv'][:-6]
            options['cnpj'] = r_args['id_inv']
        else:
            options['cnpj_raiz'] = r_args['id_inv']

        return options