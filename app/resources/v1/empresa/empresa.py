''' Controller para fornecer dados das organizações de assistência social '''
from flask_restful_swagger_2 import swagger
from flask import request
from resources.base import BaseResource
from model.empresa.empresa import Empresa

class EmpresaResource(BaseResource):
    ''' Classe de múltiplas incidências '''
    def __init__(self):
        ''' Construtor'''
        self.domain = Empresa()

    @swagger.doc({
        'tags':['empresa'],
        'description':'Obtém todos os registros de uma única empresa',
        'parameters':[
            {
                "name": "cnpj_raiz",
                "description": "CNPJ Raiz da empresa consultada",
                "required": True,
                "type": 'string',
                "in": "path"
            },
            {
                "name": "dados",
                "description": "Fonte de dados para consulta (rais, caged, catweb etc)",
                "required": False,
                "type": 'string',
                "in": "query"
            },
            {
                "name": "competencia",
                "description": "Competência a ser retornada. Depende da fonte de dados \
                    (ex. para uma fonte pode ser AAAA, enquanto para outras AAAAMM)",
                "required": False,
                "type": 'string',
                "in": "query"
            },
            {
                "name": "id_pf",
                "description": "Identificador da Pessoa Física, dentro da empresa. \
                    Tem que informar o dataset (param 'dados')",
                "required": False,
                "type": 'string',
                "in": "query"
            },
            {
                "name": "only_meta",
                "description": "Sinalizador que indica apenas o retorno dos metadados (S para sim)",
                "required": False,
                "type": 'string',
                "in": "query"
            },
            {
                "name": "reduzido",
                "description": "Sinalizador que indica conjunto reduzido de colunas (S para sim)",
                "required": False,
                "type": 'string',
                "in": "query"
            },
            {
                "name": "perspectiva",
                "description": "Valor que filtra uma perspectiva predefinida de um dataset \
                    (ex. No catweb, 'Empregador'). Nem todos os datasets tem essa opção.",
                "required": False,
                "type": 'string',
                "in": "query"
            }
        ],
        'responses': {
            '200': {
                'description': 'Todos os datasets da empresa'
            }
        }
    })
    def get(self, cnpj_raiz):
        ''' Obtém todos os datasets da empresa '''
        options = request.args.copy()
        options['id_inv'] = cnpj_raiz
        options = self.build_person_options(options)

        result = self.__get_domain().find_datasets(options)
        if 'invalid' in result:
            del result['invalid']
            return result, 202
        return result

    @swagger.doc({
        'tags':['empresa'],
        'description':'Insere uma empresa na fila de análises.',
        'parameters':[
            {
                "name": "cnpj_raiz",
                "description": "CNPJ Raiz da empresa consultada",
                "required": True,
                "type": 'string',
                "in": "path"
            },
        ],
        'responses': {
            '201': {'description': 'Empresa'}
        }
    })
    def post(self, cnpj_raiz):
        ''' Requisita uma nova análise de uma empresa '''
        try:
            self.__get_domain().produce(cnpj_raiz)
            return 'Análise em processamento', 201
        except TimeoutError:
            return "Não foi possível incluir a análise na fila. Tente novamente mais tarde", 504
        except (AttributeError, KeyError, ValueError) as err:
            return str(err), 400

    def __get_domain(self):
        ''' Carrega o modelo de domínio, se não o encontrar '''
        if self.domain is None:
            self.domain = Empresa()
        return self.domain
