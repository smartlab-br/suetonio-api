''' Controller para fornecer dados das organizações de assistência social '''
import requests
from flask import request
from flask_restful_swagger_2 import swagger
from resources.base import BaseResource
from model.empresa.empresa import Empresa

class EstabelecimentoResource(BaseResource):
    ''' Classe de múltiplas incidências '''
    def __init__(self):
        ''' Construtor'''
        self.domain = Empresa()

    @swagger.doc({
        'tags':['empresa'],
        'description':'Obtém todos os registros de um único estabelecimento',
        'parameters':[
            {
                "name": "cnpj",
                "description": "CNPJ do estabelecimento consultado",
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
                "description": "Competência a ser retornada. Depende da fonte de dados (ex. para uma fonte pode ser AAAA, enquanto para outras AAAAMM)",
                "required": False,
                "type": 'string',
                "in": "query"
            },
            {
                "name": "id_pf",
                "description": "Identificador da Pessoa Física, dentro do estabelecimento. Tem que informar o dataset (param 'dados')",
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
            }
        ],
        'responses': {
            '200': {
                'description': 'Todos os datasets da empresa'
            }
        }
    })
    def get(self, cnpj):
        ''' Obtém todos os datasets da empresa '''
        options = request.args.copy()
        options['id_inv'] = cnpj
        options = self.build_person_options(options, mod='estabelecimento')
        
        result = self.__get_domain().find_datasets(options)
        if 'invalid' in result:
            del result['invalid']
            return result, 202
        else:
            return result

    def __get_domain(self):
        ''' Carrega o modelo de domínio, se não o encontrar '''
        if self.domain is None:
            self.domain = Empresa()
        return self.domain
