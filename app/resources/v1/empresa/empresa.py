''' Controller para fornecer dados das organizações de assistência social '''
import requests
from flask import request
from flask_restful_swagger_2 import swagger
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
                "description": "Competência a ser retornada. Depende da fonte de dados (ex. para uma fonte pode ser AAAA, enquanto para outras AAAAMM)",
                "required": False,
                "type": 'string',
                "in": "query"
            },
            {
                "name": "id_pf",
                "description": "Identificador da Pessoa Física, dentro da empresa. Tem que informar o dataset (param 'dados')",
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
                "description": "Valor que filtra uma perspectiva predefinida de um dataset (ex. No catweb, 'Empregador'). Nem todos os datasets tem essa opção.",
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
        column_family = None
        if 'dados' in request.args:
            column_family = request.args['dados']
        column = None
        if 'competencia' in request.args:
            column = request.args['competencia']
        id_pf = None
        if 'id_pf' in request.args:
            id_pf = request.args['id_pf']
        perspective = None
        if 'perspectiva' in request.args:
            perspective = request.args['perspectiva']
        only_meta = False
        if 'only_meta' in request.args and request.args['only_meta'] == 'S':
            only_meta = True
        reduzido = False
        if 'reduzido' in request.args and request.args['reduzido'] == 'S':
            reduzido = True
        try:
            return self.__get_domain().find_datasets(cnpj_raiz, column_family, column, id_pf=id_pf, only_meta=only_meta, simplified=reduzido, perspective=perspective)
        except requests.exceptions.HTTPError as e:
            # Whoops it wasn't a 200
            if e.response.status_code == 404:
                return "Not found", 404
            else:
                return "Error fetching data", e.response.status_code
    def __get_domain(self):
        ''' Carrega o modelo de domínio, se não o encontrar '''
        if self.domain is None:
            self.domain = Empresa()
        return self.domain
