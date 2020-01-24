''' Controller para fornecer dados das organizações de assistência social '''
import requests
from flask import request, Response
from flask_restful_swagger_2 import swagger
from resources.base import BaseResource
from model.empresa.report import Report

class ReportResource(BaseResource):
    ''' Classe de múltiplas incidências '''
    def __init__(self):
        ''' Construtor'''
        self.domain = Report()

    @swagger.doc({
        'tags':['report'],
        'description':'Obtém o report gerado no Compliance',
        'parameters':[
            {
                "name": "cnpj_raiz",
                "description": "CNPJ Raiz da empresa consultada",
                "required": True,
                "type": 'string',
                "in": "path"
            }
        ],
        'responses': {
            '200': {
                'description': 'Report (base-64)'
            }
        }
    })
    def get(self, cnpj_raiz):
        ''' Obtém o report '''
        content = self.__get_domain().find_report(cnpj_raiz)
        rsp_code = {'FAILED': 201, 'PROCESSING': 204, 'NOTFOUND': 201}
        if isinstance(content, dict):
            return '', resp_code[content['status']]
        return Response(content, mimetype='text/html')

    @swagger.doc({
        'tags':['report'],
        'description':'Envia CNPJ RAIZ para a fila de processamento do report.',
        'parameters':[
            {
                "name": "cnpj_raiz",
                "description": "CNPJ Raiz da empresa consultada",
                "required": True,
                "type": 'string',
                "in": "path"
            }
        ],
        'responses': {
            '201': {'description': 'Report'}
        }
    })
    def post(self, cnpj_raiz):
        ''' Envia para a fila do Kafka '''
        try:
            return self.__get_domain().generate(cnpj_raiz), 201
        except TimeoutError:
            return "Falha na gravação do dicionário", 504
        except (AttributeError, KeyError, ValueError) as err:
            return str(err), 500

    def __get_domain(self):
        ''' Carrega o modelo de domínio, se não o encontrar '''
        if self.domain is None:
            self.domain = Report()
        return self.domain
