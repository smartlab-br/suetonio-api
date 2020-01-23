''' Controller para fornecer dados das organizações de assistência social '''
import requests
from flask import request
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
            },
        ],
        'responses': {
            '200': {
                'description': 'Report (base-64)'
            }
        }
    })
    def get(self, cnpj_raiz):
        ''' Obtém o report '''
        return self.__get_domain().find_report(cnpj_raiz)

    def __get_domain(self):
        ''' Carrega o modelo de domínio, se não o encontrar '''
        if self.domain is None:
            self.domain = Report()
        return self.domain
