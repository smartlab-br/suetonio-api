''' Controller para fornecer dados das organizações de assistência social '''
from flask_restful_swagger_2 import swagger
from flask import request
from resources.v1.empresa.empresa import EmpresaResource
from model.empresa.empresa import Empresa

class EstabelecimentoResource(EmpresaResource):
    ''' Classe de múltiplas incidências '''
    CUSTOM_SWAGGER_PARAMS = [
        {
            "name": "cnpj",
            "description": "CNPJ do estabelecimento consultado",
            "required": True,
            "type": 'string',
            "in": "path"
        }
    ]
    def __init__(self):
        ''' Construtor'''
        self.domain = Empresa()

    @swagger.doc({
        'tags':['empresa'],
        'description':'Obtém todos os registros de um único estabelecimento',
        'parameters': CUSTOM_SWAGGER_PARAMS + EmpresaResource.DEFAULT_SWAGGER_PARAMS,
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
        options = self.build_person_options(options, 'estabelecimento')

        result = self.__get_domain().find_datasets(options)
        if 'invalid' in result:
            del result['invalid']
            return result, 202
        return result

    def __get_domain(self):
        ''' Carrega o modelo de domínio, se não o encontrar '''
        if self.domain is None:
            self.domain = Empresa()
        return self.domain
