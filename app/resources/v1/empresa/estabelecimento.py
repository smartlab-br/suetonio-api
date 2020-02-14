''' Controller para fornecer dados das organizações de assistência social '''
from flask_restful_swagger_2 import swagger
from flask import request
from resources.v1.empresa.empresa import EmpresaResource

#pylint: disable=W0221
class EstabelecimentoResource(EmpresaResource):
    ''' Classe de múltiplas incidências '''
    CUSTOM_SWAGGER_PARAMS = [
        {
            "name": "cnpj", "required": True, "type": 'string', "in": "path",
            "description": "CNPJ do estabelecimento consultado"
        }
    ]

    @swagger.doc({
        'tags':['empresa'],
        'description':'Obtém todos os registros de um único estabelecimento',
        'parameters': CUSTOM_SWAGGER_PARAMS + EmpresaResource.DEFAULT_SWAGGER_PARAMS,
        'responses': {
            '200': {
                'description': 'Todos os datasets do estabelecimento'
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
        return result
