''' Controller para fornecer dados das organizações de assistência social '''
from flask_restful_swagger_2 import swagger
from flask import Response
from resources.v1.empresa.empresa import EmpresaResource
from model.empresa.report import Report

class ReportResource(EmpresaResource):
    ''' Classe de múltiplas incidências '''
    @swagger.doc({
        'tags':['report'],
        'description':'Obtém o report gerado no Compliance',
        'parameters': EmpresaResource.CUSTOM_SWAGGER_PARAMS,
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
            return '', rsp_code[content['status']]
        return Response(content, mimetype='text/html')

    @swagger.doc({
        'tags':['report'],
        'description': 'Envia CNPJ RAIZ para a fila de processamento do report.',
        'parameters': EmpresaResource.CUSTOM_SWAGGER_PARAMS,
        'responses': {
            '201': {'description': 'Report'}
        }
    })
    def post(self, cnpj_raiz):
        ''' Envia para a fila do Kafka '''
        try:
            return self.__get_domain().generate(cnpj_raiz), 202
        except TimeoutError:
            return "Falha na gravação do dicionário", 504
        except (AttributeError, KeyError, ValueError) as err:
            return str(err), 500

    def __get_domain(self):
        ''' Carrega o modelo de domínio, se não o encontrar '''
        if self.domain is None:
            self.domain = Report()
        return self.domain

    def __set_domain(self):
        ''' Domain setter, called from constructor '''
        self.domain = Report()
