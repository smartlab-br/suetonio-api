'''Classes de health check'''
from flask_restful import Resource

class HCAlive(Resource):
    ''' Classe para health check '''
    @classmethod
    def get(cls):
        ''' Metodo-base para health-check '''
        return {'message': 'OK'}

class HCReady(Resource):
    ''' Classe para health check da pilha '''
    @classmethod
    def get(cls):
        ''' Metodo-base para health-check '''
        raise ValueError('Dataset inválido')