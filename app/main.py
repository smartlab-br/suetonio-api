"""API Base """
import os
from flask_cors import CORS
from flask_restful_swagger_2 import Api
from flask import Flask
from flask import g

import logging
import sys

from service.request_handler import FLPORequestHandler

from resources.v1.municipio import MunicipiosResource, MunicipioResource

from resources.v1.empresa.empresa import EmpresaResource
from resources.v1.empresa.stats import EmpresaStatsResource
from resources.v1.empresa.estabelecimento import EstabelecimentoResource

from resources.v1.empresa.datasets import DatasetsResource

from resources.v1.empresa.report import ReportResource

# Endpoints genéricos de temáticos
from resources.v1.thematic import ThematicResource

from resources.v1.healthchecks import HCAlive, HCReady

CONFIG = {
    "dev": "config.dev.DevelopmentConfig",
    "prod": "config.prod.ProductionConfig",
    "staging": "config.staging.StagingConfig",
}

application = Flask(__name__, static_folder='static', static_url_path='') #pylint: disable=C0103
CONFIG_NAME = os.getenv('FLASK_CONFIGURATION', 'dev')
application.config.from_object(CONFIG[CONFIG_NAME])

@application.teardown_appcontext
def close_db_connection(_error):
    ''' Cleanup on application crash '''
    # Encerra a conexão com o impala
    if hasattr(g, 'impala_connection'):
        g.impala_connection.close()
        g.impala_connection = None
    # Encerra a conexão com o redis
    if hasattr(g, 'redis_pool'):
        del g.redis_pool
        g.redis_pool = None

CORS = CORS(application, resources={r"/*": {"origins": "*"}})
api = Api(application, api_version='0.1', api_spec_url='/api/swagger') #pylint: disable=C0103

api.add_resource(HCAlive, '/hcalive')
api.add_resource(HCReady, '/hcready')

api.add_resource(DatasetsResource, '/datasets')
api.add_resource(ReportResource, '/report/<string:cnpj_raiz>')

api.add_resource(MunicipiosResource, '/municipios')
api.add_resource(MunicipioResource, '/municipio/<int:cd_municipio_ibge>')

# Endpoints para obter datasets de empresa e estabelecimento
api.add_resource(EmpresaResource, '/empresa/<string:cnpj_raiz>')
api.add_resource(EmpresaStatsResource, '/estatisticas/empresa/<string:cnpj_raiz>')
api.add_resource(EstabelecimentoResource, '/estabelecimento/<string:cnpj>')

## Endpoints temáticos
# Endpoint temático genérico
api.add_resource(ThematicResource, '/thematic/<string:theme>')

if __name__ == '__main__':
    # logging.basicConfig(filename='werkzeug.log', level=logging.ERROR)
    print(application.config)
    logging.basicConfig(stream=sys.stderr, level=application.config.get('LOG_LEVEL'))
    LOGGER = logging.getLogger('werkzeug')
    LOGGER.setLevel(logging.ERROR)
    application.run(request_handler=FLPORequestHandler)
