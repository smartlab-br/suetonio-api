''' Repository para recuperar informações de uma empresa '''
import base64
from kafka import KafkaProducer
from flask import current_app
from repository.base import RedisRepository

#pylint: disable=R0903
class ReportRepository(RedisRepository):
    ''' Definição do repo '''
    def find_report(self, cnpj_raiz):
        ''' Localiza o report no REDIS '''
        return self.get_dao().get(self.REDIS_KEY.format(cnpj_raiz))

    def store(self, cnpj_raiz):
        ''' Inclui cnpj raiz no tópico do kafka '''
        kafka_server = f'{current_app.config["KAFKA_HOST"]}:{current_app.config["KAFKA_PORT"]}'
        producer = KafkaProducer(bootstrap_servers=[kafka_server])
        # Then publishes to Kafka
        producer.send("polaris-compliance-input-report", bytes(cnpj_raiz, 'utf-8'))
        producer.close()
