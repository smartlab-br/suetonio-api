''' Repository para recuperar informações de uma empresa '''
from kafka import KafkaProducer
from flask import current_app
from repository.base import RedisRepository

#pylint: disable=R0903
class ReportRepository(RedisRepository):
    ''' Definição do repo '''
    REDIS_KEY = 'rmd:{}'
    REDIS_STATUS_KEY = 'rmd:st:{}'

    def find_report(self, cnpj_raiz):
        ''' Localiza o report no REDIS '''
        print(self.get_dao().get(self.REDIS_STATUS_KEY.format(cnpj_raiz)))
        report = self.get_dao().get(self.REDIS_KEY.format(cnpj_raiz))
        # If no report is found, checks REDIS status
        if report is None or report == '':
            redis_report_status = self.get_dao().get(self.REDIS_STATUS_KEY.format(cnpj_raiz))
            if redis_report_status is not None and redis_report_status != '':
                if redis_report_status == 'PROCESSING':
                    # When there's a no success status in REDIS (PROCESSING, FAILED), returns status
                    return {'status': redis_report_status}
                if redis_report_status == 'FAILED':
                    # If failed, produces report item in Kafka an sends back the failed status
                    self.store(cnpj_raiz)
                    return {'status': redis_report_status}
            # In any other case, responds as not found
            self.store(cnpj_raiz)
            return {'status': "NOTFOUND"}
        return report

    def store(self, cnpj_raiz):
        ''' Inclui cnpj raiz no tópico do kafka '''
        kafka_server = f'{current_app.config["KAFKA_HOST"]}:{current_app.config["KAFKA_PORT"]}'
        producer = KafkaProducer(bootstrap_servers=[kafka_server])
        # Restart status from REDIS
        self.get_dao().set(self.REDIS_STATUS_KEY.format(cnpj_raiz), "PROCESSING")
        # Removes old report from REDIS
        self.get_dao().delete(self.REDIS_KEY.format(cnpj_raiz))
        # Then publishes to Kafka
        producer.send("polaris-compliance-input-report", bytes(cnpj_raiz, 'utf-8'))
        producer.close()
