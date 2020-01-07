import os

class DevelopmentConfig(object):
    HIVE_HOST = os.getenv('HIVE_HOST')
    HIVE_PORT = os.getenv('HIVE_PORT')
    HIVE_USER = os.getenv('HIVE_USER')
    HIVE_PWD = os.getenv('HIVE_PWD')
    IMPALA_HOST = os.getenv('IMPALA_HOST')
    IMPALA_PORT = os.getenv('IMPALA_PORT')
    IMPALA_USER = os.getenv('IMPALA_USER')
    IMPALA_PWD = os.getenv('IMPALA_PWD')

    GIT_VIEWCONF_BASE_URL = os.getenv('GIT_VIEWCONF_BASE_URL')

    HBASE_HOST = os.getenv('HBASE_HOST')
    HBASE_PORT = os.getenv('HBASE_PORT')

    KAFKA_HOST = os.getenv('KAFKA_HOST')
    KAFKA_PORT = os.getenv('KAFKA_PORT')
    KAFKA_SCHEMA = os.getenv('KAFKA_SCHEMA')
    KAFKA_TOPIC_PREFIX = os.getenv('KAFKA_TOPIC_PREFIX')