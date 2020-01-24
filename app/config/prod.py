''' Configuration loader for production environment '''
import os
from kazoo.client import KazooClient

class ProductionConfig():
    ''' Configuration handler '''
    zk = KazooClient(hosts=os.getenv('ZOOKEEPER_HOST') + ':' + os.getenv('ZOOKEEPER_PORT'))
    zk.start()
    data, stat = zk.get("/spai/suetonio-api/prod/hive_host")
    HIVE_HOST = data.decode("utf-8")
    data, stat = zk.get("/spai/suetonio-api/prod/hive_port")
    HIVE_PORT = data.decode("utf-8")
    data, stat = zk.get("/spai/suetonio-api/prod/hive_user")
    HIVE_USER = data.decode("utf-8")
    data, stat = zk.get("/spai/suetonio-api/prod/hive_pwd")
    HIVE_PWD = data.decode("utf-8")
    data, stat = zk.get("/spai/suetonio-api/prod/impala_host")
    IMPALA_HOST = data.decode("utf-8")
    data, stat = zk.get("/spai/suetonio-api/prod/impala_port")
    IMPALA_PORT = data.decode("utf-8")
    data, stat = zk.get("/spai/suetonio-api/prod/impala_user")
    IMPALA_USER = data.decode("utf-8")
    data, stat = zk.get("/spai/suetonio-api/prod/impala_pwd")
    IMPALA_PWD = data.decode("utf-8")

    data, stat = zk.get("/spai/suetonio-api/prod/hbase_host")
    HBASE_HOST = data.decode("utf-8")
    data, stat = zk.get("/spai/suetonio-api/prod/hbase_port")
    HBASE_PORT = data.decode("utf-8")

    data, stat = zk.get("/spai/suetonio-api/prod/redis_host")
    REDIS_HOST = data.decode("utf-8")
    data, stat = zk.get("/spai/suetonio-api/prod/redis_port")
    REDIS_PORT = data.decode("utf-8")
    data, stat = zk.get("/spai/suetonio-api/prod/redis_db")
    REDIS_DB = data.decode("utf-8")
    # data, stat = zk.get("/spai/suetonio-api/prod/redis_user")
    # REDIS_USER = data.decode("utf-8")
    # data, stat = zk.get("/spai/suetonio-api/prod/redis_pwd")
    # REDIS_PWD = data.decode("utf-8")

    data, stat = zk.get("/spai/suetonio-api/prod/kafka_host")
    KAFKA_HOST = data.decode("utf-8")
    data, stat = zk.get("/spai/suetonio-api/prod/kafka_port")
    KAFKA_PORT = data.decode("utf-8")
    data, stat = zk.get("/spai/suetonio-api/prod/kafka_schema")
    KAFKA_SCHEMA = data.decode("utf-8")
    data, stat = zk.get("/spai/suetonio-api/prod/kafka_topic_prefix")
    KAFKA_TOPIC_PREFIX = data.decode("utf-8")

    data, stat = zk.get("/spai/suetonio-api/prod/git_viewconf_url")
    GIT_VIEWCONF_BASE_URL = data.decode("utf-8")

    zk.stop()
    zk = None
    data = None
    stat = None
