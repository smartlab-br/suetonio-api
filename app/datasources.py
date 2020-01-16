''' Gerenciamento de conexões com fontes de dados '''
from flask import g
from flask import current_app

def get_hive_connection():
    ''' Gerencia a conexão com o hive '''
    from pyhive import hive
    if not hasattr(g, 'hive_connection'):
        g.hive_connection = hive.connect(
            host=current_app.config['HIVE_HOST'],
            port=int(current_app.config['HIVE_PORT']),
            database='spai_druid',
            username=current_app.config['HIVE_USER']
        )
    return g.hive_connection

def get_impala_connection():
    ''' Gerencia a conexão com o impala '''
    from impala.dbapi import connect
    if not hasattr(g, 'impala_connection'):
        g.impala_connection = connect(
            host=current_app.config["IMPALA_HOST"],
            database='compliance',
            port=int(current_app.config["IMPALA_PORT"]),
            user=current_app.config["IMPALA_USER"],
            use_ssl=True
        )
    return g.impala_connection

def get_redis_pool():
    ''' Gerencia a conexão com o redis '''
    import redis
    if not hasattr(g, 'redis_pool'):
        g.redis_pool = redis.Redis(
            host=current_app.config['REDIS_HOST'],
            port=current_app.config['REDIS_PORT'], # 6379,
            db=current_app.config['REDIS_DB'] # 0
        )
    return g.redis_pool

# Inativo, pois optamos por conectar via REST
def get_hbase_connection():
    ''' Gerencia a conexão com o hbase '''
    import happybase
    if not hasattr(g, 'hbase_connection'):
        g.hbase_connection = happybase.Connection(
            current_app.config["HBASE_HOST"],
            port=int(current_app.config["HBASE_PORT"]),
            table_prefix=None,
            table_prefix_separator=b'_',
            timeout=None,
            transport='buffered',
            protocol='binary',
            autoconnect=False,
            compat='0.98'
        )
        g.hbase_connection.open()
    return g.hbase_connection
