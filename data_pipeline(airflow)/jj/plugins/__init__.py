from airflow.plugins_manager import AirflowPlugin
from plugins.hooks.postgres_hook import PostgresHelper
from plugins.hooks.mssql_hook import MSSQLHelper
from plugins.hooks.mysql_hook import MySQLHelper
from plugins.hooks.oracle_hook import OracleHelper

# AnomalyTransformer 모듈 import (plugins에서 직접 사용 가능하도록)
try:
    from plugins.models.anomaly_transformer import AnomalyTransformer
except ImportError:
    # 모듈이 없어도 플러그인 자체는 정상 작동
    pass

class CustomPlugin(AirflowPlugin):
    name = "csg_plugin"
    hooks = [PostgresHelper, MSSQLHelper, MySQLHelper, OracleHelper]