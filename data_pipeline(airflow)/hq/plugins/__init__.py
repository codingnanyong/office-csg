from airflow.plugins_manager import AirflowPlugin
from plugins.hooks.postgres_hook import PostgresHelper
from plugins.hooks.mssql_hook import MSSQLHelper
from plugins.hooks.mysql_hook import MySQLHelper
from plugins.hooks.oracle_hook import OracleHelper

class CustomPlugin(AirflowPlugin):
    name = "csg_plugin"
    hooks = [PostgresHelper, MSSQLHelper, MySQLHelper, OracleHelper]