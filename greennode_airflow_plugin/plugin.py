from airflow.plugins_manager import AirflowPlugin
from greennode_airflow_plugin.hook import VNGCloudHook
from greennode_airflow_plugin.greennode_operator import GreenNodeOperator


class GreenNodePlugin(AirflowPlugin):
    name = "greennode_airflow_plugin"
    hooks = [VNGCloudHook]
    operators = [GreenNodeOperator]
