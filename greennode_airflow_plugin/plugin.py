"""GreenNode Airflow Plugin registration."""

from airflow.plugins_manager import AirflowPlugin

from greennode_airflow_plugin.greennode_operator import GreenNodeOperator
from greennode_airflow_plugin.hook import VNGCloudHook


class GreenNodePlugin(AirflowPlugin):
    name = "greennode"
    hooks = [VNGCloudHook]
    operators = [GreenNodeOperator]
