from airflow.hooks.base import BaseHook
from utils.get_meta import get_meta
import pprint
class MetaHook(BaseHook):
    def __init__(self,
                 dag_id,
                 execution_date,
                 operator_type,
                 theme_color=None):
        self.dag_id=dag_id
        self.execution_date = execution_date
        self.operator_type = operator_type
        self.theme_color = theme_color


    def execute(self,context=None):
        meta = get_meta(context['dag_run'].dag_id,context['dag_run'].execution_date, self.operator_type)
        print(len(meta))
        print(type(meta[0]))
        print("**************************************************")
        for i in meta:
            print(i)
            for k,v in i.items():
                print(f'key: {k} | value: {v} ')
        print("**************************************************")

