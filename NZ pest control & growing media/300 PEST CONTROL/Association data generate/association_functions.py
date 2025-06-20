""" This file manage and run SQL queries and also save metrics and parameters to MLFlow """
import warnings
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict
import pandas as pd
import snowflake.connector
from snowflake.connector.errors import ProgrammingError
import re
warnings.filterwarnings("ignore")
import os

def run_and_format_sql_file(
    db_con: snowflake.connector.connection.SnowflakeConnection,
    file_name: str,
    params: Dict,) -> None:
    """run queries in sql files

    Params is a dictionary which is used to conduct any subsititions in braces
    in the SQL file as though it was an f-string.

    """
    if params is None:
        params = {}
    with open(file_name, "r", encoding="UTF-8") as sql_file:
        statements = sql_file.read()
        statements = statements.format(**params)
        statements_list = statements.split(";")
        for ii, statement in enumerate(statements_list):
            statement = re.sub(r'--.*', '', statement).strip()
            # print(statement)
            if len(statement) > 0:
                try:
                    db_con.cursor().execute(statement)
                    db_con.commit()
                except ProgrammingError as ex:
                    if ex.args[0] == "42601":
                        print(f"message: Statement {ii} is empty")
                    else:
                        raise ex

def generate_association_data(params, callback):
    db_con = params['db_con']
    model_name = params['model_name']
    callback('Creating association data table ...')
    run_and_format_sql_file(db_con, 'sql/00-table-initiator.sql', params) 
    callback('Collecting transactions ...')
    run_and_format_sql_file(db_con, 'sql/01-transaction_data.sql', params) 
    print('sql_01 done')
    callback(f'Running the model ...')
    run_and_format_sql_file(db_con, 'sql/02-basket_association.sql', params)
    print('sql_02 done')
    # match params['model_type']:
    #     case 'Basket':
    #         run_and_format_sql_file(db_con, 'sql/02-basket_association.sql', params)
    #     case 'Time-window':
    #         run_and_format_sql_file(db_con, 'sql/03-time_window_association.sql', params)
    #     case 'Directional':
    #         run_and_format_sql_file(db_con, 'sql/04-directional_association.sql', params)
    callback(f"Association data is stored in {params['association_table']} table.")
   
def generate_association_metrics(params, callback):
    db_con = params['db_con']
    callback("Calculating metrics ...")
    run_and_format_sql_file(db_con, 'sql/05-metrics.sql', params) 
    callback(f"Metrics are stored in {params['metric_table']} table")