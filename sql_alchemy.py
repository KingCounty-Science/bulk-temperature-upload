import base64
import datetime as dt
from datetime import timedelta
from datetime import datetime

import pyodbc
import configparser
import pandas as pd
from datetime import date

from plotly.subplots import make_subplots
from sqlalchemy import create_engine
import urllib

config = configparser.ConfigParser()
config.read('config\gdata_config.ini')

""""SQL_String = pyodbc.connect('Driver=SQLSERVER;'
                            'Server=SERVER=KCITSQLPRNRPX01;'
                            'Database=gDATA;'
                            'Trusted_Connection=YES;')

# new sql alchemy connection
server = config['sql_connection']['Server']
driver = config['sql_connection']['Driver']
database = config['sql_connection']['Database']
trusted_connection = config['sql_connection']['Trusted_Connection']"""
   

# pyodbc has a longer pooling then sql_alchemy and needs to be reset
pyodbc.pooling = False
# not sure this fast execumetry sped things up
# info on host name connection https://docs.sqlalchemy.org/en/14/dialects/mssql.html#connecting-to-pyodbc
sql_alchemy_connection = urllib.parse.quote_plus('DRIVER=SQL Server; SERVER=KCITSQLPRNRPX01; DATABASE=gDATA; Trusted_Connection=yes;')
sql_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % sql_alchemy_connection)


def get_site_sql_id(site_id):
    with sql_engine.begin() as conn:
        #gage_lookup = pd.read_sql_query('select G_ID, SITE_CODE from tblGaugeLLID;', conn)
        site_sql_id = pd.read_sql_query(
                        f"SELECT {config['site_identification']['site_sql_id']} "
                        f"FROM {config['site_identification']['table']} WHERE {config['site_identification']['site']} = '{site_id}';", conn)
    site_sql_id = site_sql_id.iloc[0, 0]
    
    return site_sql_id
