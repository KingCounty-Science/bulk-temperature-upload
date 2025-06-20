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
from sqlalchemy.exc import IntegrityError
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

def update_15_minute_data(data, site, parameter):
    # get site sql_id
    site_sql_id = get_site_sql_id(site)
    data["datetime"] = data["datetime"] + pd.Timedelta(hours=7)
    data = data.where(pd.notnull(data), None)
    counter = 0
    for index, row in data.iterrows():
            #try:
            #    data.loc[daily_data.index == index].to_sql(config[parameter]['daily_table'], sql_engine, method=None, if_exists='append', index=False)
            #except IntegrityError:
               
        row_dict = row.to_dict()

        # ---- keys for the WHERE clause ----
        g_id    = row_dict["site_sql_id"]
        p_date  = row_dict["datetime"]          # or whatever you named the date field
        temp    = row_dict["water_temperature"]

        update_sql = f"""
        UPDATE {config[parameter]['table']}
        SET    P_WaterTemp = ?
        WHERE  {config[parameter]['site_sql_id']}  = ? 
        AND  {config[parameter]['datetime']} = ?
        """

        with sql_engine.begin() as cnn:
            cnn.execute(update_sql, [temp, g_id, p_date])
        #print(f"""uploaded {counter} of {len(data)}""")
        counter = counter+1
    print("upload complete")

def calculate_daily_values(df, parameter, site):
        site_sql_id = get_site_sql_id(site)
        start_date = df["datetime"].min().normalize() - pd.Timedelta(days=2)
        end_date = df["datetime"].max().normalize() + pd.Timedelta(days=2)
        """takes a date range from either daily insufficient data or missing data returns a df calculated daily values"""
        daily_data = []
        if start_date:
            # standard columns
            derived_mean = "" # used for discharge
            derived_max = ""    # used for discharge
            derived_min = ""    # used for discharge

            daily_sum = ""

            provisional = f"MAX( CAST({config[parameter]['provisional']} AS INT)) AS {config[parameter]['daily_provisional']}, " # standard provisional
            snow = "" # used for rain

            depth = "" # used for rain
            ice = "" # used for rain
            daily_sum = "" # used for rain

            # parameter specific specially formatted columns
            if parameter == "discharge":
                    derived_mean = f"ROUND(AVG({config[parameter]['discharge']}), 2) AS {config[parameter]['discharge_mean']}, "
                    derived_max = f"ROUND(MAX({config[parameter]['discharge']}), 2) AS {config[parameter]['discharge_max']}, "
                    derived_min = f"ROUND(MAX({config[parameter]['discharge']}), 2) AS {config[parameter]['discharge_min']}, "
                    
            if parameter == "conductivity":
                    provisional = ""

            if parameter == "rain": # this progrma will calculate min/mix/avg but those columns will be removed by column managment
                    daily_sum = f"ROUND(SUM({config[parameter]['corrected_data']}), 2) AS {config[parameter]['daily_sum']}, "
                    snow =  f"MAX( CAST({config[parameter]['snow']} AS INT)) AS {config[parameter]['daily_snow']}, "
        
            if parameter == "water_temperature":
                    depth = f"ROUND( AVG( CAST({config[parameter]['ice']} AS INT) ) , 2) AS {config[parameter]['daily_depth']}, "
                    ice =  f"MAX( CAST({config[parameter]['ice']} AS INT)) AS {config[parameter]['daily_ice']}, "
            if parameter == "groundwater_level":
                    groundwater_temperature =  f"ROUND(AVG({config[parameter]['groundwater_temperature']}), 2) AS {config[parameter]['groundwater_temperature']}, "
            with sql_engine.begin() as conn:
                    # 120 is yyyy-mm-dd hh:mi:ss
                    # 105 dd-mm-yyyy
                    #new_data = pd.read_sql_query('select '+config[parameter]['datetime']+','+config[parameter]['corrected_data']+' from '+config[parameter]['table']+' WHERE G_ID = '+str(site_sql_id)+' AND '+config[parameter]['datetime']+' between ? and ?', conn, params=[str(start_date), str(end_date)])            
                
                    daily_data = pd.read_sql_query(f"SELECT CAST({config[parameter]['datetime']} AS DATE) AS {config[parameter]['daily_datetime']}, "
                                                    f"{site_sql_id} AS {config[parameter]['site_sql_id']}, "
                                                    f"ROUND(AVG({config[parameter]['corrected_data']}), 2) AS {config[parameter]['daily_mean']}, ROUND(MAX({config[parameter]['corrected_data']}), 2) AS {config[parameter]['daily_max']}, ROUND(MIN({config[parameter]['corrected_data']}), 2) AS {config[parameter]['daily_min']}, "
                                                    f"{derived_mean}{derived_max}{derived_min}"
                                                    f"COUNT(*) AS {config[parameter]['daily_record_count']}, "
                                                    f"MAX( CAST({config[parameter]['estimate']} AS INT)) AS {config[parameter]['daily_estimate']}, "
                                                    f"MAX( CAST({config[parameter]['warning']} AS INT)) AS {config[parameter]['daily_warning']}, "
                                                    f"{daily_sum}"
                                                    f"{groundwater_temperature}"
                                                    f"{snow}"
                                                    f"{ice}"
                                                    f"{depth}"
                                                    f"{provisional}"
                                                    f"MAX( CAST({config[parameter]['lock']} AS INT)) AS {config[parameter]['daily_lock']} "
                                                    f"FROM {config[parameter]['table']} "
                                                    f"WHERE G_ID = {site_sql_id} AND CAST({config[parameter]['datetime']} AS DATE) BETWEEN '{start_date}' AND '{end_date}' "
                                                    f"GROUP BY CAST({config[parameter]['datetime']} AS DATE) ", conn)
                    daily_data[f"{config[parameter]['daily_auto_timestamp']}"] = datetime.now().strftime("%m/%d/%Y %H:%M:%S")

            # arrange columns in desired order
            with sql_engine.begin() as conn:
                desired_order = pd.read_sql_query(f"SELECT TOP 1 * "
                                                    f"FROM {config[parameter]['daily_table']} "
                                                    f"WHERE G_ID = {site_sql_id} ", conn)
                
            desired_order = desired_order.columns.tolist()
            existing_columns = [col for col in desired_order if col in daily_data.columns]         # Filter out columns that exist in the DataFrame
            daily_data = daily_data[existing_columns]

            for index, row in daily_data.iterrows():
                try:
                    daily_data.loc[daily_data.index == index].to_sql(config[parameter]['daily_table'], sql_engine, method=None, if_exists='append', index=False)
                except:
                
                    row_dict = row.to_dict()
                    
                    update_cols = [col for col in row_dict if col not in ["G_ID", config[parameter]['daily_datetime']]]
                    key_cols = ["G_ID", "P_Date"]

                    set_clause = ",\n    ".join([f"{col} = ?" for col in update_cols])
                    where_clause = " AND ".join([f"{col} = ?" for col in key_cols])

                    update_sql = f"""
                    UPDATE {config[parameter]['daily_table']}
                    SET
                        {set_clause}
                    WHERE {where_clause}
                    """
                    

                    #update_values =  [row_dict[col] for col in key_cols] + [row_dict[col] for col in update_cols]
                    update_values = [row_dict[col] for col in update_cols] + [row_dict[col] for col in key_cols]
            
                    with sql_engine.begin() as cnn:
                        cnn.execute(update_sql, update_values)

        return daily_data
