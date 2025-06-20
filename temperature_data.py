import base64
import datetime as dt
from datetime import timedelta
from datetime import datetime

import pyodbc
import configparser
import pandas as pd
from datetime import date

#from plotly.subplots import make_subplots
from sqlalchemy import create_engine
import urllib

import os
import pandas as pd
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import numpy as np
from scipy.signal import find_peaks, peak_widths
from scipy.interpolate import griddata, CubicSpline, UnivariateSpline
from scipy import interpolate

from pathlib import Path
from PIL import Image
from datetime import datetime, timedelta
import os
from sql_alchemy import get_site_sql_id, update_15_minute_data, calculate_daily_values

gdata_server = os.getenv("gdata_server")
gdata_driver = os.getenv("gdata_driver")
gdata_database = os.getenv("gdata_database")
#socrata_api_id = os.getenv("socrata_api_id")
#socrata_api_secret = os.getenv("socrata_api_secret")
config = configparser.ConfigParser()
config.read('config\gdata_config.ini')

parameter = "groundwater_level"

def import_from_folder():
    # Define the folder path
    folder_path = Path(r"bulk_upload")

    # List all subfolders
    subfolders = [subfolder for subfolder in folder_path.iterdir() if subfolder.is_dir()]
  

    # iterate over sub foulders
    for subfolder in subfolders:
        site = subfolder.name
        site_sql_id = get_site_sql_id(site)
        print(f"folder {subfolder.name}, site {site}")
        folder_path = subfolder
            # List all files in the folder
            # List all CSV files in the folder
        files = [file for file in folder_path.iterdir() if file.is_file() and file.suffix == '.csv']
        
            
        # create blank df
        data = pd.DataFrame(columns=['site', 'site_sql_id', 'datetime', 'water_temperature', 'session'])
        session = 0 # session will not necessarly be chronological but will label individual upload files
        errors = []
        for file in files:
            try:
                print("file: ",  file)
                df = pd.read_csv(file, skiprows=1, usecols=[1,3], parse_dates=[0]) # Adjust format as needed)  # Skip first row and ignore the first column
                        # clean column names 
                
                df.columns = df.columns.str.replace(r"\s*\(.*?\)", "", regex=True)
                df.columns = df.columns.str.replace(",", "", regex=False)
                df = df.rename(columns={df.columns[0]: 'datetime'})
                if df.columns[1] == "Temp °F": # if in F convert to c
                            df["Temp °F"] = (df["Temp °F"] - 32) / 1.8

                df = df.rename(columns={df.columns[1]: 'water_temperature'})  
                    
                
                df = df.dropna()
               
                # conver to deg c if need be
                if df["water_temperature"].mean() > 30:  # f
                            df["water_temperature"] = round((df["water_temperature"] - 32) * (5/9), 2)
                #### remove outliers on first and last 12 rows (3 hours)
                # get statistics
        
                mean_head = df.loc[df.index[:11], "water_temperature"].mean().round(2)
                mean_tail = df.loc[df.index[-10:], "water_temperature"].mean().round(2)

                std_head = df.loc[df.index[:11], "water_temperature"].std().round(2)
                std_tail = df.loc[df.index[-10:], "water_temperature"].std().round(2)
                # remove data outside 1 stdev
                # --- first 12 rows -------------------------------------------------
                idx_head = df.index[:12]                 
                col      = "water_temperature"

                head_vals = df.loc[idx_head, col]

                df.loc[idx_head, col] = head_vals.mask((head_vals > mean_head + std_head) | (head_vals < mean_head - std_head))

                # --- last 12 rows --------------------------------------------------
                idx_tail  = df.index[-12:]                 
                tail_vals = df.loc[idx_tail, col]

                df.loc[idx_tail, col] = tail_vals.mask((tail_vals > mean_tail + std_tail) | (tail_vals < mean_tail - std_tail))

                # fill values               
                df["water_temperature"] = df["water_temperature"].interpolate(method="linear", limit=4, limit_direction = "both")
                
                
                

                ### resample to 15 minute intervals
                # 3. Interpolate water_temperature (already done)
                df = df.set_index("datetime")
                
                df = df.resample("15T").mean()
                df.reset_index(drop=False, inplace = True)
                df["water_temperature"] = df["water_temperature"].interpolate(method="linear", limit_direction="both")
                df['water_temperature'] = df['water_temperature'].round(2)
                df["site"] = site
                df["site_sql_id"] = site_sql_id
                df["session"] = session
                session = session + 1
                df = df[['site', 'site_sql_id', 'datetime', 'water_temperature', 'session']]
                #df = df.dropna()
                print(df)

                # 4. Resample to 15-minute intervals
              
                # 5. Optionally re-interpolate if resampling introduced NaNs
                

                # Concatenate only non-duplicate datetime rows
                # Filter df to only rows with datetime not in data
                df_filtered = df[~df["datetime"].isin(data["datetime"])]
                data = pd.concat([data, df_filtered], ignore_index=True)
                #data = pd.concat([data, df])
                    
            except:
                errors.append(file.name)
                errors = []
        data = data.sort_values(by="datetime", ascending=True)
     
        import matplotlib.pyplot as plt

    
        # Ensure datetime column is datetime and sorted
        data["datetime"] = pd.to_datetime(data["datetime"])
        data = data.sort_values(by="datetime")

        ### meta data
        # make a metadata foulder
        os.makedirs(f"bulk_upload\{site}\metadata", exist_ok=True)
        # save file to metadata
        data.to_csv(f"bulk_upload\{site}\metadata\{site}_data.csv", index=False )
        # create a graph
        # Start a new plot
        plt.figure(figsize=(10, 5))

        # Plot each session as a separate line
        for session_id, group in data.groupby("session"):
            plt.plot(group["datetime"], group["water_temperature"],
                    label=f"Session {session_id}", linestyle='-')

        # Add labels and legend
        plt.xlabel("Datetime")
        plt.ylabel("Water Temperature")
        plt.title(f"{site} water temperature from upload files")
        plt.legend(title="Session", bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(False)
        plt.tight_layout()
        plt.savefig(f"bulk_upload\{site}\metadata\{site} water temperature.pdf")
        #plt.show()



def upload_data():
    """ runs seperatly  to allow for checking of data"""
    folder_path = Path(r"bulk_upload")

   
    subfolders = [subfolder for subfolder in folder_path.iterdir() if subfolder.is_dir()]
    
    # iterate over sub foulders
    for subfolder in subfolders:

        print(subfolder)
        site = subfolder.name
        data = pd.read_csv(f"bulk_upload\{site}\metadata\{site}_data.csv")
        data["datetime"] = pd.to_datetime(data["datetime"])
        print(data)
        update_15_minute_data(data, site, parameter)
        print("upload 15 minute complete")
        calculate_daily_values(data, parameter, site)
        print("daily values complete")
   

#import_from_folder()
upload_data()