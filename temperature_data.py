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
# Define the folder path
folder_path = Path(r"bulk_upload")

# List all subfolders
subfolders = [subfolder for subfolder in folder_path.iterdir() if subfolder.is_dir()]
data = []
data = pd.DataFrame(columns=['site', 'datetime', 'water_temperature'])
errors = []

# iterate over sub foulders
for subfolder in subfolders:
    print(subfolder.name)
    site = subfolder.name
    site_sql_id = get_site_sql_id(site)
    print(site_sql_id)
    folder_path = subfolder
        # List all files in the folder
        # List all CSV files in the folder
    files = [file for file in folder_path.iterdir() if file.is_file() and file.suffix == '.csv']
    
        
    # create blank df
    data = pd.DataFrame(columns=['site', 'site_sql_id', 'datetime', 'water_temperature', 'session'])
    session = 0 # session will not necessarly be chronological but will label individual upload files
    for file in files:
            #try:
        print("file: ",  file)
        df = pd.read_csv(file, skiprows=1, usecols=[1,3], parse_dates=[0]) # Adjust format as needed)  # Skip first row and ignore the first column
                # clean column names 
               
        df.columns = df.columns.str.replace(r"\s*\(.*?\)", "", regex=True)
        df.columns = df.columns.str.replace(",", "", regex=False)
        df = df.rename(columns={df.columns[0]: 'datetime'})
        if df.columns[1] == "Temp °F": # if in F convert to c
                    df["Temp °F"] = (df["Temp °F"] - 32) / 1.8

        df = df.rename(columns={df.columns[1]: 'water_temperature'})  
             
        df["site"] = site
        df["site_sql_id"] = site_sql_id
        df["session"] = session
        session = session + 1
        df = df.dropna()
        # conver to deg c if need be
        if df["data"].mean() > 30:  # f
                    df["data"] = round((df["data"] - 32) * (5/9), 2)
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
        print(df)
      
        data = pd.concat([data, df])
              
           # except:
              #  errors.append(file.name)
    data = data.sort_values(by="datetime", ascending=True)
    print(data)
    import matplotlib.pyplot as plt

   
    # Ensure datetime column is datetime and sorted
    data["datetime"] = pd.to_datetime(data["datetime"])
    data = data.sort_values(by="datetime")

    ### meta data
    # make a metadata foulder
    os.makedirs(f"bulk_upload\{site}\metadata", exist_ok=True)
    # save file to metadata
    data.to_csv(f"bulk_upload\{site}\metadata\{site}_data.csv")
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

    update_15_minute_data(data, site, parameter)

    
   
    calculate_daily_values(data, parameter, site)
   
        #print("upload errors")
        #print(errors)
        #data = data.dropna()
        #data = data.sort_values(by='datetime', ascending=True)
        
       #data.reset_index(drop=True, inplace = True)
        
        # Convert 'datetime' to numeric values (timestamps in seconds)
        #data["date_num"] = data['datetime'].dt.strftime('%Y%m%d%H%M%S')
        
        #data["date_num"] = pd.to_numeric(data["date_num"])
    """data = data.sort_values(by="date_num", ascending=True)
        
        
        x = np.linspace(data.index.min(), data.index.max(), data.shape[0])
        print("shape ", data.shape[0])
        y = data["water_temperature"].to_numpy()
        spl = CubicSpline(x, y)
        #smooth_spl = UnivariateSpline(x, y, s=.5)
        #spline_0 = spl(data.index.to_numpy(), nu = 0) # nu 1 is rate of change or slope
        #data["spline_0"] = spline_0
        #spline_1 = spl(data.index.to_numpy(), nu = 1) # nu 1 is rate of change or slope
        #data["spline_1"] = spline_1

        spline_2 = spl(data.index.to_numpy(), nu = 2) # nu 2 is curvature (increasing or decreasing)
        spline_3 = spl(data.index.to_numpy(), nu = 3) # nu 1 is rate of change or slope
        data["spline"] = spline_2
        #smooth = smooth_spl(data.index.to_numpy()) # nu 1 is rate of change or slope
        #data["smooth_spl"] = smooth
        #data["spline"] = data["spline"].astype(float, errors="ignore")
        #spline_3 = spl(data.index.to_numpy(), nu = 3) # nu 1 is rate of change or slope
        #data["spline_3"] = spline_2
        std = abs(data["spline"]).std()/4
        std= .01
        avg = abs(data["spline"].mean())
        print("avg", avg, "std ", std, "upper ", avg+std, "lower", avg-std)
       
        #print(new)
        # Ensure water_temperature is float
        data["water_temperature"] = data["water_temperature"].astype(float)

        # Apply the condition
        data.loc[abs(data["spline"]) >= std, "water_temperature"] = np.nan
        #data.loc[data["spline"] <= -std, "water_temperature"] = np.nan
        data.loc[abs(data["spline"]) >= std, "spline"] = np.nan
        #data.loc[data["spline"] <= -std, "spline"] = np.nan
        print(data)
        plt.plot(data["datetime"], data["water_temperature"], color = "lightblue")
        #plt.plot(data["datetime"], abs(data["spline_0"](), label = f"0")
        #plt.plot(data["datetime"], abs(data["spline_1"]), label = f"1")
        #plt.plot(data["datetime"], abs(data["spline_2"]), label = f"2")
        plt.plot(data["datetime"], abs(data["spline"]), label = f"spline")
        #plt.plot(data["datetime"], data["smooth_spl"], label = f"spline")
        
        #plt.plot(data["datetime"], np.linspace(std, std, data.shape[0]), color = "lightseagreen", label = f"avg")
        #plt.plot(data["datetime"], data["max"], color = "lightseagreen", label = f"max")
        plt.show()"""
"""subfolder_names = [subfolder.name for subfolder in folder_path.iterdir() if subfolder.is_dir()]

# Print the subfolder names
for name in subfolder_names:
    print(name)"""