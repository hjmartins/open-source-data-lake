import os 
import pandas as pd
import numpy as np

def new_df(df: pd.DataFrame):
    
    df_features = df.groupby('date').agg(
        city=('city', 'first'),
        temp_mean=('temperature_2m', 'mean'),
        temp_max=('temperature_2m', 'max'),
        temp_min=('temperature_2m', 'min'),
        wind_speed_max=('wind_speed_10m', 'max'),
        wind_speed_mean=('wind_speed_10m', 'mean'),
        wind_speed_min=('wind_speed_10m', 'min'),
        humidity_mean=('relative_humidity_2m', 'mean'),
        humidity_max=('relative_humidity_2m', 'max'),
        humidity_min=('relative_humidity_2m', 'min'),
        precipitation_sum=('precipitation', 'sum')
    ).reset_index()
    df_features.rename(columns={'date': 'date'}, inplace=True)
    return df_features