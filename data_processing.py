



import pandas as pd
from pyproj import CRS, Transformer
import pytz
from astral.location import LocationInfo
from astral import sun
import numpy as np
import warnings
import os
import gdown
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import seaborn as sns
import traceback
import math
from datetime import timedelta
from typing import Dict
import warnings
from plotly.subplots import make_subplots
import plotly.express as px
import time
from functools import reduce
from concurrent.futures import ThreadPoolExecutor
from config import METADATA_VARIABLES, PALETTE_DEFAUT, DATA_LIMITS, PALETTE_COULEUR, PERIOD_LABELS

# Importation pour Flask-Babel
from flask_babel import lazy_gettext as _l, get_locale
#from flask_babel import Babel, _, lazy_gettext as _l, get_locale as get_current_locale

def apply_station_specific_preprocessing(df: pd.DataFrame, station: str) -> pd.DataFrame:
    """
    Prétraite les données d'une station spécifique en fonction de son nom.
    Applique les renommages de colonnes et les sélections spécifiques.
    
    Args:
        df (pd.DataFrame): DataFrame brut à traiter
        station (str): Nom de la station
        
    Returns:
        pd.DataFrame: DataFrame prétraité avec les colonnes standardisées
    """
    # Dictionnaire de mapping des stations aux bassins
    STATION_TO_BASIN = {
        # Stations Dano
        'Tambiri 1': 'DANO',
        # Stations Dassari
        'Ouriyori 1': 'DASSARI',
        # Stations Vea Sissili
        'Oualem': 'VEA_SISSILI',
        'Nebou': 'VEA_SISSILI',
        'Nabugubelle': 'VEA_SISSILI',
        'Manyoro': 'VEA_SISSILI',
        'Gwosi': 'VEA_SISSILI',
        'Doninga': 'VEA_SISSILI',
        'Bongo Soe': 'VEA_SISSILI',
        'Aniabisi': 'VEA_SISSILI',
        'Atampisi': 'VEA_SISSILI'
    }
    
    # Déterminer le bassin à partir de la station
    bassin = STATION_TO_BASIN.get(station)
    
    if not bassin:
        # Traduction de l'avertissement
        warnings.warn(_l("Station %s non reconnue dans aucun bassin. Prétraitement standard appliqué.") % station)
        return df
    
    df_copy = df.copy()
    
    # Traitement pour le bassin Dano
    if bassin == 'DANO':
        if station == 'Tambiri 1':
            colonnes_select = ['Year', 'Month', 'Day', 'Hour', 'Minute', 'AirTC_Avg', 'RH', 
                             'WS_ms_S_WVT', 'WindDir_D1_WVT', 'Rain_mm_Tot', 'BP_mbar_Avg', 'Station']
            colonnes_renommage = {
                'AirTC_Avg': 'Air_Temp_Deg_C', 
                'RH': 'Rel_H_%', 
                'WS_ms_S_WVT': 'Wind_Sp_m/sec', 
                'WindDir_D1_WVT': 'Wind_Dir_Deg', 
                'Rain_mm_Tot': 'Rain_mm'
            }
            df_copy = df_copy[colonnes_select]
            df_copy.rename(columns=colonnes_renommage, inplace=True, errors='ignore')
    
    # Traitement pour le bassin Dassari
    elif bassin == 'DASSARI':
        if station == 'Ouriyori 1':
            colonnes_sup = ['TIMESTAMP', 'RECORD', 'WSDiag', 'Intensity_RT_Avg', 'Acc_RT_NRT_Tot', 
                          'Pluvio_Status', 'BP_mbar_Avg', 'SR01Up_Avg', 'SR01Dn_Avg', 'IR01Up_Avg', 
                          'IR01Dn_Avg', 'NR01TC_Avg', 'IR01UpCo_Avg', 'IR01DnCo_Avg',
                          'Acc_NRT_Tot', 'Acc_totNRT', 'Bucket_RT_Avg', 'Bucket_NRT',
                          'Temp_load_cell_Avg', 'Heater_Status']
            
            colonnes_renommage = {
                'Rain_mm_Tot': 'Rain_mm',
                'AirTC_Avg': 'Air_Temp_Deg_C',
                'RH': 'Rel_H_%',
                'SlrW_Avg': 'Solar_R_W/m^2',
                'WS_ms_S_WVT': 'Wind_Sp_m/sec',
                'WindDir_D1_WVT': 'Wind_Dir_Deg'
            }
            
            if 'TIMESTAMP' in df_copy.columns:
                df_copy["TIMESTAMP"] = pd.to_datetime(df_copy["TIMESTAMP"], errors="coerce")
                df_copy.dropna(subset=["TIMESTAMP"], inplace=True)
                
                df_copy["Year"] = df_copy["TIMESTAMP"].dt.year
                df_copy["Month"] = df_copy["TIMESTAMP"].dt.month
                df_copy["Day"] = df_copy["TIMESTAMP"].dt.day
                df_copy["Hour"] = df_copy["TIMESTAMP"].dt.hour
                df_copy["Minute"] = df_copy["TIMESTAMP"].dt.minute
            
            df_copy.drop(columns=colonnes_sup, inplace=True, errors='ignore')
            df_copy.rename(columns=colonnes_renommage, inplace=True, errors='ignore')
    
    # Traitement pour le bassin Vea Sissili
    elif bassin == 'VEA_SISSILI':
        stations_vea_a_9_variables = ['Oualem', 'Nebou', 'Nabugubelle', 'Manyoro', 'Gwosi', 'Doninga', 'Bongo Soe']
        
        if station in stations_vea_a_9_variables:
            colonnes_renommage = {
                'Rain_mm_Tot': 'Rain_mm',
                'AirTC_Avg': 'Air_Temp_Deg_C',
                'RH': 'Rel_H_%',
                'SlrW_Avg': 'Solar_R_W/m^2',
                'WS_ms_S_WVT': 'Wind_Sp_m/sec',
                'WindDir_D1_WVT': 'Wind_Dir_Deg',
                'Date': 'Datetime'
            }
            colonnes_sup = ['SlrkJ_Tot', 'WS_ms_Avg', 'WindDir', 'Rain_01_mm_Tot', 'Rain_02_mm_Tot']
            
        elif station == 'Aniabisi':
            colonnes_renommage = {
                'Rain_mm_Tot': 'Rain_mm',
                'AirTC_Avg': 'Air_Temp_Deg_C',
                'RH': 'Rel_H_%',
                'SlrW_Avg': 'Solar_R_W/m^2',
                'WS_ms_S_WVT': 'Wind_Sp_m/sec',
                'WindDir_D1_WVT': 'Wind_Dir_Deg',
                'Date': 'Datetime'
            }
            colonnes_sup = ['Intensity_RT_Avg', 'Acc_NRT_Tot', 'Acc_RT_NRT_Tot', 'SR01Up_Avg', 
                          'SR01Dn_Avg', 'IR01Up_Avg', 'IR01Dn_Avg', 'IR01UpCo_Avg', 'IR01DnCo_Avg']
            
        elif station == 'Atampisi':
            colonnes_renommage = {
                'Rain_01_mm_Tot': 'Rain_01_mm',
                'Rain_02_mm_Tot': 'Rain_02_mm',
                'AirTC_Avg': 'Air_Temp_Deg_C',
                'RH': 'Rel_H_%',
                'SlrW_Avg': 'Solar_R_W/m^2',
                'WS_ms_Avg': 'Wind_Sp_m/sec',
                'WindDir': 'Wind_Dir_Deg',
                'Date': 'Datetime'
            }
            colonnes_sup = []
        
        if 'Date' in df_copy.columns:
            df_copy["Date"] = pd.to_datetime(df_copy["Date"], errors="coerce")
            df_copy.dropna(subset=["Date"], inplace=True)
            
            df_copy["Year"] = df_copy["Date"].dt.year
            df_copy["Month"] = df_copy["Date"].dt.month
            df_copy["Day"] = df_copy["Date"].dt.day
            df_copy["Hour"] = df_copy["Date"].dt.hour
            df_copy["Minute"] = df_copy["Date"].dt.minute
        
        if colonnes_sup:
            df_copy.drop(columns=colonnes_sup, inplace=True, errors='ignore')
        
        df_copy.rename(columns=colonnes_renommage, inplace=True, errors='ignore')
    
    return df_copy

def create_rain_mm(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crée la colonne 'Rain_mm' en fusionnant 'Rain_01_mm' et 'Rain_02_mm'.
    Utilise 'Rain_01_mm' par défaut, puis 'Rain_02_mm' si 'Rain_01_mm' est NaN.
    """
    df_copy = df.copy()
    if 'Rain_01_mm' in df_copy.columns and 'Rain_02_mm' in df_copy.columns:
        df_copy['Rain_mm'] =  df_copy[['Rain_01_mm', 'Rain_02_mm']].mean(axis=1) # Ou une autre logique d'agrégation

    elif 'Rain_01_mm' in df_copy.columns:
        df_copy['Rain_mm'] = df_copy['Rain_01_mm']
    elif 'Rain_02_mm' in df_copy.columns:
        df_copy['Rain_mm'] = df_copy['Rain_02_mm']
    else:
        # Traduction de l'avertissement
        #df_copy['Rain_mm'] = np.nan
        warnings.warn(_l("Ni 'Rain_01_mm' ni 'Rain_02_mm' ne sont présents pour créer 'Rain_mm'. 'Rain_mm' est rempli de NaN."))
    return df_copy

def create_datetime(df: pd.DataFrame, bassin: str = None, station: str = None) -> pd.DataFrame:
    """
    Crée la colonne 'Datetime' à partir de colonnes séparées (Year, Month, Day, Hour, Minute)
    ou à partir d'une colonne 'Date' pour le bassin VEA_SISSILI.

    Args:
        df (pd.DataFrame): DataFrame d'entrée.
        bassin (str, optional): Nom du bassin ('DANO', 'DASSARI', 'VEA_SISSILI').
        station (str, optional): Nom de la station pour un traitement spécifique.

    Returns:
        pd.DataFrame: DataFrame avec la colonne 'Datetime' et ses composantes, si possible.
    """
    df_copy = df.copy()
    
    if bassin and station:
        df_copy = apply_station_specific_preprocessing(df_copy, bassin, station)

    if 'Date' in df_copy.columns and (bassin == 'VEA_SISSILI' or not any(col in df_copy.columns for col in ['Year', 'Month', 'Day', 'Hour', 'Minute'])):
        try:
            df_copy['Datetime'] = pd.to_datetime(df_copy['Date'], errors='coerce')
        except Exception as e:
            # Traduction de l'avertissement
            warnings.warn(_l("Impossible de convertir la colonne 'Date' en Datetime pour le bassin %s: %s") % (bassin, e))
            df_copy['Datetime'] = pd.NaT
    else:
        date_cols = ['Year', 'Month', 'Day', 'Hour', 'Minute']
        
        for col in date_cols:
            if col in df_copy.columns:
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')

        try:
            existing_date_components = [col for col in ['Year', 'Month', 'Day', 'Hour', 'Minute'] if col in df_copy.columns]
            
            if not existing_date_components:
                # Traduction de l'erreur
                raise ValueError(_l("Aucune colonne de composantes de date/heure (Year, Month, Day, Hour, Minute) trouvée."))

            date_strings = df_copy.apply(
                lambda row: f"{int(row.get('Year', 2000))}-"
                           f"{int(row.get('Month', 1)):02d}-"
                           f"{int(row.get('Day', 1)):02d} "
                           f"{int(row.get('Hour', 0)):02d}:"
                           f"{int(row.get('Minute', 0)):02d}",
                axis=1
            )
            df_copy['Datetime'] = pd.to_datetime(date_strings, errors='coerce')
            
        except Exception as e:
            # Traduction de l'avertissement
            warnings.warn(_l("Impossible de créer Datetime à partir des colonnes séparées. Erreur: %s. Colonnes présentes: %s") % (e, df_copy.columns.tolist()))
            df_copy['Datetime'] = pd.NaT
            
    if 'Datetime' in df_copy.columns and df_copy['Datetime'].notna().any():
        df_copy['Year'] = df_copy['Datetime'].dt.year
        df_copy['Month'] = df_copy['Datetime'].dt.month
        df_copy['Day'] = df_copy['Datetime'].dt.day
        df_copy['Hour'] = df_copy['Datetime'].dt.hour
        df_copy['Minute'] = df_copy['Datetime'].dt.minute
        if 'Date' not in df_copy.columns or not pd.api.types.is_datetime64_any_dtype(df_copy['Date']):
             df_copy['Date'] = df_copy['Datetime'].dt.date
    else:
        # Traduction de l'avertissement
        warnings.warn(_l("La colonne 'Datetime' est vide ou n'existe pas après la tentative de création. Composantes de date/heure non extraites."))

    return df_copy


# def interpolation(df: pd.DataFrame, limits: dict, df_gps: pd.DataFrame) -> pd.DataFrame:
#     """
#     Effectue toutes les interpolations météorologiques en une seule passe.
#     Cette fonction DOIT recevoir un DataFrame avec un DatetimeIndex.
#     Il doit également contenir une colonne 'Station'.

#     Args:
#         df (pd.DataFrame): Le DataFrame d'entrée avec DatetimeIndex et colonne 'Station'.
#         limits (dict): Dictionnaire définissant les limites de valeurs pour chaque variable.
#         df_gps (pd.DataFrame): Le DataFrame contenant les informations de station
#                                (colonnes 'Station', 'Lat', 'Long', 'Timezone').

#     Returns:
#         pd.DataFrame: Le DataFrame original avec les données interpolées,
#                       la colonne 'Is_Daylight' calculée, la durée du jour, et un DatetimeIndex.
#     """
#     df_processed = df.copy()

#     if not isinstance(df_processed.index, pd.DatetimeIndex):
#         raise TypeError(str(_l("Le DataFrame d'entrée pour l'interpolation DOIT avoir un DatetimeIndex.")))
    
#     initial_rows = len(df_processed)
#     df_processed = df_processed[df_processed.index.notna()]
#     if len(df_processed) == 0:
#         raise ValueError(str(_l("Après nettoyage des index temporels manquants, le DataFrame est vide. Impossible de procéder à l'interpolation.")))
#     if initial_rows - len(df_processed) > 0:
#         warnings.warn(str(_l("Suppression de %s lignes avec index Datetime manquant ou invalide dans l'interpolation.") % (initial_rows - len(df_processed))))
    
#     print(_l("DEBUG (interpolation): Type de l'index du DataFrame initial: %s") % type(df_processed.index))
#     print(_l("DEBUG (interpolation): Premières 5 valeurs de l'index après nettoyage des NaT: %s") % (df_processed.index[:5].tolist() if not df_processed.empty else 'DataFrame vide'))

#     required_gps_cols = ['Station', 'Lat', 'Long', 'Timezone']
#     if not all(col in df_gps.columns for col in required_gps_cols):
#         raise ValueError(
#             str(_l("df_gps doit contenir les colonnes %s. Colonnes actuelles dans df_gps : %s") % \
#             (required_gps_cols, df_gps.columns.tolist()))
#         )

#     if not df_gps['Station'].is_unique:
#         print(_l("Avertissement: La colonne 'Station' dans df_gps contient des noms de station dupliqués."))
#         print(_l("Ceci peut entraîner des comportements inattendus ou des stations non reconnues."))
#         df_gps_unique = df_gps.drop_duplicates(subset=['Station'], keep='first').copy()
#         print(_l("Suppression de %s doublons dans df_gps (en gardant la première occurrence).") % (len(df_gps) - len(df_gps_unique)))
#     else:
#         df_gps_unique = df_gps.copy()

#     gps_info_dict = df_gps_unique.set_index('Station')[['Lat', 'Long', 'Timezone']].to_dict('index')

#     numerical_cols = ['Air_Temp_Deg_C', 'Rel_H_%', 'BP_mbar_Avg',
#                       'Rain_01_mm', 'Rain_02_mm', 'Rain_mm', 'Wind_Sp_m/sec',
#                       'Solar_R_W/m^2', 'Wind_Dir_Deg']
#     for col in numerical_cols:
#         if col in df_processed.columns:
#             df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')
#             if col in limits:
#                 min_val = limits[col]['min']
#                 max_val = limits[col]['max']
#                 initial_nan_count = df_processed[col].isna().sum()
#                 if min_val is not None:
#                     df_processed.loc[df_processed[col] < min_val, col] = np.nan
#                 if max_val is not None:
#                     df_processed.loc[df_processed[col] > max_val, col] = np.nan
                
#                 new_nan_count = df_processed[col].isna().sum()
#                 if new_nan_count > initial_nan_count:
#                     warnings.warn(str(_l("Remplacement de %s valeurs hors limites dans '%s' par NaN.") % (new_nan_count - initial_nan_count, col)))
            
#             print(_l("DEBUG (interpolation/variable): Interpolation de '%s'. Type de l'index de df_final: %s") % (col, type(df_processed.index)))
            
#             if isinstance(df_processed.index, pd.DatetimeIndex):
#                 df_processed[col] = df_processed[col].interpolate(method='time', limit_direction='both')
#             else:
#                 print(_l("Avertissement (interpolation/variable): L'index n'est pas un DatetimeIndex pour l'interpolation de '%s'. Utilisation de la méthode 'linear'.") % col)
#                 df_processed[col] = df_processed[col].interpolate(method='linear', limit_direction='both')
#             df_processed[col] = df_processed[col].bfill().ffill()

#     df_processed_parts = []

#     for station_name, group in df_processed.groupby('Station'):
#         group_copy = group.copy()
#         print(_l("DEBUG (interpolation/groupby): Début du traitement du groupe '%s'.") % station_name)
        
#         if group_copy.index.tz is None:
#             group_copy.index = group_copy.index.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
#         elif group_copy.index.tz != pytz.utc:
#             group_copy.index = group_copy.index.tz_convert('UTC')
#         print(_l("DEBUG (interpolation/groupby): Index Datetime pour '%s' STANDARDISÉ à UTC. Dtype: %s") % (station_name, group_copy.index.dtype))
        
#         group_copy = group_copy[group_copy.index.notna()]
#         if group_copy.empty:
#             warnings.warn(str(_l("Le groupe '%s' est vide après nettoyage de l'index Datetime. Il sera ignoré.") % station_name))
#             continue

#         apply_fixed_daylight = True
#         gps_data = gps_info_dict.get(station_name)
#         if gps_data and pd.notna(gps_data.get('Lat')) and pd.notna(gps_data.get('Long')) and pd.notna(gps_data.get('Timezone')):
#             lat = gps_data['Lat']
#             long = gps_data['Long']
#             timezone_str = gps_data['Timezone']

#             try:
#                 local_tz = pytz.timezone(timezone_str)
#                 index_for_astral_local = group_copy.index.tz_convert(local_tz)

#                 daily_sun_info = {}
#                 unique_dates_ts_local = index_for_astral_local.normalize().drop_duplicates()

#                 if unique_dates_ts_local.empty:
#                     raise ValueError(str(_l("Aucune date unique trouvée pour le calcul Astral.")))
                
#                 for ts_local_aware in unique_dates_ts_local:
#                     loc = LocationInfo(station_name, "Site", timezone_str, lat, long)
#                     naive_date_for_astral = ts_local_aware.to_pydatetime().date()
#                     s = sun.sun(loc.observer, date=naive_date_for_astral) 
#                     daily_sun_info[naive_date_for_astral] = {
#                         'sunrise': s['sunrise'],
#                         'sunset': s['sunset']
#                     }

#                 naive_unique_dates_for_index = [ts.date() for ts in unique_dates_ts_local]
#                 temp_df_sun_index = pd.Index(naive_unique_dates_for_index, name='Date_Local_Naive')
#                 temp_df_sun = pd.DataFrame(index=temp_df_sun_index)
                
#                 print(_l("DEBUG (astral_calc): unique_dates_ts_local type: %s") % type(unique_dates_ts_local))
#                 print(_l("DEBUG (astral_calc): naive_unique_dates_for_index type: %s") % type(naive_unique_dates_for_index))
#                 print(_l("DEBUG (astral_calc): temp_df_sun_index type: %s") % type(temp_df_sun_index))
#                 if not temp_df_sun.empty:
#                     print(_l("DEBUG (astral_calc): First element of temp_df_sun.index: %s") % temp_df_sun.index[0])
#                     print(_l("DEBUG (astral_calc): Type of first element of temp_df_sun.index: %s") % type(temp_df_sun.index[0]))

#                 temp_df_sun['sunrise_time_local'] = [daily_sun_info.get(date, {}).get('sunrise') for date in temp_df_sun.index]
#                 temp_df_sun['sunset_time_local'] = [daily_sun_info.get(date, {}).get('sunset') for date in temp_df_sun.index]

#                 group_copy_reset = group_copy.reset_index()
#                 group_copy_reset['Date_Local_Naive'] = group_copy_reset['Datetime'].dt.tz_convert(local_tz).dt.date

#                 group_copy_reset = pd.merge(group_copy_reset, temp_df_sun, on='Date_Local_Naive', how='left')

#                 group_copy_reset['sunrise_time_utc'] = group_copy_reset['sunrise_time_local'].dt.tz_convert('UTC')
#                 group_copy_reset['sunset_time_utc'] = group_copy_reset['sunset_time_local'].dt.tz_convert('UTC')

#                 group_copy_reset.loc[:, 'Is_Daylight'] = (group_copy_reset['Datetime'] >= group_copy_reset['sunrise_time_utc']) & \
#                                                           (group_copy_reset['Datetime'] < group_copy_reset['sunset_time_utc'])

#                 daylight_timedelta_local = group_copy_reset['sunset_time_local'] - group_copy_reset['sunrise_time_local']
                
#                 def format_timedelta_to_hms(td):
#                     if pd.isna(td):
#                         return np.nan
#                     total_seconds = int(td.total_seconds())
#                     hours = total_seconds // 3600
#                     minutes = (total_seconds % 3600) // 60
#                     seconds = total_seconds % 60
#                     return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

#                 group_copy_reset.loc[:, 'Daylight_Duration'] = daylight_timedelta_local.apply(format_timedelta_to_hms)

#                 group_copy = group_copy_reset.set_index('Datetime')
#                 group_copy = group_copy.drop(columns=['Date_Local_Naive', 'sunrise_time_local', 'sunset_time_local', 'sunrise_time_utc', 'sunset_time_utc'], errors='ignore')

#                 print(_l("Lever et coucher du soleil calculés pour %s.") % station_name)
#                 apply_fixed_daylight = False

#             except Exception as e:
#                 print(_l("Erreur lors du calcul du lever/coucher du soleil avec Astral pour %s: %s.") % (station_name, e))
#                 traceback.print_exc()
#                 warnings.warn(str(_l("Calcul Astral impossible pour '%s'. Utilisation de l'indicateur jour/nuit fixe.") % station_name))
#                 apply_fixed_daylight = True
#         else:
#             print(_l("Avertissement: Coordonnées ou Fuseau horaire manquants/invalides pour le site '%s' dans df_gps. Utilisation de l'indicateur jour/nuit fixe.") % station_name)
#             apply_fixed_daylight = True

#         if apply_fixed_daylight:
#             group_copy.loc[:, 'Is_Daylight'] = (group_copy.index.hour >= 7) & (group_copy.index.hour <= 18)
#             group_copy.loc[:, 'Daylight_Duration'] = "11:00:00"
#             print(_l("Utilisation de l'indicateur jour/nuit fixe (7h-18h) pour %s.") % station_name)

#         df_processed_parts.append(group_copy)

#     if not df_processed_parts:
#         raise ValueError(str(_l("Aucune partie de DataFrame n'a pu être traitée après le regroupement par station.")))

#     df_final = pd.concat(df_processed_parts).sort_index()
#     df_final.index.name = 'Datetime' 
#     print(_l("DEBUG (interpolation/concat): Index du DataFrame final après concaténation et tri: %s") % type(df_final.index))
#     print(_l("DEBUG (interpolation/concat): Colonnes du DataFrame final après concaténation: %s") % df_final.columns.tolist())

#     cols_to_drop_after_process = ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
#     df_final = df_final.drop(columns=cols_to_drop_after_process, errors='ignore')

#     if 'Rain_mm' not in df_final.columns or df_final['Rain_mm'].isnull().all():
#         if 'Rain_01_mm' in df_final.columns and 'Rain_02_mm' in df_final.columns:
#             df_final = create_rain_mm(df_final)
#             warnings.warn(str(_l("Colonne Rain_mm créée à partir des deux capteurs.")))
#         else:
#             warnings.warn(str(_l("Rain_mm manquant et impossible à créer (capteurs pluie incomplets).")))
#             if 'Rain_mm' not in df_final.columns:
#                 df_final['Rain_mm'] = np.nan

#     standard_vars = ['Air_Temp_Deg_C', 'Rel_H_%', 'BP_mbar_Avg',
#                       'Rain_01_mm', 'Rain_02_mm', 'Rain_mm', 'Wind_Sp_m/sec',
#                       'Solar_R_W/m^2', 'Wind_Dir_Deg']

#     for var in standard_vars:
#         if var in df_final.columns:
#             df_final[var] = pd.to_numeric(df_final[var], errors='coerce')
#             if var in limits:
#                 min_val = limits[var]['min']
#                 max_val = limits[var]['max']
#                 initial_nan_count = df_final[var].isna().sum()
#                 if min_val is not None:
#                     df_final.loc[df_final[var] < min_val, var] = np.nan
#                 if max_val is not None:
#                     df_final.loc[df_final[var] > max_val, var] = np.nan
                
#                 new_nan_count = df_final[var].isna().sum()
#                 if new_nan_count > initial_nan_count:
#                     warnings.warn(str(_l("Remplacement de %s valeurs hors limites dans '%s' par NaN.") % (new_nan_count - initial_nan_count, var)))
            
#             print(_l("DEBUG (interpolation/variable): Interpolation de '%s'. Type de l'index de df_final: %s") % (var, type(df_final.index)))
            
#             if isinstance(df_final.index, pd.DatetimeIndex):
#                 df_final[var] = df_final[var].interpolate(method='time', limit_direction='both')
#             else:
#                 print(_l("Avertissement (interpolation/variable): L'index n'est pas un DatetimeIndex pour l'interpolation de '%s'. Utilisation de la méthode 'linear'.") % var)
#                 df_final[var] = df_final[var].interpolate(method='linear', limit_direction='both')
#             df_final[var] = df_final[var].bfill().ffill()

#     if 'Solar_R_W/m^2' in df_final.columns:
#         df_final['Solar_R_W/m^2'] = pd.to_numeric(df_final['Solar_R_W/m^2'], errors='coerce')

#         if 'Solar_R_W/m^2' in limits:
#             min_val = limits['Solar_R_W/m^2']['min']
#             max_val = limits['Solar_R_W/m^2']['max']
#             initial_nan_count = df_final['Solar_R_W/m^2'].isna().sum()
#             df_final.loc[(df_final['Solar_R_W/m^2'] < min_val) | (df_final['Solar_R_W/m^2'] > max_val), 'Solar_R_W/m^2'] = np.nan
#             if df_final['Solar_R_W/m^2'].isna().sum() > initial_nan_count:
#                 warnings.warn(str(_l("Remplacement de %s valeurs hors limites dans 'Solar_R_W/m^2' par NaN.") % (df_final['Solar_R_W/m^2'].isna().sum() - initial_nan_count)))

#         if 'Is_Daylight' in df_final.columns:
#             df_final.loc[~df_final['Is_Daylight'] & (df_final['Solar_R_W/m^2'] > 0), 'Solar_R_W/m^2'] = 0

#             if 'Rain_mm' in df_final.columns:
#                 cond_suspect_zeros = (df_final['Is_Daylight']) & (df_final['Solar_R_W/m^2'] == 0) & (df_final['Rain_mm'] == 0)
#             else:
#                 cond_suspect_zeros = (df_final['Is_Daylight']) & (df_final['Solar_R_W/m^2'] == 0)
#                 warnings.warn(str(_l("Rain_mm manquant. Tous les 0 de radiation solaire pendant le jour sont traités comme suspects.")))
#             df_final.loc[cond_suspect_zeros, 'Solar_R_W/m^2'] = np.nan

#             print(_l("DEBUG (interpolation/solaire): Interpolation de 'Solar_R_W/m^2' (conditionnel). Type de l'index de df_final: %s") % type(df_final.index))

#             is_day = df_final['Is_Daylight']
#             if isinstance(df_final.index, pd.DatetimeIndex):
#                 df_final.loc[is_day, 'Solar_R_W/m^2'] = df_final.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='time', limit_direction='both')
#             else:
#                 print(_l("Avertissement (interpolation/solaire): L'index n'est pas un DatetimeIndex pour l'interpolation de 'Solar_R_W/m^2'. Utilisation de la méthode 'linear'.") % type(df_final.index))
#                 df_final.loc[is_day, 'Solar_R_W/m^2'] = df_final.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='linear', limit_direction='both')

#             df_final.loc[is_day, 'Solar_R_W/m^2'] = df_final.loc[is_day, 'Solar_R_W/m^2'].bfill().ffill()

#             df_final.loc[~is_day & df_final['Solar_R_W/m^2'].isna(), 'Solar_R_W/m^2'] = 0
#             warnings.warn(str(_l("Radiation solaire interpolée avec succès.")))
#         else:
#             warnings.warn(str(_l("Colonne 'Is_Daylight' manquante. Radiation solaire interpolée standard.")))
#             if isinstance(df_final.index, pd.DatetimeIndex):
#                  df_final['Solar_R_W/m^2'] = df_final['Solar_R_W/m^2'].interpolate(method='time', limit_direction='both').bfill().ffill()
#             else:
#                  df_final['Solar_R_W/m^2'] = df_final['Solar_R_W/m^2'].interpolate(method='linear', limit_direction='both').bfill().ffill()

#     warnings.warn(str(_l("Vérification des valeurs manquantes après interpolation:")))
#     missing_after_interp = df_final.isna().sum()
#     columns_with_missing = missing_after_interp[missing_after_interp > 0]
#     if not columns_with_missing.empty:
#         warnings.warn(str(_l("Valeurs manquantes persistantes:\n%s") % columns_with_missing))
#     else:
#         warnings.warn(str(_l("Aucune valeur manquante après l'interpolation.")))

#     return df_final







# def interpolation(df: pd.DataFrame, limits: dict, df_gps: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
#     """
#     Effectue le nettoyage, l'application des limites et l'interpolation des données météorologiques.
#     Cette fonction retourne deux DataFrames :
#     1. Le DataFrame après nettoyage et application des limites (avec NaNs pour les valeurs manquantes/outliers).
#     2. Le DataFrame entièrement interpolé (sans NaNs).

#     Args:
#         df (pd.DataFrame): Le DataFrame d'entrée avec DatetimeIndex et colonne 'Station'.
#         limits (dict): Dictionnaire définissant les limites de valeurs pour chaque variable.
#         df_gps (pd.DataFrame): Le DataFrame contenant les informations de station
#                                (colonnes 'Station', 'Lat', 'Long', 'Timezone').

#     Returns:
#         tuple[pd.DataFrame, pd.DataFrame]:
#             - Le premier DataFrame contient les données après nettoyage et mise en NaN des outliers, mais AVANT interpolation.
#             - Le deuxième DataFrame contient les données entièrement interpolées.
#     """
#     df_temp_processed = df.copy()

#     # --- Vérifications initiales (identiques à l'original) ---
#     if not isinstance(df_temp_processed.index, pd.DatetimeIndex):
#         raise TypeError(str(_l("Le DataFrame d'entrée pour l'interpolation DOIT avoir un DatetimeIndex.")))
    
#     initial_rows = len(df_temp_processed)
#     df_temp_processed = df_temp_processed[df_temp_processed.index.notna()]
#     if len(df_temp_processed) == 0:
#         raise ValueError(str(_l("Après nettoyage des index temporels manquants, le DataFrame est vide. Impossible de procéder à l'interpolation.")))
#     if initial_rows - len(df_temp_processed) > 0:
#         warnings.warn(str(_l("Suppression de %s lignes avec index Datetime manquant ou invalide.") % (initial_rows - len(df_temp_processed))))
    
#     print(_l("DEBUG (interpolation): Type de l'index du DataFrame initial: %s") % type(df_temp_processed.index))
#     print(_l("DEBUG (interpolation): Premières 5 valeurs de l'index après nettoyage des NaT: %s") % (df_temp_processed.index[:5].tolist() if not df_temp_processed.empty else 'DataFrame vide'))

#     required_gps_cols = ['Station', 'Lat', 'Long', 'Timezone']
#     if not all(col in df_gps.columns for col in required_gps_cols):
#         raise ValueError(
#             str(_l("df_gps doit contenir les colonnes %s. Colonnes actuelles dans df_gps : %s") % \
#             (required_gps_cols, df_gps.columns.tolist()))
#         )

#     if not df_gps['Station'].is_unique:
#         print(_l("Avertissement: La colonne 'Station' dans df_gps contient des noms de station dupliqués."))
#         print(_l("Ceci peut entraîner des comportements inattendus ou des stations non reconnues."))
#         df_gps_unique = df_gps.drop_duplicates(subset=['Station'], keep='first').copy()
#         print(_l("Suppression de %s doublons dans df_gps (en gardant la première occurrence).") % (len(df_gps) - len(df_gps_unique)))
#     else:
#         df_gps_unique = df_gps.copy()

#     gps_info_dict = df_gps_unique.set_index('Station')[['Lat', 'Long', 'Timezone']].to_dict('index')

#     numerical_cols = ['Air_Temp_Deg_C', 'Rel_H_%', 'BP_mbar_Avg',
#                       'Rain_01_mm', 'Rain_02_mm', 'Rain_mm', 'Wind_Sp_m/sec',
#                       'Solar_R_W/m^2', 'Wind_Dir_Deg']
    
#     # --- PHASE 1: Nettoyage, application des limites et calcul des auxiliaires (AVANT INTERPOLATION) ---
#     # Appliquer les limites et convertir en NaN pour les colonnes numériques
#     for col in numerical_cols:
#         if col in df_temp_processed.columns:
#             df_temp_processed[col] = pd.to_numeric(df_temp_processed[col], errors='coerce')
#             if col in limits:
#                 min_val = limits[col]['min']
#                 max_val = limits[col]['max']
#                 initial_nan_count = df_temp_processed[col].isna().sum()
#                 if min_val is not None:
#                     df_temp_processed.loc[df_temp_processed[col] < min_val, col] = np.nan
#                 if max_val is not None:
#                     df_temp_processed.loc[df_temp_processed[col] > max_val, col] = np.nan
                
#                 new_nan_count = df_temp_processed[col].isna().sum()
#                 if new_nan_count > initial_nan_count:
#                     warnings.warn(str(_l("Remplacement de %s valeurs hors limites dans '%s' par NaN (pré-interpolation).") % (new_nan_count - initial_nan_count, col)))

#     df_parts_before_interp = []

#     for station_name, group in df_temp_processed.groupby('Station'):
#         group_copy = group.copy()
#         print(_l("DEBUG (interpolation/groupby): Début du traitement du groupe '%s' pour l'étape pré-interpolation.") % station_name)
        
#         if group_copy.index.tz is None:
#             group_copy.index = group_copy.index.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
#         elif group_copy.index.tz != pytz.utc:
#             group_copy.index = group_copy.index.tz_convert('UTC')
        
#         group_copy = group_copy[group_copy.index.notna()]
#         if group_copy.empty:
#             warnings.warn(str(_l("Le groupe '%s' est vide après nettoyage de l'index Datetime. Il sera ignoré pour l'interpolation.") % station_name))
#             continue

#         # Calcul Is_Daylight et Daylight_Duration
#         apply_fixed_daylight = True
#         gps_data = gps_info_dict.get(station_name)
#         if gps_data and pd.notna(gps_data.get('Lat')) and pd.notna(gps_data.get('Long')) and pd.notna(gps_data.get('Timezone')):
#             lat = gps_data['Lat']
#             long = gps_data['Long']
#             timezone_str = gps_data['Timezone']

#             try:
#                 local_tz = pytz.timezone(timezone_str)
#                 index_for_astral_local = group_copy.index.tz_convert(local_tz)

#                 daily_sun_info = {}
#                 unique_dates_ts_local = index_for_astral_local.normalize().drop_duplicates()

#                 if unique_dates_ts_local.empty:
#                     raise ValueError(str(_l("Aucune date unique trouvée pour le calcul Astral.")))
                
#                 for ts_local_aware in unique_dates_ts_local:
#                     loc = LocationInfo(station_name, "Site", timezone_str, lat, long)
#                     naive_date_for_astral = ts_local_aware.to_pydatetime().date()
#                     s = sun.sun(loc.observer, date=naive_date_for_astral) 
#                     daily_sun_info[naive_date_for_astral] = {
#                         'sunrise': s['sunrise'],
#                         'sunset': s['sunset']
#                     }

#                 temp_df_sun_index = pd.Index([ts.date() for ts in unique_dates_ts_local], name='Date_Local_Naive')
#                 temp_df_sun = pd.DataFrame(index=temp_df_sun_index)
                
#                 temp_df_sun['sunrise_time_local'] = [daily_sun_info.get(date, {}).get('sunrise') for date in temp_df_sun.index]
#                 temp_df_sun['sunset_time_local'] = [daily_sun_info.get(date, {}).get('sunset') for date in temp_df_sun.index]

#                 group_copy_reset = group_copy.reset_index()
#                 group_copy_reset['Date_Local_Naive'] = group_copy_reset['Datetime'].dt.tz_convert(local_tz).dt.date

#                 group_copy_reset = pd.merge(group_copy_reset, temp_df_sun, on='Date_Local_Naive', how='left')

#                 group_copy_reset['sunrise_time_utc'] = group_copy_reset['sunrise_time_local'].dt.tz_convert('UTC')
#                 group_copy_reset['sunset_time_utc'] = group_copy_reset['sunset_time_local'].dt.tz_convert('UTC')

#                 group_copy_reset.loc[:, 'Is_Daylight'] = (group_copy_reset['Datetime'] >= group_copy_reset['sunrise_time_utc']) & \
#                                                           (group_copy_reset['Datetime'] < group_copy_reset['sunset_time_utc'])

#                 daylight_timedelta_local = group_copy_reset['sunset_time_local'] - group_copy_reset['sunrise_time_local']
                
#                 def format_timedelta_to_hms(td):
#                     if pd.isna(td):
#                         return np.nan
#                     total_seconds = int(td.total_seconds())
#                     hours = total_seconds // 3600
#                     minutes = (total_seconds % 3600) // 60
#                     seconds = total_seconds % 60
#                     return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

#                 group_copy_reset.loc[:, 'Daylight_Duration'] = daylight_timedelta_local.apply(format_timedelta_to_hms)

#                 group_copy = group_copy_reset.set_index('Datetime')
#                 group_copy = group_copy.drop(columns=['Date_Local_Naive', 'sunrise_time_local', 'sunset_time_local', 'sunrise_time_utc', 'sunset_time_utc'], errors='ignore')

#                 print(_l("Lever et coucher du soleil calculés pour %s.") % station_name)
#                 apply_fixed_daylight = False

#             except Exception as e:
#                 print(_l("Erreur lors du calcul du lever/coucher du soleil avec Astral pour %s: %s.") % (station_name, e))
#                 traceback.print_exc()
#                 warnings.warn(str(_l("Calcul Astral impossible pour '%s'. Utilisation de l'indicateur jour/nuit fixe.") % station_name))
#                 apply_fixed_daylight = True
#         else:
#             print(_l("Avertissement: Coordonnées ou Fuseau horaire manquants/invalides pour le site '%s' dans df_gps. Utilisation de l'indicateur jour/nuit fixe.") % station_name)
#             apply_fixed_daylight = True

#         if apply_fixed_daylight:
#             group_copy.loc[:, 'Is_Daylight'] = (group_copy.index.hour >= 7) & (group_copy.index.hour <= 18)
#             group_copy.loc[:, 'Daylight_Duration'] = "11:00:00"
#             print(_l("Utilisation de l'indicateur jour/nuit fixe (7h-18h) pour %s.") % station_name)
        
#         df_parts_before_interp.append(group_copy)

#     if not df_parts_before_interp:
#         return pd.DataFrame(), pd.DataFrame()

#     df_before_interpolation = pd.concat(df_parts_before_interp).sort_index()
#     df_before_interpolation.index.name = 'Datetime'

#     # Gestion de 'Rain_mm'
#     if 'Rain_mm' not in df_before_interpolation.columns or df_before_interpolation['Rain_mm'].isnull().all():
#         if 'Rain_01_mm' in df_before_interpolation.columns and 'Rain_02_mm' in df_before_interpolation.columns:
#             df_before_interpolation = create_rain_mm(df_before_interpolation)
#             warnings.warn(str(_l("Colonne Rain_mm créée à partir des deux capteurs (pré-interpolation).")))
#         else:
#             warnings.warn(str(_l("Rain_mm manquant et impossible à créer (capteurs pluie incomplets) (pré-interpolation).")))
#             if 'Rain_mm' not in df_before_interpolation.columns:
#                 df_before_interpolation['Rain_mm'] = np.nan

#     # NOUVELLE LOGIQUE POUR SOLAR_R_W/m^2 EN PHASE 1:
#     # Appliquer la règle "toute valeur de radiation solaire en dehors du jour est 0" dès maintenant
#     if 'Solar_R_W/m^2' in df_before_interpolation.columns and 'Is_Daylight' in df_before_interpolation.columns:
#         # Forcer TOUTES les valeurs de Solar_R_W/m^2 à 0 quand il fait nuit
#         # Cela inclut les NaNs et les valeurs positives erronées
#         df_before_interpolation.loc[~df_before_interpolation['Is_Daylight'], 'Solar_R_W/m^2'] = 0
#         warnings.warn(str(_l("Toutes les valeurs de Solar_R_W/m^2 en dehors des heures de jour ont été mises à zéro dans df_before_interpolation.")))

#         # Ensuite, le traitement des zéros suspects pendant le JOUR (si Is_Daylight est True)
#         # Ici, on recherche les zéros suspects PENDANT LA JOURNÉE
#         cond_suspect_zeros = (df_before_interpolation['Is_Daylight']) & \
#                              (df_before_interpolation['Solar_R_W/m^2'] == 0) & \
#                              (df_before_interpolation['Rain_mm'] == 0 if 'Rain_mm' in df_before_interpolation.columns else True)
        
#         if 'Rain_mm' not in df_before_interpolation.columns:
#              warnings.warn(str(_l("Rain_mm manquant. Tous les 0 de radiation solaire pendant le jour sont traités comme suspects pour l'étape pré-interpolation.")))

#         df_before_interpolation.loc[cond_suspect_zeros, 'Solar_R_W/m^2'] = np.nan
#         warnings.warn(str(_l("Zéros suspects de radiation solaire pendant le jour mis à NaN dans df_before_interpolation.")))
    
#     cols_to_drop_after_process = ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
#     df_before_interpolation = df_before_interpolation.drop(columns=cols_to_drop_after_process, errors='ignore')

#     warnings.warn(str(_l("Vérification des valeurs manquantes dans le DataFrame AVANT interpolation:")))
#     missing_before_interp = df_before_interpolation.isna().sum()
#     columns_with_missing_before = missing_before_interp[missing_before_interp > 0]
#     if not columns_with_missing_before.empty:
#         warnings.warn(str(_l("Valeurs manquantes persistantes AVANT interpolation:\n%s") % columns_with_missing_before))
#     else:
#         warnings.warn(str(_l("Aucune valeur manquante après le prétraitement et la mise en NaN (AVANT interpolation).")))


#     # --- PHASE 2: Interpolation des valeurs (création de df_after_interpolation) ---
#     df_after_interpolation = df_before_interpolation.copy()

#     # Interpolation des variables numériques standard
#     for var in numerical_cols:
#         if var in df_after_interpolation.columns:
#             if isinstance(df_after_interpolation.index, pd.DatetimeIndex):
#                 df_after_interpolation[var] = df_after_interpolation[var].interpolate(method='time', limit_direction='both')
#             else:
#                 warnings.warn(str(_l("Avertissement (interpolation/variable): L'index n'est pas un DatetimeIndex pour l'interpolation de '%s'. Utilisation de la méthode 'linear'.") % var))
#                 df_after_interpolation[var] = df_after_interpolation[var].interpolate(method='linear', limit_direction='both')
#             df_after_interpolation[var] = df_after_interpolation[var].bfill().ffill()


#     # Traitement spécifique pour Solar_R_W/m^2 avec interpolation
#     if 'Solar_R_W/m^2' in df_after_interpolation.columns:
#         if 'Is_Daylight' in df_after_interpolation.columns:
#             is_day = df_after_interpolation['Is_Daylight']
            
#             # Interpoler uniquement pendant le jour
#             if isinstance(df_after_interpolation.index, pd.DatetimeIndex):
#                 df_after_interpolation.loc[is_day, 'Solar_R_W/m^2'] = df_after_interpolation.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='time', limit_direction='both')
#             else:
#                 warnings.warn(str(_l("Avertissement (interpolation/solaire): L'index n'est pas un DatetimeIndex pour l'interpolation de 'Solar_R_W/m^2'. Utilisation de la méthode 'linear'.")))
#                 df_after_interpolation.loc[is_day, 'Solar_R_W/m^2'] = df_after_interpolation.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='linear', limit_direction='both')
            
#             # Combler les éventuels NaN restants après interpolation pendant le jour
#             df_after_interpolation.loc[is_day, 'Solar_R_W/m^2'] = df_after_interpolation.loc[is_day, 'Solar_R_W/m^2'].bfill().ffill()

#             # Forcer TOUTES les valeurs de Solar_R_W/m^2 à 0 quand il fait nuit (redondant mais sécurisant après interpolation)
#             df_after_interpolation.loc[~is_day, 'Solar_R_W/m^2'] = 0
#             warnings.warn(str(_l("Radiation solaire interpolée et valeurs nocturnes assurées à zéro dans df_after_interpolation.")))
#         else:
#             warnings.warn(str(_l("Colonne 'Is_Daylight' manquante. Radiation solaire interpolée standard dans df_after_interpolation.")))
#             if isinstance(df_after_interpolation.index, pd.DatetimeIndex):
#                  df_after_interpolation['Solar_R_W/m^2'] = df_after_interpolation['Solar_R_W/m^2'].interpolate(method='time', limit_direction='both').bfill().ffill()
#             else:
#                  df_after_interpolation['Solar_R_W/m^2'] = df_after_interpolation['Solar_R_W/m^2'].interpolate(method='linear', limit_direction='both').bfill().ffill()

#     warnings.warn(str(_l("Vérification des valeurs manquantes dans le DataFrame APRÈS interpolation:")))
#     missing_after_interp = df_after_interpolation.isna().sum()
#     columns_with_missing_after = missing_after_interp[missing_after_interp > 0]
#     if not columns_with_missing_after.empty:
#         warnings.warn(str(_l("Valeurs manquantes persistantes APRÈS interpolation:\n%s") % columns_with_missing_after))
#     else:
#         warnings.warn(str(_l("Aucune valeur manquante après l'interpolation.")))

#     return df_before_interpolation, df_after_interpolation


# def _get_missing_ranges(series: pd.Series, station_name: str, variable_name: str) -> list[dict]:
#     """
#     Détermine les plages de valeurs manquantes (NaN) dans une série temporelle.

#     Args:
#         series (pd.Series): La série à analyser, avec un DatetimeIndex.
#         station_name (str): Le nom de la station associée à la série.
#         variable_name (str): Le nom de la variable de la série.

#     Returns:
#         list[dict]: Une liste de dictionnaires, chacun décrivant une plage manquante
#                     avec 'station', 'variable', 'start_time', 'end_time', et 'duration_hours'.
#     """
#     if not isinstance(series.index, pd.DatetimeIndex):
#         warnings.warn(f"Impossible de déterminer les plages manquantes pour {variable_name} ({station_name}): L'index n'est pas un DatetimeIndex.")
#         return []

#     missing_ranges_data = []
    
#     is_nan = series.isna()
    
#     starts = is_nan & (~is_nan.shift(1, fill_value=False))
#     ends = is_nan & (~is_nan.shift(-1, fill_value=False))

#     start_indices = series.index[starts].tolist()
#     end_indices = series.index[ends].tolist()

#     # Ajustement pour les cas où les NaNs sont au début/fin de la série
#     if len(start_indices) > len(end_indices):
#         end_indices.append(series.index.max()) # Utilise la fin de la série comme fin de la dernière plage NaN
#     elif len(end_indices) > len(start_indices):
#         start_indices.insert(0, series.index.min()) # Utilise le début de la série comme début de la première plage NaN

#     for start, end in zip(start_indices, end_indices):
#         duration_timedelta = end - start
        
#         missing_ranges_data.append({
#             'station': station_name,
#             'variable': variable_name,
#             'start_time': start, # Gardons les objets datetime pour le DataFrame
#             'end_time': end,     # Gardons les objets datetime pour le DataFrame
#             'duration_hours': duration_timedelta.total_seconds() / 3600
#         })
#     return missing_ranges_data


# def interpolation(df: pd.DataFrame, limits: dict, df_gps: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
#     """
#     Effectue le nettoyage, l'application des limites et l'interpolation des données météorologiques.
#     Cette fonction retourne quatre DataFrames :
#     1. Le DataFrame après nettoyage et application des limites (avec NaNs pour les valeurs manquantes/outliers).
#     2. Le DataFrame entièrement interpolé (sans NaNs).
#     3. Un DataFrame récapitulant les plages de valeurs manquantes pour chaque variable AVANT interpolation.
#     4. Un DataFrame récapitulant les plages de valeurs manquantes pour chaque variable APRÈS interpolation.

#     Args:
#         df (pd.DataFrame): Le DataFrame d'entrée avec DatetimeIndex et colonne 'Station'.
#         limits (dict): Dictionnaire définissant les limites de valeurs pour chaque variable.
#         df_gps (pd.DataFrame): Le DataFrame contenant les informations de station
#                                (colonnes 'Station', 'Lat', 'Long', 'Timezone').

#     Returns:
#         tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
#             - Le premier DataFrame contient les données après nettoyage et mise en NaN des outliers, mais AVANT interpolation.
#             - Le deuxième DataFrame contient les données entièrement interpolées.
#             - Le troisième est un DataFrame (cols: 'station', 'variable', 'start_time', 'end_time', 'duration_hours') AVANT interpolation.
#             - Le quatrième est un DataFrame (cols: 'station', 'variable', 'start_time', 'end_time', 'duration_hours') APRÈS interpolation.
#     """
#     df_temp_processed = df.copy()

#     # --- Vérifications initiales ---
#     if not isinstance(df_temp_processed.index, pd.DatetimeIndex):
#         raise TypeError(str(_l("Le DataFrame d'entrée pour l'interpolation DOIT avoir un DatetimeIndex.")))
    
#     initial_rows = len(df_temp_processed)
#     df_temp_processed = df_temp_processed[df_temp_processed.index.notna()]
#     if len(df_temp_processed) == 0:
#         raise ValueError(str(_l("Après nettoyage des index temporels manquants, le DataFrame est vide. Impossible de procéder à l'interpolation.")))
#     if initial_rows - len(df_temp_processed) > 0:
#         warnings.warn(str(_l("Suppression de %s lignes avec index Datetime manquant ou invalide.") % (initial_rows - len(df_temp_processed))))
    
#     print(_l("DEBUG (interpolation): Type de l'index du DataFrame initial: %s") % type(df_temp_processed.index))
#     print(_l("DEBUG (interpolation): Premières 5 valeurs de l'index après nettoyage des NaT: %s") % (df_temp_processed.index[:5].tolist() if not df_temp_processed.empty else 'DataFrame vide'))

#     required_gps_cols = ['Station', 'Lat', 'Long', 'Timezone']
#     if not all(col in df_gps.columns for col in required_gps_cols):
#         raise ValueError(
#             str(_l("df_gps doit contenir les colonnes %s. Colonnes actuelles dans df_gps : %s") % \
#             (required_gps_cols, df_gps.columns.tolist()))
#         )

#     if not df_gps['Station'].is_unique:
#         print(_l("Avertissement: La colonne 'Station' dans df_gps contient des noms de station dupliqués."))
#         print(_l("Ceci peut entraîner des comportements inattendus ou des stations non reconnues."))
#         df_gps_unique = df_gps.drop_duplicates(subset=['Station'], keep='first').copy()
#         print(_l("Suppression de %s doublons dans df_gps (en gardant la première occurrence).") % (len(df_gps) - len(df_gps_unique)))
#     else:
#         df_gps_unique = df_gps.copy()

#     gps_info_dict = df_gps_unique.set_index('Station')[['Lat', 'Long', 'Timezone']].to_dict('index')

#     numerical_cols = ['Air_Temp_Deg_C', 'Rel_H_%', 'BP_mbar_Avg',
#                       'Rain_01_mm', 'Rain_02_mm', 'Rain_mm', 'Wind_Sp_m/sec',
#                       'Solar_R_W/m^2', 'Wind_Dir_Deg']
    
#     # --- PHASE 1: Nettoyage, application des limites et calcul des auxiliaires (AVANT INTERPOLATION) ---
#     for col in numerical_cols:
#         if col in df_temp_processed.columns:
#             df_temp_processed[col] = pd.to_numeric(df_temp_processed[col], errors='coerce')
#             if col in limits:
#                 min_val = limits[col]['min']
#                 max_val = limits[col]['max']
#                 initial_nan_count = df_temp_processed[col].isna().sum()
#                 if min_val is not None:
#                     df_temp_processed.loc[df_temp_processed[col] < min_val, col] = np.nan
#                 if max_val is not None:
#                     df_temp_processed.loc[df_temp_processed[col] > max_val, col] = np.nan
                
#                 new_nan_count = df_temp_processed[col].isna().sum()
#                 if new_nan_count > initial_nan_count:
#                     warnings.warn(str(_l("Remplacement de %s valeurs hors limites dans '%s' par NaN (pré-interpolation).") % (new_nan_count - initial_nan_count, col)))

#     df_parts_before_interp = []
    
#     # Liste pour collecter tous les dictionnaires de plages manquantes (avant interpolation)
#     all_missing_ranges_before_interp_list = [] 

#     for station_name, group in df_temp_processed.groupby('Station'):
#         group_copy = group.copy()
#         print(_l("DEBUG (interpolation/groupby): Début du traitement du groupe '%s' pour l'étape pré-interpolation.") % station_name)
        
#         if group_copy.index.tz is None:
#             group_copy.index = group_copy.index.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
#         elif group_copy.index.tz != pytz.utc:
#             group_copy.index = group_copy.index.tz_convert('UTC')
        
#         group_copy = group_copy[group_copy.index.notna()]
#         if group_copy.empty:
#             warnings.warn(str(_l("Le groupe '%s' est vide après nettoyage de l'index Datetime. Il sera ignoré pour l'interpolation.") % station_name))
#             continue

#         # Initialiser les colonnes de lever/coucher du soleil
#         group_copy.loc[:, 'Is_Daylight'] = False
#         group_copy.loc[:, 'Daylight_Duration'] = pd.NA
#         group_copy.loc[:, 'sunrise_time_utc'] = pd.NaT 
#         group_copy.loc[:, 'sunset_time_utc'] = pd.NaT 

#         apply_fixed_daylight = True
#         gps_data = gps_info_dict.get(station_name)
#         if gps_data and pd.notna(gps_data.get('Lat')) and pd.notna(gps_data.get('Long')) and pd.notna(gps_data.get('Timezone')):
#             lat = gps_data['Lat']
#             long = gps_data['Long']
#             timezone_str = gps_data['Timezone']

#             try:
#                 local_tz = pytz.timezone(timezone_str)
#                 index_for_astral_local = group_copy.index.tz_convert(local_tz)

#                 daily_sun_info = {}
#                 unique_dates_ts_local = index_for_astral_local.normalize().drop_duplicates()

#                 if unique_dates_ts_local.empty:
#                     raise ValueError(str(_l("Aucune date unique trouvée pour le calcul Astral.")))
                
#                 for ts_local_aware in unique_dates_ts_local:
#                     loc = LocationInfo(station_name, "Site", timezone_str, lat, long)
#                     naive_date_for_astral = ts_local_aware.to_pydatetime().date()
#                     s = sun.sun(loc.observer, date=naive_date_for_astral) 
#                     daily_sun_info[naive_date_for_astral] = {
#                         'sunrise': s['sunrise'],
#                         'sunset': s['sunset']
#                     }

#                 temp_df_sun_index = pd.Index([ts.date() for ts in unique_dates_ts_local], name='Date_Local_Naive')
#                 temp_df_sun = pd.DataFrame(index=temp_df_sun_index)
                
#                 temp_df_sun['sunrise_time_local'] = [daily_sun_info.get(date, {}).get('sunrise') for date in temp_df_sun.index]
#                 temp_df_sun['sunset_time_local'] = [daily_sun_info.get(date, {}).get('sunset') for date in temp_df_sun.index]

#                 group_copy_reset = group_copy.reset_index()
#                 group_copy_reset['Date_Local_Naive'] = group_copy_reset['Datetime'].dt.tz_convert(local_tz).dt.date

#                 group_copy_reset = pd.merge(group_copy_reset, temp_df_sun, on='Date_Local_Naive', how='left')

#                 group_copy_reset['sunrise_time_utc'] = group_copy_reset['sunrise_time_local'].dt.tz_convert('UTC')
#                 group_copy_reset['sunset_time_utc'] = group_copy_reset['sunset_time_local'].dt.tz_convert('UTC')

#                 group_copy_reset.loc[:, 'Is_Daylight'] = (group_copy_reset['Datetime'] >= group_copy_reset['sunrise_time_utc']) & \
#                                                           (group_copy_reset['Datetime'] < group_copy_reset['sunset_time_utc'])

#                 daylight_timedelta_local = group_copy_reset['sunset_time_local'] - group_copy_reset['sunrise_time_local']
                
#                 def format_timedelta_to_hms(td):
#                     if pd.isna(td):
#                         return np.nan
#                     total_seconds = int(td.total_seconds())
#                     hours = total_seconds // 3600
#                     minutes = (total_seconds % 3600) // 60
#                     seconds = total_seconds % 60
#                     return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

#                 group_copy_reset.loc[:, 'Daylight_Duration'] = daylight_timedelta_local.apply(format_timedelta_to_hms)

#                 group_copy = group_copy_reset.set_index('Datetime')
#                 group_copy = group_copy.drop(columns=['Date_Local_Naive', 'sunrise_time_local', 'sunset_time_local'], errors='ignore')

#                 print(_l("Lever et coucher du soleil calculés pour %s.") % station_name)
#                 apply_fixed_daylight = False

#             except Exception as e:
#                 print(_l("Erreur lors du calcul du lever/coucher du soleil avec Astral pour %s: %s.") % (station_name, e))
#                 traceback.print_exc()
#                 warnings.warn(str(_l("Calcul Astral impossible pour '%s'. Utilisation de l'indicateur jour/nuit fixe.") % station_name))
#                 apply_fixed_daylight = True
#         else:
#             print(_l("Avertissement: Coordonnées ou Fuseau horaire manquants/invalides pour le site '%s' dans df_gps. Utilisation de l'indicateur jour/nuit fixe.") % station_name)
#             apply_fixed_daylight = True

#         if apply_fixed_daylight:
#             group_copy.loc[:, 'Is_Daylight'] = (group_copy.index.hour >= 7) & (group_copy.index.hour <= 18)
#             group_copy.loc[:, 'Daylight_Duration'] = "11:00:00"
#             group_copy.loc[:, 'sunrise_time_utc'] = pd.NaT 
#             group_copy.loc[:, 'sunset_time_utc'] = pd.NaT 
#             print(_l("Utilisation de l'indicateur jour/nuit fixe (7h-18h) pour %s.") % station_name)
        
#         df_parts_before_interp.append(group_copy)

#     if not df_parts_before_interp:
#         # Retourne des DataFrames vides pour les plages manquantes si le DF principal est vide
#         return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame() 

#     df_before_interpolation = pd.concat(df_parts_before_interp).sort_index()
#     df_before_interpolation.index.name = 'Datetime'

#     # Gestion de 'Rain_mm'
#     if 'Rain_mm' not in df_before_interpolation.columns or df_before_interpolation['Rain_mm'].isnull().all():
#         if 'Rain_01_mm' in df_before_interpolation.columns and 'Rain_02_mm' in df_before_interpolation.columns:
#             df_before_interpolation = create_rain_mm(df_before_interpolation)
#             warnings.warn(str(_l("Colonne Rain_mm créée à partir des deux capteurs (pré-interpolation).")))
#         else:
#             warnings.warn(str(_l("Rain_mm manquant et impossible à créer (capteurs pluie incomplets) (pré-interpolation).")))
#             if 'Rain_mm' not in df_before_interpolation.columns:
#                 df_before_interpolation['Rain_mm'] = np.nan

#     # NOUVELLE LOGIQUE POUR SOLAR_R_W/m^2 EN PHASE 1:
#     if 'Solar_R_W/m^2' in df_before_interpolation.columns and 'Is_Daylight' in df_before_interpolation.columns:
#         df_before_interpolation.loc[~df_before_interpolation['Is_Daylight'], 'Solar_R_W/m^2'] = 0
#         warnings.warn(str(_l("Toutes les valeurs de Solar_R_W/m^2 en dehors des heures de jour ont été mises à zéro dans df_before_interpolation.")))

#         cond_suspect_zeros = (df_before_interpolation['Is_Daylight']) & \
#                              (df_before_interpolation['Solar_R_W/m^2'] == 0) & \
#                              (df_before_interpolation['Rain_mm'] == 0 if 'Rain_mm' in df_before_interpolation.columns else True)
        
#         if 'Rain_mm' not in df_before_interpolation.columns:
#              warnings.warn(str(_l("Rain_mm manquant. Tous les 0 de radiation solaire pendant le jour sont traités comme suspects pour l'étape pré-interpolation.")))

#         df_before_interpolation.loc[cond_suspect_zeros, 'Solar_R_W/m^2'] = np.nan
#         warnings.warn(str(_l("Zéros suspects de radiation solaire pendant le jour mis à NaN dans df_before_interpolation.")))
    
#     if 'sunrise_time_utc' not in df_before_interpolation.columns:
#         df_before_interpolation['sunrise_time_utc'] = pd.NaT
#     if 'sunset_time_utc' not in df_before_interpolation.columns:
#         df_before_interpolation['sunset_time_utc'] = pd.NaT

#     cols_to_drop_after_process = ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
#     df_before_interpolation = df_before_interpolation.drop(columns=cols_to_drop_after_process, errors='ignore')

#     warnings.warn(str(_l("Vérification des valeurs manquantes dans le DataFrame AVANT interpolation:")))
#     missing_before_interp_counts = df_before_interpolation.isna().sum()
#     columns_with_missing_before = missing_before_interp_counts[missing_before_interp_counts > 0]
#     if not columns_with_missing_before.empty:
#         warnings.warn(str(_l("Valeurs manquantes persistantes AVANT interpolation:\n%s") % columns_with_missing_before))
#     else:
#         warnings.warn(str(_l("Aucune valeur manquante après le prétraitement et la mise en NaN (AVANT interpolation).")))

#     # DÉTERMINER LES PLAGES MANQUANTES AVANT INTERPOLATION ET COLLECTER DANS UNE LISTE
#     interpolated_cols = [col for col in numerical_cols if col in df_before_interpolation.columns and col != 'Station'] # Exclure 'Station'
#     for var in interpolated_cols:
#         for station_name, group in df_before_interpolation.groupby('Station'):
#             all_missing_ranges_before_interp_list.extend(_get_missing_ranges(group[var], station_name, var))

#     # Convertir la liste de dictionnaires en DataFrame
#     df_missing_ranges_before_interp = pd.DataFrame(all_missing_ranges_before_interp_list)
#     if not df_missing_ranges_before_interp.empty:
#         df_missing_ranges_before_interp = df_missing_ranges_before_interp.sort_values(by=['station', 'variable', 'start_time'])
#     else:
#         df_missing_ranges_before_interp = pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours'])


#     # --- PHASE 2: Interpolation des valeurs (création de df_after_interpolation) ---
#     df_after_interpolation = df_before_interpolation.copy()

#     cols_to_interpolate_standard = [col for col in numerical_cols if col in df_after_interpolation.columns and col not in ['sunrise_time_utc', 'sunset_time_utc', 'Station']]

#     for var in cols_to_interpolate_standard:
#         if isinstance(df_after_interpolation.index, pd.DatetimeIndex):
#             df_after_interpolation[var] = df_after_interpolation[var].interpolate(method='time', limit_direction='both')
#         else:
#             warnings.warn(str(_l("Avertissement (interpolation/variable): L'index n'est pas un DatetimeIndex pour l'interpolation de '%s'. Utilisation de la méthode 'linear'.") % var))
#             df_after_interpolation[var] = df_after_interpolation[var].interpolate(method='linear', limit_direction='both')
#         df_after_interpolation[var] = df_after_interpolation[var].bfill().ffill()


#     # Traitement spécifique pour Solar_R_W/m^2 avec interpolation
#     if 'Solar_R_W/m^2' in df_after_interpolation.columns:
#         if 'Is_Daylight' in df_after_interpolation.columns:
#             is_day = df_after_interpolation['Is_Daylight']
            
#             if isinstance(df_after_interpolation.index, pd.DatetimeIndex):
#                 df_after_interpolation.loc[is_day, 'Solar_R_W/m^2'] = df_after_interpolation.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='time', limit_direction='both')
#             else:
#                 warnings.warn(str(_l("Avertissement (interpolation/solaire): L'index n'est pas un DatetimeIndex pour l'interpolation de 'Solar_R_W/m^2'. Utilisation de la méthode 'linear'.")))
#                 df_after_interpolation.loc[is_day, 'Solar_R_W/m^2'] = df_after_interpolation.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='linear', limit_direction='both')
            
#             df_after_interpolation.loc[is_day, 'Solar_R_W/m^2'] = df_after_interpolation.loc[is_day, 'Solar_R_W/m^2'].bfill().ffill()

#             df_after_interpolation.loc[~is_day, 'Solar_R_W/m^2'] = 0
#             warnings.warn(str(_l("Radiation solaire interpolée et valeurs nocturnes assurées à zéro dans df_after_interpolation.")))
#         else:
#             warnings.warn(str(_l("Colonne 'Is_Daylight' manquante. Radiation solaire interpolée standard dans df_after_interpolation.")))
#             if isinstance(df_after_interpolation.index, pd.DatetimeIndex):
#                  df_after_interpolation['Solar_R_W/m^2'] = df_after_interpolation['Solar_R_W/m^2'].interpolate(method='time', limit_direction='both').bfill().ffill()
#             else:
#                  df_after_interpolation['Solar_R_W/m^2'] = df_after_interpolation['Solar_R_W/m^2'].interpolate(method='linear', limit_direction='both').bfill().ffill()

#     warnings.warn(str(_l("Vérification des valeurs manquantes dans le DataFrame APRÈS interpolation:")))
#     missing_after_interp_counts = df_after_interpolation.isna().sum()
#     columns_with_missing_after = missing_after_interp_counts[missing_after_interp_counts > 0]
#     if not columns_with_missing_after.empty:
#         warnings.warn(str(_l("Valeurs manquantes persistantes APRÈS interpolation:\n%s") % columns_with_missing_after))
#     else:
#         warnings.warn(str(_l("Aucune valeur manquante après l'interpolation.")))

#     # DÉTERMINER LES PLAGES MANQUANTES APRÈS INTERPOLATION ET COLLECTER DANS UNE LISTE
#     all_missing_ranges_after_interp_list = []
#     for var in interpolated_cols: 
#         for station_name, group in df_after_interpolation.groupby('Station'):
#             all_missing_ranges_after_interp_list.extend(_get_missing_ranges(group[var], station_name, var))
        
#     # Convertir la liste de dictionnaires en DataFrame
#     df_missing_ranges_after_interp = pd.DataFrame(all_missing_ranges_after_interp_list)
#     if not df_missing_ranges_after_interp.empty:
#         df_missing_ranges_after_interp = df_missing_ranges_after_interp.sort_values(by=['station', 'variable', 'start_time'])
#     else:
#         df_missing_ranges_after_interp = pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours']) # Assure un DF vide avec les bonnes colonnes
        
#     return df_before_interpolation, df_after_interpolation, df_missing_ranges_before_interp, df_missing_ranges_after_interp


# def interpolation(df: pd.DataFrame, limits: dict, df_gps: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
#     """
#     Effectue le nettoyage, l'application des limites et l'interpolation des données météorologiques.
#     ...
#     """
#     df_temp_processed = df.copy()

#     # --- Vérifications initiales et nettoyage de l'index ---
#     if not isinstance(df_temp_processed.index, pd.DatetimeIndex):
#         raise TypeError(str(_l("Le DataFrame d'entrée pour l'interpolation DOIT avoir un DatetimeIndex.")))
    
#     initial_rows = len(df_temp_processed)
#     df_temp_processed = df_temp_processed[df_temp_processed.index.notna()]
#     if len(df_temp_processed) == 0:
#         warnings.warn(str(_l("Après nettoyage des index temporels manquants, le DataFrame est vide. Retourne des DataFrames vides.")))
#         return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours']), pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours'])
#     if initial_rows - len(df_temp_processed) > 0:
#         warnings.warn(str(_l("Suppression de %d lignes avec index Datetime manquant ou invalide.") % (initial_rows - len(df_temp_processed))))
    
#     if df_temp_processed.index.tz is None:
#         df_temp_processed.index = df_temp_processed.index.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
#     elif df_temp_processed.index.tz != pytz.utc:
#         df_temp_processed.index = df_temp_processed.index.tz_convert('UTC')
    
#     # Assurer que l'index a un nom, ce qui aide drop_duplicates
#     if df_temp_processed.index.name is None:
#         df_temp_processed.index.name = 'Datetime'

#     if 'Station' not in df_temp_processed.columns:
#         raise ValueError(str(_l("La colonne 'Station' est manquante dans le DataFrame d'entrée. Elle est requise.")))

#     initial_df_len = len(df_temp_processed)
    
#     # Correction pour KeyError: Index(['Datetime'], dtype='object')
#     # On va dédupliquer sur la colonne 'Station' ET sur l'index lui-même.
#     # Pour ce faire, il est souvent plus simple de réinitialiser l'index temporairement,
#     # dédupliquer, puis le remettre.
    
#     # Étape 1: Réinitialiser l'index pour que 'Datetime' devienne une colonne
#     df_temp_processed_reset = df_temp_processed.reset_index()
    
#     # Étape 2: Appliquer drop_duplicates sur les colonnes 'Station' et 'Datetime'
#     df_temp_processed_reset.drop_duplicates(subset=['Station', 'Datetime'], keep='first', inplace=True)
    
#     # Étape 3: Remettre 'Datetime' comme index
#     df_temp_processed = df_temp_processed_reset.set_index('Datetime')

#     if len(df_temp_processed) < initial_df_len:
#         warnings.warn(str(_l("Suppression de %d lignes dupliquées (même Datetime et Station).") % (initial_df_len - len(df_temp_processed))))

#     df_temp_processed = df_temp_processed.sort_index()


#     # ... (Le reste de votre fonction, qui devrait être inchangé par cette correction de drop_duplicates)
#     # ...

#     # Gestion de df_gps
#     required_gps_cols = ['Station', 'Lat', 'Long', 'Timezone']
#     if not all(col in df_gps.columns for col in required_gps_cols):
#         raise ValueError(
#             str(_l("df_gps doit contenir les colonnes %s. Colonnes actuelles dans df_gps : %s") % \
#             (required_gps_cols, df_gps.columns.tolist()))
#         )

#     df_gps_unique = df_gps.drop_duplicates(subset=['Station'], keep='first').copy()
#     if len(df_gps) > len(df_gps_unique):
#         warnings.warn(str(_l("Suppression de %d doublons dans df_gps (en gardant la première occurrence).") % (len(df_gps) - len(df_gps_unique))))
    
#     gps_info_dict = df_gps_unique.set_index('Station')[['Lat', 'Long', 'Timezone']].to_dict('index')

#     numerical_cols = ['Air_Temp_Deg_C', 'Rel_H_%', 'BP_mbar_Avg',
#                       'Rain_01_mm', 'Rain_02_mm', 'Rain_mm', 'Wind_Sp_m/sec',
#                       'Solar_R_W/m^2', 'Wind_Dir_Deg']
    
#     # --- PHASE 1: Nettoyage et application des limites (AVANT INTERPOLATION) ---
#     for col in numerical_cols:
#         if col in df_temp_processed.columns:
#             df_temp_processed[col] = pd.to_numeric(df_temp_processed[col], errors='coerce')
            
#             if col in limits:
#                 min_val = limits[col].get('min')
#                 max_val = limits[col].get('max')
                
#                 if min_val is not None:
#                     df_temp_processed.loc[df_temp_processed[col] < min_val, col] = np.nan
#                 if max_val is not None:
#                     df_temp_processed.loc[df_temp_processed[col] > max_val, col] = np.nan
    
#     if 'Rain_mm' not in df_temp_processed.columns or df_temp_processed['Rain_mm'].isnull().all():
#         df_temp_processed = create_rain_mm(df_temp_processed)
#         warnings.warn(str(_l("Colonne Rain_mm créée à partir des deux capteurs (pré-interpolation).")))

#     df_temp_processed.loc[:, 'Is_Daylight'] = False
#     df_temp_processed.loc[:, 'Daylight_Duration'] = pd.NA
#     df_temp_processed.loc[:, 'sunrise_time_utc'] = pd.NaT 
#     df_temp_processed.loc[:, 'sunset_time_utc'] = pd.NaT 

#     sun_info_df_list = []

#     for station_name, group in df_temp_processed.groupby('Station'):
#         group_copy_for_sun_calc = group.copy() 

#         if station_name not in gps_info_dict or \
#            pd.isna(gps_info_dict[station_name].get('Lat')) or \
#            pd.isna(gps_info_dict[station_name].get('Long')) or \
#            pd.isna(gps_info_dict[station_name].get('Timezone')):
#             warnings.warn(str(_l("Coordonnées ou Fuseau horaire manquants/invalides pour le site '%s' dans df_gps. Utilisation de l'indicateur jour/nuit fixe pour cette station.") % station_name))
#             group_copy_for_sun_calc.loc[:, 'Is_Daylight'] = (group_copy_for_sun_calc.index.hour >= 7) & (group_copy_for_sun_calc.index.hour <= 18)
#             group_copy_for_sun_calc.loc[:, 'Daylight_Duration'] = "11:00:00"
#             group_copy_for_sun_calc.loc[:, 'sunrise_time_utc'] = pd.NaT 
#             group_copy_for_sun_calc.loc[:, 'sunset_time_utc'] = pd.NaT
#             sun_info_df_list.append(group_copy_for_sun_calc[['Is_Daylight', 'Daylight_Duration', 'sunrise_time_utc', 'sunset_time_utc']])
#             continue

#         lat = gps_info_dict[station_name]['Lat']
#         long = gps_info_dict[station_name]['Long']
#         timezone_str = gps_info_dict[station_name]['Timezone']
        
#         try:
#             local_tz = pytz.timezone(timezone_str)
            
#             unique_dates_local = group_copy_for_sun_calc.index.tz_convert(local_tz).normalize().drop_duplicates()
            
#             sun_data = []
#             loc = LocationInfo(station_name, "Site", timezone_str, lat, long)

#             for date_local_naive in unique_dates_local.to_pydatetime():
#                 s = sun.sun(loc.observer, date=date_local_naive.date())
                
#                 sunrise_utc = s['sunrise'].astimezone(pytz.utc) if s['sunrise'] and pd.notna(s['sunrise']) else pd.NaT
#                 sunset_utc = s['sunset'].astimezone(pytz.utc) if s['sunset'] and pd.notna(s['sunset']) else pd.NaT
                
#                 daylight_duration_hours = (sunset_utc - sunrise_utc).total_seconds() / 3600 if pd.notna(sunrise_utc) and pd.notna(sunset_utc) and sunset_utc > sunrise_utc else np.nan

#                 sun_data.append({
#                     'Datetime_Date_UTC': date_local_naive.date(),
#                     'sunrise_time_utc': sunrise_utc,
#                     'sunset_time_utc': sunset_utc,
#                     'Daylight_Duration_h': daylight_duration_hours
#                 })
            
#             daily_sun_df = pd.DataFrame(sun_data).set_index('Datetime_Date_UTC')

#             group_copy_for_sun_calc['Datetime_Date_UTC'] = group_copy_for_sun_calc.index.normalize().date

#             group_with_sun = pd.merge(
#                 group_copy_for_sun_calc.reset_index(),
#                 daily_sun_df,
#                 left_on='Datetime_Date_UTC',
#                 right_index=True,
#                 how='left'
#             ).set_index('Datetime')

#             group_copy_for_sun_calc.loc[:, 'sunrise_time_utc'] = group_with_sun['sunrise_time_utc']
#             group_copy_for_sun_calc.loc[:, 'sunset_time_utc'] = group_with_sun['sunset_time_utc']
#             group_copy_for_sun_calc.loc[:, 'Is_Daylight'] = (group_copy_for_sun_calc.index >= group_copy_for_sun_calc['sunrise_time_utc']) & \
#                                                              (group_copy_for_sun_calc.index < group_copy_for_sun_calc['sunset_time_utc'])
            
#             group_copy_for_sun_calc.loc[:, 'Daylight_Duration'] = group_with_sun['Daylight_Duration_h'].apply(
#                 lambda x: f"{int(x)}:{int((x*60)%60):02d}:{int((x*3600)%60):02d}" if pd.notna(x) else pd.NA
#             )

#             warnings.warn(str(_l("Lever et coucher du soleil calculés pour %s.") % station_name))
#             sun_info_df_list.append(group_copy_for_sun_calc[['Is_Daylight', 'Daylight_Duration', 'sunrise_time_utc', 'sunset_time_utc']])

#         except Exception as e:
#             warnings.warn(str(_l("Erreur lors du calcul du lever/coucher du soleil avec Astral pour %s: %s. Utilisation de l'indicateur jour/nuit fixe.") % (station_name, e)))
#             traceback.print_exc()
#             group_copy_for_sun_calc.loc[:, 'Is_Daylight'] = (group_copy_for_sun_calc.index.hour >= 7) & (group_copy_for_sun_calc.index.hour <= 18)
#             group_copy_for_sun_calc.loc[:, 'Daylight_Duration'] = "11:00:00"
#             group_copy_for_sun_calc.loc[:, 'sunrise_time_utc'] = pd.NaT 
#             group_copy_for_sun_calc.loc[:, 'sunset_time_utc'] = pd.NaT 
#             sun_info_df_list.append(group_copy_for_sun_calc[['Is_Daylight', 'Daylight_Duration', 'sunrise_time_utc', 'sunset_time_utc']])

#     if sun_info_df_list:
#         df_sun_info_concat = pd.concat(sun_info_df_list)
#         # S'assurer que les index correspondent. La reindexation directe sur l'index de df_temp_processed
#         # est maintenant sûre car df_temp_processed n'a plus d'index dupliqués.
#         df_temp_processed['Is_Daylight'] = df_sun_info_concat.reindex(df_temp_processed.index)['Is_Daylight']
#         df_temp_processed['Daylight_Duration'] = df_sun_info_concat.reindex(df_temp_processed.index)['Daylight_Duration']
#         df_temp_processed['sunrise_time_utc'] = df_sun_info_concat.reindex(df_temp_processed.index)['sunrise_time_utc']
#         df_temp_processed['sunset_time_utc'] = df_sun_info_concat.reindex(df_temp_processed.index)['sunset_time_utc']
#     else:
#         warnings.warn(str(_l("Aucune information solaire calculée. Toutes les colonnes 'Is_Daylight', etc. restent à leurs valeurs initiales par défaut.")))


#     if 'Solar_R_W/m^2' in df_temp_processed.columns and 'Is_Daylight' in df_temp_processed.columns:
#         df_temp_processed.loc[~df_temp_processed['Is_Daylight'], 'Solar_R_W/m^2'] = 0
#         warnings.warn(str(_l("Toutes les valeurs de Solar_R_W/m^2 en dehors des heures de jour ont été mises à zéro dans le DataFrame pré-interpolation.")))

#         has_rain_mm = 'Rain_mm' in df_temp_processed.columns
#         cond_suspect_zeros = (df_temp_processed['Is_Daylight']) & \
#                              (df_temp_processed['Solar_R_W/m^2'] == 0)
#         if has_rain_mm:
#             cond_suspect_zeros = cond_suspect_zeros & (df_temp_processed['Rain_mm'] == 0)
#         else:
#             warnings.warn(str(_l("Rain_mm manquant. Tous les 0 de radiation solaire pendant le jour sont traités comme suspects pour l'étape pré-interpolation.")))

#         df_temp_processed.loc[cond_suspect_zeros, 'Solar_R_W/m^2'] = np.nan
#         warnings.warn(str(_l("Zéros suspects de radiation solaire pendant le jour mis à NaN dans le DataFrame pré-interpolation.")))

#     df_before_interpolation = df_temp_processed.copy()

#     cols_to_drop_after_process = ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
#     df_before_interpolation = df_before_interpolation.drop(columns=cols_to_drop_after_process, errors='ignore')


#     all_missing_ranges_before_interp_list = [] 
#     numerical_cols_to_check = [col for col in numerical_cols if col in df_before_interpolation.columns and col != 'Station']
    
#     for station_name, group in df_before_interpolation.groupby('Station'):
#         for var in numerical_cols_to_check:
#             all_missing_ranges_before_interp_list.extend(_get_missing_ranges(group[var], station_name, var))

#     df_missing_ranges_before_interp = pd.DataFrame(all_missing_ranges_before_interp_list)
#     if not df_missing_ranges_before_interp.empty:
#         df_missing_ranges_before_interp = df_missing_ranges_before_interp.sort_values(by=['station', 'variable', 'start_time'])
#     else:
#         df_missing_ranges_before_interp = pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours'])


#     df_after_interpolation = df_before_interpolation.copy()

#     cols_to_interpolate_standard = [col for col in numerical_cols_to_check if col != 'Solar_R_W/m^2']

#     for station_name, group in df_after_interpolation.groupby('Station'):
#         group_copy_for_interp = group.copy() 
#         for var in cols_to_interpolate_standard:
#             if var in group_copy_for_interp.columns:
#                 if isinstance(group_copy_for_interp.index, pd.DatetimeIndex):
#                     group_copy_for_interp.loc[:, var] = group_copy_for_interp[var].interpolate(method='time', limit_direction='both')
#                 else:
#                     group_copy_for_interp.loc[:, var] = group_copy_for_interp[var].interpolate(method='linear', limit_direction='both')
#                 group_copy_for_interp.loc[:, var] = group_copy_for_interp[var].bfill().ffill()
        
#         if 'Solar_R_W/m^2' in group_copy_for_interp.columns and 'Is_Daylight' in group_copy_for_interp.columns:
#             is_day = group_copy_for_interp['Is_Daylight']
            
#             if isinstance(group_copy_for_interp.index, pd.DatetimeIndex):
#                 group_copy_for_interp.loc[is_day, 'Solar_R_W/m^2'] = group_copy_for_interp.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='time', limit_direction='both')
#             else:
#                 group_copy_for_interp.loc[is_day, 'Solar_R_W/m^2'] = group_copy_for_interp.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='linear', limit_direction='both')
            
#             group_copy_for_interp.loc[is_day, 'Solar_R_W/m^2'] = group_copy_for_interp.loc[is_day, 'Solar_R_W/m^2'].bfill().ffill()

#             group_copy_for_interp.loc[~is_day, 'Solar_R_W/m^2'] = 0
            
#         df_after_interpolation.loc[group_copy_for_interp.index, group_copy_for_interp.columns] = group_copy_for_interp

#     all_missing_ranges_after_interp_list = []
#     for station_name, group in df_after_interpolation.groupby('Station'):
#         for var in numerical_cols_to_check:
#             all_missing_ranges_after_interp_list.extend(_get_missing_ranges(group[var], station_name, var))
        
#     df_missing_ranges_after_interp = pd.DataFrame(all_missing_ranges_after_interp_list)
#     if not df_missing_ranges_after_interp.empty:
#         df_missing_ranges_after_interp = df_missing_ranges_after_interp.sort_values(by=['station', 'variable', 'start_time'])
#     else:
#         df_missing_ranges_after_interp = pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours'])
        
#     return df_before_interpolation, df_after_interpolation, df_missing_ranges_before_interp, df_missing_ranges_after_interp


def convert_utm_df_to_gps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convertit un DataFrame contenant des colonnes 'Easting', 'Northing', 'zone', 'hemisphere'
    de coordonnées UTM vers latitude/longitude WGS84.

    Args:
        df (pd.DataFrame): DataFrame d'entrée avec colonnes UTM.

    Returns:
        pd.DataFrame: DataFrame avec les colonnes 'Long' et 'Lat' (GPS) et sans les colonnes UTM.
    """
    df_copy = df.copy()

    required_utm_cols = ['Easting', 'Northing', 'zone', 'hemisphere']
    if not all(col in df_copy.columns for col in required_utm_cols):
        # Traduction de l'erreur
        raise ValueError(
            _l("Le DataFrame doit contenir les colonnes %s pour la conversion UTM.") % required_utm_cols
        )

    def convert_row(row):
        try:
            zone = int(row['zone'])
            hemisphere = str(row['hemisphere']).upper()
            is_northern = hemisphere == 'N'

            proj_utm = CRS.from_proj4(
                f"+proj=utm +zone={zone} +datum=WGS84 +units=m +{'north' if is_northern else 'south'}"
            )
            proj_wgs84 = CRS.from_epsg(4326)

            transformer = Transformer.from_crs(proj_utm, proj_wgs84, always_xy=True)
            lon, lat = transformer.transform(row['Easting'], row['Northing'])
            return pd.Series({'Long': lon, 'Lat': lat})
        except Exception as e:
            # Traduction de l'avertissement
            warnings.warn(_l("Erreur lors de la conversion UTM d'une ligne: %s") % e)
            return pd.Series({'Long': pd.NA, 'Lat': pd.NA})

    df_copy[['Long', 'Lat']] = df_copy.apply(convert_row, axis=1)
    df_copy = df_copy.drop(columns=['Easting', 'Northing', 'hemisphere', 'zone'], errors='ignore')

    return df_copy

def _load_and_prepare_gps_data() -> pd.DataFrame:
    """
    Charge les fichiers de coordonnées des stations depuis Google Drive,
    les prétraite (suppression/ajout de colonnes/lignes, renommage),
    convertit les coordonnées UTM en GPS pour Dano et Dassari,
    ajoute les fuseaux horaires, et fusionne tous les bassins en un seul DataFrame.
    """
    # Traduction du message
    print(_l("Début de la préparation des données de coordonnées des stations..."))
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True)

    files_info = [
        {'id': '1Iz5L_XkumG390EZvnMgYr3KwDYeesrNz', 'name': "WASCAL Basins Climate Station Coordinates.xlsx", 'bassin': 'Vea Sissili'},
        {'id': '1H8A-sVMtTok6lrD-NFHQxzHBeQ_P7g4z', 'name': "Dano Basins Climate Station Coordinates.xlsx", 'bassin': 'Dano'},
        {'id': '1SOXI0ZvWqpNp6Qwz_BGeWleUtaYMaOBU', 'name': "DASSARI Climate Station Coordinates.xlsx", 'bassin': 'Dassari'}
    ]

    loaded_dfs = []

    for file_info in files_info:
        output_file_path = os.path.join(data_dir, file_info['name'])
        
        if not os.path.exists(output_file_path):
            # Traduction du message
            print(_l("Téléchargement de %s depuis Google Drive...") % file_info['bassin'])
            gdown.download(f'https://drive.google.com/uc?id={file_info["id"]}', output_file_path, quiet=False)
            print(_l("Téléchargement de %s terminé.") % file_info['bassin'])
        else:
            # Traduction du message
            print(_l("Chargement de %s depuis le cache local: %s") % (file_info['bassin'], output_file_path))
        
        loaded_dfs.append(pd.read_excel(output_file_path))

    vea_sissili_bassin = loaded_dfs[0]
    dano_bassin = loaded_dfs[1]
    dassari_bassin = loaded_dfs[2]

    # Traduction du message
    print(_l("Début du prétraitement des données de stations..."))
    
    vea_sissili_bassin = vea_sissili_bassin.drop(columns=['No', 'Location', 'parameters'], errors='ignore')
    new_row_df_vea = pd.DataFrame([{'Name': 'Atampisi', 'Lat': 10.91501, 'Long': -0.82647}])
    vea_sissili_bassin = pd.concat([vea_sissili_bassin, new_row_df_vea], ignore_index=True)

    dassari_bassin = dassari_bassin.drop(columns=['Altitude (en m)'], errors='ignore')
    new_rows_df_dassari = pd.DataFrame([{'Site name': 'Pouri', 'Lat': 1207107, 'Long': 293642}, {'Site name': 'Fandohoun', 'Lat': 1207107, 'Long': 293642}])
    dassari_bassin = pd.concat([dassari_bassin, new_rows_df_dassari], ignore_index=True)

    dano_bassin = dano_bassin.rename(columns={'Long': 'Easting', 'Lat': 'Northing', 'Site Name': 'Name'})
    dassari_bassin = dassari_bassin.rename(columns={'Long': 'Easting', 'Lat': 'Northing', 'Site name': 'Name'})

    dano_bassin['zone'] = 30
    dano_bassin['hemisphere'] = 'N'
    dassari_bassin['zone'] = 31
    dassari_bassin['hemisphere'] = 'N'

    dano_bassin = convert_utm_df_to_gps(dano_bassin)
    dassari_bassin = convert_utm_df_to_gps(dassari_bassin)

    dano_bassin['Timezone'] = 'Africa/Ouagadougou'
    dassari_bassin['Timezone'] = 'Africa/Porto-Novo'
    vea_sissili_bassin['Timezone'] = 'Africa/Accra'

    bassins = pd.concat([vea_sissili_bassin, dano_bassin, dassari_bassin], ignore_index=True)

    bassins = bassins.rename(columns={'Name': 'Station'})

    initial_rows = len(bassins)
    bassins = bassins.dropna(subset=['Lat', 'Long', 'Timezone', 'Station'])
    if len(bassins) < initial_rows:
        # Traduction de l'avertissement
        print(_l("Attention: %s lignes avec des coordonnées ou fuseaux horaires manquants ont été supprimées du DataFrame des stations.") % (initial_rows - len(bassins)))
    
    output_json_path = os.path.join(data_dir, "station_coordinates.json")
    bassins.to_json(output_json_path, orient='records', indent=4)
    # Traduction des messages
    print(_l("\nPréparation des données terminée. Coordonnées des stations sauvegardées dans '%s'.") % output_json_path)
    print(_l("Vous pouvez maintenant lancer votre application Flask."))

    return bassins

# def gestion_doublons(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Gère les doublons dans le DataFrame en se basant sur les colonnes 'Station' et 'Datetime'.
#     Conserve la première occurrence en cas de doublon.

#     Args:
#         df (pd.DataFrame): Le DataFrame d'entrée.

#     Returns:
#         pd.DataFrame: Le DataFrame sans doublons.
#     """
#     if 'Station' in df.columns and 'Datetime' in df.columns:
#         initial_rows = len(df)
#         df_cleaned = df.drop_duplicates(subset=['Station', 'Datetime'], keep='first')
#         if len(df_cleaned) < initial_rows:
#             # Traduction de l'avertissement
#             warnings.warn(_l("Suppression de %s doublons basés sur 'Station' et 'Datetime'.") % (initial_rows - len(df_cleaned)))
#         return df_cleaned
#     else:
#         # Traduction de l'avertissement
#         warnings.warn(_l("Colonnes 'Station' ou 'Datetime' manquantes pour la gestion des doublons. Le DataFrame n'a pas été modifié."))
#         return df

def gestion_doublons(df):
    """Gère les doublons dans le DataFrame avec messages traduits."""
    try:
        if 'Station' not in df.columns or 'Datetime' not in df.index.name:
            warnings.warn(_l("Colonnes 'Station' ou 'Datetime' manquantes pour la gestion des doublons. Le DataFrame n'a pas été modifié."))
            return df
            
        duplicate_mask = df.duplicated(subset=['Station'], keep='first')
        if duplicate_mask.any():
            warnings.warn(_("Suppression de %(count)d doublons détectés", count=duplicate_mask.sum()))
            return df[~duplicate_mask]
        return df
    except Exception as e:
        warnings.warn(_("Erreur dans gestion_doublons: %(error)s", error=str(e)))
        return df

# def traiter_outliers_meteo(df: pd.DataFrame, limits: dict) -> pd.DataFrame:
#     """
#     Remplace les valeurs aberrantes par NaN pour toutes les variables météorologiques spécifiées.

#     Args:
#         df (pd.DataFrame): DataFrame d'entrée avec DatetimeIndex et colonne 'Station'.
#         limits (dict): Dictionnaire avec les limites min/max pour chaque variable.

#     Returns:
#         pd.DataFrame: DataFrame avec les valeurs aberrantes remplacées par NaN.
#     """
#     df_processed = df.copy()

#     if not isinstance(df_processed.index, pd.DatetimeIndex):
#         # Traduction de l'avertissement
#         warnings.warn(_l("L'index n'est pas un DatetimeIndex dans traiter_outliers_meteo. Tentative de conversion."))
#         try:
#             df_processed.index = pd.to_datetime(df_processed.index, errors='coerce')
#             df_processed = df_processed[df_processed.index.notna()]
#             if df_processed.empty:
#                 # Traduction de l'erreur
#                 raise ValueError(_l("DataFrame vide après nettoyage des dates invalides dans traiter_outliers_meteo."))
#         except Exception as e:
#             # Traduction de l'erreur
#             raise TypeError(_l("Impossible de garantir un DatetimeIndex pour traiter_outliers_meteo: %s") % e)

#     for var, vals in limits.items():
#         if var in df_processed.columns:
#             min_val = vals.get('min')
#             max_val = vals.get('max')
#             if min_val is not None or max_val is not None:
#                 initial_nan_count = df_processed[var].isna().sum()
#                 if min_val is not None:
#                     df_processed.loc[df_processed[var] < min_val, var] = np.nan
#                 if max_val is not None:
#                     df_processed.loc[df_processed[var] > max_val, var] = np.nan
                
#                 new_nan_count = df_processed[var].isna().sum()
#                 if new_nan_count > initial_nan_count:
#                     # Traduction de l'avertissement
#                     warnings.warn(_l("Remplacement de %s valeurs hors limites dans '%s' par NaN.") % (new_nan_count - initial_nan_count, var))
#     return df_processed

import pandas as pd
import numpy as np

import pandas as pd
import numpy as np

def traiter_outliers_meteo(df: pd.DataFrame, colonnes: list = None, coef: float = 1.5) -> pd.DataFrame:
    """
    Remplace les outliers dans un DataFrame en utilisant la méthode IQR,
    en les remplaçant par les bornes (inférieure ou supérieure) plutôt que NaN.

    Les colonnes de précipitation (Rain_01_mm, Rain_02_mm, Rain_mm) sont automatiquement exclues du traitement.

    Args:
        df (pd.DataFrame): Le DataFrame contenant les données à traiter.
        colonnes (list, optional): Liste des colonnes à traiter. Si None, toutes les colonnes numériques
                                   (sauf les précipitations et les colonnes non pertinentes comme 'Station' et 'Datetime')
                                   seront utilisées.
        coef (float): Facteur multiplicatif de l’IQR. Par défaut 1.5.

    Returns:
        pd.DataFrame: Une copie du DataFrame avec les outliers remplacés par les bornes IQR.
    """
    df_resultat = df.copy()

    # Définir les colonnes de précipitation à exclure
    exclusion_cols = ['Rain_01_mm', 'Rain_02_mm', 'Rain_mm']
    # Ajoutez également 'Station' et 'Datetime' si elles sont numériques
    # (Parfois 'Datetime' peut être numérique si non converti, et 'Station' pourrait être un ID numérique)
    exclusion_cols.extend(['Station', 'Datetime']) # Ensure these are also not treated for outliers

    # Sélection des colonnes numériques si non spécifiées
    if colonnes is None:
        # Prendre toutes les colonnes numériques
        all_numeric_cols = df_resultat.select_dtypes(include=[np.number]).columns.tolist()
        # Filtrer pour exclure les colonnes de précipitation et autres colonnes non pertinentes
        colonnes_a_traiter = [col for col in all_numeric_cols if col not in exclusion_cols]
    else:
        # Si des colonnes sont spécifiées, assurez-vous quand même d'exclure les colonnes de précipitation
        colonnes_a_traiter = [col for col in colonnes if col not in exclusion_cols]

    # Assurez-vous que les colonnes existent réellement dans le DataFrame
    colonnes_existantes = [col for col in colonnes_a_traiter if col in df_resultat.columns]

    if not colonnes_existantes:
        print("Aucune colonne valide trouvée pour le traitement des outliers après exclusion.")
        return df_resultat

    for col in colonnes_existantes:
        Q1 = df_resultat[col].quantile(0.25)
        Q3 = df_resultat[col].quantile(0.75)
        IQR = Q3 - Q1
        borne_inf = Q1 - coef * IQR
        borne_sup = Q3 + coef * IQR

        # Comptage des remplacements
        outliers_bas = df_resultat[col] < borne_inf
        outliers_haut = df_resultat[col] > borne_sup
        nb_bas = outliers_bas.sum()
        nb_haut = outliers_haut.sum()

        if nb_bas + nb_haut > 0:
            print(f"Colonne '{col}': {nb_bas + nb_haut} outlier(s) corrigé(s).")

        # Remplacement par les bornes
        df_resultat.loc[outliers_bas, col] = borne_inf
        df_resultat.loc[outliers_haut, col] = borne_sup

    return df_resultat


def calculate_daily_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les statistiques journalières (moyenne, min, max, somme) pour les variables numériques
    groupées par station.
    """
    df_copy = df.copy()

    if isinstance(df_copy.index, pd.DatetimeIndex):
        df_copy = df_copy.reset_index()

    df_copy['Datetime'] = pd.to_datetime(df_copy['Datetime'], errors='coerce')
    df_copy = df_copy.dropna(subset=['Datetime', 'Station'])

    if df_copy.empty:
        # Traduction de l'avertissement
        print(_l("Avertissement: Le DataFrame est vide après le nettoyage des dates et stations dans calculate_daily_summary_table."))
        return pd.DataFrame()

    if 'Is_Daylight' not in df_copy.columns:
        # Traduction de l'avertissement
        warnings.warn(_l("La colonne 'Is_Daylight' est manquante. Calcul en utilisant une règle fixe (7h-18h)."))
        df_copy['Is_Daylight'] = (df_copy['Datetime'].dt.hour >= 7) & (df_copy['Datetime'].dt.hour <= 18)

    numerical_cols = [col for col in df_copy.columns if pd.api.types.is_numeric_dtype(df_copy[col]) and col not in ['Station', 'Datetime', 'Is_Daylight', 'Daylight_Duration']]
    
    if not numerical_cols:
        # Traduction de l'avertissement
        warnings.warn(_l("Aucune colonne numérique valide trouvée pour le calcul des statistiques journalières."))
        return pd.DataFrame()

    daily_aggregated_df = df_copy.groupby(['Station', df_copy['Datetime'].dt.date]).agg({
        col: ['mean', 'min', 'max'] for col in numerical_cols if METADATA_VARIABLES.get(col, {}).get('is_rain') == False
    })

    daily_aggregated_df.columns = ['_'.join(col).strip() for col in daily_aggregated_df.columns.values]

    if 'Rain_mm' in df_copy.columns and METADATA_VARIABLES.get('Rain_mm', {}).get('is_rain'):
        df_daily_rain = df_copy.groupby(['Station', df_copy['Datetime'].dt.date])['Rain_mm'].sum().reset_index()
        df_daily_rain = df_daily_rain.rename(columns={'Rain_mm': 'Rain_mm_sum'})

        if not daily_aggregated_df.empty:
            daily_aggregated_df = daily_aggregated_df.reset_index()
            daily_stats_df = pd.merge(daily_aggregated_df, df_daily_rain, on=['Station', 'Datetime'], how='left')
            daily_stats_df = daily_stats_df.rename(columns={'Datetime': 'Date'})
        else:
            daily_stats_df = df_daily_rain.rename(columns={'Datetime': 'Date'})
    else:
        daily_stats_df = daily_aggregated_df.reset_index().rename(columns={'Datetime': 'Date'})

    if 'Rain_mm' in df_copy.columns and METADATA_VARIABLES.get('Rain_mm', {}).get('is_rain'):
        df_daily_rain_raw = df_copy.groupby(['Station', pd.Grouper(key='Datetime', freq='D')])['Rain_mm'].sum().reset_index()
        
        RAIN_SEASON_GAP_THRESHOLD = pd.Timedelta(days=60)
        season_stats = []
        for station_name, station_df_rain in df_daily_rain_raw.groupby('Station'):
            station_df_rain = station_df_rain.set_index('Datetime').sort_index()
            rain_events = station_df_rain[station_df_rain['Rain_mm'] > 0].index

            if rain_events.empty:
                # Traduire les clés du dictionnaire pour la cohérence si elles sont utilisées pour l'affichage
                season_stats.append({'Station': station_name, _l('Moyenne_Saison_Pluvieuse'): np.nan, _l('Debut_Saison_Pluvieuse'): pd.NaT, _l('Fin_Saison_Pluvieuse'): pd.NaT, _l('Duree_Saison_Pluvieuse_Jours'): np.nan})
                continue
            
            block_ids = (rain_events.to_series().diff() > RAIN_SEASON_GAP_THRESHOLD).cumsum()
            valid_blocks = {}
            for block_id, rain_dates_in_block in rain_events.to_series().groupby(block_ids):
                if not rain_dates_in_block.empty:
                    block_start = rain_dates_in_block.min()
                    block_end = rain_dates_in_block.max()
                    full_block_df = station_df_rain.loc[block_start:block_end]
                    valid_blocks[block_id] = full_block_df

            if not valid_blocks:
                # Traduire les clés du dictionnaire
                season_stats.append({'Station': station_name, _l('Moyenne_Saison_Pluvieuse'): np.nan, _l('Debut_Saison_Pluvieuse'): pd.NaT, _l('Fin_Saison_Pluvieuse'): pd.NaT, _l('Duree_Saison_Pluvieuse_Jours'): np.nan})
                continue

            # ...existing code...
            main_block_id = max(valid_blocks, key=lambda k: (valid_blocks[k].index.max() - valid_blocks[k].index.min()).days)
            main_season_df = valid_blocks[main_block_id]

            debut_saison = main_season_df.index.min()
            fin_saison = main_season_df.index.max()
            total_days = (fin_saison - debut_saison).days + 1
            moyenne_saison = main_season_df['Rain_mm'].sum() / total_days if total_days > 0 else 0
            # ...existing code...

            # Traduire les clés du dictionnaire
            season_stats.append({
                'Station': station_name,
                _l('Moyenne_Saison_Pluvieuse'): moyenne_saison,
                _l('Debut_Saison_Pluvieuse'): debut_saison,
                _l('Fin_Saison_Pluvieuse'): fin_saison,
                _l('Duree_Saison_Pluvieuse_Jours'): total_days
            })
        df_season_stats = pd.DataFrame(season_stats)
        
        if not df_season_stats.empty:
            daily_stats_df = pd.merge(daily_stats_df, df_season_stats, on='Station', how='left')

    final_stats_per_station = pd.DataFrame()
    for station_name in df_copy['Station'].unique():
        station_df = df_copy[df_copy['Station'] == station_name].copy()
        station_summary = {'Station': station_name}

        for var in numerical_cols:
            if var in station_df.columns and pd.api.types.is_numeric_dtype(station_df[var]):
                if var == 'Solar_R_W/m^2':
                    var_data = station_df.loc[station_df['Is_Daylight'], var].dropna()
                else:
                    var_data = station_df[var].dropna()
                
                if not var_data.empty:
                    # Traduire les clés du dictionnaire pour la cohérence
                    station_summary[f'{var}_Maximum'] = var_data.max()
                    station_summary[f'{var}_Minimum'] = var_data.min()
                    station_summary[f'{var}_Moyenne'] = var_data.mean()
                    station_summary[f'{var}_Mediane'] = var_data.median()
                    
                    if var == 'Rain_mm':
                        station_summary[f'{var}_Cumul_Annuel'] = station_df['Rain_mm'].sum()
                        rainy_days_data = station_df[station_df['Rain_mm'] > 0]['Rain_mm'].dropna()
                        station_summary[f'{var}_Moyenne_Jours_Pluvieux'] = rainy_days_data.mean() if not rainy_days_data.empty else np.nan

                        if 'Duree_Saison_Pluvieuse_Jours' in daily_stats_df.columns:
                            s_data = daily_stats_df[daily_stats_df['Station'] == station_name]
                            if not s_data.empty:
                                # Traduire les clés du dictionnaire
                                station_summary[f'{var}_{_l("Duree_Saison_Pluvieuse_Jours")}'] = s_data[_l('Duree_Saison_Pluvieuse_Jours')].iloc[0]
                                station_summary[f'{var}_{_l("Duree_Secheresse_Definie_Jours")}'] = np.nan

        final_stats_per_station = pd.concat([final_stats_per_station, pd.DataFrame([station_summary])], ignore_index=True)
        
    return final_stats_per_station



from flask_babel import lazy_gettext as _l



from flask_babel import get_locale

def get_var_label(meta, key):
    lang = str(get_locale())
    return meta[key].get(lang[:2], meta[key].get('en', list(meta[key].values())[0]))



def generate_variable_summary_plots_for_web(df: pd.DataFrame, station: str, variable: str, metadata: dict, palette: dict) -> plt.Figure:
    """
    Génère un graphique Matplotlib/Seaborn pour les statistiques agrégées d'une variable spécifique
    pour une station donnée.
    """
    df_station = df[df['Station'] == station].copy()

    if df_station.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, f"Aucune donnée pour la station '{station}'.", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
        ax.axis('off')
        return fig

    if isinstance(df_station.index, pd.DatetimeIndex):
        df_station = df_station.reset_index()
    
    df_station['Datetime'] = pd.to_datetime(df_station['Datetime'], errors='coerce')
    df_station = df_station.dropna(subset=['Datetime', 'Station'])
    df_station = df_station.set_index('Datetime').sort_index()

    if df_station.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, f"DataFrame vide après nettoyage des dates pour la station '{station}'.", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
        ax.axis('off')
        return fig

    if 'Is_Daylight' not in df_station.columns:
        df_station['Is_Daylight'] = (df_station.index.hour >= 7) & (df_station.index.hour <= 18)

    var_meta = metadata.get(variable, {'Nom': variable, 'Unite': ''})

    stats_for_plot = {}
    metrics_to_plot = []
    
    if var_meta.get('is_rain', False) and variable == 'Rain_mm':
        df_daily_rain = df_station.groupby(pd.Grouper(freq='D'))['Rain_mm'].sum().reset_index()
        df_daily_rain = df_daily_rain.rename(columns={'Rain_mm': 'Rain_mm_sum'})
        df_daily_rain['Datetime'] = pd.to_datetime(df_daily_rain['Datetime'])
        df_daily_rain = df_daily_rain.set_index('Datetime').sort_index()

        RAIN_SEASON_GAP_THRESHOLD = pd.Timedelta(days=60)
        rain_events = df_daily_rain[df_daily_rain['Rain_mm_sum'] > 0].index

        s_moyenne_saison = np.nan
        s_duree_saison = np.nan
        s_debut_saison = pd.NaT
        s_fin_saison = pd.NaT

        if not rain_events.empty:
            block_ids = (rain_events.to_series().diff() > RAIN_SEASON_GAP_THRESHOLD).cumsum()
            valid_blocks = {}
            for block_id, rain_dates_in_block in rain_events.to_series().groupby(block_ids):
                if not rain_dates_in_block.empty:
                    block_start = rain_dates_in_block.min()
                    block_end = rain_dates_in_block.max()
                    full_block_df = df_daily_rain.loc[block_start:block_end]
                    valid_blocks[block_id] = full_block_df

            if valid_blocks:
                main_block_id = max(valid_blocks, key=lambda k: (valid_blocks[k].index.max() - valid_blocks[k].index.min()).days)
                main_season_df = valid_blocks[main_block_id]

                s_debut_saison = main_season_df.index.min()
                s_fin_saison = main_season_df.index.max()
                total_days_season = (s_fin_saison - s_debut_saison).days + 1
                s_moyenne_saison = main_season_df['Rain_mm_sum'].sum() / total_days_season if total_days_season > 0 else 0
                s_duree_saison = total_days_season

        longest_dry_spell = np.nan
        debut_secheresse = pd.NaT
        fin_secheresse = pd.NaT

        full_daily_series_rain = df_daily_rain['Rain_mm_sum'].resample('D').sum().fillna(0)
        rainy_days_index = full_daily_series_rain[full_daily_series_rain > 0].index

        if not rainy_days_index.empty and pd.notna(s_moyenne_saison) and s_moyenne_saison > 0:
            temp_dry_spells = []
            for i in range(1, len(rainy_days_index)):
                prev_rain_date = rainy_days_index[i-1]
                current_rain_date = rainy_days_index[i]
                dry_days_between_rains = (current_rain_date - prev_rain_date).days - 1

                if dry_days_between_rains > 0:
                    rain_prev_day = full_daily_series_rain.loc[prev_rain_date]
                    temp_debut = pd.NaT
                    temp_duree = 0
                    for j in range(1, dry_days_between_rains + 1):
                        current_dry_date = prev_rain_date + timedelta(days=j)
                        current_ratio = rain_prev_day / j
                        if current_ratio < s_moyenne_saison:
                            temp_debut = current_dry_date
                            temp_duree = (current_rain_date - temp_debut).days
                            break
                    if pd.notna(temp_debut) and temp_duree > 0:
                        temp_dry_spells.append({
                            'Duree': temp_duree,
                            'Debut': temp_debut,
                            'Fin': current_rain_date - timedelta(days=1)
                        })
            
            if temp_dry_spells:
                df_temp_dry = pd.DataFrame(temp_dry_spells)
                idx_max_dry = df_temp_dry['Duree'].idxmax()
                longest_dry_spell = df_temp_dry.loc[idx_max_dry, 'Duree']
                debut_secheresse = df_temp_dry.loc[idx_max_dry, 'Debut']
                fin_secheresse = df_temp_dry.loc[idx_max_dry, 'Fin']

        rain_data_for_stats = df_station[variable].dropna()
        
        stats_for_plot['Maximum'] = rain_data_for_stats.max() if not rain_data_for_stats.empty else np.nan
        stats_for_plot['Minimum'] = rain_data_for_stats.min() if not rain_data_for_stats.empty else np.nan
        stats_for_plot['Mediane'] = rain_data_for_stats.median() if not rain_data_for_stats.empty else np.nan
        stats_for_plot['Cumul_Annuel'] = df_station[variable].sum()
        
        rainy_days_data = df_station[df_station[variable] > 0][variable].dropna()
        stats_for_plot['Moyenne_Jours_Pluvieux'] = rainy_days_data.mean() if not rainy_days_data.empty else np.nan
        
        stats_for_plot['Moyenne_Saison_Pluvieuse'] = s_moyenne_saison
        stats_for_plot['Duree_Saison_Pluvieuse_Jours'] = s_duree_saison
        stats_for_plot['Debut_Saison_Pluvieuse'] = s_debut_saison
        stats_for_plot['Fin_Saison_Pluvieuse'] = s_fin_saison
        
        stats_for_plot['Duree_Secheresse_Definie_Jours'] = longest_dry_spell
        stats_for_plot['Debut_Secheresse_Definie'] = debut_secheresse
        stats_for_plot['Fin_Secheresse_Definie'] = fin_secheresse

        max_date_idx = df_station[variable].idxmax() if not df_station[variable].empty else pd.NaT
        min_date_idx = df_station[variable].idxmin() if not df_station[variable].empty else pd.NaT
        stats_for_plot['Date_Maximum'] = max_date_idx if pd.notna(max_date_idx) else pd.NaT
        stats_for_plot['Date_Minimum'] = min_date_idx if pd.notna(min_date_idx) else pd.NaT
        
        metrics_to_plot = [
            'Maximum', 'Minimum', 'Cumul_Annuel', 'Mediane',
            'Moyenne_Jours_Pluvieux', 'Moyenne_Saison_Pluvieuse',
            'Duree_Saison_Pluvieuse_Jours', 'Duree_Secheresse_Definie_Jours'
        ]
        nrows, ncols = 4, 2
        figsize = (18, 16)
        
    else:
        current_var_data = df_station[variable].dropna()
        if variable == 'Solar_R_W/m^2':
            current_var_data = df_station.loc[df_station['Is_Daylight'], variable].dropna()

        if current_var_data.empty:
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, f"Aucune donnée valide pour la variable {var_meta['Nom']} à {station}.", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
            ax.axis('off')
            return fig

        stats_for_plot['Maximum'] = current_var_data.max()
        stats_for_plot['Minimum'] = current_var_data.min()
        stats_for_plot['Mediane'] = current_var_data.median()
        stats_for_plot['Moyenne'] = current_var_data.mean()

        max_idx = current_var_data.idxmax() if not current_var_data.empty else pd.NaT
        min_idx = current_var_data.idxmin() if not current_var_data.empty else pd.NaT

        stats_for_plot['Date_Maximum'] = max_idx if pd.notna(max_idx) else pd.NaT
        stats_for_plot['Date_Minimum'] = min_idx if pd.notna(min_idx) else pd.NaT

        metrics_to_plot = ['Maximum', 'Minimum', 'Moyenne', 'Mediane']
        nrows, ncols = 2, 2
        figsize = (18, 12)

    if not stats_for_plot:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, f"Impossible de calculer des statistiques pour la variable '{variable}' à la station '{station}' (données manquantes ou non numériques).", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
        ax.axis('off')
        return fig

    fig, axes = plt.subplots(nrows, ncols, figsize=figsize)
    plt.subplots_adjust(hspace=0.6, wspace=0.4)
    axes = axes.flatten()

    fig.suptitle(f'Statistiques de {var_meta["Nom"]} pour la station {station}', fontsize=16, y=0.98)

    for i, metric in enumerate(metrics_to_plot):
        ax = axes[i]
        value = stats_for_plot.get(metric)
        if pd.isna(value):
            ax.text(0.5, 0.5, "Données non disponibles", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=12, color='gray')
            ax.axis('off')
            continue

        color = palette.get(metric.replace(' ', '_'), '#cccccc')
        
        plot_data_bar = pd.DataFrame({'Metric': [metric.replace('_', ' ')], 'Value': [value]})
        sns.barplot(x='Metric', y='Value', data=plot_data_bar, ax=ax, color=color, edgecolor='none')

        text = ""
        if metric in ['Duree_Saison_Pluvieuse_Jours', 'Duree_Secheresse_Definie_Jours']:
            start_date_key = f'Debut_{metric.replace("Jours", "")}'
            end_date_key = f'Fin_{metric.replace("Jours", "")}'
            start_date = stats_for_plot.get(start_date_key)
            end_date = stats_for_plot.get(end_date_key)
            date_info = ""
            if pd.notna(start_date) and pd.notna(end_date):
                date_info = f"\ndu {start_date.strftime('%d/%m/%Y')} au {end_date.strftime('%d/%m/%Y')}"
            text = f"{int(value)} j{date_info}"
        elif metric in ['Maximum', 'Minimum', 'Cumul_Annuel', 'Moyenne_Jours_Pluvieux', 'Moyenne_Saison_Pluvieuse', 'Mediane', 'Moyenne']:
            unit = var_meta['Unite']
            date_str = ''
            if (metric == 'Maximum' and 'Date_Maximum' in stats_for_plot and pd.notna(stats_for_plot['Date_Maximum'])):
                date_str = f"\n({stats_for_plot['Date_Maximum'].strftime('%d/%m/%Y')})"
            elif (metric == 'Minimum' and 'Date_Minimum' in stats_for_plot and pd.notna(stats_for_plot['Date_Minimum'])):
                date_str = f"\n({stats_for_plot['Date_Minimum'].strftime('%d/%m/%Y')})"
            
            text = f"{value:.1f} {unit}{date_str}"
        else:
            text = f"{value:.1f} {var_meta['Unite']}"

        ax.text(0.5, value, text, ha='center', va='bottom', fontsize=9, color='black',
                bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=1))
        
        ax.set_title(f"{var_meta['Nom']} {metric.replace('_', ' ')}", fontsize=11)
        ax.set_xlabel("")
        ax.set_ylabel(f"Valeur ({var_meta['Unite']})", fontsize=10)
        ax.tick_params(axis='x', rotation=0)
        ax.set_xticklabels([])

    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.tight_layout(rect=[0, 0, 1, 0.96])

    return fig





# --- NEW: get_period_label function ---
def get_period_label(period_key):
    """
    Retrieves the translated label for a given period key based on the current locale
    from the PERIOD_LABELS dictionary.
    """
    current_locale_str = str(get_locale())
    return PERIOD_LABELS.get(period_key, {}).get(current_locale_str[:2], PERIOD_LABELS.get(period_key, {}).get('en', period_key))




import pandas as pd
import numpy as np
from datetime import timedelta
import traceback
from plotly.subplots import make_subplots
import plotly.graph_objs as go
import plotly.express as px # Added if you use px.colors or other px features
import warnings # Added if you use warnings

# --- Babel imports ---
from flask_babel import gettext as _ # <-- C'EST CETTE LIGNE QUI MANQUE OU EST MAL PLACÉE
from flask_babel import get_locale


def generer_graphique_par_variable_et_periode(df: pd.DataFrame, station: str, variables: list, periode: str, colors: dict, metadata: dict) -> go.Figure:
    """
    Génère un graphique Plotly de l'évolution de plusieurs variables pour une station sur une période donnée.
    Retourne l'objet Figure Plotly.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError(_("Le DataFrame doit avoir un DatetimeIndex pour générer le graphique.")) # Use _ for this string

    filtered_df = df[df['Station'] == station].copy()
    if filtered_df.empty:
        return go.Figure()

    fig = go.Figure()
    
    # Use get_period_label for the `periode` string
    if periode == get_period_label('Journalière'): # Check for translated string
        resample_freq = 'D'
    elif periode == get_period_label('Hebdomadaire'): # Check for translated string
        resample_freq = 'W'
    elif periode == get_period_label('Mensuelle'): # Check for translated string
        resample_freq = 'M'
    elif periode == get_period_label('Annuelle'): # Check for translated string
        resample_freq = 'Y'
    else: # Fallback to original key if not found in PERIOD_LABELS
        # This branch might be tricky if 'periode' comes pre-translated from client.
        # It's better to pass the original 'periode' key to the backend function
        # and let the backend translate it for display purposes.
        # For resampling, we need the *actual* frequency.
        # Let's assume 'periode' parameter itself is the original key ('Journalière', 'Hebdomadaire', etc.)
        # and we only translate it for display.
        # If 'periode' parameter is *already* translated, this logic needs adjustment.
        # For now, I'll assume 'periode' passed to the function is the untranslated key.
        # Reverting to direct string check for resampling logic, but still translating for display.
        pass # The current if/elif chain below handles the original keys.

    for variable in variables:
        if variable not in filtered_df.columns:
            continue
            
        # The resampling logic should use the original, untranslated period keys
        # as these are internal identifiers for pandas resample.
        if periode == 'Journalière':
            resampled_df = filtered_df[variable].resample('D').mean()
        elif periode == 'Hebdomadaire':
            resampled_df = filtered_df[variable].resample('W').mean()
        elif periode == 'Mensuelle':
            resampled_df = filtered_df[variable].resample('M').mean()
        elif periode == 'Annuelle':
            resampled_df = filtered_df[variable].resample('Y').mean()
        # else:
        #     resampled_df = filtered_df[variable]

        resampled_df = resampled_df.dropna()
        if resampled_df.empty:
            continue

        var_color = colors.get(variable, '#1f77b4')  # Default color if not found

        var_meta = metadata.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
        var_label = str(get_var_label(var_meta, 'Nom'))
        var_unit = str(get_var_label(var_meta, 'Unite'))

        
        fig.add_trace(go.Scatter(
            x=resampled_df.index, 
            y=resampled_df.values,
            mode='lines', 
            name=f"{var_label} ({var_unit})",
            line=dict(color=var_color, width=2),
            hovertemplate=f"%{{y:.2f}} {var_unit}<extra>{var_label}</extra>"
        ))

    if not fig.data:
        return go.Figure()

    # Translate 'periode' for display in the title
    translated_periode = get_period_label(periode)

    fig.update_layout(
        #title=str(_("Évolution des variables pour %(station)s (%(periode)s)", station=station, periode=translated_periode)),
        title=str(_("Évolution des variables pour %(station)s (%(periode)s)", station=station, periode=translated_periode)),
        xaxis_title=str(_("Date")), # Use _ for static strings
        yaxis_title=str(_("Valeurs")), # Use _ for static strings
        hovermode="x unified",
        legend_title=str(_("Variables")), # Use _ for static strings
        template="plotly_white",
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    
    return fig


def generer_graphique_comparatif(df: pd.DataFrame, variable: str, periode: str, colors: dict, metadata: dict) -> go.Figure:
    """
    Génère un graphique Plotly comparatif de l'évolution d'une variable entre toutes les stations.
    Chaque station utilise sa couleur personnalisée.
    Retourne l'objet Figure Plotly.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError(_("Le DataFrame doit avoir un DatetimeIndex pour générer le graphique comparatif.")) # Use _ for this string

    fig = go.Figure()
    
    all_stations = df['Station'].unique()
    if len(all_stations) < 2:
        warnings.warn(_("Moins de 2 stations disponibles pour la comparaison. Le graphique comparatif ne sera pas généré.")) # Use _ for this string
        return go.Figure()

    for station in all_stations:
        filtered_df = df[df['Station'] == station].copy()
        if filtered_df.empty:
            continue

        # Resampling logic uses original period keys
        if periode == 'Journalière':
            resampled_df = filtered_df[variable].resample('D').mean()
        elif periode == 'Hebdomadaire':
            resampled_df = filtered_df[variable].resample('W').mean()
        elif periode == 'Mensuelle':
            resampled_df = filtered_df[variable].resample('M').mean()
        elif periode == 'Annuelle':
            resampled_df = filtered_df[variable].resample('Y').mean()
        # else:
        #     resampled_df = filtered_df[variable]

        resampled_df = resampled_df.dropna()
        if resampled_df.empty:
            continue
        
        station_color = colors.get(station, '#1f77b4')  # Default color if not found
        
        fig.add_trace(go.Scatter(
            x=resampled_df.index, 
            y=resampled_df.values,
            mode='lines', 
            name=station,
            line=dict(color=station_color, width=2),
            hovertemplate=f"{variable}: %{{y:.2f}}<extra>{station}</extra>"
        ))

    if not fig.data:
        return go.Figure()

    var_meta = metadata.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
    var_label = str(get_var_label(var_meta, 'Nom'))
    var_unit = str(get_var_label(var_meta, 'Unite'))

    # Translate 'periode' for display in the title
    translated_periode = get_period_label(periode)

    fig.update_layout(
        #title=str(_("Comparaison de %(var_label)s (%(var_unit)s) entre stations (%(periode)s)", var_label=var_label, var_unit=var_unit, periode=translated_periode)),
        title=str(_("Comparaison de %(var_label)s (%(var_unit)s) entre stations (%(periode)s)", var_label=var_label, var_unit=var_unit, periode=translated_periode)),
        xaxis_title=str(_("Date")), # Use _ for static strings
        yaxis_title=f"{var_label} ({var_unit})", # This mixes translated var_label with direct string, if var_label needs translation itself this is fine.
        hovermode="x unified",
        legend_title=str(_("Stations")), # Use _ for static strings
        template="plotly_white",
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    return fig


def generate_multi_variable_station_plot(df: pd.DataFrame, station: str, variables: list, periode: str, colors: dict, metadata: dict) -> go.Figure:
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError(_("Le DataFrame doit avoir un DatetimeIndex"))

    filtered_df = df[df['Station'] == station].copy()
    if filtered_df.empty:
        return go.Figure()

    valid_vars = [v for v in variables if v in filtered_df.columns and pd.api.types.is_numeric_dtype(filtered_df[v])]
    if not valid_vars:
        warnings.warn(_("Aucune variable numérique valide trouvée"))
        return go.Figure()

    traces = []
    for i, var in enumerate(valid_vars, 1):
        # Resampling logic uses original period keys
        if periode == 'Journalière':
            serie = filtered_df[var].resample('D').mean()
        elif periode == 'Hebdomadaire':
            serie = filtered_df[var].resample('W').mean()
        elif periode == 'Mensuelle':
            serie = filtered_df[var].resample('M').mean()
        elif periode == 'Annuelle':
            serie = filtered_df[var].resample('Y').mean()
        else:
            serie = filtered_df[var]

        serie = serie.dropna()
        if serie.empty:
            continue

        min_val, max_val = serie.min(), serie.max()
        if max_val != min_val:
            serie_norm = (serie - min_val) / (max_val - min_val)
        else:
            serie_norm = serie * 0 + 0.5

        var_meta = metadata.get(var, {'Nom': {'fr': var, 'en': var}, 'Unite': {'fr': '', 'en': ''}})
        var_label = str(get_var_label(var_meta, 'Nom'))
        var_unit = str(get_var_label(var_meta, 'Unite'))
        color = colors.get(var, px.colors.qualitative.Plotly[i % len(px.colors.qualitative.Plotly)])

       
# In generate_multi_variable_station_plot:
        traces.append(
            go.Scatter(
                x=serie_norm.index,
                y=serie_norm,
                name=var_label,
                line=dict(color=color, width=2),
                mode='lines',
                hovertemplate=(
                    f"<b>{var_label}</b><br>" +
                    str(_("Date")) + ": %{x|%d/%m/%Y}<br>" +  # Removed extra curly braces around x
                    str(_("Valeur normalisée")) + ": %{y:.2f}<br>" + # Removed extra curly braces around y
                    str(_("Valeur originale")) + ": %{customdata[0]:.2f} " + var_unit + # Removed extra curly braces
                    "<extra></extra>"
                ),
                customdata=np.column_stack([serie.values])
            )
        )
        
    if not traces:
        return go.Figure()

    # Translate 'periode' for display in the title
    translated_periode = get_period_label(periode)

    fig = go.Figure(data=traces)
    fig.update_layout(
        # Use get_metric_label for the template string and pass translated period
        #title=str(get_metric_label("Comparaison normalisée des variables - %(station)s (%(periode)s)").format(station=station, periode=translated_periode)),
        title=str(_("Comparaison normalisée des variables - %(station)s (%(periode)s)", station=station, periode=translated_periode)),
        xaxis_title=str(_("Date")),
        yaxis_title=str(_("Valeur normalisée (0-1)")),
        hovermode="x unified",
        template="plotly_white",
        plot_bgcolor='white',
        paper_bgcolor='white',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        height=600,
        margin=dict(l=50, r=50, b=80, t=80, pad=4)
    )

    fig.update_layout(
        updatemenus=[
            dict(
                type="buttons",
                direction="left",
                buttons=list([
                    dict(
                        args=[{"visible": [True]*len(traces)}],
                        label=str(_("Tout afficher")),
                        method="update"
                    ),
                    dict(
                        args=[{"visible": [False]*len(traces)}],
                        label=str(_("Tout masquer")),
                        method="update"
                    )
                ]),
                pad={"r": 10, "t": 10},
                showactive=True,
                x=0.1,
                xanchor="left",
                y=1.15,
                yanchor="top"
            )
        ]
    )

    return fig


from config import METRIC_LABELS, METADATA_VARIABLES




# --- NEW: get_metric_label function ---
def get_metric_label(metric_key):
    """
    Retrieves the translated label for a given metric key based on the current locale
    from the METRIC_LABELS dictionary.
    """
    current_locale_str = str(get_locale())
    # Try the specific locale (e.g., 'fr'), then fallback to 'en', then the original key if not found
    return METRIC_LABELS.get(metric_key, {}).get(current_locale_str[:2], METRIC_LABELS.get(metric_key, {}).get('en', metric_key))



### Fonction fonctionnelle @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

# def generate_daily_stats_plot_plotly(df: pd.DataFrame, variable: str, station_colors: dict) -> go.Figure:
#     """
#     Dynamically generates interactive Plotly graphs for daily statistics,
#     including dates for maximums and minimums, with station-specific colors.
#     """
#     try:
#         df = df.copy()

#         # Remove unnecessary columns
#         col_sup = ['Rain_01_mm', 'Rain_02_mm']
#         for var in col_sup:
#             if var in df.columns:
#                 df = df.drop(var, axis=1)
                
#         if isinstance(df.index, pd.DatetimeIndex):
#             df = df.reset_index()
            
#         df['Datetime'] = pd.to_datetime(df['Datetime'], errors='coerce')
#         df = df.dropna(subset=['Datetime', 'Station'])
        
#         if 'Is_Daylight' not in df.columns:
#             df['Is_Daylight'] = (df['Datetime'].dt.hour >= 7) & (df['Datetime'].dt.hour <= 18)

#         if variable not in df.columns:
#             return go.Figure()

#         # Use get_var_label for variable specific names and units
#         var_meta = METADATA_VARIABLES.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
#         var_label = str(get_var_label(var_meta, 'Nom'))
#         var_unit = str(get_var_label(var_meta, 'Unite'))

#         # Initialize stats DataFrame
#         stats = pd.DataFrame()

#         # --- Specific processing for Rain ---
#         if var_meta.get('is_rain', False) and variable == 'Rain_mm':
#             # Daily rainfall calculation
#             df_daily_rain = df.groupby(['Station', pd.Grouper(key='Datetime', freq='D')])['Rain_mm'].sum().reset_index()

#             # Rainy season detection
#             RAIN_SEASON_GAP_THRESHOLD = pd.Timedelta(days=60)
#             season_stats = []

#             for station_name, station_df in df_daily_rain.groupby('Station'):
#                 station_df = station_df.set_index('Datetime').sort_index()
#                 rain_events = station_df[station_df['Rain_mm'] > 0].index

#                 if rain_events.empty:
#                     season_stats.append({
#                         'Station': station_name,
#                         get_metric_label('Moyenne Saison Pluvieuse'): np.nan, # Using new function
#                         get_metric_label('Début Saison Pluvieuse'): pd.NaT,
#                         get_metric_label('Fin Saison Pluvieuse'): pd.NaT,
#                         get_metric_label('Durée Saison Pluvieuse Jours'): np.nan
#                     })
#                     continue

#                 block_ids = (rain_events.to_series().diff() > RAIN_SEASON_GAP_THRESHOLD).cumsum()
#                 valid_blocks = {}

#                 for block_id, rain_dates_in_block in rain_events.to_series().groupby(block_ids):
#                     if not rain_dates_in_block.empty:
#                         block_start = rain_dates_in_block.min()
#                         block_end = rain_dates_in_block.max()
#                         full_block_df = station_df.loc[block_start:block_end]
#                         valid_blocks[block_id] = full_block_df

#                 if not valid_blocks:
#                     season_stats.append({
#                         'Station': station_name,
#                         get_metric_label('Moyenne Saison Pluvieuse'): np.nan,
#                         get_metric_label('Début Saison Pluvieuse'): pd.NaT,
#                         get_metric_label('Fin Saison Pluvieuse'): pd.NaT,
#                         get_metric_label('Durée Saison Pluvieuse Jours'): np.nan
#                     })
#                     continue

#                 main_block_id = max(valid_blocks, key=lambda k: (valid_blocks[k].index.max() - valid_blocks[k].index.min()).days)
#                 main_season_df = valid_blocks[main_block_id]

#                 debut_saison = main_season_df.index.min()
#                 fin_saison = main_season_df.index.max()
#                 total_days = (fin_saison - debut_saison).days + 1
#                 moyenne_saison = main_season_df['Rain_mm'].sum() / total_days if total_days > 0 else 0

#                 season_stats.append({
#                     'Station': station_name,
#                     get_metric_label('Moyenne Saison Pluvieuse'): moyenne_saison,
#                     get_metric_label('Début Saison Pluvieuse'): debut_saison,
#                     get_metric_label('Fin Saison Pluvieuse'): fin_saison,
#                     get_metric_label('Durée Saison Pluvieuse Jours'): total_days
#                 })

#             df_season_stats = pd.DataFrame(season_stats)

#             # Dry spell detection
#             station_dry_spell_events = []

#             for station_name, station_df in df_daily_rain.groupby('Station'):
#                 station_df = station_df.set_index('Datetime').sort_index()
#                 full_daily_series = station_df['Rain_mm'].resample('D').sum().fillna(0)
#                 rainy_days_index = full_daily_series[full_daily_series > 0].index

#                 if rainy_days_index.empty:
#                     continue

#                 for i in range(1, len(rainy_days_index)):
#                     prev_rain_date = rainy_days_index[i-1]
#                     current_rain_date = rainy_days_index[i]
#                     dry_days = (current_rain_date - prev_rain_date).days - 1

#                     if dry_days > 0:
#                         rain_prev_day = full_daily_series.loc[prev_rain_date]
#                         saison_moyenne = df_season_stats.loc[
#                             df_season_stats['Station'] == station_name,
#                             get_metric_label('Moyenne Saison Pluvieuse') # Access column by its translated name
#                         ].iloc[0] if not df_season_stats[df_season_stats['Station'] == station_name].empty else np.nan

#                         debut_secheresse = pd.NaT
#                         duree_secheresse = 0

#                         if pd.isna(saison_moyenne) or saison_moyenne == 0:
#                             continue

#                         for j in range(1, dry_days + 1):
#                             current_dry_date = prev_rain_date + timedelta(days=j)
#                             current_ratio = rain_prev_day / j

#                             if current_ratio < saison_moyenne:
#                                 debut_secheresse = current_dry_date
#                                 duree_secheresse = (current_rain_date - debut_secheresse).days
#                                 break

#                         if pd.notna(debut_secheresse) and duree_secheresse > 0:
#                             station_dry_spell_events.append({
#                                 'Station': station_name,
#                                 get_metric_label('Début Sécheresse Définie'): debut_secheresse,
#                                 get_metric_label('Fin Sécheresse Définie'): current_rain_date - timedelta(days=1),
#                                 get_metric_label('Durée Sécheresse Définie Jours'): duree_secheresse
#                             })

#             df_dry_spell_events = pd.DataFrame(station_dry_spell_events)

#             # Calculate final statistics with dates
#             stats = df[df['Rain_mm'] > 0].groupby('Station')['Rain_mm'].agg(
#                 Maximum='max', Minimum='min', Mediane='median' # These remain English keys for aggregation
#             ).reset_index()

#             # Add dates of max/min
#             max_dates = df.loc[df.groupby('Station')['Rain_mm'].idxmax()][['Station', 'Datetime']]
#             min_dates = df.loc[df.groupby('Station')['Rain_mm'].idxmin()][['Station', 'Datetime']]
            
#             # Rename columns with translatable strings
#             stats = stats.merge(
#                 max_dates.rename(columns={'Datetime': get_metric_label('Date Max')}),
#                 on='Station', how='left'
#             )
#             stats = stats.merge(
#                 min_dates.rename(columns={'Datetime': get_metric_label('Date Min')}),
#                 on='Station', how='left'
#             )

#             total_cumul = df.groupby('Station')['Rain_mm'].sum().reset_index(name=get_metric_label('Cumul Annuel'))
#             stats = stats.merge(total_cumul, on='Station', how='left')

#             stats = stats.merge(
#                 df_daily_rain[df_daily_rain['Rain_mm'] > 0].groupby('Station')['Rain_mm'].mean().reset_index().rename(
#                     columns={'Rain_mm': get_metric_label('Moyenne Jours Pluvieux')}),
#                 on='Station', how='left'
#             )
            
#             stats = stats.merge(
#                 df_season_stats[['Station', get_metric_label('Moyenne Saison Pluvieuse'), get_metric_label('Début Saison Pluvieuse'),
#                                  get_metric_label('Fin Saison Pluvieuse'), get_metric_label('Durée Saison Pluvieuse Jours')]],
#                 on='Station', how='left'
#             )

#             if not df_dry_spell_events.empty:
#                 longest_dry_spells = df_dry_spell_events.loc[
#                     df_dry_spell_events.groupby('Station')[get_metric_label('Durée Sécheresse Définie Jours')].idxmax()
#                 ][['Station', get_metric_label('Durée Sécheresse Définie Jours'), get_metric_label('Début Sécheresse Définie'), get_metric_label('Fin Sécheresse Définie')]]

#                 stats = stats.merge(longest_dry_spells, on='Station', how='left')

#             # List of original metric keys
#             metrics_to_plot_keys = [
#                 'Maximum', 'Minimum', 'Cumul Annuel', 'Mediane',
#                 'Moyenne Jours Pluvieux', 'Moyenne Saison Pluvieuse',
#                 'Durée Saison Pluvieuse Jours', 'Durée Sécheresse Définie Jours'
#             ]

#             # Generate subplot titles by translating each key using get_metric_label
#             subplot_titles = [f"{var_label} {get_metric_label(_key)}" for _key in metrics_to_plot_keys]

#             fig = make_subplots(
#                 rows=4, cols=2,
#                 subplot_titles=subplot_titles,
#                 vertical_spacing=0.1
#             )

#             for i, metric_key in enumerate(metrics_to_plot_keys):
#                 row = (i // 2) + 1
#                 col = (i % 2) + 1
                
#                 # Get the translated column name for accessing data from 'stats' DataFrame
#                 translated_col_name = get_metric_label(metric_key)

#                 if translated_col_name not in stats.columns:
#                     continue

#                 hover_text_list = []
#                 station_colors_list = []
#                 for _, row_data in stats.iterrows():
#                     station_name = row_data['Station']
#                     station_color = station_colors.get(station_name, '#1f77b4')
#                     station_colors_list.append(station_color)
                    
#                     value = row_data[translated_col_name]
#                     if pd.isna(value):
#                         hover_text_list.append("")
#                         continue

#                     # Adjust hover text logic for translations
#                     if metric_key in ['Durée Saison Pluvieuse Jours', 'Durée Sécheresse Définie Jours']:
#                         date_debut = ''
#                         date_fin = ''
#                         # Access dates by their translated column names
#                         if metric_key == 'Durée Saison Pluvieuse Jours' and pd.notna(row_data.get(get_metric_label('Début Saison Pluvieuse'))) and pd.notna(row_data.get(get_metric_label('Fin Saison Pluvieuse'))):
#                             date_debut = row_data[get_metric_label('Début Saison Pluvieuse')].strftime('%d/%m/%Y')
#                             date_fin = row_data[get_metric_label('Fin Saison Pluvieuse')].strftime('%d/%m/%Y')
#                         elif metric_key == 'Durée Sécheresse Définie Jours' and pd.notna(row_data.get(get_metric_label('Début Sécheresse Définie'))) and pd.notna(row_data.get(get_metric_label('Fin Sécheresse Définie'))):
#                             date_debut = row_data[get_metric_label('Début Sécheresse Définie')].strftime('%d/%m/%Y')
#                             date_fin = row_data[get_metric_label('Fin Sécheresse Définie')].strftime('%d/%m/%Y')

#                         hover_text = f"<b>{get_metric_label(metric_key)}</b><br>"
#                         if date_debut and date_fin:
#                             hover_text += get_metric_label("From {} to {}").format(date_debut, date_fin) + "<br>"
#                         hover_text += get_metric_label("Duration: {} days").format(int(value))
#                     elif metric_key == 'Maximum':
#                         date_str = row_data[get_metric_label('Date Max')].strftime('%d/%m/%Y') if pd.notna(row_data.get(get_metric_label('Date Max'))) else get_metric_label('Unknown date')
#                         hover_text = get_metric_label("<b>Maximum</b><br>Value: {:.1f} {}<br>Date: {}").format(value, var_unit, date_str)
#                     elif metric_key == 'Minimum':
#                         date_str = row_data[get_metric_label('Date Min')].strftime('%d/%m/%Y') if pd.notna(row_data.get(get_metric_label('Date Min'))) else get_metric_label('Unknown date')
#                         hover_text = get_metric_label("<b>Minimum</b><br>Value: {:.1f} {}<br>Date: {}").format(value, var_unit, date_str)
#                     elif metric_key in ['Cumul Annuel', 'Moyenne Jours Pluvieux', 'Moyenne Saison Pluvieuse', 'Mediane']:
#                         hover_text = get_metric_label("<b>{}</b><br>{:.1f} {}").format(get_metric_label(metric_key), value, var_unit)
#                     else:
#                         hover_text = get_metric_label("<b>{}</b><br>{:.1f} {}").format(get_metric_label(metric_key), value, var_unit)

#                     hover_text_list.append(hover_text)

#                 fig.add_trace(
#                     go.Bar(
#                         x=stats[translated_col_name],
#                         y=stats['Station'],
#                         orientation='h',
#                         marker_color=station_colors_list,
#                         name=get_metric_label(metric_key),
#                         hovertext=hover_text_list,
#                         hoverinfo='text',
#                         textposition='none'
#                     ),
#                     row=row,
#                     col=col
#                 )

#                 fig.update_xaxes(
#                     showgrid=False,
#                     row=row,
#                     col=col
#                 )
                
#                 # Dynamic x-axis title based on metric
#                 xaxis_title = get_metric_label("Days") if ("Durée" in metric_key or "Duration" in metric_key) else f"{var_label} ({var_unit})"
#                 fig.update_layout(
#                     {f'xaxis{i+1}_title': xaxis_title}
#                 )

#                 fig.update_yaxes(
#                     showgrid=False,
#                     row=row,
#                     col=col
#                 )

#             fig.update_layout(
#                 height=1200,
#                 title_text=get_metric_label("Statistics of {} by Station").format(var_label),
#                 showlegend=False,
#                 hovermode='closest',
#                 plot_bgcolor='white',
#                 paper_bgcolor='white'
#             )
            
#         else:
#             # --- Processing for other variables ---
#             df[variable] = pd.to_numeric(df[variable], errors='coerce')
#             df_clean = df.dropna(subset=[variable, 'Station']).copy()
            
#             if variable == 'Solar_R_W/m^2':
#                 df_clean = df_clean[df_clean['Is_Daylight']].copy()
            
#             if df_clean.empty:
#                 return go.Figure()
                
#             # Calculate statistics with dates
#             stats = df_clean.groupby('Station')[variable].agg(
#                 Maximum='max', Minimum='min', Mediane='median'
#             ).reset_index()
            
#             # Add dates of max/min
#             max_dates = df_clean.loc[df_clean.groupby('Station')[variable].idxmax()][['Station', 'Datetime']]
#             min_dates = df_clean.loc[df_clean.groupby('Station')[variable].idxmin()][['Station', 'Datetime']]
            
#             # Rename columns with translatable strings
#             stats = stats.merge(
#                 max_dates.rename(columns={'Datetime': get_metric_label('Date Max')}),
#                 on='Station', how='left'
#             )
#             stats = stats.merge(
#                 min_dates.rename(columns={'Datetime': get_metric_label('Date Min')}),
#                 on='Station', how='left'
#             )
            
#             stats[get_metric_label('Moyenne')] = df_clean.groupby('Station')[variable].mean().values

#             # List of original metric keys
#             metrics_to_plot_keys = ['Maximum', 'Minimum', 'Moyenne', 'Mediane']
            
#             # Generate subplot titles by translating each key using get_metric_label
#             subplot_titles = [f"{var_label} {get_metric_label(_key)}" for _key in metrics_to_plot_keys]
            
#             fig = make_subplots(
#                 rows=2, cols=2,
#                 subplot_titles=subplot_titles,
#                 vertical_spacing=0.15
#             )

#             for i, metric_key in enumerate(metrics_to_plot_keys):
#                 row = (i // 2) + 1
#                 col = (i % 2) + 1
                
#                 # Get the translated column name for accessing data from 'stats' DataFrame
#                 translated_col_name = get_metric_label(metric_key)

#                 if translated_col_name not in stats.columns:
#                     continue

#                 hover_text_list = []
#                 station_colors_list = []
#                 for _, row_data in stats.iterrows():
#                     station_name = row_data['Station']
#                     station_color = station_colors.get(station_name, '#1f77b4')
#                     station_colors_list.append(station_color)
                    
#                     value = row_data[translated_col_name]
#                     if pd.isna(value):
#                         hover_text_list.append("")
#                         continue

#                     # Adjust hover text logic for translations
#                     if metric_key == 'Maximum':
#                         date_str = row_data[get_metric_label('Date Max')].strftime('%d/%m/%Y') if pd.notna(row_data.get(get_metric_label('Date Max'))) else get_metric_label('Unknown date')
#                         hover_text = get_metric_label("<b>Maximum</b><br>Value: {:.1f} {}<br>Date: {}").format(value, var_unit, date_str)
#                     elif metric_key == 'Minimum':
#                         date_str = row_data[get_metric_label('Date Min')].strftime('%d/%m/%Y') if pd.notna(row_data.get(get_metric_label('Date Min'))) else get_metric_label('Unknown date')
#                         hover_text = get_metric_label("<b>Minimum</b><br>Value: {:.1f} {}<br>Date: {}").format(value, var_unit, date_str)
#                     else:
#                         hover_text = get_metric_label("<b>{}</b><br>{:.1f} {}").format(get_metric_label(metric_key), value, var_unit)

#                     hover_text_list.append(hover_text)

#                 fig.add_trace(
#                     go.Bar(
#                         x=stats[translated_col_name],
#                         y=stats['Station'],
#                         orientation='h',
#                         marker_color=station_colors_list,
#                         name=get_metric_label(metric_key),
#                         hovertext=hover_text_list,
#                         hoverinfo='text',
#                         textposition='none'
#                     ),
#                     row=row,
#                     col=col
#                 )

#                 fig.update_xaxes(
#                     showgrid=False,
#                     row=row,
#                     col=col
#                 )
                
#                 fig.update_layout(
#                     {f'xaxis{i+1}_title': f"{var_label} ({var_unit})"}
#                 )

#                 fig.update_yaxes(
#                     showgrid=False,
#                     row=row,
#                     col=col
#                 )

#             fig.update_layout(
#                 height=800,
#                 title_text=get_metric_label("Statistics of {} by Station").format(var_label),
#                 showlegend=False,
#                 hovermode='closest',
#                 plot_bgcolor='white',
#                 paper_bgcolor='white'
#             )
        
#         return fig
        
#     except Exception as e:
#         print(f"Erreur dans generate_daily_stats_plot_plotly: {str(e)}")
#         traceback.print_exc()
#         return go.Figure()


import pandas as pd
import plotly.express as px

# import pandas as pd
# import plotly.express as px

# def valeurs_manquantes_viz(df: pd.DataFrame):
#     """
#     Retourne une figure Plotly de type diagramme en cercle (pie chart)
#     montrant le pourcentage total de valeurs présentes et manquantes
#     pour l'ensemble du DataFrame.
#     """
#     df_copy = df.copy()

#     # Calcul du nombre total de cellules dans le DataFrame
#     total_cells = df_copy.size # df.size donne le nombre total de cellules (lignes * colonnes)

#     # Calcul du nombre de valeurs manquantes
#     missing_values_count = df_copy.isnull().sum().sum()

#     # Calcul du nombre de valeurs présentes
#     present_values_count = total_cells - missing_values_count

#     # Création d'un DataFrame pour le pie chart
#     data_for_pie = pd.DataFrame({
#         'Catégorie': ['Valeurs Présentes', 'Valeurs Manquantes'],
#         'Nombre': [present_values_count, missing_values_count]
#     })

#     # Calcul des pourcentages pour l'affichage au survol
#     data_for_pie['Pourcentage'] = (data_for_pie['Nombre'] / total_cells * 100).round(2)

#     fig = px.pie(
#         data_for_pie,
#         values='Nombre',
#         names='Catégorie',
#         title=f"Répartition globale des valeurs (Total: {total_cells} cellules)",
#         hole=0.3, # Crée un diagramme en anneau
#         color='Catégorie',
#         color_discrete_map={
#             'Valeurs Présentes': 'lightgreen',
#             'Valeurs Manquantes': 'lightcoral'
#         }
#     )

#     # Personnaliser le texte affiché au survol
#     fig.update_traces(
#         textinfo='percent+label', # Affiche le pourcentage et le label
#         hovertemplate="<b>%{label}</b><br>Nombre: %{value}<br>Pourcentage: %{percent:.2%}<extra></extra>"
#     )

#     fig.update_layout(showlegend=True)

#     return fig

# # Note: La fonction outliers_viz ne change pas et conserve la heatmap.
# # Si vous souhaitez un autre type de visualisation pour les outliers, veuillez le spécifier.

# def outliers_viz(df: pd.DataFrame, coef: float = 1.5):
#     """
#     Retourne une figure Plotly du pourcentage d'outliers (méthode IQR) par variable et station.
#     """
#     df_copy = df.copy()
#     numeric_cols = df_copy.select_dtypes(include='number').columns
#     stations = df_copy['Station'].unique()

#     data = []
#     for station in stations:
#         group = df_copy[df_copy['Station'] == station]
#         for col in numeric_cols:
#             Q1 = group[col].quantile(0.25)
#             Q3 = group[col].quantile(0.75)
#             IQR = Q3 - Q1
#             lower = Q1 - coef * IQR
#             upper = Q3 + coef * IQR
#             total = group[col].count()
#             outliers = group[(group[col] < lower) | (group[col] > upper)][col].count()
#             percent = (outliers / total * 100) if total > 0 else 0
#             data.append({'Station': station, 'Variable': col, 'Pourcentage': round(percent, 2)})

#     df_outliers = pd.DataFrame(data)

#     fig = px.imshow(
#         df_outliers.pivot(index='Variable', columns='Station', values='Pourcentage'),
#         labels=dict(color="% outliers"),
#         color_continuous_scale='YlGnBu',
#         aspect='auto'
#     )
#     fig.update_layout(
#         title="Pourcentage d'outliers par variable et station (IQR)",
#         xaxis_title="Station",
#         yaxis_title="Variable"
#     )
#     return fig

# import pandas as pd
# import plotly.express as px
# from plotly.subplots import make_subplots
# import plotly.graph_objects as go

# def valeurs_manquantes_viz(df: pd.DataFrame):
#     """
#     Retourne une figure Plotly avec des sous-plots (diagrammes en cercle)
#     montrant le pourcentage de valeurs présentes et manquantes pour chaque variable.
#     """
#     df_copy = df.copy()

#     # Exclure les colonnes non-numériques ou d'identification qui ne sont pas des "variables" à analyser
#     # Adapter cette liste si vous avez d'autres colonnes d'ID/non-mesure.
#     cols_to_exclude = ['Datetime', 'Station']
    
#     # Filtrer les colonnes qui sont réellement des variables à mesurer
#     variable_columns = [col for col in df_copy.columns if col not in cols_to_exclude]


#     if not variable_columns:
#         # Gérer le cas où il n'y a pas de variables à analyser
#         fig = go.Figure().add_annotation(
#             x=0.5, y=0.5, text="Aucune variable à analyser pour les valeurs manquantes.",
#             showarrow=False, font=dict(size=16)
#         )
#         fig.update_layout(title="Analyse des valeurs manquantes")
#         return fig

#     # Déterminer la taille de la grille des sous-plots
#     num_variables = len(variable_columns)
#     cols_per_row = 3 # Nombre de camemberts par ligne dans la grille
#     rows = (num_variables + cols_per_row - 1) // cols_per_row # Calcul le nombre de lignes nécessaires

#     # Créer les sous-plots
#     fig = make_subplots(
#         rows=rows,
#         cols=cols_per_row,
#         specs=[[{'type':'domain'}]*cols_per_row] * rows, # Spécifie que chaque sous-plot est un camembert
#         subplot_titles=variable_columns # Les titres des sous-plots seront les noms des variables
#     )

#     for i, col in enumerate(variable_columns):
#         # Calculer les valeurs présentes et manquantes pour la colonne actuelle
#         present_count = df_copy[col].count()
#         missing_count = df_copy[col].isnull().sum()
#         total_for_col = present_count + missing_count

#         if total_for_col == 0:
#             # Gérer le cas où une colonne est entièrement vide
#             pie_data = pd.DataFrame({'Catégorie': ['Aucune donnée'], 'Nombre': [1], 'Pourcentage': [100.0]})
#         else:
#             pie_data = pd.DataFrame({
#                 'Catégorie': ['Valeurs Présentes', 'Valeurs Manquantes'],
#                 'Nombre': [present_count, missing_count]
#             })
#             pie_data['Pourcentage'] = (pie_data['Nombre'] / total_for_col * 100).round(2)

#         # Ajouter le camembert au sous-plot approprié
#         row_idx = (i // cols_per_row) + 1
#         col_idx = (i % cols_per_row) + 1

#         # Utiliser go.Pie directement pour plus de contrôle dans les sous-plots
#         fig.add_trace(
#             go.Pie(
#                 labels=pie_data['Catégorie'],
#                 values=pie_data['Nombre'],
#                 name=col, # Nom de la trace
#                 textinfo='percent+label', # Affiche le pourcentage et le label sur la tranche
#                 hovertemplate="<b>%{label}</b><br>Nombre: %{value}<br>Pourcentage: %{percent:.2%}<extra></extra>", # <-- Corrected hovertemplate
#                 marker=dict(colors=['lightgreen', 'lightcoral'] if 'Aucune donnée' not in pie_data['Catégorie'].tolist() else ['lightgray'])
#             ),
#             row=row_idx, col=col_idx
#         )

#     fig.update_layout(
#         title_text="Pourcentage de valeurs manquantes par variable",
#         showlegend=False, # La légende n'est pas nécessaire car les labels sont dans les titres et camemberts
#         height=300 * rows, # Ajuster la hauteur totale de la figure
#         margin=dict(l=50, r=50, b=50, t=80) # Ajuster les marges
#     )
    
#     # Centrer les titres des sous-plots
#     for i in range(num_variables):
#         fig.layout.annotations[i].update(x=fig.layout.annotations[i].x)


#     return fig


import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# def outliers_viz(df: pd.DataFrame, coef: float = 1.5):
#     """
#     Retourne une figure Plotly avec des sous-plots (scatter plots)
#     montrant les outliers pour chaque variable numérique.
#     Les points sont colorés en fonction de leur statut d'outlier (par station).
#     """
#     df_copy = df.copy()

#     # Ensure 'Datetime' is a column for plotting (it might be an index)
#     if df_copy.index.name == 'Datetime':
#         df_copy = df_copy.reset_index()
#     elif 'Datetime' not in df_copy.columns:
#         # Fallback if Datetime is neither index nor column (shouldn't happen with previous fixes)
#         return go.Figure().add_annotation(
#             x=0.5, y=0.5, text="Colonne 'Datetime' manquante pour la visualisation des outliers.",
#             showarrow=False, font=dict(size=16)
#         )

#     # # Identify numeric columns for outlier analysis, excluding 'Station' and 'Datetime'
#     # numeric_cols = df_copy.select_dtypes(include='number').columns.drop(['Station'], errors='ignore')
#     # numeric_cols = [col for col in numeric_cols if col != 'Datetime'] # Ensure Datetime is also not treated as a variable for outliers

#     cols_to_exclude = ['Datetime', 'Station', 'Rain_01_mm', 'Rain_02_mm', 'Is_Daylight', 'Day_Duration', 'sunrise_time_utc', 'sunset_time_utc', 'Day']
#         #cols_to_exclude = ['Year', 'Month', 'Minute', 'Hour', 'Date', 'Day', 'Daylight_Duration', 'sunrise_time_utc', 'sunset_time_utc']

    
#     # Filtrer les colonnes qui sont réellement des variables à analyser
#     numeric_cols = [col for col in df_copy.columns if col not in cols_to_exclude]

#     if not numeric_cols:
#         fig = go.Figure().add_annotation(
#             x=0.5, y=0.5, text="Aucune variable numérique pour l'analyse des outliers.",
#             showarrow=False, font=dict(size=16)
#         )
#         fig.update_layout(title="Analyse des Outliers")
#         return fig

#     # Pre-calculate outlier status for each point
#     df_with_outlier_status = df_copy.copy()
    
#     # Initialize a column to store outlier status for each variable
#     for col in numeric_cols:
#         df_with_outlier_status[f'{col}_is_outlier'] = False # Default to False

#     stations = df_with_outlier_status['Station'].unique()

#     # Calculate outlier bounds per station and mark outliers
#     for station in stations:
#         station_df = df_with_outlier_status[df_with_outlier_status['Station'] == station]
#         for col in numeric_cols:
#             Q1 = station_df[col].quantile(0.25)
#             Q3 = station_df[col].quantile(0.75)
#             IQR = Q3 - Q1
#             lower = Q1 - coef * IQR
#             upper = Q3 + coef * IQR

#             # Identify outlier indices for this station and column
#             outlier_indices = station_df[(station_df[col] < lower) | (station_df[col] > upper)].index
            
#             # Update the global df_with_outlier_status DataFrame
#             df_with_outlier_status.loc[outlier_indices, f'{col}_is_outlier'] = True
    
#     # Determine subplot grid dimensions
#     num_plots = len(numeric_cols)
#     cols_per_row = 2 # Number of scatter plots per row
#     rows = (num_plots + cols_per_row - 1) // cols_per_row

#     fig = make_subplots(
#         rows=rows,
#         cols=cols_per_row,
#         subplot_titles=numeric_cols, # Titles for each subplot
#         x_title="Date",
#         y_title="Valeur" # This will be overwritten by individual subplot titles
#     )

#     # Get a list of colors for stations if multiple
#     num_stations = len(stations)
#     if num_stations > 1:
#         # Use Plotly's default color cycle if many stations, or define custom ones
#         # For simplicity, px.scatter will handle colors automatically if `color='Station'` is used
#         pass

#     for i, col in enumerate(numeric_cols):
#         row_idx = (i // cols_per_row) + 1
#         col_idx = (i % cols_per_row) + 1

#         # Create the scatter plot for the current variable
#         # Color by outlier status and optionally by station for differentiation
#         # Using px.scatter for easier coloring and legend
#         scatter_plot = px.scatter(
#             df_with_outlier_status,
#             x='Datetime',
#             y=col,
#             color=f'{col}_is_outlier', # Color by outlier status (True/False)
#             color_discrete_map={
#                 True: 'red',    # Outliers
#                 False: 'blue'   # Inliers
#             },
#             hover_name='Station', # Show Station name on hover
#             hover_data={
#                 'Datetime': '|%Y-%m-%d %H:%M:%S', # Format Datetime on hover
#                 col: True, # Show variable value
#                 'Station': True, # Show Station
#                 f'{col}_is_outlier': False # Don't show the boolean flag itself
#             },
#             labels={
#                 f'{col}_is_outlier': 'Est un Outlier', # Label for color legend
#                 col: col # Label for Y-axis
#             },
#             title=f"Distribution de {col} avec Outliers" # Individual subplot title
#         )
        
#         # Add traces from px.scatter to the make_subplots figure
#         for trace in scatter_plot.data:
#             trace.showlegend = (trace.name == 'True' or trace.name == 'False') # Show legend only for Outlier status
#             fig.add_trace(trace, row=row_idx, col=col_idx)

#         # Update layout for individual subplot axes (titles, ranges, etc.)
#         fig.update_xaxes(title_text="Date", row=row_idx, col=col_idx)
#         fig.update_yaxes(title_text=col, row=row_idx, col=col_idx)

#     # Update overall layout
#     fig.update_layout(
#         title_text="Analyse des Outliers par Variable",
#         height=400 * rows, # Adjust total height based on number of rows
#         showlegend=True, # Show a combined legend for outlier status
#         legend_title_text="Statut d'Outlier"
#     )
    
#     return fig


import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import warnings
from flask_babel import _, lazy_gettext as _l

# def outliers_viz(df: pd.DataFrame, coef: float = 1.5):
#     """
#     Retourne une figure Plotly avec des sous-plots (scatter plots)
#     montrant les outliers pour chaque variable numérique.
#     Les points sont colorés en fonction de leur statut d'outlier (par station).

#     Args:
#         df (pd.DataFrame): Le DataFrame d'entrée contenant les données.
#                            Doit contenir une colonne 'Station' et un DatetimeIndex
#                            ou une colonne 'Datetime'.
#         coef (float): Le coefficient IQR pour la détection des outliers (par défaut 1.5).

#     Returns:
#         go.Figure: Une figure Plotly visualisant les outliers.
#     """
#     df_copy = df.copy()

#     # Ensure 'Datetime' is a column for plotting (it might be an index)
#     if df_copy.index.name == 'Datetime':
#         df_copy = df_copy.reset_index()
#     elif 'Datetime' not in df_copy.columns:
#         # Fallback if Datetime is neither index nor column (shouldn't happen with previous fixes)
#         return go.Figure().add_annotation(
#             x=0.5, y=0.5, text=str(_l("Colonne 'Datetime' manquante pour la visualisation des outliers.")),
#             showarrow=False, font=dict(size=16)
#         )

#     # Convert all potentially numeric columns to numeric, coercing errors
#     # This is crucial for handling mixed types or strings that look like numbers
#     for col in df_copy.columns:
#         # Try converting only if it's not 'Datetime' or 'Station'
#         if col not in ['Datetime', 'Station']:
#             # Attempt to convert to numeric, turning unconvertible values into NaN
#             df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')

#     # Identify numeric columns for outlier analysis after conversion
#     # Exclude 'Station' as it's categorical, and 'Datetime' (handled as x-axis)
#     # Also exclude columns that are purely flags or derived categorical (like Is_Daylight)
#     # or specific rain components if only 'Rain_mm' is desired for analysis.
    
#     # Define columns that should definitively NOT be treated as numerical for outlier calculation
#     # These are typically identifiers, dates, or boolean/categorical flags.
#     base_cols_to_exclude_from_numeric_analysis = [
#         'Datetime', 'Station', 'Is_Daylight', 'Day', 
#         'sunrise_time_utc', 'sunset_time_utc', 'Daylight_Duration'
#     ]
    
#     # Also, if Rain_01_mm and Rain_02_mm are only used to derive Rain_mm,
#     # and you don't want to analyze them for outliers directly, add them.
#     # Same for 'Day_Duration' if 'Daylight_Duration' is the intended output.
#     additional_cols_to_exclude = ['Rain_01_mm', 'Rain_02_mm', 'Day_Duration']

#     all_cols_to_exclude = list(set(base_cols_to_exclude_from_numeric_analysis + additional_cols_to_exclude))

#     # Filter columns that are numeric AND not in our exclusion list
#     # Use df_copy.select_dtypes(include='number') for robustness against non-numeric data
#     numeric_cols = df_copy.select_dtypes(include='number').columns.tolist()
#     numeric_cols = [col for col in numeric_cols if col not in all_cols_to_exclude]

#     if not numeric_cols:
#         fig = go.Figure().add_annotation(
#             x=0.5, y=0.5, text=str(_l("Aucune variable numérique pour l'analyse des outliers après nettoyage.")),
#             showarrow=False, font=dict(size=16)
#         )
#         fig.update_layout(title=str(_l("Analyse des Outliers")))
#         return fig

#     # Pre-calculate outlier status for each point
#     df_with_outlier_status = df_copy.copy()
    
#     # Initialize a column to store outlier status for each variable
#     for col in numeric_cols:
#         df_with_outlier_status[f'{col}_is_outlier'] = False # Default to False

#     stations = df_with_outlier_status['Station'].unique()

#     # Calculate outlier bounds per station and mark outliers
#     for station in stations:
#         station_df = df_with_outlier_status[df_with_outlier_status['Station'] == station]
#         for col in numeric_cols:
#             # Ensure there's enough non-NaN data to calculate quantiles
#             if station_df[col].count() > 1: # Need at least 2 non-NaN values for quantile
#                 Q1 = station_df[col].quantile(0.25)
#                 Q3 = station_df[col].quantile(0.75)
#                 IQR = Q3 - Q1
#                 lower = Q1 - coef * IQR
#                 upper = Q3 + coef * IQR

#                 # Identify outlier indices for this station and column
#                 # Make sure to only consider non-NaN values for outlier detection
#                 outlier_indices = station_df[(station_df[col].notna()) & 
#                                              ((station_df[col] < lower) | (station_df[col] > upper))].index
                
#                 # Update the global df_with_outlier_status DataFrame
#                 df_with_outlier_status.loc[outlier_indices, f'{col}_is_outlier'] = True
#             else:
#                 warnings.warn(str(_l("Pas assez de données pour calculer les outliers pour la station '%s' et la variable '%s'.") % (station, col)))
    
#     # Determine subplot grid dimensions
#     num_plots = len(numeric_cols)
#     cols_per_row = 2 # Number of scatter plots per row
#     rows = (num_plots + cols_per_row - 1) // cols_per_row

#     fig = make_subplots(
#         rows=rows,
#         cols=cols_per_row,
#         subplot_titles=numeric_cols, # Titles for each subplot
#         # x_title="Date", # Cannot set for all at once, must be per axis
#         # y_title="Valeur" # This will be overwritten by individual subplot titles
#     )

#     # Get a list of colors for stations if multiple
#     num_stations = len(stations)

#     for i, col in enumerate(numeric_cols):
#         row_idx = (i // cols_per_row) + 1
#         col_idx = (i % cols_per_row) + 1

#         # Use px.scatter to generate traces, then add them to the subplot figure
#         # Create separate traces for inliers and outliers to control legend more effectively
#         df_inliers = df_with_outlier_status[df_with_outlier_status[f'{col}_is_outlier'] == False]
#         df_outliers = df_with_outlier_status[df_with_outlier_status[f'{col}_is_outlier'] == True]

#         # Inliers trace
#         fig.add_trace(
#             go.Scatter(
#                 x=df_inliers['Datetime'],
#                 y=df_inliers[col],
#                 mode='markers',
#                 name=str(_l('Valeurs normales')),
#                 marker=dict(color='blue', size=5),
#                 hovertemplate=(
#                     '<b>Station:</b> %{customdata[0]}<br>' +
#                     '<b>Date:</b> %{x|%Y-%m-%d %H:%M:%S}<br>' +
#                     f'<b>{col}:</b> %{{y}}<br>' +
#                     '<extra></extra>' # Hides trace name in hover
#                 ),
#                 customdata=df_inliers[['Station']]
#             ),
#             row=row_idx, col=col_idx
#         )

#         # Outliers trace
#         fig.add_trace(
#             go.Scatter(
#                 x=df_outliers['Datetime'],
#                 y=df_outliers[col],
#                 mode='markers',
#                 name=str(_l('Outliers')),
#                 marker=dict(color='red', size=7, symbol='circle-open', line=dict(width=2, color='red')),
#                 hovertemplate=(
#                     '<b>Station:</b> %{customdata[0]}<br>' +
#                     '<b>Date:</b> %{x|%Y-%m-%d %H:%M:%S}<br>' +
#                     f'<b>{col}:</b> %{{y}}<br>' +
#                     '<extra></extra>' # Hides trace name in hover
#                 ),
#                 customdata=df_outliers[['Station']]
#             ),
#             row=row_idx, col=col_idx
#         )
        
#         # Update layout for individual subplot axes (titles, ranges, etc.)
#         fig.update_xaxes(title_text=str(_l("Date")), row=row_idx, col=col_idx)
#         fig.update_yaxes(title_text=col, row=row_idx, col=col_idx)

#     # Update overall layout
#     fig.update_layout(
#         title_text=str(_l("Analyse des Outliers par Variable")),
#         height=400 * rows, # Adjust total height based on number of rows
#         showlegend=True, # Show a combined legend for outlier status
#         legend_title_text=str(_l("Statut d'Outlier"))
#     )
    
#     return fig


def outliers_viz(df: pd.DataFrame, coef: float = 1.5) -> go.Figure:
    """
    Visualisation des outliers pour chaque variable numérique via des scatter plots Plotly.
    """
    df = df.copy()
    if df.index.name == 'Datetime':
        df.reset_index(inplace=True)
    elif 'Datetime' not in df.columns:
        return go.Figure().add_annotation(
            x=0.5, y=0.5, text=_l("Colonne 'Datetime' manquante pour la visualisation des outliers."),
            showarrow=False, font=dict(size=16)
        )

    # Conversion des colonnes numériques (hors colonnes à exclure)
    exclude_cols = {
        'Datetime', 'Station', 'Is_Daylight', 'Day', 
        'sunrise_time_utc', 'sunset_time_utc', 'Daylight_Duration',
        'Rain_01_mm', 'Rain_02_mm', 'Day_Duration'
    }

    for col in df.columns:
        if col not in exclude_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    numeric_cols = [
        col for col in df.select_dtypes(include='number').columns
        if col not in exclude_cols
    ]

    if not numeric_cols:
        return go.Figure().add_annotation(
            x=0.5, y=0.5, text=_l("Aucune variable numérique pour l'analyse des outliers après nettoyage."),
            showarrow=False, font=dict(size=16)
        ).update_layout(title=_l("Analyse des Outliers"))

    # Détection des outliers par station et par variable
    df_out = df.copy()
    for col in numeric_cols:
        df_out[f'{col}_is_outlier'] = False

    for station, group in df.groupby('Station'):
        for col in numeric_cols:
            if group[col].count() > 1:
                Q1, Q3 = group[col].quantile([0.25, 0.75])
                IQR = Q3 - Q1
                lower, upper = Q1 - coef * IQR, Q3 + coef * IQR
                outlier_idx = group[(group[col] < lower) | (group[col] > upper)].index
                df_out.loc[outlier_idx, f'{col}_is_outlier'] = True
            else:
                warnings.warn(_l("Pas assez de données pour calculer les outliers pour la station '%s' et la variable '%s'.") % (station, col))

    # Préparation des subplots
    cols_per_row = 2
    n = len(numeric_cols)
    rows = (n + cols_per_row - 1) // cols_per_row

    fig = make_subplots(rows=rows, cols=cols_per_row, subplot_titles=numeric_cols)

    # Génération des traces
    for i, col in enumerate(numeric_cols):
        r, c = divmod(i, cols_per_row)
        r += 1; c += 1

        for outlier_status, color, label, marker_opts in [
            (False, 'blue', _l('Valeurs normales'), dict(size=5)),
            (True, 'red', _l('Outliers'), dict(size=7, symbol='circle-open', line=dict(width=2, color='red')))
        ]:
            subset = df_out[df_out[f'{col}_is_outlier'] == outlier_status]
            fig.add_trace(
                go.Scatter(
                    x=subset['Datetime'], y=subset[col],
                    mode='markers',
                    name=label,
                    marker=dict(color=color, **marker_opts),
                    hovertemplate=(
                        "<b>Station:</b> %{customdata[0]}<br>"
                        "<b>Date:</b> %{x|%Y-%m-%d %H:%M:%S}<br>"
                        f"<b>{col}:</b> %{{y}}<br><extra></extra>"
                    ),
                    customdata=subset[['Station']],
                    showlegend=(i == 0)  # Ne montrer qu'une fois la légende
                ),
                row=r, col=c
            )

        fig.update_xaxes(title_text=_l("Date"), row=r, col=c)
        fig.update_yaxes(title_text=col, row=r, col=c)

    # Mise en forme finale
    fig.update_layout(
        title=_l("Analyse des Outliers par Variable"),
        height=400 * rows,
        showlegend=True,
        legend_title_text=_l("Statut d'Outlier")
    )

    return fig

import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# def valeurs_manquantes_viz(df: pd.DataFrame):
#     """
#     Retourne une figure Plotly avec des sous-plots (diagrammes en cercle)
#     montrant le pourcentage de valeurs présentes et manquantes pour chaque variable.
#     Affiche une légende et exclut certaines variables.
#     """
#     df_copy = df.copy()

#     # Colonnes à exclure de l'analyse des valeurs manquantes
#     # 'Datetime' et 'Station' sont des colonnes d'identification, pas des mesures.
#     # Les variables spécifiques demandées sont ajoutées ici.
#     cols_to_exclude = ['Datetime', 'Station', 'Rain_01_mm', 'Rain_02_mm', 'Is_Daylight', 'Day_Duration', 'sunrise_time_utc', 'sunset_time_utc', 'Day']
#         #cols_to_exclude = ['Year', 'Month', 'Minute', 'Hour', 'Date', 'Day', 'Daylight_Duration', 'sunrise_time_utc', 'sunset_time_utc']

    
#     # Filtrer les colonnes qui sont réellement des variables à analyser
#     variable_columns = [col for col in df_copy.columns if col not in cols_to_exclude]

#     if not variable_columns:
#         # Gérer le cas où il n'y a pas de variables à analyser
#         fig = go.Figure().add_annotation(
#             x=0.5, y=0.5, text="Aucune variable à analyser pour les valeurs manquantes.",
#             showarrow=False, font=dict(size=16)
#         )
#         fig.update_layout(title="Analyse des valeurs manquantes")
#         return fig

#     # Déterminer la taille de la grille des sous-plots
#     num_variables = len(variable_columns)
#     cols_per_row = 3 # Nombre de camemberts par ligne dans la grille
#     rows = (num_variables + cols_per_row - 1) // cols_per_row # Calcul le nombre de lignes nécessaires

#     # Créer les sous-plots
#     fig = make_subplots(
#         rows=rows,
#         cols=cols_per_row,
#         specs=[[{'type':'domain'}]*cols_per_row] * rows, # Spécifie que chaque sous-plot est un camembert
#         subplot_titles=variable_columns # Les titres des sous-plots seront les noms des variables
#     )

#     # Définir les couleurs pour la légende
#     colors = ['lightgreen', 'lightcoral']
    
#     # Créer un dictionnaire pour suivre si la légende a déjà été ajoutée pour "Présentes" et "Manquantes"
#     # Plotly ajoute automatiquement une légende unique pour chaque 'name' de trace s'il y a plusieurs traces
#     # Nous allons donner un 'name' spécifique aux traces "Valeurs Présentes" et "Valeurs Manquantes"
#     # pour qu'elles apparaissent une seule fois dans la légende globale.

#     for i, col in enumerate(variable_columns):
#         present_count = df_copy[col].count()
#         missing_count = df_copy[col].isnull().sum()
#         total_for_col = present_count + missing_count

#         if total_for_col == 0:
#             pie_data = pd.DataFrame({'Catégorie': ['Aucune donnée'], 'Nombre': [1]})
#             # Pas de pourcentage car pas de données
#             trace_colors = ['lightgray']
#             textinfo_val = 'none' # Ne pas afficher de texte sur la tranche
#         else:
#             pie_data = pd.DataFrame({
#                 'Catégorie': ['Valeurs Présentes', 'Valeurs Manquantes'],
#                 'Nombre': [present_count, missing_count]
#             })
#             trace_colors = colors
#             textinfo_val = 'percent' # Affiche seulement le pourcentage sur la tranche

#         row_idx = (i // cols_per_row) + 1
#         col_idx = (i % cols_per_row) + 1

#         fig.add_trace(
#             go.Pie(
#                 labels=pie_data['Catégorie'],
#                 values=pie_data['Nombre'],
#                 # Utiliser le même nom pour toutes les traces de "Valeurs Présentes" et "Valeurs Manquantes"
#                 # pour qu'elles n'apparaissent qu'une seule fois dans la légende globale
#                 name=col, # Ceci sert de référence pour le subplot_title
#                 textinfo=textinfo_val, # N'affiche que le pourcentage sur la tranche
#                 hovertemplate="<b>%{label}</b><br>Nombre: %{value}<br>Pourcentage: %{percent:.2%}<extra></extra>",
#                 marker=dict(colors=trace_colors)
#             ),
#             row=row_idx, col=col_idx
#         )

#     # Mettre à jour la mise en page de la figure
#     fig.update_layout(
#         title_text="Pourcentage de valeurs manquantes par variable",
#         showlegend=True, # Afficher la légende globale
#         legend=dict(
#             orientation="h", # Légende horizontale
#             yanchor="bottom", # Ancrée en bas
#             y=-0.15, # Positionnement sous les graphiques
#             xanchor="center", # Ancrée au centre
#             x=0.5 # Centrée horizontalement
#         ),
#         height=350 * rows, # Ajuster la hauteur totale de la figure
#         margin=dict(l=50, r=50, b=100, t=80) # Ajuster les marges pour laisser de la place à la légende
#     )
    
#     # Centrer les titres des sous-plots
#     for i in range(num_variables):
#         fig.layout.annotations[i].update(x=fig.layout.annotations[i].x)



#     return fig


def valeurs_manquantes_viz(df: pd.DataFrame) -> go.Figure:
    """
    Génère une figure Plotly contenant des diagrammes en secteurs
    pour illustrer les pourcentages de valeurs manquantes par variable.
    """
    # Colonnes à exclure de l’analyse
    exclude = {
        'Datetime', 'Station', 'Rain_01_mm', 'Rain_02_mm',
        'Is_Daylight', 'Day_Duration', 'sunrise_time_utc', 'sunset_time_utc', 'Day'
    }

    variables = [col for col in df.columns if col not in exclude]

    if not variables:
        return go.Figure().add_annotation(
            x=0.5, y=0.5, text="Aucune variable à analyser pour les valeurs manquantes.",
            showarrow=False, font=dict(size=16)
        ).update_layout(title="Analyse des valeurs manquantes")

    # Paramètres pour l'affichage
    cols_per_row = 3
    rows = (len(variables) + cols_per_row - 1) // cols_per_row
    colors = ['lightgreen', 'lightcoral']

    # Créer la figure avec sous-graphiques de type "camembert"
    fig = make_subplots(
        rows=rows,
        cols=cols_per_row,
        specs=[[{'type': 'domain'}]*cols_per_row]*rows,
        subplot_titles=variables
    )

    for i, col in enumerate(variables):
        present = df[col].count()
        missing = df[col].isna().sum()
        total = present + missing

        if total == 0:
            values = [1]
            labels = ['Aucune donnée']
            pie_colors = ['lightgray']
            textinfo = 'none'
        else:
            values = [present, missing]
            labels = ['Valeurs Présentes', 'Valeurs Manquantes']
            pie_colors = colors
            textinfo = 'percent'

        r, c = divmod(i, cols_per_row)
        fig.add_trace(
            go.Pie(
                labels=labels,
                values=values,
                name=col,
                marker=dict(colors=pie_colors),
                textinfo=textinfo,
                hovertemplate="<b>%{label}</b><br>Nombre: %{value}<br>Pourcentage: %{percent:.2%}<extra></extra>",
                showlegend=True if i == 0 else False  # Une seule légende affichée
            ),
            row=r + 1,
            col=c + 1
        )

    # Mise en page globale
    fig.update_layout(
        title_text="Pourcentage de valeurs manquantes par variable",
        showlegend=True,
        legend=dict(
            orientation="h", yanchor="bottom", y=-0.15,
            xanchor="center", x=0.5
        ),
        height=350 * rows,
        margin=dict(l=50, r=50, b=100, t=80)
    )

    # Centrer les titres de sous-plots
    for ann in fig.layout.annotations:
        ann.update(x=ann.x)

    return fig


# def _get_missing_ranges(series: pd.Series, station_name: str, variable_name: str) -> list:
#     """
#     Détecte les plages de valeurs manquantes (NaN) dans une série temporelle.
#     Retourne une liste de dictionnaires, chacun représentant une plage manquante.
#     """
#     missing_ranges = []
#     if series.isnull().any():
#         # Trouver les débuts et fins des blocs de NaN
#         # Créer une série booléenne où True indique un NaN
#         is_nan = series.isnull()
#         # Décalage pour trouver les bords (True quand le statut NaN change)
#         start_nan = is_nan & (~is_nan.shift(1, fill_value=False))
#         end_nan = is_nan & (~is_nan.shift(-1, fill_value=False))

#         start_times = series.index[start_nan].tolist()
#         end_times = series.index[end_nan].tolist()

#         # S'assurer que chaque début a une fin correspondante
#         # Si la série se termine par des NaN, la dernière end_time sera la dernière date de l'index
#         if len(start_times) > len(end_times):
#             end_times.append(series.index[-1])
#         elif len(end_times) > len(start_times): # Cas où la série commence par des NaN
#              start_times.insert(0, series.index[0]) # Ajoute le premier index comme début

#         for start, end in zip(start_times, end_times):
#             duration = (end - start).total_seconds() / 3600
#             missing_ranges.append({
#                 'station': station_name,
#                 'variable': variable_name,
#                 'start_time': start,
#                 'end_time': end,
#                 'duration_hours': duration
#             })
#     return missing_ranges


# def interpolation(df: pd.DataFrame, limits: dict, df_gps: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
#     """
#     Effectue le nettoyage, l'application des limites et l'interpolation des données météorologiques.
#     Cette fonction retourne quatre DataFrames :
#     1. Le DataFrame après nettoyage et application des limites (avec NaNs pour les valeurs manquantes/outliers).
#     2. Le DataFrame entièrement interpolé (sans NaNs).
#     3. Un DataFrame récapitulant les plages de valeurs manquantes pour chaque variable AVANT interpolation.
#     4. Un DataFrame récapitulant les plages de valeurs manquantes pour chaque variable APRÈS interpolation.

#     Args:
#         df (pd.DataFrame): Le DataFrame d'entrée avec DatetimeIndex et colonne 'Station'.
#         limits (dict): Dictionnaire définissant les limites de valeurs pour chaque variable.
#         df_gps (pd.DataFrame): Le DataFrame contenant les informations de station
#                                (colonnes 'Station', 'Lat', 'Long', 'Timezone').

#     Returns:
#         tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
#             - Le premier DataFrame contient les données après nettoyage et mise en NaN des outliers, mais AVANT interpolation.
#             - Le deuxième DataFrame contient les données entièrement interpolées.
#             - Le troisième est un DataFrame (cols: 'station', 'variable', 'start_time', 'end_time', 'duration_hours') AVANT interpolation.
#             - Le quatrième est un DataFrame (cols: 'station', 'variable', 'start_time', 'end_time', 'duration_hours') APRÈS interpolation.
#     """
#     df_temp_processed = df.copy()

#     # --- Vérifications initiales et nettoyage de l'index ---
#     if not isinstance(df_temp_processed.index, pd.DatetimeIndex):
#         raise TypeError(str(_l("Le DataFrame d'entrée pour l'interpolation DOIT avoir un DatetimeIndex.")))
    
#     initial_rows = len(df_temp_processed)
#     df_temp_processed = df_temp_processed[df_temp_processed.index.notna()]
#     if len(df_temp_processed) == 0:
#         warnings.warn(str(_l("Après nettoyage des index temporels manquants, le DataFrame est vide. Retourne des DataFrames vides.")))
#         return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours']), pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours'])
#     if initial_rows - len(df_temp_processed) > 0:
#         warnings.warn(str(_l("Suppression de %d lignes avec index Datetime manquant ou invalide.") % (initial_rows - len(df_temp_processed))))
    
#     # Assurer que l'index est en UTC au début du traitement pour la cohérence
#     if df_temp_processed.index.tz is None:
#         df_temp_processed.index = df_temp_processed.index.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
#     elif df_temp_processed.index.tz != pytz.utc:
#         df_temp_processed.index = df_temp_processed.index.tz_convert('UTC')
    
#     # Correction pour gérer les duplicatas d'index et de station de manière robuste
#     if 'Station' not in df_temp_processed.columns:
#         raise ValueError(str(_l("La colonne 'Station' est manquante dans le DataFrame d'entrée. Elle est requise.")))

#     # S'assurer que le nom de l'index est défini avant de l'utiliser dans subset
#     if df_temp_processed.index.name is None:
#         df_temp_processed.index.name = 'Datetime'

#     initial_df_len = len(df_temp_processed)
#     df_temp_processed_reset_init = df_temp_processed.reset_index()
#     df_temp_processed_reset_init.drop_duplicates(subset=['Station', 'Datetime'], keep='first', inplace=True)
#     df_temp_processed = df_temp_processed_reset_init.set_index('Datetime')

#     if len(df_temp_processed) < initial_df_len:
#         warnings.warn(str(_l("Suppression de %d lignes dupliquées (même Datetime et Station).") % (initial_df_len - len(df_temp_processed))))

#     df_temp_processed = df_temp_processed.sort_index()


#     # Gestion de df_gps
#     required_gps_cols = ['Station', 'Lat', 'Long', 'Timezone']
#     if not all(col in df_gps.columns for col in required_gps_cols):
#         raise ValueError(
#             str(_l("df_gps doit contenir les colonnes %s. Colonnes actuelles dans df_gps : %s") % \
#             (required_gps_cols, df_gps.columns.tolist()))
#         )

#     df_gps_unique = df_gps.drop_duplicates(subset=['Station'], keep='first').copy()
#     if len(df_gps) > len(df_gps_unique):
#         warnings.warn(str(_l("Suppression de %d doublons dans df_gps (en gardant la première occurrence).") % (len(df_gps) - len(df_gps_unique))))
    
#     numerical_cols = ['Air_Temp_Deg_C', 'Rel_H_%', 'BP_mbar_Avg',
#                       'Rain_01_mm', 'Rain_02_mm', 'Rain_mm', 'Wind_Sp_m/sec',
#                       'Solar_R_W/m^2', 'Wind_Dir_Deg']
    
#     # --- PHASE 1: Nettoyage et application des limites (AVANT INTERPOLATION) ---
#     for col in numerical_cols:
#         if col in df_temp_processed.columns:
#             df_temp_processed[col] = pd.to_numeric(df_temp_processed[col], errors='coerce')
            
#             if col in limits:
#                 min_val = limits[col].get('min')
#                 max_val = limits[col].get('max')
                
#                 if min_val is not None:
#                     df_temp_processed.loc[df_temp_processed[col] < min_val, col] = np.nan
#                 if max_val is not None:
#                     df_temp_processed.loc[df_temp_processed[col] > max_val, col] = np.nan
    
#     # Pré-calcul de Rain_mm si nécessaire, sur le DataFrame complet
#     if 'Rain_mm' not in df_temp_processed.columns or df_temp_processed['Rain_mm'].isnull().all():
#         # Assurez-vous que cette fonction create_rain_mm est bien définie ou importée
#         df_temp_processed = create_rain_mm(df_temp_processed)
#         warnings.warn(str(_l("Colonne Rain_mm créée à partir des deux capteurs (pré-interpolation).")))

#     # Initialiser les colonnes de lever/coucher du soleil et de jour/nuit
#     df_temp_processed['Is_Daylight'] = False
#     df_temp_processed['Daylight_Duration'] = pd.NA
#     df_temp_processed['sunrise_time_utc'] = pd.NaT 
#     df_temp_processed['sunset_time_utc'] = pd.NaT 

#     # Optimisation du calcul Astral par station
#     temp_df_for_astral = df_temp_processed.reset_index()
#     # 'Date_UTC_Naive' sera un Timestamp (datetime.datetime) naive à minuit pour chaque jour en UTC.
#     temp_df_for_astral['Date_UTC_Naive'] = temp_df_for_astral['Datetime'].dt.normalize().dt.tz_localize(None) 
    
#     gps_info_df_for_merge = df_gps_unique[['Station', 'Lat', 'Long', 'Timezone']].set_index('Station')
    
#     temp_df_for_astral = pd.merge(
#         temp_df_for_astral,
#         gps_info_df_for_merge,
#         on='Station',
#         how='left'
#     )

#     unique_astral_inputs = temp_df_for_astral[['Station', 'Date_UTC_Naive', 'Lat', 'Long', 'Timezone']].drop_duplicates()
    
#     astral_results = []
#     for _, row in unique_astral_inputs.iterrows():
#         station_name = row['Station']
#         # date_utc_naive_ts est un Timestamp pandas, qui est un type de datetime.datetime.
#         # Il est déjà "naive" car nous avons fait .tz_localize(None) précédemment.
#         date_utc_naive_ts = row['Date_UTC_Naive']
#         lat = row['Lat']
#         long = row['Long']
#         timezone_str = row['Timezone']

#         if pd.isna(lat) or pd.isna(long) or pd.isna(timezone_str):
#             warnings.warn(str(_l("Coordonnées ou Fuseau horaire manquants/invalides pour le site '%s' à la date %s. Indicateur jour/nuit fixe sera utilisé.") % (station_name, date_utc_naive_ts.date())))
#             astral_results.append({
#                 'Station': station_name,
#                 'Date_UTC_Naive': date_utc_naive_ts,
#                 'sunrise_time_utc_calc': pd.NaT,
#                 'sunset_time_utc_calc': pd.NaT,
#                 'Daylight_Duration_h_calc': np.nan,
#                 'fixed_daylight_applied': True
#             })
#             continue

#         try:
#             loc = LocationInfo(station_name, "Site", timezone_str, lat, long)
            
#             # === CORRECTION MAJEURE ICI : Ne pas appeler .date() sur le datetime.datetime avant localize ===
#             # Convertir le Timestamp (qui est un datetime.datetime) en un objet datetime.datetime standard python,
#             # puis le localiser avec le fuseau horaire local.
#             date_local_aware = pytz.timezone(timezone_str).localize(date_utc_naive_ts.to_pydatetime(), is_dst=None)

#             # sun.sun peut prendre un datetime.datetime ou datetime.date.
#             s = sun.sun(loc.observer, date=date_local_aware)
            
#             sunrise_utc = s['sunrise'].astimezone(pytz.utc) if s['sunrise'] and pd.notna(s['sunrise']) else pd.NaT
#             sunset_utc = s['sunset'].astimezone(pytz.utc) if s['sunset'] and pd.notna(s['sunset']) else pd.NaT
            
#             daylight_duration_hours = (sunset_utc - sunrise_utc).total_seconds() / 3600 if pd.notna(sunrise_utc) and pd.notna(sunset_utc) and sunset_utc > sunrise_utc else np.nan

#             astral_results.append({
#                 'Station': station_name,
#                 'Date_UTC_Naive': date_utc_naive_ts,
#                 'sunrise_time_utc_calc': sunrise_utc,
#                 'sunset_time_utc_calc': sunset_utc,
#                 'Daylight_Duration_h_calc': daylight_duration_hours,
#                 'fixed_daylight_applied': False
#             })
            
#         except Exception as e:
#             # Pour le message d'avertissement, nous pouvons toujours afficher la date seule.
#             warnings.warn(str(_l("Erreur lors du calcul du lever/coucher du soleil avec Astral pour %s à la date %s: %s. Utilisation de l'indicateur jour/nuit fixe.") % (station_name, date_utc_naive_ts.date(), e)))
#             traceback.print_exc()
#             astral_results.append({
#                 'Station': station_name,
#                 'Date_UTC_Naive': date_utc_naive_ts,
#                 'sunrise_time_utc_calc': pd.NaT,
#                 'sunset_time_utc_calc': pd.NaT,
#                 'Daylight_Duration_h_calc': np.nan,
#                 'fixed_daylight_applied': True
#             })
    
#     astral_df = pd.DataFrame(astral_results)
    
#     df_temp_processed_for_merge = df_temp_processed.reset_index()
#     df_temp_processed_for_merge['Date_UTC_Naive'] = df_temp_processed_for_merge['Datetime'].dt.normalize().dt.tz_localize(None)

#     df_temp_processed = pd.merge(
#         df_temp_processed_for_merge,
#         astral_df,
#         on=['Station', 'Date_UTC_Naive'],
#         how='left'
#     ).set_index('Datetime')

#     df_temp_processed.loc[:, 'sunrise_time_utc'] = df_temp_processed['sunrise_time_utc_calc']
#     df_temp_processed.loc[:, 'sunset_time_utc'] = df_temp_processed['sunset_time_utc_calc']
    
#     valid_sunrise_sunset = df_temp_processed['sunrise_time_utc'].notna() & df_temp_processed['sunset_time_utc'].notna()
#     df_temp_processed.loc[valid_sunrise_sunset, 'Is_Daylight'] = (
#         df_temp_processed.loc[valid_sunrise_sunset].index >= df_temp_processed.loc[valid_sunrise_sunset, 'sunrise_time_utc']
#     ) & (
#         df_temp_processed.loc[valid_sunrise_sunset].index < df_temp_processed.loc[valid_sunrise_sunset, 'sunset_time_utc']
#     )

#     fixed_daylight_mask = df_temp_processed['fixed_daylight_applied']
#     df_temp_processed.loc[fixed_daylight_mask, 'Is_Daylight'] = (df_temp_processed.loc[fixed_daylight_mask].index.hour >= 7) & \
#                                                                  (df_temp_processed.loc[fixed_daylight_mask].index.hour <= 18)
#     df_temp_processed.loc[fixed_daylight_mask, 'Daylight_Duration'] = "11:00:00"
#     df_temp_processed.loc[fixed_daylight_mask, 'sunrise_time_utc'] = pd.NaT 
#     df_temp_processed.loc[fixed_daylight_mask, 'sunset_time_utc'] = pd.NaT 

#     calculated_daylight_mask = ~fixed_daylight_mask & df_temp_processed['Daylight_Duration_h_calc'].notna()
#     def format_duration_h(hours):
#         if pd.isna(hours):
#             return pd.NA
#         total_seconds = int(hours * 3600)
#         h = total_seconds // 3600
#         m = (total_seconds % 3600) // 60
#         s = total_seconds % 60
#         return f"{h:02d}:{m:02d}:{s:02d}"

#     df_temp_processed.loc[calculated_daylight_mask, 'Daylight_Duration'] = \
#         df_temp_processed.loc[calculated_daylight_mask, 'Daylight_Duration_h_calc'].apply(format_duration_h)

#     warnings.warn(str(_l("Calcul des indicateurs jour/nuit et durée du jour terminé pour toutes les stations.")))

#     if 'Solar_R_W/m^2' in df_temp_processed.columns and 'Is_Daylight' in df_temp_processed.columns:
#         df_temp_processed.loc[~df_temp_processed['Is_Daylight'], 'Solar_R_W/m^2'] = 0
#         warnings.warn(str(_l("Toutes les valeurs de Solar_R_W/m^2 en dehors des heures de jour ont été mises à zéro dans le DataFrame pré-interpolation.")))

#         has_rain_mm = 'Rain_mm' in df_temp_processed.columns
#         cond_suspect_zeros = (df_temp_processed['Is_Daylight']) & \
#                              (df_temp_processed['Solar_R_W/m^2'] == 0)
#         if has_rain_mm:
#             cond_suspect_zeros = cond_suspect_zeros & (df_temp_processed['Rain_mm'] == 0)
#         else:
#             warnings.warn(str(_l("Rain_mm manquant. Tous les 0 de radiation solaire pendant le jour sont traités comme suspects pour l'étape pré-interpolation.")))

#         df_temp_processed.loc[cond_suspect_zeros, 'Solar_R_W/m^2'] = np.nan
#         warnings.warn(str(_l("Zéros suspects de radiation solaire pendant le jour mis à NaN dans le DataFrame pré-interpolation.")))

#     df_before_interpolation = df_temp_processed.copy()

#     cols_to_drop_after_process = ['Date_UTC_Naive', 'Lat', 'Long', 'Timezone', 
#                                   'sunrise_time_utc_calc', 'sunset_time_utc_calc', 
#                                   'Daylight_Duration_h_calc', 'fixed_daylight_applied']
#     df_before_interpolation = df_before_interpolation.drop(columns=cols_to_drop_after_process, errors='ignore')


#     # DÉTERMINER LES PLAGES MANQUANTES AVANT INTERPOLATION
#     all_missing_ranges_before_interp_list = [] 
#     numerical_cols_to_check = [col for col in numerical_cols if col in df_before_interpolation.columns and col != 'Station']
    
#     for station_name, group in df_before_interpolation.groupby('Station'):
#         for var in numerical_cols_to_check:
#             # Assurez-vous que cette fonction _get_missing_ranges est bien définie ou importée
#             all_missing_ranges_before_interp_list.extend(_get_missing_ranges(group[var], station_name, var))

#     df_missing_ranges_before_interp = pd.DataFrame(all_missing_ranges_before_interp_list)
#     if not df_missing_ranges_before_interp.empty:
#         df_missing_ranges_before_interp = df_missing_ranges_before_interp.sort_values(by=['station', 'variable', 'start_time'])
#     else:
#         df_missing_ranges_before_interp = pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours'])


#     # --- PHASE 2: Interpolation des valeurs (création de df_after_interpolation) ---
#     df_after_interpolation = df_before_interpolation.copy()

#     cols_to_interpolate_standard = [col for col in numerical_cols_to_check if col != 'Solar_R_W/m^2']

#     for station_name, group in df_after_interpolation.groupby('Station'):
#         group_copy_for_interp = group.copy() 
#         for var in cols_to_interpolate_standard:
#             if var in group_copy_for_interp.columns:
#                 if isinstance(group_copy_for_interp.index, pd.DatetimeIndex):
#                     group_copy_for_interp.loc[:, var] = group_copy_for_interp[var].interpolate(method='time', limit_direction='both')
#                 else:
#                     group_copy_for_interp.loc[:, var] = group_copy_for_interp[var].interpolate(method='linear', limit_direction='both')
#                 group_copy_for_interp.loc[:, var] = group_copy_for_interp[var].bfill().ffill()
        
#         if 'Solar_R_W/m^2' in group_copy_for_interp.columns and 'Is_Daylight' in group_copy_for_interp.columns:
#             is_day = group_copy_for_interp['Is_Daylight']
            
#             if isinstance(group_copy_for_interp.index, pd.DatetimeIndex):
#                 group_copy_for_interp.loc[is_day, 'Solar_R_W/m^2'] = group_copy_for_interp.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='time', limit_direction='both')
#             else:
#                 group_copy_for_interp.loc[is_day, 'Solar_R_W/m^2'] = group_copy_for_interp.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='linear', limit_direction='both')
            
#             group_copy_for_interp.loc[is_day, 'Solar_R_W/m^2'] = group_copy_for_interp.loc[is_day, 'Solar_R_W/m^2'].bfill().ffill()

#             group_copy_for_interp.loc[~is_day, 'Solar_R_W/m^2'] = 0
            
#         df_after_interpolation.loc[group_copy_for_interp.index, group_copy_for_interp.columns] = group_copy_for_interp 


#     # DÉTERMINER LES PLAGES MANQUANTES APRÈS INTERPOLATION
#     all_missing_ranges_after_interp_list = []
#     for station_name, group in df_after_interpolation.groupby('Station'):
#         for var in numerical_cols_to_check:
#             # Assurez-vous que cette fonction _get_missing_ranges est bien définie ou importée
#             all_missing_ranges_after_interp_list.extend(_get_missing_ranges(group[var], station_name, var))
        
#     df_missing_ranges_after_interp = pd.DataFrame(all_missing_ranges_after_interp_list)
#     if not df_missing_ranges_after_interp.empty:
#         df_missing_ranges_after_interp = df_missing_ranges_after_interp.sort_values(by=['station', 'variable', 'start_time'])
#     else:
#         df_missing_ranges_after_interp = pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours'])
        
#     ### Exclusion des colonnes spécifiées
#     #To ensure the output dataframes only contain relevant meteorological data, the following columns will be removed: **'Year', 'Month', 'Minute', 'Hour', 'Date'**. These columns are often derived directly from the `DatetimeIndex` and are redundant in the final datasets.

#     # Define columns to exclude
#     cols_to_exclude = ['Year', 'Month', 'Minute', 'Hour', 'Date', 'Day']

#     # Drop columns from df_before_interpolation if they exist
#     existing_cols_before = [col for col in cols_to_exclude if col in df_before_interpolation.columns]
#     if existing_cols_before:
#         df_before_interpolation = df_before_interpolation.drop(columns=existing_cols_before)
#         warnings.warn(str(_l("Colonnes %s exclues de df_before_interpolation.") % existing_cols_before))

#     # Drop columns from df_after_interpolation if they exist
#     existing_cols_after = [col for col in cols_to_exclude if col in df_after_interpolation.columns]
#     if existing_cols_after:
#         df_after_interpolation = df_after_interpolation.drop(columns=existing_cols_after)
#         warnings.warn(str(_l("Colonnes %s exclues de df_after_interpolation.") % existing_cols_after))
   
#     return df_before_interpolation, df_after_interpolation, df_missing_ranges_before_interp, df_missing_ranges_after_interp



import pandas as pd
import numpy as np
import pytz # Ensure pytz is imported
from astral import LocationInfo, sun # Ensure astral is imported
from flask_babel import _, lazy_gettext as _l # Assuming these imports are correct
import traceback # For printing full tracebacks on errors

def _get_missing_ranges(series: pd.Series, station_name: str, variable_name: str) -> list:
    """
    Detects ranges of missing values (NaN) in a time series.
    Returns a list of dictionaries, each representing a missing range.
    """
    missing_ranges = []
    # Only proceed if there are any NaNs in the series
    if series.isnull().any():
        is_nan = series.isnull()
        
        # Identify blocks of NaNs.
        # A start of NaN block is when current is NaN and previous was not (or it's the very first element and is NaN).
        start_nan_mask = is_nan & (~is_nan.shift(1, fill_value=False))
        # An end of NaN block is when current is NaN and next is not (or it's the very last element and is NaN).
        end_nan_mask = is_nan & (~is_nan.shift(-1, fill_value=False))

        start_times = series.index[start_nan_mask].tolist()
        end_times = series.index[end_nan_mask].tolist()

        # Edge case: If data starts with NaN, start_nan_mask will correctly pick the first index.
        # Edge case: If data ends with NaN, end_nan_mask will correctly pick the last index.
        # The lengths should always match due to this robust mask creation.
        
        # We zip them directly. If there's any mismatch due to extreme edge cases not covered
        # by shift masks, it will be handled gracefully by zip stopping at the shortest list.
        # However, the masks as defined should make lengths equal.
        for start, end in zip(start_times, end_times):
            duration = (end - start).total_seconds() / 3600
            missing_ranges.append({
                'station': station_name,
                'variable': variable_name,
                'start_time': start,
                'end_time': end,
                'duration_hours': duration
            })
    return missing_ranges


import pandas as pd
import numpy as np
import pytz
from astral import LocationInfo, sun
import traceback
import warnings
from flask_babel import _, lazy_gettext as _l

# --- Helper Functions for Modularity and Clarity ---

def _validate_and_clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Performs initial validation and cleaning of the input DataFrame."""
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError(str(_l("Le DataFrame d'entrée pour l'interpolation DOIT avoir un DatetimeIndex.")))

    initial_rows = len(df)
    df_cleaned = df[df.index.notna()].copy() # Use .copy() to avoid SettingWithCopyWarning
    
    if len(df_cleaned) == 0:
        warnings.warn(str(_l("Après nettoyage des index temporels manquants, le DataFrame est vide.")))
        return pd.DataFrame() # Return empty DataFrame to be handled by caller
    
    if initial_rows - len(df_cleaned) > 0:
        warnings.warn(str(_l("Suppression de %d lignes avec index Datetime manquant ou invalide.") % (initial_rows - len(df_cleaned))))

    # Ensure UTC timezone
    if df_cleaned.index.tz is None:
        df_cleaned.index = df_cleaned.index.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
    elif df_cleaned.index.tz != pytz.utc:
        df_cleaned.index = df_cleaned.index.tz_convert('UTC')
    
    # Handle duplicate index + station
    if 'Station' not in df_cleaned.columns:
        raise ValueError(str(_l("La colonne 'Station' est manquante dans le DataFrame d'entrée. Elle est requise.")))
    
    # Make sure index name is set for drop_duplicates if needed
    if df_cleaned.index.name is None:
        df_cleaned.index.name = 'Datetime'

    initial_df_len = len(df_cleaned)
    # Using reset_index().drop_duplicates().set_index() is robust for duplicates on index + column
    df_cleaned_reset = df_cleaned.reset_index()
    df_cleaned_reset.drop_duplicates(subset=['Station', 'Datetime'], keep='first', inplace=True)
    df_cleaned = df_cleaned_reset.set_index('Datetime').sort_index()

    if len(df_cleaned) < initial_df_len:
        warnings.warn(str(_l("Suppression de %d lignes dupliquées (même Datetime et Station).") % (initial_df_len - len(df_cleaned))))
    
    return df_cleaned

def _apply_limits_and_coercions(df: pd.DataFrame, limits: dict, numerical_cols: list) -> pd.DataFrame:
    """Applies numerical limits and coerces types."""
    df_processed = df.copy() # Work on a copy

    for col in numerical_cols:
        if col in df_processed.columns:
            # Coerce to numeric first, turning errors into NaN
            df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')
            
            if col in limits:
                min_val = limits[col].get('min')
                max_val = limits[col].get('max')
                
                if min_val is not None:
                    df_processed.loc[df_processed[col] < min_val, col] = np.nan
                if max_val is not None:
                    df_processed.loc[df_processed[col] > max_val, col] = np.nan
    return df_processed

def _calculate_astral_data(df: pd.DataFrame, df_gps: pd.DataFrame) -> pd.DataFrame:
    """Calculates sunrise, sunset, and daylight duration using Astral."""
    # Validate df_gps
    required_gps_cols = ['Station', 'Lat', 'Long', 'Timezone']
    if not all(col in df_gps.columns for col in required_gps_cols):
        raise ValueError(
            str(_l("df_gps doit contenir les colonnes %s. Colonnes actuelles dans df_gps : %s") % \
            (required_gps_cols, df_gps.columns.tolist()))
        )
    
    df_gps_unique = df_gps.drop_duplicates(subset=['Station'], keep='first').copy()
    if len(df_gps) > len(df_gps_unique):
        warnings.warn(str(_l("Suppression de %d doublons dans df_gps (en gardant la première occurrence).") % (len(df_gps) - len(df_gps_unique))))
    
    # Prepare DataFrame for merge with GPS data
    df_temp_reset = df.reset_index()
    # Normalize Datetime to get naive UTC date for daily calculations
    df_temp_reset['Date_UTC_Naive'] = df_temp_reset['Datetime'].dt.normalize().dt.tz_localize(None) 
    
    # Merge with GPS info to get Lat, Long, Timezone for each station-date combination
    # Use only necessary columns from df_gps_unique for merge to avoid bringing too much data
    gps_info_for_merge = df_gps_unique[['Station', 'Lat', 'Long', 'Timezone']]
    df_merged_with_gps = pd.merge(
        df_temp_reset[['Station', 'Date_UTC_Naive']].drop_duplicates(), # Only unique station-dates needed for astral calculation
        gps_info_for_merge,
        on='Station',
        how='left'
    )
    
    astral_results = []
    # Iterate over unique station-date combinations to calculate astral data efficiently
    for _, row in df_merged_with_gps.iterrows():
        station_name = row['Station']
        date_utc_naive_ts = row['Date_UTC_Naive']
        lat = row['Lat']
        long = row['Long']
        timezone_str = row['Timezone']

        if pd.isna(lat) or pd.isna(long) or pd.isna(timezone_str):
            warnings.warn(str(_l("Coordonnées ou Fuseau horaire manquants/invalides pour le site '%s' à la date %s. Indicateur jour/nuit fixe sera utilisé.") % (station_name, date_utc_naive_ts.date())))
            astral_results.append({
                'Station': station_name,
                'Date_UTC_Naive': date_utc_naive_ts,
                'sunrise_time_utc_calc': pd.NaT,
                'sunset_time_utc_calc': pd.NaT,
                'Daylight_Duration_h_calc': np.nan,
                'fixed_daylight_applied': True
            })
            continue
        try:
            loc = LocationInfo(station_name, "Site", timezone_str, lat, long)
            # Convert pandas Timestamp to Python datetime object for astral
            date_local_aware = pytz.timezone(timezone_str).localize(date_utc_naive_ts.to_pydatetime(), is_dst=None)
            s = sun.sun(loc.observer, date=date_local_aware)
            
            sunrise_utc = s['sunrise'].astimezone(pytz.utc) if s['sunrise'] and pd.notna(s['sunrise']) else pd.NaT
            sunset_utc = s['sunset'].astimezone(pytz.utc) if s['sunset'] and pd.notna(s['sunset']) else pd.NaT
            
            daylight_duration_hours = (sunset_utc - sunrise_utc).total_seconds() / 3600 if pd.notna(sunrise_utc) and pd.notna(sunset_utc) and sunset_utc > sunrise_utc else np.nan
            
            astral_results.append({
                'Station': station_name,
                'Date_UTC_Naive': date_utc_naive_ts,
                'sunrise_time_utc_calc': sunrise_utc,
                'sunset_time_utc_calc': sunset_utc,
                'Daylight_Duration_h_calc': daylight_duration_hours,
                'fixed_daylight_applied': False
            })
            
        except Exception as e:
            warnings.warn(str(_l("Erreur lors du calcul du lever/coucher du soleil avec Astral pour %s à la date %s: %s. Utilisation de l'indicateur jour/nuit fixe.") % (station_name, date_utc_naive_ts.date(), e)))
            # traceback.print_exc() # Keep this for detailed debugging if needed
            astral_results.append({
                'Station': station_name,
                'Date_UTC_Naive': date_utc_naive_ts,
                'sunrise_time_utc_calc': pd.NaT,
                'sunset_time_utc_calc': pd.NaT,
                'Daylight_Duration_h_calc': np.nan,
                'fixed_daylight_applied': True
            })
    
    astral_df = pd.DataFrame(astral_results)
    return astral_df

def _integrate_astral_data(df: pd.DataFrame, astral_df: pd.DataFrame) -> pd.DataFrame:
    """Merges and applies calculated astral data to the main DataFrame."""
    df_with_astral = df.copy().reset_index()
    df_with_astral['Date_UTC_Naive'] = df_with_astral['Datetime'].dt.normalize().dt.tz_localize(None)

    df_with_astral = pd.merge(
        df_with_astral,
        astral_df,
        on=['Station', 'Date_UTC_Naive'],
        how='left'
    ).set_index('Datetime')

    # Apply sunrise/sunset and Is_Daylight flag
    df_with_astral['sunrise_time_utc'] = df_with_astral['sunrise_time_utc_calc']
    df_with_astral['sunset_time_utc'] = df_with_astral['sunset_time_utc_calc']
    df_with_astral['Is_Daylight'] = False # Default to False

    valid_sunrise_sunset = df_with_astral['sunrise_time_utc'].notna() & df_with_astral['sunset_time_utc'].notna()
    
    # Vectorized calculation for Is_Daylight
    df_with_astral.loc[valid_sunrise_sunset, 'Is_Daylight'] = (
        df_with_astral.loc[valid_sunrise_sunset].index >= df_with_astral.loc[valid_sunrise_sunset, 'sunrise_time_utc']
    ) & (
        df_with_astral.loc[valid_sunrise_sunset].index < df_with_astral.loc[valid_sunrise_sunset, 'sunset_time_utc']
    )

    # Apply fixed daylight for cases where astral calculation failed
    fixed_daylight_mask = df_with_astral['fixed_daylight_applied'].fillna(False) # Handle NaNs if merge introduced them
    df_with_astral.loc[fixed_daylight_mask, 'Is_Daylight'] = (df_with_astral.loc[fixed_daylight_mask].index.hour >= 7) & \
                                                                 (df_with_astral.loc[fixed_daylight_mask].index.hour <= 18)
    df_with_astral.loc[fixed_daylight_mask, 'Daylight_Duration'] = "11:00:00"
    df_with_astral.loc[fixed_daylight_mask, 'sunrise_time_utc'] = pd.NaT # Reset if fixed
    df_with_astral.loc[fixed_daylight_mask, 'sunset_time_utc'] = pd.NaT # Reset if fixed

    # Format Daylight_Duration from hours to HH:MM:SS
    calculated_daylight_mask = ~fixed_daylight_mask & df_with_astral['Daylight_Duration_h_calc'].notna()
    
    def _format_duration_h(hours):
        if pd.isna(hours):
            return pd.NA
        total_seconds = int(hours * 3600)
        h = total_seconds // 3600
        m = (total_seconds % 3600) // 60
        s = total_seconds % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    df_with_astral.loc[calculated_daylight_mask, 'Daylight_Duration'] = \
        df_with_astral.loc[calculated_daylight_mask, 'Daylight_Duration_h_calc'].apply(_format_duration_h)
    
    warnings.warn(str(_l("Calcul des indicateurs jour/nuit et durée du jour terminé pour toutes les stations.")))

    return df_with_astral

def _process_solar_radiation(df: pd.DataFrame) -> pd.DataFrame:
    """Applies specific rules for Solar Radiation."""
    df_solar = df.copy()
    
    if 'Solar_R_W/m^2' in df_solar.columns and 'Is_Daylight' in df_solar.columns:
        # Set solar radiation to 0 when it's not daylight
        df_solar.loc[~df_solar['Is_Daylight'], 'Solar_R_W/m^2'] = 0
        warnings.warn(str(_l("Toutes les valeurs de Solar_R_W/m^2 en dehors des heures de jour ont été mises à zéro.")))
        
        # Identify suspicious zeros during daylight (potentially due to sensor malfunction)
        has_rain_mm = 'Rain_mm' in df_solar.columns
        cond_suspect_zeros = (df_solar['Is_Daylight']) & \
                             (df_solar['Solar_R_W/m^2'] == 0)
        if has_rain_mm:
            cond_suspect_zeros = cond_suspect_zeros & (df_solar['Rain_mm'] == 0)
        else:
            warnings.warn(str(_l("Rain_mm manquant. Tous les 0 de radiation solaire pendant le jour sont traités comme suspects.")))
        
        df_solar.loc[cond_suspect_zeros, 'Solar_R_W/m^2'] = np.nan
        warnings.warn(str(_l("Zéros suspects de radiation solaire pendant le jour mis à NaN.")))
    return df_solar

def _collect_missing_ranges_for_df(df: pd.DataFrame, numerical_cols_to_check: list) -> pd.DataFrame:
    """Collects missing ranges for a given DataFrame across stations and variables."""
    all_missing_ranges_list = [] 
    # Use a direct iteration over unique stations and then apply to_numeric if not already done.
    # The _apply_limits_and_coercions already does this for numerical_cols_to_check.
    
    # Efficiently get groups
    for station_name, group in df.groupby('Station'):
        for var in numerical_cols_to_check:
            if var in group.columns: # Ensure the variable exists in this group
                all_missing_ranges_list.extend(_get_missing_ranges(group[var], station_name, var))
                
    df_missing_ranges = pd.DataFrame(all_missing_ranges_list)
    if not df_missing_ranges.empty:
        df_missing_ranges = df_missing_ranges.sort_values(by=['station', 'variable', 'start_time'])
    else:
        df_missing_ranges = pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours'])
    return df_missing_ranges

def _interpolate_data_by_station(df: pd.DataFrame, numerical_cols_to_interpolate: list) -> pd.DataFrame:
    """Applies interpolation logic to data, grouped by station."""
    df_interpolated = df.copy() # Operate on a copy
    
    # Iterate through groups to apply interpolation
    for station_name, group in df_interpolated.groupby('Station'):
        # Ensure 'Station' column is preserved after operations if it's not the index
        if 'Station' in group.columns:
            group_copy_for_interp = group.drop(columns=['Station']).copy()
        else:
            group_copy_for_interp = group.copy()
            
        for var in numerical_cols_to_interpolate:
            if var in group_copy_for_interp.columns:
                if isinstance(group_copy_for_interp.index, pd.DatetimeIndex):
                    group_copy_for_interp.loc[:, var] = group_copy_for_interp[var].interpolate(method='time', limit_direction='both')
                else:
                    group_copy_for_interp.loc[:, var] = group_copy_for_interp[var].interpolate(method='linear', limit_direction='both')
                # Fill any remaining NaNs at edges (e.g., if first/last values were NaN)
                group_copy_for_interp.loc[:, var] = group_copy_for_interp[var].bfill().ffill()
        
        # Special handling for Solar Radiation during interpolation phase
        if 'Solar_R_W/m^2' in group_copy_for_interp.columns and 'Is_Daylight' in group_copy_for_interp.columns:
            is_day = group_copy_for_interp['Is_Daylight']
            
            # Interpolate only during daylight hours
            if isinstance(group_copy_for_interp.index, pd.DatetimeIndex):
                group_copy_for_interp.loc[is_day, 'Solar_R_W/m^2'] = group_copy_for_interp.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='time', limit_direction='both')
            else:
                group_copy_for_interp.loc[is_day, 'Solar_R_W/m^2'] = group_copy_for_interp.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='linear', limit_direction='both')
            
            group_copy_for_interp.loc[is_day, 'Solar_R_W/m^2'] = group_copy_for_interp.loc[is_day, 'Solar_R_W/m^2'].bfill().ffill()
            # Ensure non-daylight Solar_R_W/m^2 is 0 after interpolation
            group_copy_for_interp.loc[~is_day, 'Solar_R_W/m^2'] = 0
            
        # Update the main interpolated DataFrame
        # Use .loc with tuple (index slice, column slice) for efficient assignment
        df_interpolated.loc[group_copy_for_interp.index, group_copy_for_interp.columns] = group_copy_for_interp

    return df_interpolated

def _drop_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drops temporary or derived columns not needed in final output."""
    cols_to_drop = [
        'Date_UTC_Naive', 'Lat', 'Long', 'Timezone',
        'sunrise_time_utc_calc', 'sunset_time_utc_calc',
        'Daylight_Duration_h_calc', 'fixed_daylight_applied'
    ]
    
    # Also drop common date components if they were created and are redundant with DatetimeIndex
    redundant_datetime_components = ['Year', 'Month', 'Minute', 'Hour', 'Date', 'Day']
    cols_to_drop.extend(redundant_datetime_components)

    # Filter to only existing columns before dropping
    existing_cols_to_drop = [col for col in cols_to_drop if col in df.columns]
    if existing_cols_to_drop:
        df_cleaned = df.drop(columns=existing_cols_to_drop)
        warnings.warn(str(_l("Colonnes %s exclues du DataFrame final.") % existing_cols_to_drop))
    else:
        df_cleaned = df.copy() # Return a copy even if nothing was dropped
        
    return df_cleaned


# --- Main Interpolation Function ---

def interpolation(df: pd.DataFrame, limits: dict, df_gps: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Performs data cleaning, limit application, and interpolation of meteorological data.
    Returns four DataFrames:
    1. The DataFrame after cleaning and applying limits (with NaNs for missing/outliers).
    2. The fully interpolated DataFrame (without NaNs).
    3. A DataFrame summarizing missing value ranges for each variable BEFORE interpolation.
    4. A DataFrame summarizing missing value ranges for each variable AFTER interpolation.

    Args:
        df (pd.DataFrame): The input DataFrame with DatetimeIndex and 'Station' column.
        limits (dict): Dictionary defining value limits for each variable.
        df_gps (pd.DataFrame): DataFrame containing station information
                               ('Station', 'Lat', 'Long', 'Timezone' columns).

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
            - The first DataFrame contains data after cleaning and outlier setting to NaN, but BEFORE interpolation.
            - The second DataFrame contains fully interpolated data.
            - The third is a DataFrame summarizing missing ranges BEFORE interpolation.
            - The fourth is a DataFrame summarizing missing ranges AFTER interpolation.
    """
    
    # Define numerical columns for processing. This list should be comprehensive.
    numerical_cols = [
        'Air_Temp_Deg_C', 'Rel_H_%', 'BP_mbar_Avg',
        'Rain_01_mm', 'Rain_02_mm', 'Rain_mm', 'Wind_Sp_m/sec',
        'Solar_R_W/m^2', 'Wind_Dir_Deg'
    ]

    # --- Step 1: Initial Validation and Cleaning ---
    df_initial_clean = _validate_and_clean_dataframe(df)
    if df_initial_clean.empty:
        return (pd.DataFrame(), pd.DataFrame(), 
                pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours']), 
                pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours']))

    # --- Step 2: Apply Limits and Coerce Types ---
    df_limited = _apply_limits_and_coercions(df_initial_clean, limits, numerical_cols)

    # --- Step 3: Create Rain_mm if needed ---
    # Ensure create_rain_mm is imported or defined elsewhere
    if 'Rain_mm' not in df_limited.columns or df_limited['Rain_mm'].isnull().all():
        # You need to ensure create_rain_mm is available in this scope.
        # Assuming it takes the DataFrame and returns it with 'Rain_mm'.
        df_limited = create_rain_mm(df_limited) 
        warnings.warn(str(_l("Colonne Rain_mm créée à partir des deux capteurs (pré-interpolation).")))

    # --- Step 4: Calculate Astral Data (Sunrise/Sunset/Daylight) ---
    astral_df = _calculate_astral_data(df_limited, df_gps)
    df_with_astral = _integrate_astral_data(df_limited, astral_df)

    # --- Step 5: Process Solar Radiation (setting night values to 0, suspicious 0s to NaN) ---
    df_pre_interpolation_final = _process_solar_radiation(df_with_astral)
    
    # Make a copy for the 'before interpolation' output
    df_before_interpolation = df_pre_interpolation_final.copy()
    
    # --- Step 6: Collect Missing Ranges BEFORE Interpolation ---
    numerical_cols_to_check = [col for col in numerical_cols if col in df_before_interpolation.columns]
    df_missing_ranges_before_interp = _collect_missing_ranges_for_df(df_before_interpolation, numerical_cols_to_check)

    # --- Step 7: Perform Interpolation ---
    # Prepare columns for interpolation (excluding Rain_01_mm, Rain_02_mm if only Rain_mm is final)
    cols_to_interpolate = [col for col in numerical_cols_to_check if col != 'Solar_R_W/m^2'] # Solar is handled specially
    
    df_after_interpolation = _interpolate_data_by_station(df_before_interpolation, cols_to_interpolate)

    # --- Step 8: Collect Missing Ranges AFTER Interpolation ---
    df_missing_ranges_after_interp = _collect_missing_ranges_for_df(df_after_interpolation, numerical_cols_to_check)
    
    # --- Step 9: Final Column Dropping for Output DataFrames ---
    df_before_interpolation = _drop_derived_columns(df_before_interpolation)
    df_after_interpolation = _drop_derived_columns(df_after_interpolation)
   
    return df_before_interpolation, df_after_interpolation, df_missing_ranges_before_interp, df_missing_ranges_after_interp



# def gaps_time_series_viz(df_data: pd.DataFrame, df_gaps: pd.DataFrame, station_name: str, title_suffix: str = "") -> go.Figure:
#     """
#     Génère une figure Plotly montrant les séries temporelles de données numériques
#     avec des mises en évidence des périodes de valeurs manquantes (gaps),
#     où les détails des gaps sont affichés dans les tooltips.

#     Args:
#         df_data (pd.DataFrame): Le DataFrame de données (brutes ou interpolées) pour une station donnée.
#                                 Doit avoir un DatetimeIndex et une colonne 'Station'.
#         df_gaps (pd.DataFrame): Le DataFrame des plages manquantes (GLOBAL_MISSING_VALUES_BEFORE/AFTER_INTERPOLATION_DF)
#                                 filtré pour la station courante.
#                                 Doit contenir 'station', 'variable', 'start_time', 'end_time', et 'duration_hours'.
#         station_name (str): Le nom de la station actuellement visualisée.
#         title_suffix (str): Un suffixe à ajouter au titre du graphique (ex: "Avant Interpolation").

#     Returns:
#         go.Figure: La figure Plotly.
#     """
#     if df_data.empty:
#         fig = go.Figure().add_annotation(
#             x=0.5, y=0.5, text=str(_l("Aucune donnée disponible pour la station sélectionnée.")),
#             showarrow=False, font=dict(size=16)
#         )
#         fig.update_layout(title=str(f"{_l('Visualisation des données et des gaps')} - {station_name} {title_suffix}"))
#         return fig

#     numerical_cols_for_viz = [
#         col for col in df_data.columns
#         if pd.api.types.is_numeric_dtype(df_data[col]) and col not in [
#             'Station', 'Is_Daylight', 'Rain_01_mm', 'Rain_02_mm', 'Wind_Dir_Deg',
#             'sunrise_time_utc', 'sunset_time_utc', 'Daylight_Duration'
#         ]
#     ]
    
#     if not numerical_cols_for_viz:
#         fig = go.Figure().add_annotation(
#             x=0.5, y=0.5, text=str(_l("Aucune variable numérique exploitable pour la visualisation des gaps.")),
#             showarrow=False, font=dict(size=16)
#         )
#         fig.update_layout(title=str(f"{_l('Visualisation des données et des gaps')} - {station_name} {title_suffix}"))
#         return fig

#     num_variables = len(numerical_cols_for_viz)
#     cols_per_row = 1
#     rows = num_variables

#     fig = make_subplots(
#         rows=rows,
#         cols=cols_per_row,
#         shared_xaxes=True,
#         vertical_spacing=0.05,
#         subplot_titles=[str(f"{_l('Série temporelle')} - {col}") for col in numerical_cols_for_viz]
#     )

#     relevant_gaps = df_gaps[
#         (df_gaps['station'] == station_name) & 
#         (df_gaps['variable'].isin(numerical_cols_for_viz))
#     ].copy()

#     for i, col in enumerate(numerical_cols_for_viz):
#         row_idx = i + 1

#         fig.add_trace(
#             go.Scatter(
#                 x=df_data.index,
#                 y=df_data[col],
#                 mode='lines+markers',
#                 name=str(f"{col} - {_l('Données')}"),
#                 line=dict(color='blue', width=1),
#                 marker=dict(size=3),
#                 hovertemplate="<b>%{x|%Y-%m-%d %H:%M}</b><br>" +
#                                f"{col}: %{{y:.2f}}<extra></extra>",
#                 showlegend=False
#             ),
#             row=row_idx, col=1
#         )

#         gaps_for_col = relevant_gaps[relevant_gaps['variable'] == col]

#         if not gaps_for_col.empty:
#             for _, gap_row in gaps_for_col.iterrows():
#                 fig.add_shape(
#                     type="rect",
#                     xref="x", yref="paper",
#                     x0=gap_row['start_time'],
#                     y0=0, y1=1,
#                     x1=gap_row['end_time'],
#                     fillcolor="rgba(255,0,0,0.2)",
#                     line_width=0,
#                     layer="below",
#                     row=  , col=1
#                 )
                
#                 # Correction pour s'assurer que le texte de l'info-bulle est un string résolu
#                 hover_text = str(f"<b>{_l('Période Manquante')}</b><br>" +
#                                  f"{_l('Variable')}: {gap_row['variable']}<br>" +
#                                  f"{_l('Début')}: {gap_row['start_time'].strftime('%Y-%m-%d %H:%M')}<br>" +
#                                  f"{_l('Fin')}: {gap_row['end_time'].strftime('%Y-%m-%d %H:%M')}<br>" +
#                                  f"{_l('Durée')}: {gap_row['duration_hours']:.2f} {_l('heures')}")

#                 fig.add_trace(
#                     go.Scatter(
#                         x=[gap_row['start_time'], gap_row['end_time'], gap_row['end_time'], gap_row['start_time'], gap_row['start_time']],
#                         y=[df_data[col].min(), df_data[col].min(), df_data[col].max(), df_data[col].max(), df_data[col].min()],
#                         mode='lines',
#                         fill='toself',
#                         fillcolor='rgba(0,0,0,0)',
#                         line=dict(color='rgba(0,0,0,0)'),
#                         showlegend=False,
#                         hoverinfo='text',
#                         text=hover_text
#                     ),
#                     row=row_idx, col=1
#                 )

#     fig.update_layout(
#         title_text=str(f"{_l('Séries temporelles avec périodes manquantes/interpolées')} - {station_name} {title_suffix}"),
#         height=350 * rows,
#         hovermode="x unified",
#         margin=dict(l=50, r=50, b=100, t=80)
#     )

#     for i, col in enumerate(numerical_cols_for_viz):
#         fig.update_yaxes(title_text=col, row=i+1, col=1)
    
#     fig.update_xaxes(title_text=str(_l("Temps")), showgrid=True, gridwidth=1, gridcolor='LightGray')

#     return fig


import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
from flask_babel import _, lazy_gettext as _l # Assuming these imports are correct

def gaps_time_series_viz(df_data: pd.DataFrame, df_gaps: pd.DataFrame, station_name: str, title_suffix: str = "") -> go.Figure:
    """
    Generates a Plotly figure showing numerical time series data
    with highlighted periods of missing values (gaps),
    where gap details are displayed in tooltips.

    Args:
        df_data (pd.DataFrame): The data DataFrame (raw or interpolated) for a given station.
                                Must have a DatetimeIndex and a 'Station' column.
        df_gaps (pd.DataFrame): The DataFrame of missing ranges (e.g., GLOBAL_MISSING_VALUES_BEFORE/AFTER_INTERPOLATION_DF)
                                filtered for the current station.
                                Must contain 'station', 'variable', 'start_time', 'end_time', and 'duration_hours'.
        station_name (str): The name of the station currently being visualized.
        title_suffix (str): A suffix to add to the chart title (e.g., "Avant Interpolation").

    Returns:
        go.Figure: The Plotly figure.
    """
    # --- Initial Data Validation ---
    if df_data.empty:
        fig = go.Figure().add_annotation(
            x=0.5, y=0.5, text=str(_l("Aucune donnée disponible pour la station sélectionnée.")),
            showarrow=False, font=dict(size=16)
        )
        fig.update_layout(title=str(f"{_l('Visualisation des données et des gaps')} - {station_name} {title_suffix}"),
                          template="plotly_white")
        return fig

    # Ensure DatetimeIndex is present
    if not isinstance(df_data.index, pd.DatetimeIndex):
        warnings.warn(str(_l("Le DataFrame de données n'a pas un DatetimeIndex. Tentative de conversion.")))
        try:
            df_data.index = pd.to_datetime(df_data.index, errors='coerce')
            df_data = df_data[df_data.index.notna()]
        except Exception:
            fig = go.Figure().add_annotation(
                x=0.5, y=0.5, text=str(_l("L'index du DataFrame n'est pas un DatetimeIndex valide et ne peut pas être converti.")),
                showarrow=False, font=dict(size=16)
            )
            fig.update_layout(title=str(f"{_l('Visualisation des données et des gaps')} - {station_name} {title_suffix}"),
                              template="plotly_white")
            return fig
        if df_data.empty: # Check again after conversion and NaN removal
            fig = go.Figure().add_annotation(
                x=0.5, y=0.5, text=str(_l("Aucune donnée valide disponible après conversion de l'index en DatetimeIndex.")),
                showarrow=False, font=dict(size=16)
            )
            fig.update_layout(title=str(f"{_l('Visualisation des données et des gaps')} - {station_name} {title_suffix}"),
                              template="plotly_white")
            return fig


    # Define columns to exclude from visualization (non-numeric or flags)
    # Using a set for faster lookups
    EXCLUDE_COLS_VIZ = {
        'Station', 'Is_Daylight', 'Rain_01_mm', 'Rain_02_mm', 'Wind_Dir_Deg',
        'sunrise_time_utc', 'sunset_time_utc', 'Daylight_Duration',
        'Date_UTC_Naive', 'Lat', 'Long', 'Timezone',
        'sunrise_time_utc_calc', 'sunset_time_utc_calc',
        'Daylight_Duration_h_calc', 'fixed_daylight_applied',
        'Year', 'Month', 'Minute', 'Hour', 'Date', 'Day' # Also exclude derived date components
    }

    # Identify numerical columns for visualization
    numerical_cols_for_viz = [
        col for col in df_data.columns
        if pd.api.types.is_numeric_dtype(df_data[col]) and col not in EXCLUDE_COLS_VIZ
    ]
    
    if not numerical_cols_for_viz:
        fig = go.Figure().add_annotation(
            x=0.5, y=0.5, text=str(_l("Aucune variable numérique exploitable pour la visualisation des gaps.")),
            showarrow=False, font=dict(size=16)
        )
        fig.update_layout(title=str(f"{_l('Visualisation des données et des gaps')} - {station_name} {title_suffix}"),
                          template="plotly_white")
        return fig

    # --- Subplot Configuration ---
    num_variables = len(numerical_cols_for_viz)
    rows = num_variables # One row per variable
    cols_per_row = 1

    fig = make_subplots(
        rows=rows,
        cols=cols_per_row,
        shared_xaxes=True, # Share x-axis across all subplots
        vertical_spacing=0.04, # Slightly reduce spacing
        subplot_titles=[str(f"{_l('Série temporelle')} - {col}") for col in numerical_cols_for_viz]
    )

    # --- Filter Gaps Data Once ---
    # Filter df_gaps for the current station and relevant numerical columns only once
    relevant_gaps_filtered = df_gaps[
        (df_gaps['station'] == station_name) & 
        (df_gaps['variable'].isin(numerical_cols_for_viz))
    ].copy()
    
    # Ensure start_time and end_time are datetime objects in df_gaps for comparison
    relevant_gaps_filtered['start_time'] = pd.to_datetime(relevant_gaps_filtered['start_time'], errors='coerce')
    relevant_gaps_filtered['end_time'] = pd.to_datetime(relevant_gaps_filtered['end_time'], errors='coerce')
    relevant_gaps_filtered.dropna(subset=['start_time', 'end_time'], inplace=True) # Drop invalid gap entries

    # --- Add Traces and Gap Highlights ---
    for i, col in enumerate(numerical_cols_for_viz):
        row_idx = i + 1

        # Add data trace
        fig.add_trace(
            go.Scatter(
                x=df_data.index,
                y=df_data[col],
                mode='lines+markers',
                name=str(f"{col} - {_l('Données')}"),
                line=dict(color='blue', width=1),
                marker=dict(size=3, opacity=0.7),
                hovertemplate="<b>%{x|%Y-%m-%d %H:%M}</b><br>" +
                               f"{col}: %{{y:.2f}}<extra></extra>", # Format y value
                showlegend=False
            ),
            row=row_idx, col=1
        )

        # Get gaps specific to this column
        gaps_for_col = relevant_gaps_filtered[relevant_gaps_filtered['variable'] == col]

        if not gaps_for_col.empty:
            # Determine y-axis range for the current subplot to make shapes scale
            y_min = df_data[col].min()
            y_max = df_data[col].max()
            # Add a small buffer to y_min/y_max if data is constant or has very small range
            if pd.isna(y_min) or pd.isna(y_max) or y_max == y_min:
                y_min, y_max = 0, 1 # Fallback to a default range
            else:
                y_buffer = (y_max - y_min) * 0.05 # 5% buffer
                y_min -= y_buffer
                y_max += y_buffer

            # Add a single shape for each gap
            for _, gap_row in gaps_for_col.iterrows():
                fig.add_shape(
                    type="rect",
                    xref="x", yref="y", # Use 'y' coordinates, not 'paper'
                    x0=gap_row['start_time'],
                    y0=y_min, # Scale with actual y-axis data range
                    y1=y_max, # Scale with actual y-axis data range
                    x1=gap_row['end_time'],
                    fillcolor="rgba(255,0,0,0.2)", # Light red transparent fill
                    line_width=0,
                    layer="below", # Place below the data trace
                    row=row_idx, col=1
                )
                
                # --- Optimized Hover Info for Gaps ---
                # Add an invisible scatter trace over the gap area for hover text
                # We place a single marker in the middle of the gap for hover info.
                gap_mid_time = gap_row['start_time'] + (gap_row['end_time'] - gap_row['start_time']) / 2
                
                # Use a small non-NaN value for y to ensure it's visible, but it won't be plotted visually
                y_for_hover = df_data[col].mean() if df_data[col].notna().any() else 0 
                
                hover_text_gap = str(f"<b>{_l('Période Manquante')}</b><br>" +
                                     f"{_l('Variable')}: {gap_row['variable']}<br>" +
                                     f"{_l('Début')}: {gap_row['start_time'].strftime('%Y-%m-%d %H:%M')}<br>" +
                                     f"{_l('Fin')}: {gap_row['end_time'].strftime('%Y-%m-%d %H:%M')}<br>" +
                                     f"{_l('Durée')}: {gap_row['duration_hours']:.2f} {_l('heures')}")

                fig.add_trace(
                    go.Scatter(
                        x=[gap_mid_time],
                        y=[y_for_hover], # A single point for hover, visually hidden
                        mode='markers',
                        marker=dict(size=1, opacity=0), # Invisible marker
                        showlegend=False,
                        hoverinfo='text',
                        text=hover_text_gap,
                        name=f"{col} {_l('Gap Info')}" # Give it a name for internal tracking
                    ),
                    row=row_idx, col=1
                )

    # --- Update Layout ---
    fig.update_layout(
        title_text=str(f"{_l('Séries temporelles avec périodes manquantes/interpolées')} - {station_name} {title_suffix}"),
        height=max(400, 300 * rows), # Dynamic height with a minimum
        hovermode="x unified", # Excellent for time series analysis
        margin=dict(l=50, r=50, b=80, t=100), # Adjust margins
        template="plotly_white", # Clean template
    )

    # Update Y-axis titles
    for i, col in enumerate(numerical_cols_for_viz):
        fig.update_yaxes(title_text=col, row=i+1, col=1, rangemode='tozero') # rangemode='tozero' often good for time series

    # Update X-axis (shared)
    fig.update_xaxes(title_text=str(_l("Temps")), showgrid=True, gridwidth=1, gridcolor='LightGray')

    return fig


    ####################


# def _l(text):
#     return text

# # --- Helper Functions for Rainfall Specific Calculations (Remain yearly) ---

# def _calculate_rainy_season_stats_yearly(df_daily_rain_station: pd.DataFrame) -> pd.DataFrame:
#     RAIN_SEASON_GAP_THRESHOLD = pd.Timedelta(days=60)
#     NON_RELEVANT_RAIN_THRESHOLD_DAYS = 25
    
#     yearly_season_stats = []
    
#     df_daily_rain_station = df_daily_rain_station.set_index('Datetime').sort_index()

#     for year, year_df in df_daily_rain_station.groupby(df_daily_rain_station.index.year):
#         rain_events = year_df[year_df['Rain_mm'] > 0].index
        
#         if rain_events.empty or len(rain_events) < 2:
#             yearly_season_stats.append({
#                 'Year': year,
#                 'Moyenne Saison Pluvieuse': np.nan, 'Début Saison Pluvieuse': pd.NaT,
#                 'Fin Saison Pluvieuse': pd.NaT, 'Durée Saison Pluvieuse Jours': np.nan
#             })
#             continue

#         block_ids = (rain_events.to_series().diff() > RAIN_SEASON_GAP_THRESHOLD).cumsum()
#         valid_blocks = {}
#         for block_id, rain_dates_in_block in rain_events.to_series().groupby(block_ids):
#             if not rain_dates_in_block.empty:
#                 block_start = rain_dates_in_block.min()
#                 block_end = rain_dates_in_block.max()
#                 full_block_df = year_df.loc[block_start:block_end]
#                 if len(full_block_df[full_block_df['Rain_mm'] > 0]) > 1:
#                      valid_blocks[block_id] = full_block_df
        
#         if not valid_blocks:
#             yearly_season_stats.append({
#                 'Year': year,
#                 'Moyenne Saison Pluvieuse': np.nan, 'Début Saison Pluvieuse': pd.NaT,
#                 'Fin Saison Pluvieuse': pd.NaT, 'Durée Saison Pluvieuse Jours': np.nan
#             })
#             continue

#         main_block_id = max(valid_blocks, key=lambda k: (valid_blocks[k].index.max() - valid_blocks[k].index.min()).days)
#         main_season_df = valid_blocks[main_block_id]
        
#         rain_events_in_main_block = main_season_df[main_season_df['Rain_mm'] > 0].index.sort_values()

#         debut_saison = pd.NaT
#         if len(rain_events_in_main_block) >= 2:
#             first_rain_date = rain_events_in_main_block[0]
#             second_rain_date = rain_events_in_main_block[1]
#             if (second_rain_date - first_rain_date).days > NON_RELEVANT_RAIN_THRESHOLD_DAYS:
#                 debut_saison = second_rain_date
#             else:
#                 debut_saison = first_rain_date
#         elif len(rain_events_in_main_block) == 1:
#             debut_saison = rain_events_in_main_block[0]
#         else:
#             debut_saison = pd.NaT

#         fin_saison = pd.NaT
#         if len(rain_events_in_main_block) >= 2:
#             last_rain_date = rain_events_in_main_block[-1]
#             second_last_rain_date = rain_events_in_main_block[-2]
#             if (last_rain_date - second_last_rain_date).days > NON_RELEVANT_RAIN_THRESHOLD_DAYS:
#                 fin_saison = second_last_rain_date
#             else:
#                 fin_saison = last_rain_date
#         elif len(rain_events_in_main_block) == 1:
#             fin_saison = rain_events_in_main_block[0]
#         else:
#             fin_saison = pd.NaT

#         total_days = (fin_saison - debut_saison).days + 1 if pd.notna(debut_saison) and pd.notna(fin_saison) else np.nan
        
#         if pd.notna(debut_saison) and pd.notna(fin_saison) and total_days > 0:
#             season_rainfall_sum = year_df.loc[debut_saison:fin_saison]['Rain_mm'].sum()
#             moyenne_saison = season_rainfall_sum / total_days
#         else:
#             moyenne_saison = np.nan

#         yearly_season_stats.append({
#             'Year': year,
#             'Moyenne Saison Pluvieuse': moyenne_saison, 'Début Saison Pluvieuse': debut_saison,
#             'Fin Saison Pluvieuse': fin_saison, 'Durée Saison Pluvieuse Jours': total_days
#         })
#     return pd.DataFrame(yearly_season_stats).set_index('Year')

# def _calculate_dry_spell_stats_yearly(df_daily_rain_station: pd.DataFrame, df_season_stats: pd.DataFrame) -> pd.DataFrame:
#     yearly_dry_spell_events = []
    
#     df_daily_rain_station = df_daily_rain_station.set_index('Datetime').sort_index()

#     for year, year_df in df_daily_rain_station.groupby(df_daily_rain_station.index.year):
#         # Retrieve the determined rainy season start and end dates for the current year
#         season_start = df_season_stats.loc[year, 'Début Saison Pluvieuse'] if year in df_season_stats.index else pd.NaT
#         season_end = df_season_stats.loc[year, 'Fin Saison Pluvieuse'] if year in df_season_stats.index else pd.NaT

#         # If no valid rainy season is defined for the year, skip dry spell calculation
#         if pd.isna(season_start) or pd.isna(season_end) or season_start >= season_end:
#             yearly_dry_spell_events.append({
#                 'Year': year,
#                 'Début Sécheresse Définie': pd.NaT,
#                 'Fin Sécheresse Définie': pd.NaT,
#                 'Durée Sécheresse Définie Jours': 0
#             })
#             continue

#         # Filter daily rain data to include only the rainy season period
#         daily_data_in_season = year_df.loc[season_start:season_end]
        
#         full_daily_series_in_season = daily_data_in_season['Rain_mm'].resample('D').sum().fillna(0)
#         rainy_days_index = full_daily_series_in_season[full_daily_series_in_season > 0].index
        
#         if rainy_days_index.empty:
#             # If no rain days in the defined season, the entire season is a dry spell
#             duration = (season_end - season_start).days + 1
#             yearly_dry_spell_events.append({
#                 'Year': year,
#                 'Début Sécheresse Définie': season_start,
#                 'Fin Sécheresse Définie': season_end,
#                 'Durée Sécheresse Définie Jours': duration
#             })
#             continue
        
#         saison_moyenne_annual = df_season_stats.loc[year, 'Moyenne Saison Pluvieuse'] if year in df_season_stats.index and pd.notna(df_season_stats.loc[year, 'Moyenne Saison Pluvieuse']) else np.nan

#         if pd.isna(saison_moyenne_annual) or saison_moyenne_annual == 0:
#             yearly_dry_spell_events.append({
#                 'Year': year,
#                 'Début Sécheresse Définie': pd.NaT,
#                 'Fin Sécheresse Définie': pd.NaT,
#                 'Durée Sécheresse Définie Jours': 0
#             })
#             continue

#         longest_dry_spell_for_year = {
#             'Year': year,
#             'Début Sécheresse Définie': pd.NaT,
#             'Fin Sécheresse Définie': pd.NaT,
#             'Durée Sécheresse Définie Jours': 0
#         }
        
#         # Add the start of the season as a "virtual" rain event if the first actual rain is not at the very beginning
#         # This allows us to potentially calculate a dry spell from the season start if it's dry until the first rain.
#         effective_rainy_days_index = rainy_days_index.to_list()
#         if effective_rainy_days_index[0] > season_start:
#             effective_rainy_days_index.insert(0, season_start)
        
#         # Add the end of the season as a "virtual" rain event if the last actual rain is not at the very end
#         # This allows us to potentially calculate a dry spell until the season end if it's dry after the last rain.
#         if effective_rainy_days_index[-1] < season_end:
#             effective_rainy_days_index.append(season_end)
        
#         # Ensure unique and sorted
#         effective_rainy_days_index = pd.to_datetime(list(set(effective_rainy_days_index))).sort_values()

#         for i in range(1, len(effective_rainy_days_index)):
#             prev_rain_date = effective_rainy_days_index[i-1]
#             current_rain_date = effective_rainy_days_index[i]
#             dry_days_in_period = (current_rain_date - prev_rain_date).days - 1

#             if dry_days_in_period > 0:
#                 # Get the rainfall on the 'previous rain date' - this needs to be from the actual data if prev_rain_date is not season_start
#                 rain_prev_day_val = full_daily_series_in_season.get(prev_rain_date, 0) # Use .get() with default 0 for virtual start_season
                
#                 # Iterate through potential dry spell days
#                 for j in range(1, dry_days_in_period + 1):
#                     current_dry_date_in_loop = prev_rain_date + timedelta(days=j)
                    
#                     # Calculate ratio (rain_prev_day_val over days since last rain including current dry days)
#                     current_ratio = rain_prev_day_val / (j)

#                     if current_ratio < saison_moyenne_annual:
#                         debut_secheresse = current_dry_date_in_loop
#                         # The dry spell ends the day *before* the current_rain_date
#                         fin_secheresse = current_rain_date - timedelta(days=1)
#                         duree_secheresse_current = (fin_secheresse - debut_secheresse).days + 1
                        
#                         if duree_secheresse_current > longest_dry_spell_for_year['Durée Sécheresse Définie Jours']:
#                             longest_dry_spell_for_year['Début Sécheresse Définie'] = debut_secheresse
#                             longest_dry_spell_for_year['Fin Sécheresse Définie'] = fin_secheresse
#                             longest_dry_spell_for_year['Durée Sécheresse Définie Jours'] = duree_secheresse_current
#                         break

#         # If no dry spell was found (e.g., constant rain), ensure a default value
#         if longest_dry_spell_for_year['Durée Sécheresse Définie Jours'] == 0:
#              yearly_dry_spell_events.append({
#                 'Year': year,
#                 'Début Sécheresse Définie': pd.NaT,
#                 'Fin Sécheresse Définie': pd.NaT,
#                 'Durée Sécheresse Définie Jours': 0
#             })
#         elif longest_dry_spell_for_year['Durée Sécheresse Définie Jours'] > 0:
#             yearly_dry_spell_events.append(longest_dry_spell_for_year)
    
#     return pd.DataFrame(yearly_dry_spell_events).set_index('Year')

# # --- Main Visualization Function (unchanged from previous version) ---

# def generate_daily_stats_plot_plotly(df: pd.DataFrame, variable: str, station_colors: dict) -> go.Figure:
#     """
#     Generates interactive Plotly graphs showing a single bar per station for each metric.
#     For each metric (except minimum), it displays the maximum of the annual values.
#     For the minimum metric, it displays the minimum of the annual minimum values.
#     Bars are colored by station and include dates and *years* for Max/Min and season/dry spell durations.
#     """
#     try:
#         df_processed = df.copy()

#         col_sup = ['Rain_01_mm', 'Rain_02_mm']
#         for var in col_sup:
#             if var in df_processed.columns:
#                 df_processed = df_processed.drop(var, axis=1)

#         if isinstance(df_processed.index, pd.DatetimeIndex):
#             df_processed = df_processed.reset_index()
            
#         df_processed['Datetime'] = pd.to_datetime(df_processed['Datetime'], errors='coerce')
#         df_processed = df_processed.dropna(subset=['Datetime', 'Station'])
        
#         if df_processed.empty:
#             return go.Figure().add_annotation(
#                 x=0.5, y=0.5, text=str(_l("Aucune donnée valide après le nettoyage initial.")),
#                 showarrow=False, font=dict(size=16)
#             ).update_layout(title=str(_l("Statistiques Annuelles")))

#         df_processed['Year'] = df_processed['Datetime'].dt.year

#         if 'Is_Daylight' not in df_processed.columns:
#             df_processed['Is_Daylight'] = (df_processed['Datetime'].dt.hour >= 7) & (df_processed['Datetime'].dt.hour <= 18)

#         if variable not in df_processed.columns:
#             return go.Figure().add_annotation(
#                 x=0.5, y=0.5, text=str(_l("La variable sélectionnée n'est pas présente dans les données.")),
#                 showarrow=False, font=dict(size=16)
#             ).update_layout(title=str(_l("Statistiques Annuelles")))

#         var_meta = METADATA_VARIABLES.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
#         var_label = str(get_var_label(var_meta, 'Nom'))
#         var_unit = str(get_var_label(var_meta, 'Unite'))

#         df_processed[variable] = pd.to_numeric(df_processed[variable], errors='coerce')
        
#         final_stats_list = []

#         for station_name, station_df_full in df_processed.groupby('Station'):
            
#             station_agg_data = {'Station': station_name}

#             df_clean_station = station_df_full.dropna(subset=[variable])
#             if variable == 'Solar_R_W/m^2':
#                 df_clean_station = df_clean_station[df_clean_station['Is_Daylight']]

#             if df_clean_station.empty:
#                 final_stats_list.append(station_agg_data)
#                 continue

#             annual_variable_mean_median = df_clean_station.groupby('Year')[variable].agg(
#                 Mediane='median',
#                 Moyenne='mean'
#             ).reset_index()

#             overall_max_row = df_clean_station.loc[df_clean_station[variable].idxmax()]
#             station_agg_data[get_metric_label('Maximum')] = overall_max_row[variable]
#             station_agg_data[get_metric_label('Date Max')] = overall_max_row['Datetime']
#             station_agg_data[get_metric_label('Year Max')] = overall_max_row['Year']

#             overall_min_row = df_clean_station.loc[df_clean_station[variable].idxmin()]
#             station_agg_data[get_metric_label('Minimum')] = overall_min_row[variable]
#             station_agg_data[get_metric_label('Date Min')] = overall_min_row['Datetime']
#             station_agg_data[get_metric_label('Year Min')] = overall_min_row['Year']
            
#             if not annual_variable_mean_median.empty:
#                 max_median_row = annual_variable_mean_median.loc[annual_variable_mean_median['Mediane'].idxmax()]
#                 station_agg_data[get_metric_label('Mediane')] = max_median_row['Mediane']
#                 station_agg_data[get_metric_label('Year Mediane')] = max_median_row['Year']

#                 max_mean_row = annual_variable_mean_median.loc[annual_variable_mean_median['Moyenne'].idxmax()]
#                 station_agg_data[get_metric_label('Moyenne')] = max_mean_row['Moyenne']
#                 station_agg_data[get_metric_label('Year Moyenne')] = max_mean_row['Year']
#             else:
#                 station_agg_data[get_metric_label('Mediane')] = np.nan
#                 station_agg_data[get_metric_label('Year Mediane')] = np.nan
#                 station_agg_data[get_metric_label('Moyenne')] = np.nan
#                 station_agg_data[get_metric_label('Year Moyenne')] = np.nan


#             if var_meta.get('is_rain', False) and variable == 'Rain_mm':
#                 df_daily_rain_station = station_df_full.groupby(pd.Grouper(key='Datetime', freq='D'))['Rain_mm'].sum().reset_index()
                
#                 annual_cumulative_rain = station_df_full.groupby('Year')['Rain_mm'].sum().reset_index()
#                 if not annual_cumulative_rain.empty:
#                     max_cumul_row = annual_cumulative_rain.loc[annual_cumulative_rain['Rain_mm'].idxmax()]
#                     station_agg_data[get_metric_label('Cumul Annuel')] = max_cumul_row['Rain_mm']
#                     station_agg_data[get_metric_label('Year Cumul Annuel')] = max_cumul_row['Year']
#                 else:
#                     station_agg_data[get_metric_label('Cumul Annuel')] = np.nan
#                     station_agg_data[get_metric_label('Year Cumul Annuel')] = np.nan

#                 mean_rainy_days_annual = station_df_full[station_df_full['Rain_mm'] > 0].groupby('Year')['Rain_mm'].mean().reset_index()
#                 if not mean_rainy_days_annual.empty:
#                     max_mean_rainy_days_row = mean_rainy_days_annual.loc[mean_rainy_days_annual['Rain_mm'].idxmax()]
#                     station_agg_data[get_metric_label('Moyenne Jours Pluvieux')] = max_mean_rainy_days_row['Rain_mm']
#                     station_agg_data[get_metric_label('Year Moyenne Jours Pluvieux')] = max_mean_rainy_days_row['Year']
#                 else:
#                     station_agg_data[get_metric_label('Moyenne Jours Pluvieux')] = np.nan
#                     station_agg_data[get_metric_label('Year Moyenne Jours Pluvieux')] = np.nan

#                 df_season_stats_yearly = _calculate_rainy_season_stats_yearly(df_daily_rain_station)
#                 if not df_season_stats_yearly.empty:
#                     max_mean_season_row = df_season_stats_yearly.loc[df_season_stats_yearly['Moyenne Saison Pluvieuse'].idxmax()] if not df_season_stats_yearly['Moyenne Saison Pluvieuse'].isnull().all() else pd.Series()
#                     if not max_mean_season_row.empty:
#                         station_agg_data[get_metric_label('Moyenne Saison Pluvieuse')] = max_mean_season_row['Moyenne Saison Pluvieuse']
#                         station_agg_data[get_metric_label('Début Saison Pluvieuse')] = max_mean_season_row['Début Saison Pluvieuse']
#                         station_agg_data[get_metric_label('Fin Saison Pluvieuse')] = max_mean_season_row['Fin Saison Pluvieuse']
#                         station_agg_data[get_metric_label('Durée Saison Pluvieuse Jours')] = max_mean_season_row['Durée Saison Pluvieuse Jours']
#                         station_agg_data[get_metric_label('Year Durée Saison Pluvieuse Jours')] = max_mean_season_row.name
#                     else:
#                         station_agg_data[get_metric_label('Moyenne Saison Pluvieuse')] = np.nan
#                         station_agg_data[get_metric_label('Début Saison Pluvieuse')] = pd.NaT
#                         station_agg_data[get_metric_label('Fin Saison Pluvieuse')] = pd.NaT
#                         station_agg_data[get_metric_label('Durée Saison Pluvieuse Jours')] = np.nan
#                         station_agg_data[get_metric_label('Year Durée Saison Pluvieuse Jours')] = np.nan

#                 df_dry_spell_events_yearly = _calculate_dry_spell_stats_yearly(df_daily_rain_station, df_season_stats_yearly)
#                 if not df_dry_spell_events_yearly.empty:
#                     longest_dry_spell_row = df_dry_spell_events_yearly.loc[df_dry_spell_events_yearly['Durée Sécheresse Définie Jours'].idxmax()] if not df_dry_spell_events_yearly['Durée Sécheresse Définie Jours'].isnull().all() else pd.Series()
#                     if not longest_dry_spell_row.empty:
#                         station_agg_data[get_metric_label('Durée Sécheresse Définie Jours')] = longest_dry_spell_row['Durée Sécheresse Définie Jours']
#                         station_agg_data[get_metric_label('Début Sécheresse Définie')] = longest_dry_spell_row['Début Sécheresse Définie']
#                         station_agg_data[get_metric_label('Fin Sécheresse Définie')] = longest_dry_spell_row['Fin Sécheresse Définie']
#                         station_agg_data[get_metric_label('Year Durée Sécheresse Définie Jours')] = longest_dry_spell_row.name
#                     else:
#                         station_agg_data[get_metric_label('Durée Sécheresse Définie Jours')] = np.nan
#                         station_agg_data[get_metric_label('Début Sécheresse Définie')] = pd.NaT
#                         station_agg_data[get_metric_label('Fin Sécheresse Définie')] = pd.NaT
#                         station_agg_data[get_metric_label('Year Durée Sécheresse Définie Jours')] = np.nan
            
#             final_stats_list.append(station_agg_data)

#         final_stats_df = pd.DataFrame(final_stats_list)

#         if final_stats_df.empty:
#             return go.Figure().add_annotation(
#                 x=0.5, y=0.5, text=str(_l("Aucune statistique à afficher pour les variables et stations sélectionnées.")),
#                 showarrow=False, font=dict(size=16)
#             ).update_layout(title=str(_l("Statistiques Annuelles")))

#         if var_meta.get('is_rain', False) and variable == 'Rain_mm':
#             metrics_to_plot_display_names = [
#                 get_metric_label('Maximum'), get_metric_label('Minimum'), get_metric_label('Mediane'), get_metric_label('Moyenne'),
#                 get_metric_label('Cumul Annuel'), get_metric_label('Moyenne Jours Pluvieux'),
#                 get_metric_label('Moyenne Saison Pluvieuse'), get_metric_label('Durée Saison Pluvieuse Jours'),
#                 get_metric_label('Durée Sécheresse Définie Jours')
#             ]
#         else:
#             metrics_to_plot_display_names = [
#                 get_metric_label('Maximum'), get_metric_label('Minimum'), get_metric_label('Mediane'), get_metric_label('Moyenne')
#             ]

#         metrics_to_plot_filtered = [
#             col for col in metrics_to_plot_display_names
#             if col in final_stats_df.columns and not final_stats_df[col].isnull().all()
#         ]
        
#         if not metrics_to_plot_filtered:
#              return go.Figure().add_annotation(
#                 x=0.5, y=0.5, text=str(_l("Aucune statistique à afficher pour les variables et stations sélectionnées.")),
#                 showarrow=False, font=dict(size=16)
#             ).update_layout(title=str(_l("Statistiques Annuelles")))

#         num_metrics = len(metrics_to_plot_filtered)
#         cols_plot = 2
#         rows_plot = (num_metrics + cols_plot - 1) // cols_plot

#         height_plot = 400 * rows_plot if rows_plot <= 2 else 300 * rows_plot

#         subplot_titles = [f"{var_label} {metric}" for metric in metrics_to_plot_filtered]

#         fig = make_subplots(
#             rows=rows_plot, cols=cols_plot,
#             subplot_titles=subplot_titles,
#             vertical_spacing=0.1
#         )
        
#         unique_stations = sorted(final_stats_df['Station'].unique())
#         final_stats_df = final_stats_df.set_index('Station').reindex(unique_stations).reset_index()

#         for i, display_metric_col_name in enumerate(metrics_to_plot_filtered):
#             row = (i // cols_plot) + 1
#             col = (i % cols_plot) + 1
            
#             hover_text_list = []
#             station_colors_for_trace = []
            
#             for _, row_data in final_stats_df.iterrows():
#                 station_name = row_data['Station']
#                 station_color = station_colors.get(station_name, '#1f77b4')
#                 station_colors_for_trace.append(station_color)
                
#                 value = row_data[display_metric_col_name]
                
#                 if pd.isna(value):
#                     hover_text_list.append("")
#                     continue

#                 hover_text = ""
#                 associated_year = np.nan

#                 if display_metric_col_name == get_metric_label('Maximum'):
#                     date_val = row_data.get(get_metric_label('Date Max'))
#                     date_str = date_val.strftime('%d/%m/%Y') if pd.notna(date_val) else str(_l('Unknown date'))
#                     associated_year = row_data.get(get_metric_label('Year Max'))
#                     hover_text = str(_l("<b>Maximum</b><br>Value: {:.1f} {}<br>Date: {}")).format(value, var_unit, date_str)
#                     if pd.notna(associated_year):
#                         hover_text += f"<br>{str(_l('Year: {}')).format(int(associated_year))}"
                
#                 elif display_metric_col_name == get_metric_label('Minimum'):
#                     date_val = row_data.get(get_metric_label('Date Min'))
#                     date_str = date_val.strftime('%d/%m/%Y') if pd.notna(date_val) else str(_l('Unknown date'))
#                     associated_year = row_data.get(get_metric_label('Year Min'))
#                     hover_text = str(_l("<b>Minimum</b><br>Value: {:.1f} {}<br>Date: {}")).format(value, var_unit, date_str)
#                     if pd.notna(associated_year):
#                         hover_text += f"<br>{str(_l('Year: {}')).format(int(associated_year))}"
                
#                 elif display_metric_col_name == get_metric_label('Mediane'):
#                     associated_year = row_data.get(get_metric_label('Year Mediane'))
#                     hover_text = str(_l("<b>Mediane</b><br>Value: {:.1f} {}")).format(value, var_unit)
#                     if pd.notna(associated_year):
#                         hover_text += f"<br>{str(_l('Year: {}')).format(int(associated_year))}"

#                 elif display_metric_col_name == get_metric_label('Moyenne'):
#                     associated_year = row_data.get(get_metric_label('Year Moyenne'))
#                     hover_text = str(_l("<b>Moyenne</b><br>Value: {:.1f} {}")).format(value, var_unit)
#                     if pd.notna(associated_year):
#                         hover_text += f"<br>{str(_l('Year: {}')).format(int(associated_year))}"

#                 elif display_metric_col_name == get_metric_label('Cumul Annuel'):
#                     associated_year = row_data.get(get_metric_label('Year Cumul Annuel'))
#                     hover_text = str(_l("<b>Cumul Annuel</b><br>Value: {:.1f} {}")).format(value, var_unit)
#                     if pd.notna(associated_year):
#                         hover_text += f"<br>{str(_l('Year: {}')).format(int(associated_year))}"

#                 elif display_metric_col_name == get_metric_label('Moyenne Jours Pluvieux'):
#                     associated_year = row_data.get(get_metric_label('Year Moyenne Jours Pluvieux'))
#                     hover_text = str(_l("<b>Moyenne Jours Pluvieux</b><br>Value: {:.1f} {}")).format(value, var_unit)
#                     if pd.notna(associated_year):
#                         hover_text += f"<br>{str(_l('Year: {}')).format(int(associated_year))}"
                
#                 elif display_metric_col_name == get_metric_label('Durée Saison Pluvieuse Jours'):
#                     date_debut = row_data.get(get_metric_label('Début Saison Pluvieuse'))
#                     date_fin = row_data.get(get_metric_label('Fin Saison Pluvieuse'))
#                     date_debut_str = date_debut.strftime('%d/%m/%Y') if pd.notna(date_debut) else str(_l('Unknown date'))
#                     date_fin_str = date_fin.strftime('%d/%m/%Y') if pd.notna(date_fin) else str(_l('Unknown date'))
#                     associated_year = row_data.get(get_metric_label('Year Durée Saison Pluvieuse Jours'))
                    
#                     hover_text = str(_l("<b>Durée Saison Pluvieuse Jours</b><br>"))
#                     if pd.notna(date_debut) and pd.notna(date_fin):
#                         hover_text += str(_l("From {} to {}")).format(date_debut_str, date_fin_str) + "<br>"
#                     hover_text += str(_l("Duration: {} days")).format(int(value))
#                     if pd.notna(associated_year):
#                         hover_text += f"<br>{str(_l('Year: {}')).format(int(associated_year))}"

#                 elif display_metric_col_name == get_metric_label('Durée Sécheresse Définie Jours'):
#                     date_debut = row_data.get(get_metric_label('Début Sécheresse Définie'))
#                     date_fin = row_data.get(get_metric_label('Fin Sécheresse Définie'))
#                     date_debut_str = date_debut.strftime('%d/%m/%Y') if pd.notna(date_debut) else str(_l('Unknown date'))
#                     date_fin_str = date_fin.strftime('%d/%m/%Y') if pd.notna(date_fin) else str(_l('Unknown date'))
#                     associated_year = row_data.get(get_metric_label('Year Durée Sécheresse Définie Jours'))
                    
#                     hover_text = str(_l("<b>Durée Sécheresse Définie Jours</b><br>"))
#                     if pd.notna(date_debut) and pd.notna(date_fin):
#                         hover_text += str(_l("From {} to {}")).format(date_debut_str, date_fin_str) + "<br>"
#                     hover_text += str(_l("Duration: {} days")).format(int(value))
#                     if pd.notna(associated_year):
#                         hover_text += f"<br>{str(_l('Year: {}')).format(int(associated_year))}"
                
#                 else:
#                     hover_text = str(_l("<b>{}</b><br>Value: {:.1f} {}")).format(display_metric_col_name, value, var_unit)
#                     year_col = f"Year {display_metric_col_name}"
#                     if year_col in row_data and pd.notna(row_data[year_col]):
#                         hover_text += f"<br>{str(_l('Year: {}')).format(int(row_data[year_col]))}"


#                 hover_text_list.append(hover_text)

#             fig.add_trace(
#                 go.Bar(
#                     x=final_stats_df[display_metric_col_name],
#                     y=final_stats_df['Station'],
#                     orientation='h',
#                     marker_color=station_colors_for_trace,
#                     name=display_metric_col_name,
#                     hovertext=hover_text_list,
#                     hoverinfo='text',
#                     showlegend=False,
#                     textposition='none'
#                 ),
#                 row=row,
#                 col=col
#             )

#             xaxis_title = str(_l("Jours")) if (get_metric_label("Durée") in display_metric_col_name or "Duration" in display_metric_col_name) else f"{var_label} ({var_unit})"
#             fig.update_xaxes(
#                 title_text=xaxis_title,
#                 showgrid=False,
#                 row=row, col=col
#             )
            
#             fig.update_yaxes(
#                 title_text=str(_l("Station")),
#                 showgrid=False,
#                 row=row, col=col,
#                 categoryorder='array',
#                 categoryarray=unique_stations,
#                 automargin=True
#             )

#         for station_name in unique_stations:
#             fig.add_trace(
#                 go.Bar(
#                     x=[0], y=[station_name],
#                     marker_color=station_colors.get(station_name, '#1f77b4'),
#                     name=station_name,
#                     showlegend=True,
#                     visible='legendonly'
#                 )
#             )

#         fig.update_layout(
#             height=height_plot,
#             title_text=str(get_metric_label("Statistics of {} by Station")).format(var_label),
#             showlegend=True,
#             legend_title_text=str(_l("Station")),
#             legend=dict(
#                 orientation="h",
#                 yanchor="bottom",
#                 y=-0.15,
#                 xanchor="center",
#                 x=0.5
#             ),
#             hovermode='closest',
#             plot_bgcolor='white',
#             paper_bgcolor='white'
#         )
        
#         return fig
        
#     except Exception as e:
#         print(f"Erreur dans generate_daily_stats_plot_plotly: {str(e)}")
#         traceback.print_exc()
#         return go.Figure().add_annotation(
#             x=0.5, y=0.5, text=str(_l("Une erreur est survenue lors de la génération du graphique: {}")).format(str(e)),
#             showarrow=False, font=dict(size=16)
#         ).update_layout(title=str(_l("Erreur de visualisation")))


####################

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import timedelta
import warnings
import traceback



def _l(text):
    return text

# --- Helper Functions for Rainfall Specific Calculations (Remain yearly) ---

def _calculate_rainy_season_stats_yearly(df_daily_rain_station: pd.DataFrame) -> pd.DataFrame:
    RAIN_SEASON_GAP_THRESHOLD = pd.Timedelta(days=60)
    NON_RELEVANT_RAIN_THRESHOLD_DAYS = 45 
    
    yearly_season_stats = []
    
    df_daily_rain_station = df_daily_rain_station.set_index('Datetime').sort_index()

    for year, year_df in df_daily_rain_station.groupby(df_daily_rain_station.index.year):
        rain_events = year_df[year_df['Rain_mm'] > 0].index
        
        if rain_events.empty or len(rain_events) < 2:
            yearly_season_stats.append({
                'Year': year,
                'Moyenne Saison Pluvieuse': np.nan, 'Début Saison Pluvieuse': pd.NaT,
                'Fin Saison Pluvieuse': pd.NaT, 'Durée Saison Pluvieuse Jours': np.nan
            })
            continue

        block_ids = (rain_events.to_series().diff() > RAIN_SEASON_GAP_THRESHOLD).cumsum()
        valid_blocks = {}
        for block_id, rain_dates_in_block in rain_events.to_series().groupby(block_ids):
            if not rain_dates_in_block.empty:
                block_start = rain_dates_in_block.min()
                block_end = rain_dates_in_block.max()
                full_block_df = year_df.loc[block_start:block_end]
                if len(full_block_df[full_block_df['Rain_mm'] > 0]) > 1:
                     valid_blocks[block_id] = full_block_df
        
        if not valid_blocks:
            yearly_season_stats.append({
                'Year': year,
                'Moyenne Saison Pluvieuse': np.nan, 'Début Saison Pluvieuse': pd.NaT,
                'Fin Saison Pluvieuse': pd.NaT, 'Durée Saison Pluvieuse Jours': np.nan
            })
            continue

        main_block_id = max(valid_blocks, key=lambda k: (valid_blocks[k].index.max() - valid_blocks[k].index.min()).days)
        main_season_df = valid_blocks[main_block_id]
        
        rain_events_in_main_block = main_season_df[main_season_df['Rain_mm'] > 0].index.sort_values()

        debut_saison = pd.NaT
        if len(rain_events_in_main_block) >= 2:
            first_rain_date = rain_events_in_main_block[0]
            second_rain_date = rain_events_in_main_block[1]
            if (second_rain_date - first_rain_date).days > NON_RELEVANT_RAIN_THRESHOLD_DAYS:
                debut_saison = second_rain_date
            else:
                debut_saison = first_rain_date
        elif len(rain_events_in_main_block) == 1:
            debut_saison = rain_events_in_main_block[0]
        else:
            debut_saison = pd.NaT

        fin_saison = pd.NaT
        if len(rain_events_in_main_block) >= 2:
            last_rain_date = rain_events_in_main_block[-1]
            second_last_rain_date = rain_events_in_main_block[-2]
            if (last_rain_date - second_last_rain_date).days > NON_RELEVANT_RAIN_THRESHOLD_DAYS:
                fin_saison = second_last_rain_date
            else:
                fin_saison = last_rain_date
        elif len(rain_events_in_main_block) == 1:
            fin_saison = rain_events_in_main_block[0]
        else:
            fin_saison = pd.NaT

        total_days = (fin_saison - debut_saison).days + 1 if pd.notna(debut_saison) and pd.notna(fin_saison) else np.nan
        
        if pd.notna(debut_saison) and pd.notna(fin_saison) and total_days > 0:
            season_rainfall_sum = year_df.loc[debut_saison:fin_saison]['Rain_mm'].sum()
            moyenne_saison = season_rainfall_sum / total_days
        else:
            moyenne_saison = np.nan

        yearly_season_stats.append({
            'Year': year,
            'Moyenne Saison Pluvieuse': moyenne_saison, 'Début Saison Pluvieuse': debut_saison,
            'Fin Saison Pluvieuse': fin_saison, 'Durée Saison Pluvieuse Jours': total_days
        })
    return pd.DataFrame(yearly_season_stats).set_index('Year')

def _calculate_dry_spell_stats_yearly(df_daily_rain_station: pd.DataFrame, df_season_stats: pd.DataFrame) -> pd.DataFrame:
    yearly_dry_spell_events = []
    
    df_daily_rain_station = df_daily_rain_station.set_index('Datetime').sort_index()

    for year, year_df in df_daily_rain_station.groupby(df_daily_rain_station.index.year):
        season_start = df_season_stats.loc[year, 'Début Saison Pluvieuse'] if year in df_season_stats.index else pd.NaT
        season_end = df_season_stats.loc[year, 'Fin Saison Pluvieuse'] if year in df_season_stats.index else pd.NaT

        if pd.isna(season_start) or pd.isna(season_end) or season_start >= season_end:
            yearly_dry_spell_events.append({
                'Year': year,
                'Début Sécheresse Définie': pd.NaT,
                'Fin Sécheresse Définie': pd.NaT,
                'Durée Sécheresse Définie Jours': 0
            })
            continue

        daily_data_in_season = year_df.loc[season_start:season_end]
        
        full_daily_series_in_season = daily_data_in_season['Rain_mm'].resample('D').sum().fillna(0)
        rainy_days_index = full_daily_series_in_season[full_daily_series_in_season > 0].index
        
        if rainy_days_index.empty:
            duration = (season_end - season_start).days + 1
            yearly_dry_spell_events.append({
                'Year': year,
                'Début Sécheresse Définie': season_start,
                'Fin Sécheresse Définie': season_end,
                'Durée Sécheresse Définie Jours': duration
            })
            continue
        
        saison_moyenne_annual = df_season_stats.loc[year, 'Moyenne Saison Pluvieuse'] if year in df_season_stats.index and pd.notna(df_season_stats.loc[year, 'Moyenne Saison Pluvieuse']) else np.nan

        if pd.isna(saison_moyenne_annual) or saison_moyenne_annual == 0:
            yearly_dry_spell_events.append({
                'Year': year,
                'Début Sécheresse Définie': pd.NaT,
                'Fin Sécheresse Définie': pd.NaT,
                'Durée Sécheresse Définie Jours': 0
            })
            continue

        longest_dry_spell_for_year = {
            'Year': year,
            'Début Sécheresse Définie': pd.NaT,
            'Fin Sécheresse Définie': pd.NaT,
            'Durée Sécheresse Définie Jours': 0
        }
        
        effective_rainy_days_index = rainy_days_index.to_list()
        if effective_rainy_days_index[0] > season_start:
            effective_rainy_days_index.insert(0, season_start)
        
        if effective_rainy_days_index[-1] < season_end:
            effective_rainy_days_index.append(season_end)
        
        effective_rainy_days_index = pd.to_datetime(list(set(effective_rainy_days_index))).sort_values()

        for i in range(1, len(effective_rainy_days_index)):
            prev_rain_date = effective_rainy_days_index[i-1]
            current_rain_date = effective_rainy_days_index[i]
            dry_days_in_period = (current_rain_date - prev_rain_date).days - 1

            if dry_days_in_period > 0:
                rain_prev_day_val = full_daily_series_in_season.get(prev_rain_date, 0)
                
                for j in range(1, dry_days_in_period + 1):
                    current_dry_date_in_loop = prev_rain_date + timedelta(days=j)
                    
                    current_ratio = rain_prev_day_val / (j)

                    if current_ratio < saison_moyenne_annual:
                        debut_secheresse = current_dry_date_in_loop
                        fin_secheresse = current_rain_date - timedelta(days=1)
                        duree_secheresse_current = (fin_secheresse - debut_secheresse).days + 1
                        
                        if duree_secheresse_current > longest_dry_spell_for_year['Durée Sécheresse Définie Jours']:
                            longest_dry_spell_for_year['Début Sécheresse Définie'] = debut_secheresse
                            longest_dry_spell_for_year['Fin Sécheresse Définie'] = fin_secheresse
                            longest_dry_spell_for_year['Durée Sécheresse Définie Jours'] = duree_secheresse_current
                        break

        if longest_dry_spell_for_year['Durée Sécheresse Définie Jours'] == 0:
             yearly_dry_spell_events.append({
                'Year': year,
                'Début Sécheresse Définie': pd.NaT,
                'Fin Sécheresse Définie': pd.NaT,
                'Durée Sécheresse Définie Jours': 0
            })
        elif longest_dry_spell_for_year['Durée Sécheresse Définie Jours'] > 0:
            yearly_dry_spell_events.append(longest_dry_spell_for_year)
    
    return pd.DataFrame(yearly_dry_spell_events).set_index('Year')

# --- Main Visualization Function ---

def generate_daily_stats_plot_plotly(df: pd.DataFrame, variable: str, station_colors: dict, df_original: pd.DataFrame = None) -> go.Figure:
    """
    Generates interactive Plotly graphs showing a single bar per station for each metric.
    For Max/Min, it uses values from df_original (raw data) if provided, otherwise from df.
    For other metrics, it uses df (potentially interpolated).
    Bars are colored by station and include dates and years for Max/Min and season/dry spell durations.
    """
    try:
        df_processed = df.copy()

        # Handle df_original for Max/Min calculations
        if df_original is not None:
            df_original_processed = df_original.copy()
            if isinstance(df_original_processed.index, pd.DatetimeIndex):
                df_original_processed = df_original_processed.reset_index()
            df_original_processed['Datetime'] = pd.to_datetime(df_original_processed['Datetime'], errors='coerce')
            df_original_processed = df_original_processed.dropna(subset=['Datetime', 'Station'])
            df_original_processed['Year'] = df_original_processed['Datetime'].dt.year
            df_original_processed[variable] = pd.to_numeric(df_original_processed[variable], errors='coerce')
            # Filter out NaNs for original Max/Min to ensure they are true observed values
            df_original_for_extremes = df_original_processed.dropna(subset=[variable])
        else:
            # If no original DF, use the processed DF for max/min as well (old behavior)
            df_original_for_extremes = df_processed.copy()

        col_sup = ['Rain_01_mm', 'Rain_02_mm']
        for var in col_sup:
            if var in df_processed.columns:
                df_processed = df_processed.drop(var, axis=1)
            if df_original is not None and var in df_original_processed.columns:
                 df_original_processed = df_original_processed.drop(var, axis=1) # Keep df_original_processed clean too

        if isinstance(df_processed.index, pd.DatetimeIndex):
            df_processed = df_processed.reset_index()
            
        df_processed['Datetime'] = pd.to_datetime(df_processed['Datetime'], errors='coerce')
        df_processed = df_processed.dropna(subset=['Datetime', 'Station'])
        
        if df_processed.empty:
            return go.Figure().add_annotation(
                x=0.5, y=0.5, text=str(_l("Aucune donnée valide après le nettoyage initial.")),
                showarrow=False, font=dict(size=16)
            ).update_layout(title=str(_l("Statistiques Annuelles")))

        df_processed['Year'] = df_processed['Datetime'].dt.year

        if 'Is_Daylight' not in df_processed.columns:
            df_processed['Is_Daylight'] = (df_processed['Datetime'].dt.hour >= 7) & (df_processed['Datetime'].dt.hour <= 18)

        if variable not in df_processed.columns:
            return go.Figure().add_annotation(
                x=0.5, y=0.5, text=str(_l("La variable sélectionnée n'est pas présente dans les données.")),
                showarrow=False, font=dict(size=16)
            ).update_layout(title=str(_l("Statistiques Annuelles")))

        var_meta = METADATA_VARIABLES.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
        var_label = str(get_var_label(var_meta, 'Nom'))
        var_unit = str(get_var_label(var_meta, 'Unite'))

        df_processed[variable] = pd.to_numeric(df_processed[variable], errors='coerce')
        
        final_stats_list = []

        for station_name, station_df_full in df_processed.groupby('Station'):
            
            station_agg_data = {'Station': station_name}

            # For Max/Min, use the original (non-interpolated) data for the specific station
            station_df_original_extremes = df_original_for_extremes[df_original_for_extremes['Station'] == station_name]
            
            # For other calculations, continue using the potentially interpolated data (df_processed)
            df_clean_station = station_df_full.dropna(subset=[variable])
            if variable == 'Solar_R_W/m^2':
                df_clean_station = df_clean_station[df_clean_station['Is_Daylight']]

            if df_clean_station.empty and station_df_original_extremes.empty:
                final_stats_list.append(station_agg_data)
                continue

            # --- CALCULATE MAX/MIN FROM ORIGINAL DATA ---
            if not station_df_original_extremes.empty and not station_df_original_extremes[variable].isnull().all():
                overall_max_row_orig = station_df_original_extremes.loc[station_df_original_extremes[variable].idxmax()]
                station_agg_data[get_metric_label('Maximum')] = overall_max_row_orig[variable]
                station_agg_data[get_metric_label('Date Max')] = overall_max_row_orig['Datetime']
                station_agg_data[get_metric_label('Year Max')] = overall_max_row_orig['Year']

                overall_min_row_orig = station_df_original_extremes.loc[station_df_original_extremes[variable].idxmin()]
                station_agg_data[get_metric_label('Minimum')] = overall_min_row_orig[variable]
                station_agg_data[get_metric_label('Date Min')] = overall_min_row_orig['Datetime']
                station_agg_data[get_metric_label('Year Min')] = overall_min_row_orig['Year']
            else:
                station_agg_data[get_metric_label('Maximum')] = np.nan
                station_agg_data[get_metric_label('Date Max')] = pd.NaT
                station_agg_data[get_metric_label('Year Max')] = np.nan
                station_agg_data[get_metric_label('Minimum')] = np.nan
                station_agg_data[get_metric_label('Date Min')] = pd.NaT
                station_agg_data[get_metric_label('Year Min')] = np.nan
            # --- END MAX/MIN ORIGINAL DATA CALCULATION ---
            
            # Calculate annual aggregates for mean and median (using potentially interpolated data)
            annual_variable_mean_median = df_clean_station.groupby('Year')[variable].agg(
                Mediane='median',
                Moyenne='mean'
            ).reset_index()

            if not annual_variable_mean_median.empty:
                max_median_row = annual_variable_mean_median.loc[annual_variable_mean_median['Mediane'].idxmax()]
                station_agg_data[get_metric_label('Mediane')] = max_median_row['Mediane']
                station_agg_data[get_metric_label('Year Mediane')] = max_median_row['Year']

                max_mean_row = annual_variable_mean_median.loc[annual_variable_mean_median['Moyenne'].idxmax()]
                station_agg_data[get_metric_label('Moyenne')] = max_mean_row['Moyenne']
                station_agg_data[get_metric_label('Year Moyenne')] = max_mean_row['Year']
            else:
                station_agg_data[get_metric_label('Mediane')] = np.nan
                station_agg_data[get_metric_label('Year Mediane')] = np.nan
                station_agg_data[get_metric_label('Moyenne')] = np.nan
                station_agg_data[get_metric_label('Year Moyenne')] = np.nan


            # Rainfall specific calculations (using potentially interpolated data)
            if var_meta.get('is_rain', False) and variable == 'Rain_mm':
                df_daily_rain_station = station_df_full.groupby(pd.Grouper(key='Datetime', freq='D'))['Rain_mm'].sum().reset_index()
                
                annual_cumulative_rain = station_df_full.groupby('Year')['Rain_mm'].sum().reset_index()
                if not annual_cumulative_rain.empty:
                    max_cumul_row = annual_cumulative_rain.loc[annual_cumulative_rain['Rain_mm'].idxmax()]
                    station_agg_data[get_metric_label('Cumul Annuel')] = max_cumul_row['Rain_mm']
                    station_agg_data[get_metric_label('Year Cumul Annuel')] = max_cumul_row['Year']
                else:
                    station_agg_data[get_metric_label('Cumul Annuel')] = np.nan
                    station_agg_data[get_metric_label('Year Cumul Annuel')] = np.nan

                mean_rainy_days_annual = station_df_full[station_df_full['Rain_mm'] > 0].groupby('Year')['Rain_mm'].mean().reset_index()
                if not mean_rainy_days_annual.empty:
                    max_mean_rainy_days_row = mean_rainy_days_annual.loc[mean_rainy_days_annual['Rain_mm'].idxmax()]
                    station_agg_data[get_metric_label('Moyenne Jours Pluvieux')] = max_mean_rainy_days_row['Rain_mm']
                    station_agg_data[get_metric_label('Year Moyenne Jours Pluvieux')] = max_mean_rainy_days_row['Year']
                else:
                    station_agg_data[get_metric_label('Moyenne Jours Pluvieux')] = np.nan
                    station_agg_data[get_metric_label('Year Moyenne Jours Pluvieux')] = np.nan

                df_season_stats_yearly = _calculate_rainy_season_stats_yearly(df_daily_rain_station)
                if not df_season_stats_yearly.empty:
                    max_mean_season_row = df_season_stats_yearly.loc[df_season_stats_yearly['Moyenne Saison Pluvieuse'].idxmax()] if not df_season_stats_yearly['Moyenne Saison Pluvieuse'].isnull().all() else pd.Series()
                    if not max_mean_season_row.empty:
                        station_agg_data[get_metric_label('Moyenne Saison Pluvieuse')] = max_mean_season_row['Moyenne Saison Pluvieuse']
                        station_agg_data[get_metric_label('Début Saison Pluvieuse')] = max_mean_season_row['Début Saison Pluvieuse']
                        station_agg_data[get_metric_label('Fin Saison Pluvieuse')] = max_mean_season_row['Fin Saison Pluvieuse']
                        station_agg_data[get_metric_label('Durée Saison Pluvieuse Jours')] = max_mean_season_row['Durée Saison Pluvieuse Jours']
                        station_agg_data[get_metric_label('Year Durée Saison Pluvieuse Jours')] = max_mean_season_row.name
                    else:
                        station_agg_data[get_metric_label('Moyenne Saison Pluvieuse')] = np.nan
                        station_agg_data[get_metric_label('Début Saison Pluvieuse')] = pd.NaT
                        station_agg_data[get_metric_label('Fin Saison Pluvieuse')] = pd.NaT
                        station_agg_data[get_metric_label('Durée Saison Pluvieuse Jours')] = np.nan
                        station_agg_data[get_metric_label('Year Durée Saison Pluvieuse Jours')] = np.nan

                df_dry_spell_events_yearly = _calculate_dry_spell_stats_yearly(df_daily_rain_station, df_season_stats_yearly)
                if not df_dry_spell_events_yearly.empty:
                    longest_dry_spell_row = df_dry_spell_events_yearly.loc[df_dry_spell_events_yearly['Durée Sécheresse Définie Jours'].idxmax()] if not df_dry_spell_events_yearly['Durée Sécheresse Définie Jours'].isnull().all() else pd.Series()
                    if not longest_dry_spell_row.empty:
                        station_agg_data[get_metric_label('Durée Sécheresse Définie Jours')] = longest_dry_spell_row['Durée Sécheresse Définie Jours']
                        station_agg_data[get_metric_label('Début Sécheresse Définie')] = longest_dry_spell_row['Début Sécheresse Définie']
                        station_agg_data[get_metric_label('Fin Sécheresse Définie')] = longest_dry_spell_row['Fin Sécheresse Définie']
                        station_agg_data[get_metric_label('Year Durée Sécheresse Définie Jours')] = longest_dry_spell_row.name
                    else:
                        station_agg_data[get_metric_label('Durée Sécheresse Définie Jours')] = np.nan
                        station_agg_data[get_metric_label('Début Sécheresse Définie')] = pd.NaT
                        station_agg_data[get_metric_label('Fin Sécheresse Définie')] = pd.NaT
                        station_agg_data[get_metric_label('Year Durée Sécheresse Définie Jours')] = np.nan
            
            final_stats_list.append(station_agg_data)

        final_stats_df = pd.DataFrame(final_stats_list)

        if final_stats_df.empty:
            return go.Figure().add_annotation(
                x=0.5, y=0.5, text=str(_l("Aucune statistique à afficher pour les variables et stations sélectionnées.")),
                showarrow=False, font=dict(size=16)
            ).update_layout(title=str(_l("Statistiques Annuelles")))

        if var_meta.get('is_rain', False) and variable == 'Rain_mm':
            metrics_to_plot_display_names = [
                get_metric_label('Maximum'), get_metric_label('Minimum'), get_metric_label('Mediane'), get_metric_label('Moyenne'),
                get_metric_label('Cumul Annuel'), get_metric_label('Moyenne Jours Pluvieux'),
                get_metric_label('Moyenne Saison Pluvieuse'), get_metric_label('Durée Saison Pluvieuse Jours'),
                get_metric_label('Durée Sécheresse Définie Jours')
            ]
        else:
            metrics_to_plot_display_names = [
                get_metric_label('Maximum'), get_metric_label('Minimum'), get_metric_label('Mediane'), get_metric_label('Moyenne')
            ]

        metrics_to_plot_filtered = [
            col for col in metrics_to_plot_display_names
            if col in final_stats_df.columns and not final_stats_df[col].isnull().all()
        ]
        
        if not metrics_to_plot_filtered:
             return go.Figure().add_annotation(
                x=0.5, y=0.5, text=str(_l("Aucune statistique à afficher pour les variables et stations sélectionnées.")),
                showarrow=False, font=dict(size=16)
            ).update_layout(title=str(_l("Statistiques Annuelles")))

        num_metrics = len(metrics_to_plot_filtered)
        cols_plot = 2
        rows_plot = (num_metrics + cols_plot - 1) // cols_plot

        height_plot = 400 * rows_plot if rows_plot <= 2 else 300 * rows_plot

        subplot_titles = [f"{var_label} {metric}" for metric in metrics_to_plot_filtered]

        fig = make_subplots(
            rows=rows_plot, cols=cols_plot,
            subplot_titles=subplot_titles,
            vertical_spacing=0.1
        )
        
        unique_stations = sorted(final_stats_df['Station'].unique())
        final_stats_df = final_stats_df.set_index('Station').reindex(unique_stations).reset_index()

        for i, display_metric_col_name in enumerate(metrics_to_plot_filtered):
            row = (i // cols_plot) + 1
            col = (i % cols_plot) + 1
            
            hover_text_list = []
            station_colors_for_trace = []
            
            for _, row_data in final_stats_df.iterrows():
                station_name = row_data['Station']
                station_color = station_colors.get(station_name, '#1f77b4')
                station_colors_for_trace.append(station_color)
                
                value = row_data[display_metric_col_name]
                
                if pd.isna(value):
                    hover_text_list.append("")
                    continue

                hover_text = ""
                associated_year = np.nan

                if display_metric_col_name == get_metric_label('Maximum'):
                    date_val = row_data.get(get_metric_label('Date Max'))
                    date_str = date_val.strftime('%d/%m/%Y') if pd.notna(date_val) else str(_l('Unknown date'))
                    associated_year = row_data.get(get_metric_label('Year Max'))
                    hover_text = str(_l("<b>Maximum</b><br>Value: {:.1f} {}<br>Date: {}{}").format(value, var_unit, date_str, str(_l(" (Observed)")))) # Added (Observed)
                    if pd.notna(associated_year):
                        hover_text += f"<br>{str(_l('Year: {}')).format(int(associated_year))}"
                
                elif display_metric_col_name == get_metric_label('Minimum'):
                    date_val = row_data.get(get_metric_label('Date Min'))
                    date_str = date_val.strftime('%d/%m/%Y') if pd.notna(date_val) else str(_l('Unknown date'))
                    associated_year = row_data.get(get_metric_label('Year Min'))
                    hover_text = str(_l("<b>Minimum</b><br>Value: {:.1f} {}<br>Date: {}{}").format(value, var_unit, date_str, str(_l(" (Observed)")))) # Added (Observed)
                    if pd.notna(associated_year):
                        hover_text += f"<br>{str(_l('Year: {}')).format(int(associated_year))}"
                
                elif display_metric_col_name == get_metric_label('Mediane'):
                    associated_year = row_data.get(get_metric_label('Year Mediane'))
                    hover_text = str(_l("<b>Mediane</b><br>Value: {:.1f} {}")).format(value, var_unit)
                    if pd.notna(associated_year):
                        hover_text += f"<br>{str(_l('Year: {}')).format(int(associated_year))}"

                elif display_metric_col_name == get_metric_label('Moyenne'):
                    associated_year = row_data.get(get_metric_label('Year Moyenne'))
                    hover_text = str(_l("<b>Moyenne</b><br>Value: {:.1f} {}")).format(value, var_unit)
                    if pd.notna(associated_year):
                        hover_text += f"<br>{str(_l('Year: {}')).format(int(associated_year))}"

                elif display_metric_col_name == get_metric_label('Cumul Annuel'):
                    associated_year = row_data.get(get_metric_label('Year Cumul Annuel'))
                    hover_text = str(_l("<b>Cumul Annuel</b><br>Value: {:.1f} {}")).format(value, var_unit)
                    if pd.notna(associated_year):
                        hover_text += f"<br>{str(_l('Year: {}')).format(int(associated_year))}"

                elif display_metric_col_name == get_metric_label('Moyenne Jours Pluvieux'):
                    associated_year = row_data.get(get_metric_label('Year Moyenne Jours Pluvieux'))
                    hover_text = str(_l("<b>Moyenne Jours Pluvieux</b><br>Value: {:.1f} {}")).format(value, var_unit)
                    if pd.notna(associated_year):
                        hover_text += f"<br>{str(_l('Year: {}')).format(int(associated_year))}"
                
                elif display_metric_col_name == get_metric_label('Durée Saison Pluvieuse Jours'):
                    date_debut = row_data.get(get_metric_label('Début Saison Pluvieuse'))
                    date_fin = row_data.get(get_metric_label('Fin Saison Pluvieuse'))
                    date_debut_str = date_debut.strftime('%d/%m/%Y') if pd.notna(date_debut) else str(_l('Unknown date'))
                    date_fin_str = date_fin.strftime('%d/%m/%Y') if pd.notna(date_fin) else str(_l('Unknown date'))
                    associated_year = row_data.get(get_metric_label('Year Durée Saison Pluvieuse Jours'))
                    
                    hover_text = str(_l("<b>Durée Saison Pluvieuse Jours</b><br>"))
                    if pd.notna(date_debut) and pd.notna(date_fin):
                        hover_text += str(_l("From {} to {}")).format(date_debut_str, date_fin_str) + "<br>"
                    hover_text += str(_l("Duration: {} days")).format(int(value))
                    if pd.notna(associated_year):
                        hover_text += f"<br>{str(_l('Year: {}')).format(int(associated_year))}"

                elif display_metric_col_name == get_metric_label('Durée Sécheresse Définie Jours'):
                    date_debut = row_data.get(get_metric_label('Début Sécheresse Définie'))
                    date_fin = row_data.get(get_metric_label('Fin Sécheresse Définie'))
                    date_debut_str = date_debut.strftime('%d/%m/%Y') if pd.notna(date_debut) else str(_l('Unknown date'))
                    date_fin_str = date_fin.strftime('%d/%m/%Y') if pd.notna(date_fin) else str(_l('Unknown date'))
                    associated_year = row_data.get(get_metric_label('Year Durée Sécheresse Définie Jours'))
                    
                    hover_text = str(_l("<b>Durée Sécheresse Définie Jours</b><br>"))
                    if pd.notna(date_debut) and pd.notna(date_fin):
                        hover_text += str(_l("From {} to {}")).format(date_debut_str, date_fin_str) + "<br>"
                    hover_text += str(_l("Duration: {} days")).format(int(value))
                    if pd.notna(associated_year):
                        hover_text += f"<br>{str(_l('Year: {}')).format(int(associated_year))}"
                
                else:
                    hover_text = str(_l("<b>{}</b><br>Value: {:.1f} {}")).format(display_metric_col_name, value, var_unit)
                    year_col = f"Year {display_metric_col_name}"
                    if year_col in row_data and pd.notna(row_data[year_col]):
                        hover_text += f"<br>{str(_l('Year: {}')).format(int(row_data[year_col]))}"


                hover_text_list.append(hover_text)

            fig.add_trace(
                go.Bar(
                    x=final_stats_df[display_metric_col_name],
                    y=final_stats_df['Station'],
                    orientation='h',
                    marker_color=station_colors_for_trace,
                    name=display_metric_col_name,
                    hovertext=hover_text_list,
                    hoverinfo='text',
                    showlegend=False,
                    textposition='none'
                ),
                row=row,
                col=col
            )

            xaxis_title = str(_l("Jours")) if (get_metric_label("Durée") in display_metric_col_name or "Duration" in display_metric_col_name) else f"{var_label} ({var_unit})"
            fig.update_xaxes(
                title_text=xaxis_title,
                showgrid=False,
                row=row, col=col
            )
            
            fig.update_yaxes(
                title_text=str(_l("Station")),
                showgrid=False,
                row=row, col=col,
                categoryorder='array',
                categoryarray=unique_stations,
                automargin=True
            )

        for station_name in unique_stations:
            fig.add_trace(
                go.Bar(
                    x=[0], y=[station_name],
                    marker_color=station_colors.get(station_name, '#1f77b4'),
                    name=station_name,
                    showlegend=True,
                    visible='legendonly'
                )
            )

        fig.update_layout(
            height=height_plot,
            title_text=str(get_metric_label("Statistics of {} by Station")).format(var_label),
            showlegend=True,
            legend_title_text=str(_l("Station")),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.15,
                xanchor="center",
                x=0.5
            ),
            hovermode='closest',
            plot_bgcolor='white',
            paper_bgcolor='white',
            margin=dict(b=150)
        )
        
        return fig
        
    except Exception as e:
        print(f"Erreur dans generate_daily_stats_plot_plotly: {str(e)}")
        traceback.print_exc()
        return go.Figure().add_annotation(
            x=0.5, y=0.5, text=str(_l("Une erreur est survenue lors de la génération du graphique: {}")).format(str(e)),
            showarrow=False, font=dict(size=16)
        ).update_layout(title=str(_l("Erreur de visualisation")))