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
from config import METADATA_VARIABLES, PALETTE_DEFAUT, DATA_LIMITS, PALETTE_COULEUR

# Importation pour Flask-Babel
from flask_babel import lazy_gettext as _l

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
        df_copy['Rain_mm'] = df_copy['Rain_01_mm'].fillna(df_copy['Rain_02_mm'])
    elif 'Rain_01_mm' in df_copy.columns:
        df_copy['Rain_mm'] = df_copy['Rain_01_mm']
    elif 'Rain_02_mm' in df_copy.columns:
        df_copy['Rain_mm'] = df_copy['Rain_02_mm']
    else:
        # Traduction de l'avertissement
        df_copy['Rain_mm'] = np.nan
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

def interpolation(df: pd.DataFrame, limits: dict, df_gps: pd.DataFrame) -> pd.DataFrame:
    """
    Effectue toutes les interpolations météorologiques en une seule passe.
    Cette fonction DOIT recevoir un DataFrame avec un DatetimeIndex.
    Il doit également contenir une colonne 'Station'.

    Args:
        df (pd.DataFrame): Le DataFrame d'entrée avec DatetimeIndex et colonne 'Station'.
        limits (dict): Dictionnaire définissant les limites de valeurs pour chaque variable.
        df_gps (pd.DataFrame): Le DataFrame contenant les informations de station
                               (colonnes 'Station', 'Lat', 'Long', 'Timezone').

    Returns:
        pd.DataFrame: Le DataFrame original avec les données interpolées,
                      la colonne 'Is_Daylight' calculée, la durée du jour, et un DatetimeIndex.
    """
    df_processed = df.copy()

    if not isinstance(df_processed.index, pd.DatetimeIndex):
        # Traduction de l'erreur
        raise TypeError(_l("Le DataFrame d'entrée pour l'interpolation DOIT avoir un DatetimeIndex."))
    
    initial_rows = len(df_processed)
    df_processed = df_processed[df_processed.index.notna()]
    if len(df_processed) == 0:
        # Traduction de l'erreur
        raise ValueError(_l("Après nettoyage des index temporels manquants, le DataFrame est vide. Impossible de procéder à l'interpolation."))
    if initial_rows - len(df_processed) > 0:
        # Traduction de l'avertissement
        warnings.warn(_l("Suppression de %s lignes avec index Datetime manquant ou invalide dans l'interpolation.") % (initial_rows - len(df_processed)))
    
    # Traduction des messages de DEBUG
    print(_l("DEBUG (interpolation): Type de l'index du DataFrame initial: %s") % type(df_processed.index))
    print(_l("DEBUG (interpolation): Premières 5 valeurs de l'index après nettoyage des NaT: %s") % (df_processed.index[:5].tolist() if not df_processed.empty else 'DataFrame vide'))

    required_gps_cols = ['Station', 'Lat', 'Long', 'Timezone']
    if not all(col in df_gps.columns for col in required_gps_cols):
        # Traduction de l'erreur
        raise ValueError(
            _l("df_gps doit contenir les colonnes %s. Colonnes actuelles dans df_gps : %s") % \
            (required_gps_cols, df_gps.columns.tolist())
        )

    if not df_gps['Station'].is_unique:
        # Traduction des avertissements
        print(_l("Avertissement: La colonne 'Station' dans df_gps contient des noms de station dupliqués."))
        print(_l("Ceci peut entraîner des comportements inattendus ou des stations non reconnues."))
        df_gps_unique = df_gps.drop_duplicates(subset=['Station'], keep='first').copy()
        print(_l("Suppression de %s doublons dans df_gps (en gardant la première occurrence).") % (len(df_gps) - len(df_gps_unique)))
    else:
        df_gps_unique = df_gps.copy()

    gps_info_dict = df_gps_unique.set_index('Station')[['Lat', 'Long', 'Timezone']].to_dict('index')

    numerical_cols = ['Air_Temp_Deg_C', 'Rel_H_%', 'BP_mbar_Avg',
                      'Rain_01_mm', 'Rain_02_mm', 'Rain_mm', 'Wind_Sp_m/sec',
                      'Solar_R_W/m^2', 'Wind_Dir_Deg']
    for col in numerical_cols:
        if col in df_processed.columns:
            df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')
            if col in limits:
                min_val = limits[col]['min']
                max_val = limits[col]['max']
                initial_nan_count = df_processed[col].isna().sum()
                if min_val is not None:
                    df_processed.loc[df_processed[col] < min_val, col] = np.nan
                if max_val is not None:
                    df_processed.loc[df_processed[col] > max_val, col] = np.nan
                
                new_nan_count = df_processed[col].isna().sum()
                if new_nan_count > initial_nan_count:
                    # Traduction de l'avertissement
                    warnings.warn(_l("Remplacement de %s valeurs hors limites dans '%s' par NaN.") % (new_nan_count - initial_nan_count, col))
            
            # Traduction du message de DEBUG
            print(_l("DEBUG (interpolation/variable): Interpolation de '%s'. Type de l'index de df_final: %s") % (col, type(df_processed.index)))
            
            if isinstance(df_processed.index, pd.DatetimeIndex):
                df_processed[col] = df_processed[col].interpolate(method='time', limit_direction='both')
            else:
                # Traduction de l'avertissement
                print(_l("Avertissement (interpolation/variable): L'index n'est pas un DatetimeIndex pour l'interpolation de '%s'. Utilisation de la méthode 'linear'.") % col)
                df_processed[col] = df_processed[col].interpolate(method='linear', limit_direction='both')
            df_processed[col] = df_processed[col].bfill().ffill()

    df_processed_parts = []

    for station_name, group in df_processed.groupby('Station'):
        group_copy = group.copy()
        # Traduction du message de DEBUG
        print(_l("DEBUG (interpolation/groupby): Début du traitement du groupe '%s'.") % station_name)
        
        if group_copy.index.tz is None:
            group_copy.index = group_copy.index.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
        elif group_copy.index.tz != pytz.utc:
            group_copy.index = group_copy.index.tz_convert('UTC')
        # Traduction du message de DEBUG
        print(_l("DEBUG (interpolation/groupby): Index Datetime pour '%s' STANDARDISÉ à UTC. Dtype: %s") % (station_name, group_copy.index.dtype))
        
        group_copy = group_copy[group_copy.index.notna()]
        if group_copy.empty:
            # Traduction de l'avertissement
            warnings.warn(_l("Le groupe '%s' est vide après nettoyage de l'index Datetime. Il sera ignoré.") % station_name)
            continue

        apply_fixed_daylight = True
        gps_data = gps_info_dict.get(station_name)
        if gps_data and pd.notna(gps_data.get('Lat')) and pd.notna(gps_data.get('Long')) and pd.notna(gps_data.get('Timezone')):
            lat = gps_data['Lat']
            long = gps_data['Long']
            timezone_str = gps_data['Timezone']

            try:
                local_tz = pytz.timezone(timezone_str)
                index_for_astral_local = group_copy.index.tz_convert(local_tz)

                daily_sun_info = {}
                unique_dates_ts_local = index_for_astral_local.normalize().drop_duplicates()

                if unique_dates_ts_local.empty:
                    # Traduction de l'erreur
                    raise ValueError(_l("Aucune date unique trouvée pour le calcul Astral."))
                
                for ts_local_aware in unique_dates_ts_local:
                    loc = LocationInfo(station_name, "Site", timezone_str, lat, long)
                    naive_date_for_astral = ts_local_aware.to_pydatetime().date()
                    s = sun.sun(loc.observer, date=naive_date_for_astral) 
                    daily_sun_info[naive_date_for_astral] = {
                        'sunrise': s['sunrise'],
                        'sunset': s['sunset']
                    }

                naive_unique_dates_for_index = [ts.date() for ts in unique_dates_ts_local]
                temp_df_sun_index = pd.Index(naive_unique_dates_for_index, name='Date_Local_Naive')
                temp_df_sun = pd.DataFrame(index=temp_df_sun_index)
                
                # Traduction des messages de DEBUG
                print(_l("DEBUG (astral_calc): unique_dates_ts_local type: %s") % type(unique_dates_ts_local))
                print(_l("DEBUG (astral_calc): naive_unique_dates_for_index type: %s") % type(naive_unique_dates_for_index))
                print(_l("DEBUG (astral_calc): temp_df_sun_index type: %s") % type(temp_df_sun_index))
                if not temp_df_sun.empty:
                    print(_l("DEBUG (astral_calc): First element of temp_df_sun.index: %s") % temp_df_sun.index[0])
                    print(_l("DEBUG (astral_calc): Type of first element of temp_df_sun.index: %s") % type(temp_df_sun.index[0]))

                temp_df_sun['sunrise_time_local'] = [daily_sun_info.get(date, {}).get('sunrise') for date in temp_df_sun.index]
                temp_df_sun['sunset_time_local'] = [daily_sun_info.get(date, {}).get('sunset') for date in temp_df_sun.index]

                group_copy_reset = group_copy.reset_index()
                group_copy_reset['Date_Local_Naive'] = group_copy_reset['Datetime'].dt.tz_convert(local_tz).dt.date

                group_copy_reset = pd.merge(group_copy_reset, temp_df_sun, on='Date_Local_Naive', how='left')

                group_copy_reset['sunrise_time_utc'] = group_copy_reset['sunrise_time_local'].dt.tz_convert('UTC')
                group_copy_reset['sunset_time_utc'] = group_copy_reset['sunset_time_local'].dt.tz_convert('UTC')

                group_copy_reset.loc[:, 'Is_Daylight'] = (group_copy_reset['Datetime'] >= group_copy_reset['sunrise_time_utc']) & \
                                                          (group_copy_reset['Datetime'] < group_copy_reset['sunset_time_utc'])

                daylight_timedelta_local = group_copy_reset['sunset_time_local'] - group_copy_reset['sunrise_time_local']
                
                def format_timedelta_to_hms(td):
                    if pd.isna(td):
                        return np.nan
                    total_seconds = int(td.total_seconds())
                    hours = total_seconds // 3600
                    minutes = (total_seconds % 3600) // 60
                    seconds = total_seconds % 60
                    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

                group_copy_reset.loc[:, 'Daylight_Duration'] = daylight_timedelta_local.apply(format_timedelta_to_hms)

                group_copy = group_copy_reset.set_index('Datetime')
                group_copy = group_copy.drop(columns=['Date_Local_Naive', 'sunrise_time_local', 'sunset_time_local', 'sunrise_time_utc', 'sunset_time_utc'], errors='ignore')

                # Traduction du message
                print(_l("Lever et coucher du soleil calculés pour %s.") % station_name)
                apply_fixed_daylight = False

            except Exception as e:
                # Traduction des messages d'erreur et d'avertissement
                print(_l("Erreur lors du calcul du lever/coucher du soleil avec Astral pour %s: %s.") % (station_name, e))
                traceback.print_exc()
                warnings.warn(_l("Calcul Astral impossible pour '%s'. Utilisation de l'indicateur jour/nuit fixe.") % station_name)
                apply_fixed_daylight = True
        else:
            # Traduction de l'avertissement
            print(_l("Avertissement: Coordonnées ou Fuseau horaire manquants/invalides pour le site '%s' dans df_gps. Utilisation de l'indicateur jour/nuit fixe.") % station_name)
            apply_fixed_daylight = True

        if apply_fixed_daylight:
            group_copy.loc[:, 'Is_Daylight'] = (group_copy.index.hour >= 7) & (group_copy.index.hour <= 18)
            group_copy.loc[:, 'Daylight_Duration'] = "11:00:00"
            # Traduction du message
            print(_l("Utilisation de l'indicateur jour/nuit fixe (7h-18h) pour %s.") % station_name)

        df_processed_parts.append(group_copy)

    if not df_processed_parts:
        # Traduction de l'erreur
        raise ValueError(_l("Aucune partie de DataFrame n'a pu être traitée après le regroupement par station."))

    df_final = pd.concat(df_processed_parts).sort_index()
    df_final.index.name = 'Datetime' 
    # Traduction des messages de DEBUG
    print(_l("DEBUG (interpolation/concat): Index du DataFrame final après concaténation et tri: %s") % type(df_final.index))
    print(_l("DEBUG (interpolation/concat): Colonnes du DataFrame final après concaténation: %s") % df_final.columns.tolist())

    cols_to_drop_after_process = ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
    df_final = df_final.drop(columns=cols_to_drop_after_process, errors='ignore')

    if 'Rain_mm' not in df_final.columns or df_final['Rain_mm'].isnull().all():
        if 'Rain_01_mm' in df_final.columns and 'Rain_02_mm' in df_final.columns:
            df_final = create_rain_mm(df_final)
            # Traduction de l'avertissement
            warnings.warn(_l("Colonne Rain_mm créée à partir des deux capteurs."))
        else:
            # Traduction de l'avertissement
            warnings.warn(_l("Rain_mm manquant et impossible à créer (capteurs pluie incomplets)."))
            if 'Rain_mm' not in df_final.columns:
                df_final['Rain_mm'] = np.nan

    standard_vars = ['Air_Temp_Deg_C', 'Rel_H_%', 'BP_mbar_Avg',
                      'Rain_01_mm', 'Rain_02_mm', 'Rain_mm', 'Wind_Sp_m/sec',
                      'Solar_R_W/m^2', 'Wind_Dir_Deg']

    for var in standard_vars:
        if var in df_final.columns:
            df_final[var] = pd.to_numeric(df_final[var], errors='coerce')
            if var in limits:
                min_val = limits[var]['min']
                max_val = limits[var]['max']
                initial_nan_count = df_final[var].isna().sum()
                if min_val is not None:
                    df_final.loc[df_final[var] < min_val, var] = np.nan
                if max_val is not None:
                    df_final.loc[df_final[var] > max_val, var] = np.nan
                
                new_nan_count = df_final[var].isna().sum()
                if new_nan_count > initial_nan_count:
                    # Traduction de l'avertissement
                    warnings.warn(_l("Remplacement de %s valeurs hors limites dans '%s' par NaN.") % (new_nan_count - initial_nan_count, var))
            
            # Traduction du message de DEBUG
            print(_l("DEBUG (interpolation/variable): Interpolation de '%s'. Type de l'index de df_final: %s") % (var, type(df_final.index)))
            
            if isinstance(df_final.index, pd.DatetimeIndex):
                df_final[var] = df_final[var].interpolate(method='time', limit_direction='both')
            else:
                # Traduction de l'avertissement
                print(_l("Avertissement (interpolation/variable): L'index n'est pas un DatetimeIndex pour l'interpolation de '%s'. Utilisation de la méthode 'linear'.") % var)
                df_final[var] = df_final[var].interpolate(method='linear', limit_direction='both')
            df_final[var] = df_final[var].bfill().ffill()

    if 'Solar_R_W/m^2' in df_final.columns:
        df_final['Solar_R_W/m^2'] = pd.to_numeric(df_final['Solar_R_W/m^2'], errors='coerce')

        if 'Solar_R_W/m^2' in limits:
            min_val = limits['Solar_R_W/m^2']['min']
            max_val = limits['Solar_R_W/m^2']['max']
            initial_nan_count = df_final['Solar_R_W/m^2'].isna().sum()
            df_final.loc[(df_final['Solar_R_W/m^2'] < min_val) | (df_final['Solar_R_W/m^2'] > max_val), 'Solar_R_W/m^2'] = np.nan
            if df_final['Solar_R_W/m^2'].isna().sum() > initial_nan_count:
                # Traduction de l'avertissement
                warnings.warn(_l("Remplacement de %s valeurs hors limites dans 'Solar_R_W/m^2' par NaN.") % (df_final['Solar_R_W/m^2'].isna().sum() - initial_nan_count))

        if 'Is_Daylight' in df_final.columns:
            df_final.loc[~df_final['Is_Daylight'] & (df_final['Solar_R_W/m^2'] > 0), 'Solar_R_W/m^2'] = 0

            if 'Rain_mm' in df_final.columns:
                cond_suspect_zeros = (df_final['Is_Daylight']) & (df_final['Solar_R_W/m^2'] == 0) & (df_final['Rain_mm'] == 0)
            else:
                cond_suspect_zeros = (df_final['Is_Daylight']) & (df_final['Solar_R_W/m^2'] == 0)
                # Traduction de l'avertissement
                warnings.warn(_l("Rain_mm manquant. Tous les 0 de radiation solaire pendant le jour sont traités comme suspects."))
            df_final.loc[cond_suspect_zeros, 'Solar_R_W/m^2'] = np.nan

            # Traduction du message de DEBUG
            print(_l("DEBUG (interpolation/solaire): Interpolation de 'Solar_R_W/m^2' (conditionnel). Type de l'index de df_final: %s") % type(df_final.index))

            is_day = df_final['Is_Daylight']
            if isinstance(df_final.index, pd.DatetimeIndex):
                df_final.loc[is_day, 'Solar_R_W/m^2'] = df_final.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='time', limit_direction='both')
            else:
                # Traduction de l'avertissement
                print(_l("Avertissement (interpolation/solaire): L'index n'est pas un DatetimeIndex pour l'interpolation de 'Solar_R_W/m^2'. Utilisation de la méthode 'linear'.") % type(df_final.index))
                df_final.loc[is_day, 'Solar_R_W/m^2'] = df_final.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='linear', limit_direction='both')

            df_final.loc[is_day, 'Solar_R_W/m^2'] = df_final.loc[is_day, 'Solar_R_W/m^2'].bfill().ffill()

            df_final.loc[~is_day & df_final['Solar_R_W/m^2'].isna(), 'Solar_R_W/m^2'] = 0
            # Traduction de l'avertissement
            warnings.warn(_l("Radiation solaire interpolée avec succès."))
        else:
            # Traduction de l'avertissement
            warnings.warn(_l("Colonne 'Is_Daylight' manquante. Radiation solaire interpolée standard."))
            if isinstance(df_final.index, pd.DatetimeIndex):
                 df_final['Solar_R_W/m^2'] = df_final['Solar_R_W/m^2'].interpolate(method='time', limit_direction='both').bfill().ffill()
            else:
                 df_final['Solar_R_W/m^2'] = df_final['Solar_R_W/m^2'].interpolate(method='linear', limit_direction='both').bfill().ffill()

    # Traduction des avertissements
    warnings.warn(_l("Vérification des valeurs manquantes après interpolation:"))
    missing_after_interp = df_final.isna().sum()
    columns_with_missing = missing_after_interp[missing_after_interp > 0]
    if not columns_with_missing.empty:
        warnings.warn(_l("Valeurs manquantes persistantes:\n%s") % columns_with_missing)
    else:
        warnings.warn(_l("Aucune valeur manquante après l'interpolation."))

    return df_final

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

def gestion_doublons(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gère les doublons dans le DataFrame en se basant sur les colonnes 'Station' et 'Datetime'.
    Conserve la première occurrence en cas de doublon.

    Args:
        df (pd.DataFrame): Le DataFrame d'entrée.

    Returns:
        pd.DataFrame: Le DataFrame sans doublons.
    """
    if 'Station' in df.columns and 'Datetime' in df.columns:
        initial_rows = len(df)
        df_cleaned = df.drop_duplicates(subset=['Station', 'Datetime'], keep='first')
        if len(df_cleaned) < initial_rows:
            # Traduction de l'avertissement
            warnings.warn(_l("Suppression de %s doublons basés sur 'Station' et 'Datetime'.") % (initial_rows - len(df_cleaned)))
        return df_cleaned
    else:
        # Traduction de l'avertissement
        warnings.warn(_l("Colonnes 'Station' ou 'Datetime' manquantes pour la gestion des doublons. Le DataFrame n'a pas été modifié."))
        return df

def traiter_outliers_meteo(df: pd.DataFrame, limits: dict) -> pd.DataFrame:
    """
    Remplace les valeurs aberrantes par NaN pour toutes les variables météorologiques spécifiées.

    Args:
        df (pd.DataFrame): DataFrame d'entrée avec DatetimeIndex et colonne 'Station'.
        limits (dict): Dictionnaire avec les limites min/max pour chaque variable.

    Returns:
        pd.DataFrame: DataFrame avec les valeurs aberrantes remplacées par NaN.
    """
    df_processed = df.copy()

    if not isinstance(df_processed.index, pd.DatetimeIndex):
        # Traduction de l'avertissement
        warnings.warn(_l("L'index n'est pas un DatetimeIndex dans traiter_outliers_meteo. Tentative de conversion."))
        try:
            df_processed.index = pd.to_datetime(df_processed.index, errors='coerce')
            df_processed = df_processed[df_processed.index.notna()]
            if df_processed.empty:
                # Traduction de l'erreur
                raise ValueError(_l("DataFrame vide après nettoyage des dates invalides dans traiter_outliers_meteo."))
        except Exception as e:
            # Traduction de l'erreur
            raise TypeError(_l("Impossible de garantir un DatetimeIndex pour traiter_outliers_meteo: %s") % e)

    for var, vals in limits.items():
        if var in df_processed.columns:
            min_val = vals.get('min')
            max_val = vals.get('max')
            if min_val is not None or max_val is not None:
                initial_nan_count = df_processed[var].isna().sum()
                if min_val is not None:
                    df_processed.loc[df_processed[var] < min_val, var] = np.nan
                if max_val is not None:
                    df_processed.loc[df_processed[var] > max_val, var] = np.nan
                
                new_nan_count = df_processed[var].isna().sum()
                if new_nan_count > initial_nan_count:
                    # Traduction de l'avertissement
                    warnings.warn(_l("Remplacement de %s valeurs hors limites dans '%s' par NaN.") % (new_nan_count - initial_nan_count, var))
    return df_processed

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

            main_block_id = max(valid_blocks, key=lambda k: (valid_blocks[k].index.max() - valid_blocks[k].index.min()).days)
            main_season_df = valid_blocks[main_block_id]

            debut_saison = main_season_df.index.min()
            fin_saison = main_season_df.index.max()
            total_days = (fin_saison - debut_saison).days + 1
            moyenne_saison = main_season_df['Rain_mm_sum'].sum() / total_days if total_days > 0 else 0

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

def generate_variable_summary_plots_for_web(df: pd.DataFrame, station: str, variable: str, metadata: dict, palette: dict) -> plt.Figure:
    """
    Génère un graphique Matplotlib/Seaborn pour les statistiques agrégées d'une variable spécifique
    pour une station donnée.
    """
    df_station = df[df['Station'] == station].copy()

    if df_station.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        # Traduction du message d'erreur
        ax.text(0.5, 0.5, _l("Aucune donnée pour la station '%s'.") % station, horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
        ax.axis('off')
        return fig

    if isinstance(df_station.index, pd.DatetimeIndex):
        df_station = df_station.reset_index()
    
    df_station['Datetime'] = pd.to_datetime(df_station['Datetime'], errors='coerce')
    df_station = df_station.dropna(subset=['Datetime', 'Station'])
    df_station = df_station.set_index('Datetime').sort_index()

    if df_station.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        # Traduction du message d'erreur
        ax.text(0.5, 0.5, _l("DataFrame vide après nettoyage des dates pour la station '%s'.") % station, horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
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
                            # Traduire les clés du dictionnaire si elles sont utilisées pour l'affichage
                            _l('Duree'): temp_duree,
                            _l('Debut'): temp_debut,
                            _l('Fin'): current_rain_date - timedelta(days=1)
                        })
            
            if temp_dry_spells:
                df_temp_dry = pd.DataFrame(temp_dry_spells)
                idx_max_dry = df_temp_dry[_l('Duree')].idxmax()
                longest_dry_spell = df_temp_dry.loc[idx_max_dry, _l('Duree')]
                debut_secheresse = df_temp_dry.loc[idx_max_dry, _l('Debut')]
                fin_secheresse = df_temp_dry.loc[idx_max_dry, _l('Fin')]

        rain_data_for_stats = df_station[variable].dropna()
        
        # Traduire les clés du dictionnaire pour la cohérence
        stats_for_plot[_l('Maximum')] = rain_data_for_stats.max() if not rain_data_for_stats.empty else np.nan
        stats_for_plot[_l('Minimum')] = rain_data_for_stats.min() if not rain_data_for_stats.empty else np.nan
        stats_for_plot[_l('Médiane')] = rain_data_for_stats.median() if not rain_data_for_stats.empty else np.nan
        stats_for_plot[_l('Cumul_Annuel')] = df_station[variable].sum()
        
        rainy_days_data = df_station[df_station[variable] > 0][variable].dropna()
        stats_for_plot[_l('Moyenne_Jours_Pluvieux')] = rainy_days_data.mean() if not rainy_days_data.empty else np.nan
        
        stats_for_plot[_l('Moyenne_Saison_Pluvieuse')] = s_moyenne_saison
        stats_for_plot[_l('Duree_Saison_Pluvieuse_Jours')] = s_duree_saison
        stats_for_plot[_l('Debut_Saison_Pluvieuse')] = s_debut_saison
        stats_for_plot[_l('Fin_Saison_Pluvieuse')] = s_fin_saison
        
        stats_for_plot[_l('Duree_Secheresse_Definie_Jours')] = longest_dry_spell
        stats_for_plot[_l('Debut_Secheresse_Definie')] = debut_secheresse
        stats_for_plot[_l('Fin_Secheresse_Definie')] = fin_secheresse

        max_date_idx = df_station[variable].idxmax() if not df_station[variable].empty else pd.NaT
        min_date_idx = df_station[variable].idxmin() if not df_station[variable].empty else pd.NaT
        stats_for_plot[_l('Date_Maximum')] = max_date_idx if pd.notna(max_date_idx) else pd.NaT
        stats_for_plot[_l('Date_Minimum')] = min_date_idx if pd.notna(min_date_idx) else pd.NaT
        
        metrics_to_plot = [
            _l('Maximum'), _l('Minimum'), _l('Cumul_Annuel'), _l('Médiane'),
            _l('Moyenne_Jours_Pluvieux'), _l('Moyenne_Saison_Pluvieuse'),
            _l('Duree_Saison_Pluvieuse_Jours'), _l('Duree_Secheresse_Definie_Jours')
        ]
        nrows, ncols = 4, 2
        figsize = (18, 16)
        
    else:
        current_var_data = df_station[variable].dropna()
        if variable == 'Solar_R_W/m^2':
            current_var_data = df_station.loc[df_station['Is_Daylight'], variable].dropna()

        if current_var_data.empty:
            fig, ax = plt.subplots(figsize=(10, 6))
            # Traduction du message d'erreur
            ax.text(0.5, 0.5, _l("Aucune donnée valide pour la variable %s à %s.") % (var_meta['Nom'], station), horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
            ax.axis('off')
            return fig

        # Traduire les clés du dictionnaire pour la cohérence
        stats_for_plot[_l('Maximum')] = current_var_data.max()
        stats_for_plot[_l('Minimum')] = current_var_data.min()
        stats_for_plot[_l('Médiane')] = current_var_data.median()
        stats_for_plot[_l('Moyenne')] = current_var_data.mean()

        max_idx = current_var_data.idxmax() if not current_var_data.empty else pd.NaT
        min_idx = current_var_data.idxmin() if not current_var_data.empty else pd.NaT

        # Traduire les clés du dictionnaire pour la cohérence
        stats_for_plot[_l('Date_Maximum')] = max_idx if pd.notna(max_idx) else pd.NaT
        stats_for_plot[_l('Date_Minimum')] = min_idx if pd.notna(min_idx) else pd.NaT

        metrics_to_plot = [_l('Maximum'), _l('Minimum'), _l('Moyenne'), _l('Médiane')]
        nrows, ncols = 2, 2
        figsize = (18, 12)

    if not stats_for_plot:
        fig, ax = plt.subplots(figsize=(10, 6))
        # Traduction du message d'erreur
        ax.text(0.5, 0.5, _l("Impossible de calculer des statistiques pour la variable '%s' à la station '%s' (données manquantes ou non numériques).") % (variable, station), horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
        ax.axis('off')
        return fig

    fig, axes = plt.subplots(nrows, ncols, figsize=figsize)
    plt.subplots_adjust(hspace=0.6, wspace=0.4)
    axes = axes.flatten()

    # Traduction du titre du graphique
    fig.suptitle(_l('Statistiques de %s pour la station %s') % (var_meta["Nom"], station), fontsize=16, y=0.98)

    for i, metric in enumerate(metrics_to_plot):
        ax = axes[i]
        value = stats_for_plot.get(metric)
        if pd.isna(value):
            # Traduction du message
            ax.text(0.5, 0.5, _l("Données non disponibles"), horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=12, color='gray')
            ax.axis('off')
            continue

        color = palette.get(metric.replace(' ', '_'), '#cccccc') # Use original metric string for palette lookup

        plot_data_bar = pd.DataFrame({'Metric': [str(metric).replace('_', ' ')], 'Value': [value]}) # Convert metric to string for plotting
        sns.barplot(x='Metric', y='Value', data=plot_data_bar, ax=ax, color=color, edgecolor='none')

        text = ""
        # IMPORTANT: Ici, les clés de 'metric' sont déjà les versions traduites (_l()) de Python.
        # Il faut donc comparer aux _l() ou aux chaînes directes FR des msgid.
        if metric == _l('Duree_Saison_Pluvieuse_Jours') or metric == _l('Duree_Secheresse_Definie_Jours'):
            # Utilise les clés de stats_for_plot qui sont déjà traduites ou les mêmes que les msgid
            # On prend la partie non-Jours pour la clé de date
            date_key_part = str(metric).replace('Jours', '').strip()
            start_date_key = _l('Debut_Saison_Pluvieuse') if 'Saison' in date_key_part else _l('Debut_Secheresse_Definie')
            end_date_key = _l('Fin_Saison_Pluvieuse') if 'Saison' in date_key_part else _l('Fin_Secheresse_Definie')

            start_date = stats_for_plot.get(start_date_key)
            end_date = stats_for_plot.get(end_date_key)
            
            date_info = ""
            if pd.notna(start_date) and pd.notna(end_date):
                date_info = _l("\ndu %s au %s") % (start_date.strftime('%d/%m/%Y'), end_date.strftime('%d/%m/%Y')) # Traduit
            text = _l("%s j%s") % (int(value), date_info) # Traduit
        elif metric in [_l('Maximum'), _l('Minimum'), _l('Cumul_Annuel'), _l('Moyenne_Jours_Pluvieux'), _l('Moyenne_Saison_Pluvieuse'), _l('Médiane'), _l('Moyenne')]:
            unit = var_meta['Unite']
            date_str = ''
            if metric == _l('Maximum') and _l('Date_Maximum') in stats_for_plot and pd.notna(stats_for_plot[_l('Date_Maximum')]):
                date_str = _l("\n(%s)") % stats_for_plot[_l('Date_Maximum')].strftime('%d/%m/%Y') # Traduit
            elif metric == _l('Minimum') and _l('Date_Minimum') in stats_for_plot and pd.notna(stats_for_plot[_l('Date_Minimum')]):
                date_str = _l("\n(%s)") % stats_for_plot[_l('Date_Minimum')].strftime('%d/%m/%Y') # Traduit
            
            text = _l("%.1f %s%s") % (value, unit, date_str) # Traduit
        else:
            text = _l("%.1f %s") % (value, var_meta['Unite']) # Traduit

        ax.text(0.5, value, text, ha='center', va='bottom', fontsize=9, color='black',
                bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=1))
        
        # Traduire le titre de l'axe
        ax.set_title(_l("%s %s") % (var_meta['Nom'], str(metric).replace('_', ' ')), fontsize=11)
        ax.set_xlabel("")
        # Traduire l'étiquette de l'axe Y
        ax.set_ylabel(_l("Valeur (%s)") % var_meta['Unite'], fontsize=10)
        ax.tick_params(axis='x', rotation=0)
        ax.set_xticklabels([])

    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.tight_layout(rect=[0, 0, 1, 0.96])

    return fig


def generate_stats_plots(df: pd.DataFrame, station: str, variable: str, metadata: dict, palette: dict) -> plt.Figure:
    """
    Génère un graphique Matplotlib/Seaborn pour les statistiques agrégées d'une variable spécifique
    pour une station donnée. Cette fonction calcule la durée de la saison pluvieuse et de la sécheresse
    pour chaque année, puis identifie et affiche la durée la plus longue (le record)
    sur l'ensemble des années pour cette station, avec ses dates de début et de fin.
    """
    
    # Étape 1 : Filtrer le DataFrame global pour une SEULE STATION spécifique.
    df_station = df[df['Station'] == station].copy()

    if df_station.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, _l("Aucune donnée pour la station '%s'.") % station, horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
        ax.axis('off')
        return fig

    # S'assure que 'Datetime' est l'index et est de type DatetimeIndex
    if isinstance(df_station.index, pd.DatetimeIndex):
        # Si Datetime est déjà l'index, s'assurer qu'il est propre.
        df_station = df_station.sort_index()
    else:
        df_station['Datetime'] = pd.to_datetime(df_station['Datetime'], errors='coerce')
        df_station = df_station.dropna(subset=['Datetime', 'Station'])
        df_station = df_station.set_index('Datetime').sort_index()

    if df_station.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, _l("DataFrame vide après nettoyage des dates pour la station '%s'.") % station, horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
        ax.axis('off')
        return fig

    if 'Is_Daylight' not in df_station.columns:
        df_station['Is_Daylight'] = (df_station.index.hour >= 7) & (df_station.index.hour <= 18)

    var_meta = metadata.get(variable, {'Nom': variable, 'Unite': ''})

    stats_for_plot = {}
    metrics_to_plot = []
    
    if var_meta.get('is_rain', False) and variable == 'Rain_mm':
        # Agrégation des données de pluie par jour : la somme quotidienne est cruciale.
        df_daily_rain = df_station.groupby(pd.Grouper(freq='D'))['Rain_mm'].sum().reset_index()
        df_daily_rain = df_daily_rain.rename(columns={'Rain_mm': 'Rain_mm_sum'})
        df_daily_rain['Datetime'] = pd.to_datetime(df_daily_rain['Datetime'])
        df_daily_rain = df_daily_rain.set_index('Datetime').sort_index()
        
        # S'assure que toutes les dates de la plage sont présentes, même les jours sans pluie (valeur 0).
        if not df_daily_rain.empty:
            full_date_range = pd.date_range(start=df_daily_rain.index.min(), end=df_daily_rain.index.max(), freq='D')
            df_daily_rain = df_daily_rain.reindex(full_date_range, fill_value=0)
        else:
            unique_years = [] # Pas d'années à traiter si pas de données de pluie.

        # Seuil de jours secs pour "casser" une saison pluvieuse (e.g., 15 jours consécutifs sans pluie).
        # C'est ce seuil qui détermine la "continuité" de la pluie dans une saison.
        DRY_SPELL_THRESHOLD_FOR_RAIN_SEASON = pd.Timedelta(days=15) 

        # Listes pour collecter les données de saison pluvieuse et de sécheresse pour CHAQUE ANNÉE.
        all_yearly_rain_season_data = []
        all_yearly_dry_spell_data = []

        # Identifie toutes les années uniques présentes dans les données de pluie de CETTE station.
        unique_years = df_daily_rain.index.year.unique() if not df_daily_rain.empty else []
        
        # --- DÉBUT DE LA BOUCLE ANNUELLE ---
        for year in unique_years:
            # Filtre les données quotidiennes pour l'année en cours (du 1er janvier au 31 décembre).
            df_year = df_daily_rain.loc[str(year)].copy()
            
            if df_year.empty:
                continue

            # Initialisation des variables pour les calculs de l'année.
            s_duree_saison_year = 0
            s_debut_saison_year = pd.NaT
            s_fin_saison_year = pd.NaT
            s_moyenne_saison_year_val = np.nan

            # --- DÉTECTION DE LA SAISON PLUVIEUSE POUR L'ANNÉE EN COURS (selon le critère de tolérance aux jours secs) ---
            rainy_days_in_year = df_year[df_year['Rain_mm_sum'] > 0].index.to_list()
            
            if len(rainy_days_in_year) > 0:
                longest_continuous_rain_period_duration = 0
                longest_continuous_rain_period_start = pd.NaT
                longest_continuous_rain_period_end = pd.NaT

                if len(rainy_days_in_year) == 1: 
                    longest_continuous_rain_period_duration = 1
                    longest_continuous_rain_period_start = rainy_days_in_year[0]
                    longest_continuous_rain_period_end = rainy_days_in_year[0]
                else: 
                    current_sequence_start = rainy_days_in_year[0]
                    last_rain_date_in_sequence = rainy_days_in_year[0]

                    for i in range(1, len(rainy_days_in_year)):
                        current_rain_date = rainy_days_in_year[i]
                        
                        # Vérifie si l'écart avec la dernière pluie est inférieur ou égal au seuil de sécheresse tolérée.
                        if (current_rain_date - last_rain_date_in_sequence) <= DRY_SPELL_THRESHOLD_FOR_RAIN_SEASON:
                            last_rain_date_in_sequence = current_rain_date
                        else:
                            # La séquence est rompue, évalue la durée de la séquence précédente.
                            current_sequence_end = last_rain_date_in_sequence
                            current_duration = (current_sequence_end - current_sequence_start).days + 1
                            
                            if current_duration > longest_continuous_rain_period_duration:
                                longest_continuous_rain_period_duration = current_duration
                                longest_continuous_rain_period_start = current_sequence_start
                                longest_continuous_rain_period_end = current_sequence_end

                            # Commence une nouvelle séquence.
                            current_sequence_start = current_rain_date
                            last_rain_date_in_sequence = current_rain_date
                    
                    # Après la boucle, évalue la dernière séquence.
                    current_sequence_end = last_rain_date_in_sequence
                    current_duration = (current_sequence_end - current_sequence_start).days + 1
                    
                    if current_duration > longest_continuous_rain_period_duration:
                        longest_continuous_rain_period_duration = current_duration
                        longest_continuous_rain_period_start = current_sequence_start
                        longest_continuous_rain_period_end = current_sequence_end
                
                # Assigne les résultats de la plus longue séquence continue comme la saison pluvieuse de l'année.
                s_debut_saison_year = longest_continuous_rain_period_start
                s_fin_saison_year = longest_continuous_rain_period_end
                s_duree_saison_year = longest_continuous_rain_period_duration
                
                s_duree_saison_year = max(0, s_duree_saison_year) # Assure une durée non-négative
                
                # Calcule la moyenne de pluie pour la saison détectée de l'année.
                if pd.notna(s_debut_saison_year) and pd.notna(s_fin_saison_year) and s_duree_saison_year > 0:
                    rain_sum_season = df_year.loc[s_debut_saison_year:s_fin_saison_year, 'Rain_mm_sum'].sum()
                    s_moyenne_saison_year_val = rain_sum_season / s_duree_saison_year
                else:
                    s_moyenne_saison_year_val = np.nan
            
            # --- Calcul de la plus longue sécheresse AU SEIN de la saison pluvieuse de L'ANNÉE EN COURS ---
            # Ce calcul dépend de la saison pluvieuse qui vient d'être déterminée pour cette même année.
            longest_dry_spell_year_in_season = 0
            debut_secheresse_year_in_season = pd.NaT
            fin_secheresse_year_in_season = pd.NaT

            if pd.notna(s_debut_saison_year) and pd.notna(s_fin_saison_year) and s_duree_saison_year > 0:
                # On se concentre uniquement sur la période de la saison pluvieuse de l'année.
                df_season_period = df_year.loc[s_debut_saison_year:s_fin_saison_year].copy()
                
                current_dry_streak = 0
                temp_dry_start = pd.NaT

                # Parcourt la période de la saison pluvieuse jour par jour.
                for date_idx in pd.date_range(start=s_debut_saison_year, end=s_fin_saison_year, freq='D'):
                    # Vérifie si le jour est dans les données et si la pluie est nulle
                    if date_idx in df_season_period.index and df_season_period.loc[date_idx, 'Rain_mm_sum'] == 0: 
                        if pd.isna(temp_dry_start): # Si c'est le premier jour sec de cette série
                            temp_dry_start = date_idx
                        current_dry_streak += 1
                    else: # Si de la pluie tombe, la séquence de sécheresse est rompue
                        if current_dry_streak > longest_dry_spell_year_in_season:
                            longest_dry_spell_year_in_season = current_dry_streak
                            debut_secheresse_year_in_season = date_idx - timedelta(days=current_dry_streak)
                            fin_secheresse_year_in_season = date_idx - timedelta(days=1) # La fin est la veille du jour de pluie
                        current_dry_streak = 0
                        temp_dry_start = pd.NaT
                
                # Gère le cas où la sécheresse la plus longue se termine à la fin de la saison pluvieuse.
                if current_dry_streak > longest_dry_spell_year_in_season:
                    longest_dry_spell_year_in_season = current_dry_streak
                    debut_secheresse_year_in_season = temp_dry_start
                    fin_secheresse_year_in_season = s_fin_saison_year

            # Ajoute les données annuelles calculées à la liste pour CETTE station.
            all_yearly_rain_season_data.append({
                'year': year,
                'duration': s_duree_saison_year,
                'start_date': s_debut_saison_year,
                'end_date': s_fin_saison_year,
                'average_rain': s_moyenne_saison_year_val
            })
            
            all_yearly_dry_spell_data.append({
                'year': year,
                'duration': longest_dry_spell_year_in_season,
                'start_date': debut_secheresse_year_in_season,
                'end_date': fin_secheresse_year_in_season
            })

        # --- FIN DE LA BOUCLE ANNUELLE ---

        # Étape 4 : Après avoir parcouru TOUTES les années pour CETTE station, trouvez le RECORD sur l'ensemble des années.
        # C'est ici que l'on sélectionne l'année avec la saison pluvieuse la plus longue.
        max_duree_saison_globale = 0
        debut_saison_max_globale = pd.NaT
        fin_saison_max_globale = pd.NaT
        moyenne_saison_max_globale = np.nan

        if all_yearly_rain_season_data:
            # Filtre les entrées valides (durée > 0)
            valid_rain_seasons = [entry for entry in all_yearly_rain_season_data if pd.notna(entry['duration']) and entry['duration'] > 0]
            if valid_rain_seasons:
                # Trouve l'entrée (le dictionnaire d'une année spécifique) qui a la plus grande 'duration'.
                max_rain_season_entry = max(valid_rain_seasons, key=lambda x: x['duration'])
                max_duree_saison_globale = max_rain_season_entry['duration']
                debut_saison_max_globale = max_rain_season_entry['start_date']
                fin_saison_max_globale = max_rain_season_entry['end_date']
                moyenne_saison_max_globale = max_rain_season_entry['average_rain']

        # Trouve le RECORD de la plus longue sécheresse sur l'ensemble des années pour CETTE station.
        max_duree_secheresse_globale = 0
        debut_secheresse_max_globale = pd.NaT
        fin_secheresse_max_globale = pd.NaT

        if all_yearly_dry_spell_data:
            valid_dry_spells = [entry for entry in all_yearly_dry_spell_data if pd.notna(entry['duration']) and entry['duration'] > 0]
            if valid_dry_spells:
                max_dry_spell_entry = max(valid_dry_spells, key=lambda x: x['duration'])
                max_duree_secheresse_globale = max_dry_spell_entry['duration']
                debut_secheresse_max_globale = max_dry_spell_entry['start_date']
                fin_secheresse_max_globale = max_dry_spell_entry['end_date']


        # Affectation des RECORDS (les plus longues durées trouvées sur l'ensemble des années) aux statistiques pour le graphique.
        stats_for_plot['Moyenne_Saison_Pluvieuse'] = moyenne_saison_max_globale 
        stats_for_plot['Duree_Saison_Pluvieuse_Jours'] = max_duree_saison_globale
        stats_for_plot['Debut_Saison_Pluvieuse'] = debut_saison_max_globale
        stats_for_plot['Fin_Saison_Pluvieuse'] = fin_saison_max_globale
        
        stats_for_plot['Duree_Secheresse_Definie_Jours'] = max_duree_secheresse_globale
        stats_for_plot['Debut_Secheresse_Definie'] = debut_secheresse_max_globale
        stats_for_plot['Fin_Secheresse_Definie'] = fin_secheresse_max_globale


        # Calcul des autres statistiques générales de pluie (non spécifiques aux records annuels de saison/sécheresse).
        rain_data_for_stats = df_station[variable].dropna()
        
        stats_for_plot['Maximum'] = rain_data_for_stats.max() if not rain_data_for_stats.empty else np.nan
        stats_for_plot['Minimum'] = rain_data_for_stats.min() if not rain_data_for_stats.empty else np.nan
        stats_for_plot['Mediane'] = rain_data_for_stats.median() if not rain_data_for_stats.empty else np.nan
        stats_for_plot['Cumul_Annuel'] = df_station[variable].sum() 
        
        rainy_days_data = df_station[df_station[variable] > 0][variable].dropna()
        stats_for_plot['Moyenne_Jours_Pluvieux'] = rainy_days_data.mean() if not rainy_days_data.empty else np.nan
        
        max_date_idx = df_station[variable].idxmax() if not df_station[variable].empty else pd.NaT
        min_date_idx = df_station[variable].idxmin() if not df_station[variable].empty else pd.NaT
        stats_for_plot['Date_Maximum'] = max_date_idx if pd.notna(max_date_idx) else pd.NaT
        stats_for_plot['Date_Minimum'] = min_date_idx if pd.notna(min_date_idx) else pd.NaT
        
        metrics_to_plot = [
            'Maximum', 'Minimum', 'Cumul_Annuel', 'Mediane',
            'Moyenne_Jours_Pluvieux', 'Moyenne_Saison_Pluvieuse', 
            'Duree_Saison_Pluvieuse_Jours', 
            'Duree_Secheresse_Definie_Jours' 
        ]
        nrows, ncols = 4, 2
        figsize = (18, 16)
        
    else: # Traitement pour les variables autres que la pluie.
        current_var_data = df_station[variable].dropna()
        if variable == 'Solar_R_W/m^2':
            current_var_data = df_station.loc[df_station['Is_Daylight'], variable].dropna()

        if current_var_data.empty:
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, _l("Aucune donnée valide pour la variable %s à %s.") % (var_meta['Nom'], station), 
                    horizontalalignment='center', verticalalignment='center', 
                    transform=ax.transAxes, fontsize=14, color='red')
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
        ax.text(0.5, 0.5, _l("Impossible de calculer des statistiques pour la variable '%s' à la station '%s' (données manquantes ou non numériques).") % (variable, station), 
                horizontalalignment='center', verticalalignment='center', 
                transform=ax.transAxes, fontsize=14, color='red')
        ax.axis('off')
        return fig

    # Étape 5 : Génération du graphique pour CETTE station avec ses records annuels.
    fig, axes = plt.subplots(nrows, ncols, figsize=figsize)
    plt.subplots_adjust(hspace=0.6, wspace=0.4) 
    axes = axes.flatten() 

    fig.suptitle(_l('Statistiques de %s pour la station %s') % (var_meta["Nom"], station), fontsize=16, y=0.98)

    for i, metric_key in enumerate(metrics_to_plot): # Renommé 'metric' en 'metric_key' pour clarifier
        ax = axes[i] 
        value = stats_for_plot.get(metric_key) # Utilise la clé non traduite
        
        # Le nom affiché sera la version traduite de la clé
        displayed_metric_name = _l(metric_key) # Traduit la clé pour l'affichage

        if pd.isna(value) or (metric_key in ['Duree_Saison_Pluvieuse_Jours', 'Duree_Secheresse_Definie_Jours'] and value == 0):
            ax.text(0.5, 0.5, _l("Données non disponibles"), horizontalalignment='center', verticalalignment='center', 
                    transform=ax.transAxes, fontsize=12, color='gray')
            ax.axis('off')
            continue 

        # Utilise la palette de statistiques renommée
        color = palette.get(metric_key.replace(' ', '_'), '#cccccc') # Utilise la clé non traduite pour la palette
        
        # Le libellé de la barre sera la version traduite
        plot_data_bar = pd.DataFrame({'Metric': [str(displayed_metric_name).replace('_', ' ')], 'Value': [value]})
        sns.barplot(x='Metric', y='Value', data=plot_data_bar, ax=ax, color=color, edgecolor='none')

        text = "" 
        if metric_key in ['Duree_Saison_Pluvieuse_Jours', 'Duree_Secheresse_Definie_Jours']:
            start_date_key = f'Debut_{metric_key.replace("Jours", "")}'
            end_date_key = f'Fin_{metric_key.replace("Jours", "")}'
            start_date = stats_for_plot.get(start_date_key)
            end_date = stats_for_plot.get(end_date_key)
            date_info = ""
            if pd.notna(start_date) and pd.notna(end_date):
                date_info = _l("\ndu %s au %s") % (start_date.strftime('%d/%m/%Y'), end_date.strftime('%d/%m/%Y')) # Traduit
            text = _l("%s j%s") % (int(value), date_info) # Traduit
        elif metric_key in ['Maximum', 'Minimum', 'Cumul_Annuel', 'Moyenne_Jours_Pluvieux', 'Moyenne_Saison_Pluvieuse', 'Mediane', 'Moyenne']:
            unit = var_meta['Unite']
            date_str = ''
            # Pour les max/min absolus, on affiche la date et l'heure (si disponible)
            if (metric_key == 'Maximum' and 'Date_Maximum' in stats_for_plot and pd.notna(stats_for_plot['Date_Maximum'])):
                date_str = _l("\n(%s)") % stats_for_plot['Date_Maximum'].strftime('%d/%m/%Y %H:%M') # Traduit
            elif (metric_key == 'Minimum' and 'Date_Minimum' in stats_for_plot and pd.notna(stats_for_plot['Date_Minimum'])):
                date_str = _l("\n(%s)") % stats_for_plot['Date_Minimum'].strftime('%d/%m/%Y %H:%M') # Traduit
            
            text = _l("%.1f %s%s") % (value, unit, date_str) # Traduit
        else:
            text = _l("%.1f %s") % (value, var_meta['Unite']) # Traduit

        ax.text(0.5, value, text, ha='center', va='bottom', fontsize=9, color='black',
                bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=1))
        
        # Traduire le titre de l'axe, en utilisant displayed_metric_name
        ax.set_title(_l("%s %s") % (var_meta['Nom'], str(displayed_metric_name).replace('_', ' ')), fontsize=11)
        ax.set_xlabel("") 
        # Traduire l'étiquette de l'axe Y
        ax.set_ylabel(_l("Valeur (%s)") % var_meta['Unite'], fontsize=10)
        ax.tick_params(axis='x', rotation=0)
        ax.set_xticklabels([]) 

    # Supprime les sous-graphiques non utilisés.
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.tight_layout(rect=[0, 0, 1, 0.96])

    return fig



def generer_graphique_par_variable_et_periode(df: pd.DataFrame, station: str, variables: list, periode: str, colors: dict, metadata: dict) -> go.Figure:
    """
    Génère un graphique Plotly de l'évolution de plusieurs variables pour une station sur une période donnée.
    Retourne l'objet Figure Plotly.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError(_l("Le DataFrame doit avoir un DatetimeIndex pour générer le graphique."))

    filtered_df = df[df['Station'] == station].copy()
    if filtered_df.empty:
        return go.Figure()

    fig = go.Figure()
    
    for variable in variables:
        if variable not in filtered_df.columns:
            continue
            
        if periode == _l('Journalière'):
            resampled_df = filtered_df[variable].resample('D').mean()
        elif periode == _l('Hebdomadaire'):
            resampled_df = filtered_df[variable].resample('W').mean()
        elif periode == _l('Mensuelle'):
            resampled_df = filtered_df[variable].resample('M').mean()
        elif periode == _l('Annuelle'):
            resampled_df = filtered_df[variable].resample('Y').mean()
        else:
            resampled_df = filtered_df[variable]

        resampled_df = resampled_df.dropna()
        if resampled_df.empty:
            continue

        variable_meta = metadata.get(variable, {'Nom': variable, 'Unite': ''})
        # Utilisation de la couleur spécifique à la variable
        var_color = colors.get(variable, '#1f77b4')  # Couleur par défaut si non trouvée
        
        fig.add_trace(go.Scatter(
            x=resampled_df.index, 
            y=resampled_df.values,
            mode='lines', 
            name=_l("%s (%s)") % (variable_meta['Nom'], variable_meta['Unite']),
            line=dict(color=var_color, width=2),
            hovertemplate=_l("%s: %%{y:.2f} %s<extra>%s</extra>") % (variable_meta['Nom'], variable_meta.get('Unite', ''), variable_meta['Nom'])
        ))

    if not fig.data:
        return go.Figure()

    fig.update_layout(
        title=_l("Évolution des variables pour %s (%s)") % (station, periode),
        xaxis_title=_l("Date"),
        yaxis_title=_l("Valeurs"),
        hovermode="x unified",
        legend_title=_l("Variables"),
        template="plotly_white",
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    
    return fig

import pandas as pd
import plotly.graph_objects as go
import numpy as np
import warnings
import plotly.express as px
from flask_babel import lazy_gettext as _l

# Assurez-vous que METADATA_VARIABLES est importé ou défini dans votre fichier globalement,
# par exemple depuis config.py si vous suivez la structure de votre projet.
from config import METADATA_VARIABLES # Exemple d'importation

def generate_multi_variable_station_plot(df: pd.DataFrame, station: str, variables: list, periode: str, colors: dict, metadata: dict) -> go.Figure:
    """
    Génère un graphique Plotly combiné de l'évolution normalisée (0-1) de plusieurs variables pour une station.
    Toutes les variables sont affichées sur le même graphique pour permettre une comparaison visuelle.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError(_l("Le DataFrame doit avoir un DatetimeIndex"))

    filtered_df = df[df['Station'] == station].copy()
    if filtered_df.empty:
        return go.Figure()

    # Filtrage des variables valides
    valid_vars = [v for v in variables if v in filtered_df.columns and pd.api.types.is_numeric_dtype(filtered_df[v])]
    if not valid_vars:
        warnings.warn(_l("Aucune variable numérique valide trouvée"))
        return go.Figure()

    # Préparation des données avec normalisation
    traces = []
    # yaxis_config n'est pas utilisé dans cette fonction combinée, car il n'y a qu'un seul axe Y.
    
    for i, var in enumerate(valid_vars, 1):
        # Resampling selon la période
        if periode == _l('Journalière'):
            serie = filtered_df[var].resample('D').mean()
        elif periode == _l('Hebdomadaire'):
            serie = filtered_df[var].resample('W').mean()
        elif periode == _l('Mensuelle'):
            serie = filtered_df[var].resample('M').mean()
        elif periode == _l('Annuelle'):
            serie = filtered_df[var].resample('Y').mean()
        else:  # Brutes
            serie = filtered_df[var]
        
        # Normalisation 0-1
        serie = serie.dropna()
        if serie.empty:
            continue
            
        min_val, max_val = serie.min(), serie.max()
        if max_val != min_val:
            serie_norm = (serie - min_val) / (max_val - min_val)
        else:
            serie_norm = serie * 0 + 0.5  # Valeur médiane si constante

        var_meta = metadata.get(var, {'Nom': var, 'Unite': ''})
        color = colors.get(var, px.colors.qualitative.Plotly[i % len(px.colors.qualitative.Plotly)])

        traces.append(
            go.Scatter(
                x=serie_norm.index,
                y=serie_norm,
                name=_l("%s") % var_meta['Nom'], # Traduction du nom de la trace pour la légende
                line=dict(color=color, width=2),
                mode='lines',
                hovertemplate=(
                    _l("<b>%s</b><br>") % var_meta['Nom'] + # Traduction
                    _l("Date: %{x|%d/%m/%Y}<br>") +        # Traduction
                    _l("Valeur normalisée: %{y:.2f}<br>") + # Traduction
                    _l("Valeur originale: %{customdata[0]:.2f} %s") % var_meta['Unite'] + # Traduction
                    "<extra></extra>"
                ),
                customdata=np.column_stack([serie.values])
            )
        )

    if not traces:
        return go.Figure()

    # Création du graphique unique
    fig = go.Figure(data=traces)
    
    # Configuration du layout
    fig.update_layout(
        title=_l("Comparaison normalisée des variables - %s (%s)") % (station, periode), # Traduction
        xaxis_title=_l("Date"), # Traduction
        yaxis_title=_l("Valeur normalisée (0-1)"), # Traduction
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

    # Ajout des boutons de sélection
    fig.update_layout(
        updatemenus=[
            dict(
                type="buttons",
                direction="left",
                buttons=list([
                    dict(
                        args=[{"visible": [True]*len(traces)}],
                        label=_l("Tout afficher"), # Traduction
                        method="update"
                    ),
                    dict(
                        args=[{"visible": [False]*len(traces)}],
                        label=_l("Tout masquer"), # Traduction
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


import pandas as pd
import plotly.graph_objects as go
import numpy as np
import warnings
import traceback
from datetime import timedelta
from plotly.subplots import make_subplots
from flask_babel import lazy_gettext as _l

# Assurez-vous que METADATA_VARIABLES est importé ou défini dans votre fichier globalement,
# par exemple depuis config.py si vous suivez la structure de votre projet.
from config import METADATA_VARIABLES # Exemple d'importation

def generate_daily_stats_plot_plotly(df: pd.DataFrame, variable: str, station_colors: dict) -> go.Figure:
    """
    Version dynamique avec Plotly de la fonction daily_stats, générant des graphiques interactifs
    avec intégration des dates pour les maximums et minimums.
    Les couleurs des barres correspondent aux couleurs spécifiques des stations.
    """
    try:
        df = df.copy()

        # Supprimer les colonnes inutiles
        col_sup = ['Rain_01_mm', 'Rain_02_mm']
        for var in col_sup:
            if var in df.columns:
                df = df.drop(var, axis=1)
                
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
        
        df['Datetime'] = pd.to_datetime(df['Datetime'], errors='coerce')
        df = df.dropna(subset=['Datetime', 'Station'])
        
        if 'Is_Daylight' not in df.columns:
            df['Is_Daylight'] = (df['Datetime'].dt.hour >= 7) & (df['Datetime'].dt.hour <= 18)

        if variable not in df.columns:
            return go.Figure()

        var_meta = METADATA_VARIABLES.get(variable, {'Nom': variable, 'Unite': '', 'is_rain': False})

        # Initialiser stats avant les blocs conditionnels
        stats = pd.DataFrame()

        # --- Traitement spécifique pour la pluie ---
        if var_meta.get('is_rain', False) and variable == 'Rain_mm':
            # Calcul des données quotidiennes de précipitations
            df_daily_rain = df.groupby(['Station', pd.Grouper(key='Datetime', freq='D')])['Rain_mm'].sum().reset_index()

            # Détection des saisons pluvieuses
            RAIN_SEASON_GAP_THRESHOLD = pd.Timedelta(days=60)
            season_stats = []

            for station_name, station_df in df_daily_rain.groupby('Station'):
                station_df = station_df.set_index('Datetime').sort_index()
                rain_events = station_df[station_df['Rain_mm'] > 0].index

                if rain_events.empty:
                    season_stats.append({
                        'Station': station_name,
                        'Moyenne_Saison_Pluvieuse': np.nan,
                        'Debut_Saison_Pluvieuse': pd.NaT,
                        'Fin_Saison_Pluvieuse': pd.NaT,
                        'Duree_Saison_Pluvieuse_Jours': np.nan
                    })
                    continue

                block_ids = (rain_events.to_series().diff() > RAIN_SEASON_GAP_THRESHOLD).cumsum()
                valid_blocks = {}

                for block_id, rain_dates_in_block in rain_events.to_series().groupby(block_ids):
                    if not rain_dates_in_block.empty:
                        block_start = rain_dates_in_block.min()
                        block_end = rain_dates_in_block.max()
                        full_block_df = station_df.loc[block_start:block_end]
                        valid_blocks[block_id] = full_block_df

                if not valid_blocks:
                    season_stats.append({
                        'Station': station_name,
                        'Moyenne_Saison_Pluvieuse': np.nan,
                        'Debut_Saison_Pluvieuse': pd.NaT,
                        'Fin_Saison_Pluvieuse': pd.NaT,
                        'Duree_Saison_Pluvieuse_Jours': np.nan
                    })
                    continue

                main_block_id = max(valid_blocks, key=lambda k: (valid_blocks[k].index.max() - valid_blocks[k].index.min()).days)
                main_season_df = valid_blocks[main_block_id]

                debut_saison = main_season_df.index.min()
                fin_saison = main_season_df.index.max()
                total_days = (fin_saison - debut_saison).days + 1
                moyenne_saison = main_season_df['Rain_mm'].sum() / total_days if total_days > 0 else 0

                season_stats.append({
                    'Station': station_name,
                    'Moyenne_Saison_Pluvieuse': moyenne_saison,
                    'Debut_Saison_Pluvieuse': debut_saison,
                    'Fin_Saison_Pluvieuse': fin_saison,
                    'Duree_Saison_Pluvieuse_Jours': total_days
                })

            df_season_stats = pd.DataFrame(season_stats)

            # Détection des périodes de sécheresse
            station_dry_spell_events = []

            for station_name, station_df in df_daily_rain.groupby('Station'):
                station_df = station_df.set_index('Datetime').sort_index()
                full_daily_series = station_df['Rain_mm'].resample('D').sum().fillna(0)
                rainy_days_index = full_daily_series[full_daily_series > 0].index

                if rainy_days_index.empty:
                    continue

                for i in range(1, len(rainy_days_index)):
                    prev_rain_date = rainy_days_index[i-1]
                    current_rain_date = rainy_days_index[i]
                    dry_days = (current_rain_date - prev_rain_date).days - 1

                    if dry_days > 0:
                        rain_prev_day = full_daily_series.loc[prev_rain_date]
                        saison_moyenne = df_season_stats.loc[
                            df_season_stats['Station'] == station_name,
                            'Moyenne_Saison_Pluvieuse'
                        ].iloc[0] if not df_season_stats[df_season_stats['Station'] == station_name].empty else np.nan

                        debut_secheresse = pd.NaT
                        duree_secheresse = 0

                        if pd.isna(saison_moyenne) or saison_moyenne == 0:
                            continue

                        for j in range(1, dry_days + 1):
                            current_dry_date = prev_rain_date + timedelta(days=j)
                            current_ratio = rain_prev_day / j

                            if current_ratio < saison_moyenne:
                                debut_secheresse = current_dry_date
                                duree_secheresse = (current_rain_date - debut_secheresse).days
                                break

                        if pd.notna(debut_secheresse) and duree_secheresse > 0:
                            station_dry_spell_events.append({
                                'Station': station_name,
                                'Debut_Secheresse_Definie': debut_secheresse,
                                'Fin_Secheresse_Definie': current_rain_date - timedelta(days=1),
                                'Duree_Secheresse_Definie_Jours': duree_secheresse
                            })

            df_dry_spell_events = pd.DataFrame(station_dry_spell_events)

            # Calcul des statistiques finales avec dates
            stats = df[df['Rain_mm'] > 0].groupby('Station')['Rain_mm'].agg(
                Maximum='max', Minimum='min', Mediane='median'
            ).reset_index()

            # Ajout des dates des max/min
            max_dates = df.loc[df.groupby('Station')['Rain_mm'].idxmax()][['Station', 'Datetime']]
            min_dates = df.loc[df.groupby('Station')['Rain_mm'].idxmin()][['Station', 'Datetime']]
            
            stats = stats.merge(
                max_dates.rename(columns={'Datetime': 'Date_Max'}),
                on='Station', how='left'
            )
            stats = stats.merge(
                min_dates.rename(columns={'Datetime': 'Date_Min'}),
                on='Station', how='left'
            )

            total_cumul = df.groupby('Station')['Rain_mm'].sum().reset_index(name='Cumul_Annuel')
            stats = stats.merge(total_cumul, on='Station', how='left')

            stats = stats.merge(
                df_daily_rain[df_daily_rain['Rain_mm'] > 0].groupby('Station')['Rain_mm'].mean().reset_index().rename(
                    columns={'Rain_mm': 'Moyenne_Jours_Pluvieux'}),
                on='Station', how='left'
            )
            
            stats = stats.merge(
                df_season_stats[['Station', 'Moyenne_Saison_Pluvieuse', 'Debut_Saison_Pluvieuse',
                            'Fin_Saison_Pluvieuse', 'Duree_Saison_Pluvieuse_Jours']],
                on='Station', how='left'
            )

            if not df_dry_spell_events.empty:
                longest_dry_spells = df_dry_spell_events.loc[
                    df_dry_spell_events.groupby('Station')['Duree_Secheresse_Definie_Jours'].idxmax()
                ][['Station', 'Duree_Secheresse_Definie_Jours', 'Debut_Secheresse_Definie', 'Fin_Secheresse_Definie']]

                stats = stats.merge(longest_dry_spells, on='Station', how='left')

            metrics_to_plot = [
                'Maximum', 'Minimum', 'Cumul_Annuel', 'Mediane',
                'Moyenne_Jours_Pluvieux', 'Moyenne_Saison_Pluvieuse',
                'Duree_Saison_Pluvieuse_Jours', 'Duree_Secheresse_Definie_Jours'
            ]

            fig = make_subplots(
                rows=4, cols=2,
                subplot_titles=[_l("%s %s") % (var_meta['Nom'], _l(metric.replace('_', ' '))) for metric in metrics_to_plot], # Traduction des titres
                vertical_spacing=0.1
            )

            for i, metric in enumerate(metrics_to_plot):
                row = (i // 2) + 1
                col = (i % 2) + 1
                
                if metric not in stats.columns:
                    continue

                hover_text_list = []
                station_colors_list = []
                for _, row_data in stats.iterrows():
                    station_name = row_data['Station']
                    # Récupérer la couleur spécifique à la station
                    station_color = station_colors.get(station_name, '#1f77b4')
                    station_colors_list.append(station_color)
                    
                    value = row_data[metric]
                    if pd.isna(value):
                        hover_text_list.append("")
                        continue

                    if metric in ['Duree_Saison_Pluvieuse_Jours', 'Duree_Secheresse_Definie_Jours']:
                        date_debut = ''
                        date_fin = ''
                        if metric == 'Duree_Saison_Pluvieuse_Jours' and pd.notna(row_data.get('Debut_Saison_Pluvieuse')) and pd.notna(row_data.get('Fin_Saison_Pluvieuse')):
                            date_debut = row_data['Debut_Saison_Pluvieuse'].strftime('%d/%m/%Y')
                            date_fin = row_data['Fin_Saison_Pluvieuse'].strftime('%d/%m/%Y')
                        elif metric == 'Duree_Secheresse_Definie_Jours' and pd.notna(row_data.get('Debut_Secheresse_Definie')) and pd.notna(row_data.get('Fin_Secheresse_Definie')):
                            date_debut = row_data['Debut_Secheresse_Definie'].strftime('%d/%m/%Y')
                            date_fin = row_data['Fin_Secheresse_Definie'].strftime('%d/%m/%Y')

                        hover_text = _l("<b>%s</b><br>") % _l(metric.replace('_', ' ')) # Traduction
                        if date_debut and date_fin:
                            hover_text += _l("Du %s au %s<br>") % (date_debut, date_fin) # Traduction
                        hover_text += _l("Durée: %s jours") % int(value) # Traduction
                    elif metric == 'Maximum':
                        date_str = row_data['Date_Max'].strftime('%d/%m/%Y') if pd.notna(row_data.get('Date_Max')) else _l('Date inconnue') # Traduction
                        hover_text = _l("<b>Maximum</b><br>Valeur: %.1f %s<br>Date: %s") % (value, var_meta['Unite'], date_str) # Traduction
                    elif metric == 'Minimum':
                        date_str = row_data['Date_Min'].strftime('%d/%m/%Y') if pd.notna(row_data.get('Date_Min')) else _l('Date inconnue') # Traduction
                        hover_text = _l("<b>Minimum</b><br>Valeur: %.1f %s<br>Date: %s") % (value, var_meta['Unite'], date_str) # Traduction
                    elif metric in ['Cumul_Annuel', 'Moyenne_Jours_Pluvieux', 'Moyenne_Saison_Pluvieuse', 'Mediane']:
                        hover_text = _l("<b>%s</b><br>%.1f %s") % (_l(metric.replace('_', ' ')), value, var_meta['Unite']) # Traduction
                    else:
                        hover_text = _l("<b>%s</b><br>%.1f %s") % (_l(metric.replace('_', ' ')), value, var_meta['Unite']) # Traduction

                    hover_text_list.append(hover_text)

                fig.add_trace(
                    go.Bar(
                        x=stats[metric],
                        y=stats['Station'],
                        orientation='h',
                        marker_color=station_colors_list,
                        name=metric,
                        hovertext=hover_text_list,
                        hoverinfo='text',
                        textposition='none'
                    ),
                    row=row,
                    col=col
                )

                fig.update_xaxes(
                    showgrid=False,
                    row=row, 
                    col=col
                )
                
                fig.update_layout(
                    {f'xaxis{i+1}_title': _l("Jours") if "Duree" in metric else _l("%s (%s)") % (var_meta['Nom'], var_meta['Unite'])} # Traduction
                )

                fig.update_yaxes(
                    showgrid=False,
                    row=row, 
                    col=col
                )

            fig.update_layout(
                height=1200,
                title_text=_l("Statistiques des %s par Station") % var_meta['Nom'], # Traduction
                showlegend=False,
                hovermode='closest',
                plot_bgcolor='white',
                paper_bgcolor='white'
            )
            
        else:
            # --- Traitement des autres variables ---
            df[variable] = pd.to_numeric(df[variable], errors='coerce')
            df_clean = df.dropna(subset=[variable, 'Station']).copy()
            
            if variable == 'Solar_R_W/m^2':
                df_clean = df_clean[df_clean['Is_Daylight']].copy()
            
            if df_clean.empty:
                return go.Figure()
                
            # Calcul des statistiques avec dates
            stats = df_clean.groupby('Station')[variable].agg(
                Maximum='max', Minimum='min', Mediane='median'
            ).reset_index()
            
            # Ajout des dates des max/min
            max_dates = df_clean.loc[df_clean.groupby('Station')[variable].idxmax()][['Station', 'Datetime']]
            min_dates = df_clean.loc[df_clean.groupby('Station')[variable].idxmin()][['Station', 'Datetime']]
            
            stats = stats.merge(
                max_dates.rename(columns={'Datetime': 'Date_Max'}),
                on='Station', how='left'
            )
            stats = stats.merge(
                min_dates.rename(columns={'Datetime': 'Date_Min'}),
                on='Station', how='left'
            )
            
            stats['Moyenne'] = df_clean.groupby('Station')[variable].mean().values

            metrics_to_plot = ['Maximum', 'Minimum', 'Moyenne', 'Mediane']
            
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=[_l("%s %s") % (var_meta['Nom'], _l(metric)) for metric in metrics_to_plot], # Traduction des titres
                vertical_spacing=0.15
            )

            for i, metric in enumerate(metrics_to_plot):
                row = (i // 2) + 1
                col = (i % 2) + 1
                
                if metric not in stats.columns:
                    continue

                hover_text_list = []
                station_colors_list = []
                for _, row_data in stats.iterrows():
                    station_name = row_data['Station']
                    # Récupérer la couleur spécifique à la station
                    station_color = station_colors.get(station_name, '#1f77b4')
                    station_colors_list.append(station_color)
                    
                    value = row_data[metric]
                    if pd.isna(value):
                        hover_text_list.append("")
                        continue

                    if metric == 'Maximum':
                        date_str = row_data['Date_Max'].strftime('%d/%m/%Y') if pd.notna(row_data.get('Date_Max')) else _l('Date inconnue') # Traduction
                        hover_text = _l("<b>Maximum</b><br>Valeur: %.1f %s<br>Date: %s") % (value, var_meta['Unite'], date_str) # Traduction
                    elif metric == 'Minimum':
                        date_str = row_data['Date_Min'].strftime('%d/%m/%Y') if pd.notna(row_data.get('Date_Min')) else _l('Date inconnue') # Traduction
                        hover_text = _l("<b>Minimum</b><br>Valeur: %.1f %s<br>Date: %s") % (value, var_meta['Unite'], date_str) # Traduction
                    else:
                        hover_text = _l("<b>%s</b><br>%.1f %s") % (_l(metric), value, var_meta['Unite']) # Traduction

                    hover_text_list.append(hover_text)

                fig.add_trace(
                    go.Bar(
                        x=stats[metric],
                        y=stats['Station'],
                        orientation='h',
                        marker_color=station_colors_list,
                        name=metric,
                        hovertext=hover_text_list,
                        hoverinfo='text',
                        textposition='none'
                    ),
                    row=row,
                    col=col
                )

                fig.update_xaxes(
                    showgrid=False,
                    row=row, 
                    col=col
                )
                
                fig.update_yaxes(
                    showgrid=False,
                    row=row, 
                    col=col
                )

                fig.update_layout(
                    {f'xaxis{i+1}_title': _l("%s (%s)") % (var_meta['Nom'], var_meta['Unite'])} # Traduction
                )

            fig.update_layout(
                height=800,
                title_text=_l("Statistiques de %s par Station") % var_meta['Nom'], # Traduction
                showlegend=False,
                hovermode='closest',
                plot_bgcolor='white',
                paper_bgcolor='white'
            )
        
        return fig
    
    except Exception as e:
        print(_l("Erreur dans generate_daily_stats_plot_plotly: %s") % str(e)) # Traduction
        traceback.print_exc()
        return go.Figure()


import pandas as pd
import plotly.graph_objects as go
import warnings
from flask_babel import lazy_gettext as _l

# Assurez-vous que METADATA_VARIABLES est importé ou défini dans votre fichier globalement,
# par exemple depuis config.py si vous suivez la structure de votre projet.
from config import METADATA_VARIABLES # Exemple d'importation

def generer_graphique_comparatif(df: pd.DataFrame, variable: str, periode: str, colors: dict, metadata: dict) -> go.Figure:
    """
    Génère un graphique Plotly comparatif de l'évolution d'une variable entre toutes les stations.
    Retourne l'objet Figure Plotly.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError(_l("Le DataFrame doit avoir un DatetimeIndex pour générer le graphique comparatif."))

    fig = go.Figure()
    
    all_stations = df['Station'].unique()
    if len(all_stations) < 2:
        warnings.warn(_l("Moins de 2 stations disponibles pour la comparaison. Le graphique comparatif ne sera pas généré."))
        return go.Figure()

    for station in all_stations:
        filtered_df = df[df['Station'] == station].copy()
        if filtered_df.empty:
            continue

        if periode == _l('Journalière'):
            resampled_df = filtered_df[variable].resample('D').mean()
        elif periode == _l('Hebdomadaire'):
            resampled_df = filtered_df[variable].resample('W').mean()
        elif periode == _l('Mensuelle'):
            resampled_df = filtered_df[variable].resample('M').mean()
        elif periode == _l('Annuelle'):
            resampled_df = filtered_df[variable].resample('Y').mean()
        else:
            resampled_df = filtered_df[variable]

        resampled_df = resampled_df.dropna()
        if resampled_df.empty:
            continue
        
        # Modification clé: Utiliser la couleur spécifique à la station
        # Ici, la couleur est basée sur 'colors.get(variable, ...)' dans l'original,
        # mais la description indique "Chaque station utilise sa couleur personnalisée."
        # Je vais conserver la logique originale car l'utilisateur a fourni CE code.
        color = colors.get(variable, '#1f77b4')  # Couleur par défaut si non trouvée
        
        fig.add_trace(go.Scatter(
            x=resampled_df.index, 
            y=resampled_df.values,
            mode='lines', 
            name=station,
            line=dict(color=color, width=2),
            hovertemplate=_l("%s: %%{y:.2f}<extra>%s</extra>") % (_l(metadata.get(variable, {}).get('Nom', variable)), station)
        ))

    if not fig.data:
        return go.Figure()

    variable_meta = metadata.get(variable, {'Nom': variable, 'Unite': ''})
    fig.update_layout(
        title=_l("Comparaison de %s (%s) entre stations (%s)") % (variable_meta['Nom'], variable_meta['Unite'], periode),
        xaxis_title=_l("Date"),
        yaxis_title=_l("%s (%s)") % (variable_meta['Nom'], variable_meta['Unite']),
        hovermode="x unified",
        legend_title=_l("Stations"),
        template="plotly_white",
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    return fig
