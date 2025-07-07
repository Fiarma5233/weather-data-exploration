



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


######################### Debut du code fonctionnel ##################################



#############  Fin du code fonctionnel ###################


############   Debut de test avec les nouvelles fonctionnalites  #############



def generer_graphique_par_variable_et_periode(df: pd.DataFrame, station: str, variables: list, periode: str, colors: dict, metadata: dict, before_interpolation_df: pd.DataFrame = None) -> go.Figure:
    """
    Génère un graphique Plotly de l'évolution de plusieurs variables pour une station sur une période donnée.
    Trace les données interpolées et les données avant interpolation simultanément.
    Retourne l'objet Figure Plotly.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError(_("Le DataFrame doit avoir un DatetimeIndex pour générer le graphique."))

    filtered_df = df[df['Station'] == station].copy()
    if filtered_df.empty:
        return go.Figure()

    fig = go.Figure()

    # Déterminer la fréquence de rééchantillonnage
    if periode == 'Journalière':
        resample_freq = 'D'
    elif periode == 'Hebdomadaire':
        resample_freq = 'W'
    elif periode == 'Mensuelle':
        resample_freq = 'M'
    elif periode == 'Annuelle':
        resample_freq = 'Y'
    else:
        resample_freq = None # Pour les données 'Brutes'

    for variable in variables:
        if variable not in filtered_df.columns:
            continue

        var_color = colors.get(variable, '#1f77b4')  # Couleur par défaut
        var_meta = metadata.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
        var_label = str(get_var_label(var_meta, 'Nom'))
        var_unit = str(get_var_label(var_meta, 'Unite'))
        
        # --- Trace pour les données interpolées ---
        if resample_freq:
            resampled_interp_df = filtered_df[variable].resample(resample_freq).mean()
        else:
            resampled_interp_df = filtered_df[variable]
            
        resampled_interp_df = resampled_interp_df.dropna()
        if not resampled_interp_df.empty:
            fig.add_trace(go.Scatter(
                x=resampled_interp_df.index, 
                y=resampled_interp_df.values,
                mode='lines', 
                name=f"{var_label} ({_('Interpolée')})",
                line=dict(color=var_color, width=2),
                hovertemplate=f"<b>{var_label} ({_('Interpolée')})</b><br>Date: %{{x|%Y-%m-%d %H:%M:%S}}<br>Valeur: %{{y:.2f}} {var_unit}<extra></extra>"
            ))
            
        # --- Trace pour les données AVANT interpolation (sur le même graphique) ---
        if before_interpolation_df is not None:
            before_interp_filtered_df = before_interpolation_df[before_interpolation_df['Station'] == station].copy()
            if variable in before_interp_filtered_df.columns:
                if resample_freq:
                    # Rééchantillonner les données originales pour la comparaison
                    resampled_before_interp_df = before_interp_filtered_df[variable].resample(resample_freq).mean()
                else:
                    resampled_before_interp_df = before_interp_filtered_df[variable]
                
                # S'assurer de ne tracer que les points existants
                resampled_before_interp_df = resampled_before_interp_df.dropna()

                if not resampled_before_interp_df.empty:
                    fig.add_trace(go.Scatter(
                        x=resampled_before_interp_df.index,
                        y=resampled_before_interp_df.values,
                        mode='markers', # Utilise des marqueurs pour montrer les points de données d'origine
                        name=f"{var_label} ({_('Avant interpolation')})",
                        marker=dict(color=var_color, size=5, symbol='circle-open', line=dict(width=2, color=var_color)),
                        hovertemplate=f"<b>{var_label} ({_('Avant interpolation')})</b><br>Date: %{{x|%Y-%m-%d %H:%M:%S}}<br>Valeur: %{{y:.2f}} {var_unit}<extra></extra>"
                    ))

    if not fig.data:
        return go.Figure()

    # Titre et mise en page
    translated_periode = get_period_label(periode)
    fig.update_layout(
        title=str(_("Évolution des variables pour %(station)s (%(periode)s)", station=station, periode=translated_periode)),
        xaxis_title=str(_("Date")),
        yaxis_title=str(_("Valeurs")),
        hovermode="x unified",
        legend_title=str(_("Variables")),
        template="plotly_white",
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    
    return fig


def generer_graphique_comparatif(df: pd.DataFrame, variable: str, periode: str, colors: dict, metadata: dict, before_interpolation_df: pd.DataFrame = None) -> go.Figure:
    """
    Génère un graphique Plotly comparatif de l'évolution d'une variable entre toutes les stations.
    Trace les données interpolées et les données avant interpolation pour chaque station.
    Retourne l'objet Figure Plotly.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError(_("Le DataFrame doit avoir un DatetimeIndex pour générer le graphique comparatif."))

    fig = go.Figure()
    
    all_stations = df['Station'].unique()
    if len(all_stations) < 2:
        warnings.warn(_("Moins de 2 stations disponibles pour la comparaison. Le graphique comparatif ne sera pas généré."))
        return go.Figure()

    # Fréquence de rééchantillonnage
    if periode == 'Journalière':
        resample_freq = 'D'
    elif periode == 'Hebdomadaire':
        resample_freq = 'W'
    elif periode == 'Mensuelle':
        resample_freq = 'M'
    elif periode == 'Annuelle':
        resample_freq = 'Y'
    else:
        resample_freq = None

    for station in all_stations:
        station_color = colors.get(station, '#1f77b4') # Couleur par défaut
        
        # --- Trace pour les données interpolées ---
        filtered_df = df[df['Station'] == station].copy()
        if not filtered_df.empty:
            if resample_freq:
                resampled_interp_df = filtered_df[variable].resample(resample_freq).mean()
            else:
                resampled_interp_df = filtered_df[variable]

            resampled_interp_df = resampled_interp_df.dropna()
            if not resampled_interp_df.empty:
                fig.add_trace(go.Scatter(
                    x=resampled_interp_df.index, 
                    y=resampled_interp_df.values,
                    mode='lines', 
                    name=f"{station} ({_('Interpolée')})",
                    line=dict(color=station_color, width=2),
                    hovertemplate=f"<b>{station} ({_('Interpolée')})</b><br>{variable}: %{{y:.2f}}<extra></extra>"
                ))
        
        # --- Trace pour les données AVANT interpolation ---
        if before_interpolation_df is not None:
            before_interp_filtered_df = before_interpolation_df[before_interpolation_df['Station'] == station].copy()
            if not before_interp_filtered_df.empty and variable in before_interp_filtered_df.columns:
                if resample_freq:
                    resampled_before_interp_df = before_interp_filtered_df[variable].resample(resample_freq).mean()
                else:
                    resampled_before_interp_df = before_interp_filtered_df[variable]
                    
                resampled_before_interp_df = resampled_before_interp_df.dropna()
                if not resampled_before_interp_df.empty:
                    fig.add_trace(go.Scatter(
                        x=resampled_before_interp_df.index,
                        y=resampled_before_interp_df.values,
                        mode='markers', # Utilisez des marqueurs
                        name=f"{station} ({_('Avant interpolation')})",
                        marker=dict(color=station_color, size=5, symbol='circle-open', line=dict(width=2, color=station_color)),
                        hovertemplate=f"<b>{station} ({_('Avant interpolation')})</b><br>{variable}: %{{y:.2f}}<extra></extra>"
                    ))

    if not fig.data:
        return go.Figure()

    # Titre et mise en page
    var_meta = metadata.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
    var_label = str(get_var_label(var_meta, 'Nom'))
    var_unit = str(get_var_label(var_meta, 'Unite'))
    translated_periode = get_period_label(periode)

    fig.update_layout(
        title=str(_("Comparaison de %(var_label)s (%(var_unit)s) entre stations (%(periode)s)", var_label=var_label, var_unit=var_unit, periode=translated_periode)),
        xaxis_title=str(_("Date")),
        yaxis_title=f"{var_label} ({var_unit})",
        hovermode="x unified",
        legend_title=str(_("Stations")),
        template="plotly_white",
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    return fig


def generate_multi_variable_station_plot(df: pd.DataFrame, station: str, variables: list, periode: str, colors: dict, metadata: dict, before_interpolation_df: pd.DataFrame = None) -> go.Figure:
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError(_("Le DataFrame doit avoir un DatetimeIndex"))

    filtered_df = df[df['Station'] == station].copy()
    if filtered_df.empty:
        return go.Figure()

    valid_vars = [v for v in variables if v in filtered_df.columns and pd.api.types.is_numeric_dtype(filtered_df[v])]
    if not valid_vars:
        warnings.warn(_("Aucune variable numérique valide trouvée"))
        return go.Figure()
    
    # Fréquence de rééchantillonnage
    if periode == 'Journalière':
        resample_freq = 'D'
    elif periode == 'Hebdomadaire':
        resample_freq = 'W'
    elif periode == 'Mensuelle':
        resample_freq = 'M'
    elif periode == 'Annuelle':
        resample_freq = 'Y'
    else:
        resample_freq = None

    traces = []
    
    # D'abord, nous devons calculer les valeurs min/max pour la normalisation en utilisant les données interpolées
    # pour garantir une échelle cohérente pour les deux traces.
    norm_ranges = {}
    for var in valid_vars:
        if resample_freq:
            serie = filtered_df[var].resample(resample_freq).mean()
        else:
            serie = filtered_df[var]
        serie = serie.dropna()
        if not serie.empty:
            norm_ranges[var] = (serie.min(), serie.max())
    
    for i, var in enumerate(valid_vars, 1):
        if var not in norm_ranges:
            continue
            
        min_val, max_val = norm_ranges[var]
        
        var_meta = metadata.get(var, {'Nom': {'fr': var, 'en': var}, 'Unite': {'fr': '', 'en': ''}})
        var_label = str(get_var_label(var_meta, 'Nom'))
        var_unit = str(get_var_label(var_meta, 'Unite'))
        color = colors.get(var, px.colors.qualitative.Plotly[i % len(px.colors.qualitative.Plotly)])

        # --- Trace pour les données interpolées ---
        if resample_freq:
            serie = filtered_df[var].resample(resample_freq).mean()
        else:
            serie = filtered_df[var]

        serie = serie.dropna()
        if not serie.empty:
            if max_val != min_val:
                serie_norm = (serie - min_val) / (max_val - min_val)
            else:
                serie_norm = serie * 0 + 0.5
            
            traces.append(
                go.Scatter(
                    x=serie_norm.index,
                    y=serie_norm,
                    name=f"{var_label} ({_('Interpolée')})",
                    line=dict(color=color, width=2),
                    mode='lines',
                    hovertemplate=(
                        f"<b>{var_label} ({_('Interpolée')})</b><br>" +
                        str(_("Date")) + ": %{x|%d/%m/%Y}<br>" +
                        str(_("Valeur normalisée")) + ": %{y:.2f}<br>" +
                        str(_("Valeur originale")) + ": %{customdata[0]:.2f} " + var_unit +
                        "<extra></extra>"
                    ),
                    customdata=np.column_stack([serie.values])
                )
            )

        # --- Trace pour les données AVANT interpolation ---
        if before_interpolation_df is not None:
            before_interp_filtered_df = before_interpolation_df[before_interpolation_df['Station'] == station].copy()
            if var in before_interp_filtered_df.columns and pd.api.types.is_numeric_dtype(before_interp_filtered_df[var]):
                if resample_freq:
                    serie_before_interp = before_interp_filtered_df[var].resample(resample_freq).mean()
                else:
                    serie_before_interp = before_interp_filtered_df[var]

                serie_before_interp = serie_before_interp.dropna()
                if not serie_before_interp.empty:
                    # Normaliser en utilisant les mêmes min/max pour maintenir l'échelle
                    if max_val != min_val:
                        serie_norm_before_interp = (serie_before_interp - min_val) / (max_val - min_val)
                    else:
                        serie_norm_before_interp = serie_before_interp * 0 + 0.5
                    
                    traces.append(
                        go.Scatter(
                            x=serie_norm_before_interp.index,
                            y=serie_norm_before_interp,
                            name=f"{var_label} ({_('Avant interpolation')})",
                            mode='markers', # Utilise des marqueurs
                            marker=dict(color=color, size=5, symbol='x', opacity=0.8), # Symbole différent pour plus de clarté
                            hovertemplate=(
                                f"<b>{var_label} ({_('Avant interpolation')})</b><br>" +
                                str(_("Date")) + ": %{x|%d/%m/%Y}<br>" +
                                str(_("Valeur normalisée")) + ": %{y:.2f}<br>" +
                                str(_("Valeur originale")) + ": %{customdata[0]:.2f} " + var_unit +
                                "<extra></extra>"
                            ),
                            customdata=np.column_stack([serie_before_interp.values])
                        )
                    )

    if not traces:
        return go.Figure()

    # Titre et mise en page
    translated_periode = get_period_label(periode)
    fig = go.Figure(data=traces)
    fig.update_layout(
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

############   Fin de test avec les nouvelles fonctionnalites  #############




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




import pandas as pd
import plotly.express as px




import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go



import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import warnings
from flask_babel import _, lazy_gettext as _l



# def outliers_viz(df: pd.DataFrame, coef: float = 1.5) -> go.Figure:
#     """
#     Visualisation des outliers pour chaque variable numérique via des scatter plots Plotly.
#     """
#     df = df.copy()
#     if df.index.name == 'Datetime':
#         df.reset_index(inplace=True)
#     elif 'Datetime' not in df.columns:
#         return go.Figure().add_annotation(
#             x=0.5, y=0.5, text=_l("Colonne 'Datetime' manquante pour la visualisation des outliers."),
#             showarrow=False, font=dict(size=16)
#         )

#     # Conversion des colonnes numériques (hors colonnes à exclure)
#     exclude_cols = {
#         'Datetime', 'Station', 'Is_Daylight', 'Day', 
#         'sunrise_time_utc', 'sunset_time_utc', 'Daylight_Duration',
#         'Rain_01_mm', 'Rain_02_mm', 'Day_Duration'
#     }

#     for col in df.columns:
#         if col not in exclude_cols:
#             df[col] = pd.to_numeric(df[col], errors='coerce')

#     numeric_cols = [
#         col for col in df.select_dtypes(include='number').columns
#         if col not in exclude_cols
#     ]

#     if not numeric_cols:
#         return go.Figure().add_annotation(
#             x=0.5, y=0.5, text=_l("Aucune variable numérique pour l'analyse des outliers après nettoyage."),
#             showarrow=False, font=dict(size=16)
#         ).update_layout(title=_l("Analyse des Outliers"))

#     # Détection des outliers par station et par variable
#     df_out = df.copy()
#     for col in numeric_cols:
#         df_out[f'{col}_is_outlier'] = False

#     for station, group in df.groupby('Station'):
#         for col in numeric_cols:
#             if group[col].count() > 1:
#                 Q1, Q3 = group[col].quantile([0.25, 0.75])
#                 IQR = Q3 - Q1
#                 lower, upper = Q1 - coef * IQR, Q3 + coef * IQR
#                 outlier_idx = group[(group[col] < lower) | (group[col] > upper)].index
#                 df_out.loc[outlier_idx, f'{col}_is_outlier'] = True
#             else:
#                 warnings.warn(_l("Pas assez de données pour calculer les outliers pour la station '%s' et la variable '%s'.") % (station, col))

#     # Préparation des subplots
#     cols_per_row = 2
#     n = len(numeric_cols)
#     rows = (n + cols_per_row - 1) // cols_per_row

#     fig = make_subplots(rows=rows, cols=cols_per_row, subplot_titles=numeric_cols)

#     # Génération des traces
#     for i, col in enumerate(numeric_cols):
#         r, c = divmod(i, cols_per_row)
#         r += 1; c += 1

#         for outlier_status, color, label, marker_opts in [
#             (False, 'blue', _l('Valeurs normales'), dict(size=5)),
#             (True, 'red', _l('Outliers'), dict(size=7, symbol='circle-open', line=dict(width=2, color='red')))
#         ]:
#             subset = df_out[df_out[f'{col}_is_outlier'] == outlier_status]
#             fig.add_trace(
#                 go.Scatter(
#                     x=subset['Datetime'], y=subset[col],
#                     mode='markers',
#                     name=label,
#                     marker=dict(color=color, **marker_opts),
#                     hovertemplate=(
#                         "<b>Station:</b> %{customdata[0]}<br>"
#                         "<b>Date:</b> %{x|%Y-%m-%d %H:%M:%S}<br>"
#                         f"<b>{col}:</b> %{{y}}<br><extra></extra>"
#                     ),
#                     customdata=subset[['Station']],
#                     showlegend=(i == 0)  # Ne montrer qu'une fois la légende
#                 ),
#                 row=r, col=c
#             )

#         fig.update_xaxes(title_text=_l("Date"), row=r, col=c)
#         fig.update_yaxes(title_text=col, row=r, col=c)

#     # Mise en forme finale
#     fig.update_layout(
#         title=_l("Analyse des Outliers par Variable"),
#         height=400 * rows,
#         showlegend=True,
#         legend_title_text=_l("Statut d'Outlier")
#     )

#     return fig

# import pandas as pd
# import plotly.express as px
# from plotly.subplots import make_subplots
# import plotly.graph_objects as go




# def valeurs_manquantes_viz(df: pd.DataFrame) -> go.Figure:
#     """
#     Génère une figure Plotly contenant des diagrammes en secteurs
#     pour illustrer les pourcentages de valeurs manquantes par variable.
#     """
#     # Colonnes à exclure de l’analyse
#     exclude = {
#         'Datetime', 'Station', 'Rain_01_mm', 'Rain_02_mm',
#         'Is_Daylight', 'Day_Duration', 'sunrise_time_utc', 'sunset_time_utc', 'Day'
#     }

#     variables = [col for col in df.columns if col not in exclude]

#     if not variables:
#         return go.Figure().add_annotation(
#             x=0.5, y=0.5, text="Aucune variable à analyser pour les valeurs manquantes.",
#             showarrow=False, font=dict(size=16)
#         ).update_layout(title="Analyse des valeurs manquantes")

#     # Paramètres pour l'affichage
#     cols_per_row = 3
#     rows = (len(variables) + cols_per_row - 1) // cols_per_row
#     colors = ['lightgreen', 'lightcoral']

#     # Créer la figure avec sous-graphiques de type "camembert"
#     fig = make_subplots(
#         rows=rows,
#         cols=cols_per_row,
#         specs=[[{'type': 'domain'}]*cols_per_row]*rows,
#         subplot_titles=variables
#     )

#     for i, col in enumerate(variables):
#         present = df[col].count()
#         missing = df[col].isna().sum()
#         total = present + missing

#         if total == 0:
#             values = [1]
#             labels = ['Aucune donnée']
#             pie_colors = ['lightgray']
#             textinfo = 'none'
#         else:
#             values = [present, missing]
#             labels = ['Valeurs Présentes', 'Valeurs Manquantes']
#             pie_colors = colors
#             textinfo = 'percent'

#         r, c = divmod(i, cols_per_row)
#         fig.add_trace(
#             go.Pie(
#                 labels=labels,
#                 values=values,
#                 name=col,
#                 marker=dict(colors=pie_colors),
#                 textinfo=textinfo,
#                 hovertemplate="<b>%{label}</b><br>Nombre: %{value}<br>Pourcentage: %{percent:.2%}<extra></extra>",
#                 showlegend=True if i == 0 else False  # Une seule légende affichée
#             ),
#             row=r + 1,
#             col=c + 1
#         )

#     # Mise en page globale
#     fig.update_layout(
#         title_text="Pourcentage de valeurs manquantes par variable",
#         showlegend=True,
#         legend=dict(
#             orientation="h", yanchor="bottom", y=-0.15,
#             xanchor="center", x=0.5
#         ),
#         height=350 * rows,
#         margin=dict(l=50, r=50, b=100, t=80)
#     )

#     # Centrer les titres de sous-plots
#     for ann in fig.layout.annotations:
#         ann.update(x=ann.x)

#     return fig




################## Fonction d'interpolation #################

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


########### Fin de la fonction d'interpolation #######################

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



# ############# Fonction pour visualiser les plages des valeurs manquantes #####################
# import pandas as pd
# import numpy as np
# import plotly.graph_objects as go
# from plotly.subplots import make_subplots
# import warnings
# from flask_babel import _, lazy_gettext as _l # Assuming these imports are correct

# def gaps_time_series_viz(df_data: pd.DataFrame, df_gaps: pd.DataFrame, station_name: str, title_suffix: str = "") -> go.Figure:
#     """
#     Generates a Plotly figure showing numerical time series data
#     with highlighted periods of missing values (gaps),
#     where gap details are displayed in tooltips.

#     Args:
#         df_data (pd.DataFrame): The data DataFrame (raw or interpolated) for a given station.
#                                 Must have a DatetimeIndex and a 'Station' column.
#         df_gaps (pd.DataFrame): The DataFrame of missing ranges (e.g., GLOBAL_MISSING_VALUES_BEFORE/AFTER_INTERPOLATION_DF)
#                                 filtered for the current station.
#                                 Must contain 'station', 'variable', 'start_time', 'end_time', and 'duration_hours'.
#         station_name (str): The name of the station currently being visualized.
#         title_suffix (str): A suffix to add to the chart title (e.g., "Avant Interpolation").

#     Returns:
#         go.Figure: The Plotly figure.
#     """
#     # --- Initial Data Validation ---
#     if df_data.empty:
#         fig = go.Figure().add_annotation(
#             x=0.5, y=0.5, text=str(_l("Aucune donnée disponible pour la station sélectionnée.")),
#             showarrow=False, font=dict(size=16)
#         )
#         fig.update_layout(title=str(f"{_l('Visualisation des données et des gaps')} - {station_name} {title_suffix}"),
#                           template="plotly_white")
#         return fig

#     # Ensure DatetimeIndex is present
#     if not isinstance(df_data.index, pd.DatetimeIndex):
#         warnings.warn(str(_l("Le DataFrame de données n'a pas un DatetimeIndex. Tentative de conversion.")))
#         try:
#             df_data.index = pd.to_datetime(df_data.index, errors='coerce')
#             df_data = df_data[df_data.index.notna()]
#         except Exception:
#             fig = go.Figure().add_annotation(
#                 x=0.5, y=0.5, text=str(_l("L'index du DataFrame n'est pas un DatetimeIndex valide et ne peut pas être converti.")),
#                 showarrow=False, font=dict(size=16)
#             )
#             fig.update_layout(title=str(f"{_l('Visualisation des données et des gaps')} - {station_name} {title_suffix}"),
#                               template="plotly_white")
#             return fig
#         if df_data.empty: # Check again after conversion and NaN removal
#             fig = go.Figure().add_annotation(
#                 x=0.5, y=0.5, text=str(_l("Aucune donnée valide disponible après conversion de l'index en DatetimeIndex.")),
#                 showarrow=False, font=dict(size=16)
#             )
#             fig.update_layout(title=str(f"{_l('Visualisation des données et des gaps')} - {station_name} {title_suffix}"),
#                               template="plotly_white")
#             return fig


#     # Define columns to exclude from visualization (non-numeric or flags)
#     # Using a set for faster lookups
#     EXCLUDE_COLS_VIZ = {
#         'Station', 'Is_Daylight', 'Rain_01_mm', 'Rain_02_mm', 'Wind_Dir_Deg',
#         'sunrise_time_utc', 'sunset_time_utc', 'Daylight_Duration',
#         'Date_UTC_Naive', 'Lat', 'Long', 'Timezone',
#         'sunrise_time_utc_calc', 'sunset_time_utc_calc',
#         'Daylight_Duration_h_calc', 'fixed_daylight_applied',
#         'Year', 'Month', 'Minute', 'Hour', 'Date', 'Day' # Also exclude derived date components
#     }

#     # Identify numerical columns for visualization
#     numerical_cols_for_viz = [
#         col for col in df_data.columns
#         if pd.api.types.is_numeric_dtype(df_data[col]) and col not in EXCLUDE_COLS_VIZ
#     ]
    
#     if not numerical_cols_for_viz:
#         fig = go.Figure().add_annotation(
#             x=0.5, y=0.5, text=str(_l("Aucune variable numérique exploitable pour la visualisation des gaps.")),
#             showarrow=False, font=dict(size=16)
#         )
#         fig.update_layout(title=str(f"{_l('Visualisation des données et des gaps')} - {station_name} {title_suffix}"),
#                           template="plotly_white")
#         return fig

#     # --- Subplot Configuration ---
#     num_variables = len(numerical_cols_for_viz)
#     rows = num_variables # One row per variable
#     cols_per_row = 1

#     fig = make_subplots(
#         rows=rows,
#         cols=cols_per_row,
#         shared_xaxes=True, # Share x-axis across all subplots
#         vertical_spacing=0.04, # Slightly reduce spacing
#         subplot_titles=[str(f"{_l('Série temporelle')} - {col}") for col in numerical_cols_for_viz]
#     )

#     # --- Filter Gaps Data Once ---
#     # Filter df_gaps for the current station and relevant numerical columns only once
#     relevant_gaps_filtered = df_gaps[
#         (df_gaps['station'] == station_name) & 
#         (df_gaps['variable'].isin(numerical_cols_for_viz))
#     ].copy()
    
#     # Ensure start_time and end_time are datetime objects in df_gaps for comparison
#     relevant_gaps_filtered['start_time'] = pd.to_datetime(relevant_gaps_filtered['start_time'], errors='coerce')
#     relevant_gaps_filtered['end_time'] = pd.to_datetime(relevant_gaps_filtered['end_time'], errors='coerce')
#     relevant_gaps_filtered.dropna(subset=['start_time', 'end_time'], inplace=True) # Drop invalid gap entries

#     # --- Add Traces and Gap Highlights ---
#     for i, col in enumerate(numerical_cols_for_viz):
#         row_idx = i + 1

#         # Add data trace
#         fig.add_trace(
#             go.Scatter(
#                 x=df_data.index,
#                 y=df_data[col],
#                 mode='lines+markers',
#                 name=str(f"{col} - {_l('Données')}"),
#                 line=dict(color='blue', width=1),
#                 marker=dict(size=3, opacity=0.7),
#                 hovertemplate="<b>%{x|%Y-%m-%d %H:%M}</b><br>" +
#                                f"{col}: %{{y:.2f}}<extra></extra>", # Format y value
#                 showlegend=False
#             ),
#             row=row_idx, col=1
#         )

#         # Get gaps specific to this column
#         gaps_for_col = relevant_gaps_filtered[relevant_gaps_filtered['variable'] == col]

#         if not gaps_for_col.empty:
#             # Determine y-axis range for the current subplot to make shapes scale
#             y_min = df_data[col].min()
#             y_max = df_data[col].max()
#             # Add a small buffer to y_min/y_max if data is constant or has very small range
#             if pd.isna(y_min) or pd.isna(y_max) or y_max == y_min:
#                 y_min, y_max = 0, 1 # Fallback to a default range
#             else:
#                 y_buffer = (y_max - y_min) * 0.05 # 5% buffer
#                 y_min -= y_buffer
#                 y_max += y_buffer

#             # Add a single shape for each gap
#             for _, gap_row in gaps_for_col.iterrows():
#                 fig.add_shape(
#                     type="rect",
#                     xref="x", yref="y", # Use 'y' coordinates, not 'paper'
#                     x0=gap_row['start_time'],
#                     y0=y_min, # Scale with actual y-axis data range
#                     y1=y_max, # Scale with actual y-axis data range
#                     x1=gap_row['end_time'],
#                     fillcolor="rgba(255,0,0,0.2)", # Light red transparent fill
#                     line_width=0,
#                     layer="below", # Place below the data trace
#                     row=row_idx, col=1
#                 )
                
#                 # --- Optimized Hover Info for Gaps ---
#                 # Add an invisible scatter trace over the gap area for hover text
#                 # We place a single marker in the middle of the gap for hover info.
#                 gap_mid_time = gap_row['start_time'] + (gap_row['end_time'] - gap_row['start_time']) / 2
                
#                 # Use a small non-NaN value for y to ensure it's visible, but it won't be plotted visually
#                 y_for_hover = df_data[col].mean() if df_data[col].notna().any() else 0 
                
#                 hover_text_gap = str(f"<b>{_l('Période Manquante')}</b><br>" +
#                                      f"{_l('Variable')}: {gap_row['variable']}<br>" +
#                                      f"{_l('Début')}: {gap_row['start_time'].strftime('%Y-%m-%d %H:%M')}<br>" +
#                                      f"{_l('Fin')}: {gap_row['end_time'].strftime('%Y-%m-%d %H:%M')}<br>" +
#                                      f"{_l('Durée')}: {gap_row['duration_hours']:.2f} {_l('heures')}")

#                 fig.add_trace(
#                     go.Scatter(
#                         x=[gap_mid_time],
#                         y=[y_for_hover], # A single point for hover, visually hidden
#                         mode='markers',
#                         marker=dict(size=1, opacity=0), # Invisible marker
#                         showlegend=False,
#                         hoverinfo='text',
#                         text=hover_text_gap,
#                         name=f"{col} {_l('Gap Info')}" # Give it a name for internal tracking
#                     ),
#                     row=row_idx, col=1
#                 )

#     # --- Update Layout ---
#     fig.update_layout(
#         title_text=str(f"{_l('Séries temporelles avec périodes manquantes/interpolées')} - {station_name} {title_suffix}"),
#         height=max(400, 300 * rows), # Dynamic height with a minimum
#         hovermode="x unified", # Excellent for time series analysis
#         margin=dict(l=50, r=50, b=80, t=100), # Adjust margins
#         template="plotly_white", # Clean template
#     )

#     # Update Y-axis titles
#     for i, col in enumerate(numerical_cols_for_viz):
#         fig.update_yaxes(title_text=col, row=i+1, col=1, rangemode='tozero') # rangemode='tozero' often good for time series

#     # Update X-axis (shared)
#     fig.update_xaxes(title_text=str(_l("Temps")), showgrid=True, gridwidth=1, gridcolor='LightGray')

#     return fig


# ############# Fin de la Fonction pour visualiser les plages des valeurs manquantes #####################





############# Debut du code fonctionnel pour  les statistiques  ##########################





import logging
from flask_babel import Babel, _, lazy_gettext as _l, get_locale as get_current_locale


def _calculate_rainy_season_stats_yearly(df_daily_rain_station: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les statistiques de la saison des pluies (début, fin, durée, moyenne) sur une base annuelle.
    """
    RAIN_SEASON_GAP_THRESHOLD = pd.Timedelta(days=60)
    NON_RELEVANT_RAIN_THRESHOLD_DAYS = 45
    yearly_season_stats = []

    # Ensure Datetime is the index for time-series operations
    if 'Datetime' in df_daily_rain_station.columns:
        df_daily_rain_station = df_daily_rain_station.set_index('Datetime').sort_index()

    for year, year_df in df_daily_rain_station.groupby(df_daily_rain_station.index.year):
        # Filter for actual rain events within the current year's data
        rain_events = year_df[year_df['Rain_mm'] > 0].index

        # Initialize default values for the year
        year_stat = {
            'Year': year,
            'Moyenne Saison Pluvieuse': np.nan,
            'Début Saison Pluvieuse': pd.NaT,
            'Fin Saison Pluvieuse': pd.NaT,
            'Durée Saison Pluvieuse Jours': np.nan
        }

        # If there are no rain events or too few to define a season, skip to next year
        if rain_events.empty or len(rain_events) < 2:
            yearly_season_stats.append(year_stat)
            continue

        # Group rain events into potential blocks based on the gap threshold
        block_ids = (rain_events.to_series().diff() > RAIN_SEASON_GAP_THRESHOLD).cumsum()
        valid_blocks = {}

        for block_id, rain_dates_in_block in rain_events.to_series().groupby(block_ids):
            if not rain_dates_in_block.empty:
                block_start = rain_dates_in_block.min()
                block_end = rain_dates_in_block.max()

                # Get all daily data within this block
                full_block_df = year_df.loc[block_start:block_end]

                # A block is considered "valid" if it contains at least two rain days
                if len(full_block_df[full_block_df['Rain_mm'] > 0]) > 1:
                    valid_blocks[block_id] = full_block_df

        if not valid_blocks:
            yearly_season_stats.append(year_stat)
            continue

        # Determine the main season as the longest duration block
        main_block_id = max(valid_blocks, key=lambda k: (valid_blocks[k].index.max() - valid_blocks[k].index.min()).days)
        main_season_df = valid_blocks[main_block_id]

        # Get sorted rain event dates within the identified main season
        rain_events_in_main_block = main_season_df[main_season_df['Rain_mm'] > 0].index.sort_values()

        # Determine start of the season
        debut_saison = pd.NaT
        if len(rain_events_in_main_block) >= 2:
            first_rain_date = rain_events_in_main_block[0]
            second_rain_date = rain_events_in_main_block[1]
            # If gap between first two rains is too large, second rain defines start
            if (second_rain_date - first_rain_date).days > NON_RELEVANT_RAIN_THRESHOLD_DAYS:
                debut_saison = second_rain_date
            else:
                debut_saison = first_rain_date
        elif len(rain_events_in_main_block) == 1:
            # If only one rain event, that's the start
            debut_saison = rain_events_in_main_block[0]
        # Else: debut_saison remains pd.NaT if no or insufficient rain events

        # Determine end of the season
        fin_saison = pd.NaT
        if len(rain_events_in_main_block) >= 2:
            last_rain_date = rain_events_in_main_block[-1]
            second_last_rain_date = rain_events_in_main_block[-2]
            # If gap before last two rains is too large, second to last rain defines end
            if (last_rain_date - second_last_rain_date).days > NON_RELEVANT_RAIN_THRESHOLD_DAYS:
                fin_saison = second_last_rain_date
            else:
                fin_saison = last_rain_date
        elif len(rain_events_in_main_block) == 1:
            # If only one rain event, that's the end
            fin_saison = rain_events_in_main_block[0]
        # Else: fin_saison remains pd.NaT

        total_days = np.nan
        moyenne_saison = np.nan

        if pd.notna(debut_saison) and pd.notna(fin_saison) and debut_saison <= fin_saison:
            total_days = (fin_saison - debut_saison).days + 1
            if total_days > 0:
                season_rainfall_sum = year_df.loc[debut_saison:fin_saison]['Rain_mm'].sum()
                moyenne_saison = season_rainfall_sum / total_days
            else: # If total_days is 0 (start and end are the same day), handle as no duration
                total_days = np.nan

        yearly_season_stats.append({
            'Year': year,
            'Moyenne Saison Pluvieuse': moyenne_saison,
            'Début Saison Pluvieuse': debut_saison,
            'Fin Saison Pluvieuse': fin_saison,
            'Durée Saison Pluvieuse Jours': total_days
        })

    return pd.DataFrame(yearly_season_stats).set_index('Year')

def _calculate_consecutive_dry_days_yearly(df_daily_rain_station: pd.DataFrame, df_season_stats: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule la plus longue série de jours sans pluie (consécutifs) *dans la saison pluvieuse* pour chaque année.
    Retourne aussi les dates de début et de fin (pluies encadrantes) de cette période sèche.
    """
    yearly_consecutive_dry_days = []
    if 'Datetime' in df_daily_rain_station.columns:
        df_daily_rain_station = df_daily_rain_station.set_index('Datetime').sort_index()

    for year, year_df in df_daily_rain_station.groupby(df_daily_rain_station.index.year):
        season_start = df_season_stats.loc[year, 'Début Saison Pluvieuse'] if year in df_season_stats.index else pd.NaT
        season_end = df_season_stats.loc[year, 'Fin Saison Pluvieuse'] if year in df_season_stats.index else pd.NaT

        longest_dry_spell_in_season = 0
        longest_dry_spell_start_rain_date = pd.NaT # Date de la pluie précédente
        longest_dry_spell_end_rain_date = pd.NaT # Date de la pluie suivante

        if pd.notna(season_start) and pd.notna(season_end) and season_start <= season_end:
            # Filter daily data to be strictly within the rainy season
            daily_data_in_season = year_df.loc[season_start:season_end]

            # Resample to ensure a continuous daily series, filling missing days with 0 rain
            full_daily_series_in_season = daily_data_in_season['Rain_mm'].resample('D').sum().fillna(0)

            # Identify rain events (days with > 0 rain) within the season
            rainy_days_in_season_dates = full_daily_series_in_season[full_daily_series_in_season > 0].index.sort_values()

            if len(rainy_days_in_season_dates) >= 2:
                for i in range(len(rainy_days_in_season_dates) - 1):
                    rain_day1_date = rainy_days_in_season_dates[i]
                    rain_day2_date = rainy_days_in_season_dates[i+1]

                    # Calculate gap: number of dry days between two consecutive rain days
                    gap = (rain_day2_date - rain_day1_date).days - 1

                    if gap > longest_dry_spell_in_season:
                        longest_dry_spell_in_season = gap
                        longest_dry_spell_start_rain_date = rain_day1_date
                        longest_dry_spell_end_rain_date = rain_day2_date

        yearly_consecutive_dry_days.append({
            'Year': year,
            'Durée Jours Sans Pluie': longest_dry_spell_in_season,
            'Jours Sans Pluie_Date_Debut_Pluie_Prec': longest_dry_spell_start_rain_date,
            'Jours Sans Pluie_Date_Fin_Pluie_Suiv': longest_dry_spell_end_rain_date
        })

    return pd.DataFrame(yearly_consecutive_dry_days).set_index('Year')

def _calculate_dry_spell_stats_yearly(df_daily_rain_station: pd.DataFrame, df_season_stats: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule la plus longue période de sécheresse définie (selon la nouvelle logique basée sur le ratio P1/j)
    *dans la saison pluvieuse* pour chaque année.
    Retourne aussi les dates de début et de fin de cette sécheresse définie.
    """
    yearly_dry_spell_events = []
    if 'Datetime' in df_daily_rain_station.columns:
        df_daily_rain_station = df_daily_rain_station.set_index('Datetime').sort_index()

    for year, year_df in df_daily_rain_station.groupby(df_daily_rain_station.index.year):
        season_start = df_season_stats.loc[year, 'Début Saison Pluvieuse'] if year in df_season_stats.index else pd.NaT
        season_end = df_season_stats.loc[year, 'Fin Saison Pluvieuse'] if year in df_season_stats.index else pd.NaT
        saison_moyenne_annual = df_season_stats.loc[year, 'Moyenne Saison Pluvieuse'] if year in df_season_stats.index and pd.notna(df_season_stats.loc[year, 'Moyenne Saison Pluvieuse']) else np.nan

        longest_defined_dry_spell_duration = 0
        longest_defined_dry_spell_start_date = pd.NaT
        longest_defined_dry_spell_end_date = pd.NaT # Date de la veille du 2e jour de pluie consécutif

        # If season cannot be defined or mean is invalid, skip
        if pd.isna(season_start) or pd.isna(season_end) or season_start >= season_end or pd.isna(saison_moyenne_annual) or saison_moyenne_annual <= 0:
            yearly_dry_spell_events.append({
                'Year': year,
                'Durée Sécheresse Définie Jours': longest_defined_dry_spell_duration, # Should be 0 here
                'Sécheresse_Date_Debut': longest_defined_dry_spell_start_date, # Should be NaT
                'Sécheresse_Date_Fin': longest_defined_dry_spell_end_date # Should be NaT
            })
            continue

        # Filter and resample daily data for the season
        daily_data_in_season = year_df.loc[season_start:season_end]
        full_daily_series_in_season = daily_data_in_season['Rain_mm'].resample('D').sum().fillna(0)
        rainy_days_in_season_dates = full_daily_series_in_season[full_daily_series_in_season > 0].index.sort_values()

        if len(rainy_days_in_season_dates) < 2:
            yearly_dry_spell_events.append({
                'Year': year,
                'Durée Sécheresse Définie Jours': longest_defined_dry_spell_duration, # Should be 0
                'Sécheresse_Date_Debut': longest_defined_dry_spell_start_date, # Should be NaT
                'Sécheresse_Date_Fin': longest_defined_dry_spell_end_date # Should be NaT
            })
            continue

        for i in range(len(rainy_days_in_season_dates) - 1):
            rain_day1_date = rainy_days_in_season_dates[i]
            rain_day2_date = rainy_days_in_season_dates[i+1]

            p1_value = full_daily_series_in_season.get(rain_day1_date, 0)

            # Skip if P1 is zero, as it cannot initiate the defined dry spell logic
            if p1_value <= 0:
                continue

            current_date_in_gap = rain_day1_date + timedelta(days=1)
            dry_day_count_in_gap = 1 # 'j' starts from 1 for the first dry day

            debut_secheresse_candidate = pd.NaT
            duree_secheresse_candidate = 0

            # Loop through the dry days between rain_day1 and rain_day2
            while current_date_in_gap < rain_day2_date:
                current_ratio = p1_value / dry_day_count_in_gap

                if current_ratio < saison_moyenne_annual:
                    debut_secheresse_candidate = current_date_in_gap
                    fin_secheresse_candidate = rain_day2_date - timedelta(days=1) # The day before the second rain day
                    duree_secheresse_candidate = (fin_secheresse_candidate - debut_secheresse_candidate).days + 1
                    break # Found the start of the dry spell for this pair, exit inner loop

                current_date_in_gap += timedelta(days=1)
                dry_day_count_in_gap += 1

            # Update the longest duration and its associated dates if a candidate was found
            if duree_secheresse_candidate > longest_defined_dry_spell_duration:
                longest_defined_dry_spell_duration = duree_secheresse_candidate
                longest_defined_dry_spell_start_date = debut_secheresse_candidate
                longest_defined_dry_spell_end_date = fin_secheresse_candidate

        yearly_dry_spell_events.append({
            'Year': year,
            'Durée Sécheresse Définie Jours': longest_defined_dry_spell_duration,
            'Sécheresse_Date_Debut': longest_defined_dry_spell_start_date,
            'Sécheresse_Date_Fin': longest_defined_dry_spell_end_date
        })

    return pd.DataFrame(yearly_dry_spell_events).set_index('Year')

# --- Modified generate_plot_stats_over_period_plotly function ---

def generate_plot_stats_over_period_plotly(df: pd.DataFrame, variable: str, station_colors: dict, time_frequency: str = 'yearly', df_original: pd.DataFrame = None, logger: logging.Logger = None) -> go.Figure:
    """
    Génère des graphiques de statistiques agrégées par période (Annuelle, Mensuelle, Hebdomadaire, Journalière)
    pour une variable donnée et plusieurs stations.
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        logger.addHandler(logging.NullHandler())

    plot_title_prefix = _("Statistiques")

    try:
        df_processed = df.copy()
        if 'Datetime' not in df_processed.columns and isinstance(df_processed.index, pd.DatetimeIndex):
            df_processed = df_processed.reset_index()

        df_processed['Datetime'] = pd.to_datetime(df_processed['Datetime'], errors='coerce')
        df_processed['Year'] = df_processed['Datetime'].dt.year
        df_processed = df_processed.dropna(subset=['Datetime', 'Station', variable])

        df_original_for_extremes = None
        if df_original is not None:
            df_original_processed = df_original.copy()
            if 'Datetime' not in df_original_processed.columns and isinstance(df_original_processed.index, pd.DatetimeIndex):
                df_original_processed = df_original_processed.reset_index()
            df_original_processed['Datetime'] = pd.to_datetime(df_original_processed['Datetime'], errors='coerce')
            df_original_processed = df_original_processed.dropna(subset=['Datetime', 'Station'])
            df_original_processed[variable] = pd.to_numeric(df_original_processed[variable], errors='coerce')
            df_original_for_extremes = df_original_processed.dropna(subset=[variable])
        else:
            df_original_for_extremes = df_processed.copy() # Use processed if original not provided

        # Drop any supplementary columns if they exist (e.g., from previous data processing)
        col_sup = ['Rain_01_mm', 'Rain_02_mm']
        for col_name in col_sup:
            if col_name in df_processed.columns: df_processed = df_processed.drop(col_name, axis=1)
            if df_original_for_extremes is not None and col_name in df_original_for_extremes.columns: df_original_for_extremes = df_original_for_extremes.drop(col_name, axis=1)

        # Basic check for empty dataframe before proceeding
        if df_processed.empty:
            logger.warning(f"df_processed is empty after initial filtering for variable {variable}.")
            return go.Figure().add_annotation(x=0.5, y=0.5, text=_("Aucune donnée valide pour la période ou la variable sélectionnée."), showarrow=False, font=dict(size=16)).update_layout(title=_("Statistiques par période"))

        # Ensure the variable column is numeric
        df_processed[variable] = pd.to_numeric(df_processed[variable], errors='coerce')
        df_processed = df_processed.dropna(subset=[variable])

        if df_processed.empty:
            logger.warning(f"df_processed is empty after numeric conversion of {variable}.")
            return go.Figure().add_annotation(x=0.5, y=0.5, text=_("Aucune donnée valide après la conversion numérique."), showarrow=False, font=dict(size=16)).update_layout(title=_("Statistiques par période"))

        var_meta = METADATA_VARIABLES.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
        var_label = str(get_var_label(var_meta, 'Nom'))
        var_unit = str(get_var_label(var_meta, 'Unite'))
        is_rain_variable = var_meta.get('is_rain', False)

        period_title_map = {
            'yearly': get_metric_label('Annuelle'),
            'monthly': get_metric_label('Mensuelle'),
            'weekly': get_metric_label('Hebdomadaire'),
            'daily': get_metric_label('Journalière')
        }
        plot_base_title = _("Statistiques {} de {} par Station").format(
            period_title_map.get(time_frequency, time_frequency.capitalize()),
            var_label
        )

        unique_stations = sorted(df_processed['Station'].unique())
        unique_years = sorted(df_processed['Year'].unique()) # Get all unique years present in the data

        # Calculate overall data period for hover texts
        start_date_global = df_processed['Datetime'].min()
        end_date_global = df_processed['Datetime'].max()
        global_period_str = ""
        if pd.notna(start_date_global) and pd.notna(end_date_global):
            global_period_str = _("Période de Données: de {} à {}").format(start_date_global.year, end_date_global.year) if start_date_global.year != end_date_global.year else _("Période de Données: Année {}").format(start_date_global.year)


        # --- SPECIAL HANDLING FOR YEARLY RAINFALL STATS ---
        if time_frequency == 'yearly' and is_rain_variable:
            all_station_yearly_data = []
            all_station_season_stats = {} # Store season stats for potentially filtering other periods

            for station_name in unique_stations:
                station_df_full = df_processed[df_processed['Station'] == station_name]

                # Create a template DataFrame for all years in the dataset, ensuring all years are present for each station
                station_yearly_template = pd.DataFrame({'Year': unique_years}).set_index('Year')

                # Filter for actual rain data for this station for daily operations
                # Ensure daily data for rainfall calculations
                station_rain_data_daily = station_df_full.groupby(pd.Grouper(key='Datetime', freq='D'))['Rain_mm'].sum().reset_index()


                # Calculate annual cumulative rain
                annual_cumulative_rain = station_df_full.groupby('Year')['Rain_mm'].sum().reset_index(name='Cumul Annuel')


                # Number of rainy days per year (days with Rain_mm > 0)
                annual_rainy_days_count = station_df_full[station_df_full['Rain_mm'] > 0].groupby('Year').size().reset_index(name='Nombre Jours Pluvieux')


                # Calculate Precipitation moyenne des jours pluvieux: Cumul annuel / Nombre de jours pluvieux
                annual_mean_rainy_days_calculated = pd.merge(annual_cumulative_rain, annual_rainy_days_count, on='Year', how='left')
                annual_mean_rainy_days_calculated['Moyenne Jours Pluvieux'] = annual_mean_rainy_days_calculated.apply(
                    lambda row: row['Cumul Annuel'] / row['Nombre Jours Pluvieux'] if row['Nombre Jours Pluvieux'] > 0 else np.nan, axis=1
                )


                # Calculate season and dry spell stats (these may produce NaNs if conditions aren't met)
                annual_season_stats = _calculate_rainy_season_stats_yearly(station_rain_data_daily)
                all_station_season_stats[station_name] = annual_season_stats # Store for later use by other frequencies

                annual_dry_spell_stats = _calculate_dry_spell_stats_yearly(station_rain_data_daily, annual_season_stats)
                annual_consecutive_dry_days_stats = _calculate_consecutive_dry_days_yearly(station_rain_data_daily, annual_season_stats)


                # Max/Min daily rain on rainy days (only days with Rain_mm > 0)
                annual_max_rain_date = station_df_full[station_df_full['Rain_mm'] > 0].loc[station_df_full[station_df_full['Rain_mm'] > 0].groupby('Year')['Rain_mm'].idxmax()]
                annual_min_rain_date = station_df_full[station_df_full['Rain_mm'] > 0].loc[station_df_full[station_df_full['Rain_mm'] > 0].groupby('Year')['Rain_mm'].idxmin()]

                annual_max_min_rainy_days = annual_max_rain_date[['Year', 'Rain_mm', 'Datetime']].rename(columns={'Rain_mm': 'Valeur Max Jours Pluvieux Annuelle', 'Datetime': 'Date Max Pluvieux Annuelle'})
                annual_max_min_rainy_days = annual_max_min_rainy_days.merge(
                    annual_min_rain_date[['Year', 'Rain_mm', 'Datetime']].rename(columns={'Rain_mm': 'Valeur Min Jours Pluvieux Annuelle', 'Datetime': 'Date Min Pluvieux Annuelle'}),
                    on='Year', how='outer'
                )


                # Merge all calculated annual stats into a single DataFrame for the current station
                yearly_combined_data = station_yearly_template \
                    .merge(annual_cumulative_rain.set_index('Year'), how='left', left_index=True, right_index=True) \
                    .merge(annual_mean_rainy_days_calculated.set_index('Year')[['Moyenne Jours Pluvieux']], how='left', left_index=True, right_index=True) \
                    .merge(annual_season_stats, how='left', left_index=True, right_index=True) \
                    .merge(annual_dry_spell_stats, how='left', left_index=True, right_index=True) \
                    .merge(annual_consecutive_dry_days_stats, how='left', left_index=True, right_index=True) \
                    .merge(annual_max_min_rainy_days.set_index('Year'), how='left', left_index=True, right_index=True) \
                    .reset_index() # Reset index to make 'Year' a column again

                yearly_combined_data['Station'] = station_name
                all_station_yearly_data.append(yearly_combined_data)

            df_yearly_metrics = pd.concat(all_station_yearly_data, ignore_index=True)

            # Define subplot titles and order for existing plots
            subplot_titles_ordered_existing = [
                get_metric_label("Cumul Annuel des Précipitations"),
                get_metric_label("Précipitation Moyenne des Jours Pluvieux"),
                get_metric_label("Précipitation Moyenne de la Saison Pluvieuse"),
                get_metric_label("Jour le plus pluvieux"),
                get_metric_label("Jour le moins pluvieux"),
                get_metric_label("Durée de la Saison Pluvieuse"),
                get_metric_label("Plus Longue Durée des Jours Sans Pluie"),
                get_metric_label("Durée de la Sécheresse Définie")
            ]

            fig_existing = make_subplots(rows=4, cols=2, subplot_titles=subplot_titles_ordered_existing,
                                vertical_spacing=0.08, horizontal_spacing=0.05)


            # Map metrics to their display names and corresponding columns in df_yearly_metrics for existing plots
            metrics_to_plot_info_existing = [
                {'metric_col': 'Cumul Annuel', 'title': get_metric_label("Cumul Annuel des Précipitations"), 'row': 1, 'col': 1, 'unit': var_unit},
                {'metric_col': 'Moyenne Jours Pluvieux', 'title': get_metric_label("Précipitation Moyenne des Jours Pluvieux"), 'row': 1, 'col': 2, 'unit': var_unit},
                {'metric_col': 'Moyenne Saison Pluvieuse', 'title': get_metric_label("Précipitation Moyenne de la Saison Pluvieuse"), 'row': 2, 'col': 1, 'unit': var_unit},
                {'metric_col': 'Valeur Max Jours Pluvieux Annuelle', 'title': get_metric_label("Jour le plus pluvieux"), 'row': 2, 'col': 2, 'unit': var_unit, 'date_col': 'Date Max Pluvieux Annuelle'},
                {'metric_col': 'Valeur Min Jours Pluvieux Annuelle', 'title': get_metric_label("Jour le moins pluvieux"), 'row': 3, 'col': 1, 'unit': var_unit, 'date_col': 'Date Min Pluvieux Annuelle'},
                {'metric_col': 'Durée Saison Pluvieuse Jours', 'title': get_metric_label("Durée de la Saison Pluvieuse"), 'row': 3, 'col': 2, 'unit': get_metric_label("jours")},
                {'metric_col': 'Durée Jours Sans Pluie', 'title': get_metric_label("Plus Longue Durée des Jours Sans Pluie"), 'row': 4, 'col': 1, 'unit': get_metric_label("jours")},
                {'metric_col': 'Durée Sécheresse Définie Jours', 'title': get_metric_label("Durée de la Sécheresse Définie"), 'row': 4, 'col': 2, 'unit': get_metric_label("jours")}
            ]

            for plot_info in metrics_to_plot_info_existing:
                metric = plot_info['metric_col']
                row, col = plot_info['row'], plot_info['col']
                unit = plot_info['unit']
                date_col = plot_info.get('date_col')

                for station_name in unique_stations:
                    station_data = df_yearly_metrics[df_yearly_metrics['Station'] == station_name].copy()

                    station_data[metric] = station_data[metric].fillna(0)

                    y_vals, x_vals = station_data[metric].values, station_data['Year'].values

                    hover_texts = []
                    for idx, val in enumerate(y_vals):
                        year = x_vals[idx]

                        hover_text = f"<b>{_('Station')}:</b> {station_name}<br>" \
                                     f"<b>{plot_info['title']}:</b> {val:.2f} {unit}<br>" \
                                     f"<b>{_('Année')}:</b> {year}"

                        if date_col and pd.notna(station_data.loc[station_data['Year'] == year, date_col].iloc[0]):
                            exact_date = station_data.loc[station_data['Year'] == year, date_col].iloc[0]
                            hover_text += f"<br><b>{_('Date')}:</b> {exact_date.strftime('%d/%m/%Y')}"

                        if metric == 'Durée Saison Pluvieuse Jours':
                            debut_saison_date = station_data.loc[station_data['Year'] == year, 'Début Saison Pluvieuse'].iloc[0]
                            fin_saison_date = station_data.loc[station_data['Year'] == year, 'Fin Saison Pluvieuse'].iloc[0]
                            if pd.notna(debut_saison_date) and pd.notna(fin_saison_date):
                                hover_text += f"<br><b>{_('Période')}:</b> {debut_saison_date.strftime('%d/%m/%Y')} - {fin_saison_date.strftime('%d/%m/%Y')}"
                        elif metric == 'Durée Jours Sans Pluie':
                            debut_pluie_prec = station_data.loc[station_data['Year'] == year, 'Jours Sans Pluie_Date_Debut_Pluie_Prec'].iloc[0]
                            fin_pluie_suiv = station_data.loc[station_data['Year'] == year, 'Jours Sans Pluie_Date_Fin_Pluie_Suiv'].iloc[0]
                            if pd.notna(debut_pluie_prec) and pd.notna(fin_pluie_suiv):
                                hover_text += f"<br><b>{_('Pluie Précédente')}:</b> {debut_pluie_prec.strftime('%d/%m/%Y')}"
                                hover_text += f"<br><b>{_('Pluie Suivante')}:</b> {fin_pluie_suiv.strftime('%d/%m/%Y')}"
                        elif metric == 'Durée Sécheresse Définie Jours':
                            debut_secheresse_def = station_data.loc[station_data['Year'] == year, 'Sécheresse_Date_Debut'].iloc[0]
                            fin_secheresse_def = station_data.loc[station_data['Year'] == year, 'Sécheresse_Date_Fin'].iloc[0]
                            if pd.notna(debut_secheresse_def) and pd.notna(fin_secheresse_def):
                                hover_text += f"<br><b>{_('Début Sécheresse')}:</b> {debut_secheresse_def.strftime('%d/%m/%Y')}"
                                hover_text += f"<br><b>{_('Fin Sécheresse')}:</b> {fin_secheresse_def.strftime('%d/%m/%Y')}"

                        if global_period_str: hover_text += f"<br>{global_period_str}"

                        hover_texts.append(hover_text)

                    current_color = station_colors.get(station_name, '#1f77b4')
                    fig_existing.add_trace(go.Bar(x=x_vals, y=y_vals, marker_color=current_color, name=f"{station_name}", hoverinfo='text', hovertext=hover_texts, showlegend=False), row=row, col=col)


            for station_name in unique_stations:
                fig_existing.add_trace(go.Bar(x=[None], y=[None], marker_color=station_colors.get(station_name, '#1f77b4'), name=station_name, showlegend=True))


            fig_existing.update_layout(height=1400, title_text=_("Statistiques Annuelles Détaillées de {} par Station").format(var_label), showlegend=True, legend_title_text=_("Station"), legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5), hovermode='closest', plot_bgcolor='white', paper_bgcolor='white', margin=dict(b=200))


            if not fig_existing.data or len([trace for trace in fig_existing.data if trace.x is not None]) == 0:
                logger.warning("The generated yearly rain plot has no visible data traces after all. Returning empty figure.")
                return go.Figure().add_annotation(x=0.5, y=0.5, text=_("Aucune donnée de précipitation annuelle disponible.") + " " + _("Vérifiez que vos données contiennent bien des valeurs de pluie."), showarrow=False, font=dict(size=16)).update_layout(title=_("Statistiques Annuelles de Précipitation"))


            # --- NEW PLOTS: Annual Means and Extremes for specific metrics ---
            subplot_title_col1 = get_metric_label("Moyenne sur la période des données")
            subplot_title_col2 = get_metric_label("Moyenne supérieure à la moyenne sur la période des données")
            subplot_title_col3 = get_metric_label("Moyenne inférieure à la moyenne sur la période des données")

            metrics_for_new_plots = [
                {'col': 'Cumul Annuel', 'unit': var_unit, 'label': get_metric_label("Cumul Annuel des Précipitations")},
                {'col': 'Moyenne Jours Pluvieux', 'unit': var_unit, 'label': get_metric_label("Précipitation Moyenne des Jours Pluvieux")},
                {'col': 'Moyenne Saison Pluvieuse', 'unit': var_unit, 'label': get_metric_label("Précipitation Moyenne de la Saison Pluvieuse")},
                {'col': 'Durée Saison Pluvieuse Jours', 'unit': get_metric_label("jours"), 'label': get_metric_label("Durée de la Saison Pluvieuse")},
                {'col': 'Durée Jours Sans Pluie', 'unit': get_metric_label("jours"), 'label': get_metric_label("Plus Longue Durée des Jours Sans Pluie")},
                {'col': 'Durée Sécheresse Définie Jours', 'unit': get_metric_label("jours"), 'label': get_metric_label("Durée de la Sécheresse Définie")}
            ]

            fig_new_stats = make_subplots(rows=len(metrics_for_new_plots), cols=3,
                                        subplot_titles=[subplot_title_col1, subplot_title_col2, subplot_title_col3] * len(metrics_for_new_plots),
                                        vertical_spacing=0.08, horizontal_spacing=0.05, # Reset spacing here.
                                        row_titles=[metric['label'] for metric in metrics_for_new_plots]
                                        )

            # Adjust vertical spacing after each row of 3 plots by manipulating subplot layout directly
            # This is a bit of a hack, but can create the desired "wide space" effect
            # We can't insert <br> directly, so we need to adjust the plot height and vertical_spacing.
            # Let's try to increase total height and distribute spacing.
            # Initial vertical_spacing 0.08 means 8% of plot area is vertical spacing.
            # With 6 rows, total spacing is 5 * 0.08 = 0.4.
            # To "aérer", let's increase the total height and potentially slightly increase spacing.
            total_rows = len(metrics_for_new_plots)
            # A new way to achieve horizontal spacing (more "air") is to
            # set fixed height per row and let plotly handle scaling,
            # or add dummy rows if explicit wide spacing is required.
            # For now, let's just make the plot taller to give elements more room.
            # This simulates "aération" without using <br>.
            row_height_per_plot = 350 # Increased from 300
            fig_new_stats.update_layout(height=row_height_per_plot * total_rows, title_text=_("Statistiques Récapitulatives Annuelles de {} par Station").format(var_label))


            row_idx = 0
            for metric_info in metrics_for_new_plots:
                row_idx += 1
                metric_col = metric_info['col']
                metric_label = metric_info['label']
                unit = metric_info['unit']

                overall_means_for_metric = df_yearly_metrics.groupby('Station')[metric_col].mean().reset_index()
                overall_means_for_metric.rename(columns={metric_col: 'Overall_Mean'}, inplace=True)

                station_extreme_means = []
                for station_name in unique_stations:
                    station_data = df_yearly_metrics[df_yearly_metrics['Station'] == station_name].copy()
                    station_annual_values = station_data[[metric_col, 'Year']].dropna()

                    overall_mean_for_station = overall_means_for_metric[overall_means_for_metric['Station'] == station_name]['Overall_Mean'].iloc[0] if not overall_means_for_metric[overall_means_for_metric['Station'] == station_name].empty else np.nan

                    max_val_above_mean = np.nan
                    min_val_below_mean = np.nan
                    max_val_above_mean_year = pd.NaT
                    min_val_below_mean_year = pd.NaT

                    if not station_annual_values.empty and pd.notna(overall_mean_for_station):
                        annual_means_above_overall = station_annual_values[station_annual_values[metric_col] > overall_mean_for_station]
                        if not annual_means_above_overall.empty:
                            idx_max = annual_means_above_overall[metric_col].idxmax()
                            max_val_above_mean = annual_means_above_overall.loc[idx_max, metric_col]
                            max_val_above_mean_year = annual_means_above_overall.loc[idx_max, 'Year']

                        annual_means_below_overall = station_annual_values[station_annual_values[metric_col] < overall_mean_for_station]
                        if not annual_means_below_overall.empty:
                            idx_min = annual_means_below_overall[metric_col].idxmin()
                            min_val_below_mean = annual_means_below_overall.loc[idx_min, metric_col]
                            min_val_below_mean_year = annual_means_below_overall.loc[idx_min, 'Year']

                    station_extreme_means.append({
                        'Station': station_name,
                        'Overall_Mean': overall_mean_for_station,
                        'Max_Above_Overall': max_val_above_mean,
                        'Max_Above_Overall_Period': max_val_above_mean_year,
                        'Min_Below_Overall': min_val_below_mean,
                        'Min_Below_Overall_Period': min_val_below_mean_year
                    })

                df_station_extreme_means = pd.DataFrame(station_extreme_means)

                for station_name in unique_stations:
                    station_mean_val = df_station_extreme_means[df_station_extreme_means['Station'] == station_name]['Overall_Mean'].iloc[0]
                    hover_text_mean = f"<b>{_('Station')}:</b> {station_name}<br><b>{metric_label}:</b> {station_mean_val:.2f} {unit}"
                    if global_period_str: hover_text_mean += f"<br>{global_period_str}"

                    if pd.notna(station_mean_val):
                        fig_new_stats.add_trace(go.Bar(
                            x=[station_name],
                            y=[station_mean_val],
                            marker_color=station_colors.get(station_name, '#1f77b4'),
                            name=f"{station_name} {metric_label}",
                            hoverinfo='text',
                            hovertext=hover_text_mean,
                            showlegend=False
                        ), row=row_idx, col=1)
                    else:
                        fig_new_stats.add_trace(go.Bar(x=[station_name], y=[0], marker_color='rgba(0,0,0,0)', showlegend=False, hoverinfo='none'), row=row_idx, col=1)


                for station_name in unique_stations:
                    station_max_val = df_station_extreme_means[df_station_extreme_means['Station'] == station_name]['Max_Above_Overall'].iloc[0]
                    station_max_period = df_station_extreme_means[df_station_extreme_means['Station'] == station_name]['Max_Above_Overall_Period'].iloc[0]
                    overall_mean_for_station = df_station_extreme_means[df_station_extreme_means['Station'] == station_name]['Overall_Mean'].iloc[0]

                    hover_text_max = f"<b>{_('Station')}:</b> {station_name}<br><b>{metric_label}:</b> {station_max_val:.2f} {unit}"
                    if pd.notna(station_max_period): hover_text_max += f"<br><b>{_('Année')}:</b> {int(station_max_period)}"
                    if pd.notna(overall_mean_for_station):
                        hover_text_max += f"<br>({_('Moyenne sur la période des données')}: {overall_mean_for_station:.2f} {unit})"
                    if global_period_str: hover_text_max += f"<br>{global_period_str}"

                    if pd.notna(station_max_val):
                        fig_new_stats.add_trace(go.Bar(
                            x=[station_name],
                            y=[station_max_val],
                            marker_color=station_colors.get(station_name, '#1f77b4'),
                            name=f"{station_name} {metric_label}",
                            hoverinfo='text',
                            hovertext=hover_text_max,
                            showlegend=False
                        ), row=row_idx, col=2)
                    else:
                        fig_new_stats.add_trace(go.Bar(x=[station_name], y=[0], marker_color='rgba(0,0,0,0)', showlegend=False, hoverinfo='none'), row=row_idx, col=2)


                for station_name in unique_stations:
                    station_min_val = df_station_extreme_means[df_station_extreme_means['Station'] == station_name]['Min_Below_Overall'].iloc[0]
                    station_min_period = df_station_extreme_means[df_station_extreme_means['Station'] == station_name]['Min_Below_Overall_Period'].iloc[0]
                    overall_mean_for_station = df_station_extreme_means[df_station_extreme_means['Station'] == station_name]['Overall_Mean'].iloc[0]

                    hover_text_min = f"<b>{_('Station')}:</b> {station_name}<br><b>{metric_label}:</b> {station_min_val:.2f} {unit}"
                    if pd.notna(station_min_period): hover_text_min += f"<br><b>{_('Année')}:</b> {int(station_min_period)}"
                    if pd.notna(overall_mean_for_station):
                        hover_text_min += f"<br>({_('Moyenne sur la période des données')}: {overall_mean_for_station:.2f} {unit})"
                    if global_period_str: hover_text_min += f"<br>{global_period_str}"

                    if pd.notna(station_min_val):
                        fig_new_stats.add_trace(go.Bar(
                            x=[station_name],
                            y=[station_min_val],
                            marker_color=station_colors.get(station_name, '#1f77b4'),
                            name=f"{station_name} {metric_label}",
                            hoverinfo='text',
                            hovertext=hover_text_min,
                            showlegend=False
                        ), row=row_idx, col=3)
                    else:
                        fig_new_stats.add_trace(go.Bar(x=[station_name], y=[0], marker_color='rgba(0,0,0,0)', showlegend=False, hoverinfo='none'), row=row_idx, col=3)

                fig_new_stats.update_yaxes(title_text=f"{metric_label} ({unit})", row=row_idx, col=1)
                fig_new_stats.update_yaxes(title_text=f"{metric_label} ({unit})", row=row_idx, col=2)
                fig_new_stats.update_yaxes(title_text=f"{metric_label} ({unit})", row=row_idx, col=3)

                fig_new_stats.update_xaxes(title_text="", row=row_idx, col=1)
                fig_new_stats.update_xaxes(title_text="", row=row_idx, col=2)
                fig_new_stats.update_xaxes(title_text="", row=row_idx, col=3)


            for station_name in unique_stations:
                fig_new_stats.add_trace(go.Bar(x=[None], y=[None], marker_color=station_colors.get(station_name, '#1f77b4'), name=station_name, showlegend=True))

            fig_new_stats.update_layout(
                title_text=_("Statistiques Récapitulatives Annuelles de {} par Station").format(var_label),
                showlegend=True,
                legend_title_text=_("Station"),
                legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
                hovermode='closest',
                plot_bgcolor='white',
                paper_bgcolor='white',
                margin=dict(b=150, t=100)
            )

            if not fig_new_stats.data or not any(trace.x is not None for trace in fig_new_stats.data):
                logger.warning("The generated new yearly rain stats plot has no visible data traces. Returning empty figure.")
                fig_new_stats_empty = go.Figure().add_annotation(x=0.5, y=0.5, text=_("Aucune statistique récapitulative annuelle de précipitation disponible."), showarrow=False, font=dict(size=16)).update_layout(title=_("Statistiques Récapitulatives Annuelles de Précipitation"))
                return fig_existing, fig_new_stats_empty


            return fig_existing, fig_new_stats


        else:
            # --- Generic Case (Monthly, Weekly, Daily, or Non-Rain Variables) ---

            all_station_period_data = []

            freq_map = {
                'monthly': 'M',
                'weekly': 'W',
                'daily': 'D',
                'yearly': 'Y' # This case is now handled by the specific yearly rain logic or this generic logic for non-rain
            }
            freq_str = freq_map.get(time_frequency)

            # Define formatting for hover text based on frequency
            period_hover_label_map = {
                'monthly': _("Mois"),
                'weekly': _("Semaine"),
                'daily': _("Jour"),
                'yearly': _("Année")
            }
            period_hover_label = period_hover_label_map.get(time_frequency, _("Période"))

            # For rainfall data on non-yearly frequencies, filter within the rainy season
            df_season_stats_all = {}
            if is_rain_variable:
                for station_name_temp in unique_stations:
                    station_df_daily_temp = df_processed[df_processed['Station'] == station_name_temp]
                    # Ensure we're calculating season stats from daily rainfall sums
                    station_df_daily_rain_for_season = station_df_daily_temp.groupby(pd.Grouper(key='Datetime', freq='D'))['Rain_mm'].sum().reset_index()
                    df_season_stats_all[station_name_temp] = _calculate_rainy_season_stats_yearly(station_df_daily_rain_for_season)

            for station_name in unique_stations:
                station_data_processed = df_processed[df_processed['Station'] == station_name].copy()

                # --- NEW: Filter for daylight hours using 'Is_Daylight' for Solar_R ---
                if variable == 'Solar_R_W/m^2':
                    if 'Is_Daylight' in station_data_processed.columns:
                        station_data_processed = station_data_processed[station_data_processed['Is_Daylight'] == True]
                    else: # Fallback to hour-based filtering if Is_Daylight is not available
                        station_data_processed['Hour'] = station_data_processed['Datetime'].dt.hour
                        station_data_processed = station_data_processed[
                            (station_data_processed['Hour'] >= 7) & (station_data_processed['Hour'] <= 18)
                        ]
                        station_data_processed = station_data_processed.drop(columns=['Hour'])

                # For Rain_mm, filter data to be strictly within the rainy season for non-yearly calculations
                if is_rain_variable and time_frequency != 'yearly':
                    station_season_stats = df_season_stats_all.get(station_name)
                    filtered_data_in_season = []
                    if station_season_stats is not None:
                        for year in station_data_processed['Datetime'].dt.year.unique():
                            if year in station_season_stats.index:
                                season_start = station_season_stats.loc[year, 'Début Saison Pluvieuse']
                                season_end = station_season_stats.loc[year, 'Fin Saison Pluvieuse']

                                if pd.notna(season_start) and pd.notna(season_end) and season_start <= season_end:
                                    yearly_df = station_data_processed[station_data_processed['Datetime'].dt.year == year]
                                    filtered_data_in_season.append(yearly_df[(yearly_df['Datetime'] >= season_start) & (yearly_df['Datetime'] <= season_end)])
                    if filtered_data_in_season:
                        station_data_processed = pd.concat(filtered_data_in_season)
                    else:
                        station_data_processed = pd.DataFrame(columns=station_data_processed.columns)


                # Aggregate data to the desired frequency for plotting
                aggregated_data = pd.DataFrame(columns=['Datetime', 'Agg_Value'])
                if not station_data_processed.empty:
                    if is_rain_variable and time_frequency != 'yearly':
                        # For rain, non-yearly: calculate daily sum, then mean of these daily sums within the season
                        daily_rain_in_season = station_data_processed.groupby(pd.Grouper(key='Datetime', freq='D'))['Rain_mm'].sum().reset_index(name='Daily_Rain_Sum')
                        # Resample to target frequency and take the MEAN of daily sums within that frequency's period
                        aggregated_data = daily_rain_in_season.groupby(pd.Grouper(key='Datetime', freq=freq_str))['Daily_Rain_Sum'].mean().reset_index(name='Agg_Value')
                    else:
                        # For other variables, calculate mean directly
                        aggregated_data = station_data_processed.groupby(pd.Grouper(key='Datetime', freq=freq_str))[variable].mean().reset_index(name='Agg_Value')
                else:
                    logger.warning(f"No data for {variable} for station {station_name} after season filtering for {time_frequency} period.")


                # Calculate overall mean for the station for this period
                overall_mean_for_station = aggregated_data['Agg_Value'].mean()

                max_val_above_mean = np.nan
                min_val_below_mean = np.nan
                max_val_above_mean_period = pd.NaT
                min_val_below_mean_period = pd.NaT

                if not aggregated_data.empty and pd.notna(overall_mean_for_station):
                    above_mean_data = aggregated_data[aggregated_data['Agg_Value'] > overall_mean_for_station]
                    if not above_mean_data.empty:
                        idx_max_above = above_mean_data['Agg_Value'].idxmax()
                        max_val_above_mean = above_mean_data.loc[idx_max_above, 'Agg_Value']
                        max_val_above_mean_period = above_mean_data.loc[idx_max_above, 'Datetime']

                    below_mean_data = aggregated_data[aggregated_data['Agg_Value'] < overall_mean_for_station]
                    if not below_mean_data.empty:
                        idx_min_below = below_mean_data['Agg_Value'].idxmin()
                        min_val_below_mean = below_mean_data.loc[idx_min_below, 'Agg_Value']
                        min_val_below_mean_period = below_mean_data.loc[idx_min_below, 'Datetime']

                all_station_period_data.append({
                    'Station': station_name,
                    'Overall_Mean': overall_mean_for_station,
                    'Max_Above_Overall': max_val_above_mean,
                    'Max_Above_Overall_Period': max_val_above_mean_period,
                    'Min_Below_Overall': min_val_below_mean,
                    'Min_Below_Overall_Period': min_val_below_mean_period
                })

            df_period_metrics = pd.DataFrame(all_station_period_data)

            subplot_titles_generic = [
                get_metric_label("Moyenne sur la période des données"),
                get_metric_label("Moyenne supérieure à la moyenne sur la période des données"),
                get_metric_label("Moyenne inférieure à la moyenne sur la période des données")
            ]

            fig_generic = make_subplots(rows=1, cols=3, subplot_titles=subplot_titles_generic,
                                        vertical_spacing=0.08, horizontal_spacing=0.05)


            # Helper to format period for hover text
            def format_period_for_hover(dt_obj, freq):
                if pd.isna(dt_obj):
                    return _("N/A")
                if freq == 'monthly':
                    return dt_obj.strftime('%B %Y') # Ex: Janvier 2023
                elif freq == 'weekly':
                    # Calculate start and end of the ISO week
                    week_start = dt_obj - timedelta(days=dt_obj.weekday()) # Monday
                    week_end = week_start + timedelta(days=6) # Sunday
                    return _("du {} au {}").format(week_start.strftime('%d-%m-%Y'), week_end.strftime('%d-%m-%Y'))
                elif freq == 'daily':
                    return dt_obj.strftime('%d-%m-%Y')
                elif freq == 'yearly':
                    return dt_obj.strftime('%Y')
                return str(dt_obj)

            # Plot 1: Overall Mean
            for station_name in unique_stations:
                station_mean_val = df_period_metrics[df_period_metrics['Station'] == station_name]['Overall_Mean'].iloc[0] if not df_period_metrics[df_period_metrics['Station'] == station_name].empty else np.nan
                hover_text_mean = f"<b>{_('Station')}:</b> {station_name}<br><b>{var_label}:</b> {station_mean_val:.2f} {var_unit}"
                if global_period_str: hover_text_mean += f"<br>{global_period_str}"

                if pd.notna(station_mean_val):
                    fig_generic.add_trace(go.Bar(
                        x=[station_name],
                        y=[station_mean_val],
                        marker_color=station_colors.get(station_name, '#1f77b4'),
                        name=station_name,
                        hoverinfo='text',
                        hovertext=hover_text_mean,
                        showlegend=False
                    ), row=1, col=1)
                else:
                    fig_generic.add_trace(go.Bar(x=[station_name], y=[0], marker_color='rgba(0,0,0,0)', showlegend=False, hoverinfo='none'), row=1, col=1)

            # Plot 2: Max Above Overall Mean
            for station_name in unique_stations:
                station_max_val = df_period_metrics[df_period_metrics['Station'] == station_name]['Max_Above_Overall'].iloc[0] if not df_period_metrics[df_period_metrics['Station'] == station_name].empty else np.nan
                station_max_period = df_period_metrics[df_period_metrics['Station'] == station_name]['Max_Above_Overall_Period'].iloc[0]
                station_mean_val = df_period_metrics[df_period_metrics['Station'] == station_name]['Overall_Mean'].iloc[0] if not df_period_metrics[df_period_metrics['Station'] == station_name].empty else np.nan

                hover_text_max = f"<b>{_('Station')}:</b> {station_name}<br><b>{var_label}:</b> {station_max_val:.2f} {var_unit}"
                if pd.notna(station_max_period): hover_text_max += f"<br><b>{period_hover_label}:</b> {format_period_for_hover(station_max_period, time_frequency)}"
                if pd.notna(station_mean_val):
                    hover_text_max += f"<br>({_('Moyenne sur la période des données')}: {station_mean_val:.2f} {var_unit})"
                if global_period_str: hover_text_max += f"<br>{global_period_str}"

                if pd.notna(station_max_val):
                    fig_generic.add_trace(go.Bar(
                        x=[station_name],
                        y=[station_max_val],
                        marker_color=station_colors.get(station_name, '#1f77b4'),
                        name=station_name,
                        hoverinfo='text',
                        hovertext=hover_text_max,
                        showlegend=False
                    ), row=1, col=2)
                else:
                    fig_generic.add_trace(go.Bar(x=[station_name], y=[0], marker_color='rgba(0,0,0,0)', showlegend=False, hoverinfo='none'), row=1, col=2)

            # Plot 3: Min Below Overall Mean
            for station_name in unique_stations:
                station_min_val = df_period_metrics[df_period_metrics['Station'] == station_name]['Min_Below_Overall'].iloc[0] if not df_period_metrics[df_period_metrics['Station'] == station_name].empty else np.nan
                station_min_period = df_period_metrics[df_period_metrics['Station'] == station_name]['Min_Below_Overall_Period'].iloc[0]
                station_mean_val = df_period_metrics[df_period_metrics['Station'] == station_name]['Overall_Mean'].iloc[0] if not df_period_metrics[df_period_metrics['Station'] == station_name].empty else np.nan

                hover_text_min = f"<b>{_('Station')}:</b> {station_name}<br><b>{var_label}:</b> {station_min_val:.2f} {var_unit}"
                if pd.notna(station_min_period): hover_text_min += f"<br><b>{period_hover_label}:</b> {format_period_for_hover(station_min_period, time_frequency)}"
                if pd.notna(station_mean_val):
                    hover_text_min += f"<br>({_('Moyenne sur la période des données')}: {station_mean_val:.2f} {var_unit})"
                if global_period_str: hover_text_min += f"<br>{global_period_str}"

                if pd.notna(station_min_val):
                    fig_generic.add_trace(go.Bar(
                        x=[station_name],
                        y=[station_min_val],
                        marker_color=station_colors.get(station_name, '#1f77b4'),
                        name=station_name,
                        hoverinfo='text',
                        hovertext=hover_text_min,
                        showlegend=False
                    ), row=1, col=3)
                else:
                    fig_generic.add_trace(go.Bar(x=[station_name], y=[0], marker_color='rgba(0,0,0,0)', showlegend=False, hoverinfo='none'), row=1, col=3)


            for station_name in unique_stations:
                fig_generic.add_trace(go.Bar(x=[None], y=[None], marker_color=station_colors.get(station_name, '#1f77b4'), name=station_name, showlegend=True))

            fig_generic.update_layout(
                height=500,
                title_text=plot_base_title,
                showlegend=True,
                legend_title_text=_("Station"),
                legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
                hovermode='closest',
                plot_bgcolor='white',
                paper_bgcolor='white',
                margin=dict(b=150, t=100)
            )

            fig_generic.update_xaxes(title_text="", row=1, col=1)
            fig_generic.update_xaxes(title_text="", row=1, col=2)
            fig_generic.update_xaxes(title_text="", row=1, col=3)

            fig_generic.update_yaxes(title_text=f"{var_label} ({var_unit})", row=1, col=1)
            fig_generic.update_yaxes(title_text=f"{var_label} ({var_unit})", row=1, col=2)
            fig_generic.update_yaxes(title_text=f"{var_label} ({var_unit})", row=1, col=3)

            if not fig_generic.data or not any(trace.x is not None for trace in fig_generic.data):
                logger.warning(f"The generated generic plot for {variable} at {time_frequency} has no visible data traces. Returning empty figure.")
                return go.Figure().add_annotation(x=0.5, y=0.5, text=_("Aucune statistique à afficher pour cette variable et fréquence."), showarrow=False, font=dict(size=16)).update_layout(title=plot_base_title)

            return fig_generic

    except Exception as e:
        if logger:
            logger.error(_("Une erreur est survenue dans generate_plot_stats_over_period_plotly: {}").format(str(e)), exc_info=True)
            return go.Figure().add_annotation(x=0.5, y=0.5, text=_("Une erreur est survenue lors de la génération du graphique: {}").format(str(e)), showarrow=False, font=dict(size=16)).update_layout(title=plot_title_prefix)

############# Fin du code fonctionnel pour  les statistiques  ##########################













# ###############  Fonction pour visualiser les valeurs aberantes #######################

# def outliers_viz(df: pd.DataFrame, coef: float = 1.5) -> go.Figure:
#     """
#     Visualisation des outliers pour chaque variable numérique via des scatter plots Plotly.
#     """
#     df = df.copy()
#     if df.index.name == 'Datetime':
#         df.reset_index(inplace=True)
#     elif 'Datetime' not in df.columns:
#         return go.Figure().add_annotation(
#             x=0.5, y=0.5, text=_l("Colonne 'Datetime' manquante pour la visualisation des outliers."),
#             showarrow=False, font=dict(size=16)
#         )

#     # Conversion des colonnes numériques (hors colonnes à exclure)
#     exclude_cols = {
#         'Datetime', 'Station', 'Is_Daylight', 'Day', 
#         'sunrise_time_utc', 'sunset_time_utc', 'Daylight_Duration',
#         'Rain_01_mm', 'Rain_02_mm', 'Day_Duration'
#     }

#     for col in df.columns:
#         if col not in exclude_cols:
#             df[col] = pd.to_numeric(df[col], errors='coerce')

#     numeric_cols = [
#         col for col in df.select_dtypes(include='number').columns
#         if col not in exclude_cols
#     ]

#     if not numeric_cols:
#         return go.Figure().add_annotation(
#             x=0.5, y=0.5, text=_l("Aucune variable numérique pour l'analyse des outliers après nettoyage."),
#             showarrow=False, font=dict(size=16)
#         ).update_layout(title=_l("Analyse des Outliers"))

#     # Détection des outliers par station et par variable
#     df_out = df.copy()
#     for col in numeric_cols:
#         df_out[f'{col}_is_outlier'] = False

#     for station, group in df.groupby('Station'):
#         for col in numeric_cols:
#             if group[col].count() > 1:
#                 Q1, Q3 = group[col].quantile([0.25, 0.75])
#                 IQR = Q3 - Q1
#                 lower, upper = Q1 - coef * IQR, Q3 + coef * IQR
#                 outlier_idx = group[(group[col] < lower) | (group[col] > upper)].index
#                 df_out.loc[outlier_idx, f'{col}_is_outlier'] = True
#             else:
#                 warnings.warn(_l("Pas assez de données pour calculer les outliers pour la station '%s' et la variable '%s'.") % (station, col))

#     # Préparation des subplots
#     cols_per_row = 2
#     n = len(numeric_cols)
#     rows = (n + cols_per_row - 1) // cols_per_row

#     fig = make_subplots(rows=rows, cols=cols_per_row, subplot_titles=numeric_cols)

#     # Génération des traces
#     for i, col in enumerate(numeric_cols):
#         r, c = divmod(i, cols_per_row)
#         r += 1; c += 1

#         for outlier_status, color, label, marker_opts in [
#             (False, 'blue', _l('Valeurs normales'), dict(size=5)),
#             (True, 'red', _l('Outliers'), dict(size=7, symbol='circle-open', line=dict(width=2, color='red')))
#         ]:
#             subset = df_out[df_out[f'{col}_is_outlier'] == outlier_status]
#             fig.add_trace(
#                 go.Scatter(
#                     x=subset['Datetime'], y=subset[col],
#                     mode='markers',
#                     name=label,
#                     marker=dict(color=color, **marker_opts),
#                     hovertemplate=(
#                         "<b>Station:</b> %{customdata[0]}<br>"
#                         "<b>Date:</b> %{x|%Y-%m-%d %H:%M:%S}<br>"
#                         f"<b>{col}:</b> %{{y}}<br><extra></extra>"
#                     ),
#                     customdata=subset[['Station']],
#                     showlegend=(i == 0)  # Ne montrer qu'une fois la légende
#                 ),
#                 row=r, col=c
#             )

#         fig.update_xaxes(title_text=_l("Date"), row=r, col=c)
#         fig.update_yaxes(title_text=col, row=r, col=c)

#     # Mise en forme finale
#     fig.update_layout(
#         title=_l("Analyse des Outliers par Variable"),
#         height=400 * rows,
#         showlegend=True,
#         legend_title_text=_l("Statut d'Outlier")
#     )

#     return fig
# def valeurs_manquantes_viz(df: pd.DataFrame) -> go.Figure:
#     """
#     Génère une figure Plotly contenant des diagrammes en secteurs
#     pour illustrer les pourcentages de valeurs manquantes par variable.
#     """
#     # Colonnes à exclure de l’analyse
#     exclude = {
#         'Datetime', 'Station', 'Rain_01_mm', 'Rain_02_mm',
#         'Is_Daylight', 'Day_Duration', 'sunrise_time_utc', 'sunset_time_utc', 'Day'
#     }

#     variables = [col for col in df.columns if col not in exclude]

#     if not variables:
#         return go.Figure().add_annotation(
#             x=0.5, y=0.5, text="Aucune variable à analyser pour les valeurs manquantes.",
#             showarrow=False, font=dict(size=16)
#         ).update_layout(title="Analyse des valeurs manquantes")

#     # Paramètres pour l'affichage
#     cols_per_row = 3
#     rows = (len(variables) + cols_per_row - 1) // cols_per_row
#     colors = ['lightgreen', 'lightcoral']

#     # Créer la figure avec sous-graphiques de type "camembert"
#     fig = make_subplots(
#         rows=rows,
#         cols=cols_per_row,
#         specs=[[{'type': 'domain'}]*cols_per_row]*rows,
#         subplot_titles=variables
#     )

#     for i, col in enumerate(variables):
#         present = df[col].count()
#         missing = df[col].isna().sum()
#         total = present + missing

#         if total == 0:
#             values = [1]
#             labels = ['Aucune donnée']
#             pie_colors = ['lightgray']
#             textinfo = 'none'
#         else:
#             values = [present, missing]
#             labels = ['Valeurs Présentes', 'Valeurs Manquantes']
#             pie_colors = colors
#             textinfo = 'percent'

#         r, c = divmod(i, cols_per_row)
#         fig.add_trace(
#             go.Pie(
#                 labels=labels,
#                 values=values,
#                 name=col,
#                 marker=dict(colors=pie_colors),
#                 textinfo=textinfo,
#                 hovertemplate="<b>%{label}</b><br>Nombre: %{value}<br>Pourcentage: %{percent:.2%}<extra></extra>",
#                 showlegend=True if i == 0 else False  # Une seule légende affichée
#             ),
#             row=r + 1,
#             col=c + 1
#         )

#     # Mise en page globale
#     fig.update_layout(
#         title_text="Pourcentage de valeurs manquantes par variable",
#         showlegend=True,
#         legend=dict(
#             orientation="h", yanchor="bottom", y=-0.15,
#             xanchor="center", x=0.5
#         ),
#         height=350 * rows,
#         margin=dict(l=50, r=50, b=100, t=80)
#     )

#     # Centrer les titres de sous-plots
#     for ann in fig.layout.annotations:
#         ann.update(x=ann.x)

#     return fig


# ############# Fin de la Fonction pour visualiser les valeurs aberantes #####################



# ############# Fonction pour visualiser les plages des valeurs manquantes #####################
# import pandas as pd
# import numpy as np
# import plotly.graph_objects as go
# from plotly.subplots import make_subplots
# import warnings
# from flask_babel import _, lazy_gettext as _l # Assuming these imports are correct


# def gaps_time_series_viz(df_data: pd.DataFrame, df_gaps: pd.DataFrame, station_name: str, title_suffix: str = "") -> go.Figure:
#     """
#     Generates a Plotly figure showing numerical time series data
#     with highlighted periods of missing values (gaps),
#     where gap details are displayed in tooltips.

#     Args:
#         df_data (pd.DataFrame): The data DataFrame (raw or interpolated) for a given station.
#                                 Must have a DatetimeIndex and a 'Station' column.
#         df_gaps (pd.DataFrame): The DataFrame of missing ranges (e.g., GLOBAL_MISSING_VALUES_BEFORE/AFTER_INTERPOLATION_DF)
#                                 filtered for the current station.
#                                 Must contain 'station', 'variable', 'start_time', 'end_time', and 'duration_hours'.
#         station_name (str): The name of the station currently being visualized.
#         title_suffix (str): A suffix to add to the chart title (e.g., "Avant Interpolation").

#     Returns:
#         go.Figure: The Plotly figure.
#     """
#     # --- Initial Data Validation ---
#     if df_data.empty:
#         fig = go.Figure().add_annotation(
#             x=0.5, y=0.5, text=str(_l("Aucune donnée disponible pour la station sélectionnée.")),
#             showarrow=False, font=dict(size=16)
#         )
#         fig.update_layout(title=str(f"{_l('Visualisation des données et des gaps')} - {station_name} {title_suffix}"),
#                           template="plotly_white")
#         return fig

#     # Ensure DatetimeIndex is present
#     if not isinstance(df_data.index, pd.DatetimeIndex):
#         warnings.warn(str(_l("Le DataFrame de données n'a pas un DatetimeIndex. Tentative de conversion.")))
#         try:
#             df_data.index = pd.to_datetime(df_data.index, errors='coerce')
#             df_data = df_data[df_data.index.notna()]
#         except Exception:
#             fig = go.Figure().add_annotation(
#                 x=0.5, y=0.5, text=str(_l("L'index du DataFrame n'est pas un DatetimeIndex valide et ne peut pas être converti.")),
#                 showarrow=False, font=dict(size=16)
#             )
#             fig.update_layout(title=str(f"{_l('Visualisation des données et des gaps')} - {station_name} {title_suffix}"),
#                               template="plotly_white")
#             return fig
#         if df_data.empty: # Check again after conversion and NaN removal
#             fig = go.Figure().add_annotation(
#                 x=0.5, y=0.5, text=str(_l("Aucune donnée valide disponible après conversion de l'index en DatetimeIndex.")),
#                 showarrow=False, font=dict(size=16)
#             )
#             fig.update_layout(title=str(f"{_l('Visualisation des données et des gaps')} - {station_name} {title_suffix}"),
#                               template="plotly_white")
#             return fig


#     # Define columns to exclude from visualization (non-numeric or flags)
#     # Using a set for faster lookups
#     EXCLUDE_COLS_VIZ = {
#         'Station', 'Is_Daylight', 'Rain_01_mm', 'Rain_02_mm', 'Wind_Dir_Deg',
#         'sunrise_time_utc', 'sunset_time_utc', 'Daylight_Duration',
#         'Date_UTC_Naive', 'Lat', 'Long', 'Timezone',
#         'sunrise_time_utc_calc', 'sunset_time_utc_calc',
#         'Daylight_Duration_h_calc', 'fixed_daylight_applied',
#         'Year', 'Month', 'Minute', 'Hour', 'Date', 'Day' # Also exclude derived date components
#     }

#     # Identify numerical columns for visualization
#     numerical_cols_for_viz = [
#         col for col in df_data.columns
#         if pd.api.types.is_numeric_dtype(df_data[col]) and col not in EXCLUDE_COLS_VIZ
#     ]
    
#     if not numerical_cols_for_viz:
#         fig = go.Figure().add_annotation(
#             x=0.5, y=0.5, text=str(_l("Aucune variable numérique exploitable pour la visualisation des gaps.")),
#             showarrow=False, font=dict(size=16)
#         )
#         fig.update_layout(title=str(f"{_l('Visualisation des données et des gaps')} - {station_name} {title_suffix}"),
#                           template="plotly_white")
#         return fig

#     # --- Subplot Configuration ---
#     num_variables = len(numerical_cols_for_viz)
#     rows = num_variables # One row per variable
#     cols_per_row = 1

#     fig = make_subplots(
#         rows=rows,
#         cols=cols_per_row,
#         shared_xaxes=True, # Share x-axis across all subplots
#         vertical_spacing=0.04, # Slightly reduce spacing
#         subplot_titles=[str(f"{_l('Série temporelle')} - {col}") for col in numerical_cols_for_viz]
#     )

#     # --- Filter Gaps Data Once ---
#     # Filter df_gaps for the current station and relevant numerical columns only once
#     relevant_gaps_filtered = df_gaps[
#         (df_gaps['station'] == station_name) & 
#         (df_gaps['variable'].isin(numerical_cols_for_viz))
#     ].copy()
    
#     # Ensure start_time and end_time are datetime objects in df_gaps for comparison
#     relevant_gaps_filtered['start_time'] = pd.to_datetime(relevant_gaps_filtered['start_time'], errors='coerce')
#     relevant_gaps_filtered['end_time'] = pd.to_datetime(relevant_gaps_filtered['end_time'], errors='coerce')
#     relevant_gaps_filtered.dropna(subset=['start_time', 'end_time'], inplace=True) # Drop invalid gap entries

#     # --- Add Traces and Gap Highlights ---
#     for i, col in enumerate(numerical_cols_for_viz):
#         row_idx = i + 1

#         # Add data trace
#         fig.add_trace(
#             go.Scatter(
#                 x=df_data.index,
#                 y=df_data[col],
#                 mode='lines+markers',
#                 name=str(f"{col} - {_l('Données')}"),
#                 line=dict(color='blue', width=1),
#                 marker=dict(size=3, opacity=0.7),
#                 hovertemplate="<b>%{x|%Y-%m-%d %H:%M}</b><br>" +
#                                f"{col}: %{{y:.2f}}<extra></extra>", # Format y value
#                 showlegend=False
#             ),
#             row=row_idx, col=1
#         )

#         # Get gaps specific to this column
#         gaps_for_col = relevant_gaps_filtered[relevant_gaps_filtered['variable'] == col]

#         if not gaps_for_col.empty:
#             # Determine y-axis range for the current subplot to make shapes scale
#             y_min = df_data[col].min()
#             y_max = df_data[col].max()
#             # Add a small buffer to y_min/y_max if data is constant or has very small range
#             if pd.isna(y_min) or pd.isna(y_max) or y_max == y_min:
#                 y_min, y_max = 0, 1 # Fallback to a default range
#             else:
#                 y_buffer = (y_max - y_min) * 0.05 # 5% buffer
#                 y_min -= y_buffer
#                 y_max += y_buffer

#             # Add a single shape for each gap
#             for _, gap_row in gaps_for_col.iterrows():
#                 fig.add_shape(
#                     type="rect",
#                     xref="x", yref="y", # Use 'y' coordinates, not 'paper'
#                     x0=gap_row['start_time'],
#                     y0=y_min, # Scale with actual y-axis data range
#                     y1=y_max, # Scale with actual y-axis data range
#                     x1=gap_row['end_time'],
#                     fillcolor="rgba(255,0,0,0.2)", # Light red transparent fill
#                     line_width=0,
#                     layer="below", # Place below the data trace
#                     row=row_idx, col=1
#                 )
                
#                 # --- Optimized Hover Info for Gaps ---
#                 # Add an invisible scatter trace over the gap area for hover text
#                 # We place a single marker in the middle of the gap for hover info.
#                 gap_mid_time = gap_row['start_time'] + (gap_row['end_time'] - gap_row['start_time']) / 2
                
#                 # Use a small non-NaN value for y to ensure it's visible, but it won't be plotted visually
#                 y_for_hover = df_data[col].mean() if df_data[col].notna().any() else 0 
                
#                 hover_text_gap = str(f"<b>{_l('Période Manquante')}</b><br>" +
#                                      f"{_l('Variable')}: {gap_row['variable']}<br>" +
#                                      f"{_l('Début')}: {gap_row['start_time'].strftime('%Y-%m-%d %H:%M')}<br>" +
#                                      f"{_l('Fin')}: {gap_row['end_time'].strftime('%Y-%m-%d %H:%M')}<br>" +
#                                      f"{_l('Durée')}: {gap_row['duration_hours']:.2f} {_l('heures')}")

#                 fig.add_trace(
#                     go.Scatter(
#                         x=[gap_mid_time],
#                         y=[y_for_hover], # A single point for hover, visually hidden
#                         mode='markers',
#                         marker=dict(size=1, opacity=0), # Invisible marker
#                         showlegend=False,
#                         hoverinfo='text',
#                         text=hover_text_gap,
#                         name=f"{col} {_l('Gap Info')}" # Give it a name for internal tracking
#                     ),
#                     row=row_idx, col=1
#                 )

#     # --- Update Layout ---
#     fig.update_layout(
#         title_text=str(f"{_l('Séries temporelles avec périodes manquantes/interpolées')} - {station_name} {title_suffix}"),
#         height=max(400, 300 * rows), # Dynamic height with a minimum
#         hovermode="x unified", # Excellent for time series analysis
#         margin=dict(l=50, r=50, b=80, t=100), # Adjust margins
#         template="plotly_white", # Clean template
#     )

#     # Update Y-axis titles
#     for i, col in enumerate(numerical_cols_for_viz):
#         fig.update_yaxes(title_text=col, row=i+1, col=1, rangemode='tozero') # rangemode='tozero' often good for time series

#     # Update X-axis (shared)
#     fig.update_xaxes(title_text=str(_l("Temps")), showgrid=True, gridwidth=1, gridcolor='LightGray')

#     return fig



############ Fin de la Fonction pour visualiser les plages des valeurs manquantes #####################


def outliers_viz(df: pd.DataFrame, coef: float = 1.5) -> go.Figure:
    """
    Visualisation des outliers pour chaque variable numérique via des scatter plots Plotly.
    """
    df = df.copy()
    if df.index.name == 'Datetime':
        df.reset_index(inplace=True)
    elif 'Datetime' not in df.columns:
        return go.Figure().add_annotation(
            x=0.5, y=0.5, text=str(_l("Colonne 'Datetime' manquante pour la visualisation des outliers.")),
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
            x=0.5, y=0.5, text=str(_l("Aucune variable numérique pour l'analyse des outliers après nettoyage.")),
            showarrow=False, font=dict(size=16)
        ).update_layout(title=str(_l("Analyse des Outliers")))

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
                warnings.warn(str(_l("Pas assez de données pour calculer les outliers pour la station '%s' et la variable '%s'.") % (station, col)))

    # Préparation des subplots
    cols_per_row = 2
    n = len(numeric_cols)
    rows = (n + cols_per_row - 1) // cols_per_row

    fig = make_subplots(rows=rows, cols=cols_per_row, subplot_titles=numeric_cols)

    # Génération des traces
    for i, col in enumerate(numeric_cols):
        r, c = divmod(i, cols_per_row)
        r += 1; c += 1

        # Convertir les labels LazyString en strings
        normal_label = str(_l('Valeurs normales'))
        outlier_label = str(_l('Outliers'))

        for outlier_status, color, label, marker_opts in [
            (False, 'blue', normal_label, dict(size=5)),
            (True, 'red', outlier_label, dict(size=7, symbol='circle-open', line=dict(width=2, color='red')))
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
                    showlegend=(i == 0)
                ),
                row=r, col=c
            )

        fig.update_xaxes(title_text=str(_l("Date")), row=r, col=c)
        fig.update_yaxes(title_text=col, row=r, col=c)

    # Mise en forme finale
    fig.update_layout(
        title=str(_l("Analyse des Outliers par Variable")),
        height=400 * rows,
        showlegend=True,
        legend_title_text=str(_l("Statut d'Outlier"))
    )

    return fig

def valeurs_manquantes_viz(df: pd.DataFrame) -> go.Figure:
    """
    Génère une figure Plotly contenant des diagrammes en secteurs
    pour illustrer les pourcentages de valeurs manquantes par variable.
    """
    # Colonnes à exclure de l'analyse
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

    # Convertir les labels LazyString en strings
    present_label = str(_l('Valeurs Présentes'))
    missing_label = str(_l('Valeurs Manquantes'))
    no_data_label = str(_l('Aucune donnée'))

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
            labels = [no_data_label]
            pie_colors = ['lightgray']
            textinfo = 'none'
        else:
            values = [present, missing]
            labels = [present_label, missing_label]
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
                showlegend=(i == 0)
            ),
            row=r + 1,
            col=c + 1
        )

    # Mise en page globale
    fig.update_layout(
        title_text=str(_l("Pourcentage de valeurs manquantes par variable")),
        showlegend=True,
        legend=dict(
            orientation="h", yanchor="bottom", y=-0.15,
            xanchor="center", x=0.5
        ),
        height=350 * rows,
        margin=dict(l=50, r=50, b=100, t=80)
    )

    return fig

def gaps_time_series_viz(df_data: pd.DataFrame, df_gaps: pd.DataFrame, station_name: str, title_suffix: str = "") -> go.Figure:
    """
    Generates a Plotly figure showing numerical time series data
    with highlighted periods of missing values (gaps).
    """
    if df_data.empty:
        fig = go.Figure().add_annotation(
            x=0.5, y=0.5, text=str(_l("Aucune donnée disponible pour la station sélectionnée.")),
            showarrow=False, font=dict(size=16)
        )
        fig.update_layout(title=str(f"{str(_l('Visualisation des données et des gaps'))} - {station_name} {title_suffix}"),
                          template="plotly_white")
        return fig

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
            fig.update_layout(title=str(f"{str(_l('Visualisation des données et des gaps'))} - {station_name} {title_suffix}"),
                              template="plotly_white")
            return fig
        if df_data.empty:
            fig = go.Figure().add_annotation(
                x=0.5, y=0.5, text=str(_l("Aucune donnée valide disponible après conversion de l'index en DatetimeIndex.")),
                showarrow=False, font=dict(size=16)
            )
            fig.update_layout(title=str(f"{str(_l('Visualisation des données et des gaps'))} - {station_name} {title_suffix}"),
                              template="plotly_white")
            return fig

    EXCLUDE_COLS_VIZ = {
        'Station', 'Is_Daylight', 'Rain_01_mm', 'Rain_02_mm', 'Wind_Dir_Deg',
        'sunrise_time_utc', 'sunset_time_utc', 'Daylight_Duration',
        'Date_UTC_Naive', 'Lat', 'Long', 'Timezone',
        'sunrise_time_utc_calc', 'sunset_time_utc_calc',
        'Daylight_Duration_h_calc', 'fixed_daylight_applied',
        'Year', 'Month', 'Minute', 'Hour', 'Date', 'Day'
    }

    numerical_cols_for_viz = [
        col for col in df_data.columns
        if pd.api.types.is_numeric_dtype(df_data[col]) and col not in EXCLUDE_COLS_VIZ
    ]
    
    if not numerical_cols_for_viz:
        fig = go.Figure().add_annotation(
            x=0.5, y=0.5, text=str(_l("Aucune variable numérique exploitable pour la visualisation des gaps.")),
            showarrow=False, font=dict(size=16)
        )
        fig.update_layout(title=str(f"{str(_l('Visualisation des données et des gaps'))} - {station_name} {title_suffix}"),
                          template="plotly_white")
        return fig

    fig = make_subplots(
        rows=len(numerical_cols_for_viz), cols=1,
        shared_xaxes=True,
        vertical_spacing=0.04,
        subplot_titles=[str(f"{str(_l('Série temporelle'))} - {col}") for col in numerical_cols_for_viz]
    )

    relevant_gaps_filtered = df_gaps[
        (df_gaps['station'] == station_name) & 
        (df_gaps['variable'].isin(numerical_cols_for_viz))
    ].copy()
    
    relevant_gaps_filtered['start_time'] = pd.to_datetime(relevant_gaps_filtered['start_time'], errors='coerce')
    relevant_gaps_filtered['end_time'] = pd.to_datetime(relevant_gaps_filtered['end_time'], errors='coerce')
    relevant_gaps_filtered.dropna(subset=['start_time', 'end_time'], inplace=True)

    for i, col in enumerate(numerical_cols_for_viz, 1):
        fig.add_trace(
            go.Scatter(
                x=df_data.index,
                y=df_data[col],
                mode='lines+markers',
                name=str(f"{col} - {str(_l('Données'))}"),
                line=dict(color='blue', width=1),
                marker=dict(size=3, opacity=0.7),
                hovertemplate="<b>%{x|%Y-%m-%d %H:%M}</b><br>" +
                               f"{col}: %{{y:.2f}}<extra></extra>",
                showlegend=False
            ),
            row=i, col=1
        )

        gaps_for_col = relevant_gaps_filtered[relevant_gaps_filtered['variable'] == col]
        if not gaps_for_col.empty:
            y_min = df_data[col].min()
            y_max = df_data[col].max()
            if pd.isna(y_min) or pd.isna(y_max) or y_max == y_min:
                y_min, y_max = 0, 1
            else:
                y_buffer = (y_max - y_min) * 0.05
                y_min -= y_buffer
                y_max += y_buffer

            for _, gap_row in gaps_for_col.iterrows():
                fig.add_shape(
                    type="rect",
                    xref="x", yref="y",
                    x0=gap_row['start_time'],
                    y0=y_min,
                    y1=y_max,
                    x1=gap_row['end_time'],
                    fillcolor="rgba(255,0,0,0.2)",
                    line_width=0,
                    layer="below",
                    row=i, col=1
                )
                
                gap_mid_time = gap_row['start_time'] + (gap_row['end_time'] - gap_row['start_time']) / 2
                y_for_hover = df_data[col].mean() if df_data[col].notna().any() else 0 
                
                hover_text_gap = str(f"<b>{str(_l('Période Manquante'))}</b><br>" +
                                     f"{str(_l('Variable'))}: {gap_row['variable']}<br>" +
                                     f"{str(_l('Début'))}: {gap_row['start_time'].strftime('%Y-%m-%d %H:%M')}<br>" +
                                     f"{str(_l('Fin'))}: {gap_row['end_time'].strftime('%Y-%m-%d %H:%M')}<br>" +
                                     f"{str(_l('Durée'))}: {gap_row['duration_hours']:.2f} {str(_l('heures'))}")

                fig.add_trace(
                    go.Scatter(
                        x=[gap_mid_time],
                        y=[y_for_hover],
                        mode='markers',
                        marker=dict(size=1, opacity=0),
                        showlegend=False,
                        hoverinfo='text',
                        text=hover_text_gap,
                        name=f"{col} {str(_l('Gap Info'))}"
                    ),
                    row=i, col=1
                )

    fig.update_layout(
        title_text=str(f"{str(_l('Séries temporelles avec périodes manquantes/interpolées'))} - {station_name} {title_suffix}"),
        height=max(400, 300 * len(numerical_cols_for_viz)),
        hovermode="x unified",
        margin=dict(l=50, r=50, b=80, t=100),
        template="plotly_white",
    )

    for i, col in enumerate(numerical_cols_for_viz, 1):
        fig.update_yaxes(title_text=col, row=i, col=1, rangemode='tozero')

    fig.update_xaxes(title_text=str(_l("Temps")), showgrid=True, gridwidth=1, gridcolor='LightGray')

    return fig