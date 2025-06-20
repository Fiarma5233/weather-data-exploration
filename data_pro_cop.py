import pandas as pd
from pyproj import CRS, Transformer
import pytz
from astral.location import LocationInfo
from astral import sun
import numpy as np
import warnings
import os
import gdown # Importation du module gdown pour le téléchargement
import plotly.graph_objects as go # Importation pour gérer les objets Figure Plotly
import matplotlib.pyplot as plt # NOUVEAUX IMPORTS pour Matplotlib
import seaborn as sns
import traceback # Importation de traceback pour les messages d'erreur détaillés
import math # Ajouté pour math.ceil
from datetime import timedelta # Ajouté pour timedelta

# Importation des configurations et métadonnées depuis config.py
from config import METADATA_VARIABLES, PALETTE_DEFAUT, DATA_LIMITS

# Fonction utilitaire pour créer Rain_mm si nécessaire
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
        df_copy['Rain_mm'] = np.nan # Crée la colonne même si aucune source n'est disponible
        warnings.warn("Ni 'Rain_01_mm' ni 'Rain_02_mm' ne sont présents pour créer 'Rain_mm'. 'Rain_mm' est rempli de NaN.")
    return df_copy


def create_datetime(df: pd.DataFrame, bassin: str = None) -> pd.DataFrame:
    """
    Crée la colonne 'Datetime' à partir de colonnes séparées (Year, Month, Day, Hour, Minute)
    ou à partir d'une colonne 'Date' pour le bassin VEA_SISSILI.

    Args:
        df (pd.DataFrame): DataFrame d'entrée.
        bassin (str, optional): Nom du bassin ('DANO', 'DASSARI', 'VEA_SISSILI').
                                Utilisé pour la logique de conversion de la colonne 'Date'.

    Returns:
        pd.DataFrame: DataFrame avec la colonne 'Datetime' et ses composantes, si possible.
    """
    df_copy = df.copy()

    # Prioriser la colonne 'Date' si elle existe pour VEA_SISSILI ou si pas de colonnes Y/M/D/H/M
    if 'Date' in df_copy.columns and (bassin == 'VEA_SISSILI' or not any(col in df_copy.columns for col in ['Year', 'Month', 'Day', 'Hour', 'Minute'])):
        try:
            df_copy['Datetime'] = pd.to_datetime(df_copy['Date'], errors='coerce')
        except Exception as e:
            warnings.warn(f"Impossible de convertir la colonne 'Date' en Datetime pour le bassin {bassin}: {e}")
            df_copy['Datetime'] = pd.NaT
    else:
        date_cols = ['Year', 'Month', 'Day', 'Hour', 'Minute']
        
        for col in date_cols:
            if col in df_copy.columns:
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')

        try:
            existing_date_components = [col for col in ['Year', 'Month', 'Day', 'Hour', 'Minute'] if col in df_copy.columns]
            
            if not existing_date_components:
                raise ValueError("Aucune colonne de composantes de date/heure (Year, Month, Day, Hour, Minute) trouvée.")

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
            warnings.warn(f"Impossible de créer Datetime à partir des colonnes séparées. Erreur: {e}. Colonnes présentes: {df_copy.columns.tolist()}")
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
        warnings.warn("La colonne 'Datetime' est vide ou n'existe pas après la tentative de création. Composantes de date/heure non extraites.")

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

    # Assurez-vous que l'index est bien DatetimeIndex et nettoyez les NaT
    if not isinstance(df_processed.index, pd.DatetimeIndex):
        raise TypeError("Le DataFrame d'entrée pour l'interpolation DOIT avoir un DatetimeIndex.")
    
    initial_rows = len(df_processed)
    df_processed = df_processed[df_processed.index.notna()]
    if len(df_processed) == 0:
        raise ValueError("Après nettoyage des index temporels manquants, le DataFrame est vide. Impossible de procéder à l'interpolation.")
    if initial_rows - len(df_processed) > 0:
        warnings.warn(f"Suppression de {initial_rows - len(df_processed)} lignes avec index Datetime manquant ou invalide dans l'interpolation.")
    
    print(f"DEBUG (interpolation): Type de l'index du DataFrame initial: {type(df_processed.index)}")
    print(f"DEBUG (interpolation): Premières 5 valeurs de l'index après nettoyage des NaT: {df_processed.index[:5].tolist() if not df_processed.empty else 'DataFrame vide'}")

    required_gps_cols = ['Station', 'Lat', 'Long', 'Timezone']
    if not all(col in df_gps.columns for col in required_gps_cols):
        raise ValueError(
            f"df_gps doit contenir les colonnes {required_gps_cols}. "
            f"Colonnes actuelles dans df_gps : {df_gps.columns.tolist()}"
        )

    if not df_gps['Station'].is_unique:
        print("Avertissement: La colonne 'Station' dans df_gps contient des noms de station dupliqués.")
        print("Ceci peut entraîner des comportements inattendus ou des stations non reconnues.")
        df_gps_unique = df_gps.drop_duplicates(subset=['Station'], keep='first').copy()
        print(f"Suppression de {len(df_gps) - len(df_gps_unique)} doublons dans df_gps (en gardant la première occurrence).")
    else:
        df_gps_unique = df_gps.copy()

    gps_info_dict = df_gps_unique.set_index('Station')[['Lat', 'Long', 'Timezone']].to_dict('index')

    numerical_cols = ['Air_Temp_Deg_C', 'Rel_H_%', 'BP_mbar_Avg',
                      'Rain_01_mm', 'Rain_02_mm', 'Rain_mm', 'Wind_Sp_m/sec',
                      'Solar_R_W/m^2', 'Wind_Dir_Deg']
    for col in numerical_cols:
        if col in df_processed.columns:
            df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')

    df_processed_parts = []

    for station_name, group in df_processed.groupby('Station'):
        group_copy = group.copy()
        print(f"DEBUG (interpolation/groupby): Début du traitement du groupe '{station_name}'.")
        
        # Standardize group_copy.index to UTC first
        # This block ensures the index is UTC-aware before proceeding
        if group_copy.index.tz is None:
            group_copy.index = group_copy.index.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
        elif group_copy.index.tz != pytz.utc: # If it's already tz-aware but not UTC, convert to UTC
            group_copy.index = group_copy.index.tz_convert('UTC')
        print(f"DEBUG (interpolation/groupby): Index Datetime pour '{station_name}' STANDARDISÉ à UTC. Dtype: {group_copy.index.dtype}")
        
        # S'assurer que l'index n'a pas de NaT après localisation
        group_copy = group_copy[group_copy.index.notna()]
        if group_copy.empty:
            warnings.warn(f"Le groupe '{station_name}' est vide après nettoyage de l'index Datetime. Il sera ignoré.")
            continue


        # --- Calculs Astral en utilisant le fuseau horaire local ---
        apply_fixed_daylight = True
        gps_data = gps_info_dict.get(station_name)
        if gps_data and pd.notna(gps_data.get('Lat')) and pd.notna(gps_data.get('Long')) and pd.notna(gps_data.get('Timezone')):
            lat = gps_data['Lat']
            long = gps_data['Long']
            timezone_str = gps_data['Timezone']

            try:
                local_tz = pytz.timezone(timezone_str)
                # Create a local timezone-aware version of the index for Astral calculations
                index_for_astral_local = group_copy.index.tz_convert(local_tz)

                daily_sun_info = {}
                # Get unique dates from the local-time index (these are timezone-aware Timestamps)
                # Utilisation de .drop_duplicates() au lieu de .unique() pour garantir un objet Pandas
                unique_dates_ts_local = index_for_astral_local.normalize().drop_duplicates()

                # Ensure unique_dates is not empty before processing
                if unique_dates_ts_local.empty: # Utilisez .empty pour les objets Pandas
                    raise ValueError("No unique dates found for Astral calculation.")
                
                for ts_local_aware in unique_dates_ts_local: # Iterate over timezone-aware Timestamps
                    loc = LocationInfo(station_name, "Site", timezone_str, lat, long)
                    
                    # Convert the timezone-aware Timestamp to a naive datetime.date object
                    # This is what Astral's sun.sun function expects for its 'date' parameter
                    # This conversion helps avoid potential re-localization warnings from Astral/pytz.
                    naive_date_for_astral = ts_local_aware.to_pydatetime().date()
                    
                    # Pass the NAIVE date object to Astral. Astral's observer handles the timezone internally.
                    s = sun.sun(loc.observer, date=naive_date_for_astral) 
                    daily_sun_info[naive_date_for_astral] = {
                        'sunrise': s['sunrise'],
                        'sunset': s['sunset']
                    }

                # Explicitly create a list of naive dates for the DataFrame index
                naive_unique_dates_for_index = [ts.date() for ts in unique_dates_ts_local] # Use .date() here as keys are naive
                temp_df_sun_index = pd.Index(naive_unique_dates_for_index, name='Date_Local_Naive')
                temp_df_sun = pd.DataFrame(index=temp_df_sun_index)
                
                # NOUVEAUX DÉBOGAGES pour comprendre le type juste avant l'opération
                print(f"DEBUG (astral_calc): unique_dates_ts_local type: {type(unique_dates_ts_local)}")
                print(f"DEBUG (astral_calc): naive_unique_dates_for_index type: {type(naive_unique_dates_for_index)}")
                print(f"DEBUG (astral_calc): temp_df_sun_index type: {type(temp_df_sun_index)}")
                if not temp_df_sun.empty:
                    print(f"DEBUG (astral_calc): First element of temp_df_sun.index: {temp_df_sun.index[0]}")
                    print(f"DEBUG (astral_calc): Type of first element of temp_df_sun.index: {type(temp_df_sun.index[0])}")

                # Correction: Utilisation de la compréhension de liste pour éviter le problème de .map()
                temp_df_sun['sunrise_time_local'] = [daily_sun_info.get(date, {}).get('sunrise') for date in temp_df_sun.index]
                temp_df_sun['sunset_time_local'] = [daily_sun_info.get(date, {}).get('sunset') for date in temp_df_sun.index]

                # Merge with group_copy (which has UTC index)
                # To merge, create a normalized local date column (naive) in group_copy
                group_copy_reset = group_copy.reset_index()
                group_copy_reset['Date_Local_Naive'] = group_copy_reset['Datetime'].dt.tz_convert(local_tz).dt.date

                group_copy_reset = pd.merge(group_copy_reset, temp_df_sun, on='Date_Local_Naive', how='left')

                # Convert merged local times back to UTC for comparison with original 'Datetime' (which is UTC)
                group_copy_reset['sunrise_time_utc'] = group_copy_reset['sunrise_time_local'].dt.tz_convert('UTC')
                group_copy_reset['sunset_time_utc'] = group_copy_reset['sunset_time_local'].dt.tz_convert('UTC')

                group_copy_reset.loc[:, 'Is_Daylight'] = (group_copy_reset['Datetime'] >= group_copy_reset['sunrise_time_utc']) & \
                                                          (group_copy_reset['Datetime'] < group_copy_reset['sunset_time_utc'])

                daylight_timedelta_local = group_copy_reset['sunset_time_local'] - group_copy_reset['sunrise_time_local']
                group_copy_reset.loc[:, 'Daylight_Duration'] = daylight_timedelta_local.astype(str).replace('NaT', np.nan)

                group_copy = group_copy_reset.set_index('Datetime')
                group_copy = group_copy.drop(columns=['Date_Local_Naive', 'sunrise_time_local', 'sunset_time_local', 'sunrise_time_utc', 'sunset_time_utc'], errors='ignore')

                print(f"Lever et coucher du soleil calculés pour {station_name}.")
                apply_fixed_daylight = False

            except Exception as e:
                print(f"Erreur lors du calcul du lever/coucher du soleil avec Astral pour {station_name}: {e}.")
                traceback.print_exc() # Print full traceback for this specific error
                warnings.warn(f"Calcul Astral impossible pour '{station_name}'. Utilisation de l'indicateur jour/nuit fixe.")
                apply_fixed_daylight = True
        else:
            print(f"Avertissement: Coordonnées ou Fuseau horaire manquants/invalides pour le site '{station_name}' dans df_gps. Utilisation de l'indicateur jour/nuit fixe.")
            apply_fixed_daylight = True

        if apply_fixed_daylight:
            group_copy.loc[:, 'Is_Daylight'] = (group_copy.index.hour >= 7) & (group_copy.index.hour <= 18)
            group_copy.loc[:, 'Daylight_Duration'] = "11:00:00"
            print(f"Utilisation de l'indicateur jour/nuit fixe (7h-18h) pour {station_name}.")

        df_processed_parts.append(group_copy)

    if not df_processed_parts:
        raise ValueError("Aucune partie de DataFrame n'a pu être traitée après le regroupement par station.")

    # Concaténation de toutes les parties. Puisque chaque partie a un DatetimeIndex,
    # nous pouvons concaténer directement sans ignore_index=True
    df_final = pd.concat(df_processed_parts)
    # Assurez-vous que l'index est trié et qu'il est bien nommé 'Datetime'
    df_final = pd.concat(df_processed_parts).sort_index()
    df_final.index.name = 'Datetime' 
    print(f"DEBUG (interpolation/concat): Index du DataFrame final après concaténation et tri: {type(df_final.index)}")
    print(f"DEBUG (interpolation/concat): Colonnes du DataFrame final après concaténation: {df_final.columns.tolist()}")


    # Nettoyage des colonnes temporaires (elles ne sont plus nécessaires ou n'existent plus)
    cols_to_drop_after_process = ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
    df_final = df_final.drop(columns=cols_to_drop_after_process, errors='ignore')

    # Gestion intelligente de Rain_mm
    if 'Rain_mm' not in df_final.columns or df_final['Rain_mm'].isnull().all():
        if 'Rain_01_mm' in df_final.columns and 'Rain_02_mm' in df_final.columns:
            df_final = create_rain_mm(df_final)
            warnings.warn("Colonne Rain_mm créée à partir des deux capteurs.")
        else:
            warnings.warn("Rain_mm manquant et impossible à créer (capteurs pluie incomplets).")
            if 'Rain_mm' not in df_final.columns:
                df_final['Rain_mm'] = np.nan


    # Interpolation standard et bornage pour les variables numériques
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
                    warnings.warn(f"Remplacement de {new_nan_count - initial_nan_count} valeurs hors limites dans '{var}' par NaN.")
            
            print(f"DEBUG (interpolation/variable): Interpolation de '{var}'. Type de l'index de df_final: {type(df_final.index)}")
            
            # Interpolation seulement si l'index est bien DatetimeIndex
            if isinstance(df_final.index, pd.DatetimeIndex):
                df_final[var] = df_final[var].interpolate(method='time', limit_direction='both')
            else:
                print(f"Avertissement (interpolation/variable): L'index n'est pas un DatetimeIndex pour l'interpolation de '{var}'. Utilisation de la méthode 'linear'.")
                df_final[var] = df_final[var].interpolate(method='linear', limit_direction='both')
            df_final[var] = df_final[var].bfill().ffill()

    # Interpolation CONDITIONNELLE de la radiation solaire
    if 'Solar_R_W/m^2' in df_final.columns:
        df_final['Solar_R_W/m^2'] = pd.to_numeric(df_final['Solar_R_W/m^2'], errors='coerce')

        if 'Solar_R_W/m^2' in limits:
            min_val = limits['Solar_R_W/m^2']['min']
            max_val = limits['Solar_R_W/m^2']['max']
            initial_nan_count = df_final['Solar_R_W/m^2'].isna().sum()
            df_final.loc[(df_final['Solar_R_W/m^2'] < min_val) | (df_final['Solar_R_W/m^2'] > max_val), 'Solar_R_W/m^2'] = np.nan
            if df_final['Solar_R_W/m^2'].isna().sum() > initial_nan_count:
                warnings.warn(f"Remplacement de {df_final['Solar_R_W/m^2'].isna().sum() - initial_nan_count} valeurs hors limites dans 'Solar_R_W/m^2' par NaN.")

        if 'Is_Daylight' in df_final.columns:
            df_final.loc[~df_final['Is_Daylight'] & (df_final['Solar_R_W/m^2'] > 0), 'Solar_R_W/m^2'] = 0

            if 'Rain_mm' in df_final.columns:
                cond_suspect_zeros = (df_final['Is_Daylight']) & (df_final['Solar_R_W/m^2'] == 0) & (df_final['Rain_mm'] == 0)
            else:
                cond_suspect_zeros = (df_final['Is_Daylight']) & (df_final['Solar_R_W/m^2'] == 0)
                warnings.warn("Rain_mm manquant. Tous les 0 de radiation solaire pendant le jour sont traités comme suspects.")
            df_final.loc[cond_suspect_zeros, 'Solar_R_W/m^2'] = np.nan

            print(f"DEBUG (interpolation/solaire): Interpolation de 'Solar_R_W/m^2' (conditionnel). Type de l'index de df_final: {type(df_final.index)}")

            is_day = df_final['Is_Daylight']
            if isinstance(df_final.index, pd.DatetimeIndex):
                df_final.loc[is_day, 'Solar_R_W/m^2'] = df_final.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='time', limit_direction='both')
            else:
                print(f"Avertissement (interpolation/solaire): L'index n'est pas un DatetimeIndex pour l'interpolation de 'Solar_R_W/m^2'. Utilisation de la méthode 'linear'.")
                df_final.loc[is_day, 'Solar_R_W/m^2'] = df_final.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='linear', limit_direction='both')

            df_final.loc[is_day, 'Solar_R_W/m^2'] = df_final.loc[is_day, 'Solar_R_W/m^2'].bfill().ffill()

            df_final.loc[~is_day & df_final['Solar_R_W/m^2'].isna(), 'Solar_R_W/m^2'] = 0
            warnings.warn("Radiation solaire interpolée avec succès.")
        else:
            warnings.warn("Colonne 'Is_Daylight' manquante. Radiation solaire interpolée standard.")
            if isinstance(df_final.index, pd.DatetimeIndex):
                 df_final['Solar_R_W/m^2'] = df_final['Solar_R_W/m^2'].interpolate(method='time', limit_direction='both').bfill().ffill()
            else:
                 df_final['Solar_R_W/m^2'] = df_final['Solar_R_W/m^2'].interpolate(method='linear', limit_direction='both').bfill().ffill()

    warnings.warn("Vérification des valeurs manquantes après interpolation:")
    missing_after_interp = df_final.isna().sum()
    columns_with_missing = missing_after_interp[missing_after_interp > 0]
    if not columns_with_missing.empty:
        warnings.warn(f"Valeurs manquantes persistantes:\n{columns_with_missing}")
    else:
        warnings.warn("Aucune valeur manquante après l'interpolation.")

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
        raise ValueError(
            f"Le DataFrame doit contenir les colonnes {required_utm_cols} pour la conversion UTM."
        )

    def convert_row(row):
        try:
            zone = int(row['zone'])
            hemisphere = str(row['hemisphere']).upper()
            is_northern = hemisphere == 'N'

            proj_utm = CRS.from_proj4(
                f"+proj=utm +zone={zone} +datum=WGS84 +units=m +{'north' if is_northern else 'south'}"
            )
            proj_wgs84 = CRS.from_epsg(4326) # WGS84 Lat/Long

            transformer = Transformer.from_crs(proj_utm, proj_wgs84, always_xy=True)
            lon, lat = transformer.transform(row['Easting'], row['Northing'])
            return pd.Series({'Long': lon, 'Lat': lat})
        except Exception as e:
            warnings.warn(f"Erreur lors de la conversion UTM d'une ligne: {e}")
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

    Cette fonction est destinée à être appelée une seule fois au démarrage de l'application.
    Elle vérifie d'abord si les fichiers existent localement pour éviter des téléchargements répétés.

    Returns:
        pd.DataFrame: Un DataFrame consolidé de toutes les stations avec
                      leurs coordonnées GPS (Lat, Long) et fuseaux horaires.
    """
    print("Début de la préparation des données de coordonnées des stations...")
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True) # Assurez-vous que le dossier 'data' existe

    files_info = [
        {'id': '1Iz5L_XkumG390EZvnMgYr3KwDYeesrNz', 'name': "WASCAL Basins Climate Station Coordinates.xlsx", 'bassin': 'Vea Sissili'},
        {'id': '1H8A-sVMtTok6lrD-NFHQxzHBeQ_P7g4z', 'name': "Dano Basins Climate Station Coordinates.xlsx", 'bassin': 'Dano'},
        {'id': '1SOXI0ZvWqpNp6Qwz_BGeWleUtaYMaOBU', 'name': "DASSARI Climate Station Coordinates.xlsx", 'bassin': 'Dassari'}
    ]

    loaded_dfs = []

    for file_info in files_info:
        output_file_path = os.path.join(data_dir, file_info['name'])
        
        if not os.path.exists(output_file_path):
            print(f"Téléchargement de {file_info['bassin']} depuis Google Drive...")
            gdown.download(f'https://drive.google.com/uc?id={file_info["id"]}', output_file_path, quiet=False)
            print(f"Téléchargement de {file_info['bassin']} terminé.")
        else:
            print(f"Chargement de {file_info['bassin']} depuis le cache local: {output_file_path}")
        
        loaded_dfs.append(pd.read_excel(output_file_path))

    vea_sissili_bassin = loaded_dfs[0]
    dano_bassin = loaded_dfs[1]
    dassari_bassin = loaded_dfs[2]

    # Prétraitement des DataFrames (votre code original)
    print("Début du prétraitement des données de stations...")
    
    # Vea Sissili
    vea_sissili_bassin = vea_sissili_bassin.drop(columns=['No', 'Location', 'parameters'], errors='ignore')
    new_row_df_vea = pd.DataFrame([{'Name': 'Atampisi', 'Lat': 10.91501, 'Long': -0.82647}])
    vea_sissili_bassin = pd.concat([vea_sissili_bassin, new_row_df_vea], ignore_index=True)

    # Dassari
    dassari_bassin = dassari_bassin.drop(columns=['Altitude (en m)'], errors='ignore')
    new_rows_df_dassari = pd.DataFrame([{'Site name': 'Pouri', 'Lat': 1207107, 'Long': 293642}, {'Site name': 'Fandohoun', 'Lat': 1207107, 'Long': 293642}])
    dassari_bassin = pd.concat([dassari_bassin, new_rows_df_dassari], ignore_index=True)

    # Dano et Dassari : Renommage et ajout des colonnes UTM
    dano_bassin = dano_bassin.rename(columns={'Long': 'Easting', 'Lat': 'Northing', 'Site Name': 'Name'})
    dassari_bassin = dassari_bassin.rename(columns={'Long': 'Easting', 'Lat': 'Northing', 'Site name': 'Name'})

    dano_bassin['zone'] = 30
    dano_bassin['hemisphere'] = 'N'
    dassari_bassin['zone'] = 31
    dassari_bassin['hemisphere'] = 'N'

    # Application de la fonction de conversion UTM vers GPS
    dano_bassin = convert_utm_df_to_gps(dano_bassin)
    dassari_bassin = convert_utm_df_to_gps(dassari_bassin)

    # Ajout des fuseaux horaires
    dano_bassin['Timezone'] = 'Africa/Ouagadougou'
    dassari_bassin['Timezone'] = 'Africa/Porto-Novo'
    vea_sissili_bassin['Timezone'] = 'Africa/Accra' # Assurez-vous que c'est le bon fuseau horaire pour Vea Sissili

    # Fusion de tous les bassins
    bassins = pd.concat([vea_sissili_bassin, dano_bassin, dassari_bassin], ignore_index=True)

    # Renommer 'Name' en 'Station'
    bassins = bassins.rename(columns={'Name': 'Station'})

    # Nettoyer les lignes avec des valeurs NaN dans Lat/Long/Timezone qui pourraient résulter d'erreurs de conversion
    initial_rows = len(bassins)
    bassins = bassins.dropna(subset=['Lat', 'Long', 'Timezone', 'Station'])
    if len(bassins) < initial_rows:
        print(f"Attention: {initial_rows - len(bassins)} lignes avec des coordonnées ou fuseaux horaires manquants ont été supprimées du DataFrame des stations.")
    
    # 5. Sauvegarde du DataFrame final en JSON
    output_json_path = os.path.join(data_dir, "station_coordinates.json")
    # Utiliser to_json avec orient='records' pour un format plus lisible et facile à charger
    bassins.to_json(output_json_path, orient='records', indent=4)
    print(f"\nPréparation des données terminée. Coordonnées des stations sauvegardées dans '{output_json_path}'.")
    print("Vous pouvez maintenant lancer votre application Flask.")

    return bassins # Retourner le DataFrame des données GPS


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
            warnings.warn(f"Suppression de {initial_rows - len(df_cleaned)} doublons basés sur 'Station' et 'Datetime'.")
        return df_cleaned
    else:
        warnings.warn("Colonnes 'Station' ou 'Datetime' manquantes pour la gestion des doublons. Le DataFrame n'a pas été modifié.")
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
        warnings.warn("L'index n'est pas un DatetimeIndex dans traiter_outliers_meteo. Tentative de conversion.")
        try:
            df_processed.index = pd.to_datetime(df_processed.index, errors='coerce')
            df_processed = df_processed[df_processed.index.notna()]
            if df_processed.empty:
                raise ValueError("DataFrame vide après nettoyage des dates invalides dans traiter_outliers_meteo.")
        except Exception as e:
            raise TypeError(f"Impossible de garantir un DatetimeIndex pour traiter_outliers_meteo: {e}")

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
                    warnings.warn(f"Remplacement de {new_nan_count - initial_nan_count} valeurs hors limites dans '{var}' par NaN.")
    return df_processed

def generer_graphique_par_variable_et_periode(df: pd.DataFrame, station: str, variable: str, periode: str, colors: dict, metadata: dict) -> go.Figure:
    """
    Génère un graphique Plotly de l'évolution d'une variable pour une station sur une période donnée.
    Retourne l'objet Figure Plotly.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("Le DataFrame doit avoir un DatetimeIndex pour générer le graphique par variable et période.")

    filtered_df = df[df['Station'] == station].copy()
    if filtered_df.empty:
        return go.Figure() # Retourne une figure vide si pas de données

    if periode == 'Journalière':
        resampled_df = filtered_df[variable].resample('D').mean()
    elif periode == 'Hebdomadaire':
        resampled_df = filtered_df[variable].resample('W').mean()
    elif periode == 'Mensuelle':
        resampled_df = filtered_df[variable].resample('M').mean()
    elif periode == 'Annuelle':
        resampled_df = filtered_df[variable].resample('Y').mean()
    else: # Données Brutes
        resampled_df = filtered_df[variable]

    resampled_df = resampled_df.dropna()

    if resampled_df.empty:
        return go.Figure() # Retourne une figure vide si pas de données après resample/dropna

    variable_meta = metadata.get(variable, {'Nom': variable, 'Unite': ''})
    color = colors.get(station, '#1f77b4')

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=resampled_df.index, y=resampled_df.values,
                             mode='lines', name=f'{variable_meta["Nom"]} - {station}',
                             line=dict(color=color)))

    fig.update_layout(
        title=f"Évolution de {variable_meta['Nom']} ({variable_meta['Unite']}) pour {station} ({periode})",
        xaxis_title="Date",
        yaxis_title=f"{variable_meta['Nom']} ({variable_meta['Unite']})",
        hovermode="x unified",
        template="plotly_white" # Utiliser un template Plotly plus clair
    )
    return fig

def generer_graphique_comparatif(df: pd.DataFrame, variable: str, periode: str, colors: dict, metadata: dict) -> go.Figure:
    """
    Génère un graphique Plotly comparatif de l'évolution d'une variable entre toutes les stations.
    Retourne l'objet Figure Plotly.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("Le DataFrame doit avoir un DatetimeIndex pour générer le graphique comparatif.")

    fig = go.Figure()
    
    all_stations = df['Station'].unique()
    if len(all_stations) < 2:
        warnings.warn("Moins de 2 stations disponibles pour la comparaison. Le graphique comparatif ne sera pas généré.")
        return go.Figure() # Retourne une figure vide si moins de 2 stations

    for station in all_stations:
        filtered_df = df[df['Station'] == station].copy()
        if filtered_df.empty:
            continue

        if periode == 'Journalière':
            resampled_df = filtered_df[variable].resample('D').mean()
        elif periode == 'Hebdomadaire':
            resampled_df = filtered_df[variable].resample('W').mean()
        elif periode == 'Mensuelle':
            resampled_df = filtered_df[variable].resample('M').mean()
        elif periode == 'Annuelle':
            resampled_df = filtered_df[variable].resample('Y').mean()
        else: # Données Brutes
            resampled_df = filtered_df[variable]

        resampled_df = resampled_df.dropna()
        if resampled_df.empty:
            continue
        
        color = colors.get(station, '#1f77b4')
        fig.add_trace(go.Scatter(x=resampled_df.index, y=resampled_df.values,
                                 mode='lines', name=station,
                                 line=dict(color=color)))

    if not fig.data:
        return go.Figure() # Retourne une figure vide si aucune trace n'a été ajoutée

    variable_meta = metadata.get(variable, {'Nom': variable, 'Unite': ''})
    fig.update_layout(
        title=f"Comparaison de {variable_meta['Nom']} ({variable_meta['Unite']}) entre stations ({periode})",
        xaxis_title="Date",
        yaxis_title=f"{variable_meta['Nom']} ({variable_meta['Unite']})",
        hovermode="x unified",
        legend_title="Variables",
        template="plotly_white"
    )
    return fig


def generate_multi_variable_station_plot(df: pd.DataFrame, station: str, colors: dict, metadata: dict) -> go.Figure:
    """
    Génère un graphique Plotly de l'évolution normalisée de plusieurs variables pour une station donnée.
    Retourne l'objet Figure Plotly.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("Le DataFrame doit avoir un DatetimeIndex pour générer le graphique multi-variables.")

    filtered_df = df[df['Station'] == station].copy()
    if filtered_df.empty:
        return go.Figure() # Retourne une figure vide si pas de données

    numerical_vars = [col for col in filtered_df.columns if pd.api.types.is_numeric_dtype(filtered_df[col]) and col not in ['Station', 'Is_Daylight', 'Daylight_Duration']]

    if not numerical_vars:
        warnings.warn("Aucune variable numérique trouvée pour la station sélectionnée.")
        return go.Figure() # Retourne une figure vide si pas de variables numériques

    normalized_df = filtered_df[numerical_vars].copy()
    for col in normalized_df.columns:
        min_val = normalized_df[col].min()
        max_val = normalized_df[col].max()
        if max_val != min_val:
            normalized_df[col] = (normalized_df[col] - min_val) / (max_val - min_val)
        else:
            normalized_df[col] = 0.5 if pd.notna(min_val) else np.nan

    normalized_df = normalized_df.dropna(how='all')

    if normalized_df.empty:
        return go.Figure() # Retourne une figure vide si pas de données après normalisation/dropna
    
    fig = go.Figure()
    for var in normalized_df.columns:
        var_meta = metadata.get(var, {'Nom': var, 'Unite': ''})
        color = colors.get(var, None)

        fig.add_trace(go.Scatter(x=normalized_df.index, y=normalized_df[var],
                                 mode='lines', name=var_meta['Nom'],
                                 line=dict(color=color)))

    fig.update_layout(
        title=f"Évolution Normalisée des Variables Météorologiques pour la station {station}",
        xaxis_title="Date",
        yaxis_title="Valeur Normalisée (0-1)",
        hovermode="x unified",
        legend_title="Variables",
        template="plotly_white"
    )
    return fig

def calculate_daily_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les statistiques journalières (moyenne, min, max, somme) pour les variables numériques
    groupées par station. Cette fonction renvoie un DataFrame de statistiques, non un graphique.
    C'est la fonction qui remplace l'ancienne 'daily_stats'.

    Args:
        df (pd.DataFrame): DataFrame d'entrée avec DatetimeIndex et colonne 'Station'.

    Returns:
        pd.DataFrame: DataFrame avec les statistiques journalières.
    """
    df_copy = df.copy()

    # Assurez-vous que 'Datetime' est une colonne et de type datetime
    if isinstance(df_copy.index, pd.DatetimeIndex):
        df_copy = df_copy.reset_index()

    df_copy['Datetime'] = pd.to_datetime(df_copy['Datetime'], errors='coerce')
    df_copy = df_copy.dropna(subset=['Datetime', 'Station'])

    if df_copy.empty:
        print("Avertissement: Le DataFrame est vide après le nettoyage des dates et stations dans calculate_daily_summary_table.")
        return pd.DataFrame()

    if 'Is_Daylight' not in df_copy.columns:
        warnings.warn("La colonne 'Is_Daylight' est manquante. Calcul en utilisant une règle fixe (7h-18h).")
        df_copy['Is_Daylight'] = (df_copy['Datetime'].dt.hour >= 7) & (df_copy['Datetime'].dt.hour <= 18)

    numerical_cols = [col for col in df_copy.columns if pd.api.types.is_numeric_dtype(df_copy[col]) and col not in ['Station', 'Datetime', 'Is_Daylight', 'Daylight_Duration']]
    
    if not numerical_cols:
        warnings.warn("Aucune colonne numérique valide trouvée pour le calcul des statistiques journalières.")
        return pd.DataFrame()

    # Calcul des statistiques de base par jour
    daily_aggregated_df = df_copy.groupby(['Station', df_copy['Datetime'].dt.date]).agg({
        col: ['mean', 'min', 'max'] for col in numerical_cols if METADATA_VARIABLES.get(col, {}).get('is_rain') == False
    })

    # Renommage des colonnes agrégées pour les non-pluies
    daily_aggregated_df.columns = ['_'.join(col).strip() for col in daily_aggregated_df.columns.values]


    # Traitement spécifique pour la pluie (Rain_mm)
    if 'Rain_mm' in df_copy.columns and METADATA_VARIABLES.get('Rain_mm', {}).get('is_rain'):
        df_daily_rain = df_copy.groupby(['Station', df_copy['Datetime'].dt.date])['Rain_mm'].sum().reset_index()
        df_daily_rain = df_daily_rain.rename(columns={'Rain_mm': 'Rain_mm_sum'})

        # Fusionner les statistiques de pluie avec les autres
        if not daily_aggregated_df.empty:
            daily_aggregated_df = daily_aggregated_df.reset_index()
            daily_stats_df = pd.merge(daily_aggregated_df, df_daily_rain, on=['Station', 'Datetime'], how='left')
            daily_stats_df = daily_stats_df.rename(columns={'Datetime': 'Date'})
        else:
            daily_stats_df = df_daily_rain.rename(columns={'Datetime': 'Date'})
    else:
        daily_stats_df = daily_aggregated_df.reset_index().rename(columns={'Datetime': 'Date'})


    # Calcul des statistiques de saison et de sécheresse pour la pluie
    if 'Rain_mm' in df_copy.columns and METADATA_VARIABLES.get('Rain_mm', {}).get('is_rain'):
        df_daily_rain_raw = df_copy.groupby(['Station', pd.Grouper(key='Datetime', freq='D')])['Rain_mm'].sum().reset_index()
        
        RAIN_SEASON_GAP_THRESHOLD = pd.Timedelta(days=60)
        season_stats = []
        for station_name, station_df_rain in df_daily_rain_raw.groupby('Station'):
            station_df_rain = station_df_rain.set_index('Datetime').sort_index()
            rain_events = station_df_rain[station_df_rain['Rain_mm'] > 0].index

            if rain_events.empty:
                season_stats.append({'Station': station_name, 'Moyenne_Saison_Pluvieuse': np.nan, 'Debut_Saison_Pluvieuse': pd.NaT, 'Fin_Saison_Pluvieuse': pd.NaT, 'Duree_Saison_Pluvieuse_Jours': np.nan})
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
                season_stats.append({'Station': station_name, 'Moyenne_Saison_Pluvieuse': np.nan, 'Debut_Saison_Pluvieuse': pd.NaT, 'Fin_Saison_Pluvieuse': pd.NaT, 'Duree_Saison_Pluvieuse_Jours': np.nan})
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
        
        # Merge season stats into main daily_stats_df
        if not df_season_stats.empty:
            daily_stats_df = pd.merge(daily_stats_df, df_season_stats, on='Station', how='left')


    # Calcul des statistiques globales (Max, Min, Moyenne, Médiane, etc.) par station
    # pour les variables numériques, à inclure dans le tableau récapitulatif.
    final_stats_per_station = pd.DataFrame()
    for station_name in df_copy['Station'].unique():
        station_df = df_copy[df_copy['Station'] == station_name].copy()
        station_summary = {'Station': station_name}

        for var in numerical_cols:
            if var in station_df.columns and pd.api.types.is_numeric_dtype(station_df[var]):
                # Filtrage pour la radiation solaire (seulement pendant le jour)
                if var == 'Solar_R_W/m^2':
                    var_data = station_df.loc[station_df['Is_Daylight'], var].dropna()
                else:
                    var_data = station_df[var].dropna()
                
                if not var_data.empty:
                    station_summary[f'{var}_Maximum'] = var_data.max()
                    station_summary[f'{var}_Minimum'] = var_data.min()
                    station_summary[f'{var}_Moyenne'] = var_data.mean()
                    station_summary[f'{var}_Mediane'] = var_data.median()
                    
                    # Pour Rain_mm uniquement
                    if var == 'Rain_mm':
                        station_summary[f'{var}_Cumul_Annuel'] = station_df['Rain_mm'].sum()
                        # Moyenne des jours pluvieux (seulement les jours où il a plu)
                        rainy_days_data = station_df[station_df['Rain_mm'] > 0]['Rain_mm'].dropna()
                        station_summary[f'{var}_Moyenne_Jours_Pluvieux'] = rainy_days_data.mean() if not rainy_days_data.empty else np.nan

                        # Ajouter la durée de la saison pluvieuse et de sécheresse du df_season_stats
                        if 'Duree_Saison_Pluvieuse_Jours' in daily_stats_df.columns:
                            s_data = daily_stats_df[daily_stats_df['Station'] == station_name]
                            if not s_data.empty:
                                station_summary[f'{var}_Duree_Saison_Pluvieuse_Jours'] = s_data['Duree_Saison_Pluvieuse_Jours'].iloc[0]
                                # Placeholder pour sécheresse si non calculé ailleurs
                                station_summary[f'{var}_Duree_Secheresse_Definie_Jours'] = np.nan # Vous devrez calculer ceci plus tard

        final_stats_per_station = pd.concat([final_stats_per_station, pd.DataFrame([station_summary])], ignore_index=True)
        
    return final_stats_per_station # Retourne le DataFrame de statistiques agrégées par station


def generate_variable_summary_plots_for_web(df: pd.DataFrame, station: str, variable: str, metadata: dict, palette: dict) -> plt.Figure:
    """
    Génère un graphique Matplotlib/Seaborn pour les statistiques agrégées d'une variable spécifique
    pour une station donnée, en utilisant la logique fournie par l'utilisateur pour 'daily_stats'.

    Args:
        df (pd.DataFrame): Le DataFrame global de données traitées avec DatetimeIndex et colonne 'Station'.
        station (str): Le nom de la station à visualiser.
        variable (str): La variable à visualiser (e.g., 'Air_Temp_Deg_C', 'Rain_mm').
        metadata (dict): Dictionnaire de métadonnées pour les variables (Nom, Unite, is_rain).
        palette (dict): Dictionnaire de couleurs pour les différentes métriques statistiques.

    Returns:
        plt.Figure: Un objet Figure Matplotlib contenant tous les sous-graphiques pour la variable sélectionnée.
    """
    df_station = df[df['Station'] == station].copy()

    if df_station.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, f"Aucune donnée pour la station '{station}'.", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
        ax.axis('off')
        return fig

    # S'assurer que Datetime est un index datetime correct pour les données de la station
    if isinstance(df_station.index, pd.DatetimeIndex):
        df_station = df_station.reset_index() # Réinitialise l'index pour un accès facile aux colonnes
    
    df_station['Datetime'] = pd.to_datetime(df_station['Datetime'], errors='coerce')
    df_station = df_station.dropna(subset=['Datetime', 'Station'])
    df_station = df_station.set_index('Datetime').sort_index() # Remet l'index pour les opérations de séries temporelles

    if df_station.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, f"DataFrame vide après nettoyage des dates pour la station '{station}'.", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
        ax.axis('off')
        return fig

    # Assurez-vous que 'Is_Daylight' existe (généralement géré par interpolation, mais au cas où)
    if 'Is_Daylight' not in df_station.columns:
        df_station['Is_Daylight'] = (df_station.index.hour >= 7) & (df_station.index.hour <= 18)


    var_meta = metadata.get(variable, {'Nom': variable, 'Unite': ''})

    # Dictionnaire pour stocker les statistiques pour la variable sélectionnée
    stats_for_plot = {}
    metrics_to_plot = []
    
    # --- LOGIQUE SPÉCIFIQUE À RAIN_MM (transférée de la fonction daily_stats de l'utilisateur) ---
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

        # Logique de détection de la sécheresse
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
                        if current_ratio < s_moyenne_saison: # Condition pour la sécheresse définie
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

        # Statistiques pour Rain_mm (tirées de la vue consolidée de daily_stats de l'utilisateur)
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

        # Récupérer les dates pour max/min pour Rain_mm
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
        
    # --- LOGIQUE POUR LES AUTRES VARIABLES (transférée de la fonction daily_stats de l'utilisateur) ---
    else:
        current_var_data = df_station[variable].dropna()
        if variable == 'Solar_R_W/m^2':
            current_var_data = df_station.loc[df_station['Is_Daylight'], variable].dropna()

        if current_var_data.empty:
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, f"Aucune donnée valide pour la variable {var_meta['Nom']} à {station}.", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
            ax.axis('off')
            return fig

        # Statistiques pour les autres variables (de daily_stats de l'utilisateur)
        stats_for_plot['Maximum'] = current_var_data.max()
        stats_for_plot['Minimum'] = current_var_data.min()
        stats_for_plot['Mediane'] = current_var_data.median()
        stats_for_plot['Moyenne'] = current_var_data.mean()

        # Récupérer les dates pour max/min
        max_idx = current_var_data.idxmax() if not current_var_data.empty else pd.NaT
        min_idx = current_var_data.idxmin() if not current_var_data.empty else pd.NaT

        stats_for_plot['Date_Maximum'] = max_idx if pd.notna(max_idx) else pd.NaT
        stats_for_plot['Date_Minimum'] = min_idx if pd.notna(min_idx) else pd.NaT

        metrics_to_plot = ['Maximum', 'Minimum', 'Moyenne', 'Mediane']
        nrows, ncols = 2, 2
        figsize = (18, 12)

    # Gérer le cas où stats_for_plot pourrait être vide (ex: toutes les données sont NaN)
    if not stats_for_plot:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, f"Impossible de calculer des statistiques pour la variable '{variable}' à la station '{station}' (données manquantes ou non numériques).", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
        ax.axis('off')
        return fig

    # --- PARTIE COMMUNE DE TRACÉ (adaptée de la fonction daily_stats de l'utilisateur) ---
    fig, axes = plt.subplots(nrows, ncols, figsize=figsize)
    plt.subplots_adjust(hspace=0.6, wspace=0.4) # Ajustement de l'espacement pour un meilleur ajustement
    axes = axes.flatten()

    fig.suptitle(f'Statistiques de {var_meta["Nom"]} pour la station {station}', fontsize=16, y=0.98)

    for i, metric in enumerate(metrics_to_plot):
        ax = axes[i]
        value = stats_for_plot.get(metric)
        if pd.isna(value):
            ax.text(0.5, 0.5, "Données non disponibles", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=12, color='gray')
            ax.axis('off')
            continue

        color = palette.get(metric.replace(' ', '_'), '#cccccc') # Utilisation de la `palette` passée en argument
        
        # Créer un DataFrame factice pour sns.barplot, car il attend un DataFrame
        plot_data_bar = pd.DataFrame({'Metric': [metric.replace('_', ' ')], 'Value': [value]})
        sns.barplot(x='Metric', y='Value', data=plot_data_bar, ax=ax, color=color, edgecolor='none')

        # Ajouter les annotations
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
            # Vérifier les clés de date pour max/min si elles existent dans stats_for_plot
            if (metric == 'Maximum' and 'Date_Maximum' in stats_for_plot and pd.notna(stats_for_plot['Date_Maximum'])):
                date_str = f"\n({stats_for_plot['Date_Maximum'].strftime('%d/%m/%Y')})"
            elif (metric == 'Minimum' and 'Date_Minimum' in stats_for_plot and pd.notna(stats_for_plot['Date_Minimum'])):
                date_str = f"\n({stats_for_plot['Date_Minimum'].strftime('%d/%m/%Y')})"
            
            text = f"{value:.1f} {unit}{date_str}"
        else:
            text = f"{value:.1f} {var_meta['Unite']}"

        # Positionne le texte sur le graphique.
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                text, ha='center', va='bottom', fontsize=9, color='black',
                bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=1))
        
        ax.set_title(f"{var_meta['Nom']} {metric.replace('_', ' ')}", fontsize=11)
        ax.set_xlabel("")
        ax.set_ylabel(f"Valeur ({var_meta['Unite']})", fontsize=10)
        ax.tick_params(axis='x', rotation=0)
        ax.set_xticklabels([])

    # Si moins de graphiques que de sous-graphiques, désactiver les axes inutilisés
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.tight_layout(rect=[0, 0, 1, 0.96])

    return fig

def daily_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les statistiques journalières (moyenne, min, max, somme) pour les variables numériques
    groupées par station.

    Args:
        df (pd.DataFrame): DataFrame d'entrée avec DatetimeIndex et colonne 'Station'.

    Returns:
        pd.DataFrame: DataFrame avec les statistiques journalières.
    """
    df = df.copy()

    # Assurez-vous que 'Datetime' est une colonne et de type datetime
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index()

    df['Datetime'] = pd.to_datetime(df['Datetime'], errors='coerce')
    df = df.dropna(subset=['Datetime', 'Station'])

    if df.empty:
        print("Avertissement: Le DataFrame est vide après le nettoyage des dates et stations dans daily_stats.")
        return pd.DataFrame()

    if 'Is_Daylight' not in df.columns:
        warnings.warn("La colonne 'Is_Daylight' est manquante. Calcul en utilisant une règle fixe (7h-18h).")
        df['Is_Daylight'] = (df['Datetime'].dt.hour >= 7) & (df['Datetime'].dt.hour <= 18)

    numerical_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col]) and col not in ['Station', 'Datetime', 'Is_Daylight']]
    
    if not numerical_cols:
        warnings.warn("Aucune colonne numérique valide trouvée pour le calcul des statistiques journalières.")
        return pd.DataFrame()

    agg_funcs = {col: ['mean', 'min', 'max'] for col in numerical_cols}
    
    if 'Rain_mm' in numerical_cols:
        agg_funcs['Rain_mm'].append('sum')

    daily_stats_df = df.groupby(['Station', df['Datetime'].dt.date]).agg(agg_funcs)

    daily_stats_df.columns = ['_'.join(col).strip() for col in daily_stats_df.columns.values]

    daily_stats_df = daily_stats_df.rename_axis(index={'Datetime': 'Date'})
    daily_stats_df = daily_stats_df.reset_index()

    return daily_stats_df

#N2
import pandas as pd
from pyproj import CRS, Transformer
import pytz
from astral.location import LocationInfo
from astral import sun
import numpy as np
import warnings
import os
import gdown # Importation du module gdown pour le téléchargement
import plotly.graph_objects as go # Importation pour gérer les objets Figure Plotly
import matplotlib.pyplot as plt # NOUVEAUX IMPORTS pour Matplotlib
import seaborn as sns
import traceback # Importation de traceback pour les messages d'erreur détaillés
import math # Ajouté pour math.ceil
from datetime import timedelta # Ajouté pour timedelta

# Importation des configurations et métadonnées depuis config.py
from config import METADATA_VARIABLES, PALETTE_DEFAUT, DATA_LIMITS

# Fonction utilitaire pour créer Rain_mm si nécessaire
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
        #df_copy['Rain_mm'] = np.nan # Crée la colonne même si aucune source n'est disponible
        warnings.warn("Ni 'Rain_01_mm' ni 'Rain_02_mm' ne sont présents pour créer 'Rain_mm'. ")
    return df_copy


def create_datetime(df: pd.DataFrame, bassin: str = None) -> pd.DataFrame:
    """
    Crée la colonne 'Datetime' à partir de colonnes séparées (Year, Month, Day, Hour, Minute)
    ou à partir d'une colonne 'Date' pour le bassin VEA_SISSILI.

    Args:
        df (pd.DataFrame): DataFrame d'entrée.
        bassin (str, optional): Nom du bassin ('DANO', 'DASSARI', 'VEA_SISSILI').
                                Utilisé pour la logique de conversion de la colonne 'Date'.

    Returns:
        pd.DataFrame: DataFrame avec la colonne 'Datetime' et ses composantes, si possible.
    """
    df_copy = df.copy()

    # Prioriser la colonne 'Date' si elle existe pour VEA_SISSILI ou si pas de colonnes Y/M/D/H/M
    if 'Date' in df_copy.columns and (bassin == 'VEA_SISSILI' or not any(col in df_copy.columns for col in ['Year', 'Month', 'Day', 'Hour', 'Minute'])):
        try:
            df_copy['Datetime'] = pd.to_datetime(df_copy['Date'], errors='coerce')
        except Exception as e:
            warnings.warn(f"Impossible de convertir la colonne 'Date' en Datetime pour le bassin {bassin}: {e}")
            df_copy['Datetime'] = pd.NaT
    else:
        date_cols = ['Year', 'Month', 'Day', 'Hour', 'Minute']
        
        for col in date_cols:
            if col in df_copy.columns:
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')

        try:
            existing_date_components = [col for col in ['Year', 'Month', 'Day', 'Hour', 'Minute'] if col in df_copy.columns]
            
            if not existing_date_components:
                raise ValueError("Aucune colonne de composantes de date/heure (Year, Month, Day, Hour, Minute) trouvée.")

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
            warnings.warn(f"Impossible de créer Datetime à partir des colonnes séparées. Erreur: {e}. Colonnes présentes: {df_copy.columns.tolist()}")
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
        warnings.warn("La colonne 'Datetime' est vide ou n'existe pas après la tentative de création. Composantes de date/heure non extraites.")

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

    # Assurez-vous que l'index est bien DatetimeIndex et nettoyez les NaT
    if not isinstance(df_processed.index, pd.DatetimeIndex):
        raise TypeError("Le DataFrame d'entrée pour l'interpolation DOIT avoir un DatetimeIndex.")
    
    initial_rows = len(df_processed)
    df_processed = df_processed[df_processed.index.notna()]
    if len(df_processed) == 0:
        raise ValueError("Après nettoyage des index temporels manquants, le DataFrame est vide. Impossible de procéder à l'interpolation.")
    if initial_rows - len(df_processed) > 0:
        warnings.warn(f"Suppression de {initial_rows - len(df_processed)} lignes avec index Datetime manquant ou invalide dans l'interpolation.")
    
    print(f"DEBUG (interpolation): Type de l'index du DataFrame initial: {type(df_processed.index)}")
    print(f"DEBUG (interpolation): Premières 5 valeurs de l'index après nettoyage des NaT: {df_processed.index[:5].tolist() if not df_processed.empty else 'DataFrame vide'}")

    required_gps_cols = ['Station', 'Lat', 'Long', 'Timezone']
    if not all(col in df_gps.columns for col in required_gps_cols):
        raise ValueError(
            f"df_gps doit contenir les colonnes {required_gps_cols}. "
            f"Colonnes actuelles dans df_gps : {df_gps.columns.tolist()}"
        )

    if not df_gps['Station'].is_unique:
        print("Avertissement: La colonne 'Station' dans df_gps contient des noms de station dupliqués.")
        print("Ceci peut entraîner des comportements inattendus ou des stations non reconnues.")
        df_gps_unique = df_gps.drop_duplicates(subset=['Station'], keep='first').copy()
        print(f"Suppression de {len(df_gps) - len(df_gps_unique)} doublons dans df_gps (en gardant la première occurrence).")
    else:
        df_gps_unique = df_gps.copy()

    gps_info_dict = df_gps_unique.set_index('Station')[['Lat', 'Long', 'Timezone']].to_dict('index')

    numerical_cols = ['Air_Temp_Deg_C', 'Rel_H_%', 'BP_mbar_Avg',
                      'Rain_01_mm', 'Rain_02_mm', 'Rain_mm', 'Wind_Sp_m/sec',
                      'Solar_R_W/m^2', 'Wind_Dir_Deg']
    for col in numerical_cols:
        if col in df_processed.columns:
            df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')

    df_processed_parts = []

    for station_name, group in df_processed.groupby('Station'):
        group_copy = group.copy()
        print(f"DEBUG (interpolation/groupby): Début du traitement du groupe '{station_name}'.")
        
        # Standardize group_copy.index to UTC first
        # This block ensures the index is UTC-aware before proceeding
        if group_copy.index.tz is None:
            group_copy.index = group_copy.index.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
        elif group_copy.index.tz != pytz.utc: # If it's already tz-aware but not UTC, convert to UTC
            group_copy.index = group_copy.index.tz_convert('UTC')
        print(f"DEBUG (interpolation/groupby): Index Datetime pour '{station_name}' STANDARDISÉ à UTC. Dtype: {group_copy.index.dtype}")
        
        # S'assurer que l'index n'a pas de NaT après localisation
        group_copy = group_copy[group_copy.index.notna()]
        if group_copy.empty:
            warnings.warn(f"Le groupe '{station_name}' est vide après nettoyage de l'index Datetime. Il sera ignoré.")
            continue


        # --- Calculs Astral en utilisant le fuseau horaire local ---
        apply_fixed_daylight = True
        gps_data = gps_info_dict.get(station_name)
        if gps_data and pd.notna(gps_data.get('Lat')) and pd.notna(gps_data.get('Long')) and pd.notna(gps_data.get('Timezone')):
            lat = gps_data['Lat']
            long = gps_data['Long']
            timezone_str = gps_data['Timezone']

            try:
                local_tz = pytz.timezone(timezone_str)
                # Create a local timezone-aware version of the index for Astral calculations
                index_for_astral_local = group_copy.index.tz_convert(local_tz)

                daily_sun_info = {}
                # Get unique dates from the local-time index (these are timezone-aware Timestamps)
                # Utilisation de .drop_duplicates() au lieu de .unique() pour garantir un objet Pandas
                unique_dates_ts_local = index_for_astral_local.normalize().drop_duplicates()

                # Ensure unique_dates is not empty before processing
                if unique_dates_ts_local.empty: # Utilisez .empty pour les objets Pandas
                    raise ValueError("No unique dates found for Astral calculation.")
                
                for ts_local_aware in unique_dates_ts_local: # Iterate over timezone-aware Timestamps
                    loc = LocationInfo(station_name, "Site", timezone_str, lat, long)
                    
                    # Convert the timezone-aware Timestamp to a naive datetime.date object
                    # This is what Astral's sun.sun function expects for its 'date' parameter
                    # This conversion helps avoid potential re-localization warnings from Astral/pytz.
                    naive_date_for_astral = ts_local_aware.to_pydatetime().date()
                    
                    # Pass the NAIVE date object to Astral. Astral's observer handles the timezone internally.
                    s = sun.sun(loc.observer, date=naive_date_for_astral) 
                    daily_sun_info[naive_date_for_astral] = {
                        'sunrise': s['sunrise'],
                        'sunset': s['sunset']
                    }

                # Explicitly create a list of naive dates for the DataFrame index
                naive_unique_dates_for_index = [ts.date() for ts in unique_dates_ts_local] # Use .date() here as keys are naive
                temp_df_sun_index = pd.Index(naive_unique_dates_for_index, name='Date_Local_Naive')
                temp_df_sun = pd.DataFrame(index=temp_df_sun_index)
                
                # NOUVEAUX DÉBOGAGES pour comprendre le type juste avant l'opération
                print(f"DEBUG (astral_calc): unique_dates_ts_local type: {type(unique_dates_ts_local)}")
                print(f"DEBUG (astral_calc): naive_unique_dates_for_index type: {type(naive_unique_dates_for_index)}")
                print(f"DEBUG (astral_calc): temp_df_sun_index type: {type(temp_df_sun_index)}")
                if not temp_df_sun.empty:
                    print(f"DEBUG (astral_calc): First element of temp_df_sun.index: {temp_df_sun.index[0]}")
                    print(f"DEBUG (astral_calc): Type of first element of temp_df_sun.index: {type(temp_df_sun.index[0])}")

                # Correction: Utilisation de la compréhension de liste pour éviter le problème de .map()
                temp_df_sun['sunrise_time_local'] = [daily_sun_info.get(date, {}).get('sunrise') for date in temp_df_sun.index]
                temp_df_sun['sunset_time_local'] = [daily_sun_info.get(date, {}).get('sunset') for date in temp_df_sun.index]

                # Merge with group_copy (which has UTC index)
                # To merge, create a normalized local date column (naive) in group_copy
                group_copy_reset = group_copy.reset_index()
                group_copy_reset['Date_Local_Naive'] = group_copy_reset['Datetime'].dt.tz_convert(local_tz).dt.date

                group_copy_reset = pd.merge(group_copy_reset, temp_df_sun, on='Date_Local_Naive', how='left')

                # Convert merged local times back to UTC for comparison with original 'Datetime' (which is UTC)
                group_copy_reset['sunrise_time_utc'] = group_copy_reset['sunrise_time_local'].dt.tz_convert('UTC')
                group_copy_reset['sunset_time_utc'] = group_copy_reset['sunset_time_local'].dt.tz_convert('UTC')

                group_copy_reset.loc[:, 'Is_Daylight'] = (group_copy_reset['Datetime'] >= group_copy_reset['sunrise_time_utc']) & \
                                                          (group_copy_reset['Datetime'] < group_copy_reset['sunset_time_utc'])

                daylight_timedelta_local = group_copy_reset['sunset_time_local'] - group_copy_reset['sunrise_time_local']
                
                # Correction ici pour formater la durée en HH:MM:SS
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

                print(f"Lever et coucher du soleil calculés pour {station_name}.")
                apply_fixed_daylight = False

            except Exception as e:
                print(f"Erreur lors du calcul du lever/coucher du soleil avec Astral pour {station_name}: {e}.")
                traceback.print_exc() # Print full traceback for this specific error
                warnings.warn(f"Calcul Astral impossible pour '{station_name}'. Utilisation de l'indicateur jour/nuit fixe.")
                apply_fixed_daylight = True
        else:
            print(f"Avertissement: Coordonnées ou Fuseau horaire manquants/invalides pour le site '{station_name}' dans df_gps. Utilisation de l'indicateur jour/nuit fixe.")
            apply_fixed_daylight = True

        if apply_fixed_daylight:
            group_copy.loc[:, 'Is_Daylight'] = (group_copy.index.hour >= 7) & (group_copy.index.hour <= 18)
            group_copy.loc[:, 'Daylight_Duration'] = "11:00:00" # Règle fixe pour la durée
            print(f"Utilisation de l'indicateur jour/nuit fixe (7h-18h) pour {station_name}.")

        df_processed_parts.append(group_copy)

    if not df_processed_parts:
        raise ValueError("Aucune partie de DataFrame n'a pu être traitée après le regroupement par station.")

    # Concaténation de toutes les parties. Puisque chaque partie a un DatetimeIndex,
    # nous pouvons concaténer directement sans ignore_index=True
    df_final = pd.concat(df_processed_parts)
    # Assurez-vous que l'index est trié et qu'il est bien nommé 'Datetime'
    df_final = pd.concat(df_processed_parts).sort_index()
    df_final.index.name = 'Datetime' 
    print(f"DEBUG (interpolation/concat): Index du DataFrame final après concaténation et tri: {type(df_final.index)}")
    print(f"DEBUG (interpolation/concat): Colonnes du DataFrame final après concaténation: {df_final.columns.tolist()}")


    # Nettoyage des colonnes temporaires (elles ne sont plus nécessaires ou n'existent plus)
    cols_to_drop_after_process = ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
    df_final = df_final.drop(columns=cols_to_drop_after_process, errors='ignore')

    # Gestion intelligente de Rain_mm
    if 'Rain_mm' not in df_final.columns or df_final['Rain_mm'].isnull().all():
        if 'Rain_01_mm' in df_final.columns and 'Rain_02_mm' in df_final.columns:
            df_final = create_rain_mm(df_final)
            warnings.warn("Colonne Rain_mm créée à partir des deux capteurs.")
        else:
            warnings.warn("Rain_mm manquant et impossible à créer (capteurs pluie incomplets).")
            # if 'Rain_mm' not in df_final.columns:
            #     df_final['Rain_mm'] = np.nan


    # Interpolation standard et bornage pour les variables numériques
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
                    warnings.warn(f"Remplacement de {new_nan_count - initial_nan_count} valeurs hors limites dans '{var}' par NaN.")
            
            print(f"DEBUG (interpolation/variable): Interpolation de '{var}'. Type de l'index de df_final: {type(df_final.index)}")
            
            # Interpolation seulement si l'index est bien DatetimeIndex
            if isinstance(df_final.index, pd.DatetimeIndex):
                df_final[var] = df_final[var].interpolate(method='time', limit_direction='both')
            else:
                print(f"Avertissement (interpolation/variable): L'index n'est pas un DatetimeIndex pour l'interpolation de '{var}'. Utilisation de la méthode 'linear'.")
                df_final[var] = df_final[var].interpolate(method='linear', limit_direction='both')
            df_final[var] = df_final[var].bfill().ffill()

    # Interpolation CONDITIONNELLE de la radiation solaire
    if 'Solar_R_W/m^2' in df_final.columns:
        df_final['Solar_R_W/m^2'] = pd.to_numeric(df_final['Solar_R_W/m^2'], errors='coerce')

        if 'Solar_R_W/m^2' in limits:
            min_val = limits['Solar_R_W/m^2']['min']
            max_val = limits['Solar_R_W/m^2']['max']
            initial_nan_count = df_final['Solar_R_W/m^2'].isna().sum()
            df_final.loc[(df_final['Solar_R_W/m^2'] < min_val) | (df_final['Solar_R_W/m^2'] > max_val), 'Solar_R_W/m^2'] = np.nan
            if df_final['Solar_R_W/m^2'].isna().sum() > initial_nan_count:
                warnings.warn(f"Remplacement de {df_final['Solar_R_W/m^2'].isna().sum() - initial_nan_count} valeurs hors limites dans 'Solar_R_W/m^2' par NaN.")

        if 'Is_Daylight' in df_final.columns:
            df_final.loc[~df_final['Is_Daylight'] & (df_final['Solar_R_W/m^2'] > 0), 'Solar_R_W/m^2'] = 0

            if 'Rain_mm' in df_final.columns:
                cond_suspect_zeros = (df_final['Is_Daylight']) & (df_final['Solar_R_W/m^2'] == 0) & (df_final['Rain_mm'] == 0)
            else:
                cond_suspect_zeros = (df_final['Is_Daylight']) & (df_final['Solar_R_W/m^2'] == 0)
                warnings.warn("Rain_mm manquant. Tous les 0 de radiation solaire pendant le jour sont traités comme suspects.")
            df_final.loc[cond_suspect_zeros, 'Solar_R_W/m^2'] = np.nan

            print(f"DEBUG (interpolation/solaire): Interpolation de 'Solar_R_W/m^2' (conditionnel). Type de l'index de df_final: {type(df_final.index)}")

            is_day = df_final['Is_Daylight']
            if isinstance(df_final.index, pd.DatetimeIndex):
                df_final.loc[is_day, 'Solar_R_W/m^2'] = df_final.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='time', limit_direction='both')
            else:
                print(f"Avertissement (interpolation/solaire): L'index n'est pas un DatetimeIndex pour l'interpolation de 'Solar_R_W/m^2'. Utilisation de la méthode 'linear'.")
                df_final.loc[is_day, 'Solar_R_W/m^2'] = df_final.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='linear', limit_direction='both')

            df_final.loc[is_day, 'Solar_R_W/m^2'] = df_final.loc[is_day, 'Solar_R_W/m^2'].bfill().ffill()

            df_final.loc[~is_day & df_final['Solar_R_W/m^2'].isna(), 'Solar_R_W/m^2'] = 0
            warnings.warn("Radiation solaire interpolée avec succès.")
        else:
            warnings.warn("Colonne 'Is_Daylight' manquante. Radiation solaire interpolée standard.")
            if isinstance(df_final.index, pd.DatetimeIndex):
                 df_final['Solar_R_W/m^2'] = df_final['Solar_R_W/m^2'].interpolate(method='time', limit_direction='both').bfill().ffill()
            else:
                 df_final['Solar_R_W/m^2'] = df_final['Solar_R_W/m^2'].interpolate(method='linear', limit_direction='both').bfill().ffill()

    warnings.warn("Vérification des valeurs manquantes après interpolation:")
    missing_after_interp = df_final.isna().sum()
    columns_with_missing = missing_after_interp[missing_after_interp > 0]
    if not columns_with_missing.empty:
        warnings.warn(f"Valeurs manquantes persistantes:\n{columns_with_missing}")
    else:
        warnings.warn("Aucune valeur manquante après l'interpolation.")

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
        raise ValueError(
            f"Le DataFrame doit contenir les colonnes {required_utm_cols} pour la conversion UTM."
        )

    def convert_row(row):
        try:
            zone = int(row['zone'])
            hemisphere = str(row['hemisphere']).upper()
            is_northern = hemisphere == 'N'

            proj_utm = CRS.from_proj4(
                f"+proj=utm +zone={zone} +datum=WGS84 +units=m +{'north' if is_northern else 'south'}"
            )
            proj_wgs84 = CRS.from_epsg(4326) # WGS84 Lat/Long

            transformer = Transformer.from_crs(proj_utm, proj_wgs84, always_xy=True)
            lon, lat = transformer.transform(row['Easting'], row['Northing'])
            return pd.Series({'Long': lon, 'Lat': lat})
        except Exception as e:
            warnings.warn(f"Erreur lors de la conversion UTM d'une ligne: {e}")
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

    Cette fonction est destinée à être appelée une seule fois au démarrage de l'application.
    Elle vérifie d'abord si les fichiers existent localement pour éviter des téléchargements répétés.

    Returns:
        pd.DataFrame: Un DataFrame consolidé de toutes les stations avec
                      leurs coordonnées GPS (Lat, Long) et fuseaux horaires.
    """
    print("Début de la préparation des données de coordonnées des stations...")
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True) # Assurez-vous que le dossier 'data' existe

    files_info = [
        {'id': '1Iz5L_XkumG390EZvnMgYr3KwDYeesrNz', 'name': "WASCAL Basins Climate Station Coordinates.xlsx", 'bassin': 'Vea Sissili'},
        {'id': '1H8A-sVMtTok6lrD-NFHQxzHBeQ_P7g4z', 'name': "Dano Basins Climate Station Coordinates.xlsx", 'bassin': 'Dano'},
        {'id': '1SOXI0ZvWqpNp6Qwz_BGeWleUtaYMaOBU', 'name': "DASSARI Climate Station Coordinates.xlsx", 'bassin': 'Dassari'}
    ]

    loaded_dfs = []

    for file_info in files_info:
        output_file_path = os.path.join(data_dir, file_info['name'])
        
        if not os.path.exists(output_file_path):
            print(f"Téléchargement de {file_info['bassin']} depuis Google Drive...")
            gdown.download(f'https://drive.google.com/uc?id={file_info["id"]}', output_file_path, quiet=False)
            print(f"Téléchargement de {file_info['bassin']} terminé.")
        else:
            print(f"Chargement de {file_info['bassin']} depuis le cache local: {output_file_path}")
        
        loaded_dfs.append(pd.read_excel(output_file_path))

    vea_sissili_bassin = loaded_dfs[0]
    dano_bassin = loaded_dfs[1]
    dassari_bassin = loaded_dfs[2]

    # Prétraitement des DataFrames (votre code original)
    print("Début du prétraitement des données de stations...")
    
    # Vea Sissili
    vea_sissili_bassin = vea_sissili_bassin.drop(columns=['No', 'Location', 'parameters'], errors='ignore')
    new_row_df_vea = pd.DataFrame([{'Name': 'Atampisi', 'Lat': 10.91501, 'Long': -0.82647}])
    vea_sissili_bassin = pd.concat([vea_sissili_bassin, new_row_df_vea], ignore_index=True)

    # Dassari
    dassari_bassin = dassari_bassin.drop(columns=['Altitude (en m)'], errors='ignore')
    new_rows_df_dassari = pd.DataFrame([{'Site name': 'Pouri', 'Lat': 1207107, 'Long': 293642}, {'Site name': 'Fandohoun', 'Lat': 1207107, 'Long': 293642}])
    dassari_bassin = pd.concat([dassari_bassin, new_rows_df_dassari], ignore_index=True)

    # Dano et Dassari : Renommage et ajout des colonnes UTM
    dano_bassin = dano_bassin.rename(columns={'Long': 'Easting', 'Lat': 'Northing', 'Site Name': 'Name'})
    dassari_bassin = dassari_bassin.rename(columns={'Long': 'Easting', 'Lat': 'Northing', 'Site name': 'Name'})

    dano_bassin['zone'] = 30
    dano_bassin['hemisphere'] = 'N'
    dassari_bassin['zone'] = 31
    dassari_bassin['hemisphere'] = 'N'

    # Application de la fonction de conversion UTM vers GPS
    dano_bassin = convert_utm_df_to_gps(dano_bassin)
    dassari_bassin = convert_utm_df_to_gps(dassari_bassin)

    # Ajout des fuseaux horaires
    dano_bassin['Timezone'] = 'Africa/Ouagadougou'
    dassari_bassin['Timezone'] = 'Africa/Porto-Novo'
    vea_sissili_bassin['Timezone'] = 'Africa/Accra' # Assurez-vous que c'est le bon fuseau horaire pour Vea Sissili

    # Fusion de tous les bassins
    bassins = pd.concat([vea_sissili_bassin, dano_bassin, dassari_bassin], ignore_index=True)

    # Renommer 'Name' en 'Station'
    bassins = bassins.rename(columns={'Name': 'Station'})

    # Nettoyer les lignes avec des valeurs NaN dans Lat/Long/Timezone qui pourraient résulter d'erreurs de conversion
    initial_rows = len(bassins)
    bassins = bassins.dropna(subset=['Lat', 'Long', 'Timezone', 'Station'])
    if len(bassins) < initial_rows:
        print(f"Attention: {initial_rows - len(bassins)} lignes avec des coordonnées ou fuseaux horaires manquants ont été supprimées du DataFrame des stations.")
    
    # 5. Sauvegarde du DataFrame final en JSON
    output_json_path = os.path.join(data_dir, "station_coordinates.json")
    # Utiliser to_json avec orient='records' pour un format plus lisible et facile à charger
    bassins.to_json(output_json_path, orient='records', indent=4)
    print(f"\nPréparation des données terminée. Coordonnées des stations sauvegardées dans '{output_json_path}'.")
    print("Vous pouvez maintenant lancer votre application Flask.")

    return bassins # Retourner le DataFrame des données GPS


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
            warnings.warn(f"Suppression de {initial_rows - len(df_cleaned)} doublons basés sur 'Station' et 'Datetime'.")
        return df_cleaned
    else:
        warnings.warn("Colonnes 'Station' ou 'Datetime' manquantes pour la gestion des doublons. Le DataFrame n'a pas été modifié.")
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
        warnings.warn("L'index n'est pas un DatetimeIndex dans traiter_outliers_meteo. Tentative de conversion.")
        try:
            df_processed.index = pd.to_datetime(df_processed.index, errors='coerce')
            df_processed = df_processed[df_processed.index.notna()]
            if df_processed.empty:
                raise ValueError("DataFrame vide après nettoyage des dates invalides dans traiter_outliers_meteo.")
        except Exception as e:
            raise TypeError(f"Impossible de garantir un DatetimeIndex pour traiter_outliers_meteo: {e}")

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
                    warnings.warn(f"Remplacement de {new_nan_count - initial_nan_count} valeurs hors limites dans '{var}' par NaN.")
    return df_processed

def generer_graphique_par_variable_et_periode(df: pd.DataFrame, station: str, variable: str, periode: str, colors: dict, metadata: dict) -> go.Figure:
    """
    Génère un graphique Plotly de l'évolution d'une variable pour une station sur une période donnée.
    Retourne l'objet Figure Plotly.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("Le DataFrame doit avoir un DatetimeIndex pour générer le graphique par variable et période.")

    filtered_df = df[df['Station'] == station].copy()
    if filtered_df.empty:
        return go.Figure() # Retourne une figure vide si pas de données

    if periode == 'Journalière':
        resampled_df = filtered_df[variable].resample('D').mean()
    elif periode == 'Hebdomadaire':
        resampled_df = filtered_df[variable].resample('W').mean()
    elif periode == 'Mensuelle':
        resampled_df = filtered_df[variable].resample('M').mean()
    elif periode == 'Annuelle':
        resampled_df = filtered_df[variable].resample('Y').mean()
    else: # Données Brutes
        resampled_df = filtered_df[variable]

    resampled_df = resampled_df.dropna()

    if resampled_df.empty:
        return go.Figure() # Retourne une figure vide si pas de données après resample/dropna

    variable_meta = metadata.get(variable, {'Nom': variable, 'Unite': ''})
    color = colors.get(station, '#1f77b4')

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=resampled_df.index, y=resampled_df.values,
                             mode='lines', name=f'{variable_meta["Nom"]} - {station}',
                             line=dict(color=color)))

    fig.update_layout(
        title=f"Évolution de {variable_meta['Nom']} ({variable_meta['Unite']}) pour {station} ({periode})",
        xaxis_title="Date",
        yaxis_title=f"{variable_meta['Nom']} ({variable_meta['Unite']})",
        hovermode="x unified",
        template="plotly_white" # Utiliser un template Plotly plus clair
    )
    return fig

def generer_graphique_comparatif(df: pd.DataFrame, variable: str, periode: str, colors: dict, metadata: dict) -> go.Figure:
    """
    Génère un graphique Plotly comparatif de l'évolution d'une variable entre toutes les stations.
    Retourne l'objet Figure Plotly.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("Le DataFrame doit avoir un DatetimeIndex pour générer le graphique comparatif.")

    fig = go.Figure()
    
    all_stations = df['Station'].unique()
    if len(all_stations) < 2:
        warnings.warn("Moins de 2 stations disponibles pour la comparaison. Le graphique comparatif ne sera pas généré.")
        return go.Figure() # Retourne une figure vide si moins de 2 stations

    for station in all_stations:
        filtered_df = df[df['Station'] == station].copy()
        if filtered_df.empty:
            continue

        if periode == 'Journalière':
            resampled_df = filtered_df[variable].resample('D').mean()
        elif periode == 'Hebdomadaire':
            resampled_df = filtered_df[variable].resample('W').mean()
        elif periode == 'Mensuelle':
            resampled_df = filtered_df[variable].resample('M').mean()
        elif periode == 'Annuelle':
            resampled_df = filtered_df[variable].resample('Y').mean()
        else: # Données Brutes
            resampled_df = filtered_df[variable]

        resampled_df = resampled_df.dropna()
        if resampled_df.empty:
            continue
        
        color = colors.get(station, '#1f77b4')
        fig.add_trace(go.Scatter(x=resampled_df.index, y=resampled_df.values,
                                 mode='lines', name=station,
                                 line=dict(color=color)))

    if not fig.data:
        return go.Figure() # Retourne une figure vide si aucune trace n'a été ajoutée

    variable_meta = metadata.get(variable, {'Nom': variable, 'Unite': ''})
    fig.update_layout(
        title=f"Comparaison de {variable_meta['Nom']} ({variable_meta['Unite']}) entre stations ({periode})",
        xaxis_title="Date",
        yaxis_title=f"{variable_meta['Nom']} ({variable_meta['Unite']})",
        hovermode="x unified",
        legend_title="Variables",
        template="plotly_white"
    )
    return fig


def generate_multi_variable_station_plot(df: pd.DataFrame, station: str, colors: dict, metadata: dict) -> go.Figure:
    """
    Génère un graphique Plotly de l'évolution normalisée de plusieurs variables pour une station donnée.
    Retourne l'objet Figure Plotly.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("Le DataFrame doit avoir un DatetimeIndex pour générer le graphique multi-variables.")

    filtered_df = df[df['Station'] == station].copy()
    if filtered_df.empty:
        return go.Figure() # Retourne une figure vide si pas de données

    numerical_vars = [col for col in filtered_df.columns if pd.api.types.is_numeric_dtype(filtered_df[col]) and col not in ['Station', 'Is_Daylight', 'Daylight_Duration']]

    if not numerical_vars:
        warnings.warn("Aucune variable numérique trouvée pour la station sélectionnée.")
        return go.Figure() # Retourne une figure vide si pas de variables numériques

    normalized_df = filtered_df[numerical_vars].copy()
    for col in normalized_df.columns:
        min_val = normalized_df[col].min()
        max_val = normalized_df[col].max()
        if max_val != min_val:
            normalized_df[col] = (normalized_df[col] - min_val) / (max_val - min_val)
        else:
            normalized_df[col] = 0.5 if pd.notna(min_val) else np.nan

    normalized_df = normalized_df.dropna(how='all')

    if normalized_df.empty:
        return go.Figure() # Retourne une figure vide si pas de données après normalisation/dropna
    
    fig = go.Figure()
    for var in normalized_df.columns:
        var_meta = metadata.get(var, {'Nom': var, 'Unite': ''})
        color = colors.get(var, None)

        fig.add_trace(go.Scatter(x=normalized_df.index, y=normalized_df[var],
                                 mode='lines', name=var_meta['Nom'],
                                 line=dict(color=color)))

    fig.update_layout(
        title=f"Évolution Normalisée des Variables Météorologiques pour la station {station}",
        xaxis_title="Date",
        yaxis_title="Valeur Normalisée (0-1)",
        hovermode="x unified",
        legend_title="Variables",
        template="plotly_white"
    )
    return fig

def calculate_daily_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les statistiques journalières (moyenne, min, max, somme) pour les variables numériques
    groupées par station. Cette fonction renvoie un DataFrame de statistiques, non un graphique.
    C'est la fonction qui remplace l'ancienne 'daily_stats'.

    Args:
        df (pd.DataFrame): DataFrame d'entrée avec DatetimeIndex et colonne 'Station'.

    Returns:
        pd.DataFrame: DataFrame avec les statistiques journalières.
    """
    df_copy = df.copy()

    # Assurez-vous que 'Datetime' est une colonne et de type datetime
    if isinstance(df_copy.index, pd.DatetimeIndex):
        df_copy = df_copy.reset_index()

    df_copy['Datetime'] = pd.to_datetime(df_copy['Datetime'], errors='coerce')
    df_copy = df_copy.dropna(subset=['Datetime', 'Station'])

    if df_copy.empty:
        print("Avertissement: Le DataFrame est vide après le nettoyage des dates et stations dans calculate_daily_summary_table.")
        return pd.DataFrame()

    if 'Is_Daylight' not in df_copy.columns:
        warnings.warn("La colonne 'Is_Daylight' est manquante. Calcul en utilisant une règle fixe (7h-18h).")
        df_copy['Is_Daylight'] = (df_copy['Datetime'].dt.hour >= 7) & (df_copy['Datetime'].dt.hour <= 18)

    numerical_cols = [col for col in df_copy.columns if pd.api.types.is_numeric_dtype(df_copy[col]) and col not in ['Station', 'Datetime', 'Is_Daylight', 'Daylight_Duration']]
    
    if not numerical_cols:
        warnings.warn("Aucune colonne numérique valide trouvée pour le calcul des statistiques journalières.")
        return pd.DataFrame()

    # Calcul des statistiques de base par jour
    daily_aggregated_df = df_copy.groupby(['Station', df_copy['Datetime'].dt.date]).agg({
        col: ['mean', 'min', 'max'] for col in numerical_cols if METADATA_VARIABLES.get(col, {}).get('is_rain') == False
    })

    # Renommage des colonnes agrégées pour les non-pluies
    daily_aggregated_df.columns = ['_'.join(col).strip() for col in daily_aggregated_df.columns.values]


    # Traitement spécifique pour la pluie (Rain_mm)
    if 'Rain_mm' in df_copy.columns and METADATA_VARIABLES.get('Rain_mm', {}).get('is_rain'):
        df_daily_rain = df_copy.groupby(['Station', df_copy['Datetime'].dt.date])['Rain_mm'].sum().reset_index()
        df_daily_rain = df_daily_rain.rename(columns={'Rain_mm': 'Rain_mm_sum'})

        # Fusionner les statistiques de pluie avec les autres
        if not daily_aggregated_df.empty:
            daily_aggregated_df = daily_aggregated_df.reset_index()
            daily_stats_df = pd.merge(daily_aggregated_df, df_daily_rain, on=['Station', 'Datetime'], how='left')
            daily_stats_df = daily_stats_df.rename(columns={'Datetime': 'Date'})
        else:
            daily_stats_df = df_daily_rain.rename(columns={'Datetime': 'Date'})
    else:
        daily_stats_df = daily_aggregated_df.reset_index().rename(columns={'Datetime': 'Date'})


    # Calcul des statistiques de saison et de sécheresse pour la pluie
    if 'Rain_mm' in df_copy.columns and METADATA_VARIABLES.get('Rain_mm', {}).get('is_rain'):
        df_daily_rain_raw = df_copy.groupby(['Station', pd.Grouper(key='Datetime', freq='D')])['Rain_mm'].sum().reset_index()
        
        RAIN_SEASON_GAP_THRESHOLD = pd.Timedelta(days=60)
        season_stats = []
        for station_name, station_df_rain in df_daily_rain_raw.groupby('Station'):
            station_df_rain = station_df_rain.set_index('Datetime').sort_index()
            rain_events = station_df_rain[station_df_rain['Rain_mm'] > 0].index

            if rain_events.empty:
                season_stats.append({'Station': station_name, 'Moyenne_Saison_Pluvieuse': np.nan, 'Debut_Saison_Pluvieuse': pd.NaT, 'Fin_Saison_Pluvieuse': pd.NaT, 'Duree_Saison_Pluvieuse_Jours': np.nan})
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
                season_stats.append({'Station': station_name, 'Moyenne_Saison_Pluvieuse': np.nan, 'Debut_Saison_Pluvieuse': pd.NaT, 'Fin_Saison_Pluvieuse': pd.NaT, 'Duree_Saison_Pluvieuse_Jours': np.nan})
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
        
        # Merge season stats into main daily_stats_df
        if not df_season_stats.empty:
            daily_stats_df = pd.merge(daily_stats_df, df_season_stats, on='Station', how='left')


    # Calcul des statistiques globales (Max, Min, Moyenne, Médiane, etc.) par station
    # pour les variables numériques, à inclure dans le tableau récapitulatif.
    final_stats_per_station = pd.DataFrame()
    for station_name in df_copy['Station'].unique():
        station_df = df_copy[df_copy['Station'] == station_name].copy()
        station_summary = {'Station': station_name}

        for var in numerical_cols:
            if var in station_df.columns and pd.api.types.is_numeric_dtype(station_df[var]):
                # Filtrage pour la radiation solaire (seulement pendant le jour)
                if var == 'Solar_R_W/m^2':
                    var_data = station_df.loc[station_df['Is_Daylight'], var].dropna()
                else:
                    var_data = station_df[var].dropna()
                
                if not var_data.empty:
                    station_summary[f'{var}_Maximum'] = var_data.max()
                    station_summary[f'{var}_Minimum'] = var_data.min()
                    station_summary[f'{var}_Moyenne'] = var_data.mean()
                    station_summary[f'{var}_Mediane'] = var_data.median()
                    
                    # Pour Rain_mm uniquement
                    if var == 'Rain_mm':
                        station_summary[f'{var}_Cumul_Annuel'] = station_df['Rain_mm'].sum()
                        # Moyenne des jours pluvieux (seulement les jours où il a plu)
                        rainy_days_data = station_df[station_df['Rain_mm'] > 0]['Rain_mm'].dropna()
                        station_summary[f'{var}_Moyenne_Jours_Pluvieux'] = rainy_days_data.mean() if not rainy_days_data.empty else np.nan

                        # Ajouter la durée de la saison pluvieuse et de sécheresse du df_season_stats
                        if 'Duree_Saison_Pluvieuse_Jours' in daily_stats_df.columns:
                            s_data = daily_stats_df[daily_stats_df['Station'] == station_name]
                            if not s_data.empty:
                                station_summary[f'{var}_Duree_Saison_Pluvieuse_Jours'] = s_data['Duree_Saison_Pluvieuse_Jours'].iloc[0]
                                # Placeholder pour sécheresse si non calculé ailleurs
                                station_summary[f'{var}_Duree_Secheresse_Definie_Jours'] = np.nan # Vous devrez calculer ceci plus tard

        final_stats_per_station = pd.concat([final_stats_per_station, pd.DataFrame([station_summary])], ignore_index=True)
        
    return final_stats_per_station # Retourne le DataFrame de statistiques agrégées par station


def generate_variable_summary_plots_for_web(df: pd.DataFrame, station: str, variable: str, metadata: dict, palette: dict) -> plt.Figure:
    """
    Génère un graphique Matplotlib/Seaborn pour les statistiques agrégées d'une variable spécifique
    pour une station donnée, en utilisant la logique fournie par l'utilisateur pour 'daily_stats'.

    Args:
        df (pd.DataFrame): Le DataFrame global de données traitées avec DatetimeIndex et colonne 'Station'.
        station (str): Le nom de la station à visualiser.
        variable (str): La variable à visualiser (e.g., 'Air_Temp_Deg_C', 'Rain_mm').
        metadata (dict): Dictionnaire de métadonnées pour les variables (Nom, Unite, is_rain).
        palette (dict): Dictionnaire de couleurs pour les différentes métriques statistiques.

    Returns:
        plt.Figure: Un objet Figure Matplotlib contenant tous les sous-graphiques pour la variable sélectionnée.
    """
    df_station = df[df['Station'] == station].copy()

    if df_station.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, f"Aucune donnée pour la station '{station}'.", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
        ax.axis('off')
        return fig

    # S'assurer que Datetime est un index datetime correct pour les données de la station
    if isinstance(df_station.index, pd.DatetimeIndex):
        df_station = df_station.reset_index() # Réinitialise l'index pour un accès facile aux colonnes
    
    df_station['Datetime'] = pd.to_datetime(df_station['Datetime'], errors='coerce')
    df_station = df_station.dropna(subset=['Datetime', 'Station'])
    df_station = df_station.set_index('Datetime').sort_index() # Remet l'index pour les opérations de séries temporelles

    if df_station.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, f"DataFrame vide après nettoyage des dates pour la station '{station}'.", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
        ax.axis('off')
        return fig

    # Assurez-vous que 'Is_Daylight' existe (généralement géré par interpolation, mais au cas où)
    if 'Is_Daylight' not in df_station.columns:
        df_station['Is_Daylight'] = (df_station.index.hour >= 7) & (df_station.index.hour <= 18)


    var_meta = metadata.get(variable, {'Nom': variable, 'Unite': ''})

    # Dictionnaire pour stocker les statistiques pour la variable sélectionnée
    stats_for_plot = {}
    metrics_to_plot = []
    
    # --- LOGIQUE SPÉCIFIQUE À RAIN_MM (transférée de la fonction daily_stats de l'utilisateur) ---
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

        # Logique de détection de la sécheresse
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
                        if current_ratio < s_moyenne_saison: # Condition pour la sécheresse définie
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

        # Statistiques pour Rain_mm (tirées de la vue consolidée de daily_stats de l'utilisateur)
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

        # Récupérer les dates pour max/min pour Rain_mm
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
        
    # --- LOGIQUE POUR LES AUTRES VARIABLES (transférée de la fonction daily_stats de l'utilisateur) ---
    else:
        current_var_data = df_station[variable].dropna()
        if variable == 'Solar_R_W/m^2':
            current_var_data = df_station.loc[df_station['Is_Daylight'], variable].dropna()

        if current_var_data.empty:
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, f"Aucune donnée valide pour la variable {var_meta['Nom']} à {station}.", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
            ax.axis('off')
            return fig

        # Statistiques pour les autres variables (de daily_stats de l'utilisateur)
        stats_for_plot['Maximum'] = current_var_data.max()
        stats_for_plot['Minimum'] = current_var_data.min()
        stats_for_plot['Mediane'] = current_var_data.median()
        stats_for_plot['Moyenne'] = current_var_data.mean()

        # Récupérer les dates pour max/min
        max_idx = current_var_data.idxmax() if not current_var_data.empty else pd.NaT
        min_idx = current_var_data.idxmin() if not current_var_data.empty else pd.NaT

        stats_for_plot['Date_Maximum'] = max_idx if pd.notna(max_idx) else pd.NaT
        stats_for_plot['Date_Minimum'] = min_idx if pd.notna(min_idx) else pd.NaT

        metrics_to_plot = ['Maximum', 'Minimum', 'Moyenne', 'Mediane']
        nrows, ncols = 2, 2
        figsize = (18, 12)

    # Gérer le cas où stats_for_plot pourrait être vide (ex: toutes les données sont NaN)
    if not stats_for_plot:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, f"Impossible de calculer des statistiques pour la variable '{variable}' à la station '{station}' (données manquantes ou non numériques).", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
        ax.axis('off')
        return fig

    # --- PARTIE COMMUNE DE TRACÉ (adaptée de la fonction daily_stats de l'utilisateur) ---
    fig, axes = plt.subplots(nrows, ncols, figsize=figsize)
    plt.subplots_adjust(hspace=0.6, wspace=0.4) # Ajustement de l'espacement pour un meilleur ajustement
    axes = axes.flatten()

    fig.suptitle(f'Statistiques de {var_meta["Nom"]} pour la station {station}', fontsize=16, y=0.98)

    for i, metric in enumerate(metrics_to_plot):
        ax = axes[i]
        value = stats_for_plot.get(metric)
        if pd.isna(value):
            ax.text(0.5, 0.5, "Données non disponibles", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=12, color='gray')
            ax.axis('off')
            continue

        color = palette.get(metric.replace(' ', '_'), '#cccccc') # Utilisation de la `palette` passée en argument
        
        # Créer un DataFrame factice pour sns.barplot, car il attend un DataFrame
        plot_data_bar = pd.DataFrame({'Metric': [metric.replace('_', ' ')], 'Value': [value]})
        sns.barplot(x='Metric', y='Value', data=plot_data_bar, ax=ax, color=color, edgecolor='none')

        # Ajouter les annotations
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
            # Vérifier les clés de date pour max/min si elles existent dans stats_for_plot
            if (metric == 'Maximum' and 'Date_Maximum' in stats_for_plot and pd.notna(stats_for_plot['Date_Maximum'])):
                date_str = f"\n({stats_for_plot['Date_Maximum'].strftime('%d/%m/%Y')})"
            elif (metric == 'Minimum' and 'Date_Minimum' in stats_for_plot and pd.notna(stats_for_plot['Date_Minimum'])):
                date_str = f"\n({stats_for_plot['Date_Minimum'].strftime('%d/%m/%Y')})"
            
            text = f"{value:.1f} {unit}{date_str}"
        else:
            text = f"{value:.1f} {var_meta['Unite']}"

        # Positionne le texte sur le graphique.
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                text, ha='center', va='bottom', fontsize=9, color='black',
                bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=1))
        
        ax.set_title(f"{var_meta['Nom']} {metric.replace('_', ' ')}", fontsize=11)
        ax.set_xlabel("")
        ax.set_ylabel(f"Valeur ({var_meta['Unite']})", fontsize=10)
        ax.tick_params(axis='x', rotation=0)
        ax.set_xticklabels([])

    # Si moins de graphiques que de sous-graphiques, désactiver les axes inutilisés
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.tight_layout(rect=[0, 0, 1, 0.96])

    return fig


def daily_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les statistiques journalières (moyenne, min, max, somme) pour les variables numériques
    groupées par station.

    Args:
        df (pd.DataFrame): DataFrame d'entrée avec DatetimeIndex et colonne 'Station'.

    Returns:
        pd.DataFrame: DataFrame avec les statistiques journalières.
    """
    df = df.copy()

    # Assurez-vous que 'Datetime' est une colonne et de type datetime
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index()

    df['Datetime'] = pd.to_datetime(df['Datetime'], errors='coerce')
    df = df.dropna(subset=['Datetime', 'Station'])

    if df.empty:
        print("Avertissement: Le DataFrame est vide après le nettoyage des dates et stations dans daily_stats.")
        return pd.DataFrame()

    if 'Is_Daylight' not in df.columns:
        warnings.warn("La colonne 'Is_Daylight' est manquante. Calcul en utilisant une règle fixe (7h-18h).")
        df['Is_Daylight'] = (df['Datetime'].dt.hour >= 7) & (df['Datetime'].dt.hour <= 18)

    numerical_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col]) and col not in ['Station', 'Datetime', 'Is_Daylight']]
    
    if not numerical_cols:
        warnings.warn("Aucune colonne numérique valide trouvée pour le calcul des statistiques journalières.")
        return pd.DataFrame()

    agg_funcs = {col: ['mean', 'min', 'max'] for col in numerical_cols}
    
    if 'Rain_mm' in numerical_cols:
        agg_funcs['Rain_mm'].append('sum')

    daily_stats_df = df.groupby(['Station', df['Datetime'].dt.date]).agg(agg_funcs)

    daily_stats_df.columns = ['_'.join(col).strip() for col in daily_stats_df.columns.values]

    daily_stats_df = daily_stats_df.rename_axis(index={'Datetime': 'Date'})
    daily_stats_df = daily_stats_df.reset_index()

    return daily_stats_df

#N3
import pandas as pd
from pyproj import CRS, Transformer
import pytz
from astral.location import LocationInfo
from astral import sun
import numpy as np
import warnings
import os
import gdown # Importation du module gdown pour le téléchargement
import plotly.graph_objects as go # Importation pour gérer les objets Figure Plotly
import matplotlib.pyplot as plt # NOUVEAUX IMPORTS pour Matplotlib
import seaborn as sns
import traceback # Importation de traceback pour les messages d'erreur détaillés
import math # Ajouté pour math.ceil
from datetime import timedelta # Ajouté pour timedelta

# Importation des configurations et métadonnées depuis config.py
from config import METADATA_VARIABLES, PALETTE_DEFAUT, DATA_LIMITS

# Fonction utilitaire pour créer Rain_mm si nécessaire
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
        df_copy['Rain_mm'] = np.nan # Crée la colonne même si aucune source n'est disponible
        warnings.warn("Ni 'Rain_01_mm' ni 'Rain_02_mm' ne sont présents pour créer 'Rain_mm'. 'Rain_mm' est rempli de NaN.")
    return df_copy


def create_datetime(df: pd.DataFrame, bassin: str = None) -> pd.DataFrame:
    """
    Crée la colonne 'Datetime' à partir de colonnes séparées (Year, Month, Day, Hour, Minute)
    ou à partir d'une colonne 'Date' pour le bassin VEA_SISSILI.

    Args:
        df (pd.DataFrame): DataFrame d'entrée.
        bassin (str, optional): Nom du bassin ('DANO', 'DASSARI', 'VEA_SISSILI').
                                Utilisé pour la logique de conversion de la colonne 'Date'.

    Returns:
        pd.DataFrame: DataFrame avec la colonne 'Datetime' et ses composantes, si possible.
    """
    df_copy = df.copy()

    # Prioriser la colonne 'Date' si elle existe pour VEA_SISSILI ou si pas de colonnes Y/M/D/H/M
    if 'Date' in df_copy.columns and (bassin == 'VEA_SISSILI' or not any(col in df_copy.columns for col in ['Year', 'Month', 'Day', 'Hour', 'Minute'])):
        try:
            df_copy['Datetime'] = pd.to_datetime(df_copy['Date'], errors='coerce')
        except Exception as e:
            warnings.warn(f"Impossible de convertir la colonne 'Date' en Datetime pour le bassin {bassin}: {e}")
            df_copy['Datetime'] = pd.NaT
    else:
        date_cols = ['Year', 'Month', 'Day', 'Hour', 'Minute']
        
        for col in date_cols:
            if col in df_copy.columns:
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')

        try:
            existing_date_components = [col for col in ['Year', 'Month', 'Day', 'Hour', 'Minute'] if col in df_copy.columns]
            
            if not existing_date_components:
                raise ValueError("Aucune colonne de composantes de date/heure (Year, Month, Day, Hour, Minute) trouvée.")

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
            warnings.warn(f"Impossible de créer Datetime à partir des colonnes séparées. Erreur: {e}. Colonnes présentes: {df_copy.columns.tolist()}")
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
        warnings.warn("La colonne 'Datetime' est vide ou n'existe pas après la tentative de création. Composantes de date/heure non extraites.")

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

    # Assurez-vous que l'index est bien DatetimeIndex et nettoyez les NaT
    if not isinstance(df_processed.index, pd.DatetimeIndex):
        raise TypeError("Le DataFrame d'entrée pour l'interpolation DOIT avoir un DatetimeIndex.")
    
    initial_rows = len(df_processed)
    df_processed = df_processed[df_processed.index.notna()]
    if len(df_processed) == 0:
        raise ValueError("Après nettoyage des index temporels manquants, le DataFrame est vide. Impossible de procéder à l'interpolation.")
    if initial_rows - len(df_processed) > 0:
        warnings.warn(f"Suppression de {initial_rows - len(df_processed)} lignes avec index Datetime manquant ou invalide dans l'interpolation.")
    
    print(f"DEBUG (interpolation): Type de l'index du DataFrame initial: {type(df_processed.index)}")
    print(f"DEBUG (interpolation): Premières 5 valeurs de l'index après nettoyage des NaT: {df_processed.index[:5].tolist() if not df_processed.empty else 'DataFrame vide'}")

    required_gps_cols = ['Station', 'Lat', 'Long', 'Timezone']
    if not all(col in df_gps.columns for col in required_gps_cols):
        raise ValueError(
            f"df_gps doit contenir les colonnes {required_gps_cols}. "
            f"Colonnes actuelles dans df_gps : {df_gps.columns.tolist()}"
        )

    if not df_gps['Station'].is_unique:
        print("Avertissement: La colonne 'Station' dans df_gps contient des noms de station dupliqués.")
        print("Ceci peut entraîner des comportements inattendus ou des stations non reconnues.")
        df_gps_unique = df_gps.drop_duplicates(subset=['Station'], keep='first').copy()
        print(f"Suppression de {len(df_gps) - len(df_gps_unique)} doublons dans df_gps (en gardant la première occurrence).")
    else:
        df_gps_unique = df_gps.copy()

    gps_info_dict = df_gps_unique.set_index('Station')[['Lat', 'Long', 'Timezone']].to_dict('index')

    numerical_cols = ['Air_Temp_Deg_C', 'Rel_H_%', 'BP_mbar_Avg',
                      'Rain_01_mm', 'Rain_02_mm', 'Rain_mm', 'Wind_Sp_m/sec',
                      'Solar_R_W/m^2', 'Wind_Dir_Deg']
    for col in numerical_cols:
        if col in df_processed.columns:
            df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')

    df_processed_parts = []

    for station_name, group in df_processed.groupby('Station'):
        group_copy = group.copy()
        print(f"DEBUG (interpolation/groupby): Début du traitement du groupe '{station_name}'.")
        
        # Standardize group_copy.index to UTC first
        # This block ensures the index is UTC-aware before proceeding
        if group_copy.index.tz is None:
            group_copy.index = group_copy.index.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
        elif group_copy.index.tz != pytz.utc: # If it's already tz-aware but not UTC, convert to UTC
            group_copy.index = group_copy.index.tz_convert('UTC')
        print(f"DEBUG (interpolation/groupby): Index Datetime pour '{station_name}' STANDARDISÉ à UTC. Dtype: {group_copy.index.dtype}")
        
        # S'assurer que l'index n'a pas de NaT après localisation
        group_copy = group_copy[group_copy.index.notna()]
        if group_copy.empty:
            warnings.warn(f"Le groupe '{station_name}' est vide après nettoyage de l'index Datetime. Il sera ignoré.")
            continue


        # --- Calculs Astral en utilisant le fuseau horaire local ---
        apply_fixed_daylight = True
        gps_data = gps_info_dict.get(station_name)
        if gps_data and pd.notna(gps_data.get('Lat')) and pd.notna(gps_data.get('Long')) and pd.notna(gps_data.get('Timezone')):
            lat = gps_data['Lat']
            long = gps_data['Long']
            timezone_str = gps_data['Timezone']

            try:
                local_tz = pytz.timezone(timezone_str)
                # Create a local timezone-aware version of the index for Astral calculations
                index_for_astral_local = group_copy.index.tz_convert(local_tz)

                daily_sun_info = {}
                # Get unique dates from the local-time index (these are timezone-aware Timestamps)
                # Utilisation de .drop_duplicates() au lieu de .unique() pour garantir un objet Pandas
                unique_dates_ts_local = index_for_astral_local.normalize().drop_duplicates()

                # Ensure unique_dates is not empty before processing
                if unique_dates_ts_local.empty: # Utilisez .empty pour les objets Pandas
                    raise ValueError("No unique dates found for Astral calculation.")
                
                for ts_local_aware in unique_dates_ts_local: # Iterate over timezone-aware Timestamps
                    loc = LocationInfo(station_name, "Site", timezone_str, lat, long)
                    
                    # Convert the timezone-aware Timestamp to a naive datetime.date object
                    # This is what Astral's sun.sun function expects for its 'date' parameter
                    # This conversion helps avoid potential re-localization warnings from Astral/pytz.
                    naive_date_for_astral = ts_local_aware.to_pydatetime().date()
                    
                    # Pass the NAIVE date object to Astral. Astral's observer handles the timezone internally.
                    s = sun.sun(loc.observer, date=naive_date_for_astral) 
                    daily_sun_info[naive_date_for_astral] = {
                        'sunrise': s['sunrise'],
                        'sunset': s['sunset']
                    }

                # Explicitly create a list of naive dates for the DataFrame index
                naive_unique_dates_for_index = [ts.date() for ts in unique_dates_ts_local] # Use .date() here as keys are naive
                temp_df_sun_index = pd.Index(naive_unique_dates_for_index, name='Date_Local_Naive')
                temp_df_sun = pd.DataFrame(index=temp_df_sun_index)
                
                # NOUVEAUX DÉBOGAGES pour comprendre le type juste avant l'opération
                print(f"DEBUG (astral_calc): unique_dates_ts_local type: {type(unique_dates_ts_local)}")
                print(f"DEBUG (astral_calc): naive_unique_dates_for_index type: {type(naive_unique_dates_for_index)}")
                print(f"DEBUG (astral_calc): temp_df_sun_index type: {type(temp_df_sun_index)}")
                if not temp_df_sun.empty:
                    print(f"DEBUG (astral_calc): First element of temp_df_sun.index: {temp_df_sun.index[0]}")
                    print(f"DEBUG (astral_calc): Type of first element of temp_df_sun.index: {type(temp_df_sun.index[0])}")

                # Correction: Utilisation de la compréhension de liste pour éviter le problème de .map()
                temp_df_sun['sunrise_time_local'] = [daily_sun_info.get(date, {}).get('sunrise') for date in temp_df_sun.index]
                temp_df_sun['sunset_time_local'] = [daily_sun_info.get(date, {}).get('sunset') for date in temp_df_sun.index]

                # Merge with group_copy (which has UTC index)
                # To merge, create a normalized local date column (naive) in group_copy
                group_copy_reset = group_copy.reset_index()
                group_copy_reset['Date_Local_Naive'] = group_copy_reset['Datetime'].dt.tz_convert(local_tz).dt.date

                group_copy_reset = pd.merge(group_copy_reset, temp_df_sun, on='Date_Local_Naive', how='left')

                # Convert merged local times back to UTC for comparison with original 'Datetime' (which is UTC)
                group_copy_reset['sunrise_time_utc'] = group_copy_reset['sunrise_time_local'].dt.tz_convert('UTC')
                group_copy_reset['sunset_time_utc'] = group_copy_reset['sunset_time_local'].dt.tz_convert('UTC')

                group_copy_reset.loc[:, 'Is_Daylight'] = (group_copy_reset['Datetime'] >= group_copy_reset['sunrise_time_utc']) & \
                                                          (group_copy_reset['Datetime'] < group_copy_reset['sunset_time_utc'])

                daylight_timedelta_local = group_copy_reset['sunset_time_local'] - group_copy_reset['sunrise_time_local']
                
                # Correction ici pour formater la durée en HH:MM:SS
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

                print(f"Lever et coucher du soleil calculés pour {station_name}.")
                apply_fixed_daylight = False

            except Exception as e:
                print(f"Erreur lors du calcul du lever/coucher du soleil avec Astral pour {station_name}: {e}.")
                traceback.print_exc() # Print full traceback for this specific error
                warnings.warn(f"Calcul Astral impossible pour '{station_name}'. Utilisation de l'indicateur jour/nuit fixe.")
                apply_fixed_daylight = True
        else:
            print(f"Avertissement: Coordonnées ou Fuseau horaire manquants/invalides pour le site '{station_name}' dans df_gps. Utilisation de l'indicateur jour/nuit fixe.")
            apply_fixed_daylight = True

        if apply_fixed_daylight:
            group_copy.loc[:, 'Is_Daylight'] = (group_copy.index.hour >= 7) & (group_copy.index.hour <= 18)
            group_copy.loc[:, 'Daylight_Duration'] = "11:00:00" # Règle fixe pour la durée
            print(f"Utilisation de l'indicateur jour/nuit fixe (7h-18h) pour {station_name}.")

        df_processed_parts.append(group_copy)

    if not df_processed_parts:
        raise ValueError("Aucune partie de DataFrame n'a pu être traitée après le regroupement par station.")

    # Concaténation de toutes les parties. Puisque chaque partie a un DatetimeIndex,
    # nous pouvons concaténer directement sans ignore_index=True
    df_final = pd.concat(df_processed_parts)
    # Assurez-vous que l'index est trié et qu'il est bien nommé 'Datetime'
    df_final = pd.concat(df_processed_parts).sort_index()
    df_final.index.name = 'Datetime' 
    print(f"DEBUG (interpolation/concat): Index du DataFrame final après concaténation et tri: {type(df_final.index)}")
    print(f"DEBUG (interpolation/concat): Colonnes du DataFrame final après concaténation: {df_final.columns.tolist()}")


    # Nettoyage des colonnes temporaires (elles ne sont plus nécessaires ou n'existent plus)
    cols_to_drop_after_process = ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
    df_final = df_final.drop(columns=cols_to_drop_after_process, errors='ignore')

    # Gestion intelligente de Rain_mm
    if 'Rain_mm' not in df_final.columns or df_final['Rain_mm'].isnull().all():
        if 'Rain_01_mm' in df_final.columns and 'Rain_02_mm' in df_final.columns:
            df_final = create_rain_mm(df_final)
            warnings.warn("Colonne Rain_mm créée à partir des deux capteurs.")
        else:
            warnings.warn("Rain_mm manquant et impossible à créer (capteurs pluie incomplets).")
            if 'Rain_mm' not in df_final.columns:
                df_final['Rain_mm'] = np.nan


    # Interpolation standard et bornage pour les variables numériques
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
                    warnings.warn(f"Remplacement de {new_nan_count - initial_nan_count} valeurs hors limites dans '{var}' par NaN.")
            
            print(f"DEBUG (interpolation/variable): Interpolation de '{var}'. Type de l'index de df_final: {type(df_final.index)}")
            
            # Interpolation seulement si l'index est bien DatetimeIndex
            if isinstance(df_final.index, pd.DatetimeIndex):
                df_final[var] = df_final[var].interpolate(method='time', limit_direction='both')
            else:
                print(f"Avertissement (interpolation/variable): L'index n'est pas un DatetimeIndex pour l'interpolation de '{var}'. Utilisation de la méthode 'linear'.")
                df_final[var] = df_final[var].interpolate(method='linear', limit_direction='both')
            df_final[var] = df_final[var].bfill().ffill()

    # Interpolation CONDITIONNELLE de la radiation solaire
    if 'Solar_R_W/m^2' in df_final.columns:
        df_final['Solar_R_W/m^2'] = pd.to_numeric(df_final['Solar_R_W/m^2'], errors='coerce')

        if 'Solar_R_W/m^2' in limits:
            min_val = limits['Solar_R_W/m^2']['min']
            max_val = limits['Solar_R_W/m^2']['max']
            initial_nan_count = df_final['Solar_R_W/m^2'].isna().sum()
            df_final.loc[(df_final['Solar_R_W/m^2'] < min_val) | (df_final['Solar_R_W/m^2'] > max_val), 'Solar_R_W/m^2'] = np.nan
            if df_final['Solar_R_W/m^2'].isna().sum() > initial_nan_count:
                warnings.warn(f"Remplacement de {df_final['Solar_R_W/m^2'].isna().sum() - initial_nan_count} valeurs hors limites dans 'Solar_R_W/m^2' par NaN.")

        if 'Is_Daylight' in df_final.columns:
            df_final.loc[~df_final['Is_Daylight'] & (df_final['Solar_R_W/m^2'] > 0), 'Solar_R_W/m^2'] = 0

            if 'Rain_mm' in df_final.columns:
                cond_suspect_zeros = (df_final['Is_Daylight']) & (df_final['Solar_R_W/m^2'] == 0) & (df_final['Rain_mm'] == 0)
            else:
                cond_suspect_zeros = (df_final['Is_Daylight']) & (df_final['Solar_R_W/m^2'] == 0)
                warnings.warn("Rain_mm manquant. Tous les 0 de radiation solaire pendant le jour sont traités comme suspects.")
            df_final.loc[cond_suspect_zeros, 'Solar_R_W/m^2'] = np.nan

            print(f"DEBUG (interpolation/solaire): Interpolation de 'Solar_R_W/m^2' (conditionnel). Type de l'index de df_final: {type(df_final.index)}")

            is_day = df_final['Is_Daylight']
            if isinstance(df_final.index, pd.DatetimeIndex):
                df_final.loc[is_day, 'Solar_R_W/m^2'] = df_final.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='time', limit_direction='both')
            else:
                print(f"Avertissement (interpolation/solaire): L'index n'est pas un DatetimeIndex pour l'interpolation de 'Solar_R_W/m^2'. Utilisation de la méthode 'linear'.")
                df_final.loc[is_day, 'Solar_R_W/m^2'] = df_final.loc[is_day, 'Solar_R_W/m^2'].interpolate(method='linear', limit_direction='both')

            df_final.loc[is_day, 'Solar_R_W/m^2'] = df_final.loc[is_day, 'Solar_R_W/m^2'].bfill().ffill()

            df_final.loc[~is_day & df_final['Solar_R_W/m^2'].isna(), 'Solar_R_W/m^2'] = 0
            warnings.warn("Radiation solaire interpolée avec succès.")
        else:
            warnings.warn("Colonne 'Is_Daylight' manquante. Radiation solaire interpolée standard.")
            if isinstance(df_final.index, pd.DatetimeIndex):
                 df_final['Solar_R_W/m^2'] = df_final['Solar_R_W/m^2'].interpolate(method='time', limit_direction='both').bfill().ffill()
            else:
                 df_final['Solar_R_W/m^2'] = df_final['Solar_R_W/m^2'].interpolate(method='linear', limit_direction='both').bfill().ffill()

    warnings.warn("Vérification des valeurs manquantes après interpolation:")
    missing_after_interp = df_final.isna().sum()
    columns_with_missing = missing_after_interp[missing_after_interp > 0]
    if not columns_with_missing.empty:
        warnings.warn(f"Valeurs manquantes persistantes:\n{columns_with_missing}")
    else:
        warnings.warn("Aucune valeur manquante après l'interpolation.")

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
        raise ValueError(
            f"Le DataFrame doit contenir les colonnes {required_utm_cols} pour la conversion UTM."
        )

    def convert_row(row):
        try:
            zone = int(row['zone'])
            hemisphere = str(row['hemisphere']).upper()
            is_northern = hemisphere == 'N'

            proj_utm = CRS.from_proj4(
                f"+proj=utm +zone={zone} +datum=WGS84 +units=m +{'north' if is_northern else 'south'}"
            )
            proj_wgs84 = CRS.from_epsg(4326) # WGS84 Lat/Long

            transformer = Transformer.from_crs(proj_utm, proj_wgs84, always_xy=True)
            lon, lat = transformer.transform(row['Easting'], row['Northing'])
            return pd.Series({'Long': lon, 'Lat': lat})
        except Exception as e:
            warnings.warn(f"Erreur lors de la conversion UTM d'une ligne: {e}")
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

    Cette fonction est destinée à être appelée une seule fois au démarrage de l'application.
    Elle vérifie d'abord si les fichiers existent localement pour éviter des téléchargements répétés.

    Returns:
        pd.DataFrame: Un DataFrame consolidé de toutes les stations avec
                      leurs coordonnées GPS (Lat, Long) et fuseaux horaires.
    """
    print("Début de la préparation des données de coordonnées des stations...")
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True) # Assurez-vous que le dossier 'data' existe

    files_info = [
        {'id': '1Iz5L_XkumG390EZvnMgYr3KwDYeesrNz', 'name': "WASCAL Basins Climate Station Coordinates.xlsx", 'bassin': 'Vea Sissili'},
        {'id': '1H8A-sVMtTok6lrD-NFHQxzHBeQ_P7g4z', 'name': "Dano Basins Climate Station Coordinates.xlsx", 'bassin': 'Dano'},
        {'id': '1SOXI0ZvWqpNp6Qwz_BGeWleUtaYMaOBU', 'name': "DASSARI Climate Station Coordinates.xlsx", 'bassin': 'Dassari'}
    ]

    loaded_dfs = []

    for file_info in files_info:
        output_file_path = os.path.join(data_dir, file_info['name'])
        
        if not os.path.exists(output_file_path):
            print(f"Téléchargement de {file_info['bassin']} depuis Google Drive...")
            gdown.download(f'https://drive.google.com/uc?id={file_info["id"]}', output_file_path, quiet=False)
            print(f"Téléchargement de {file_info['bassin']} terminé.")
        else:
            print(f"Chargement de {file_info['bassin']} depuis le cache local: {output_file_path}")
        
        loaded_dfs.append(pd.read_excel(output_file_path))

    vea_sissili_bassin = loaded_dfs[0]
    dano_bassin = loaded_dfs[1]
    dassari_bassin = loaded_dfs[2]

    # Prétraitement des DataFrames (votre code original)
    print("Début du prétraitement des données de stations...")
    
    # Vea Sissili
    vea_sissili_bassin = vea_sissili_bassin.drop(columns=['No', 'Location', 'parameters'], errors='ignore')
    new_row_df_vea = pd.DataFrame([{'Name': 'Atampisi', 'Lat': 10.91501, 'Long': -0.82647}])
    vea_sissili_bassin = pd.concat([vea_sissili_bassin, new_row_df_vea], ignore_index=True)

    # Dassari
    dassari_bassin = dassari_bassin.drop(columns=['Altitude (en m)'], errors='ignore')
    new_rows_df_dassari = pd.DataFrame([{'Site name': 'Pouri', 'Lat': 1207107, 'Long': 293642}, {'Site name': 'Fandohoun', 'Lat': 1207107, 'Long': 293642}])
    dassari_bassin = pd.concat([dassari_bassin, new_rows_df_dassari], ignore_index=True)

    # Dano et Dassari : Renommage et ajout des colonnes UTM
    dano_bassin = dano_bassin.rename(columns={'Long': 'Easting', 'Lat': 'Northing', 'Site Name': 'Name'})
    dassari_bassin = dassari_bassin.rename(columns={'Long': 'Easting', 'Lat': 'Northing', 'Site name': 'Name'})

    dano_bassin['zone'] = 30
    dano_bassin['hemisphere'] = 'N'
    dassari_bassin['zone'] = 31
    dassari_bassin['hemisphere'] = 'N'

    # Application de la fonction de conversion UTM vers GPS
    dano_bassin = convert_utm_df_to_gps(dano_bassin)
    dassari_bassin = convert_utm_df_to_gps(dassari_bassin)

    # Ajout des fuseaux horaires
    dano_bassin['Timezone'] = 'Africa/Ouagadougou'
    dassari_bassin['Timezone'] = 'Africa/Porto-Novo'
    vea_sissili_bassin['Timezone'] = 'Africa/Accra' # Assurez-vous que c'est le bon fuseau horaire pour Vea Sissili

    # Fusion de tous les bassins
    bassins = pd.concat([vea_sissili_bassin, dano_bassin, dassari_bassin], ignore_index=True)

    # Renommer 'Name' en 'Station'
    bassins = bassins.rename(columns={'Name': 'Station'})

    # Nettoyer les lignes avec des valeurs NaN dans Lat/Long/Timezone qui pourraient résulter d'erreurs de conversion
    initial_rows = len(bassins)
    bassins = bassins.dropna(subset=['Lat', 'Long', 'Timezone', 'Station'])
    if len(bassins) < initial_rows:
        print(f"Attention: {initial_rows - len(bassins)} lignes avec des coordonnées ou fuseaux horaires manquants ont été supprimées du DataFrame des stations.")
    
    # 5. Sauvegarde du DataFrame final en JSON
    output_json_path = os.path.join(data_dir, "station_coordinates.json")
    # Utiliser to_json avec orient='records' pour un format plus lisible et facile à charger
    bassins.to_json(output_json_path, orient='records', indent=4)
    print(f"\nPréparation des données terminée. Coordonnées des stations sauvegardées dans '{output_json_path}'.")
    print("Vous pouvez maintenant lancer votre application Flask.")

    return bassins # Retourner le DataFrame des données GPS


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
            warnings.warn(f"Suppression de {initial_rows - len(df_cleaned)} doublons basés sur 'Station' et 'Datetime'.")
        return df_cleaned
    else:
        warnings.warn("Colonnes 'Station' ou 'Datetime' manquantes pour la gestion des doublons. Le DataFrame n'a pas été modifié.")
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
        warnings.warn("L'index n'est pas un DatetimeIndex dans traiter_outliers_meteo. Tentative de conversion.")
        try:
            df_processed.index = pd.to_datetime(df_processed.index, errors='coerce')
            df_processed = df_processed[df_processed.index.notna()]
            if df_processed.empty:
                raise ValueError("DataFrame vide après nettoyage des dates invalides dans traiter_outliers_meteo.")
        except Exception as e:
            raise TypeError(f"Impossible de garantir un DatetimeIndex pour traiter_outliers_meteo: {e}")

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
                    warnings.warn(f"Remplacement de {new_nan_count - initial_nan_count} valeurs hors limites dans '{var}' par NaN.")
    return df_processed

def generer_graphique_par_variable_et_periode(df: pd.DataFrame, station: str, variable: str, periode: str, colors: dict, metadata: dict) -> go.Figure:
    """
    Génère un graphique Plotly de l'évolution d'une variable pour une station sur une période donnée.
    Retourne l'objet Figure Plotly.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("Le DataFrame doit avoir un DatetimeIndex pour générer le graphique par variable et période.")

    filtered_df = df[df['Station'] == station].copy()
    if filtered_df.empty:
        return go.Figure() # Retourne une figure vide si pas de données

    if periode == 'Journalière':
        resampled_df = filtered_df[variable].resample('D').mean()
    elif periode == 'Hebdomadaire':
        resampled_df = filtered_df[variable].resample('W').mean()
    elif periode == 'Mensuelle':
        resampled_df = filtered_df[variable].resample('M').mean()
    elif periode == 'Annuelle':
        resampled_df = filtered_df[variable].resample('Y').mean()
    else: # Données Brutes
        resampled_df = filtered_df[variable]

    resampled_df = resampled_df.dropna()

    if resampled_df.empty:
        return go.Figure() # Retourne une figure vide si pas de données après resample/dropna

    variable_meta = metadata.get(variable, {'Nom': variable, 'Unite': ''})
    color = colors.get(station, '#1f77b4')

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=resampled_df.index, y=resampled_df.values,
                             mode='lines', name=f'{variable_meta["Nom"]} - {station}',
                             line=dict(color=color)))

    fig.update_layout(
        title=f"Évolution de {variable_meta['Nom']} ({variable_meta['Unite']}) pour {station} ({periode})",
        xaxis_title="Date",
        yaxis_title=f"{variable_meta['Nom']} ({variable_meta['Unite']})",
        hovermode="x unified",
        template="plotly_white" # Utiliser un template Plotly plus clair
    )
    return fig

def generer_graphique_comparatif(df: pd.DataFrame, variable: str, periode: str, colors: dict, metadata: dict) -> go.Figure:
    """
    Génère un graphique Plotly comparatif de l'évolution d'une variable entre toutes les stations.
    Retourne l'objet Figure Plotly.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("Le DataFrame doit avoir un DatetimeIndex pour générer le graphique comparatif.")

    fig = go.Figure()
    
    all_stations = df['Station'].unique()
    if len(all_stations) < 2:
        warnings.warn("Moins de 2 stations disponibles pour la comparaison. Le graphique comparatif ne sera pas généré.")
        return go.Figure() # Retourne une figure vide si moins de 2 stations

    for station in all_stations:
        filtered_df = df[df['Station'] == station].copy()
        if filtered_df.empty:
            continue

        if periode == 'Journalière':
            resampled_df = filtered_df[variable].resample('D').mean()
        elif periode == 'Hebdomadaire':
            resampled_df = filtered_df[variable].resample('W').mean()
        elif periode == 'Mensuelle':
            resampled_df = filtered_df[variable].resample('M').mean()
        elif periode == 'Annuelle':
            resampled_df = filtered_df[variable].resample('Y').mean()
        else: # Données Brutes
            resampled_df = filtered_df[variable]

        resampled_df = resampled_df.dropna()
        if resampled_df.empty:
            continue
        
        color = colors.get(station, '#1f77b4')
        fig.add_trace(go.Scatter(x=resampled_df.index, y=resampled_df.values,
                                 mode='lines', name=station,
                                 line=dict(color=color)))

    if not fig.data:
        return go.Figure() # Retourne une figure vide si aucune trace n'a été ajoutée

    variable_meta = metadata.get(variable, {'Nom': variable, 'Unite': ''})
    fig.update_layout(
        title=f"Comparaison de {variable_meta['Nom']} ({variable_meta['Unite']}) entre stations ({periode})",
        xaxis_title="Date",
        yaxis_title=f"{variable_meta['Nom']} ({variable_meta['Unite']})",
        hovermode="x unified",
        legend_title="Variables",
        template="plotly_white"
    )
    return fig


def generate_multi_variable_station_plot(df: pd.DataFrame, station: str, colors: dict, metadata: dict) -> go.Figure:
    """
    Génère un graphique Plotly de l'évolution normalisée de plusieurs variables pour une station donnée.
    Retourne l'objet Figure Plotly.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("Le DataFrame doit avoir un DatetimeIndex pour générer le graphique multi-variables.")

    filtered_df = df[df['Station'] == station].copy()
    if filtered_df.empty:
        return go.Figure() # Retourne une figure vide si pas de données

    numerical_vars = [col for col in filtered_df.columns if pd.api.types.is_numeric_dtype(filtered_df[col]) and col not in ['Station', 'Is_Daylight', 'Daylight_Duration']]

    if not numerical_vars:
        warnings.warn("Aucune variable numérique trouvée pour la station sélectionnée.")
        return go.Figure() # Retourne une figure vide si pas de variables numériques

    normalized_df = filtered_df[numerical_vars].copy()
    for col in normalized_df.columns:
        min_val = normalized_df[col].min()
        max_val = normalized_df[col].max()
        if max_val != min_val:
            normalized_df[col] = (normalized_df[col] - min_val) / (max_val - min_val)
        else:
            normalized_df[col] = 0.5 if pd.notna(min_val) else np.nan

    normalized_df = normalized_df.dropna(how='all')

    if normalized_df.empty:
        return go.Figure() # Retourne une figure vide si pas de données après normalisation/dropna
    
    fig = go.Figure()
    for var in normalized_df.columns:
        var_meta = metadata.get(var, {'Nom': var, 'Unite': ''})
        color = colors.get(var, None)

        fig.add_trace(go.Scatter(x=normalized_df.index, y=normalized_df[var],
                                 mode='lines', name=var_meta['Nom'],
                                 line=dict(color=color)))

    fig.update_layout(
        title=f"Évolution Normalisée des Variables Météorologiques pour la station {station}",
        xaxis_title="Date",
        yaxis_title="Valeur Normalisée (0-1)",
        hovermode="x unified",
        legend_title="Variables",
        template="plotly_white"
    )
    return fig

def calculate_daily_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les statistiques journalières (moyenne, min, max, somme) pour les variables numériques
    groupées par station. Cette fonction renvoie un DataFrame de statistiques, non un graphique.
    C'est la fonction qui remplace l'ancienne 'daily_stats'.

    Args:
        df (pd.DataFrame): DataFrame d'entrée avec DatetimeIndex et colonne 'Station'.

    Returns:
        pd.DataFrame: DataFrame avec les statistiques journalières.
    """
    df_copy = df.copy()

    # Assurez-vous que 'Datetime' est une colonne et de type datetime
    if isinstance(df_copy.index, pd.DatetimeIndex):
        df_copy = df_copy.reset_index()

    df_copy['Datetime'] = pd.to_datetime(df_copy['Datetime'], errors='coerce')
    df_copy = df_copy.dropna(subset=['Datetime', 'Station'])

    if df_copy.empty:
        print("Avertissement: Le DataFrame est vide après le nettoyage des dates et stations dans calculate_daily_summary_table.")
        return pd.DataFrame()

    if 'Is_Daylight' not in df_copy.columns:
        warnings.warn("La colonne 'Is_Daylight' est manquante. Calcul en utilisant une règle fixe (7h-18h).")
        df_copy['Is_Daylight'] = (df_copy['Datetime'].dt.hour >= 7) & (df_copy['Datetime'].dt.hour <= 18)

    numerical_cols = [col for col in df_copy.columns if pd.api.types.is_numeric_dtype(df_copy[col]) and col not in ['Station', 'Datetime', 'Is_Daylight', 'Daylight_Duration']]
    
    if not numerical_cols:
        warnings.warn("Aucune colonne numérique valide trouvée pour le calcul des statistiques journalières.")
        return pd.DataFrame()

    # Calcul des statistiques de base par jour
    daily_aggregated_df = df_copy.groupby(['Station', df_copy['Datetime'].dt.date]).agg({
        col: ['mean', 'min', 'max'] for col in numerical_cols if METADATA_VARIABLES.get(col, {}).get('is_rain') == False
    })

    # Renommage des colonnes agrégées pour les non-pluies
    daily_aggregated_df.columns = ['_'.join(col).strip() for col in daily_aggregated_df.columns.values]


    # Traitement spécifique pour la pluie (Rain_mm)
    if 'Rain_mm' in df_copy.columns and METADATA_VARIABLES.get('Rain_mm', {}).get('is_rain'):
        df_daily_rain = df_copy.groupby(['Station', df_copy['Datetime'].dt.date])['Rain_mm'].sum().reset_index()
        df_daily_rain = df_daily_rain.rename(columns={'Rain_mm': 'Rain_mm_sum'})

        # Fusionner les statistiques de pluie avec les autres
        if not daily_aggregated_df.empty:
            daily_aggregated_df = daily_aggregated_df.reset_index()
            daily_stats_df = pd.merge(daily_aggregated_df, df_daily_rain, on=['Station', 'Datetime'], how='left')
            daily_stats_df = daily_stats_df.rename(columns={'Datetime': 'Date'})
        else:
            daily_stats_df = df_daily_rain.rename(columns={'Datetime': 'Date'})
    else:
        daily_stats_df = daily_aggregated_df.reset_index().rename(columns={'Datetime': 'Date'})


    # Calcul des statistiques de saison et de sécheresse pour la pluie
    if 'Rain_mm' in df_copy.columns and METADATA_VARIABLES.get('Rain_mm', {}).get('is_rain'):
        df_daily_rain_raw = df_copy.groupby(['Station', pd.Grouper(key='Datetime', freq='D')])['Rain_mm'].sum().reset_index()
        
        RAIN_SEASON_GAP_THRESHOLD = pd.Timedelta(days=60)
        season_stats = []
        for station_name, station_df_rain in df_daily_rain_raw.groupby('Station'):
            station_df_rain = station_df_rain.set_index('Datetime').sort_index()
            rain_events = station_df_rain[station_df_rain['Rain_mm'] > 0].index

            if rain_events.empty:
                season_stats.append({'Station': station_name, 'Moyenne_Saison_Pluvieuse': np.nan, 'Debut_Saison_Pluvieuse': pd.NaT, 'Fin_Saison_Pluvieuse': pd.NaT, 'Duree_Saison_Pluvieuse_Jours': np.nan})
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
                season_stats.append({'Station': station_name, 'Moyenne_Saison_Pluvieuse': np.nan, 'Debut_Saison_Pluvieuse': pd.NaT, 'Fin_Saison_Pluvieuse': pd.NaT, 'Duree_Saison_Pluvieuse_Jours': np.nan})
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
        
        # Merge season stats into main daily_stats_df
        if not df_season_stats.empty:
            daily_stats_df = pd.merge(daily_stats_df, df_season_stats, on='Station', how='left')


    # Calcul des statistiques globales (Max, Min, Moyenne, Médiane, etc.) par station
    # pour les variables numériques, à inclure dans le tableau récapitulatif.
    final_stats_per_station = pd.DataFrame()
    for station_name in df_copy['Station'].unique():
        station_df = df_copy[df_copy['Station'] == station_name].copy()
        station_summary = {'Station': station_name}

        for var in numerical_cols:
            if var in station_df.columns and pd.api.types.is_numeric_dtype(station_df[var]):
                # Filtrage pour la radiation solaire (seulement pendant le jour)
                if var == 'Solar_R_W/m^2':
                    var_data = station_df.loc[station_df['Is_Daylight'], var].dropna()
                else:
                    var_data = station_df[var].dropna()
                
                if not var_data.empty:
                    station_summary[f'{var}_Maximum'] = var_data.max()
                    station_summary[f'{var}_Minimum'] = var_data.min()
                    station_summary[f'{var}_Moyenne'] = var_data.mean()
                    station_summary[f'{var}_Mediane'] = var_data.median()
                    
                    # Pour Rain_mm uniquement
                    if var == 'Rain_mm':
                        station_summary[f'{var}_Cumul_Annuel'] = station_df['Rain_mm'].sum()
                        # Moyenne des jours pluvieux (seulement les jours où il a plu)
                        rainy_days_data = station_df[station_df['Rain_mm'] > 0]['Rain_mm'].dropna()
                        station_summary[f'{var}_Moyenne_Jours_Pluvieux'] = rainy_days_data.mean() if not rainy_days_data.empty else np.nan

                        # Ajouter la durée de la saison pluvieuse et de sécheresse du df_season_stats
                        if 'Duree_Saison_Pluvieuse_Jours' in daily_stats_df.columns:
                            s_data = daily_stats_df[daily_stats_df['Station'] == station_name]
                            if not s_data.empty:
                                station_summary[f'{var}_Duree_Saison_Pluvieuse_Jours'] = s_data['Duree_Saison_Pluvieuse_Jours'].iloc[0]
                                # Placeholder pour sécheresse si non calculé ailleurs
                                station_summary[f'{var}_Duree_Secheresse_Definie_Jours'] = np.nan # Vous devrez calculer ceci plus tard

        final_stats_per_station = pd.concat([final_stats_per_station, pd.DataFrame([station_summary])], ignore_index=True)
        
    return final_stats_per_station # Retourne le DataFrame de statistiques agrégées par station


def generate_variable_summary_plots_for_web(df: pd.DataFrame, station: str, variable: str, metadata: dict, palette: dict) -> plt.Figure:
    """
    Génère un graphique Matplotlib/Seaborn pour les statistiques agrégées d'une variable spécifique
    pour une station donnée, en utilisant la logique fournie par l'utilisateur pour 'daily_stats'.

    Args:
        df (pd.DataFrame): Le DataFrame global de données traitées avec DatetimeIndex et colonne 'Station'.
        station (str): Le nom de la station à visualiser.
        variable (str): La variable à visualiser (e.g., 'Air_Temp_Deg_C', 'Rain_mm').
        metadata (dict): Dictionnaire de métadonnées pour les variables (Nom, Unite, is_rain).
        palette (dict): Dictionnaire de couleurs pour les différentes métriques statistiques.

    Returns:
        plt.Figure: Un objet Figure Matplotlib contenant tous les sous-graphiques pour la variable sélectionnée.
    """
    df_station = df[df['Station'] == station].copy()

    if df_station.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, f"Aucune donnée pour la station '{station}'.", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
        ax.axis('off')
        return fig

    # S'assurer que Datetime est un index datetime correct pour les données de la station
    if isinstance(df_station.index, pd.DatetimeIndex):
        df_station = df_station.reset_index() # Réinitialise l'index pour un accès facile aux colonnes
    
    df_station['Datetime'] = pd.to_datetime(df_station['Datetime'], errors='coerce')
    df_station = df_station.dropna(subset=['Datetime', 'Station'])
    df_station = df_station.set_index('Datetime').sort_index() # Remet l'index pour les opérations de séries temporelles

    if df_station.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, f"DataFrame vide après nettoyage des dates pour la station '{station}'.", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
        ax.axis('off')
        return fig

    # Assurez-vous que 'Is_Daylight' existe (généralement géré par interpolation, mais au cas où)
    if 'Is_Daylight' not in df_station.columns:
        df_station['Is_Daylight'] = (df_station.index.hour >= 7) & (df_station.index.hour <= 18)


    var_meta = metadata.get(variable, {'Nom': variable, 'Unite': ''})

    # Dictionnaire pour stocker les statistiques pour la variable sélectionnée
    stats_for_plot = {}
    metrics_to_plot = []
    
    # --- LOGIQUE SPÉCIFIQUE À RAIN_MM (transférée de la fonction daily_stats de l'utilisateur) ---
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

        # Logique de détection de la sécheresse
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
                        if current_ratio < s_moyenne_saison: # Condition pour la sécheresse définie
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

        # Statistiques pour Rain_mm (tirées de la vue consolidée de daily_stats de l'utilisateur)
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

        # Récupérer les dates pour max/min pour Rain_mm
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
        
    # --- LOGIQUE POUR LES AUTRES VARIABLES (transférée de la fonction daily_stats de l'utilisateur) ---
    else:
        current_var_data = df_station[variable].dropna()
        if variable == 'Solar_R_W/m^2':
            current_var_data = df_station.loc[df_station['Is_Daylight'], variable].dropna()

        if current_var_data.empty:
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, f"Aucune donnée valide pour la variable {var_meta['Nom']} à {station}.", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
            ax.axis('off')
            return fig

        # Statistiques pour les autres variables (de daily_stats de l'utilisateur)
        stats_for_plot['Maximum'] = current_var_data.max()
        stats_for_plot['Minimum'] = current_var_data.min()
        stats_for_plot['Mediane'] = current_var_data.median()
        stats_for_plot['Moyenne'] = current_var_data.mean()

        # Récupérer les dates pour max/min
        max_idx = current_var_data.idxmax() if not current_var_data.empty else pd.NaT
        min_idx = current_var_data.idxmin() if not current_var_data.empty else pd.NaT

        stats_for_plot['Date_Maximum'] = max_idx if pd.notna(max_idx) else pd.NaT
        stats_for_plot['Date_Minimum'] = min_idx if pd.notna(min_idx) else pd.NaT

        metrics_to_plot = ['Maximum', 'Minimum', 'Moyenne', 'Mediane']
        nrows, ncols = 2, 2
        figsize = (18, 12)

    # Gérer le cas où stats_for_plot pourrait être vide (ex: toutes les données sont NaN)
    if not stats_for_plot:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, f"Impossible de calculer des statistiques pour la variable '{variable}' à la station '{station}' (données manquantes ou non numériques).", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=14, color='red')
        ax.axis('off')
        return fig

    # --- PARTIE COMMUNE DE TRACÉ (adaptée de la fonction daily_stats de l'utilisateur) ---
    fig, axes = plt.subplots(nrows, ncols, figsize=figsize)
    plt.subplots_adjust(hspace=0.6, wspace=0.4) # Ajustement de l'espacement pour un meilleur ajustement
    axes = axes.flatten()

    fig.suptitle(f'Statistiques de {var_meta["Nom"]} pour la station {station}', fontsize=16, y=0.98)

    for i, metric in enumerate(metrics_to_plot):
        ax = axes[i]
        value = stats_for_plot.get(metric)
        if pd.isna(value):
            ax.text(0.5, 0.5, "Données non disponibles", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=12, color='gray')
            ax.axis('off')
            continue

        color = palette.get(metric.replace(' ', '_'), '#cccccc') # Utilisation de la `palette` passée en argument
        
        # Créer un DataFrame factice pour sns.barplot, car il attend un DataFrame
        plot_data_bar = pd.DataFrame({'Metric': [metric.replace('_', ' ')], 'Value': [value]})
        sns.barplot(x='Metric', y='Value', data=plot_data_bar, ax=ax, color=color, edgecolor='none')

        # Ajouter les annotations
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
            # Vérifier les clés de date pour max/min si elles existent dans stats_for_plot
            if (metric == 'Maximum' and 'Date_Maximum' in stats_for_plot and pd.notna(stats_for_plot['Date_Maximum'])):
                date_str = f"\n({stats_for_plot['Date_Maximum'].strftime('%d/%m/%Y')})"
            elif (metric == 'Minimum' and 'Date_Minimum' in stats_for_plot and pd.notna(stats_for_plot['Date_Minimum'])):
                date_str = f"\n({stats_for_plot['Date_Minimum'].strftime('%d/%m/%Y')})"
            
            text = f"{value:.1f} {unit}{date_str}"
        else:
            text = f"{value:.1f} {var_meta['Unite']}"

        # Positionne le texte sur le graphique.
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                text, ha='center', va='bottom', fontsize=9, color='black',
                bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=1))
        
        ax.set_title(f"{var_meta['Nom']} {metric.replace('_', ' ')}", fontsize=11)
        ax.set_xlabel("")
        ax.set_ylabel(f"Valeur ({var_meta['Unite']})", fontsize=10)
        ax.tick_params(axis='x', rotation=0)
        ax.set_xticklabels([])

    # Si moins de graphiques que de sous-graphiques, désactiver les axes inutilisés
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.tight_layout(rect=[0, 0, 1, 0.96])

    return fig


def daily_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les statistiques journalières (moyenne, min, max, somme) pour les variables numériques
    groupées par station.

    Args:
        df (pd.DataFrame): DataFrame d'entrée avec DatetimeIndex et colonne 'Station'.

    Returns:
        pd.DataFrame: DataFrame avec les statistiques journalières.
    """
    df = df.copy()

    # Assurez-vous que 'Datetime' est une colonne et de type datetime
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index()

    df['Datetime'] = pd.to_datetime(df['Datetime'], errors='coerce')
    df = df.dropna(subset=['Datetime', 'Station'])

    if df.empty:
        print("Avertissement: Le DataFrame est vide après le nettoyage des dates et stations dans daily_stats.")
        return pd.DataFrame()

    if 'Is_Daylight' not in df.columns:
        warnings.warn("La colonne 'Is_Daylight' est manquante. Calcul en utilisant une règle fixe (7h-18h).")
        df['Is_Daylight'] = (df['Datetime'].dt.hour >= 7) & (df['Datetime'].dt.hour <= 18)

    numerical_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col]) and col not in ['Station', 'Datetime', 'Is_Daylight']]
    
    if not numerical_cols:
        warnings.warn("Aucune colonne numérique valide trouvée pour le calcul des statistiques journalières.")
        return pd.DataFrame()

    agg_funcs = {col: ['mean', 'min', 'max'] for col in numerical_cols}
    
    if 'Rain_mm' in numerical_cols:
        agg_funcs['Rain_mm'].append('sum')

    daily_stats_df = df.groupby(['Station', df['Datetime'].dt.date]).agg(agg_funcs)

    daily_stats_df.columns = ['_'.join(col).strip() for col in daily_stats_df.columns.values]

    daily_stats_df = daily_stats_df.rename_axis(index={'Datetime': 'Date'})
    daily_stats_df = daily_stats_df.reset_index()

    return daily_stats_df

#N4
def generate_stats_plots(df: pd.DataFrame, variable: str) -> plt.Figure:
    """
    Génère les graphiques statistiques pour une variable donnée.
    
    Args:
        df (pd.DataFrame): DataFrame contenant les données météo
        variable (str): Variable à analyser
        
    Returns:
        plt.Figure: Figure matplotlib contenant les graphiques
    """
    import matplotlib.pyplot as plt
    import seaborn as sns
    import numpy as np
    from config import METADATA_VARIABLES, PALETTE_COULEUR
    
    try:
        # Vérification initiale
        if variable not in df.columns:
            raise ValueError(f"Variable '{variable}' non trouvée dans les données")
            
        if 'Station' not in df.columns:
            raise ValueError("Colonne 'Station' requise")

        # Préparation des données
        working_df = df.copy()
        if isinstance(working_df.index, pd.DatetimeIndex):
            working_df = working_df.reset_index()
        
        # Gestion des dates
        if 'Datetime' not in working_df.columns:
            raise ValueError("Colonne 'Datetime' requise")
            
        working_df['Datetime'] = pd.to_datetime(working_df['Datetime'], errors='coerce')
        working_df = working_df.dropna(subset=['Datetime', 'Station'])

        # Calcul des statistiques de base
        stats = working_df.groupby('Station')[variable].agg(
            Maximum='max',
            Minimum='min',
            Mediane='median',
            Moyenne='mean'
        ).reset_index()

        # Gestion spéciale pour la pluie
        if variable == 'Rain_mm':
            stats['Cumul_Annuel'] = working_df.groupby('Station')['Rain_mm'].sum().values

        # Ajout des dates des extrêmes
        try:
            max_dates = working_df.loc[working_df.groupby('Station')[variable].idxmax()][['Station', 'Datetime']]
            min_dates = working_df.loc[working_df.groupby('Station')[variable].idxmin()][['Station', 'Datetime']]
            
            stats = stats.merge(
                max_dates.rename(columns={'Datetime': 'Date_Max'}),
                on='Station',
                how='left'
            )
            stats = stats.merge(
                min_dates.rename(columns={'Datetime': 'Date_Min'}),
                on='Station',
                how='left'
            )
        except:
            stats['Date_Max'] = pd.NaT
            stats['Date_Min'] = pd.NaT

        # Configuration des graphiques
        if variable == 'Rain_mm':
            metrics = ['Maximum', 'Minimum', 'Mediane', 'Moyenne', 'Cumul_Annuel']
            nrows, ncols = 3, 2
        else:
            metrics = ['Maximum', 'Minimum', 'Moyenne', 'Mediane']
            nrows, ncols = 2, 2

        # Création de la figure
        fig, axes = plt.subplots(nrows, ncols, figsize=(15, 5*nrows))
        plt.subplots_adjust(hspace=0.4, wspace=0.3)
        
        if isinstance(axes, np.ndarray):
            axes = axes.flatten()
        else:
            axes = [axes]

        # Titre principal
        var_meta = METADATA_VARIABLES.get(variable, {})
        fig.suptitle(f'Statistiques de {var_meta.get("Nom", variable)} par Station', 
                    fontsize=16, y=0.98)

        # Génération des graphiques
        for i, metric in enumerate(metrics):
            if i >= len(axes):
                break
                
            ax = axes[i]
            color = PALETTE_COULEUR.get(metric, '#cccccc')
            
            sns.barplot(data=stats, x=metric, y='Station', ax=ax, color=color)
            
            # Ajout des étiquettes
            for bar, (_, row) in zip(ax.patches, stats.iterrows()):
                value = row[metric]
                if pd.isna(value):
                    continue
                    
                unit = var_meta.get('Unite', '')
                text = f"{value:.1f} {unit}"
                
                if metric in ['Maximum', 'Minimum']:
                    date_col = f'Date_{metric[:3]}'
                    if date_col in row and pd.notna(row[date_col]):
                        date_str = pd.to_datetime(row[date_col]).strftime('%d/%m/%Y')
                        text = f"{value:.1f}\n{date_str}"
                            
                ax.text(bar.get_width() + 0.05, 
                       bar.get_y() + bar.get_height()/2, 
                       text,
                       ha='left', va='center')
            
            ax.set_title(f"{var_meta.get('Nom', variable)} {metric}")
            ax.set_xlabel(unit)
            ax.set_ylabel('')

        # Masquer les axes inutilisés
        for j in range(i+1, len(axes)):
            fig.delaxes(axes[j])
        
        plt.tight_layout()
        return fig

    except Exception as e:
        import traceback
        print(f"Erreur dans generate_stats_plots: {str(e)}")
        print(traceback.format_exc())
        if 'fig' in locals():
            plt.close(fig)
        return None
