import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from typing import Dict, List, Optional
import traceback
import numpy as np # Assurez-vous que numpy est importé pour np.nan


def get_stations_list(processing_type: str = 'before') -> List[str]:
    """
    Retourne la liste exacte des noms de stations/tables sans modification.
    
    Args:
        processing_type: 'before' ou 'after' pour choisir la base de données
    
    Returns:
        Liste des noms de tables/stations exacts tels qu'en base
    
    Raises:
        ValueError: Si le processing_type est invalide
    """
    # Validation du paramètre
    if processing_type not in ('before', 'after'):
        raise ValueError("Le paramètre processing_type doit être 'before' ou 'after'")
    
    db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
    
    try:
        with get_connection(db_name) as conn:
            with conn.cursor() as cursor:
                # Requête pour récupérer uniquement les tables utilisateur
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
                
                # Retourne les noms exacts des tables sans aucune modification
                return [table[0] for table in cursor.fetchall()]
            
    except psycopg2.Error as e:
        print(f"Erreur de base de données: {e}")
        return []
    except Exception as e:
        print(f"Erreur inattendue: {e}")
        return []
    

def get_station_data(station: str, processing_type: str = 'before') -> Optional[pd.DataFrame]:
    """Récupère les données d'une station depuis la base de données"""
    db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
    table_name = station
    
    try:
        with get_connection(db_name) as conn:
            query = f'SELECT * FROM "{table_name}"'
            return pd.read_sql(query, conn)
    except Exception as e:
        print(f"Erreur lors de la récupération des données pour {station}: {e}")
        return None

def delete_station_data(station: str, processing_type: str = 'after'):
    """Supprime les données d'une station de la base de données"""
    db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
    table_name = station.replace(' ', '_').replace('-', '_')
    
    try:
        with get_connection(db_name) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql.SQL('DROP TABLE IF EXISTS meteo.{}').format(
                    sql.Identifier(table_name)))
                conn.commit()
                return True
    except Exception as e:
        print(f"Erreur lors de la suppression des données pour {station}: {e}")
        return False

def reset_processed_data():
    """Réinitialise toutes les données traitées (supprime toutes les tables dans after_processing)"""
    try:
        with get_connection('after_processing_db') as conn:
            with conn.cursor() as cursor:
                # Récupérer la liste de toutes les tables
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'meteo'
                """)
                tables = cursor.fetchall()
                
                # Supprimer chaque table
                for table in tables:
                    cursor.execute(sql.SQL('DROP TABLE IF EXISTS meteo.{}').format(
                        sql.Identifier(table[0])))
                
                conn.commit()
                return True
    except Exception as e:
        print(f"Erreur lors de la réinitialisation des données traitées: {e}")
        return False

def check_data_exists(station: str, datetime_values: List, processing_type: str = 'before') -> List:
    """Vérifie si des données existent déjà pour les dates données"""
    db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
    table_name = station.replace(' ', '_').replace('-', '_')
    
    try:
        with get_connection(db_name) as conn:
            with conn.cursor() as cursor:
                placeholders = ','.join(['%s'] * len(datetime_values))
                query = f"""
                    SELECT Datetime FROM meteo."{table_name}" 
                    WHERE Datetime IN ({placeholders})
                """
                cursor.execute(query, datetime_values)
                existing = cursor.fetchall()
                return [row[0] for row in existing]
    except Exception as e:
        print(f"Erreur lors de la vérification des données existantes: {e}")
        return []


import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from typing import Dict, List, Optional
import traceback
import numpy as np # Assurez-vous que numpy est importé pour np.nan

# Charger les variables d'environnement
load_dotenv()

# Configuration de la base de données
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME_BEFORE', 'before_processing_db'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': os.getenv('DB_PORT')
}

def get_connection(database: str = None):
    """Établit une connexion à la base de données PostgreSQL"""
    config = DB_CONFIG.copy()
    if database:
        config['database'] = database
    try:
        conn = psycopg2.connect(**config)
        return conn
    except Exception as e:
        print(f"Erreur de connexion à la base de données: {e}")
        raise

# def get_station_columns(station: str, processing_type: str) -> Dict[str, str]:
#     """Retourne les colonnes attendues pour une station donnée selon le type de traitement"""
    
#     # Nettoyage du nom de la station
#     station = station.strip()
#     # BEFORE_PROCESSING
#     if processing_type == 'before':
#         # Bassin DANO
#         if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
#             return {
#                 "Datetime": 'timestamp PRIMARY KEY',
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_%': 'float',
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         elif station in ['Lare', 'Tambiri 2']:
#             return {
#                 'Datetime': 'timestamp PRIMARY KEY',
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float'
#             }
#         elif station == 'Tambiri 1':
#             return {
#                 'Datetime': 'timestamp PRIMARY KEY',
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_mm': 'float',
#                 'BP_mbar_Avg': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_%': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         # Bassin DASSARI
#         elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
#             return {
#                 'Datetime': 'timestamp PRIMARY KEY',
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 #'Rel_H_%': 'float',
#                 'Rel_H_Pct': 'float',
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         elif station == 'Ouriyori 1':
#             return {
#                 'Datetime': 'timestamp PRIMARY KEY',
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_%': 'float',
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         # Bassin VEA SISSILI
#         elif station in ['Oualem', 'Nebou', 'Nabugubelle', 'Manyoro', 'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabisi']:
#             return {
#                 'Datetime': 'timestamp PRIMARY KEY',
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_%': 'float',
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'BP_mbar_Avg': 'float'
#             }
#         elif station == 'Atampisi':
#             return {
#                 'Datetime': 'timestamp PRIMARY KEY',
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_%': 'float',
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         elif station == 'Aniabisi':
#             return {
#                 'Datetime': 'timestamp PRIMARY KEY',
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_%': 'float',
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
    
#     # AFTER_PROCESSING
#     elif processing_type == 'after':
#         # Bassin DANO
#         if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
#             return {
#                 'Datetime': 'timestamp PRIMARY KEY',
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_%': 'float',
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         elif station in ['Lare', 'Tambiri 2']:
#             return {
#                 'Datetime': 'timestamp PRIMARY KEY',
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float'
#             }
#         elif station == 'Tambiri 1':
#             return {
#                 'Datetime': 'timestamp PRIMARY KEY',
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_mm': 'float',
#                 'BP_mbar_Avg': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_%': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         # Bassin DASSARI
#         elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
#             return {
#                 'Datetime': 'timestamp PRIMARY KEY',
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_%': 'float',
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         elif station == 'Ouriyori 1':
#             return {
#                 'Datetime': 'timestamp PRIMARY KEY',
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_%': 'float',
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         # Bassin VEA SISSILI
#         elif station in ['Oualem', 'Nebou', 'Nabugubelle', 'Manyoro', 'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabisi']:
#             return {
#                 'Datetime': 'timestamp PRIMARY KEY',
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_%': 'float',
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'BP_mbar_Avg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         elif station == 'Atampisi':
#             return {
#                 'Datetime': 'timestamp PRIMARY KEY',
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_%': 'float',
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
    
#         elif station == 'Aniabisi':
#             return {
#                 'Datetime': 'timestamp PRIMARY KEY',
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_%': 'float',
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#     return {}


from typing import Dict

from typing import Dict

def get_station_columns(station: str, processing_type: str) -> Dict[str, str]:
    """Retourne les colonnes attendues pour une station donnée selon le type de traitement"""
    
    # Nettoyage du nom de la station
    station = station.strip()

    # BEFORE_PROCESSING
    if processing_type == 'before':
        # Bassin DANO
        if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
            return {
                "Datetime": 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI  JUSTE POUR SE LIMITER A MINUTES
                'Year': 'integer',
                'Month': 'integer', 
                'Day': 'integer',
                'Hour': 'integer',
                'Minute': 'integer',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_Pct': 'float', 
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float'
            }
        elif station in ['Lare', 'Tambiri 2']:
            return {
                'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
                'Year': 'integer',
                'Month': 'integer', 
                'Day': 'integer',
                'Hour': 'integer',
                'Minute': 'integer',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float'
            }
        elif station == 'Tambiri 1':
            return {
                'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
                'Year': 'integer',
                'Month': 'integer', 
                'Day': 'integer',
                'Hour': 'integer',
                'Minute': 'integer',
                'Rain_mm': 'float',
                'BP_mbar_Avg': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_Pct': 'float', 
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float'
            }
        # Bassin DASSARI
        elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
            return {
                'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
                'Year': 'integer',
                'Month': 'integer', 
                'Day': 'integer',
                'Hour': 'integer',
                'Minute': 'integer',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_Pct': 'float', 
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float'
            }
        elif station == 'Ouriyori 1':
            return {
                'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
                'Year': 'integer',
                'Month': 'integer', 
                'Day': 'integer',
                'Hour': 'integer',
                'Minute': 'integer',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_Pct': 'float', 
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float'
            }
        # Bassin VEA SISSILI
        elif station in ['Oualem', 'Nebou', 'Nabugubulle', 'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabiisi']:
            return {
                'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
                'Date': 'date', # Garde Date comme date pure
                'Rain_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_Pct': 'float', 
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float',
                'BP_mbar_Avg': 'float'
            }


        elif station == 'Manyoro':
            return {
                'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
                'Date': 'date',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_Pct': 'float', 
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float',
                #'BP_mbar_Avg': 'float'
            }
        elif station == 'Atampisi':
            return {
                'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
                'Date': 'date',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Rain_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_Pct': 'float', 
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float'
            }
        elif station == 'Aniabisi':
            return {
                'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
                'Date': 'date',
                'Rain_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_Pct': 'float', 
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float'
            }
    
    # AFTER_PROCESSING
    elif processing_type == 'after':
        # Bassin DANO
        if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
            return {
                'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
                'Year': 'integer',
                'Month': 'integer', 
                'Day': 'integer',
                'Hour': 'integer',
                'Minute': 'integer',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Rain_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_Pct': 'float', 
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float',
                'sunrise_time_utc': 'timestamp', # Ces colonnes peuvent rester timestamp (elles ne sont pas forcément à la minute)
                'sunset_time_utc': 'timestamp',   # ou passer à timestamp(0) si c'est votre exigence pour elles aussi.
                'Is_Daylight': 'boolean',
                'Daylight_Duration': 'float'
            }
        elif station in ['Lare', 'Tambiri 2']:
            return {
                'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
                'Year': 'integer',
                'Month': 'integer', 
                'Day': 'integer',
                'Hour': 'integer',
                'Minute': 'integer',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Rain_mm': 'float'
            }
        elif station == 'Tambiri 1':
            return {
                'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
                'Year': 'integer',
                'Month': 'integer', 
                'Day': 'integer',
                'Hour': 'integer',
                'Minute': 'integer',
                'Rain_mm': 'float',
                'BP_mbar_Avg': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_Pct': 'float', 
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float'
            }
        # Bassin DASSARI
        elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
            return {
                'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
                'Year': 'integer',
                'Month': 'integer', 
                'Day': 'integer',
                'Hour': 'integer',
                'Minute': 'integer',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Rain_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_Pct': 'float', 
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float',
                'sunrise_time_utc': 'timestamp',
                'sunset_time_utc': 'timestamp',
                'Is_Daylight': 'boolean',
                'Daylight_Duration': 'float'
            }
        elif station == 'Ouriyori 1':
            return {
                'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
                'Year': 'integer',
                'Month': 'integer', 
                'Day': 'integer',
                'Hour': 'integer',
                'Minute': 'integer',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Rain_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_Pct': 'float', 
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float',
                'sunrise_time_utc': 'timestamp',
                'sunset_time_utc': 'timestamp',
                'Is_Daylight': 'boolean',
                'Daylight_Duration': 'float'
            }
        # Bassin VEA SISSILI
        elif station in ['Oualem', 'Nebou', 'Nabugubulle',  'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabiisi']:
            return {
                'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
                'Date': 'date',
                'Rain_mm': 'float',
                # 'Rain_mm': 'float', # Cette ligne est un doublon, elle devrait être supprimée ou corrigée si c'est une autre colonne.
                'Air_Temp_Deg_C': 'float',
                'Rel_H_Pct': 'float', 
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float',
                'BP_mbar_Avg': 'float',
                'sunrise_time_utc': 'timestamp',
                'sunset_time_utc': 'timestamp',
                'Is_Daylight': 'boolean',
                'Daylight_Duration': 'float'
            }

        elif station == 'Manyoro':
            return {
                'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
                'Date': 'date',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_Pct': 'float', 
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float',
                #'BP_mbar_Avg': 'float'
                'sunrise_time_utc': 'timestamp',
                'sunset_time_utc': 'timestamp',
                'Is_Daylight': 'boolean',
                'Daylight_Duration': 'float'
            }
        elif station == 'Atampisi':
            return {
                'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
                'Date': 'date',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Rain_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_Pct': 'float', 
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float',
                'sunrise_time_utc': 'timestamp',
                'sunset_time_utc': 'timestamp',
                'Is_Daylight': 'boolean',
                'Daylight_Duration': 'float'
            }
    
        elif station == 'Aniabisi': # Cette clause est également redondante si déjà dans la liste ci-dessus
            return {
                'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
                'Date': 'date',
                'Rain_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_Pct': 'float', 
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float',
                'sunrise_time_utc': 'timestamp',
                'sunset_time_utc': 'timestamp',
                'Is_Daylight': 'boolean',
                'Daylight_Duration': 'float'
            }
    return {}


def initialize_database():
    """Initialise simplement les bases de données si elles n'existent pas"""
    try:
        db_user = DB_CONFIG['user']
        
        with get_connection('postgres') as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                for db_name in ['before_processing_db', 'after_processing_db']:
                    cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
                    if not cursor.fetchone():
                        cursor.execute(f"""
                            CREATE DATABASE {db_name}
                            WITH OWNER = {db_user}
                            ENCODING = 'UTF8'
                        """)
                        print(f"Base {db_name} créée avec succès")
        
        print("Initialisation des bases terminée")
        return True
        
    except Exception as e:
        print(f"Erreur initialisation DB: {e}")
        raise

# def get_pg_type(dtype):
#     """Convertit le type logique en type PostgreSQL"""
#     return {
#         'integer': 'INTEGER',
#         'float': 'FLOAT',
#         'timestamp': 'TIMESTAMP',
#         'date': 'DATE',
#         'boolean': 'BOOLEAN',
#         'text': 'TEXT'
#     }.get(dtype.split()[0], 'TEXT')


import numpy as np
import pandas as pd

################# code corrected  pour Lare #################


# def get_pg_type(dtype):
#     """Convertit le type logique en type PostgreSQL"""
#     return {
#         'integer': 'INTEGER',
#         'float': 'FLOAT',
#         'timestamp': 'TIMESTAMP',
#         'date': 'DATE',
#         'boolean': 'BOOLEAN',
#         'text': 'TEXT'
#     }.get(dtype.split()[0], 'TEXT')

# def create_station_table(station: str, processing_type: str = 'before'):
#     """Crée la table avec clé primaire sur Datetime si présente, sans colonne id"""
#     columns_config = get_station_columns(station, processing_type)
#     if not columns_config:
#         raise ValueError(f"Configuration des colonnes manquante pour {station}")
    
#     db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')
#     table_name = station.strip()
#     pk_col = 'Datetime' if 'Datetime' in columns_config else None

#     try:
#         with get_connection(db_name) as conn:
#             conn.autocommit = True
#             with conn.cursor() as cursor:
#                 # Vérification existence table
#                 cursor.execute("""
#                     SELECT EXISTS (
#                         SELECT FROM information_schema.tables 
#                         WHERE table_schema = 'public' AND table_name = %s
#                     )
#                 """, (table_name,))
#                 table_exists = cursor.fetchone()[0]
#                 if not table_exists:
#                     column_defs = []
#                     for col, dtype in columns_config.items():
#                         pg_type = get_pg_type(dtype)
#                         if pk_col and col == pk_col:
#                             column_defs.append(f'"{col}" {pg_type} PRIMARY KEY')
#                         else:
#                             column_defs.append(f'"{col}" {pg_type}')
#                     create_query = f"""
#                         CREATE TABLE "{table_name}" (
#                             {', '.join(column_defs)}
#                         )
#                     """
#                     cursor.execute(create_query)
#                     print(f"Table '{table_name}' créée avec succès")
#                 else:
#                     print(f"Table '{table_name}' existe déjà")
#                 return True
#     except Exception as e:
#         print(f"Erreur création table '{table_name}': {e}")
#         raise

# # --- DÉBUT DE LA FONCTION save_to_database CORRIGÉE ---
# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before') -> bool:
#     """
#     Sauvegarde un DataFrame dans la base de données, en s'assurant que les colonnes
#     correspondent au schéma attendu et gérant les conflits.
#     """
#     conn = None 
#     try:
#         if df.empty:
#             print(f"Warning: DataFrame vide reçu pour la station '{station}' ({processing_type}), aucune donnée à sauvegarder.")
#             return True

#         station = station.strip()
#         table_name = station
#         db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')

#         if not db_name:
#             raise ValueError(f"Nom de la base de données non configuré pour processing_type '{processing_type}'. Vérifiez .env")

#         columns_config = get_station_columns(station, processing_type)
#         if not columns_config:
#             raise ValueError(f"Configuration des colonnes manquante pour la station '{station}' ({processing_type}). Impossible de sauvegarder.")

#         expected_columns = list(columns_config.keys())
#         pk_col = 'Datetime' if 'Datetime' in expected_columns else None 

#         # --- NOUVELLE VÉRIFICATION : Normalisation et validation de la casse des noms de colonnes ---
#         # Normaliser les noms de colonnes du DataFrame pour la comparaison et l'alignement
#         # Par exemple, "datetime" devient "Datetime" si c'est la casse attendue.
#         original_df_columns = df.columns.tolist() # Conserver pour diagnostic
#         df.columns = [col if col in expected_columns else col.title() for col in df.columns] # Tente de normaliser la casse si le nom est proche

#         print("\n--- VÉRIFICATION DES NOMS DE COLONNES (CASSE) ---")
#         df_cols_after_norm = df.columns.tolist()
#         missing_in_df = set(expected_columns) - set(df_cols_after_norm)
#         extra_in_df = set(df_cols_after_norm) - set(expected_columns)
        
#         print(f"Colonnes attendues par la DB: {expected_columns}")
#         print(f"Colonnes du DataFrame (après normalisation de casse): {df_cols_after_norm}")

#         if missing_in_df:
#             print(f"⚠️ **ATTENTION:** Colonnes attendues MAIS ABSENTES du DataFrame (après normalisation): {list(missing_in_df)}")
#             for col in missing_in_df:
#                 df[col] = None # Ajouter les colonnes manquantes avec None
        
#         if extra_in_df:
#             print(f"🗑️ **INFO:** Colonnes présentes dans le DataFrame MAIS NON ATTENDUES par la DB: {list(extra_in_df)}")
#             df = df.drop(columns=list(extra_in_df))
        
#         if not missing_in_df and not extra_in_df and expected_columns == df_cols_after_norm:
#              print("✅ Tous les noms de colonnes du DataFrame correspondent EXACTEMENT (casse incluse) aux colonnes attendues par la DB.")
#         else:
#             print("❌ Des divergences de noms de colonnes (ou de casse) existent. Vérifiez les listes ci-dessus.")
#         print("--- FIN VÉRIFICATION DES NOMS DE COLONNES ---\n")
#         # --- FIN NOUVELLE VÉRIFICATION ---


#         # Réordonner le DataFrame strictement selon l'ordre des colonnes attendues
#         df = df[expected_columns]

#         df = df.replace({np.nan: None, pd.NA: None, pd.NaT: None})

#         type_converters = {
#             'timestamp': lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) and x is not None else None,
#             'date': lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) and x is not None else None,
#             'float': lambda x: float(x) if pd.notna(x) and x is not None else None,
#             'integer': lambda x: int(x) if pd.notna(x) and x is not None else None,
#             'boolean': lambda x: bool(x) if pd.notna(x) and x is not None else None,
#             'text': lambda x: str(x) if pd.notna(x) and x is not None else None
#         }

#         # --- NOUVELLE VÉRIFICATION : Diagnostic détaillé des types de données de chaque colonne ---
#         print("\n--- VÉRIFICATION DES TYPES DE DONNÉES ET VALEURS ANORMALES ---")
#         for col, pg_type_str in columns_config.items():
#             pg_base_type = pg_type_str.split()[0].lower()
            
#             if col not in df.columns:
#                 print(f"Skipping type conversion for '{col}': Column not found in DataFrame.")
#                 continue

#             original_series_dtype = df[col].dtype
#             print(f"Colonne '{col}': Type Pandas original = {original_series_dtype}, Type PG attendu = {pg_base_type}")

#             try:
#                 # Appliquer les conversions de type spécifiques
#                 if pg_base_type in ['timestamp', 'date']:
#                     df[col] = pd.to_datetime(df[col], errors='coerce')
#                     df[col] = df[col].apply(type_converters[pg_base_type])
#                 elif pg_base_type in type_converters:
#                     # Pour les types numériques (float, integer), gérer les NaN/None explicitement
#                     if pg_base_type == 'float':
#                         df[col] = df[col].apply(lambda x: float(x) if pd.notna(x) and x is not None else None)
#                     elif pg_base_type == 'integer':
#                         # Convertir en float d'abord pour gérer NaN, puis en int pour les non-NaN, puis en None pour NaN
#                         df[col] = pd.to_numeric(df[col], errors='coerce')
#                         df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) and x is not None else None)
#                     elif pg_base_type == 'boolean':
#                         df[col] = df[col].apply(lambda x: bool(x) if pd.notna(x) and x is not None else None)
#                     else: # Pour 'text' ou autres
#                         df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) and x is not None else None)

#             except Exception as e:
#                 print(f"--- ⚠️ ERREUR DE CONVERSION SPÉCIFIQUE ⚠️ ---")
#                 print(f"    Conversion de type échouée pour la colonne '{col}' (attendu: {pg_base_type}) pour station '{station}': {str(e)}")
#                 print(f"    Valeurs uniques de la colonne '{col}' avant l'échec: {df[col].unique()[:5]} (max 5)")
#                 # Forcer à None pour permettre l'insertion si possible, mais c'est un signe de données sales.
#                 df[col] = None 
#                 print(f"--- FIN ERREUR DE CONVERSION SPÉCIFIQUE ---")

#         print("--- FIN VÉRIFICATION DES TYPES DE DONNÉES ET VALEURS ANORMALES ---\n")
#         # --- FIN NOUVELLE VÉRIFICATION ---

#         conn = get_connection(db_name)
#         if not conn:
#             raise ConnectionError(f"Impossible d'établir une connexion à la base de données '{db_name}'.")

#         with conn.cursor() as cursor:
#             cursor.execute("""
#                 SELECT EXISTS (
#                     SELECT FROM information_schema.tables
#                     WHERE table_schema = 'public' AND table_name = %s
#                 )
#             """, (table_name,))
#             table_exists = cursor.fetchone()[0]
#             if not table_exists:
#                 print(f"Table '{table_name}' n'existe pas dans '{db_name}'. Tentative de création...")
#                 create_station_table(station, processing_type)
#                 cursor.execute("""
#                     SELECT EXISTS (
#                         SELECT FROM information_schema.tables
#                         WHERE table_schema = 'public' AND table_name = %s
#                     )
#                 """, (table_name,))
#                 if not cursor.fetchone()[0]:
#                     raise Exception(f"La création de la table '{table_name}' a échoué. Vérifiez logs de create_station_table.")
#                 print(f"Table '{table_name}' créée avec succès.")

#             cols_sql = ', '.join([f'"{col}"' for col in expected_columns])
#             placeholders = ', '.join(['%s'] * len(expected_columns))

#             query = ""
#             if pk_col and pk_col in expected_columns:
#                 set_clause = ', '.join(
#                     f'"{col}" = EXCLUDED."{col}"'
#                     for col in expected_columns
#                     if col != pk_col
#                 )
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders})
#                     ON CONFLICT ("{pk_col}") DO UPDATE SET
#                         {set_clause};
#                 """
#             else:
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders});
#                 """
            
#             data_to_insert = [tuple(row) for row in df.values]

#             print("\n--- DIAGNOSTIC FINAL DE LA SAUVEGARDE DB ---")
#             print(f"Station: {station}, Type de Traitement: {processing_type}, Table: {table_name}")
#             print(f"1. Nombre de colonnes attendues (expected_columns): {len(expected_columns)}")
#             print(f"   Liste des expected_columns: {expected_columns}")
#             print(f"2. Nombre de colonnes du DataFrame APRES préparation et alignement: {len(df.columns)}")
#             print(f"   Liste des colonnes du DataFrame APRES préparation et alignement: {df.columns.tolist()}")
            
#             if expected_columns != df.columns.tolist():
#                 print("!!! INCOHÉRENCE FINALE: L'ORDRE/NOMBRE DES COLONNES DF NE CORRESPOND PAS AUX ATTENTES SQL !!!")
#                 print(f"   Zip (Attendu, Actuel): {list(zip(expected_columns, df.columns.tolist()))}")
#             else:
#                 print("✅ L'ordre et le nombre des colonnes du DataFrame correspondent parfaitement à la requête SQL.")

#             print(f"3. Requête SQL générée (nombre de %s: {query.count('%s')}):")
#             print(query)
            
#             if data_to_insert:
#                 print(f"4. Nombre total de lignes à insérer: {len(data_to_insert)}")
#                 print(f"   Premier tuple de données (longueur: {len(data_to_insert[0])}): {data_to_insert[0]}")
#                 print(f"   Types des éléments du premier tuple: {[type(item).__name__ for item in data_to_insert[0]]}")
#             else:
#                 print("AUCUNE DONNÉE À INSÉRER (data_to_insert est vide).")
#             print("--- FIN DU DIAGNOSTIC FINAL ---\n")

#             batch_size = 1000
#             inserted_rows = 0
#             for i in range(0, len(data_to_insert), batch_size):
#                 batch = data_to_insert[i:i + batch_size]
#                 if not batch:
#                     continue

#                 try:
#                     execute_batch(cursor, query, batch)
#                     conn.commit()
#                     inserted_rows += len(batch)
#                 except Exception as e:
#                     conn.rollback()
#                     print(f"Échec de l'insertion du lot {i//batch_size + 1} dans '{table_name}': {str(e)}")
#                     if batch:
#                         print(f"Données de la 1ère ligne du lot en erreur: {batch[0]}")
#                         print(f"Types des données de la 1ère ligne du lot en erreur: {[type(item).__name__ for item in batch[0]]}")
#                     raise 

#             print(f"SUCCÈS: {inserted_rows}/{len(df)} lignes insérées/mises à jour dans '{table_name}'.")
#             return True

#     except Exception as e:
#         print(f"\nERREUR CRITIQUE DANS save_to_database pour '{station}': {str(e)}")
#         print("Traceback complet:")
#         traceback.print_exc()
#         if 'df' in locals() and not df.empty:
#             print("Détails du DataFrame au moment de l'erreur:")
#             print(f"Colonnes du DataFrame: {df.columns.tolist()}")
#             print(f"Types de colonnes du DataFrame:\n{df.dtypes}")
#             print("5 premières lignes du DataFrame:")
#             print(df.head().to_string())
#         return False
#     finally:
#         if conn:
#             conn.close()


################## Fin code correct pour Lare ##################

# def create_station_table(station: str, processing_type: str = 'before'):
#     """
#     Crée la table dans la base de données si elle n'existe pas,
#     avec une clé primaire sur 'Datetime' si présente.
#     """
#     columns_config = get_station_columns(station, processing_type)
#     if not columns_config:
#         print(f"Error: Configuration des colonnes manquante pour {station}. Impossible de créer la table.")
#         return False
    
#     db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')
#     table_name = station.strip()
#     pk_col = 'Datetime' if 'Datetime' in columns_config else None

#     try:
#         with get_connection(db_name) as conn:
#             if not conn:
#                 return False
#             conn.autocommit = True # Pour les opérations DDL
#             with conn.cursor() as cursor:
#                 # Vérifier si la table existe
#                 cursor.execute("""
#                     SELECT EXISTS (
#                         SELECT FROM information_schema.tables 
#                         WHERE table_schema = 'public' AND table_name = %s
#                     )
#                 """, (table_name,))
#                 table_exists = cursor.fetchone()[0]
#                 if not table_exists:
#                     column_defs = []
#                     for col, dtype in columns_config.items():
#                         pg_type = get_pg_type(dtype)
#                         if pk_col and col == pk_col:
#                             column_defs.append(f'"{col}" {pg_type} PRIMARY KEY')
#                         else:
#                             column_defs.append(f'"{col}" {pg_type}')
#                     create_query = f"""
#                         CREATE TABLE "{table_name}" (
#                             {', '.join(column_defs)}
#                         )
#                     """
#                     print(f"Executing table creation query for '{table_name}':\n{create_query}")
#                     cursor.execute(create_query)
#                     print(f"Table '{table_name}' créée avec succès")
#                 else:
#                     print(f"Table '{table_name}' existe déjà")
#                 return True
#     except Exception as e:
#         print(f"Erreur lors de la création de la table '{table_name}': {e}")
#         traceback.print_exc()
#         return False

# from psycopg2 import extras
# from datetime import datetime

# # --- DÉBUT DE LA FONCTION save_to_database CORRIGÉE ---
# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before') -> bool:
#     """
#     Sauvegarde un DataFrame dans la base de données, en s'assurant que les colonnes
#     correspondent au schéma attendu et gérant les conflits en ignorant les doublons.
#     """
#     conn = None 
#     try:
#         if df.empty:
#             print(f"Warning: DataFrame vide reçu pour la station '{station}' ({processing_type}), aucune donnée à sauvegarder.")
#             return True

#         station = station.strip()
#         table_name = station
#         db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')

#         if not db_name:
#             raise ValueError(f"Nom de la base de données non configuré pour processing_type '{processing_type}'. Vérifiez .env")

#         columns_config = get_station_columns(station, processing_type)
#         if not columns_config:
#             raise ValueError(f"Configuration des colonnes manquante pour la station '{station}' ({processing_type}). Impossible de sauvegarder.")

#         expected_columns = list(columns_config.keys())
#         pk_col = 'Datetime' if 'Datetime' in expected_columns else None 

#         # --- Normalisation et validation de la casse des noms de colonnes ---
#         # (Cette section reste inchangée, elle est robuste)
#         df = df.copy() 

#         column_mapping = {col.lower(): col for col in expected_columns}
#         new_df_columns = []
#         for col_df in df.columns:
#             if col_df in expected_columns:
#                 new_df_columns.append(col_df)
#             elif col_df.lower() in column_mapping:
#                 new_df_columns.append(column_mapping[col_df.lower()])
#             else:
#                 new_df_columns.append(col_df)
        
#         df.columns = new_df_columns

#         print("\n--- VÉRIFICATION DES NOMS DE COLONNES (CASSE) ---")
#         df_cols_after_norm = df.columns.tolist()
#         missing_in_df = set(expected_columns) - set(df_cols_after_norm)
#         extra_in_df = set(df_cols_after_norm) - set(expected_columns)
        
#         print(f"Colonnes attendues par la DB: {expected_columns}")
#         print(f"Colonnes du DataFrame (après normalisation de casse): {df_cols_after_norm}")

#         if missing_in_df:
#             print(f"⚠️ **ATTENTION:** Colonnes attendues MAIS ABSENTES du DataFrame (après normalisation): {list(missing_in_df)}")
#             for col in missing_in_df:
#                 df[col] = None 
        
#         if extra_in_df:
#             print(f"🗑️ **INFO:** Colonnes présentes dans le DataFrame MAIS NON ATTENDUES par la DB: {list(extra_in_df)}")
#             df = df.drop(columns=list(extra_in_df))
        
#         if not missing_in_df and not extra_in_df and set(expected_columns) == set(df.columns.tolist()):
#              print("✅ Tous les noms de colonnes du DataFrame correspondent EXACTEMENT (casse incluse) aux colonnes attendues par la DB.")
#         else:
#             print("❌ Des divergences de noms de colonnes (ou de casse) existent. Vérifiez les listes ci-dessus.")
#         print("--- FIN VÉRIFICATION DES NOMS DE COLONNES ---\n")

#         # Réordonner le DataFrame strictement selon l'ordre des colonnes attendues
#         df = df[[col for col in expected_columns if col in df.columns]]

#         print("\n--- VÉRIFICATION DES TYPES DE DONNÉES ET VALEURS ANORMALES ---")
#         # (Cette section reste inchangée, elle est robuste)
#         nan_value_strings = ['NaN', 'NAN', 'nan', '', ' ', '#VALUE!', '-', 'NULL', 'null', 'N/A', 'NA', 'None', 'NONE', 'NV', 'N.V.', '#DIV/0!']

#         for col, pg_type_str in columns_config.items():
#             pg_base_type = pg_type_str.split()[0].lower()
            
#             if col not in df.columns:
#                 print(f"Skipping initial type processing for '{col}': Column not found in DataFrame after alignment.")
#                 continue 

#             original_series_dtype = df[col].dtype
#             print(f"Colonne '{col}': Type Pandas original = {original_series_dtype}, Type PG attendu = {pg_base_type}")

#             try:
#                 df[col] = df[col].replace(nan_value_strings, np.nan)
                
#                 if pg_base_type in ['timestamp', 'date']:
#                     df[col] = pd.to_datetime(df[col], errors='coerce') 
                
#                 elif pg_base_type in ['float', 'integer']:
#                     df[col] = pd.to_numeric(df[col], errors='coerce')
                
#             except Exception as e:
#                 print(f"--- ⚠️ ERREUR DE CONVERSION SPÉCIFIQUE (PRÉ-TRAITEMENT PANDAS) ⚠️ ---")
#                 print(f"    Problème avec la colonne '{col}' (attendu: {pg_base_type}) pour station '{station}': {str(e)}")
#                 problematic_vals_series = df[col][pd.to_numeric(df[col], errors='coerce').isna() & df[col].notna()]
#                 if not problematic_vals_series.empty:
#                     print(f"    Exemples de valeurs problématiques non convertibles dans '{col}': {problematic_vals_series.astype(str).unique()[:5]} (max 5)")
#                 print(f"--- FIN ERREUR DE CONVERSION SPÉCIFIQUE (PRÉ-TRAITEMENT PANDAS) ---")

#         print("--- FIN VÉRIFICATION DES TYPES DE DONNÉES ET VALEURES ANORMALES ---\n")
        
#         # --- CRITICAL STEP: FINAL check and tuple creation ---
#         data_to_insert = []
        
#         type_converters = []
#         for col in expected_columns:
#             pg_type_str = columns_config.get(col, 'text')
#             pg_base_type = pg_type_str.split()[0].lower()

#             if pg_base_type == 'timestamp':
#                 type_converters.append(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None)
#             elif pg_base_type == 'date':
#                 type_converters.append(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
#             elif pg_base_type == 'float':
#                 type_converters.append(lambda x: float(x) if pd.notna(x) else None)
#             elif pg_base_type == 'integer':
#                 type_converters.append(lambda x: int(x) if pd.notna(x) and (isinstance(x, (int, float)) and x == int(x)) else None)
#             elif pg_base_type == 'boolean':
#                 type_converters.append(lambda x: bool(x) if pd.notna(x) else None)
#             elif pg_base_type == 'text':
#                 type_converters.append(lambda x: str(x) if pd.notna(x) else None)
#             else:
#                 type_converters.append(lambda x: x if pd.notna(x) else None) 

#         for index, row in df.iterrows():
#             try:
#                 row_values = []
#                 for i, col in enumerate(expected_columns):
#                     value = row[col] 
#                     processed_value = type_converters[i](value) 
#                     row_values.append(processed_value)
#                 data_to_insert.append(tuple(row_values))
#             except KeyError as e:
#                 print(f"--- ⚠️ CRITICAL ERROR: Missing column '{e}' when creating tuple for row {index} ⚠️ ---")
#                 print(f"    DataFrame columns are: {df.columns.tolist()}")
#                 continue 
#             except Exception as e:
#                 print(f"--- ⚠️ GENERIC CRITICAL ERROR when creating tuple for row {index} ⚠️ ---")
#                 print(f"    Details: {str(e)}")
#                 print(f"    Row values (first 5): {row.head().to_dict()}") 
#                 continue

#         conn = get_connection(db_name)
#         if not conn:
#             raise ConnectionError(f"Impossible d'établir une connexion à la base de données '{db_name}'.")

#         with conn.cursor() as cursor:
#             # Check if table exists, create if not
#             cursor.execute("""
#                 SELECT EXISTS (
#                     SELECT FROM information_schema.tables
#                     WHERE table_schema = 'public' AND table_name = %s
#                 )
#             """, (table_name,))
#             table_exists = cursor.fetchone()[0]
#             if not table_exists:
#                 print(f"Table '{table_name}' n'existe pas dans '{db_name}'. Tentative de création...")
#                 if not create_station_table(station, processing_type):
#                     raise Exception(f"La création de la table '{table_name}' a échoué. Vérifiez logs de create_station_table.")
#                 cursor.execute("""
#                     SELECT EXISTS (
#                         SELECT FROM information_schema.tables
#                         WHERE table_schema = 'public' AND table_name = %s
#                     )
#                 """, (table_name,))
#                 if not cursor.fetchone()[0]:
#                     raise Exception(f"La table '{table_name}' n'a pas été trouvée après la tentative de création.")
#                 print(f"Table '{table_name}' créée avec succès.")

#             cols_sql = ', '.join([f'"{col}"' for col in expected_columns])
#             placeholders = ', '.join(['%s'] * len(expected_columns))

#             # --- MODIFICATION CLÉ ICI : UTILISATION DE DO NOTHING ---
#             query = f"""
#                 INSERT INTO "{table_name}" ({cols_sql})
#                 VALUES ({placeholders})
#                 ON CONFLICT ("{pk_col}") DO NOTHING; 
#             """
#             # --- FIN MODIFICATION CLÉ ---
            
#             print("\n--- DIAGNOSTIC FINAL DE LA SAUVEGARDE DB ---")
#             print(f"Station: {station}, Type de Traitement: {processing_type}, Table: {table_name}")
#             print(f"1. Nombre de colonnes attendues (expected_columns): {len(expected_columns)}")
#             print(f"   Liste des expected_columns: {expected_columns}")
#             print(f"2. Nombre de colonnes du DataFrame APRES préparation et alignement: {len(df.columns)}")
#             print(f"   Liste des colonnes du DataFrame APRES préparation et alignement: {df.columns.tolist()}")
            
#             if expected_columns != df.columns.tolist():
#                 print("!!! INCOHÉRENCE FINALE: L'ORDRE/NOMBRE DES COLONNES DF NE CORRESPOND PAS AUX ATTENTES SQL !!!")
#                 print(f"   Zip (Attendu, Actuel): {list(zip(expected_columns, df.columns.tolist()))}")
#             else:
#                 print("✅ L'ordre et le nombre des colonnes du DataFrame correspondent parfaitement à la requête SQL.")

#             print(f"3. Requête SQL générée (nombre de %s: {query.count('%s')}):")
#             print(query)
            
#             if data_to_insert:
#                 print(f"4. Nombre total de lignes à insérer: {len(data_to_insert)}")
#                 if len(data_to_insert) > 0: 
#                     first_tuple_len = len(data_to_insert[0]) if data_to_insert[0] is not None else 0
#                     print(f"   Premier tuple de données (longueur: {first_tuple_len}): {data_to_insert[0]}")
#                     print(f"   Types des éléments du premier tuple: {[type(item).__name__ for item in data_to_insert[0]]}")
#                     if first_tuple_len != len(expected_columns):
#                          print(f"!!! DISCREPANCY: Length of first tuple ({first_tuple_len}) does not match expected columns ({len(expected_columns)}) !!!")
#             else:
#                 print("AUCUNE DONNÉE À INSÉRER (data_to_insert est vide après nettoyage/préparation).")
#             print("--- FIN DU DIAGNOSTIC FINAL ---\n")

#             # NOUVEAU DIAGNOSTIC CRITIQUE MANUEL DU MOGRIFY (inchangé, toujours utile)
#             print("\n--- DIAGNOSTIC CRITIQUE MANUEL DU MOGRIFY ---")
#             if data_to_insert:
#                 try:
#                     first_row_tuple = data_to_insert[0]
#                     print(f"Attempting cursor.mogrify() with query and first data tuple:")
#                     print(f"Query: {query}")
#                     print(f"First tuple: {first_row_tuple}")
#                     print(f"Types in first tuple: {[type(item).__name__ for item in first_row_tuple]}")
                    
#                     mogrified_sql = cursor.mogrify(query, first_row_tuple)
#                     print(f"SUCCESS: mogrify() produced SQL:")
#                     print(mogrified_sql.decode('utf-8')) # Decode bytes to string for printing
#                     print("This means the tuple length and types are likely fine for psycopg2 to process.")
#                 except Exception as e:
#                     print(f"ERROR: mogrify() failed for the first tuple: {str(e)}")
#                     print(f"This is the direct cause of the IndexError. Please copy this error and its traceback.")
#                     traceback.print_exc() # Print full traceback for this specific mogrify error
#                     raise
#             else:
#                 print("No data to insert, skipping manual mogrify check.")
#             print("--- FIN DIAGNOSTIC CRITIQUE MANUEL DU MOGRIFY ---\n")

#             batch_size = 1000
#             inserted_rows = 0
#             if not data_to_insert: 
#                 print("Aucune donnée valide à insérer après la préparation.")
#                 return True 
                
#             for i in range(0, len(data_to_insert), batch_size):
#                 batch = data_to_insert[i:i + batch_size]
#                 if not batch:
#                     continue

#                 try:
#                     for row_idx, row_tuple in enumerate(batch):
#                         if len(row_tuple) != len(expected_columns):
#                             print(f"ERROR: Tuple at batch index {row_idx} (global index {i + row_idx}) has incorrect length: {len(row_tuple)} vs {len(expected_columns)} expected.")
#                             print(f"Problematic tuple: {row_tuple}")
#                             raise ValueError("Incorrect tuple length detected before execute_batch. Data likely corrupted or misaligned.")

#                     extras.execute_batch(cursor, query, batch)
#                     conn.commit()
#                     inserted_rows += len(batch)
#                 except Exception as e:
#                     conn.rollback()
#                     print(f"Échec de l'insertion du lot {i//batch_size + 1} dans '{table_name}': {str(e)}")
#                     if batch:
#                         print(f"Données de la 1ère ligne du lot en erreur: {batch[0]}")
#                         print(f"Types des données de la 1ère ligne du lot en erreur: {[type(item).__name__ for item in batch[0]]}")
#                     raise 

#             print(f"SUCCÈS: {inserted_rows}/{len(df)} lignes insérées/mises à jour dans '{table_name}'.")
#             return True

#     except Exception as e:
#         print(f"\nERREUR CRITIQUE DANS save_to_database pour '{station}': {str(e)}")
#         print("Traceback complet:")
#         traceback.print_exc()
#         if 'df' in locals() and not df.empty:
#             print("Détails du DataFrame au moment de l'erreur:")
#             print(f"Colonnes du DataFrame: {df.columns.tolist()}")
#             print(f"Types de colonnes du DataFrame:\n{df.dtypes}")
#             print("5 premières lignes du DataFrame:")
#             print(df.head().to_string()) 
#         return False
#     finally:
#         if conn:
#             conn.close()



################################## Fin code correct pour Lare ##################################
####################code deepseek bien fait en terme de logique #################


# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before') -> bool:
#     """
#     Sauvegarde un DataFrame dans la base de données, avec vérifications complètes et journalisation détaillée.
#     """
#     conn = None 
#     try:
#         # Vérification initiale
#         if df.empty:
#             print(f"Warning: DataFrame vide reçu pour la station '{station}' ({processing_type}), aucune donnée à sauvegarder.")
#             return True

#         station = station.strip()
#         table_name = station
#         db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')

#         if not db_name:
#             raise ValueError(f"Nom de la base de données non configuré pour processing_type '{processing_type}'. Vérifiez .env")

#         columns_config = get_station_columns(station, processing_type)
#         if not columns_config:
#             raise ValueError(f"Configuration des colonnes manquante pour la station '{station}' ({processing_type}). Impossible de sauvegarder.")

#         expected_columns = list(columns_config.keys())

#         # ========== VÉRIFICATION 1: NOMS DE COLONNES ==========
#         print("\n" + "="*50)
#         print("VÉRIFICATION DES NOMS DE COLONNES (CASSE ET CORRESPONDANCE)")
#         print("="*50)
        
#         # Normalisation des noms de colonnes
#         column_mapping = {col.lower(): col for col in expected_columns}
#         new_df_columns = []
#         for col_df in df.columns:
#             if col_df in expected_columns:
#                 new_df_columns.append(col_df)
#             elif col_df.lower() in column_mapping:
#                 new_df_columns.append(column_mapping[col_df.lower()])
#             else:
#                 new_df_columns.append(col_df)
        
#         df.columns = new_df_columns

#         # Journalisation des résultats
#         df_cols_after_norm = df.columns.tolist()
#         missing_in_df = set(expected_columns) - set(df_cols_after_norm)
#         extra_in_df = set(df_cols_after_norm) - set(expected_columns)
        
#         print(f"\nColonnes attendues par la DB: {expected_columns}")
#         print(f"Colonnes du DataFrame (après normalisation): {df_cols_after_norm}")

#         if missing_in_df:
#             print(f"\n⚠️ ATTENTION: Colonnes attendues mais absentes du DataFrame:")
#             for col in missing_in_df:
#                 print(f"- {col}")
#                 df[col] = None  # Ajout des colonnes manquantes avec valeurs NULL
        
#         if extra_in_df:
#             print(f"\nℹ️ INFO: Colonnes supplémentaires dans le DataFrame:")
#             for col in extra_in_df:
#                 print(f"- {col}")
#             df = df.drop(columns=list(extra_in_df))

#         if not missing_in_df and not extra_in_df and set(expected_columns) == set(df.columns.tolist()):
#             print("\n✅ Tous les noms de colonnes correspondent exactement")
#         else:
#             print("\n❌ Des divergences de noms de colonnes existent")

#         # Réordonnancement des colonnes
#         df = df[expected_columns]
#         print("\nOrdre final des colonnes:", df.columns.tolist())
#         print("="*50 + "\n")

#         # ========== VÉRIFICATION 2: TYPES DE DONNÉES ==========
#         print("\n" + "="*50)
#         print("VÉRIFICATION DES TYPES DE DONNÉES ET VALEURS ANORMALES")
#         print("="*50)
        
#         nan_value_strings = ['NaN', 'NAN', 'nan', '', ' ', '#VALUE!', '-', 'NULL', 'null', 'N/A', 'NA', 'None', 'NONE', 'NV', 'N.V.', '#DIV/0!']
        
#         for col, pg_type_str in columns_config.items():
#             if col not in df.columns:
#                 print(f"\n⚠️ Colonne '{col}' non trouvée - ignorée")
#                 continue

#             original_dtype = str(df[col].dtype)
#             pg_base_type = pg_type_str.split()[0].lower()
            
#             print(f"\nColonne: {col}")
#             print(f"- Type Pandas original: {original_dtype}")
#             print(f"- Type PostgreSQL attendu: {pg_base_type}")
            
#             try:
#                 # Nettoyage des valeurs
#                 df[col] = df[col].replace(nan_value_strings, np.nan)
                
#                 # Conversion des types
#                 if pg_base_type in ['timestamp', 'date']:
#                     df[col] = pd.to_datetime(df[col], errors='coerce')
#                     print("- Converti en datetime")
#                 elif pg_base_type in ['float', 'integer']:
#                     df[col] = pd.to_numeric(df[col], errors='coerce')
#                     print("- Converti en numérique")
                
#                 # Statistiques après conversion
#                 null_count = df[col].isna().sum()
#                 print(f"- Valeurs NULL après conversion: {null_count}/{len(df)} ({null_count/len(df)*100:.2f}%)")
                
#                 if null_count > 0:
#                     sample_null = df[df[col].isna()].head(3)
#                     print("- Exemple de lignes avec NULL:", sample_null[[col]].to_string())
                    
#             except Exception as e:
#                 print(f"\n❌ ERREUR DE CONVERSION pour '{col}': {str(e)}")
#                 problematic_vals = df[col][~df[col].isna() & pd.to_numeric(df[col], errors='coerce').isna()]
#                 if not problematic_vals.empty:
#                     print("- Valeurs problématiques:", problematic_vals.unique()[:5])
#                 traceback.print_exc()

#         print("\nRésumé des types après conversion:")
#         print(df.dtypes)
#         print("="*50 + "\n")

#         # ========== PRÉPARATION DES DONNÉES ==========
#         print("\n" + "="*50)
#         print("PRÉPARATION DES DONNÉES POUR INSERTION")
#         print("="*50)
        
#         data_to_insert = []
#         type_converters = []
        
#         # Création des convertisseurs de type
#         for col in expected_columns:
#             pg_type_str = columns_config.get(col, 'text')
#             pg_base_type = pg_type_str.split()[0].lower()

#             if pg_base_type == 'timestamp':
#                 type_converters.append(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None)
#             elif pg_base_type == 'date':
#                 type_converters.append(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
#             elif pg_base_type == 'float':
#                 type_converters.append(lambda x: float(x) if pd.notna(x) else None)
#             elif pg_base_type == 'integer':
#                 type_converters.append(lambda x: int(x) if pd.notna(x) and x == int(x) else None)
#             else:
#                 type_converters.append(lambda x: str(x) if pd.notna(x) else None)

#         # Conversion des lignes
#         for idx, row in df.iterrows():
#             try:
#                 row_values = [type_converters[i](row[col]) for i, col in enumerate(expected_columns)]
#                 data_to_insert.append(tuple(row_values))
                
#                 # Journalisation pour les premières lignes
#                 if idx < 2:
#                     print(f"\nExemple ligne {idx}:")
#                     for i, col in enumerate(expected_columns):
#                         val = row_values[i]
#                         print(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                        
#             except Exception as e:
#                 print(f"\n❌ ERREUR ligne {idx}: {str(e)}")
#                 print("Valeurs:", row.to_dict())
#                 traceback.print_exc()
#                 continue

#         print(f"\nTotal lignes préparées: {len(data_to_insert)}")
#         print("="*50 + "\n")

#         # ========== CONNEXION À LA BASE ==========
#         conn = get_connection(db_name)
#         if not conn:
#             raise ConnectionError(f"Impossible de se connecter à la base '{db_name}'")

#         with conn.cursor() as cursor:
#             # Vérification de l'existence de la table
#             cursor.execute("""
#                 SELECT EXISTS (
#                     SELECT FROM information_schema.tables
#                     WHERE table_schema = 'public' AND table_name = %s
#                 )
#             """, (table_name,))
#             table_exists = cursor.fetchone()[0]
            
#             if not table_exists:
#                 print(f"\nℹ️ La table '{table_name}' n'existe pas, création...")
#                 if not create_station_table(station, processing_type):
#                     raise Exception(f"Échec de la création de la table '{table_name}'")
#                 print("✅ Table créée avec succès")

#             # Détection de la clé primaire
#             cursor.execute("""
#                 SELECT a.attname
#                 FROM pg_index i
#                 JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
#                 WHERE i.indrelid = %s::regclass AND i.indisprimary
#             """, (table_name,))
#             pk_info = cursor.fetchone()
#             pk_col = pk_info[0] if pk_info else None
#             print(f"\nClé primaire détectée: {pk_col if pk_col else 'Aucune'}")

#             # Construction de la requête
#             cols_sql = ', '.join([f'"{col}"' for col in expected_columns])
#             placeholders = ', '.join(['%s'] * len(expected_columns))
            
#             if pk_col:
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders})
#                     ON CONFLICT ("{pk_col}") DO NOTHING
#                 """
#             else:
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders})
#                 """

#             print("\nRequête SQL générée:")
#             print(query)

#             # ========== VÉRIFICATION FINALE ==========
#             print("\n" + "="*50)
#             print("VÉRIFICATION FINALE AVANT INSERTION")
#             print("="*50)
            
#             print(f"\nNombre de colonnes attendues: {len(expected_columns)}")
#             print(f"Nombre de colonnes préparées: {len(df.columns)}")
            
#             if len(expected_columns) != len(df.columns):
#                 print("\n❌ ERREUR: Nombre de colonnes incompatible!")
#                 print("Colonnes attendues vs préparées:")
#                 for exp, act in zip(expected_columns, df.columns):
#                     print(f"- {exp} => {act}")

#             if data_to_insert:
#                 first_row = data_to_insert[0]
#                 print(f"\nPremière ligne de données ({len(first_row)} valeurs):")
#                 for val, col in zip(first_row, expected_columns):
#                     print(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                
#                 # Test de mogrify
#                 try:
#                     mogrified = cursor.mogrify(query, first_row).decode('utf-8')
#                     print("\nTest mogrify réussi:")
#                     print(mogrified[:500] + ("..." if len(mogrified) > 500 else ""))
#                 except Exception as e:
#                     print("\n❌ ERREUR mogrify:", str(e))
#                     traceback.print_exc()
#                     raise
#             else:
#                 print("\n⚠️ Aucune donnée à insérer!")
#                 return False

#             print("="*50 + "\n")

#             # ========== INSERTION ==========
#             batch_size = 1000
#             inserted_rows = 0
#             start_time = datetime.now()
            
#             print(f"\nDébut de l'insertion de {len(data_to_insert)} lignes par lots de {batch_size}...")
            
#             for i in range(0, len(data_to_insert), batch_size):
#                 batch = data_to_insert[i:i + batch_size]
#                 try:
#                     extras.execute_batch(cursor, query, batch)
#                     conn.commit()
#                     inserted_rows += len(batch)
                    
#                     # Journalisation périodique
#                     if (i // batch_size) % 10 == 0:
#                         elapsed = (datetime.now() - start_time).total_seconds()
#                         rate = inserted_rows / elapsed if elapsed > 0 else 0
#                         print(f"Lot {i//batch_size + 1}: {inserted_rows} lignes ({rate:.1f} lignes/sec)")
                        
#                 except Exception as e:
#                     conn.rollback()
#                     print(f"\n❌ ERREUR lot {i//batch_size + 1}: {str(e)}")
#                     if batch:
#                         print("Exemple de ligne problématique:", batch[0])
#                     traceback.print_exc()
#                     raise

#             total_time = (datetime.now() - start_time).total_seconds()
#             print(f"\n✅ Insertion terminée: {inserted_rows} lignes en {total_time:.2f} secondes ({inserted_rows/max(total_time,1):.1f} lignes/sec)")
#             return True

#     except Exception as e:
#         print(f"\n❌❌❌ ERREUR CRITIQUE: {str(e)}")
#         traceback.print_exc()
        
#         if 'df' in locals():
#             print("\nÉtat du DataFrame au moment de l'erreur:")
#             print(f"Shape: {df.shape}")
#             print("5 premières lignes:")
#             print(df.head().to_string())
            
#         return False
        
#     finally:
#         if conn:
#             conn.close()
#             print("\nConnexion à la base de données fermée")

################### Fin code deepseek ###################
import os
import traceback
import numpy as np
import pandas as pd
from psycopg2 import extras
from datetime import datetime

def get_pg_type(dtype):
    """Convertit le type logique en type PostgreSQL"""
    return {
        'integer': 'INTEGER',
        'float': 'FLOAT',
        'timestamp': 'TIMESTAMP',
        'date': 'DATE',
        'boolean': 'BOOLEAN',
        'text': 'TEXT'
    }.get(dtype.split()[0], 'TEXT')

# def create_station_table(station: str, processing_type: str = 'before'):
#     """
#     Crée la table dans la base de données si elle n'existe pas,
#     avec une clé primaire sur 'Datetime' si présente.
#     """
#     columns_config = get_station_columns(station, processing_type)
#     if not columns_config:
#         print(f"Error: Configuration des colonnes manquante pour {station}. Impossible de créer la table.")
#         return False
    
#     db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')
#     table_name = station.strip()

#     try:
#         with get_connection(db_name) as conn:
#             if not conn:
#                 return False
#             conn.autocommit = True
#             with conn.cursor() as cursor:
#                 # Vérifier si la table existe
#                 cursor.execute("""
#                     SELECT EXISTS (
#                         SELECT FROM information_schema.tables 
#                         WHERE table_schema = 'public' AND table_name = %s
#                     )
#                 """, (table_name,))
#                 table_exists = cursor.fetchone()[0]
                
#                 if not table_exists:
#                     column_defs = []
#                     for col, dtype in columns_config.items():
#                         pg_type = get_pg_type(dtype)
#                         column_defs.append(f'"{col}" {pg_type}')
                    
#                     # Ajouter la clé primaire si la colonne 'Datetime' existe
#                     if 'Datetime' in columns_config:
#                         column_defs.append('PRIMARY KEY ("Datetime")')
                    
#                     create_query = f"""
#                         CREATE TABLE "{table_name}" (
#                             {', '.join(column_defs)}
#                         )
#                     """
#                     print(f"Executing table creation query for '{table_name}':\n{create_query}")
#                     cursor.execute(create_query)
#                     print(f"Table '{table_name}' créée avec succès")
#                 else:
#                     print(f"Table '{table_name}' existe déjà")
#                 return True
#     except Exception as e:
#         print(f"Erreur lors de la création de la table '{table_name}': {e}")
#         traceback.print_exc()
#         return False
    
    
# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before') -> bool:
#     """
#     Sauvegarde un DataFrame dans la base de données, avec vérifications complètes et journalisation détaillée.
#     """
#     conn = None 
#     try:
#         # Vérification initiale
#         if df.empty:
#             print(f"Warning: DataFrame vide reçu pour la station '{station}' ({processing_type}), aucune donnée à sauvegarder.")
#             return True

#         station = station.strip()
#         table_name = station
#         db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')

#         if not db_name:
#             raise ValueError(f"Nom de la base de données non configuré pour processing_type '{processing_type}'. Vérifiez .env")

#         columns_config = get_station_columns(station, processing_type)
#         if not columns_config:
#             raise ValueError(f"Configuration des colonnes manquante pour la station '{station}' ({processing_type}). Impossible de sauvegarder.")

#         expected_columns = list(columns_config.keys())

#         # ========== VÉRIFICATION 1: NOMS DE COLONNES ==========
#         print("\n" + "="*50)
#         print("VÉRIFICATION DES NOMS DE COLONNES (CASSE ET CORRESPONDANCE)")
#         print("="*50)
        
#         # Normalisation des noms de colonnes
#         column_mapping = {col.lower(): col for col in expected_columns}
#         new_df_columns = []
#         for col_df in df.columns:
#             if col_df in expected_columns:
#                 new_df_columns.append(col_df)
#             elif col_df.lower() in column_mapping:
#                 new_df_columns.append(column_mapping[col_df.lower()])
#             else:
#                 new_df_columns.append(col_df)
        
#         df.columns = new_df_columns

#         # Journalisation des résultats
#         df_cols_after_norm = df.columns.tolist()
#         missing_in_df = set(expected_columns) - set(df_cols_after_norm)
#         extra_in_df = set(df_cols_after_norm) - set(expected_columns)
        
#         print(f"\nColonnes attendues par la DB: {expected_columns}")
#         print(f"Colonnes du DataFrame (après normalisation): {df_cols_after_norm}")

#         if missing_in_df:
#             print(f"\n⚠️ ATTENTION: Colonnes attendues mais absentes du DataFrame:")
#             for col in missing_in_df:
#                 print(f"- {col}")
#                 df[col] = None  # Ajout des colonnes manquantes avec valeurs NULL
        
#         if extra_in_df:
#             print(f"\nℹ️ INFO: Colonnes supplémentaires dans le DataFrame:")
#             for col in extra_in_df:
#                 print(f"- {col}")
#             df = df.drop(columns=list(extra_in_df))

#         if not missing_in_df and not extra_in_df and set(expected_columns) == set(df.columns.tolist()):
#             print("\n✅ Tous les noms de colonnes correspondent exactement")
#         else:
#             print("\n❌ Des divergences de noms de colonnes existent")

#         # Réordonnancement des colonnes
#         df = df[expected_columns]
#         print("\nOrdre final des colonnes:", df.columns.tolist())
#         print("="*50 + "\n")

#         # ========== VÉRIFICATION 2: TYPES DE DONNÉES ==========
#         print("\n" + "="*50)
#         print("VÉRIFICATION DES TYPES DE DONNÉES ET VALEURS ANORMALES")
#         print("="*50)
        
#         nan_value_strings = ['NaN', 'NAN', 'nan', '', ' ', '#VALUE!', '-', 'NULL', 'null', 'N/A', 'NA', 'None', 'NONE', 'NV', 'N.V.', '#DIV/0!']
        
#         for col, pg_type_str in columns_config.items():
#             if col not in df.columns:
#                 print(f"\n⚠️ Colonne '{col}' non trouvée - ignorée")
#                 continue

#             original_dtype = str(df[col].dtype)
#             pg_base_type = pg_type_str.split()[0].lower()
            
#             print(f"\nColonne: {col}")
#             print(f"- Type Pandas original: {original_dtype}")
#             print(f"- Type PostgreSQL attendu: {pg_base_type}")
            
#             try:
#                 # Nettoyage des valeurs
#                 df[col] = df[col].replace(nan_value_strings, np.nan)
                
#                 # Conversion des types
#                 if pg_base_type in ['timestamp', 'date']:
#                     df[col] = pd.to_datetime(df[col], errors='coerce')
#                     print("- Converti en datetime")
#                 elif pg_base_type in ['float', 'integer']:
#                     df[col] = pd.to_numeric(df[col], errors='coerce')
#                     print("- Converti en numérique")
                
#                 # Statistiques après conversion
#                 null_count = df[col].isna().sum()
#                 print(f"- Valeurs NULL après conversion: {null_count}/{len(df)} ({null_count/len(df)*100:.2f}%)")
                
#                 if null_count > 0:
#                     sample_null = df[df[col].isna()].head(3)
#                     print("- Exemple de lignes avec NULL:", sample_null[[col]].to_string())
                    
#             except Exception as e:
#                 print(f"\n❌ ERREUR DE CONVERSION pour '{col}': {str(e)}")
#                 problematic_vals = df[col][~df[col].isna() & pd.to_numeric(df[col], errors='coerce').isna()]
#                 if not problematic_vals.empty:
#                     print("- Valeurs problématiques:", problematic_vals.unique()[:5])
#                 traceback.print_exc()

#         print("\nRésumé des types après conversion:")
#         print(df.dtypes)
#         print("="*50 + "\n")

#         # ========== PRÉPARATION DES DONNÉES ==========
#         print("\n" + "="*50)
#         print("PRÉPARATION DES DONNÉES POUR INSERTION")
#         print("="*50)
        
#         data_to_insert = []
#         type_converters = []
        
#         # Création des convertisseurs de type
#         for col in expected_columns:
#             pg_type_str = columns_config.get(col, 'text')
#             pg_base_type = pg_type_str.split()[0].lower()

#             if pg_base_type == 'timestamp':
#                 type_converters.append(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None)
#             elif pg_base_type == 'date':
#                 type_converters.append(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
#             elif pg_base_type == 'float':
#                 type_converters.append(lambda x: float(x) if pd.notna(x) else None)
#             elif pg_base_type == 'integer':
#                 type_converters.append(lambda x: int(x) if pd.notna(x) and x == int(x) else None)
#             else:
#                 type_converters.append(lambda x: str(x) if pd.notna(x) else None)

#         # Conversion des lignes
#         for idx, row in df.iterrows():
#             try:
#                 row_values = [type_converters[i](row[col]) for i, col in enumerate(expected_columns)]
#                 data_to_insert.append(tuple(row_values))
                
#                 # Journalisation pour les premières lignes
#                 if idx < 2:
#                     print(f"\nExemple ligne {idx}:")
#                     for i, col in enumerate(expected_columns):
#                         val = row_values[i]
#                         print(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                        
#             except Exception as e:
#                 print(f"\n❌ ERREUR ligne {idx}: {str(e)}")
#                 print("Valeurs:", row.to_dict())
#                 traceback.print_exc()
#                 continue

#         print(f"\nTotal lignes préparées: {len(data_to_insert)}")
#         print("="*50 + "\n")

#         # ========== CONNEXION À LA BASE ==========
#         conn = get_connection(db_name)
#         if not conn:
#             raise ConnectionError(f"Impossible de se connecter à la base '{db_name}'")

#         with conn.cursor() as cursor:
#             # Vérification de l'existence de la table
#             cursor.execute("""
#                 SELECT EXISTS (
#                     SELECT FROM information_schema.tables
#                     WHERE table_schema = 'public' AND table_name = %s
#                 )
#             """, (table_name,))
#             table_exists = cursor.fetchone()[0]
            
#             if not table_exists:
#                 print(f"\nℹ️ La table '{table_name}' n'existe pas, création...")
#                 if not create_station_table(station, processing_type):
#                     raise Exception(f"Échec de la création de la table '{table_name}'")
#                 print("✅ Table créée avec succès")

#             # ========== NOUVELLE VÉRIFICATION ROBUSTE DE CLÉ PRIMAIRE ==========
#             pk_col = None
#             try:
#                 # Méthode 1: Utilisation de information_schema (plus fiable)
#                 cursor.execute("""
#                     SELECT kcu.column_name
#                     FROM information_schema.table_constraints tc
#                     JOIN information_schema.key_column_usage kcu
#                         ON tc.constraint_name = kcu.constraint_name
#                         AND tc.table_schema = kcu.table_schema
#                     WHERE tc.table_name = %s
#                     AND tc.constraint_type = 'PRIMARY KEY'
#                     LIMIT 1
#                 """, (table_name,))
#                 pk_info = cursor.fetchone()
                
#                 # Méthode 2: Fallback avec pg_index (pour les cas complexes)
#                 if not pk_info:
#                     print("ℹ️ Essai méthode alternative de détection de clé primaire...")
#                     try:
#                         cursor.execute(f"""
#                             SELECT a.attname
#                             FROM pg_index i
#                             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
#                             WHERE i.indrelid = %s::regclass AND i.indisprimary
#                         """, (f'"{table_name}"',))
#                         pk_info = cursor.fetchone()
#                     except Exception as e:
#                         print(f"⚠️ Échec méthode alternative: {str(e)}")
#                         pk_info = None
                
#                 if pk_info:
#                     pk_col = pk_info[0]
#                     print(f"✅ Clé primaire détectée: {pk_col}")
#                 else:
#                     print("ℹ️ Aucune clé primaire détectée")
                    
#             except Exception as e:
#                 print(f"⚠️ Erreur lors de la détection de la clé primaire: {str(e)}")
#                 if 'Datetime' in expected_columns:
#                     pk_col = 'Datetime'
#                     print("ℹ️ Utilisation de 'Datetime' comme clé primaire par défaut")
#                 traceback.print_exc()

#             # Construction de la requête SQL
#             cols_sql = ', '.join([f'"{col}"' for col in expected_columns])
#             placeholders = ', '.join(['%s'] * len(expected_columns))
            
#             if pk_col:
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders})
#                     ON CONFLICT ("{pk_col}") DO NOTHING
#                 """
#                 print("ℹ️ Utilisation de ON CONFLICT avec la clé primaire")
#             else:
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders})
#                 """
#                 print("ℹ️ Insertion simple (aucune clé primaire détectée)")

#             print("\nRequête SQL générée:")
#             print(query)

#             # ========== VÉRIFICATION FINALE ==========
#             print("\n" + "="*50)
#             print("VÉRIFICATION FINALE AVANT INSERTION")
#             print("="*50)
            
#             print(f"\nNombre de colonnes attendues: {len(expected_columns)}")
#             print(f"Nombre de colonnes préparées: {len(df.columns)}")
            
#             if len(expected_columns) != len(df.columns):
#                 print("\n❌ ERREUR: Nombre de colonnes incompatible!")
#                 print("Colonnes attendues vs préparées:")
#                 for exp, act in zip(expected_columns, df.columns):
#                     print(f"- {exp} => {act}")

#             if data_to_insert:
#                 first_row = data_to_insert[0]
#                 print(f"\nPremière ligne de données ({len(first_row)} valeurs):")
#                 for val, col in zip(first_row, expected_columns):
#                     print(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                
#                 # Test de mogrify
#                 try:
#                     mogrified = cursor.mogrify(query, first_row).decode('utf-8')
#                     print("\nTest mogrify réussi:")
#                     print(mogrified[:500] + ("..." if len(mogrified) > 500 else ""))
#                 except Exception as e:
#                     print("\n❌ ERREUR mogrify:", str(e))
#                     traceback.print_exc()
#                     raise
#             else:
#                 print("\n⚠️ Aucune donnée à insérer!")
#                 return False

#             print("="*50 + "\n")

#             # ========== INSERTION ==========
#             batch_size = 1000
#             inserted_rows = 0
#             start_time = datetime.now()
            
#             print(f"\nDébut de l'insertion de {len(data_to_insert)} lignes par lots de {batch_size}...")
            
#             for i in range(0, len(data_to_insert), batch_size):
#                 batch = data_to_insert[i:i + batch_size]
#                 try:
#                     extras.execute_batch(cursor, query, batch)
#                     conn.commit()
#                     inserted_rows += len(batch)
                    
#                     # Journalisation périodique
#                     if (i // batch_size) % 10 == 0:
#                         elapsed = (datetime.now() - start_time).total_seconds()
#                         rate = inserted_rows / elapsed if elapsed > 0 else 0
#                         print(f"Lot {i//batch_size + 1}: {inserted_rows} lignes ({rate:.1f} lignes/sec)")
                        
#                 except Exception as e:
#                     conn.rollback()
#                     print(f"\n❌ ERREUR lot {i//batch_size + 1}: {str(e)}")
#                     if batch:
#                         print("Exemple de ligne problématique:", batch[0])
#                     traceback.print_exc()
#                     raise

#             total_time = (datetime.now() - start_time).total_seconds()
#             print(f"\n✅ Insertion terminée: {inserted_rows} lignes en {total_time:.2f} secondes ({inserted_rows/max(total_time,1):.1f} lignes/sec)")
#             return True

#     except Exception as e:
#         print(f"\n❌❌❌ ERREUR CRITIQUE: {str(e)}")
#         traceback.print_exc()
        
#         if 'df' in locals():
#             print("\nÉtat du DataFrame au moment de l'erreur:")
#             print(f"Shape: {df.shape}")
#             print("5 premières lignes:")
#             print(df.head().to_string())
            
#         return False
        
#     finally:
#         if conn:
#             conn.close()
#             print("\nConnexion à la base de données fermée")


####################### Code remplacement de la colonne Rel_H_%


def create_station_table(station: str, processing_type: str = 'before'):
    """
    Crée la table dans la base de données si elle n'existe pas,
    avec une clé primaire sur 'Datetime' si présente.
    """
    # Ici, nous utilisons directement le nom de la station fourni comme nom de table,
    # car vous souhaitez conserver les noms d'origine (y compris accents et espaces).
    # PostgreSQL gérera cela avec les guillemets doubles.
    table_name = station.strip()

    # Le columns_config doit déjà contenir le nom de colonne 'Rel_H_Pct'
    columns_config = get_station_columns(station, processing_type)
    if not columns_config:
        print(f"Error: Configuration des colonnes manquante pour {station}. Impossible de créer la table.")
        return False
    
    db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')

    try:
        with get_connection(db_name) as conn:
            if not conn:
                return False
            conn.autocommit = True
            with conn.cursor() as cursor:
                # Vérifier si la table existe
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' AND table_name = %s
                    )
                """, (table_name,))
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    column_defs = []
                    # Construire les définitions de colonnes avec les noms de colonnes issus de columns_config
                    # qui doit maintenant inclure 'Rel_H_Pct'.
                    for col, dtype in columns_config.items():
                        pg_type = get_pg_type(dtype)
                        column_defs.append(f'"{col}" {pg_type}')
                    
                    # Ajouter la clé primaire si la colonne 'Datetime' existe
                    if 'Datetime' in columns_config:
                        column_defs.append('PRIMARY KEY ("Datetime")')
                    
                    create_query = f"""
                        CREATE TABLE "{table_name}" (
                            {', '.join(column_defs)}
                        )
                    """
                    print(f"\nℹ️ La table '{table_name}' n'existe pas, création...")
                    print(f"Executing table creation query for '{table_name}':\n{create_query}")
                    cursor.execute(create_query)
                    print(f"Table '{table_name}' créée avec succès")
                else:
                    print(f"Table '{table_name}' existe déjà")
                return True
    except Exception as e:
        print(f"Erreur lors de la création de la table '{table_name}': {e}")
        traceback.print_exc()
        return False
    
    
# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before') -> bool:
#     """
#     Sauvegarde un DataFrame dans la base de données, avec vérifications complètes et journalisation détaillée.
#     """
#     conn = None 
#     try:
#         # Vérification initiale
#         if df.empty:
#             print(f"Warning: DataFrame vide reçu pour la station '{station}' ({processing_type}), aucune donnée à sauvegarder.")
#             return True

#         # Le nom de la table dans la DB sera exactement le nom de la station
#         table_name = station.strip()

#         db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')

#         if not db_name:
#             raise ValueError(f"Nom de la base de données non configuré pour processing_type '{processing_type}'. Vérifiez .env")

#         # Récupérez la configuration des colonnes attendues (doit déjà inclure 'Rel_H_Pct')
#         columns_config = get_station_columns(station, processing_type)
#         if not columns_config:
#             raise ValueError(f"Configuration des colonnes manquante pour la station '{station}' ({processing_type}). Impossible de sauvegarder.")

#         expected_columns = list(columns_config.keys()) # Ce sont les noms propres attendus (ex: 'Rel_H_Pct')

#         # --- DÉBUT DE LA CORRECTION : Renommage Spécifique de la Colonne `Rel_H_%` dans le DataFrame ---
#         # Cette étape garantit que le DataFrame a le nom de colonne "propre" ('Rel_H_Pct')
#         # avant toute autre comparaison ou construction de requête SQL.
#         df_column_rename_map = {}
#         for df_col in df.columns:
#             # Vérifiez si la colonne du DataFrame est 'Rel_H_%' ou une variation (insensible à la casse si besoin)
#             if df_col == 'Rel_H_%': # Correspondance exacte avec le problème initial
#                 df_column_rename_map[df_col] = 'Rel_H_Pct'
#             elif df_col.lower() == 'rel_h': # Si elle arrive parfois sous cette forme
#                  df_column_rename_map[df_col] = 'Rel_H_Pct'
#             # ATTENTION : Si d'autres colonnes de vos CSV contiennent des caractères spéciaux problématiques pour mogrify,
#             # vous devrez ajouter des règles de renommage ici également.
#             # Cependant, basé sur nos discussions, seul 'Rel_H_%' était le coupable.

#         if df_column_rename_map:
#             df = df.rename(columns=df_column_rename_map)
#             print(f"DEBUG: Colonnes du DataFrame renommées: {df_column_rename_map}")
#         # --- FIN DE LA CORRECTION ---


#         # ========== VÉRIFICATION 1: NOMS DE COLONNES ==========
#         print("\n" + "="*50)
#         print("VÉRIFICATION DES NOMS DE COLONNES (CASSE ET CORRESPONDANCE)")
#         print("="*50)
        
#         # Normalisation des noms de colonnes du DataFrame (après le renommage explicite ci-dessus)
#         # La logique de normalisation de casse reste utile pour d'autres colonnes.
#         column_mapping_expected = {col.lower(): col for col in expected_columns}
#         new_df_columns = []
#         for col_df in df.columns:
#             if col_df in expected_columns: # Le nom est déjà parfait (ex: 'Rel_H_Pct')
#                 new_df_columns.append(col_df)
#             elif col_df.lower() in column_mapping_expected: # Gérer les différences de casse
#                 new_df_columns.append(column_mapping_expected[col_df.lower()])
#             else: # Conserver les colonnes non reconnues pour l'instant (seront supprimées plus tard si extra)
#                 new_df_columns.append(col_df)
        
#         df.columns = new_df_columns

#         # Journalisation des résultats
#         df_cols_after_norm = df.columns.tolist()
#         missing_in_df = set(expected_columns) - set(df_cols_after_norm)
#         extra_in_df = set(df_cols_after_norm) - set(expected_columns)
        
#         print(f"\nColonnes attendues par la DB: {expected_columns}")
#         print(f"Colonnes du DataFrame (après normalisation): {df_cols_after_norm}")

#         if missing_in_df:
#             print(f"\n⚠️ ATTENTION: Colonnes attendues mais absentes du DataFrame:")
#             for col in missing_in_df:
#                 print(f"- {col}")
#                 df[col] = None  # Ajout des colonnes manquantes avec valeurs NULL
        
#         if extra_in_df:
#             print(f"\nℹ️ INFO: Colonnes supplémentaires dans le DataFrame:")
#             for col in extra_in_df:
#                 print(f"- {col}")
#             df = df.drop(columns=list(extra_in_df))

#         # Vérification finale de correspondance (après le renommage initial et la normalisation)
#         if not missing_in_df and not extra_in_df and set(expected_columns) == set(df.columns.tolist()):
#             print("\n✅ Tous les noms de colonnes correspondent exactement")
#         else:
#             print("\n❌ Des divergences de noms de colonnes existent")

#         # Réordonnancement des colonnes pour correspondre à l'ordre attendu par la DB
#         df = df[expected_columns]
#         print("\nOrdre final des colonnes:", df.columns.tolist())
#         print("="*50 + "\n")

#         # ========== VÉRIFICATION 2: TYPES DE DONNÉES ==========
#         print("\n" + "="*50)
#         print("VÉRIFICATION DES TYPES DE DONNÉES ET VALEURS ANORMALES")
#         print("="*50)
        
#         nan_value_strings = ['NaN', 'NAN', 'nan', '', ' ', '#VALUE!', '-', 'NULL', 'null', 'N/A', 'NA', 'None', 'NONE', 'NV', 'N.V.', '#DIV/0!']
        
#         for col, pg_type_str in columns_config.items():
#             if col not in df.columns:
#                 print(f"\n⚠️ Colonne '{col}' non trouvée - ignorée")
#                 continue

#             original_dtype = str(df[col].dtype)
#             pg_base_type = pg_type_str.split()[0].lower()
            
#             print(f"\nColonne: {col}")
#             print(f"- Type Pandas original: {original_dtype}")
#             print(f"- Type PostgreSQL attendu: {pg_base_type}")
            
#             try:
#                 # Nettoyage des valeurs
#                 df[col] = df[col].replace(nan_value_strings, np.nan)
                
#                 # Conversion des types
#                 if pg_base_type in ['timestamp', 'date']:
#                     df[col] = pd.to_datetime(df[col], errors='coerce')
#                     print("- Converti en datetime")
#                 elif pg_base_type in ['float', 'integer']:
#                     df[col] = pd.to_numeric(df[col], errors='coerce')
#                     print("- Converti en numérique")
                
#                 # Statistiques après conversion
#                 null_count = df[col].isna().sum()
#                 print(f"- Valeurs NULL après conversion: {null_count}/{len(df)} ({null_count/len(df)*100:.2f}%)")
                
#                 if null_count > 0:
#                     sample_null = df[df[col].isna()].head(3)
#                     print("- Exemple de lignes avec NULL:", sample_null[[col]].to_string())
                    
#             except Exception as e:
#                 print(f"\n❌ ERREUR DE CONVERSION pour '{col}': {str(e)}")
#                 problematic_vals = df[col][~df[col].isna() & pd.to_numeric(df[col], errors='coerce').isna()]
#                 if not problematic_vals.empty:
#                     print("- Valeurs problématiques:", problematic_vals.unique()[:5])
#                 traceback.print_exc()

#         print("\nRésumé des types après conversion:")
#         print(df.dtypes)
#         print("="*50 + "\n")

#         # ========== PRÉPARATION DES DONNÉES ==========
#         print("\n" + "="*50)
#         print("PRÉPARATION DES DONNÉES POUR INSERTION")
#         print("="*50)
        
#         data_to_insert = []
#         type_converters = []
        
#         # Création des convertisseurs de type
#         for col in expected_columns: # Utilise les expected_columns (qui incluent 'Rel_H_Pct')
#             pg_type_str = columns_config.get(col, 'text')
#             pg_base_type = pg_type_str.split()[0].lower()

#             if pg_base_type == 'timestamp':
#                 #type_converters.append(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None)
#                 type_converters.append(lambda x: x.strftime('%Y-%m-%d %H:%M') if pd.notna(x) else None)
#             elif pg_base_type == 'date':
#                 type_converters.append(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
#             elif pg_base_type == 'float':
#                 type_converters.append(lambda x: float(x) if pd.notna(x) else None)
#             elif pg_base_type == 'integer':
#                 type_converters.append(lambda x: int(x) if pd.notna(x) and x == int(x) else None)
#             else:
#                 type_converters.append(lambda x: str(x) if pd.notna(x) else None)

#         # Conversion des lignes
#         for idx, row in df.iterrows():
#             try:
#                 row_values = [type_converters[i](row[col]) for i, col in enumerate(expected_columns)]
#                 data_to_insert.append(tuple(row_values))
                
#                 # Journalisation pour les premières lignes
#                 if idx < 2:
#                     print(f"\nExemple ligne {idx}:")
#                     for i, col in enumerate(expected_columns):
#                         val = row_values[i]
#                         print(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                        
#             except Exception as e:
#                 print(f"\n❌ ERREUR ligne {idx}: {str(e)}")
#                 print("Valeurs:", row.to_dict())
#                 traceback.print_exc()
#                 continue

#         print(f"\nTotal lignes préparées: {len(data_to_insert)}")
#         print("="*50 + "\n")

#         # ========== CONNEXION À LA BASE ==========
#         conn = get_connection(db_name)
#         if not conn:
#             raise ConnectionError(f"Impossible de se connecter à la base '{db_name}'")

#         with conn.cursor() as cursor:
#             # Vérification de l'existence de la table (avec le nom de table original)
#             cursor.execute("""
#                 SELECT EXISTS (
#                     SELECT FROM information_schema.tables
#                     WHERE table_schema = 'public' AND table_name = %s
#                 )
#             """, (table_name,))
#             table_exists = cursor.fetchone()[0]
            
#             if not table_exists:
#                 print(f"\nℹ️ La table '{table_name}' n'existe pas, création...")
#                 # Appel de create_station_table avec le nom de station original
#                 if not create_station_table(station, processing_type):
#                     raise Exception(f"Échec de la création de la table '{table_name}'")
#                 print("✅ Table créée avec succès")

#             # ========== NOUVELLE VÉRIFICATION ROBUSTE DE CLÉ PRIMAIRE ==========
#             pk_col = None
#             try:
#                 # Méthode 1: Utilisation de information_schema (plus fiable)
#                 cursor.execute("""
#                     SELECT kcu.column_name
#                     FROM information_schema.table_constraints tc
#                     JOIN information_schema.key_column_usage kcu
#                         ON tc.constraint_name = kcu.constraint_name
#                         AND tc.table_schema = kcu.table_schema
#                     WHERE tc.table_name = %s
#                     AND tc.constraint_type = 'PRIMARY KEY'
#                     LIMIT 1
#                 """, (table_name,))
#                 pk_info = cursor.fetchone()
                
#                 # Méthode 2: Fallback avec pg_index (pour les cas complexes)
#                 if not pk_info:
#                     print("ℹ️ Essai méthode alternative de détection de clé primaire...")
#                     try:
#                         cursor.execute(f"""
#                             SELECT a.attname
#                             FROM pg_index i
#                             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
#                             WHERE i.indrelid = %s::regclass AND i.indisprimary
#                         """, (f'"{table_name}"',))
#                         pk_info = cursor.fetchone()
#                     except Exception as e:
#                         print(f"⚠️ Échec méthode alternative: {str(e)}")
#                         pk_info = None
                
#                 if pk_info:
#                     pk_col = pk_info[0]
#                     print(f"✅ Clé primaire détectée: {pk_col}")
#                 else:
#                     print("ℹ️ Aucune clé primaire détectée")
                    
#             except Exception as e:
#                 print(f"⚠️ Erreur lors de la détection de la clé primaire: {str(e)}")
#                 if 'Datetime' in expected_columns:
#                     pk_col = 'Datetime'
#                     print("ℹ️ Utilisation de 'Datetime' comme clé primaire par défaut")
#                 traceback.print_exc()

#             # Construction de la requête SQL (utilisant le nom de table original et les noms de colonnes attendus)
#             cols_sql = ', '.join([f'"{col}"' for col in expected_columns])
#             placeholders = ', '.join(['%s'] * len(expected_columns))
            
#             if pk_col:
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders})
#                     ON CONFLICT ("{pk_col}") DO NOTHING
#                 """
#                 print("ℹ️ Utilisation de ON CONFLICT avec la clé primaire")
#             else:
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders})
#                 """
#                 print("ℹ️ Insertion simple (aucune clé primaire détectée)")

#             print("\nRequête SQL générée:")
#             print(query)

#             # ========== VÉRIFICATION FINALE AVANT INSERTION ==========
#             print("\n" + "="*50)
#             print("VÉRIFICATION FINALE AVANT INSERTION")
#             print("="*50)
            
#             print(f"\nNombre de colonnes attendues: {len(expected_columns)}")
#             print(f"Nombre de colonnes préparées (dans DataFrame): {len(df.columns)}") # Vérification supplémentaire
            
#             if len(expected_columns) != len(df.columns):
#                 print("\n❌ ERREUR: Nombre de colonnes incompatible entre expected_columns et le DataFrame final!")
#                 print("Colonnes attendues:", expected_columns)
#                 print("Colonnes DataFrame:", df.columns.tolist())
#                 raise ValueError("Incompatibilité de colonnes après préparation") # Relancer pour un échec clair

#             if data_to_insert:
#                 first_row = data_to_insert[0]
#                 print(f"\nPremière ligne de données ({len(first_row)} valeurs):")
#                 for val, col in zip(first_row, expected_columns):
#                     print(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                
#                 # Test de mogrify (AVEC LE NOM DE COLONNE CORRIGÉ)
#                 try:
#                     mogrified = cursor.mogrify(query, first_row).decode('utf-8')
#                     print("\n✅ Test mogrify réussi:")
#                     print(mogrified[:500] + ("..." if len(mogrified) > 500 else ""))
#                 except Exception as e:
#                     print("\n❌ ERREUR mogrify:", str(e))
#                     traceback.print_exc()
#                     raise # Relancer l'erreur pour arrêter le processus si mogrify échoue
#             else:
#                 print("\n⚠️ Aucune donnée à insérer!")
#                 return False

#             print("="*50 + "\n")

#             # ========== INSERTION ==========
#             batch_size = 10000
#             inserted_rows = 0
#             start_time = datetime.now()
            
#             print(f"\nDébut de l'insertion de {len(data_to_insert)} lignes par lots de {batch_size}...")
            
#             for i in range(0, len(data_to_insert), batch_size):
#                 batch = data_to_insert[i:i + batch_size]
#                 try:
#                     extras.execute_batch(cursor, query, batch)
#                     conn.commit()
#                     inserted_rows += len(batch)
                    
#                     # Journalisation périodique
#                     if (i // batch_size) % 10 == 0:
#                         elapsed = (datetime.now() - start_time).total_seconds()
#                         rate = inserted_rows / elapsed if elapsed > 0 else 0
#                         print(f"Lot {i//batch_size + 1}: {inserted_rows} lignes ({rate:.1f} lignes/sec)")
                        
#                 except Exception as e:
#                     conn.rollback()
#                     print(f"\n❌ ERREUR lot {i//batch_size + 1}: {str(e)}")
#                     if batch:
#                         print("Exemple de ligne problématique:", batch[0])
#                     traceback.print_exc()
#                     raise # Relancer l'erreur pour que la fonction appelante sache qu'il y a eu un échec

#             total_time = (datetime.now() - start_time).total_seconds()
#             print(f"\n✅ Insertion terminée: {inserted_rows} lignes en {total_time:.2f} secondes ({inserted_rows/max(total_time,1):.1f} lignes/sec)")
#             return True

#     except Exception as e:
#         print(f"\n❌❌❌ ERREUR CRITIQUE: {str(e)}")
#         traceback.print_exc()
        
#         if 'df' in locals():
#             print("\nÉtat du DataFrame au moment de l'erreur:")
#             print(f"Shape: {df.shape}")
#             print("5 premières lignes:")
#             print(df.head().to_string())
            
#         return False
        
#     finally:
#         if conn:
#             conn.close()
#             print("\nConnexion à la base de données fermée")


def save_to_database(df: pd.DataFrame, station: str, conn, processing_type: str = 'before') -> bool:
    """
    Sauvegarde un DataFrame dans la base de données, avec vérifications complètes et journalisation détaillée.
    La connexion à la base de données est passée en argument.
    """
    try:
        # Vérification initiale
        if df.empty:
            print(f"Warning: DataFrame vide reçu pour la station '{station}' ({processing_type}), aucune donnée à sauvegarder.")
            return True

        # Le nom de la table dans la DB sera exactement le nom de la station
        table_name = station.strip()

        db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')

        # La connexion est maintenant passée en argument, donc plus besoin de la créer ici
        if not conn: # Vérification au cas où la connexion passée est nulle
            raise ConnectionError(f"Connexion à la base de données non fournie ou nulle pour '{db_name}'")

        # Récupérez la configuration des colonnes attendues (doit déjà inclure 'Rel_H_Pct')
        columns_config = get_station_columns(station, processing_type)
        if not columns_config:
            raise ValueError(f"Configuration des colonnes manquante pour la station '{station}' ({processing_type}). Impossible de sauvegarder.")

        expected_columns = list(columns_config.keys())

        # --- DÉBUT DE LA CORRECTION : Renommage Spécifique de la Colonne `Rel_H_%` dans le DataFrame ---
        df_column_rename_map = {}
        for df_col in df.columns:
            if df_col == 'Rel_H_%':
                df_column_rename_map[df_col] = 'Rel_H_Pct'
            elif df_col.lower() == 'rel_h': 
                 df_column_rename_map[df_col] = 'Rel_H_Pct'

        if df_column_rename_map:
            df = df.rename(columns=df_column_rename_map)
            print(f"DEBUG: Colonnes du DataFrame renommées: {df_column_rename_map}")
        # --- FIN DE LA CORRECTION ---


        # ========== VÉRIFICATION 1: NOMS DE COLONNES ==========
        print("\n" + "="*50)
        print("VÉRIFICATION DES NOMS DE COLONNES (CASSE ET CORRESPONDANCE)")
        print("="*50)
        
        column_mapping_expected = {col.lower(): col for col in expected_columns}
        new_df_columns = []
        for col_df in df.columns:
            if col_df in expected_columns: 
                new_df_columns.append(col_df)
            elif col_df.lower() in column_mapping_expected: 
                new_df_columns.append(column_mapping_expected[col_df.lower()])
            else: 
                new_df_columns.append(col_df)
        
        df.columns = new_df_columns

        df_cols_after_norm = df.columns.tolist()
        missing_in_df = set(expected_columns) - set(df_cols_after_norm)
        extra_in_df = set(df_cols_after_norm) - set(expected_columns)
        
        print(f"\nColonnes attendues par la DB: {expected_columns}")
        print(f"Colonnes du DataFrame (après normalisation): {df_cols_after_norm}")

        if missing_in_df:
            print(f"\n⚠️ ATTENTION: Colonnes attendues mais absentes du DataFrame:")
            for col in missing_in_df:
                print(f"- {col}")
                df[col] = None 
        
        if extra_in_df:
            print(f"\nℹ️ INFO: Colonnes supplémentaires dans le DataFrame:")
            for col in extra_in_df:
                print(f"- {col}")
            df = df.drop(columns=list(extra_in_df))

        if not missing_in_df and not extra_in_df and set(expected_columns) == set(df.columns.tolist()):
            print("\n✅ Tous les noms de colonnes correspondent exactement")
        else:
            print("\n❌ Des divergences de noms de colonnes existent")

        df = df[expected_columns]
        print("\nOrdre final des colonnes:", df.columns.tolist())
        print("="*50 + "\n")

        # ========== VÉRIFICATION 2: TYPES DE DONNÉES ==========
        print("\n" + "="*50)
        print("VÉRIFICATION DES TYPES DE DONNÉES ET VALEURS ANORMALES")
        print("="*50)
        
        nan_value_strings = ['NaN', 'NAN', 'nan', '', ' ', '#VALUE!', '-', 'NULL', 'null', 'N/A', 'NA', 'None', 'NONE', 'NV', 'N.V.', '#DIV/0!']
        
        for col, pg_type_str in columns_config.items():
            if col not in df.columns:
                print(f"\n⚠️ Colonne '{col}' non trouvée - ignorée")
                continue

            original_dtype = str(df[col].dtype)
            pg_base_type = pg_type_str.split()[0].lower()
            
            print(f"\nColonne: {col}")
            print(f"- Type Pandas original: {original_dtype}")
            print(f"- Type PostgreSQL attendu: {pg_base_type}")
            
            try:
                df[col] = df[col].replace(nan_value_strings, np.nan)
                
                if pg_base_type in ['timestamp', 'date']:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    print("- Converti en datetime")
                elif pg_base_type in ['float', 'integer']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    print("- Converti en numérique")
                
                null_count = df[col].isna().sum()
                print(f"- Valeurs NULL après conversion: {null_count}/{len(df)} ({null_count/len(df)*100:.2f}%)")
                
                if null_count > 0:
                    sample_null = df[df[col].isna()].head(3)
                    print("- Exemple de lignes avec NULL:", sample_null[[col]].to_string())
                    
            except Exception as e:
                print(f"\n❌ ERREUR DE CONVERSION pour '{col}': {str(e)}")
                problematic_vals = df[col][~df[col].isna() & pd.to_numeric(df[col], errors='coerce').isna()]
                if not problematic_vals.empty:
                    print("- Valeurs problématiques:", problematic_vals.unique()[:5])
                traceback.print_exc()

        print("\nRésumé des types après conversion:")
        print(df.dtypes)
        print("="*50 + "\n")

        # ========== PRÉPARATION DES DONNÉES ==========
        print("\n" + "="*50)
        print("PRÉPARATION DES DONNÉES POUR INSERTION")
        print("="*50)
        
        data_to_insert = []
        type_converters = []
        
        for col in expected_columns:
            pg_type_str = columns_config.get(col, 'text')
            pg_base_type = pg_type_str.split()[0].lower()

            if pg_base_type == 'timestamp':
                type_converters.append(lambda x: x.strftime('%Y-%m-%d %H:%M') if pd.notna(x) else None) # <-- MODIFIÉ ICI
            elif pg_base_type == 'date':
                type_converters.append(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
            elif pg_base_type == 'float':
                type_converters.append(lambda x: float(x) if pd.notna(x) else None)
            elif pg_base_type == 'integer':
                type_converters.append(lambda x: int(x) if pd.notna(x) and x == int(x) else None)
            else:
                type_converters.append(lambda x: str(x) if pd.notna(x) else None)

        for idx, row in df.iterrows():
            try:
                row_values = [type_converters[i](row[col]) for i, col in enumerate(expected_columns)]
                data_to_insert.append(tuple(row_values))
                
                if idx < 2:
                    print(f"\nExemple ligne {idx}:")
                    for i, col in enumerate(expected_columns):
                        val = row_values[i]
                        print(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                        
            except Exception as e:
                print(f"\n❌ ERREUR ligne {idx}: {str(e)}")
                print("Valeurs:", row.to_dict())
                traceback.print_exc()
                continue

        print(f"\nTotal lignes préparées: {len(data_to_insert)}")
        print("="*50 + "\n")

        # ========== CONNEXION À LA BASE ==========
        # La connexion est passée en argument `conn` ici.
        # Plus besoin de get_connection(db_name) ou de vérifier `if not conn:`
        # directement après un appel à get_connection().
        conn.autocommit = True # Assurez-vous que la connexion passée est en autocommit

        with conn.cursor() as cursor:
            # Vérification de l'existence de la table (avec le nom de table original)
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = %s
                )
            """, (table_name,))
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                print(f"\nℹ️ La table '{table_name}' n'existe pas, création...")
                # Appel de create_station_table avec le nom de station original
                # create_station_table doit pouvoir utiliser une connexion interne ou être adapté pour en prendre une
                # Pour l'instant, create_station_table garde son propre get_connection, ce qui est acceptable ici.
                if not create_station_table(station, processing_type): # <-- create_station_table n'a pas besoin de la connexion passée
                    raise Exception(f"Échec de la création de la table '{table_name}'")
                print("✅ Table créée avec succès")

            # ========== NOUVELLE VÉRIFICATION ROBUSTE DE CLÉ PRIMAIRE ==========
            pk_col = None
            try:
                cursor.execute("""
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    WHERE tc.table_name = %s
                    AND tc.constraint_type = 'PRIMARY KEY'
                    LIMIT 1
                """, (table_name,))
                pk_info = cursor.fetchone()
                
                if not pk_info:
                    print("ℹ️ Essai méthode alternative de détection de clé primaire...")
                    try:
                        cursor.execute(f"""
                            SELECT a.attname
                            FROM pg_index i
                            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                            WHERE i.indrelid = %s::regclass AND i.indisprimary
                        """, (f'"{table_name}"',))
                        pk_info = cursor.fetchone()
                    except Exception as e:
                        print(f"⚠️ Échec méthode alternative: {str(e)}")
                        pk_info = None
                
                if pk_info:
                    pk_col = pk_info[0]
                    print(f"✅ Clé primaire détectée: {pk_col}")
                else:
                    print("ℹ️ Aucune clé primaire détectée")
                    
            except Exception as e:
                print(f"⚠️ Erreur lors de la détection de la clé primaire: {str(e)}")
                if 'Datetime' in expected_columns:
                    pk_col = 'Datetime'
                    print("ℹ️ Utilisation de 'Datetime' comme clé primaire par défaut")
                traceback.print_exc()

            cols_sql = ', '.join([f'"{col}"' for col in expected_columns])
            placeholders = ', '.join(['%s'] * len(expected_columns))
            
            if pk_col:
                query = f"""
                    INSERT INTO "{table_name}" ({cols_sql})
                    VALUES ({placeholders})
                    ON CONFLICT ("{pk_col}") DO NOTHING
                """
                print("ℹ️ Utilisation de ON CONFLICT avec la clé primaire")
            else:
                query = f"""
                    INSERT INTO "{table_name}" ({cols_sql})
                    VALUES ({placeholders})
                """
                print("ℹ️ Insertion simple (aucune clé primaire détectée)")

            print("\nRequête SQL générée:")
            print(query)

            # ========== VÉRIFICATION FINALE AVANT INSERTION ==========
            print("\n" + "="*50)
            print("VÉRIFICATION FINALE AVANT INSERTION")
            print("="*50)
            
            print(f"\nNombre de colonnes attendues: {len(expected_columns)}")
            print(f"Nombre de colonnes préparées (dans DataFrame): {len(df.columns)}") 
            
            if len(expected_columns) != len(df.columns):
                print("\n❌ ERREUR: Nombre de colonnes incompatible entre expected_columns et le DataFrame final!")
                print("Colonnes attendues:", expected_columns)
                print("Colonnes DataFrame:", df.columns.tolist())
                raise ValueError("Incompatibilité de colonnes après préparation") 

            if data_to_insert:
                first_row = data_to_insert[0]
                print(f"\nPremière ligne de données ({len(first_row)} valeurs):")
                for val, col in zip(first_row, expected_columns):
                    print(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                
                try:
                    mogrified = cursor.mogrify(query, first_row).decode('utf-8')
                    print("\n✅ Test mogrify réussi:")
                    print(mogrified[:500] + ("..." if len(mogrified) > 500 else ""))
                except Exception as e:
                    print("\n❌ ERREUR mogrify:", str(e))
                    traceback.print_exc()
                    raise 
            else:
                print("\n⚠️ Aucune donnée à insérer!")
                return False

            print("="*50 + "\n")

            # ========== INSERTION ==========
            batch_size = 10000
            # inserted_rows = 0 # Nous ne compterons plus les tentatives d'insertion ici pour éviter la confusion
            start_time = datetime.now()
            
            print(f"\nDébut de l'insertion de {len(data_to_insert)} lignes par lots de {batch_size}...")
            
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i:i + batch_size]
                try:
                    extras.execute_batch(cursor, query, batch)
                    # La connexion sera commitée par Flask à la fin du traitement global du lot de fichiers
                    # ou automatiquement si autocommit est activé et que chaque requête est une transaction
                    # Sinon, il faudrait faire un conn.commit() ici après chaque batch
                    # Cependant, pour execute_batch, la documentation de psycopg2 recommande execute_batch
                    # avec un autocommit ou un commit manuel après tous les batchs.
                    # Pour être sûr que chaque lot est enregistré, ajoutons conn.commit()
                    conn.commit() # <-- Ajouté ici pour commiter chaque lot
                    
                    # inserted_rows += len(batch) # Non utilisé pour le compte final, peut être retiré
                    
                    # Journalisation périodique
                    if (i // batch_size) % 10 == 0:
                        elapsed = (datetime.now() - start_time).total_seconds()
                        # rate = inserted_rows / elapsed if elapsed > 0 else 0 # Retiré car inserted_rows ne reflète pas le réel
                        print(f"Lot {i//batch_size + 1}: {len(batch)} lignes traitées...")
                        
                except Exception as e:
                    conn.rollback()
                    print(f"\n❌ ERREUR lot {i//batch_size + 1}: {str(e)}")
                    if batch:
                        print("Exemple de ligne problématique:", batch[0])
                    traceback.print_exc()
                    raise 

            total_time = (datetime.now() - start_time).total_seconds()
            print(f"\n✅ Traitement d'insertion terminé pour '{station}': {len(data_to_insert)} lignes préparées en {total_time:.2f} secondes.")
            print("Note: Le nombre exact de lignes insérées peut différer en raison de la clause ON CONFLICT.")
            return True

    except Exception as e:
        print(f"\n❌❌❌ ERREUR CRITIQUE lors de la sauvegarde pour '{station}': {str(e)}")
        traceback.print_exc()
        
        if 'df' in locals():
            print("\nÉtat du DataFrame au moment de l'erreur:")
            print(f"Shape: {df.shape}")
            print("5 premières lignes:")
            print(df.head().to_string())
            
        return False