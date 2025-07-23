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

def get_station_columns(station: str, processing_type: str) -> Dict[str, str]:
    """Retourne les colonnes attendues pour une station donnée selon le type de traitement"""
    
    # Nettoyage du nom de la station
    station = station.strip()
    # BEFORE_PROCESSING
    if processing_type == 'before':
        # Bassin DANO
        if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
            return {
                "Datetime": 'timestamp PRIMARY KEY',
                'Year': 'integer',
                'Month': 'integer', 
                'Day': 'integer',
                'Hour': 'integer',
                'Minute': 'integer',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_%': 'float',
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float'
            }
        elif station in ['Lare', 'Tambiri 2']:
            return {
                'Datetime': 'timestamp PRIMARY KEY',
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
                'Datetime': 'timestamp PRIMARY KEY',
                'Year': 'integer',
                'Month': 'integer', 
                'Day': 'integer',
                'Hour': 'integer',
                'Minute': 'integer',
                'Rain_mm': 'float',
                'BP_mbar_Avg': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_%': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float'
            }
        # Bassin DASSARI
        elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
            return {
                'Datetime': 'timestamp PRIMARY KEY',
                'Year': 'integer',
                'Month': 'integer', 
                'Day': 'integer',
                'Hour': 'integer',
                'Minute': 'integer',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_%': 'float',
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float'
            }
        elif station == 'Ouriyori 1':
            return {
                'Datetime': 'timestamp PRIMARY KEY',
                'Year': 'integer',
                'Month': 'integer', 
                'Day': 'integer',
                'Hour': 'integer',
                'Minute': 'integer',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_%': 'float',
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float'
            }
        # Bassin VEA SISSILI
        elif station in ['Oualem', 'Nebou', 'Nabugubelle', 'Manyoro', 'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabisi']:
            return {
                'Datetime': 'timestamp PRIMARY KEY',
                'Date': 'date',
                'Rain_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_%': 'float',
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float',
                'BP_mbar_Avg': 'float'
            }
        elif station == 'Atampisi':
            return {
                'Datetime': 'timestamp PRIMARY KEY',
                'Date': 'date',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Rain_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_%': 'float',
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float'
            }
        elif station == 'Aniabisi':
            return {
                'Datetime': 'timestamp PRIMARY KEY',
                'Date': 'date',
                'Rain_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_%': 'float',
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float'
            }
    
    # AFTER_PROCESSING
    elif processing_type == 'after':
        # Bassin DANO
        if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
            return {
                'Datetime': 'timestamp PRIMARY KEY',
                'Year': 'integer',
                'Month': 'integer', 
                'Day': 'integer',
                'Hour': 'integer',
                'Minute': 'integer',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Rain_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_%': 'float',
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float',
                'sunrise_time_utc': 'timestamp',
                'sunset_time_utc': 'timestamp',
                'Is_Daylight': 'boolean',
                'Daylight_Duration': 'float'
            }
        elif station in ['Lare', 'Tambiri 2']:
            return {
                'Datetime': 'timestamp PRIMARY KEY',
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
                'Datetime': 'timestamp PRIMARY KEY',
                'Year': 'integer',
                'Month': 'integer', 
                'Day': 'integer',
                'Hour': 'integer',
                'Minute': 'integer',
                'Rain_mm': 'float',
                'BP_mbar_Avg': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_%': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float'
            }
        # Bassin DASSARI
        elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
            return {
                'Datetime': 'timestamp PRIMARY KEY',
                'Year': 'integer',
                'Month': 'integer', 
                'Day': 'integer',
                'Hour': 'integer',
                'Minute': 'integer',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Rain_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_%': 'float',
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
                'Datetime': 'timestamp PRIMARY KEY',
                'Year': 'integer',
                'Month': 'integer', 
                'Day': 'integer',
                'Hour': 'integer',
                'Minute': 'integer',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Rain_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_%': 'float',
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float',
                'sunrise_time_utc': 'timestamp',
                'sunset_time_utc': 'timestamp',
                'Is_Daylight': 'boolean',
                'Daylight_Duration': 'float'
            }
        # Bassin VEA SISSILI
        elif station in ['Oualem', 'Nebou', 'Nabugubelle', 'Manyoro', 'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabisi']:
            return {
                'Datetime': 'timestamp PRIMARY KEY',
                'Date': 'date',
                'Rain_mm': 'float',
                'Rain_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_%': 'float',
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float',
                'BP_mbar_Avg': 'float',
                'sunrise_time_utc': 'timestamp',
                'sunset_time_utc': 'timestamp',
                'Is_Daylight': 'boolean',
                'Daylight_Duration': 'float'
            }
        elif station == 'Atampisi':
            return {
                'Datetime': 'timestamp PRIMARY KEY',
                'Date': 'date',
                'Rain_01_mm': 'float',
                'Rain_02_mm': 'float',
                'Rain_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_%': 'float',
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float',
                'sunrise_time_utc': 'timestamp',
                'sunset_time_utc': 'timestamp',
                'Is_Daylight': 'boolean',
                'Daylight_Duration': 'float'
            }
    
        elif station == 'Aniabisi':
            return {
                'Datetime': 'timestamp PRIMARY KEY',
                'Date': 'date',
                'Rain_mm': 'float',
                'Air_Temp_Deg_C': 'float',
                'Rel_H_%': 'float',
                'Solar_R_W/m^2': 'float',
                'Wind_Sp_m/sec': 'float',
                'Wind_Dir_Deg': 'float'
            }
    return {}

# def get_station_columns(station: str, processing_type: str) -> Dict[str, str]:
#     """Retourne les colonnes attendues pour une station donnée selon le type de traitement"""
    
#     # Nettoyage du nom de la station
#     station = station.strip()
#     # BEFORE_PROCESSING
#     if processing_type == 'before':
#         # Bassin DANO
#         if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
#             return {
#                 '"Datetime"': 'TIMESTAMP PRIMARY KEY',
#                 '"Year"': 'INTEGER',
#                 '"Month"': 'INTEGER', 
#                 '"Day"': 'INTEGER',
#                 '"Hour"': 'INTEGER',
#                 '"Minute"': 'INTEGER',
#                 '"Rain_01_mm"': 'DOUBLE PRECISION',
#                 '"Rain_02_mm"': 'DOUBLE PRECISION',
#                 '"Air_Temp_Deg_C"': 'DOUBLE PRECISION',
#                 '"Rel_H_%"': 'DOUBLE PRECISION',
#                 '"Solar_R_W/m^2"': 'DOUBLE PRECISION',
#                 '"Wind_Sp_m/sec"': 'DOUBLE PRECISION',
#                 '"Wind_Dir_Deg"': 'DOUBLE PRECISION'
#             }
#         elif station in ['Lare', 'Tambiri 2']:
#             return {
#                 '"Datetime"': 'TIMESTAMP PRIMARY KEY',
#                 '"Year"': 'INTEGER',
#                 '"Month"': 'INTEGER', 
#                 '"Day"': 'INTEGER',
#                 '"Hour"': 'INTEGER',
#                 '"Minute"': 'INTEGER',
#                 '"Rain_01_mm"': 'DOUBLE PRECISION',
#                 '"Rain_02_mm"': 'DOUBLE PRECISION'
#             }
#         elif station == 'Tambiri 1':
#             return {
#                 '"Datetime"': 'TIMESTAMP PRIMARY KEY',
#                 '"Year"': 'INTEGER',
#                 '"Month"': 'INTEGER', 
#                 '"Day"': 'INTEGER',
#                 '"Hour"': 'INTEGER',
#                 '"Minute"': 'INTEGER',
#                 '"Rain_mm"': 'DOUBLE PRECISION',
#                 '"BP_mbar_Avg"': 'DOUBLE PRECISION',
#                 '"Air_Temp_Deg_C"': 'DOUBLE PRECISION',
#                 '"Rel_H_%"': 'DOUBLE PRECISION',
#                 '"Wind_Sp_m/sec"': 'DOUBLE PRECISION',
#                 '"Wind_Dir_Deg"': 'DOUBLE PRECISION'
#             }
#         # Bassin DASSARI
#         elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
#             return {
#                 '"Datetime"': 'TIMESTAMP PRIMARY KEY',
#                 '"Year"': 'INTEGER',
#                 '"Month"': 'INTEGER', 
#                 '"Day"': 'INTEGER',
#                 '"Hour"': 'INTEGER',
#                 '"Minute"': 'INTEGER',
#                 '"Rain_01_mm"': 'DOUBLE PRECISION',
#                 '"Rain_02_mm"': 'DOUBLE PRECISION',
#                 '"Air_Temp_Deg_C"': 'DOUBLE PRECISION',
#                 '"Rel_H_%"': 'DOUBLE PRECISION',
#                 '"Solar_R_W/m^2"': 'DOUBLE PRECISION',
#                 '"Wind_Sp_m/sec"': 'DOUBLE PRECISION',
#                 '"Wind_Dir_Deg"': 'DOUBLE PRECISION'
#             }
#         elif station == 'Ouriyori 1':
#             return {
#                 '"Datetime"': 'TIMESTAMP PRIMARY KEY',
#                 '"Year"': 'INTEGER',
#                 '"Month"': 'INTEGER', 
#                 '"Day"': 'INTEGER',
#                 '"Hour"': 'INTEGER',
#                 '"Minute"': 'INTEGER',
#                 '"Rain_01_mm"': 'DOUBLE PRECISION',
#                 '"Rain_02_mm"': 'DOUBLE PRECISION',
#                 '"Air_Temp_Deg_C"': 'DOUBLE PRECISION',
#                 '"Rel_H_%"': 'DOUBLE PRECISION',
#                 '"Solar_R_W/m^2"': 'DOUBLE PRECISION',
#                 '"Wind_Sp_m/sec"': 'DOUBLE PRECISION',
#                 '"Wind_Dir_Deg"': 'DOUBLE PRECISION'
#             }
#         # Bassin VEA SISSILI
#         elif station in ['Oualem', 'Nebou', 'Nabugubelle', 'Manyoro', 'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabisi']:
#             return {
#                 '"Datetime"': 'TIMESTAMP PRIMARY KEY',
#                 '"Date"': 'DATE',
#                 '"Rain_mm"': 'DOUBLE PRECISION',
#                 '"Air_Temp_Deg_C"': 'DOUBLE PRECISION',
#                 '"Rel_H_%"': 'DOUBLE PRECISION',
#                 '"Solar_R_W/m^2"': 'DOUBLE PRECISION',
#                 '"Wind_Sp_m/sec"': 'DOUBLE PRECISION',
#                 '"Wind_Dir_Deg"': 'DOUBLE PRECISION',
#                 '"BP_mbar_Avg"': 'DOUBLE PRECISION'
#             }
#         elif station == 'Atampisi':
#             return {
#                 '"Datetime"': 'TIMESTAMP PRIMARY KEY',
#                 '"Date"': 'DATE',
#                 '"Rain_01_mm"': 'DOUBLE PRECISION',
#                 '"Rain_02_mm"': 'DOUBLE PRECISION',
#                 '"Rain_mm"': 'DOUBLE PRECISION',
#                 '"Air_Temp_Deg_C"': 'DOUBLE PRECISION',
#                 '"Rel_H_%"': 'DOUBLE PRECISION',
#                 '"Solar_R_W/m^2"': 'DOUBLE PRECISION',
#                 '"Wind_Sp_m/sec"': 'DOUBLE PRECISION',
#                 '"Wind_Dir_Deg"': 'DOUBLE PRECISION'
#             }
#         elif station == 'Aniabisi':
#             return {
#                 '"Datetime"': 'TIMESTAMP PRIMARY KEY',
#                 '"Date"': 'DATE',
#                 '"Rain_mm"': 'DOUBLE PRECISION',
#                 '"Air_Temp_Deg_C"': 'DOUBLE PRECISION',
#                 '"Rel_H_%"': 'DOUBLE PRECISION',
#                 '"Solar_R_W/m^2"': 'DOUBLE PRECISION',
#                 '"Wind_Sp_m/sec"': 'DOUBLE PRECISION',
#                 '"Wind_Dir_Deg"': 'DOUBLE PRECISION'
#             }
    
#     # AFTER_PROCESSING
#     elif processing_type == 'after':
#         # Bassin DANO
#         if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
#             return {
#                 '"Datetime"': 'TIMESTAMP PRIMARY KEY',
#                 '"Year"': 'INTEGER',
#                 '"Month"': 'INTEGER', 
#                 '"Day"': 'INTEGER',
#                 '"Hour"': 'INTEGER',
#                 '"Minute"': 'INTEGER',
#                 '"Rain_01_mm"': 'DOUBLE PRECISION',
#                 '"Rain_02_mm"': 'DOUBLE PRECISION',
#                 '"Rain_mm"': 'DOUBLE PRECISION',
#                 '"Air_Temp_Deg_C"': 'DOUBLE PRECISION',
#                 '"Rel_H_%"': 'DOUBLE PRECISION',
#                 '"Solar_R_W/m^2"': 'DOUBLE PRECISION',
#                 '"Wind_Sp_m/sec"': 'DOUBLE PRECISION',
#                 '"Wind_Dir_Deg"': 'DOUBLE PRECISION',
#                 '"sunrise_time_utc"': 'TIMESTAMP',
#                 '"sunset_time_utc"': 'TIMESTAMP',
#                 '"Is_Daylight"': 'BOOLEAN',
#                 '"Daylight_Duration"': 'DOUBLE PRECISION'
#             }
#         elif station in ['Lare', 'Tambiri 2']:
#             return {
#                 '"Datetime"': 'TIMESTAMP PRIMARY KEY',
#                 '"Year"': 'INTEGER',
#                 '"Month"': 'INTEGER', 
#                 '"Day"': 'INTEGER',
#                 '"Hour"': 'INTEGER',
#                 '"Minute"': 'INTEGER',
#                 '"Rain_01_mm"': 'DOUBLE PRECISION',
#                 '"Rain_02_mm"': 'DOUBLE PRECISION',
#                 '"Rain_mm"': 'DOUBLE PRECISION'
#             }
#         elif station == 'Tambiri 1':
#             return {
#                 '"Datetime"': 'TIMESTAMP PRIMARY KEY',
#                 '"Year"': 'INTEGER',
#                 '"Month"': 'INTEGER', 
#                 '"Day"': 'INTEGER',
#                 '"Hour"': 'INTEGER',
#                 '"Minute"': 'INTEGER',
#                 '"Rain_mm"': 'DOUBLE PRECISION',
#                 '"BP_mbar_Avg"': 'DOUBLE PRECISION',
#                 '"Air_Temp_Deg_C"': 'DOUBLE PRECISION',
#                 '"Rel_H_%"': 'DOUBLE PRECISION',
#                 '"Wind_Sp_m/sec"': 'DOUBLE PRECISION',
#                 '"Wind_Dir_Deg"': 'DOUBLE PRECISION'
#             }
#         # Bassin DASSARI
#         elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
#             return {
#                 '"Datetime"': 'TIMESTAMP PRIMARY KEY',
#                 '"Year"': 'INTEGER',
#                 '"Month"': 'INTEGER', 
#                 '"Day"': 'INTEGER',
#                 '"Hour"': 'INTEGER',
#                 '"Minute"': 'INTEGER',
#                 '"Rain_01_mm"': 'DOUBLE PRECISION',
#                 '"Rain_02_mm"': 'DOUBLE PRECISION',
#                 '"Rain_mm"': 'DOUBLE PRECISION',
#                 '"Air_Temp_Deg_C"': 'DOUBLE PRECISION',
#                 '"Rel_H_%"': 'DOUBLE PRECISION',
#                 '"Solar_R_W/m^2"': 'DOUBLE PRECISION',
#                 '"Wind_Sp_m/sec"': 'DOUBLE PRECISION',
#                 '"Wind_Dir_Deg"': 'DOUBLE PRECISION',
#                 '"sunrise_time_utc"': 'TIMESTAMP',
#                 '"sunset_time_utc"': 'TIMESTAMP',
#                 '"Is_Daylight"': 'BOOLEAN',
#                 '"Daylight_Duration"': 'DOUBLE PRECISION'
#             }
#         elif station == 'Ouriyori 1':
#             return {
#                 '"Datetime"': 'TIMESTAMP PRIMARY KEY',
#                 '"Year"': 'INTEGER',
#                 '"Month"': 'INTEGER', 
#                 '"Day"': 'INTEGER',
#                 '"Hour"': 'INTEGER',
#                 '"Minute"': 'INTEGER',
#                 '"Rain_01_mm"': 'DOUBLE PRECISION',
#                 '"Rain_02_mm"': 'DOUBLE PRECISION',
#                 '"Rain_mm"': 'DOUBLE PRECISION',
#                 '"Air_Temp_Deg_C"': 'DOUBLE PRECISION',
#                 '"Rel_H_%"': 'DOUBLE PRECISION',
#                 '"Solar_R_W/m^2"': 'DOUBLE PRECISION',
#                 '"Wind_Sp_m/sec"': 'DOUBLE PRECISION',
#                 '"Wind_Dir_Deg"': 'DOUBLE PRECISION',
#                 '"sunrise_time_utc"': 'TIMESTAMP',
#                 '"sunset_time_utc"': 'TIMESTAMP',
#                 '"Is_Daylight"': 'BOOLEAN',
#                 '"Daylight_Duration"': 'DOUBLE PRECISION'
#             }
#         # Bassin VEA SISSILI
#         elif station in ['Oualem', 'Nebou', 'Nabugubelle', 'Manyoro', 'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabisi']:
#             return {
#                 '"Datetime"': 'TIMESTAMP PRIMARY KEY',
#                 '"Date"': 'DATE',
#                 '"Rain_mm"': 'DOUBLE PRECISION',
#                 '"Air_Temp_Deg_C"': 'DOUBLE PRECISION',
#                 '"Rel_H_%"': 'DOUBLE PRECISION',
#                 '"Solar_R_W/m^2"': 'DOUBLE PRECISION',
#                 '"Wind_Sp_m/sec"': 'DOUBLE PRECISION',
#                 '"Wind_Dir_Deg"': 'DOUBLE PRECISION',
#                 '"BP_mbar_Avg"': 'DOUBLE PRECISION',
#                 '"sunrise_time_utc"': 'TIMESTAMP',
#                 '"sunset_time_utc"': 'TIMESTAMP',
#                 '"Is_Daylight"': 'BOOLEAN',
#                 '"Daylight_Duration"': 'DOUBLE PRECISION'
#             }
#         elif station == 'Atampisi':
#             return {
#                 '"Datetime"': 'TIMESTAMP PRIMARY KEY',
#                 '"Date"': 'DATE',
#                 '"Rain_01_mm"': 'DOUBLE PRECISION',
#                 '"Rain_02_mm"': 'DOUBLE PRECISION',
#                 '"Rain_mm"': 'DOUBLE PRECISION',
#                 '"Air_Temp_Deg_C"': 'DOUBLE PRECISION',
#                 '"Rel_H_%"': 'DOUBLE PRECISION',
#                 '"Solar_R_W/m^2"': 'DOUBLE PRECISION',
#                 '"Wind_Sp_m/sec"': 'DOUBLE PRECISION',
#                 '"Wind_Dir_Deg"': 'DOUBLE PRECISION',
#                 '"sunrise_time_utc"': 'TIMESTAMP',
#                 '"sunset_time_utc"': 'TIMESTAMP',
#                 '"Is_Daylight"': 'BOOLEAN',
#                 '"Daylight_Duration"': 'DOUBLE PRECISION'
#             }
#         elif station == 'Aniabisi':
#             return {
#                 '"Datetime"': 'TIMESTAMP PRIMARY KEY',
#                 '"Date"': 'DATE',
#                 '"Rain_mm"': 'DOUBLE PRECISION',
#                 '"Air_Temp_Deg_C"': 'DOUBLE PRECISION',
#                 '"Rel_H_%"': 'DOUBLE PRECISION',
#                 '"Solar_R_W/m^2"': 'DOUBLE PRECISION',
#                 '"Wind_Sp_m/sec"': 'DOUBLE PRECISION',
#                 '"Wind_Dir_Deg"': 'DOUBLE PRECISION'
#             }
#     return {}

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


import numpy as np
import pandas as pd

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

def create_station_table(station: str, processing_type: str = 'before'):
    """Crée la table avec clé primaire sur Datetime si présente, sans colonne id"""
    columns_config = get_station_columns(station, processing_type)
    if not columns_config:
        raise ValueError(f"Configuration des colonnes manquante pour {station}")
    
    db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
    table_name = station.strip()
    pk_col = 'Datetime' if 'Datetime' in columns_config else None

    try:
        with get_connection(db_name) as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                # Vérification existence table
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' AND table_name = %s
                    )
                """, (table_name,))
                table_exists = cursor.fetchone()[0]
                if not table_exists:
                    column_defs = []
                    for col, dtype in columns_config.items():
                        pg_type = get_pg_type(dtype)
                        if pk_col and col == pk_col:
                            column_defs.append(f'"{col}" {pg_type} PRIMARY KEY')
                        else:
                            column_defs.append(f'"{col}" {pg_type}')
                    create_query = f"""
                        CREATE TABLE "{table_name}" (
                            {', '.join(column_defs)}
                        )
                    """
                    cursor.execute(create_query)
                    print(f"Table '{table_name}' créée avec succès")
                else:
                    print(f"Table '{table_name}' existe déjà")
                return True
    except Exception as e:
        print(f"Erreur création table '{table_name}': {e}")
        raise


# --- DÉBUT DE LA FONCTION save_to_database CORRIGÉE ---
def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before') -> bool:
    """
    Sauvegarde un DataFrame dans la base de données, en s'assurant que les colonnes
    correspondent au schéma attendu et gérant les conflits.
    """
    conn = None # Initialiser la connexion à None
    try:
        # 1. Validation initiale du DataFrame
        if df.empty:
            print(f"Warning: DataFrame vide reçu pour la station '{station}' ({processing_type}), aucune donnée à sauvegarder.")
            return True

        station = station.strip()
        table_name = station
        db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')

        if not db_name:
            raise ValueError(f"Nom de la base de données non configuré pour processing_type '{processing_type}'. Vérifiez .env")

        # 2. Récupération de la configuration des colonnes attendues
        columns_config = get_station_columns(station, processing_type)
        if not columns_config:
            raise ValueError(f"Configuration des colonnes manquante pour la station '{station}' ({processing_type}). Impossible de sauvegarder.")

        expected_columns = list(columns_config.keys())
        pk_col = 'Datetime' if 'Datetime' in expected_columns else None # Détection de la clé primaire

        # 3. Alignement et préparation du DataFrame
        # Assurer que toutes les colonnes attendues sont présentes, sinon ajouter avec None
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None # psycops2 gère None comme NULL

        # Supprimer les colonnes du DataFrame qui ne sont PAS dans la configuration attendue
        extra_cols = set(df.columns) - set(expected_columns)
        if extra_cols:
            df = df.drop(columns=list(extra_cols))
            print(f"Info: Colonnes supplémentaires supprimées pour '{station}': {list(extra_cols)}")

        # Réordonner le DataFrame strictement selon l'ordre des colonnes attendues
        # C'est LA ligne cruciale pour éviter 'tuple index out of range'
        df = df[expected_columns]

        # 4. Conversion des types de données pour psycopg2
        # Remplacer NaN/NaT par None pour une insertion propre dans la DB
        df = df.replace({np.nan: None, pd.NA: None, pd.NaT: None})

        type_converters = {
            'timestamp': lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) and x is not None else None,
            'date': lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) and x is not None else None,
            'float': lambda x: float(x) if pd.notna(x) and x is not None else None,
            'integer': lambda x: int(x) if pd.notna(x) and x is not None else None,
            'boolean': lambda x: bool(x) if pd.notna(x) and x is not None else None,
            'text': lambda x: str(x) if pd.notna(x) and x is not None else None
        }

        for col, pg_type_str in columns_config.items():
            pg_base_type = pg_type_str.split()[0].lower()
            
            if col not in df.columns: # Cette colonne a été ajoutée avec None, pas besoin de la convertir
                continue

            try:
                if pg_base_type in ['timestamp', 'date']:
                    # Convertir la colonne en type datetime de Pandas d'abord
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    # Puis appliquer le convertisseur pour la chaîne formatée ou None
                    df[col] = df[col].apply(type_converters[pg_base_type])
                elif pg_base_type in type_converters:
                    df[col] = df[col].apply(type_converters[pg_base_type])
                else: # Pour les types non explicitement gérés (ex: 'text' si pas déjà converti)
                    df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) and x is not None else None)

            except Exception as e:
                print(f"Warning: Conversion de type échouée pour la colonne '{col}' (attendu: {pg_base_type}) pour station '{station}': {str(e)}")
                df[col] = None # Forcer à None pour permettre l'insertion

        # 5. Connexion à la base de données
        conn = get_connection(db_name)
        if not conn:
            raise ConnectionError(f"Impossible d'établir une connexion à la base de données '{db_name}'.")

        with conn.cursor() as cursor:
            # 6. Vérifier et créer la table si nécessaire
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = %s
                )
            """, (table_name,))
            table_exists = cursor.fetchone()[0]
            if not table_exists:
                print(f"Table '{table_name}' n'existe pas dans '{db_name}'. Tentative de création...")
                create_station_table(station, processing_type)
                # Re-vérifier que la table existe après la tentative de création
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = %s
                    )
                """, (table_name,))
                if not cursor.fetchone()[0]:
                    raise Exception(f"La création de la table '{table_name}' a échoué. Vérifiez logs de create_station_table.")
                print(f"Table '{table_name}' créée avec succès.")

            # 7. Construction de la requête SQL d'insertion/upsert
            cols_sql = ', '.join([f'"{col}"' for col in expected_columns])
            placeholders = ', '.join(['%s'] * len(expected_columns))

            query = ""
            if pk_col and pk_col in expected_columns:
                # Utilise ON CONFLICT DO UPDATE pour gérer les doublons via la clé primaire
                set_clause = ', '.join(
                    f'"{col}" = EXCLUDED."{col}"'
                    for col in expected_columns
                    if col != pk_col
                )
                query = f"""
                    INSERT INTO "{table_name}" ({cols_sql})
                    VALUES ({placeholders})
                    ON CONFLICT ("{pk_col}") DO UPDATE SET
                        {set_clause};
                """
            else:
                # Si pas de clé primaire ou pas dans les colonnes attendues, insertion simple
                # Attention: Sans PK, ON CONFLICT DO NOTHING n'est pas fiable sans contrainte UNIQUE
                query = f"""
                    INSERT INTO "{table_name}" ({cols_sql})
                    VALUES ({placeholders});
                """
            
            # 8. Préparation des données pour execute_batch
            data_to_insert = [tuple(row) for row in df.values]

            # --- DIAGNOSTICS FINAUX AVANT L'EXÉCUTION ---
            print("\n--- DIAGNOSTIC FINAL DE LA SAUVEGARDE DB ---")
            print(f"Station: {station}, Type de Traitement: {processing_type}, Table: {table_name}")
            print(f"1. Nombre de colonnes attendues (expected_columns): {len(expected_columns)}")
            print(f"   Liste des expected_columns: {expected_columns}")
            print(f"2. Nombre de colonnes du DataFrame APRES préparation: {len(df.columns)}")
            print(f"   Liste des colonnes du DataFrame APRES préparation: {df.columns.tolist()}")
            if expected_columns != df.columns.tolist():
                print("!!! INCOHÉRENCE: L'ORDRE/NOMBRE DES COLONNES DF NE CORRESPOND PAS AUX ATTENTES SQL !!!")
                print(f"   Zip (Attendu, Actuel): {list(zip(expected_columns, df.columns.tolist()))}")
            
            print(f"3. Requête SQL générée (nombre de %s: {query.count('%s')}):")
            print(query)
            
            if data_to_insert:
                print(f"4. Nombre total de lignes à insérer: {len(data_to_insert)}")
                print(f"   Premier tuple de données (longueur: {len(data_to_insert[0])}): {data_to_insert[0]}")
                print(f"   Types des éléments du premier tuple: {[type(item).__name__ for item in data_to_insert[0]]}")
            else:
                print("AUCUNE DONNÉE À INSÉRER (data_to_insert est vide).")
            print("--- FIN DU DIAGNOSTIC FINAL ---\n")
            # --- FIN DES DIAGNOSTICS ---

            # 9. Exécution par lots
            batch_size = 1000
            inserted_rows = 0
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i:i + batch_size]
                if not batch:
                    continue # Passe si le lot est vide

                try:
                    execute_batch(cursor, query, batch)
                    conn.commit()
                    inserted_rows += len(batch)
                except Exception as e:
                    conn.rollback()
                    print(f"Échec de l'insertion du lot {i//batch_size + 1} dans '{table_name}': {str(e)}")
                    if batch:
                        # Afficher un exemple détaillé de la première ligne du lot en erreur
                        print(f"Données de la 1ère ligne du lot en erreur: {batch[0]}")
                        print(f"Types des données de la 1ère ligne du lot en erreur: {[type(item).__name__ for item in batch[0]]}")
                    raise # Propager l'erreur pour un traitement supérieur

            print(f"SUCCÈS: {inserted_rows}/{len(df)} lignes insérées/mises à jour dans '{table_name}'.")
            return True

    except Exception as e:
        print(f"\nERREUR CRITIQUE DANS save_to_database pour '{station}': {str(e)}")
        print("Traceback complet:")
        traceback.print_exc()
        if 'df' in locals() and not df.empty: # Affiche les détails du DF si disponible et non vide
            print("Détails du DataFrame au moment de l'erreur:")
            print(f"Colonnes du DataFrame: {df.columns.tolist()}")
            print(f"Types de colonnes du DataFrame:\n{df.dtypes}")
            print("5 premières lignes du DataFrame:")
            print(df.head().to_string())
        return False
    finally:
        if conn:
            conn.close()