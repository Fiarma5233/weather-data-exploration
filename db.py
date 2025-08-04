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

# # Charger les variables d'environnement
# load_dotenv()

# # Configuration de la base de données
# DB_CONFIG = {
#     'host': os.getenv('DB_HOST'),
#     #'database': os.getenv('DB_NAME_BEFORE', 'before_processing_db'),
#     'user': os.getenv('DB_USER'),
#     'password': os.getenv('DB_PASSWORD'),
#     'port': os.getenv('DB_PORT')
# }

# # Obtenir les noms des bases de données
# DB_NAMES = {
#     'before': os.getenv('DB_NAME_BEFORE'),
#     'after': os.getenv('DB_NAME_AFTER'),
#     'missing_before': os.getenv('DB_NAME_MISSING_BEFORE_DB'),
#     'missing_after': os.getenv('DB_NAME_MISSING_AFTER_DB'),
#     'admin_db':'postgres'
# }



# # Fonction de connexion aux bases de donnees
# def get_connection(db_key: str):
#     """
#     Établit une connexion à la base de données PostgreSQL en fonction d'une clé.

#     Args:
#         db_key (str): Clé pour déterminer la base de données (ex: 'before', 'after', 
#                       'missing_before', 'missing_after').
#     Returns:
#         psycopg2.Connection: L'objet de connexion à la base de données.
#     Raises:
#         ValueError: Si la clé de base de données est inconnue.
#         Exception: Si la connexion à la base de données échoue.
#     """
#     db_name = DB_NAMES.get(db_key)
#     if not db_name:
#         raise ValueError(f"Clé de base de données inconnue ou non configurée: '{db_key}'. "
#                          f"Clés disponibles: {list(DB_NAMES.keys())}")

#     config = DB_CONFIG.copy()
#     config['database'] = db_name # Définir la base de données spécifique ici

#     try:
#         conn = psycopg2.connect(**config)
#         return conn
#     except Exception as e:
#         print(f"Erreur de connexion à la base de données '{db_name}': {e}")
#         raise # Relaisser l'exception pour que l'appelant la gère

############## code bien commenté pour la suite ##############



# def get_connection(database: str = None):
#     """Établit une connexion à la base de données PostgreSQL"""
#     config = DB_CONFIG.copy()
#     if database:
#         config['database'] = database
#     try:
#         conn = psycopg2.connect(**config)
#         return conn
#     except Exception as e:
#         print(f"Erreur de connexion à la base de données: {e}")
#         raise

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

# def get_station_columns(station: str, processing_type: str) -> Dict[str, str]:
#     """Retourne les colonnes attendues pour une station donnée selon le type de traitement"""
    
#     # Nettoyage du nom de la station
#     station = station.strip()

#     # BEFORE_PROCESSING
#     if processing_type == 'before':
#         # Bassin DANO
#         if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
#             return {
#                 "Datetime": 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI  JUSTE POUR SE LIMITER A MINUTES
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         elif station in ['Lare', 'Tambiri 2']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
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
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_mm': 'float',
#                 'BP_mbar_Avg': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         # Bassin DASSARI
#         elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         elif station == 'Ouriyori 1':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         # Bassin VEA SISSILI
#         elif station in ['Oualem', 'Nebou', 'Nabugubulle', 'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabiisi']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date', # Garde Date comme date pure
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'BP_mbar_Avg': 'float'
#             }


#         elif station == 'Manyoro':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 #'BP_mbar_Avg': 'float'
#             }
#         elif station == 'Atampisi':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         elif station == 'Aniabisi':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
    
#     # AFTER_PROCESSING
#     elif processing_type == 'after':
#         # Bassin DANO
#         if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp', # Ces colonnes peuvent rester timestamp (elles ne sont pas forcément à la minute)
#                 'sunset_time_utc': 'timestamp',   # ou passer à timestamp(0) si c'est votre exigence pour elles aussi.
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         elif station in ['Lare', 'Tambiri 2']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
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
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_mm': 'float',
#                 'BP_mbar_Avg': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         # Bassin DASSARI
#         elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
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
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         # Bassin VEA SISSILI
#         elif station in ['Oualem', 'Nebou', 'Nabugubulle',  'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabiisi']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 # 'Rain_mm': 'float', # Cette ligne est un doublon, elle devrait être supprimée ou corrigée si c'est une autre colonne.
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'BP_mbar_Avg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }

#         elif station == 'Manyoro':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 #'BP_mbar_Avg': 'float'
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         elif station == 'Atampisi':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
    
#         elif station == 'Aniabisi': # Cette clause est également redondante si déjà dans la liste ci-dessus
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }

#     elif processing_type == 'missing_before' or processing_type == 'missing_after':
#         return{
#             'id': 'serial primary key unique',
#             'variable': 'varchar(255) not null',
#             'start_time': 'timestamp not null',      
#             'end_time': 'timestamp not null',
#             'duration_hours': 'float'
#         }
    
#     return {}






# def initialize_database():
#     """Initialise simplement les bases de données si elles n'existent pas"""
#     try:
#         db_user = DB_CONFIG['user']
        
#         with get_connection('postgres') as conn:
#             conn.autocommit = True
#             with conn.cursor() as cursor:
#                 for db_name in ['before_processing_db', 'after_processing_db']:
#                     cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
#                     if not cursor.fetchone():
#                         cursor.execute(f"""
#                             CREATE DATABASE {db_name}
#                             WITH OWNER = {db_user}
#                             ENCODING = 'UTF8'
#                         """)
#                         print(f"Base {db_name} créée avec succès")
        
#         print("Initialisation des bases terminée")
#         return True
        
#     except Exception as e:
#         print(f"Erreur initialisation DB: {e}")
#         raise

# Fichier : db.py

# ... (vos imports, DB_CONFIG, DB_NAMES, get_connection et autres fonctions) ...



# def get_station_columns(station: str, processing_type: str) -> Dict[str, str]:
#     """Retourne les colonnes attendues pour une station donnée selon le type de traitement"""
    
#     # Nettoyage du nom de la station
#     station = station.strip()

#     # BEFORE_PROCESSING
#     if processing_type == 'before':
#         # Bassin DANO
#         if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
#             return {
#                 "Datetime": 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI  JUSTE POUR SE LIMITER A MINUTES
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         elif station in ['Lare', 'Tambiri 2']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
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
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_mm': 'float',
#                 'BP_mbar_Avg': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         # Bassin DASSARI
#         elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         elif station == 'Ouriyori 1':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         # Bassin VEA SISSILI
#         elif station in ['Oualem', 'Nebou', 'Nabugubulle', 'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabiisi']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date', # Garde Date comme date pure
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'BP_mbar_Avg': 'float'
#             }


#         elif station == 'Manyoro':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 #'BP_mbar_Avg': 'float'
#             }
#         elif station == 'Atampisi':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         elif station == 'Aniabisi':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
    
#     # AFTER_PROCESSING
#     elif processing_type == 'after':
#         # Bassin DANO
#         if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp', # Ces colonnes peuvent rester timestamp (elles ne sont pas forcément à la minute)
#                 'sunset_time_utc': 'timestamp',   # ou passer à timestamp(0) si c'est votre exigence pour elles aussi.
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         elif station in ['Lare', 'Tambiri 2']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
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
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_mm': 'float',
#                 'BP_mbar_Avg': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         # Bassin DASSARI
#         elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
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
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         # Bassin VEA SISSILI
#         elif station in ['Oualem', 'Nebou', 'Nabugubulle',  'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabiisi']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 # 'Rain_mm': 'float', # Cette ligne est un doublon, elle devrait être supprimée ou corrigée si c'est une autre colonne.
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'BP_mbar_Avg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }

#         elif station == 'Manyoro':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 #'BP_mbar_Avg': 'float'
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         elif station == 'Atampisi':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
    
#         elif station == 'Aniabisi': # Cette clause est également redondante si déjà dans la liste ci-dessus
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }

#     elif processing_type == 'missing_before' or processing_type == 'missing_after':
#         return{
#             'id': 'serial primary key unique',
#             'variable': 'varchar(255) not null',
#             'start_time': 'timestamp not null',      
#             'end_time': 'timestamp not null',
#             'duration_hours': 'float'
#         }
    
#     return {}

# def initialize_database():
#     """Initialise simplement les bases de données si elles n'existent pas"""
#     try:
#         db_user = DB_CONFIG['user'] 
#         if not db_user:
#             raise ValueError("L'utilisateur de la base de données (DB_USER) n'est pas configuré dans DB_CONFIG.")
        
#         # Utilisez 'admin_db' pour vous connecter à la base de données 'postgres'
#         conn = get_connection('admin_db') # <--- PAS DE 'with' ICI POUR LA CONNEXION DIRECTE
#         if not conn:
#             print("Erreur : Impossible d'établir une connexion admin pour l'initialisation des bases de données.")
#             return False
        
#         conn.autocommit = True # Assurez-vous que autocommit est VRAIMENT activé
        
#         try:
#             # Récupérez les noms réels des bases de données que vous voulez créer
#             target_db_names = [
#                 DB_NAMES['before'],
#                 DB_NAMES['after'],
#                 DB_NAMES['missing_before'],
#                 DB_NAMES['missing_after']
#             ]

#             for db_name in target_db_names:
#                 if not db_name: 
#                     print(f"Avertissement : Le nom d'une base de données est vide. Veuillez vérifier vos variables d'environnement.")
#                     continue
                
#                 # Exécuter la vérification et la création SANS UN BLOC 'with cursor()'
#                 # pour le CREATE DATABASE afin d'éviter la transaction implicite.
#                 # Créez le curseur ici, utilisez-le, puis fermez-le immédiatement après.
#                 cursor = conn.cursor()
#                 try:
#                     cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
#                     if not cursor.fetchone():
#                         # La commande CREATE DATABASE doit être exécutée seule,
#                         # sans être englobée dans un bloc de transaction.
#                         # Le autocommit sur la connexion devrait gérer ça.
#                         cursor.execute(f"""
#                             CREATE DATABASE {db_name}
#                             WITH OWNER = {db_user}
#                             ENCODING = 'UTF8'
#                         """)
#                         print(f"Base **{db_name}** créée avec succès")
#                     else:
#                         print(f"Base **{db_name}** existe déjà")
#                 finally:
#                     # Assurez-vous de fermer le curseur après chaque opération
#                     cursor.close() 

#             print("Initialisation des bases terminée")
#             return True
            
#         finally:
#             # Assurez-vous que la connexion est fermée à la fin, même en cas d'erreur
#             if conn:
#                 conn.close()

#     except Exception as e:
#         print(f"Erreur initialisation DB: {e}")
#         traceback.print_exc() 
#         return False



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


# def create_station_table(station: str, processing_type: str = 'before'):
#     """
#     Crée la table dans la base de données si elle n'existe pas,
#     avec une clé primaire sur 'Datetime' si présente.
#     """
#     # Ici, nous utilisons directement le nom de la station fourni comme nom de table,
#     # car vous souhaitez conserver les noms d'origine (y compris accents et espaces).
#     # PostgreSQL gérera cela avec les guillemets doubles.
#     table_name = station.strip()

#     # Le columns_config doit déjà contenir le nom de colonne 'Rel_H_Pct'
#     columns_config = get_station_columns(station, processing_type)
#     if not columns_config:
#         print(f"Error: Configuration des colonnes manquante pour {station}. Impossible de créer la table.")
#         return False
    
#     db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')

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
#                     # Construire les définitions de colonnes avec les noms de colonnes issus de columns_config
#                     # qui doit maintenant inclure 'Rel_H_Pct'.
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
#                     print(f"\nℹ️ La table '{table_name}' n'existe pas, création...")
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

# def get_station_columns(station: str, processing_type: str) -> Dict[str, str]:
#     """Retourne les colonnes attendues pour une station donnée selon le type de traitement"""
    
#     # Nettoyage du nom de la station
#     station = station.strip()

#     # BEFORE_PROCESSING
#     if processing_type == 'before':
#         # Bassin DANO
#         if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
#             return {
#                 "Datetime": 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI  JUSTE POUR SE LIMITER A MINUTES
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         elif station in ['Lare', 'Tambiri 2']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
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
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_mm': 'float',
#                 'BP_mbar_Avg': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         # Bassin DASSARI
#         elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         elif station == 'Ouriyori 1':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         # Bassin VEA SISSILI
#         elif station in ['Oualem', 'Nebou', 'Nabugubulle', 'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabiisi']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date', # Garde Date comme date pure
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'BP_mbar_Avg': 'float'
#             }


#         elif station == 'Manyoro':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 #'BP_mbar_Avg': 'float'
#             }
#         elif station == 'Atampisi':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         elif station == 'Aniabisi':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
    
#     # AFTER_PROCESSING
#     elif processing_type == 'after':
#         # Bassin DANO
#         if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp', # Ces colonnes peuvent rester timestamp (elles ne sont pas forcément à la minute)
#                 'sunset_time_utc': 'timestamp',   # ou passer à timestamp(0) si c'est votre exigence pour elles aussi.
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         elif station in ['Lare', 'Tambiri 2']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
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
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_mm': 'float',
#                 'BP_mbar_Avg': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         # Bassin DASSARI
#         elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
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
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         # Bassin VEA SISSILI
#         elif station in ['Oualem', 'Nebou', 'Nabugubulle',  'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabiisi']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 # 'Rain_mm': 'float', # Cette ligne est un doublon, elle devrait être supprimée ou corrigée si c'est une autre colonne.
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'BP_mbar_Avg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }

#         elif station == 'Manyoro':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 #'BP_mbar_Avg': 'float'
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         elif station == 'Atampisi':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
    
#         elif station == 'Aniabisi': # Cette clause est également redondante si déjà dans la liste ci-dessus
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }

#     elif processing_type == 'missing_before' or processing_type == 'missing_after':
#         return{
#             'id': 'serial primary key unique',
#             'variable': 'varchar(255) not null',
#             'start_time': 'timestamp not null',      
#             'end_time': 'timestamp not null',
#             'duration_hours': 'float'
#         }
    
#     return {}

# def initialize_database():
#     """Initialise simplement les bases de données si elles n'existent pas"""
#     try:
#         db_user = DB_CONFIG['user'] 
#         if not db_user:
#             raise ValueError("L'utilisateur de la base de données (DB_USER) n'est pas configuré dans DB_CONFIG.")
        
#         # Utilisez 'admin_db' pour vous connecter à la base de données 'postgres'
#         conn = get_connection('admin_db') # <--- PAS DE 'with' ICI POUR LA CONNEXION DIRECTE
#         if not conn:
#             print("Erreur : Impossible d'établir une connexion admin pour l'initialisation des bases de données.")
#             return False
        
#         conn.autocommit = True # Assurez-vous que autocommit est VRAIMENT activé
        
#         try:
#             # Récupérez les noms réels des bases de données que vous voulez créer
#             target_db_names = [
#                 DB_NAMES['before'],
#                 DB_NAMES['after'],
#                 DB_NAMES['missing_before'],
#                 DB_NAMES['missing_after']
#             ]

#             for db_name in target_db_names:
#                 if not db_name: 
#                     print(f"Avertissement : Le nom d'une base de données est vide. Veuillez vérifier vos variables d'environnement.")
#                     continue
                
#                 # Exécuter la vérification et la création SANS UN BLOC 'with cursor()'
#                 # pour le CREATE DATABASE afin d'éviter la transaction implicite.
#                 # Créez le curseur ici, utilisez-le, puis fermez-le immédiatement après.
#                 cursor = conn.cursor()
#                 try:
#                     cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
#                     if not cursor.fetchone():
#                         # La commande CREATE DATABASE doit être exécutée seule,
#                         # sans être englobée dans un bloc de transaction.
#                         # Le autocommit sur la connexion devrait gérer ça.
#                         cursor.execute(f"""
#                             CREATE DATABASE {db_name}
#                             WITH OWNER = {db_user}
#                             ENCODING = 'UTF8'
#                         """)
#                         print(f"Base **{db_name}** créée avec succès")
#                     else:
#                         print(f"Base **{db_name}** existe déjà")
#                 finally:
#                     # Assurez-vous de fermer le curseur après chaque opération
#                     cursor.close() 

#             print("Initialisation des bases terminée")
#             return True
            
#         finally:
#             # Assurez-vous que la connexion est fermée à la fin, même en cas d'erreur
#             if conn:
#                 conn.close()

#     except Exception as e:
#         print(f"Erreur initialisation DB: {e}")
#         traceback.print_exc() 
#         return False
    
# def get_pg_type(python_type_str: str) -> str:
#     """Convertit un type Python en un type PostgreSQL."""
#     type_map = {
#         'timestamp(0) PRIMARY KEY': 'timestamp(0) PRIMARY KEY',
#         'timestamp': 'timestamp',
#         'date': 'date',
#         'integer': 'integer',
#         'float': 'double precision',
#         'boolean': 'boolean',
#         'varchar(255)': 'varchar(255)',
#         'varchar(255) NOT NULL': 'varchar(255) NOT NULL',
#         'serial primary key unique': 'SERIAL PRIMARY KEY'
#     }
#     return type_map.get(python_type_str.lower(), 'text')


# def create_station_table(station: str, processing_type: str = 'before'):
#     """
#     Crée la table dans la base de données si elle n'existe pas,
#     avec une clé primaire sur 'Datetime' si présente.
#     """
#     # Ici, nous utilisons directement le nom de la station fourni comme nom de table,
#     # car vous souhaitez conserver les noms d'origine (y compris accents et espaces).
#     # PostgreSQL gérera cela avec les guillemets doubles.
#     table_name = station.strip()

#     # Le columns_config doit déjà contenir le nom de colonne 'Rel_H_Pct'
#     columns_config = get_station_columns(station, processing_type)
#     if not columns_config:
#         print(f"Error: Configuration des colonnes manquante pour {station}. Impossible de créer la table.")
#         return False
    
#     db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')

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
#                     # Construire les définitions de colonnes avec les noms de colonnes issus de columns_config
#                     # qui doit maintenant inclure 'Rel_H_Pct'.
#                     for col, dtype in columns_config.items():
#                         pg_type = get_pg_type(dtype)
#                         column_defs.append(f'"{col}" {pg_type}')
                    
#                     # Ajouter la clé primaire si la colonne 'Datetime' existe
#                     if 'Datetime' in columns_config:
#                         column_defs.append('PRIMARY KEY ("Datetime")')

#                     elif 'id' in columns_config:
#                         column_defs.append('id serial primary key unique')
                    
#                     create_query = f"""
#                         CREATE TABLE "{table_name}" (
#                             {', '.join(column_defs)}
#                         )
#                     """

                   
#                     print(f"\nℹ️ La table '{table_name}' n'existe pas, création...")
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





# Fichier : db.py (ou où cette fonction est définie)

# def create_station_table(station: str, processing_type: str = 'before'):
#     """
#     Crée la table dans la base de données si elle n'existe pas,
#     avec une clé primaire sur 'Datetime' si présente.
#     """
#     table_name = station.strip()

#     columns_config = get_station_columns(station, processing_type)
#     if not columns_config:
#         print(f"Error: Configuration des colonnes manquante pour {station}. Impossible de créer la table.")
#         return False
    
#     # Ancien code (problématique):
#     # db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')
    
#     # NOUVELLE LIGNE : Utilisez directement 'processing_type' qui est la clé attendue par get_connection
#     # Cette variable est déjà la clé courte ('before' ou 'after')
#     db_key_for_connection = processing_type 

#     try:
#         # CORRECTION ICI : Passez db_key_for_connection (la clé courte)
#         with get_connection(db_key_for_connection) as conn:
#             if not conn:
#                 print(f"Erreur: Impossible d'obtenir la connexion pour la clé '{db_key_for_connection}'.")
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
#                     # Assurez-vous que 'Datetime' est un type de données approprié pour une PK (ex: timestamp)
#                     if 'Datetime' in columns_config:
#                         # Nous devons remplacer la définition existante de Datetime
#                         # pour ajouter PRIMARY KEY.
#                         # Une meilleure approche serait de générer les définitions de colonnes
#                         # en sachant à l'avance quelle est la clé primaire.
#                         final_column_defs = []
#                         for col, dtype in columns_config.items():
#                             pg_type = get_pg_type(dtype)
#                             if col == 'Datetime':
#                                 final_column_defs.append(f'"{col}" {pg_type} PRIMARY KEY')
#                             else:
#                                 final_column_defs.append(f'"{col}" {pg_type}')
                        
#                         create_query = f"""
#                             CREATE TABLE "{table_name}" (
#                                 {', '.join(final_column_defs)}
#                             )
#                         """
#                     elif 'id' in columns_config: # Si vous utilisez un 'id' auto-incrémenté comme PK
#                         # Assurez-vous que 'id' est bien défini comme serial primary key unique dans columns_config
#                         # Ou ajoutez-le ici si vous voulez qu'il soit toujours présent.
#                         final_column_defs = []
#                         for col, dtype in columns_config.items():
#                             pg_type = get_pg_type(dtype)
#                             if col == 'id' and 'id serial primary key unique' not in column_defs:
#                                 final_column_defs.append(f'"{col}" SERIAL PRIMARY KEY UNIQUE')
#                             else:
#                                 final_column_defs.append(f'"{col}" {pg_type}')
#                         create_query = f"""
#                             CREATE TABLE "{table_name}" (
#                                 {', '.join(final_column_defs)}
#                             )
#                         """
#                     else:
#                         # Cas où ni 'Datetime' ni 'id' n'est une clé primaire définie explicitement.
#                         # Cela peut causer l'erreur ON CONFLICT si vous utilisez DO NOTHING sans PK/UNIQUE.
#                         create_query = f"""
#                             CREATE TABLE "{table_name}" (
#                                 {', '.join(column_defs)}
#                             )
#                         """
#                         print(f"Attention: Aucune clé primaire (Datetime ou id) détectée pour la table '{table_name}'. "
#                               "L'insertion avec ON CONFLICT pourrait échouer si aucune contrainte UNIQUE n'existe.")
                   
#                     print(f"\nℹ️ La table '{table_name}' n'existe pas, création...")
#                     print(f"Executing table creation query for '{table_name}':\n{create_query}")
#                     cursor.execute(create_query)
#                     print(f"Table '{table_name}' créée avec succès")
#                 else:
#                     print(f"Table '{table_name}' existe déjà")
#                 return True
#     except ValueError as ve: # Capture l'erreur de get_connection spécifiquement
#         print(f"Erreur de configuration de la base de données lors de la création de table pour '{table_name}': {ve}")
#         traceback.print_exc()
#         return False
#     except Exception as e:
#         print(f"Erreur lors de la création de la table '{table_name}': {e}")
#         traceback.print_exc()
#         return False


import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def get_station_columns(station: str, processing_type: str) -> Dict[str, str]:
#     """Retourne les colonnes attendues pour une station donnée selon le type de traitement"""
    
#     # Nettoyage du nom de la station
#     station = station.strip()

#     # BEFORE_PROCESSING
#     if processing_type == 'before':
#         # Bassin DANO
#         if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
#             return {
#                 "Datetime": 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI  JUSTE POUR SE LIMITER A MINUTES
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         elif station in ['Lare', 'Tambiri 2']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
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
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_mm': 'float',
#                 'BP_mbar_Avg': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         # Bassin DASSARI
#         elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         elif station == 'Ouriyori 1':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         # Bassin VEA SISSILI
#         elif station in ['Oualem', 'Nebou', 'Nabugubulle', 'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabiisi']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date', # Garde Date comme date pure
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'BP_mbar_Avg': 'float'
#             }


#         elif station == 'Manyoro':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 #'BP_mbar_Avg': 'float'
#             }
#         elif station == 'Atampisi':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         elif station == 'Aniabisi':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
    
#     # AFTER_PROCESSING
#     elif processing_type == 'after':
#         # Bassin DANO
#         if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp', # Ces colonnes peuvent rester timestamp (elles ne sont pas forcément à la minute)
#                 'sunset_time_utc': 'timestamp',   # ou passer à timestamp(0) si c'est votre exigence pour elles aussi.
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         elif station in ['Lare', 'Tambiri 2']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
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
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_mm': 'float',
#                 'BP_mbar_Avg': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         # Bassin DASSARI
#         elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
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
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         # Bassin VEA SISSILI
#         elif station in ['Oualem', 'Nebou', 'Nabugubulle',  'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabiisi']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 # 'Rain_mm': 'float', # Cette ligne est un doublon, elle devrait être supprimée ou corrigée si c'est une autre colonne.
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'BP_mbar_Avg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }

#         elif station == 'Manyoro':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 #'BP_mbar_Avg': 'float'
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         elif station == 'Atampisi':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
    
#         elif station == 'Aniabisi': # Cette clause est également redondante si déjà dans la liste ci-dessus
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }

#     elif processing_type == 'missing_before' or processing_type == 'missing_after':
#         return{
#             'id': 'serial primary key unique',
#             'variable': 'varchar(255) not null',
#             'start_time': 'timestamp not null',      
#             'end_time': 'timestamp not null',
#             'duration_hours': 'float'
#         }
    
#     return {}

# def initialize_database():
#     """Initialise simplement les bases de données si elles n'existent pas"""
#     try:
#         db_user = DB_CONFIG['user'] 
#         if not db_user:
#             raise ValueError("L'utilisateur de la base de données (DB_USER) n'est pas configuré dans DB_CONFIG.")
        
#         # Utilisez 'admin_db' pour vous connecter à la base de données 'postgres'
#         conn = get_connection('admin_db') # <--- PAS DE 'with' ICI POUR LA CONNEXION DIRECTE
#         if not conn:
#             print("Erreur : Impossible d'établir une connexion admin pour l'initialisation des bases de données.")
#             return False
        
#         conn.autocommit = True # Assurez-vous que autocommit est VRAIMENT activé
        
#         try:
#             # Récupérez les noms réels des bases de données que vous voulez créer
#             target_db_names = [
#                 DB_NAMES['before'],
#                 DB_NAMES['after'],
#                 DB_NAMES['missing_before'],
#                 DB_NAMES['missing_after']
#             ]

#             for db_name in target_db_names:
#                 if not db_name: 
#                     print(f"Avertissement : Le nom d'une base de données est vide. Veuillez vérifier vos variables d'environnement.")
#                     continue
                
#                 # Exécuter la vérification et la création SANS UN BLOC 'with cursor()'
#                 # pour le CREATE DATABASE afin d'éviter la transaction implicite.
#                 # Créez le curseur ici, utilisez-le, puis fermez-le immédiatement après.
#                 cursor = conn.cursor()
#                 try:
#                     cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
#                     if not cursor.fetchone():
#                         # La commande CREATE DATABASE doit être exécutée seule,
#                         # sans être englobée dans un bloc de transaction.
#                         # Le autocommit sur la connexion devrait gérer ça.
#                         cursor.execute(f"""
#                             CREATE DATABASE {db_name}
#                             WITH OWNER = {db_user}
#                             ENCODING = 'UTF8'
#                         """)
#                         print(f"Base **{db_name}** créée avec succès")
#                     else:
#                         print(f"Base **{db_name}** existe déjà")
#                 finally:
#                     # Assurez-vous de fermer le curseur après chaque opération
#                     cursor.close() 

#             print("Initialisation des bases terminée")
#             return True
            
#         finally:
#             # Assurez-vous que la connexion est fermée à la fin, même en cas d'erreur
#             if conn:
#                 conn.close()

#     except Exception as e:
#         print(f"Erreur initialisation DB: {e}")
#         traceback.print_exc() 
#         return False
    
# def get_pg_type(python_type_str: str) -> str:
#     """Convertit un type Python en un type PostgreSQL."""
#     type_map = {
#         'timestamp(0) PRIMARY KEY': 'timestamp(0) PRIMARY KEY',
#         'timestamp': 'timestamp',
#         'date': 'date',
#         'integer': 'integer',
#         'float': 'double precision',
#         'boolean': 'boolean',
#         'varchar(255)': 'varchar(255)',
#         'varchar(255) NOT NULL': 'varchar(255) NOT NULL',
#         'serial primary key unique': 'SERIAL PRIMARY KEY'
#     }
#     return type_map.get(python_type_str.lower(), 'text')


# def create_station_table(station: str, processing_type: str = 'before'):
#     """
#     Crée la table dans la base de données si elle n'existe pas,
#     avec une clé primaire sur 'Datetime' ou 'id' selon le type de traitement.
#     """
#     table_name = station.strip()

#     columns_config = get_station_columns(station, processing_type)
#     if not columns_config:
#         print(f"Error: Configuration des colonnes manquante pour {station}. Impossible de créer la table.")
#         return False
    
#     db_key_for_connection = processing_type 

#     try:
#         with get_connection(db_key_for_connection) as conn:
#             if not conn:
#                 print(f"Erreur: Impossible d'obtenir la connexion pour la clé '{db_key_for_connection}'.")
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
#                     primary_key_defined = False

#                     # Prioritize 'id' for 'missing_before/after' as SERIAL PRIMARY KEY
#                     if processing_type in ['missing_before', 'missing_after'] and 'id' in columns_config:
#                         # Ensure 'id' is defined as SERIAL PRIMARY KEY
#                         column_defs.append(f'"id" SERIAL PRIMARY KEY UNIQUE')
#                         primary_key_defined = True
#                         # Add other columns from config, excluding 'id' if already added
#                         for col, dtype in columns_config.items():
#                             if col != 'id':
#                                 pg_type = get_pg_type(dtype)
#                                 column_defs.append(f'"{col}" {pg_type}')
                        
#                     # Then 'Datetime' for 'before/after' tables
#                     elif 'Datetime' in columns_config:
#                         for col, dtype in columns_config.items():
#                             pg_type = get_pg_type(dtype)
#                             if col == 'Datetime':
#                                 column_defs.append(f'"{col}" {pg_type} PRIMARY KEY')
#                                 primary_key_defined = True
#                             else:
#                                 column_defs.append(f'"{col}" {pg_type}')
#                     else:
#                         # General case: add all columns from config
#                         for col, dtype in columns_config.items():
#                             pg_type = get_pg_type(dtype)
#                             column_defs.append(f'"{col}" {pg_type}')

#                     if not primary_key_defined:
#                          print(f"Attention: Aucune clé primaire (Datetime ou id) détectée pour la table '{table_name}'. "
#                                "L'insertion avec ON CONFLICT pourrait échouer si aucune contrainte UNIQUE n'existe.")
                    
#                     create_query = f"""
#                         CREATE TABLE "{table_name}" (
#                             {', '.join(column_defs)}
#                         )
#                     """
                   
#                     print(f"\nℹ️ La table '{table_name}' n'existe pas, création...")
#                     print(f"Executing table creation query for '{table_name}':\n{create_query}")
#                     cursor.execute(create_query)
#                     print(f"Table '{table_name}' créée avec succès")
#                 else:
#                     print(f"Table '{table_name}' existe déjà")
#                 return True
#     except ValueError as ve: 
#         print(f"Erreur de configuration de la base de données lors de la création de table pour '{table_name}': {ve}")
#         traceback.print_exc()
#         return False
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


# def save_to_database(df: pd.DataFrame, station: str, conn, processing_type: str = 'before') -> bool:
#     """
#     Sauvegarde un DataFrame dans la base de données, avec vérifications complètes et journalisation détaillée.
#     La connexion à la base de données est passée en argument.
#     """
#     try:
#         # Vérification initiale
#         if df.empty:
#             print(f"Warning: DataFrame vide reçu pour la station '{station}' ({processing_type}), aucune donnée à sauvegarder.")
#             return True

#         # Le nom de la table dans la DB sera exactement le nom de la station
#         table_name = station.strip()

#         db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')

#         # La connexion est maintenant passée en argument, donc plus besoin de la créer ici
#         if not conn: # Vérification au cas où la connexion passée est nulle
#             raise ConnectionError(f"Connexion à la base de données non fournie ou nulle pour '{db_name}'")

#         # Récupérez la configuration des colonnes attendues (doit déjà inclure 'Rel_H_Pct')
#         columns_config = get_station_columns(station, processing_type)
#         if not columns_config:
#             raise ValueError(f"Configuration des colonnes manquante pour la station '{station}' ({processing_type}). Impossible de sauvegarder.")

#         expected_columns = list(columns_config.keys())

#         # --- DÉBUT DE LA CORRECTION : Renommage Spécifique de la Colonne `Rel_H_%` dans le DataFrame ---
#         df_column_rename_map = {}
#         for df_col in df.columns:
#             if df_col == 'Rel_H_%':
#                 df_column_rename_map[df_col] = 'Rel_H_Pct'
#             elif df_col.lower() == 'rel_h': 
#                  df_column_rename_map[df_col] = 'Rel_H_Pct'

#         if df_column_rename_map:
#             df = df.rename(columns=df_column_rename_map)
#             print(f"DEBUG: Colonnes du DataFrame renommées: {df_column_rename_map}")
#         # --- FIN DE LA CORRECTION ---


#         # ========== VÉRIFICATION 1: NOMS DE COLONNES ==========
#         print("\n" + "="*50)
#         print("VÉRIFICATION DES NOMS DE COLONNES (CASSE ET CORRESPONDANCE)")
#         print("="*50)
        
#         column_mapping_expected = {col.lower(): col for col in expected_columns}
#         new_df_columns = []
#         for col_df in df.columns:
#             if col_df in expected_columns: 
#                 new_df_columns.append(col_df)
#             elif col_df.lower() in column_mapping_expected: 
#                 new_df_columns.append(column_mapping_expected[col_df.lower()])
#             else: 
#                 new_df_columns.append(col_df)
        
#         df.columns = new_df_columns

#         df_cols_after_norm = df.columns.tolist()
#         missing_in_df = set(expected_columns) - set(df_cols_after_norm)
#         extra_in_df = set(df_cols_after_norm) - set(expected_columns)
        
#         print(f"\nColonnes attendues par la DB: {expected_columns}")
#         print(f"Colonnes du DataFrame (après normalisation): {df_cols_after_norm}")

#         if missing_in_df:
#             print(f"\n⚠️ ATTENTION: Colonnes attendues mais absentes du DataFrame:")
#             for col in missing_in_df:
#                 print(f"- {col}")
#                 df[col] = None 
        
#         if extra_in_df:
#             print(f"\nℹ️ INFO: Colonnes supplémentaires dans le DataFrame:")
#             for col in extra_in_df:
#                 print(f"- {col}")
#             df = df.drop(columns=list(extra_in_df))

#         if not missing_in_df and not extra_in_df and set(expected_columns) == set(df.columns.tolist()):
#             print("\n✅ Tous les noms de colonnes correspondent exactement")
#         else:
#             print("\n❌ Des divergences de noms de colonnes existent")

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
#                 df[col] = df[col].replace(nan_value_strings, np.nan)
                
#                 if pg_base_type in ['timestamp', 'date']:
#                     df[col] = pd.to_datetime(df[col], errors='coerce')
#                     print("- Converti en datetime")
#                 elif pg_base_type in ['float', 'integer']:
#                     df[col] = pd.to_numeric(df[col], errors='coerce')
#                     print("- Converti en numérique")
                
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
        
#         for col in expected_columns:
#             pg_type_str = columns_config.get(col, 'text')
#             pg_base_type = pg_type_str.split()[0].lower()

#             if pg_base_type == 'timestamp':
#                 type_converters.append(lambda x: x.strftime('%Y-%m-%d %H:%M') if pd.notna(x) else None) # <-- MODIFIÉ ICI
#             elif pg_base_type == 'date':
#                 type_converters.append(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
#             elif pg_base_type == 'float':
#                 type_converters.append(lambda x: float(x) if pd.notna(x) else None)
#             elif pg_base_type == 'integer':
#                 type_converters.append(lambda x: int(x) if pd.notna(x) and x == int(x) else None)
#             else:
#                 type_converters.append(lambda x: str(x) if pd.notna(x) else None)

#         for idx, row in df.iterrows():
#             try:
#                 row_values = [type_converters[i](row[col]) for i, col in enumerate(expected_columns)]
#                 data_to_insert.append(tuple(row_values))
                
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
#         # La connexion est passée en argument `conn` ici.
#         # Plus besoin de get_connection(db_name) ou de vérifier `if not conn:`
#         # directement après un appel à get_connection().
#         conn.autocommit = True # Assurez-vous que la connexion passée est en autocommit

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
#                 # create_station_table doit pouvoir utiliser une connexion interne ou être adapté pour en prendre une
#                 # Pour l'instant, create_station_table garde son propre get_connection, ce qui est acceptable ici.
#                 if not create_station_table(station, processing_type): # <-- create_station_table n'a pas besoin de la connexion passée
#                     raise Exception(f"Échec de la création de la table '{table_name}'")
#                 print("✅ Table créée avec succès")

#             # ========== NOUVELLE VÉRIFICATION ROBUSTE DE CLÉ PRIMAIRE ==========
#             pk_col = None
#             try:
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
#             print(f"Nombre de colonnes préparées (dans DataFrame): {len(df.columns)}") 
            
#             if len(expected_columns) != len(df.columns):
#                 print("\n❌ ERREUR: Nombre de colonnes incompatible entre expected_columns et le DataFrame final!")
#                 print("Colonnes attendues:", expected_columns)
#                 print("Colonnes DataFrame:", df.columns.tolist())
#                 raise ValueError("Incompatibilité de colonnes après préparation") 

#             if data_to_insert:
#                 first_row = data_to_insert[0]
#                 print(f"\nPremière ligne de données ({len(first_row)} valeurs):")
#                 for val, col in zip(first_row, expected_columns):
#                     print(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                
#                 try:
#                     mogrified = cursor.mogrify(query, first_row).decode('utf-8')
#                     print("\n✅ Test mogrify réussi:")
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
#             batch_size = 10000
#             # inserted_rows = 0 # Nous ne compterons plus les tentatives d'insertion ici pour éviter la confusion
#             start_time = datetime.now()
            
#             print(f"\nDébut de l'insertion de {len(data_to_insert)} lignes par lots de {batch_size}...")
            
#             for i in range(0, len(data_to_insert), batch_size):
#                 batch = data_to_insert[i:i + batch_size]
#                 try:
#                     extras.execute_batch(cursor, query, batch)
#                     # La connexion sera commitée par Flask à la fin du traitement global du lot de fichiers
#                     # ou automatiquement si autocommit est activé et que chaque requête est une transaction
#                     # Sinon, il faudrait faire un conn.commit() ici après chaque batch
#                     # Cependant, pour execute_batch, la documentation de psycopg2 recommande execute_batch
#                     # avec un autocommit ou un commit manuel après tous les batchs.
#                     # Pour être sûr que chaque lot est enregistré, ajoutons conn.commit()
#                     conn.commit() # <-- Ajouté ici pour commiter chaque lot
                    
#                     # inserted_rows += len(batch) # Non utilisé pour le compte final, peut être retiré
                    
#                     # Journalisation périodique
#                     if (i // batch_size) % 10 == 0:
#                         elapsed = (datetime.now() - start_time).total_seconds()
#                         # rate = inserted_rows / elapsed if elapsed > 0 else 0 # Retiré car inserted_rows ne reflète pas le réel
#                         print(f"Lot {i//batch_size + 1}: {len(batch)} lignes traitées...")
                        
#                 except Exception as e:
#                     conn.rollback()
#                     print(f"\n❌ ERREUR lot {i//batch_size + 1}: {str(e)}")
#                     if batch:
#                         print("Exemple de ligne problématique:", batch[0])
#                     traceback.print_exc()
#                     raise 

#             total_time = (datetime.now() - start_time).total_seconds()
#             print(f"\n✅ Traitement d'insertion terminé pour '{station}': {len(data_to_insert)} lignes préparées en {total_time:.2f} secondes.")
#             print("Note: Le nombre exact de lignes insérées peut différer en raison de la clause ON CONFLICT.")
#             return True

#     except Exception as e:
#         print(f"\n❌❌❌ ERREUR CRITIQUE lors de la sauvegarde pour '{station}': {str(e)}")
#         traceback.print_exc()
        
#         if 'df' in locals():
#             print("\nÉtat du DataFrame au moment de l'erreur:")
#             print(f"Shape: {df.shape}")
#             print("5 premières lignes:")
#             print(df.head().to_string())
            
#         return False



# def save_to_database(df: pd.DataFrame, station: str, conn, processing_type: str = 'before') -> bool:
#     """
#     Sauvegarde un DataFrame dans la base de données, avec vérifications complètes et journalisation détaillée.
#     La connexion à la base de données est passée en argument.
#     """
#     try:
#         # Vérification initiale
#         if df.empty:
#             print(f"Warning: DataFrame vide reçu pour la station '{station}' ({processing_type}), aucune donnée à sauvegarder.")
#             return True

#         table_name = station.strip()

#         db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')

#         if not conn:
#             raise ConnectionError(f"Connexion à la base de données non fournie ou nulle pour '{db_name}'")

#         columns_config = get_station_columns(station, processing_type)
#         if not columns_config:
#             raise ValueError(f"Configuration des colonnes manquante pour la station '{station}' ({processing_type}). Impossible de sauvegarder.")

#         expected_columns = list(columns_config.keys())

#         # --- DÉBUT DE LA CORRECTION : Renommage Spécifique de la Colonne `Rel_H_%` dans le DataFrame ---
#         df_column_rename_map = {}
#         for df_col in df.columns:
#             if df_col == 'Rel_H_%':
#                 df_column_rename_map[df_col] = 'Rel_H_Pct'
#             elif df_col.lower() == 'rel_h': 
#                  df_column_rename_map[df_col] = 'Rel_H_Pct'

#         if df_column_rename_map:
#             df = df.rename(columns=df_column_rename_map)
#             print(f"DEBUG: Colonnes du DataFrame renommées: {df_column_rename_map}")
#         # --- FIN DE LA CORRECTION ---


#         # ========== VÉRIFICATION 1: NOMS DE COLONNES ==========
#         print("\n" + "="*50)
#         print("VÉRIFICATION DES NOMS DE COLONNES (CASSE ET CORRESPONDANCE)")
#         print("="*50)
        
#         column_mapping_expected = {col.lower(): col for col in expected_columns}
#         new_df_columns = []
#         for col_df in df.columns:
#             if col_df in expected_columns: 
#                 new_df_columns.append(col_df)
#             elif col_df.lower() in column_mapping_expected: 
#                 new_df_columns.append(column_mapping_expected[col_df.lower()])
#             else: 
#                 new_df_columns.append(col_df)
        
#         df.columns = new_df_columns

#         df_cols_after_norm = df.columns.tolist()
#         missing_in_df = set(expected_columns) - set(df_cols_after_norm)
#         extra_in_df = set(df_cols_after_norm) - set(expected_columns)
        
#         print(f"\nColonnes attendues par la DB: {expected_columns}")
#         print(f"Colonnes du DataFrame (après normalisation): {df_cols_after_norm}")

#         if missing_in_df:
#             print(f"\n⚠️ ATTENTION: Colonnes attendues mais absentes du DataFrame:")
#             for col in missing_in_df:
#                 print(f"- {col}")
#                 df[col] = None 
        
#         if extra_in_df:
#             print(f"\nℹ️ INFO: Colonnes supplémentaires dans le DataFrame:")
#             for col in extra_in_df:
#                 print(f"- {col}")
#             df = df.drop(columns=list(extra_in_df))

#         if not missing_in_df and not extra_in_df and set(expected_columns) == set(df.columns.tolist()):
#             print("\n✅ Tous les noms de colonnes correspondent exactement")
#         else:
#             print("\n❌ Des divergences de noms de colonnes existent")

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
#                 df[col] = df[col].replace(nan_value_strings, np.nan)
                
#                 if pg_base_type in ['timestamp', 'date']:
#                     df[col] = pd.to_datetime(df[col], errors='coerce')
#                     print("- Converti en datetime")
#                 elif pg_base_type in ['float', 'integer']:
#                     df[col] = pd.to_numeric(df[col], errors='coerce')
#                     print("- Converti en numérique")
#                 elif pg_base_type == 'boolean': # Ajout de la gestion des booléens
#                     # Convertir les valeurs en booléen. Gère True, False, 1, 0, 'True', 'False', etc.
#                     df[col] = df[col].apply(lambda x: bool(x) if pd.notna(x) else None)
#                     print("- Converti en booléen")
                
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
        
#         for col in expected_columns:
#             pg_type_str = columns_config.get(col, 'text')
#             pg_base_type = pg_type_str.split()[0].lower()

#             if pg_base_type == 'timestamp':
#                 # Convertir Timestamp en chaîne formatée, y compris les fuseaux horaires si nécessaire
#                 type_converters.append(lambda x: x.isoformat(timespec='minutes') if pd.notna(x) else None)
#             elif pg_base_type == 'date':
#                 type_converters.append(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
#             elif pg_base_type == 'float':
#                 type_converters.append(lambda x: float(x) if pd.notna(x) else None)
#             elif pg_base_type == 'integer':
#                 # Assurez-vous que la conversion en int est sûre (pas de décimales)
#                 type_converters.append(lambda x: int(x) if pd.notna(x) and pd.api.types.is_integer(x) else None)
#             elif pg_base_type == 'boolean': # Convertisseur pour les booléens
#                 type_converters.append(lambda x: bool(x) if pd.notna(x) else None)
#             else:
#                 type_converters.append(lambda x: str(x) if pd.notna(x) else None)

#         # CORRECTION ICI : Utilisation d'enumerate avec df.itertuples()
#         for i, row_tuple in enumerate(df.itertuples(index=False)): # i sera l'index numérique
#             try:
#                 # Accédez aux valeurs par index numérique si vous utilisez itertuples(index=False)
#                 # Ou par attribut si vous utilisez itertuples() sans index=False
#                 # Pour la clarté, nous allons mapper les noms de colonnes aux positions pour itertuples(index=False)
#                 # Assumons que l'ordre des colonnes dans row_tuple correspond à expected_columns
#                 row_values = [type_converters[j](row_tuple[j]) for j in range(len(expected_columns))]
#                 data_to_insert.append(tuple(row_values))
                
#                 if i < 2: # 'i' est l'index numérique de la ligne
#                     print(f"\nExemple ligne {i}:")
#                     for j, col in enumerate(expected_columns):
#                         val = row_values[j]
#                         print(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                        
#             except Exception as e:
#                 # Utilisez df.index[i] pour obtenir le Datetime original de la ligne problématique
#                 print(f"\n❌ ERREUR ligne {df.index[i]}: {str(e)}") 
#                 print("Valeurs:", {col: df.loc[df.index[i], col] for col in df.columns}) # Affiche les valeurs originales
#                 traceback.print_exc()
#                 continue

#         print(f"\nTotal lignes préparées: {len(data_to_insert)}")
#         print("="*50 + "\n")

#         # ========== CONNEXION À LA BASE (Logique existante) ==========
#         conn.autocommit = True

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
#                 if not create_station_table(station, processing_type): # Assurez-vous que create_station_table est importé ou défini
#                     raise Exception(f"Échec de la création de la table '{table_name}'")
#                 print("✅ Table créée avec succès")

#             # ========== NOUVELLE VÉRIFICATION ROBUSTE DE CLÉ PRIMAIRE ==========
#             pk_col = None
#             try:
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
                
#                 if not pk_info:
#                     print("ℹ️ Essai méthode alternative de détection de clé primaire...")
#                     try:
#                         cursor.execute(f"""
#                             SELECT a.attname
#                             FROM pg_index i
#                             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
#                             WHERE i.indrelid = %s::regclass AND i.indisprimary
#                         """, (sql.Literal(table_name).as_string(cursor),)) # Utilisation de sql.Literal pour le nom de table
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
            
#             # Correction: Utilisez len(expected_columns) pour vérifier la taille de la ligne préparée
#             # et len(df.columns) pour la taille du DataFrame. Ils doivent être cohérents.
#             print(f"\nNombre de colonnes attendues pour l'insertion: {len(expected_columns)}")
#             print(f"Nombre de colonnes dans le DataFrame après préparation: {len(df.columns)}") 
            
#             if len(expected_columns) != len(df.columns):
#                 print("\n❌ ERREUR: Nombre de colonnes incompatible entre expected_columns et le DataFrame final!")
#                 print("Colonnes attendues:", expected_columns)
#                 print("Colonnes DataFrame:", df.columns.tolist())
#                 raise ValueError("Incompatibilité de colonnes après préparation") 

#             if data_to_insert:
#                 first_row = data_to_insert[0]
#                 print(f"\nPremière ligne de données ({len(first_row)} valeurs):")
#                 for val, col in zip(first_row, expected_columns):
#                     print(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                
#                 try:
#                     mogrified = cursor.mogrify(query, first_row).decode('utf-8')
#                     print("\n✅ Test mogrify réussi:")
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
#             batch_size = 10000
#             start_time = datetime.now()
            
#             print(f"\nDébut de l'insertion de {len(data_to_insert)} lignes par lots de {batch_size}...")
            
#             for i in range(0, len(data_to_insert), batch_size):
#                 batch = data_to_insert[i:i + batch_size]
#                 try:
#                     extras.execute_batch(cursor, query, batch)
#                     conn.commit() # Commit après chaque lot pour assurer la persistance
                    
#                     if (i // batch_size) % 10 == 0:
#                         elapsed = (datetime.now() - start_time).total_seconds()
#                         print(f"Lot {i//batch_size + 1}: {len(batch)} lignes traitées en {elapsed:.2f} secondes au total...")
                        
#                 except Exception as e:
#                     conn.rollback()
#                     print(f"\n❌ ERREUR lot {i//batch_size + 1}: {str(e)}")
#                     if batch:
#                         print("Exemple de ligne problématique:", batch[0])
#                     traceback.print_exc()
#                     raise 

#             total_time = (datetime.now() - start_time).total_seconds()
#             print(f"\n✅ Traitement d'insertion terminé pour '{station}': {len(data_to_insert)} lignes préparées en {total_time:.2f} secondes.")
#             print("Note: Le nombre exact de lignes insérées peut différer en raison de la clause ON CONFLICT.")
#             return True

#     except Exception as e:
#         print(f"\n❌❌❌ ERREUR CRITIQUE lors de la sauvegarde pour '{station}': {str(e)}")
#         traceback.print_exc()
        
#         if 'df' in locals():
#             print("\nÉtat du DataFrame au moment de l'erreur:")
#             print(f"Shape: {df.shape}")
#             print("5 premières lignes:")
#             print(df.head().to_string())
            
#         return False



###############  Jeudi 23 Juil 2025 ###############
import logging
# def save_to_database(df: pd.DataFrame, station: str, conn, processing_type: str = 'before') -> bool:
#     """
#     Sauvegarde un DataFrame dans la base de données, avec vérifications complètes et journalisation détaillée.
#     La connexion à la base de données est passée en argument.
#     """
#     original_df_state = None # Pour le débogage
#     try:
#         # Vérification initiale
#         if df.empty:
#             logging.warning(f"DataFrame vide reçu pour la station '{station}' ({processing_type}), aucune donnée à sauvegarder.")
#             return True

#         table_name = station.strip()

#         db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')

#         if not conn:
#             raise ConnectionError(f"Connexion à la base de données non fournie ou nulle pour '{db_name}'")

#         columns_config = get_station_columns(station, processing_type)
#         if not columns_config:
#             raise ValueError(f"Configuration des colonnes manquante pour la station '{station}' ({processing_type}). Impossible de sauvegarder.")

#         expected_columns = list(columns_config.keys())

#         # --- DÉBUT DE LA VRAIE CORRECTION POUR DATETIMEINDEX ---
#         # Si le DataFrame a un DatetimeIndex, le transformer en colonne 'Datetime'.
#         # C'est la ligne la plus importante pour résoudre le problème de NULL.
#         if isinstance(df.index, pd.DatetimeIndex):
#             # Utilisez reset_index(). Elle transformera l'index 'Datetime' en une colonne 'Datetime'.
#             # Si l'index n'avait pas de nom, la nouvelle colonne serait 'index',
#             # mais votre pipeline assure qu'il s'appelle 'Datetime'.
#             df_processed = df.reset_index()
#             logging.info(f"DatetimeIndex converti en colonne 'Datetime'. Nouvelles colonnes: {df_processed.columns.tolist()}")
#         else:
#             df_processed = df.copy() # Travailler sur une copie pour éviter SettingWithCopyWarning
#             logging.info("Le DataFrame n'a pas de DatetimeIndex, aucune conversion nécessaire pour l'index.")

#         # Assurez-vous que la colonne 'Datetime' est bien présente après reset_index()
#         if 'Datetime' not in df_processed.columns:
#             raise ValueError(f"La colonne 'Datetime' est introuvable après la conversion de l'index pour la station {station}.")
#         # --- FIN DE LA VRAIE CORRECTION ---


#         # --- CORRECTION POUR `Rel_H_%` (déjà présente, juste réorganiser le contexte) ---
#         df_column_rename_map = {}
#         for df_col in df_processed.columns: # Utilisez df_processed ici
#             if df_col == 'Rel_H_%':
#                 df_column_rename_map[df_col] = 'Rel_H_Pct'
#             elif df_col.lower() == 'rel_h': 
#                  df_column_rename_map[df_col] = 'Rel_H_Pct'

#         if df_column_rename_map:
#             df_processed = df_processed.rename(columns=df_column_rename_map) # Renommer sur df_processed
#             logging.debug(f"Colonnes du DataFrame renommées: {df_column_rename_map}")
#         # --- FIN DE LA CORRECTION ---


#         # ========== VÉRIFICATION 1: NOMS DE COLONNES ==========
#         logging.info("\n" + "="*50)
#         logging.info("VÉRIFICATION DES NOMS DE COLONNES (CASSE ET CORRESPONDANCE)")
#         logging.info("="*50)
        
#         column_mapping_expected = {col.lower(): col for col in expected_columns}
#         new_df_columns = []
#         for col_df in df_processed.columns: # Utilisez df_processed ici
#             if col_df in expected_columns: 
#                 new_df_columns.append(col_df)
#             elif col_df.lower() in column_mapping_expected: 
#                 new_df_columns.append(column_mapping_expected[col_df.lower()])
#             else: 
#                 new_df_columns.append(col_df)
        
#         df_processed.columns = new_df_columns # Appliquer les renommages sur df_processed

#         df_cols_after_norm = df_processed.columns.tolist() # Utilisez df_processed ici
#         missing_in_df = set(expected_columns) - set(df_cols_after_norm)
#         extra_in_df = set(df_cols_after_norm) - set(expected_columns)
        
#         logging.info(f"\nColonnes attendues par la DB: {expected_columns}")
#         logging.info(f"Colonnes du DataFrame (après normalisation): {df_cols_after_norm}")

#         if missing_in_df:
#             logging.warning(f"\n⚠️ ATTENTION: Colonnes attendues mais absentes du DataFrame:")
#             for col in missing_in_df:
#                 logging.warning(f"- {col}")
#                 df_processed[col] = None # Utiliser df_processed
        
#         if extra_in_df:
#             logging.info(f"\nℹ️ INFO: Colonnes supplémentaires dans le DataFrame:")
#             for col in extra_in_df:
#                 logging.info(f"- {col}")
#             df_processed = df_processed.drop(columns=list(extra_in_df)) # Utiliser df_processed

#         if not missing_in_df and not extra_in_df and set(expected_columns) == set(df_processed.columns.tolist()): # Utiliser df_processed
#             logging.info("\n✅ Tous les noms de colonnes correspondent exactement")
#         else:
#             logging.warning("\n❌ Des divergences de noms de colonnes existent")

#         df_processed = df_processed[expected_columns] # Utiliser df_processed
#         logging.info("\nOrdre final des colonnes:" + str(df_processed.columns.tolist())) # Utiliser df_processed
#         logging.info("="*50 + "\n")

#         # ========== VÉRIFICATION 2: TYPES DE DONNÉES ==========
#         logging.info("\n" + "="*50)
#         logging.info("VÉRIFICATION DES TYPES DE DONNÉES ET VALEURS ANORMALES")
#         logging.info("="*50)
        
#         nan_value_strings = ['NaN', 'NAN', 'nan', '', ' ', '#VALUE!', '-', 'NULL', 'null', 'N/A', 'NA', 'None', 'NONE', 'NV', 'N.V.', '#DIV/0!']
        
#         for col, pg_type_str in columns_config.items():
#             if col not in df_processed.columns: # Utilisez df_processed ici
#                 logging.warning(f"\n⚠️ Colonne '{col}' non trouvée - ignorée")
#                 continue

#             original_dtype = str(df_processed[col].dtype) # Utilisez df_processed ici
#             pg_base_type = pg_type_str.split()[0].lower()
            
#             logging.info(f"\nColonne: {col}")
#             logging.info(f"- Type Pandas original: {original_dtype}")
#             logging.info(f"- Type PostgreSQL attendu: {pg_base_type}")
            
#             try:
#                 df_processed[col] = df_processed[col].replace(nan_value_strings, np.nan) # Utilisez df_processed ici
                
#                 if pg_base_type in ['timestamp', 'date']:
#                     df_processed[col] = pd.to_datetime(df_processed[col], errors='coerce') # Utilisez df_processed ici
#                     logging.info("- Converti en datetime")
#                 elif pg_base_type in ['float', 'integer', 'double precision', 'real']: # Added common float/int types
#                     df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce') # Utilisez df_processed ici
#                     # Convert float to int if target is integer and no decimals present
#                     if pg_base_type == 'integer' and pd.api.types.is_float_dtype(df_processed[col]):
#                         # Check for non-integer float values before converting to int
#                         if (df_processed[col].notna() & (df_processed[col] % 1 != 0)).any():
#                             logging.warning(f"Colonne '{col}' contient des décimales mais le type attendu est INTEGER. Les valeurs seront tronquées ou mises à NaN.")
#                         df_processed[col] = df_processed[col].astype('Int64', errors='coerce') # Use nullable integer type
#                     logging.info("- Converti en numérique")
#                 elif pg_base_type == 'boolean':
#                     df_processed[col] = df_processed[col].apply(lambda x: bool(x) if pd.notna(x) else None) # Utilisez df_processed ici
#                     logging.info("- Converti en booléen")
                
#                 null_count = df_processed[col].isna().sum() # Utilisez df_processed ici
#                 logging.info(f"- Valeurs NULL après conversion: {null_count}/{len(df_processed)} ({null_count/len(df_processed)*100:.2f}%)") # Utilisez df_processed ici
                
#                 if null_count > 0:
#                     sample_null = df_processed[df_processed[col].isna()].head(3) # Utilisez df_processed ici
#                     logging.info("- Exemple de lignes avec NULL:\n" + sample_null[[col]].to_string())
                    
#             except Exception as e:
#                 logging.error(f"\n❌ ERREUR DE CONVERSION pour '{col}': {str(e)}")
#                 # Utilisation de df_processed pour le débogage
#                 problematic_vals = df_processed[col][df_processed[col].notna() & pd.to_numeric(df_processed[col], errors='coerce').isna()]
#                 if not problematic_vals.empty:
#                     logging.error("- Valeurs problématiques:" + str(problematic_vals.unique()[:5]))
#                 traceback.print_exc()

#         logging.info("\nRésumé des types après conversion:")
#         logging.info(str(df_processed.dtypes)) # Utilisez df_processed ici
#         logging.info("="*50 + "\n")

#         # ========== PRÉPARATION DES DONNÉES ==========
#         logging.info("\n" + "="*50)
#         logging.info("PRÉPARATION DES DONNÉES POUR INSERTION")
#         logging.info("="*50)
        
#         data_to_insert = []
#         type_converters = []
        
#         for col in expected_columns:
#             pg_type_str = columns_config.get(col, 'text')
#             pg_base_type = pg_type_str.split()[0].lower()

#             # Les convertisseurs sont appliqués sur les valeurs de la ligne
#             if pg_base_type in ['timestamp', 'timestamp with time zone']:
#                 type_converters.append(lambda x: x.isoformat(timespec='microseconds') if pd.notna(x) else None)
#             elif pg_base_type == 'date':
#                 type_converters.append(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
#             elif pg_base_type in ['float', 'double precision', 'real']:
#                 type_converters.append(lambda x: float(x) if pd.notna(x) else None)
#             elif pg_base_type == 'integer':
#                 type_converters.append(lambda x: int(x) if pd.notna(x) and pd.api.types.is_integer(x) else None)
#             elif pg_base_type == 'boolean':
#                 type_converters.append(lambda x: bool(x) if pd.notna(x) else None)
#             else:
#                 type_converters.append(lambda x: str(x) if pd.notna(x) else None)

#         # CORRECTION ICI : Utilisation de df_processed pour l'itération
#         # itertuples() est rapide et efficace. L'ordre des valeurs dans le tuple
#         # correspond à l'ordre des colonnes du DataFrame.
#         for i, row_tuple in enumerate(df_processed.itertuples(index=False)):
#             try:
#                 row_values = [type_converters[j](row_tuple[j]) for j in range(len(expected_columns))]
#                 data_to_insert.append(tuple(row_values))
                
#                 if i < 2:
#                     logging.debug(f"\nExemple ligne {i}:")
#                     for j, col in enumerate(expected_columns):
#                         val = row_values[j]
#                         logging.debug(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                        
#             except Exception as e:
#                 # Utiliser l'index du DataFrame original pour un meilleur contexte de débogage si possible.
#                 # Mais puisque nous avons fait reset_index, l'index est maintenant numérique.
#                 logging.error(f"\n❌ ERREUR lors de la préparation de la ligne {i}: {str(e)}")
#                 # Affiche les valeurs originales de la ligne problématique à partir du DataFrame
#                 logging.error("Valeurs brutes de la ligne problématique (peut différer après conversion de type):")
#                 logging.error(str(df_processed.iloc[i].to_dict()))
#                 traceback.print_exc()
#                 continue

#         logging.info(f"\nTotal lignes préparées: {len(data_to_insert)}")
#         logging.info("="*50 + "\n")

#         # ========== CONNEXION À LA BASE (Logique existante) ==========
#         # Assurez-vous que `conn` est une connexion psycopg2 valide.
#         # Si vous utilisez un engine SQLAlchemy, la gestion du commit/rollback est différente.
#         # Pour une connexion psycopg2:
#         conn.autocommit = True # Cela rend chaque exécution de requête immédiate, peut ne pas être idéal pour les transactions

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
#                 logging.info(f"\nℹ️ La table '{table_name}' n'existe pas, création...")
#                 # Assurez-vous que create_station_table est compatible avec votre `conn` (psycopg2)
#                 if not create_station_table(station, processing_type):
#                     raise Exception(f"Échec de la création de la table '{table_name}'")
#                 logging.info("✅ Table créée avec succès")

#             # ========== NOUVELLE VÉRIFICATION ROBUSTE DE CLÉ PRIMAIRE ==========
#             pk_col = None
#             try:
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
                
#                 if not pk_info:
#                     logging.info("ℹ️ Essai méthode alternative de détection de clé primaire...")
#                     try:
#                         # Assurez-vous que `sql` est importé de `psycopg2`
#                         from psycopg2 import sql 
#                         cursor.execute(f"""
#                             SELECT a.attname
#                             FROM pg_index i
#                             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
#                             WHERE i.indrelid = %s::regclass AND i.indisprimary
#                         """, (sql.Literal(table_name).as_string(cursor),))
#                         pk_info = cursor.fetchone()
#                     except Exception as e:
#                         logging.warning(f"⚠️ Échec méthode alternative: {str(e)}")
#                         pk_info = None
                
#                 if pk_info:
#                     pk_col = pk_info[0]
#                     logging.info(f"✅ Clé primaire détectée: {pk_col}")
#                 else:
#                     logging.info("ℹ️ Aucune clé primaire détectée")
                    
#             except Exception as e:
#                 logging.warning(f"⚠️ Erreur lors de la détection de la clé primaire: {str(e)}")
#                 if 'Datetime' in expected_columns:
#                     pk_col = 'Datetime'
#                     logging.info("ℹ️ Utilisation de 'Datetime' comme clé primaire par défaut")
#                 traceback.print_exc()

#             cols_sql = ', '.join([f'"{col}"' for col in expected_columns])
#             placeholders = ', '.join(['%s'] * len(expected_columns))
            
#             if pk_col:
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders})
#                     ON CONFLICT ("{pk_col}") DO NOTHING
#                 """
#                 logging.info("ℹ️ Utilisation de ON CONFLICT avec la clé primaire")
#             else:
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders})
#                 """
#                 logging.info("ℹ️ Insertion simple (aucune clé primaire détectée)")

#             logging.info("\nRequête SQL générée:\n" + query)

#             # ========== VÉRIFICATION FINALE AVANT INSERTION ==========
#             logging.info("\n" + "="*50)
#             logging.info("VÉRIFICATION FINALE AVANT INSERTION")
#             logging.info("="*50)
            
#             logging.info(f"\nNombre de colonnes attendues pour l'insertion: {len(expected_columns)}")
#             logging.info(f"Nombre de colonnes dans le DataFrame après préparation: {len(df_processed.columns)}") 
            
#             # Cette vérification est essentielle maintenant que df_processed a été manipulé
#             if len(expected_columns) != len(df_processed.columns):
#                 logging.error("\n❌ ERREUR: Nombre de colonnes incompatible entre expected_columns et le DataFrame final!")
#                 logging.error("Colonnes attendues:" + str(expected_columns))
#                 logging.error("Colonnes DataFrame:" + str(df_processed.columns.tolist()))
#                 raise ValueError("Incompatibilité de colonnes après préparation") 

#             if data_to_insert:
#                 first_row = data_to_insert[0]
#                 logging.info(f"\nPremière ligne de données ({len(first_row)} valeurs):")
#                 for val, col in zip(first_row, expected_columns):
#                     logging.info(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                
#                 try:
#                     # Assurez-vous que `psycopg2.sql` est importé si vous utilisez sql.Literal
#                     # ou si vous voulez mogrify la query avec des paramètres pour vérifier
#                     # Sinon, mogrify(query, first_row) est pour les requêtes dynamiques
#                     # ici, la query est déjà formatée.
#                     # Utiliser cursor.mogrify(query, first_row) si query a des %s
#                     # Ou juste imprimer query si tout est statique sauf les valeurs.
#                     mogrified = cursor.mogrify(query, first_row).decode('utf-8')
#                     logging.info("\n✅ Test mogrify réussi:\n" + mogrified[:500] + ("..." if len(mogrified) > 500 else ""))
#                 except Exception as e:
#                     logging.error("\n❌ ERREUR mogrify:" + str(e))
#                     traceback.print_exc()
#                     raise 
#             else:
#                 logging.warning("\n⚠️ Aucune donnée à insérer!")
#                 return False

#             logging.info("="*50 + "\n")

#             # ========== INSERTION ==========
#             batch_size = 10000
#             start_time = datetime.now()
            
#             logging.info(f"\nDébut de l'insertion de {len(data_to_insert)} lignes par lots de {batch_size}...")
            
#             for i in range(0, len(data_to_insert), batch_size):
#                 batch = data_to_insert[i:i + batch_size]
#                 try:
#                     # Assurez-vous que `psycopg2.extras` est importé pour execute_batch
#                     from psycopg2 import extras
#                     extras.execute_batch(cursor, query, batch)
#                     # Comme conn.autocommit = True, chaque execute_batch est automatiquement commitée.
#                     # Un conn.commit() explicite ici serait redondant avec autocommit=True,
#                     # mais nécessaire si autocommit=False.
#                     # Il est généralement préférable de gérer les commits manuellement pour les transactions
#                     # Si vous voulez un seul commit à la fin, mettez conn.autocommit = False au début
#                     # et un seul conn.commit() juste avant le 'return True' final.
                    
#                     if (i // batch_size) % 10 == 0:
#                         elapsed = (datetime.now() - start_time).total_seconds()
#                         logging.info(f"Lot {i//batch_size + 1}: {len(batch)} lignes traitées en {elapsed:.2f} secondes au total...")
                        
#                 except Exception as e:
#                     # Si autocommit est True, le rollback n'aura pas d'effet sur les lots déjà traités.
#                     # Si autocommit est False, conn.rollback() annulera tout depuis le dernier commit.
#                     conn.rollback() # Gardez-le pour le cas où autocommit serait False
#                     logging.error(f"\n❌ ERREUR lot {i//batch_size + 1}: {str(e)}")
#                     if batch:
#                         logging.error("Exemple de ligne problématique:" + str(batch[0]))
#                     traceback.print_exc()
#                     raise 

#             total_time = (datetime.now() - start_time).total_seconds()
#             logging.info(f"\n✅ Traitement d'insertion terminé pour '{station}': {len(data_to_insert)} lignes préparées en {total_time:.2f} secondes.")
#             logging.info("Note: Le nombre exact de lignes insérées peut différer en raison de la clause ON CONFLICT.")
#             return True

#     except Exception as e:
#         logging.error(f"\n❌❌❌ ERREUR CRITIQUE lors de la sauvegarde pour '{station}': {str(e)}")
#         traceback.print_exc()
        
#         # Utilisez df_processed pour le débogage si elle est définie
#         if 'df_processed' in locals():
#             logging.error("\nÉtat du DataFrame au moment de l'erreur:")
#             logging.error(f"Shape: {df_processed.shape}")
#             logging.error("5 premières lignes:\n" + df_processed.head().to_string())
            
#         return False

##########################


# def save_to_database(df: pd.DataFrame, station: str, conn, processing_type: str = 'before') -> bool:
#     """
#     Sauvegarde un DataFrame dans la base de données, avec vérifications complètes et journalisation détaillée.
#     La connexion à la base de données est passée en argument.
#     """
#     original_df_state = None # Pour le débogage
#     try:
#         # Vérification initiale
#         if df.empty:
#             logging.warning(f"DataFrame vide reçu pour la station '{station}' ({processing_type}), aucune donnée à sauvegarder.")
#             return True

#         table_name = station.strip()

#         # db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')
#         if processing_type == 'before':
#             db_key = 'before'
#         elif processing_type == 'after':
#             db_key = 'after'
#         elif processing_type == 'missing_before':
#             db_key = 'missing_before'
#         elif processing_type == 'missing_after':
#             db_key = 'missing_after'
#         else:
#             logging.error(f"Type de traitement inconnu '{processing_type}'. Impossible de déterminer la base de données.")
#             return False

#         if not conn:
#             raise ConnectionError(f"Connexion à la base de données non fournie ou nulle pour '{processing_type}'")

#         columns_config = get_station_columns(station, processing_type)
#         if not columns_config:
#             raise ValueError(f"Configuration des colonnes manquante pour la station '{station}' ({processing_type}). Impossible de sauvegarder.")

#         expected_columns = list(columns_config.keys())

#         # --- DÉBUT DE LA VRAIE CORRECTION POUR DATETIMEINDEX ---
#         # Si le DataFrame a un DatetimeIndex, le transformer en colonne 'Datetime'.
#         # C'est la ligne la plus importante pour résoudre le problème de NULL.
#         if isinstance(df.index, pd.DatetimeIndex):
#             # Utilisez reset_index(). Elle transformera l'index 'Datetime' en une colonne 'Datetime'.
#             # Si l'index n'avait pas de nom, la nouvelle colonne serait 'index',
#             # mais votre pipeline assure qu'il s'appelle 'Datetime'.
#             df_processed = df.reset_index()
#             logging.info(f"DatetimeIndex converti en colonne 'Datetime'. Nouvelles colonnes: {df_processed.columns.tolist()}")
#         else:
#             df_processed = df.copy() # Travailler sur une copie pour éviter SettingWithCopyWarning
#             logging.info("Le DataFrame n'a pas de DatetimeIndex, aucune conversion nécessaire pour l'index.")

#         # Assurez-vous que la colonne 'Datetime' est bien présente après reset_index()
#         if 'Datetime' not in df_processed.columns:
#             raise ValueError(f"La colonne 'Datetime' est introuvable après la conversion de l'index pour la station {station}.")
#         # --- FIN DE LA VRAIE CORRECTION ---


#         # --- CORRECTION POUR `Rel_H_%` (déjà présente, juste réorganiser le contexte) ---
#         df_column_rename_map = {}
#         for df_col in df_processed.columns: # Utilisez df_processed ici
#             if df_col == 'Rel_H_%':
#                 df_column_rename_map[df_col] = 'Rel_H_Pct'
#             elif df_col.lower() == 'rel_h': 
#                  df_column_rename_map[df_col] = 'Rel_H_Pct'

#         if df_column_rename_map:
#             df_processed = df_processed.rename(columns=df_column_rename_map) # Renommer sur df_processed
#             logging.debug(f"Colonnes du DataFrame renommées: {df_column_rename_map}")
#         # --- FIN DE LA CORRECTION ---


#         # ========== VÉRIFICATION 1: NOMS DE COLONNES ==========
#         logging.info("\n" + "="*50)
#         logging.info("VÉRIFICATION DES NOMS DE COLONNES (CASSE ET CORRESPONDANCE)")
#         logging.info("="*50)
        
#         column_mapping_expected = {col.lower(): col for col in expected_columns}
#         new_df_columns = []
#         for col_df in df_processed.columns: # Utilisez df_processed ici
#             if col_df in expected_columns: 
#                 new_df_columns.append(col_df)
#             elif col_df.lower() in column_mapping_expected: 
#                 new_df_columns.append(column_mapping_expected[col_df.lower()])
#             else: 
#                 new_df_columns.append(col_df)
        
#         df_processed.columns = new_df_columns # Appliquer les renommages sur df_processed

#         df_cols_after_norm = df_processed.columns.tolist() # Utilisez df_processed ici
#         missing_in_df = set(expected_columns) - set(df_cols_after_norm)
#         extra_in_df = set(df_cols_after_norm) - set(expected_columns)
        
#         logging.info(f"\nColonnes attendues par la DB: {expected_columns}")
#         logging.info(f"Colonnes du DataFrame (après normalisation): {df_cols_after_norm}")

#         if missing_in_df:
#             logging.warning(f"\n⚠️ ATTENTION: Colonnes attendues mais absentes du DataFrame:")
#             for col in missing_in_df:
#                 logging.warning(f"- {col}")
#                 df_processed[col] = None # Utiliser df_processed
        
#         if extra_in_df:
#             logging.info(f"\nℹ️ INFO: Colonnes supplémentaires dans le DataFrame:")
#             for col in extra_in_df:
#                 logging.info(f"- {col}")
#             df_processed = df_processed.drop(columns=list(extra_in_df)) # Utiliser df_processed

#         if not missing_in_df and not extra_in_df and set(expected_columns) == set(df_processed.columns.tolist()): # Utiliser df_processed
#             logging.info("\n✅ Tous les noms de colonnes correspondent exactement")
#         else:
#             logging.warning("\n❌ Des divergences de noms de colonnes existent")

#         df_processed = df_processed[expected_columns] # Utiliser df_processed
#         logging.info("\nOrdre final des colonnes:" + str(df_processed.columns.tolist())) # Utiliser df_processed
#         logging.info("="*50 + "\n")

#         # ========== VÉRIFICATION 2: TYPES DE DONNÉES ==========
#         logging.info("\n" + "="*50)
#         logging.info("VÉRIFICATION DES TYPES DE DONNÉES ET VALEURS ANORMALES")
#         logging.info("="*50)
        
#         nan_value_strings = ['NaN', 'NAN', 'nan', '', ' ', '#VALUE!', '-', 'NULL', 'null', 'N/A', 'NA', 'None', 'NONE', 'NV', 'N.V.', '#DIV/0!']
        
#         for col, pg_type_str in columns_config.items():
#             if col not in df_processed.columns: # Utilisez df_processed ici
#                 logging.warning(f"\n⚠️ Colonne '{col}' non trouvée - ignorée")
#                 continue

#             original_dtype = str(df_processed[col].dtype) # Utilisez df_processed ici
#             pg_base_type = pg_type_str.split()[0].lower()
            
#             logging.info(f"\nColonne: {col}")
#             logging.info(f"- Type Pandas original: {original_dtype}")
#             logging.info(f"- Type PostgreSQL attendu: {pg_base_type}")
            
#             try:
#                 df_processed[col] = df_processed[col].replace(nan_value_strings, np.nan) # Utilisez df_processed ici
                
#                 if pg_base_type in ['timestamp', 'date']:
#                     df_processed[col] = pd.to_datetime(df_processed[col], errors='coerce') # Utilisez df_processed ici
#                     logging.info("- Converti en datetime")
#                 elif pg_base_type in ['float', 'integer', 'double precision', 'real']: # Added common float/int types
#                     df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce') # Utilisez df_processed ici
#                     # Convert float to int if target is integer and no decimals present
#                     if pg_base_type == 'integer' and pd.api.types.is_float_dtype(df_processed[col]):
#                         # Check for non-integer float values before converting to int
#                         if (df_processed[col].notna() & (df_processed[col] % 1 != 0)).any():
#                             logging.warning(f"Colonne '{col}' contient des décimales mais le type attendu est INTEGER. Les valeurs seront tronquées ou mises à NaN.")
#                         df_processed[col] = df_processed[col].astype('Int64', errors='coerce') # Use nullable integer type
#                     logging.info("- Converti en numérique")
#                 elif pg_base_type == 'boolean':
#                     df_processed[col] = df_processed[col].apply(lambda x: bool(x) if pd.notna(x) else None) # Utilisez df_processed ici
#                     logging.info("- Converti en booléen")
                
#                 null_count = df_processed[col].isna().sum() # Utilisez df_processed ici
#                 logging.info(f"- Valeurs NULL après conversion: {null_count}/{len(df_processed)} ({null_count/len(df_processed)*100:.2f}%)") # Utilisez df_processed ici
                
#                 if null_count > 0:
#                     sample_null = df_processed[df_processed[col].isna()].head(3) # Utilisez df_processed ici
#                     logging.info("- Exemple de lignes avec NULL:\n" + sample_null[[col]].to_string())
                    
#             except Exception as e:
#                 logging.error(f"\n❌ ERREUR DE CONVERSION pour '{col}': {str(e)}")
#                 # Utilisation de df_processed pour le débogage
#                 problematic_vals = df_processed[col][df_processed[col].notna() & pd.to_numeric(df_processed[col], errors='coerce').isna()]
#                 if not problematic_vals.empty:
#                     logging.error("- Valeurs problématiques:" + str(problematic_vals.unique()[:5]))
#                 traceback.print_exc()

#         logging.info("\nRésumé des types après conversion:")
#         logging.info(str(df_processed.dtypes)) # Utilisez df_processed ici
#         logging.info("="*50 + "\n")

#         # ========== PRÉPARATION DES DONNÉES ==========
#         logging.info("\n" + "="*50)
#         logging.info("PRÉPARATION DES DONNÉES POUR INSERTION")
#         logging.info("="*50)
        
#         data_to_insert = []
#         type_converters = []
        
#         for col in expected_columns:
#             pg_type_str = columns_config.get(col, 'text')
#             pg_base_type = pg_type_str.split()[0].lower()

#             # Les convertisseurs sont appliqués sur les valeurs de la ligne
#             if pg_base_type in ['timestamp', 'timestamp with time zone']:
#                 type_converters.append(lambda x: x.isoformat(timespec='microseconds') if pd.notna(x) else None)
#             elif pg_base_type == 'date':
#                 type_converters.append(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
#             elif pg_base_type in ['float', 'double precision', 'real']:
#                 type_converters.append(lambda x: float(x) if pd.notna(x) else None)
#             elif pg_base_type == 'integer':
#                 type_converters.append(lambda x: int(x) if pd.notna(x) and pd.api.types.is_integer(x) else None)
#             elif pg_base_type == 'boolean':
#                 type_converters.append(lambda x: bool(x) if pd.notna(x) else None)
#             else:
#                 type_converters.append(lambda x: str(x) if pd.notna(x) else None)

#         # CORRECTION ICI : Utilisation de df_processed pour l'itération
#         # itertuples() est rapide et efficace. L'ordre des valeurs dans le tuple
#         # correspond à l'ordre des colonnes du DataFrame.
#         for i, row_tuple in enumerate(df_processed.itertuples(index=False)):
#             try:
#                 row_values = [type_converters[j](row_tuple[j]) for j in range(len(expected_columns))]
#                 data_to_insert.append(tuple(row_values))
                
#                 if i < 2:
#                     logging.debug(f"\nExemple ligne {i}:")
#                     for j, col in enumerate(expected_columns):
#                         val = row_values[j]
#                         logging.debug(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                        
#             except Exception as e:
#                 # Utiliser l'index du DataFrame original pour un meilleur contexte de débogage si possible.
#                 # Mais puisque nous avons fait reset_index, l'index est maintenant numérique.
#                 logging.error(f"\n❌ ERREUR lors de la préparation de la ligne {i}: {str(e)}")
#                 # Affiche les valeurs originales de la ligne problématique à partir du DataFrame
#                 logging.error("Valeurs brutes de la ligne problématique (peut différer après conversion de type):")
#                 logging.error(str(df_processed.iloc[i].to_dict()))
#                 traceback.print_exc()
#                 continue

#         logging.info(f"\nTotal lignes préparées: {len(data_to_insert)}")
#         logging.info("="*50 + "\n")

#          # ========== CONNEXION À LA BASE (Logique existante) ==========
#         # `conn` est passé en argument. Assurez-vous qu'il est valide.
#         if not conn:
#             raise ConnectionError(f"Connexion à la base de données non fournie ou nulle pour la station '{station}'")

#         conn.autocommit = True

#         with conn.cursor() as cursor:
#             # Vérification de l'existence de la table (déjà dans votre code, utilisant votre create_station_table)
#             cursor.execute("""
#                 SELECT EXISTS (
#                     SELECT FROM information_schema.tables
#                     WHERE table_schema = 'public' AND table_name = %s
#                 )
#             """, (table_name,))
#             table_exists = cursor.fetchone()[0]
            
#             if not table_exists:
#                 logging.info(f"\nℹ️ La table '{table_name}' n'existe pas, création...")
#                 if not create_station_table(station, processing_type): # Appeler votre fonction
#                     raise Exception(f"Échec de la création de la table '{table_name}'")
#                 logging.info("✅ Table créée avec succès")

#             # ========== DÉTECTION ET CONSTRUCTION DE LA REQUÊTE SQL ==========
#             pk_col = None
#             cols_to_insert_in_query = expected_columns.copy() # Colonnes à inclure dans l'INSERT
            
#             # Détecter la clé primaire basée sur le processing_type
#             if processing_type in ['before', 'after']:
#                 if 'Datetime' in columns_config: # Vérifier si Datetime est bien dans le schéma
#                     pk_col = 'Datetime'
#                     logging.info(f"✅ Clé primaire détectée pour '{processing_type}': '{pk_col}'")
#                 else:
#                     logging.warning(f"⚠️ 'Datetime' n'est pas configuré comme PK pour '{processing_type}'.")
#             elif processing_type in ['missing_before', 'missing_after']:
#                 if 'id' in columns_config: # Vérifier si id est bien dans le schéma
#                     pk_col = 'id'
#                     # Pour les tables 'missing', 'id' est SERIAL, donc on ne l'insère pas
#                     cols_to_insert_in_query.remove('id')
#                     logging.info(f"✅ Clé primaire détectée pour '{processing_type}': '{pk_col}' (SERIAL, non incluse dans l'INSERT direct)")
#                 else:
#                     logging.warning(f"⚠️ 'id' n'est pas configuré comme PK pour '{processing_type}'.")
            
#             # Fallback si PK non détectée par la logique processing_type
#             if not pk_col:
#                 logging.warning("ℹ️ Aucune clé primaire automatique détectée. Tentative de détection par la DB (moins efficace).")
#                 try:
#                     cursor.execute("""
#                         SELECT kcu.column_name
#                         FROM information_schema.table_constraints tc
#                         JOIN information_schema.key_column_usage kcu
#                             ON tc.constraint_name = kcu.constraint_name
#                             AND tc.table_schema = kcu.table_schema
#                         WHERE tc.table_name = %s
#                         AND tc.constraint_type = 'PRIMARY KEY'
#                         LIMIT 1
#                     """, (table_name,))
#                     pk_info = cursor.fetchone()
#                     if pk_info:
#                         pk_col = pk_info[0]
#                         logging.info(f"✅ Clé primaire détectée via DB: {pk_col}")
#                         if pk_col == 'id' and 'id' in cols_to_insert_in_query: # Si 'id' est trouvé par la DB et toujours dans la liste
#                              cols_to_insert_in_query.remove('id') # S'assurer qu'il est retiré
#                     else:
#                         logging.info("ℹ️ Aucune clé primaire détectée par la DB.")
#                 except Exception as e:
#                     logging.error(f"❌ Erreur lors de la détection de la clé primaire via DB: {str(e)}")

#             cols_sql = ', '.join([f'"{col}"' for col in cols_to_insert_in_query])
#             placeholders = ', '.join(['%s'] * len(cols_to_insert_in_query))
            
#             query = ""
#             if pk_col == 'Datetime': # Votre logique originale pour Datetime
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders})
#                     ON CONFLICT ("{pk_col}") DO NOTHING;
#                 """
#                 logging.info(f"ℹ️ Utilisation de ON CONFLICT ('{pk_col}') DO NOTHING pour '{table_name}'.")
#             elif pk_col == 'id': # Votre logique pour ID (simple insert, pas de ON CONFLICT sur ID SERIAL)
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders});
#                 """
#                 logging.info(f"ℹ️ Insertion simple pour '{table_name}' (clé primaire '{pk_col}' SERIAL, non incluse dans l'INSERT).")
#             else: # Fallback pour les cas sans clé primaire détectée ou comportement inconnu
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders});
#                 """
#                 logging.warning(f"⚠️ Insertion simple sans ON CONFLICT pour '{table_name}' (aucune clé primaire pertinente pour ON CONFLICT).")

#             logging.info(f"\nRequête SQL générée:\n{query}")


#             # ========== VÉRIFICATION FINALE AVANT INSERTION ==========
#             logging.info("\n" + "="*50)
#             logging.info("VÉRIFICATION FINALE AVANT INSERTION")
#             logging.info("="*50)
            
#             logging.info(f"\nNombre de colonnes attendues pour l'insertion: {len(expected_columns)}")
#             logging.info(f"Nombre de colonnes dans le DataFrame après préparation: {len(df_processed.columns)}") 
            
#             # Cette vérification est essentielle maintenant que df_processed a été manipulé
#             if len(expected_columns) != len(df_processed.columns):
#                 logging.error("\n❌ ERREUR: Nombre de colonnes incompatible entre expected_columns et le DataFrame final!")
#                 logging.error("Colonnes attendues:" + str(expected_columns))
#                 logging.error("Colonnes DataFrame:" + str(df_processed.columns.tolist()))
#                 raise ValueError("Incompatibilité de colonnes après préparation") 

#             if data_to_insert:
#                 first_row = data_to_insert[0]
#                 logging.info(f"\nPremière ligne de données ({len(first_row)} valeurs):")
#                 for val, col in zip(first_row, expected_columns):
#                     logging.info(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                
#                 try:
#                     # Assurez-vous que `psycopg2.sql` est importé si vous utilisez sql.Literal
#                     # ou si vous voulez mogrify la query avec des paramètres pour vérifier
#                     # Sinon, mogrify(query, first_row) est pour les requêtes dynamiques
#                     # ici, la query est déjà formatée.
#                     # Utiliser cursor.mogrify(query, first_row) si query a des %s
#                     # Ou juste imprimer query si tout est statique sauf les valeurs.
#                     mogrified = cursor.mogrify(query, first_row).decode('utf-8')
#                     logging.info("\n✅ Test mogrify réussi:\n" + mogrified[:500] + ("..." if len(mogrified) > 500 else ""))
#                 except Exception as e:
#                     logging.error("\n❌ ERREUR mogrify:" + str(e))
#                     traceback.print_exc()
#                     raise 
#             else:
#                 logging.warning("\n⚠️ Aucune donnée à insérer!")
#                 return False

#             logging.info("="*50 + "\n")

#             # ========== INSERTION ==========
#             batch_size = 10000
#             start_time = datetime.now()
            
#             logging.info(f"\nDébut de l'insertion de {len(data_to_insert)} lignes par lots de {batch_size}...")
            
#             for i in range(0, len(data_to_insert), batch_size):
#                 batch = data_to_insert[i:i + batch_size]
#                 try:
#                     # Assurez-vous que `psycopg2.extras` est importé pour execute_batch
#                     from psycopg2 import extras
#                     extras.execute_batch(cursor, query, batch)
#                     # Comme conn.autocommit = True, chaque execute_batch est automatiquement commitée.
#                     # Un conn.commit() explicite ici serait redondant avec autocommit=True,
#                     # mais nécessaire si autocommit=False.
#                     # Il est généralement préférable de gérer les commits manuellement pour les transactions
#                     # Si vous voulez un seul commit à la fin, mettez conn.autocommit = False au début
#                     # et un seul conn.commit() juste avant le 'return True' final.
                    
#                     if (i // batch_size) % 10 == 0:
#                         elapsed = (datetime.now() - start_time).total_seconds()
#                         logging.info(f"Lot {i//batch_size + 1}: {len(batch)} lignes traitées en {elapsed:.2f} secondes au total...")
                        
#                 except Exception as e:
#                     # Si autocommit est True, le rollback n'aura pas d'effet sur les lots déjà traités.
#                     # Si autocommit est False, conn.rollback() annulera tout depuis le dernier commit.
#                     conn.rollback() # Gardez-le pour le cas où autocommit serait False
#                     logging.error(f"\n❌ ERREUR lot {i//batch_size + 1}: {str(e)}")
#                     if batch:
#                         logging.error("Exemple de ligne problématique:" + str(batch[0]))
#                     traceback.print_exc()
#                     raise 

#             total_time = (datetime.now() - start_time).total_seconds()
#             logging.info(f"\n✅ Traitement d'insertion terminé pour '{station}': {len(data_to_insert)} lignes préparées en {total_time:.2f} secondes.")
#             logging.info("Note: Le nombre exact de lignes insérées peut différer en raison de la clause ON CONFLICT.")
#             return True

#     except Exception as e:
#         logging.error(f"\n❌❌❌ ERREUR CRITIQUE lors de la sauvegarde pour '{station}': {str(e)}")
#         traceback.print_exc()
        
#         # Utilisez df_processed pour le débogage si elle est définie
#         if 'df_processed' in locals():
#             logging.error("\nÉtat du DataFrame au moment de l'erreur:")
#             logging.error(f"Shape: {df_processed.shape}")
#             logging.error("5 premières lignes:\n" + df_processed.head().to_string())
            
#         return False


######################### Copie #################
# def get_station_columns(station: str, processing_type: str) -> Dict[str, str]:
#     """Retourne les colonnes attendues pour une station donnée selon le type de traitement"""
    
#     # Nettoyage du nom de la station
#     station = station.strip()

#     # BEFORE_PROCESSING
#     if processing_type == 'before':
#         # Bassin DANO
#         if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
#             return {
#                 "Datetime": 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI  JUSTE POUR SE LIMITER A MINUTES
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         elif station in ['Lare', 'Tambiri 2']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
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
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_mm': 'float',
#                 'BP_mbar_Avg': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         # Bassin DASSARI
#         elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         elif station == 'Ouriyori 1':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         # Bassin VEA SISSILI
#         elif station in ['Oualem', 'Nebou', 'Nabugubulle', 'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabiisi']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date', # Garde Date comme date pure
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'BP_mbar_Avg': 'float'
#             }


#         elif station == 'Manyoro':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 #'BP_mbar_Avg': 'float'
#             }
#         elif station == 'Atampisi':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         elif station == 'Aniabisi':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
    
#     # AFTER_PROCESSING
#     elif processing_type == 'after':
#         # Bassin DANO
#         if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp', # Ces colonnes peuvent rester timestamp (elles ne sont pas forcément à la minute)
#                 'sunset_time_utc': 'timestamp',   # ou passer à timestamp(0) si c'est votre exigence pour elles aussi.
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         elif station in ['Lare', 'Tambiri 2']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
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
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_mm': 'float',
#                 'BP_mbar_Avg': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float'
#             }
#         # Bassin DASSARI
#         elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
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
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Year': 'integer',
#                 'Month': 'integer', 
#                 'Day': 'integer',
#                 'Hour': 'integer',
#                 'Minute': 'integer',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         # Bassin VEA SISSILI
#         elif station in ['Oualem', 'Nebou', 'Nabugubulle',  'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabiisi']:
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 # 'Rain_mm': 'float', # Cette ligne est un doublon, elle devrait être supprimée ou corrigée si c'est une autre colonne.
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'BP_mbar_Avg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }

#         elif station == 'Manyoro':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 #'BP_mbar_Avg': 'float'
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
#         elif station == 'Atampisi':
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_01_mm': 'float',
#                 'Rain_02_mm': 'float',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }
    
#         elif station == 'Aniabisi': # Cette clause est également redondante si déjà dans la liste ci-dessus
#             return {
#                 'Datetime': 'timestamp(0) PRIMARY KEY', # <--- MODIFIÉ ICI
#                 'Date': 'date',
#                 'Rain_mm': 'float',
#                 'Air_Temp_Deg_C': 'float',
#                 'Rel_H_Pct': 'float', 
#                 'Solar_R_W/m^2': 'float',
#                 'Wind_Sp_m/sec': 'float',
#                 'Wind_Dir_Deg': 'float',
#                 'sunrise_time_utc': 'timestamp',
#                 'sunset_time_utc': 'timestamp',
#                 'Is_Daylight': 'boolean',
#                 'Daylight_Duration': 'float'
#             }

#     elif processing_type == 'missing_before' or processing_type == 'missing_after':
#         return{
#             'id': 'serial primary key unique',
#             'variable': 'varchar(255) not null',
#             'start_time': 'timestamp not null',      
#             'end_time': 'timestamp not null',
#             'duration_hours': 'float'
#         }
    
#     return {}

# def initialize_database():
#     """Initialise simplement les bases de données si elles n'existent pas"""
#     try:
#         db_user = DB_CONFIG['user'] 
#         if not db_user:
#             raise ValueError("L'utilisateur de la base de données (DB_USER) n'est pas configuré dans DB_CONFIG.")
        
#         # Utilisez 'admin_db' pour vous connecter à la base de données 'postgres'
#         conn = get_connection('admin_db') # <--- PAS DE 'with' ICI POUR LA CONNEXION DIRECTE
#         if not conn:
#             print("Erreur : Impossible d'établir une connexion admin pour l'initialisation des bases de données.")
#             return False
        
#         conn.autocommit = True # Assurez-vous que autocommit est VRAIMENT activé
        
#         try:
#             # Récupérez les noms réels des bases de données que vous voulez créer
#             target_db_names = [
#                 DB_NAMES['before'],
#                 DB_NAMES['after'],
#                 DB_NAMES['missing_before'],
#                 DB_NAMES['missing_after']
#             ]

#             for db_name in target_db_names:
#                 if not db_name: 
#                     print(f"Avertissement : Le nom d'une base de données est vide. Veuillez vérifier vos variables d'environnement.")
#                     continue
                
#                 # Exécuter la vérification et la création SANS UN BLOC 'with cursor()'
#                 # pour le CREATE DATABASE afin d'éviter la transaction implicite.
#                 # Créez le curseur ici, utilisez-le, puis fermez-le immédiatement après.
#                 cursor = conn.cursor()
#                 try:
#                     cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
#                     if not cursor.fetchone():
#                         # La commande CREATE DATABASE doit être exécutée seule,
#                         # sans être englobée dans un bloc de transaction.
#                         # Le autocommit sur la connexion devrait gérer ça.
#                         cursor.execute(f"""
#                             CREATE DATABASE {db_name}
#                             WITH OWNER = {db_user}
#                             ENCODING = 'UTF8'
#                         """)
#                         print(f"Base **{db_name}** créée avec succès")
#                     else:
#                         print(f"Base **{db_name}** existe déjà")
#                 finally:
#                     # Assurez-vous de fermer le curseur après chaque opération
#                     cursor.close() 

#             print("Initialisation des bases terminée")
#             return True
            
#         finally:
#             # Assurez-vous que la connexion est fermée à la fin, même en cas d'erreur
#             if conn:
#                 conn.close()

#     except Exception as e:
#         print(f"Erreur initialisation DB: {e}")
#         traceback.print_exc() 
#         return False
    
# def get_pg_type(python_type_str: str) -> str:
#     """Convertit un type Python en un type PostgreSQL."""
#     type_map = {
#         'timestamp(0) PRIMARY KEY': 'timestamp(0) PRIMARY KEY',
#         'timestamp': 'timestamp',
#         'date': 'date',
#         'integer': 'integer',
#         'float': 'double precision',
#         'boolean': 'boolean',
#         'varchar(255)': 'varchar(255)',
#         'varchar(255) NOT NULL': 'varchar(255) NOT NULL',
#         'serial primary key unique': 'SERIAL PRIMARY KEY'
#     }
#     return type_map.get(python_type_str.lower(), 'text')


# def create_station_table(station: str, processing_type: str = 'before'):
#     """
#     Crée la table dans la base de données si elle n'existe pas,
#     avec une clé primaire sur 'Datetime' ou 'id' selon le type de traitement.
#     """
#     table_name = station.strip()

#     columns_config = get_station_columns(station, processing_type)
#     if not columns_config:
#         print(f"Error: Configuration des colonnes manquante pour {station}. Impossible de créer la table.")
#         return False
    
#     db_key_for_connection = processing_type 

#     try:
#         with get_connection(db_key_for_connection) as conn:
#             if not conn:
#                 print(f"Erreur: Impossible d'obtenir la connexion pour la clé '{db_key_for_connection}'.")
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
#                     primary_key_defined = False

#                     # Prioritize 'id' for 'missing_before/after' as SERIAL PRIMARY KEY
#                     if processing_type in ['missing_before', 'missing_after'] and 'id' in columns_config:
#                         # Ensure 'id' is defined as SERIAL PRIMARY KEY
#                         column_defs.append(f'"id" SERIAL PRIMARY KEY UNIQUE')
#                         primary_key_defined = True
#                         # Add other columns from config, excluding 'id' if already added
#                         for col, dtype in columns_config.items():
#                             if col != 'id':
#                                 pg_type = get_pg_type(dtype)
#                                 column_defs.append(f'"{col}" {pg_type}')
                        
#                     # Then 'Datetime' for 'before/after' tables
#                     elif 'Datetime' in columns_config:
#                         for col, dtype in columns_config.items():
#                             pg_type = get_pg_type(dtype)
#                             if col == 'Datetime':
#                                 column_defs.append(f'"{col}" {pg_type} PRIMARY KEY')
#                                 primary_key_defined = True
#                             else:
#                                 column_defs.append(f'"{col}" {pg_type}')
#                     else:
#                         # General case: add all columns from config
#                         for col, dtype in columns_config.items():
#                             pg_type = get_pg_type(dtype)
#                             column_defs.append(f'"{col}" {pg_type}')

#                     if not primary_key_defined:
#                          print(f"Attention: Aucune clé primaire (Datetime ou id) détectée pour la table '{table_name}'. "
#                                "L'insertion avec ON CONFLICT pourrait échouer si aucune contrainte UNIQUE n'existe.")
                    
#                     create_query = f"""
#                         CREATE TABLE "{table_name}" (
#                             {', '.join(column_defs)}
#                         )
#                     """
                   
#                     print(f"\nℹ️ La table '{table_name}' n'existe pas, création...")
#                     print(f"Executing table creation query for '{table_name}':\n{create_query}")
#                     cursor.execute(create_query)
#                     print(f"Table '{table_name}' créée avec succès")
#                 else:
#                     print(f"Table '{table_name}' existe déjà")
#                 return True
#     except ValueError as ve: 
#         print(f"Erreur de configuration de la base de données lors de la création de table pour '{table_name}': {ve}")
#         traceback.print_exc()
#         return False
#     except Exception as e:
#         print(f"Erreur lors de la création de la table '{table_name}': {e}")
#         traceback.print_exc()
#         return False



# def save_to_database(df: pd.DataFrame, station: str, conn, processing_type: str = 'before') -> bool:
#     """
#     Sauvegarde un DataFrame dans la base de données, avec vérifications complètes et journalisation détaillée.
#     La connexion à la base de données est passée en argument.
#     """
#     try:
#         if df.empty:
#             logging.warning(f"DataFrame vide reçu pour la station '{station}' ({processing_type}), aucune donnée à sauvegarder.")
#             return True

#         table_name = station.strip()

#         db_key = processing_type 

#         if not conn:
#             raise ConnectionError(f"Connexion à la base de données non fournie ou nulle pour '{processing_type}'")

#         columns_config = get_station_columns(station, processing_type)
#         if not columns_config:
#             raise ValueError(f"Configuration des colonnes manquante pour la station '{station}' ({processing_type}). Impossible de sauvegarder.")

#         expected_columns = list(columns_config.keys())

#         if isinstance(df.index, pd.DatetimeIndex):
#             df_processed = df.reset_index()
#             logging.info(f"DatetimeIndex converti en colonne 'Datetime'. Nouvelles colonnes: {df_processed.columns.tolist()}")
#         else:
#             df_processed = df.copy() 
#             logging.info("Le DataFrame n'a pas de DatetimeIndex, aucune conversion nécessaire pour l'index.")

#         if 'Datetime' not in df_processed.columns and 'id' not in df_processed.columns and processing_type not in ['missing_before', 'missing_after']:
#             # This check is important if Datetime is expected but not found
#             logging.warning(f"La colonne 'Datetime' est introuvable après la conversion de l'index pour la station {station}, et 'id' n'est pas non plus présent.")


#         df_column_rename_map = {}
#         for df_col in df_processed.columns: 
#             if df_col == 'Rel_H_%':
#                 df_column_rename_map[df_col] = 'Rel_H_Pct'
#             elif df_col.lower() == 'rel_h': 
#                  df_column_rename_map[df_col] = 'Rel_H_Pct'

#         if df_column_rename_map:
#             df_processed = df_processed.rename(columns=df_column_rename_map) 
#             logging.debug(f"Colonnes du DataFrame renommées: {df_column_rename_map}")

#         logging.info("\n" + "="*50)
#         logging.info("VÉRIFICATION DES NOMS DE COLONNES (CASSE ET CORRESPONDANCE)")
#         logging.info("="*50)
        
#         column_mapping_expected = {col.lower(): col for col in expected_columns}
#         new_df_columns = []
#         for col_df in df_processed.columns: 
#             if col_df in expected_columns: 
#                 new_df_columns.append(col_df)
#             elif col_df.lower() in column_mapping_expected: 
#                 new_df_columns.append(column_mapping_expected[col_df.lower()])
#             else: 
#                 new_df_columns.append(col_df)
        
#         df_processed.columns = new_df_columns 

#         df_cols_after_norm = df_processed.columns.tolist() 
#         missing_in_df = set(expected_columns) - set(df_cols_after_norm)
#         extra_in_df = set(df_cols_after_norm) - set(expected_columns)
        
#         logging.info(f"\nColonnes attendues par la DB: {expected_columns}")
#         logging.info(f"Colonnes du DataFrame (après normalisation): {df_cols_after_norm}")

#         if missing_in_df:
#             logging.warning(f"\n⚠️ ATTENTION: Colonnes attendues mais absentes du DataFrame:")
#             for col in missing_in_df:
#                 logging.warning(f"- {col}")
#                 df_processed[col] = None 
        
#         if extra_in_df:
#             logging.info(f"\nℹ️ INFO: Colonnes supplémentaires dans le DataFrame:")
#             for col in extra_in_df:
#                 logging.info(f"- {col}")
#             df_processed = df_processed.drop(columns=list(extra_in_df)) 

#         if not missing_in_df and not extra_in_df and set(expected_columns) == set(df_processed.columns.tolist()): 
#             logging.info("\n✅ Tous les noms de colonnes correspondent exactement")
#         else:
#             logging.warning("\n❌ Des divergences de noms de colonnes existent")

#         df_processed = df_processed[expected_columns] 
#         logging.info("\nOrdre final des colonnes:" + str(df_processed.columns.tolist())) 
#         logging.info("="*50 + "\n")

#         logging.info("\n" + "="*50)
#         logging.info("VÉRIFICATION DES TYPES DE DONNÉES ET VALEURS ANORMALES")
#         logging.info("="*50)
        
#         nan_value_strings = ['NaN', 'NAN', 'nan', '', ' ', '#VALUE!', '-', 'NULL', 'null', 'N/A', 'NA', 'None', 'NONE', 'NV', 'N.V.', '#DIV/0!']
        
#         for col, pg_type_str in columns_config.items():
#             if col not in df_processed.columns: 
#                 logging.warning(f"\n⚠️ Colonne '{col}' non trouvée - ignorée")
#                 continue

#             original_dtype = str(df_processed[col].dtype) 
#             pg_base_type = pg_type_str.split()[0].lower()
            
#             logging.info(f"\nColonne: {col}")
#             logging.info(f"- Type Pandas original: {original_dtype}")
#             logging.info(f"- Type PostgreSQL attendu: {pg_base_type}")
            
#             try:
#                 df_processed[col] = df_processed[col].replace(nan_value_strings, np.nan) 
                
#                 if pg_base_type in ['timestamp', 'date']:
#                     df_processed[col] = pd.to_datetime(df_processed[col], errors='coerce') 
#                     logging.info("- Converti en datetime")
#                 elif pg_base_type in ['float', 'integer', 'double precision', 'real']: 
#                     df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce') 
#                     if pg_base_type == 'integer' and pd.api.types.is_float_dtype(df_processed[col]):
#                         if (df_processed[col].notna() & (df_processed[col] % 1 != 0)).any():
#                             logging.warning(f"Colonne '{col}' contient des décimales mais le type attendu est INTEGER. Les valeurs seront tronquées ou mises à NaN.")
#                         df_processed[col] = df_processed[col].astype('Int64', errors='coerce') 
#                     logging.info("- Converti en numérique")
#                 elif pg_base_type == 'boolean':
#                     df_processed[col] = df_processed[col].apply(lambda x: bool(x) if pd.notna(x) else None) 
#                     logging.info("- Converti en booléen")
                
#                 null_count = df_processed[col].isna().sum() 
#                 logging.info(f"- Valeurs NULL après conversion: {null_count}/{len(df_processed)} ({null_count/len(df_processed)*100:.2f}%)") 
                
#                 if null_count > 0:
#                     sample_null = df_processed[df_processed[col].isna()].head(3) 
#                     logging.info("- Exemple de lignes avec NULL:\n" + sample_null[[col]].to_string())
                    
#             except Exception as e:
#                 logging.error(f"\n❌ ERREUR DE CONVERSION pour '{col}': {str(e)}")
#                 problematic_vals = df_processed[col][df_processed[col].notna() & pd.to_numeric(df_processed[col], errors='coerce').isna()]
#                 if not problematic_vals.empty:
#                     logging.error("- Valeurs problématiques:" + str(problematic_vals.unique()[:5]))
#                 traceback.print_exc()

#         logging.info("\nRésumé des types après conversion:")
#         logging.info(str(df_processed.dtypes)) 
#         logging.info("="*50 + "\n")

#         logging.info("\n" + "="*50)
#         logging.info("PRÉPARATION DES DONNÉES POUR INSERTION")
#         logging.info("="*50)
        
#         data_to_insert = []
#         type_converters = []
        
#         # Determine the columns to be actually inserted into the database
#         # This is where we ensure 'id' (SERIAL PK) is not included if it's auto-generated.
#         insert_columns = []
#         if processing_type in ['missing_before', 'missing_after']:
#             # For 'missing' tables, 'id' is SERIAL, so don't include it in INSERT statement
#             insert_columns = [col for col in expected_columns if col != 'id']
#         else:
#             insert_columns = expected_columns # For 'before'/'after', all expected columns are inserted

#         # Create type converters only for columns that will be inserted
#         for col in insert_columns:
#             pg_type_str = columns_config.get(col, 'text')
#             pg_base_type = pg_type_str.split()[0].lower()

#             if pg_base_type in ['timestamp', 'timestamp with time zone']:
#                 type_converters.append(lambda x: x.isoformat(timespec='microseconds') if pd.notna(x) else None)
#             elif pg_base_type == 'date':
#                 type_converters.append(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
#             elif pg_base_type in ['float', 'double precision', 'real']:
#                 type_converters.append(lambda x: float(x) if pd.notna(x) else None)
#             elif pg_base_type == 'integer':
#                 type_converters.append(lambda x: int(x) if pd.notna(x) and pd.api.types.is_integer(x) else None)
#             elif pg_base_type == 'boolean':
#                 type_converters.append(lambda x: bool(x) if pd.notna(x) else None)
#             else:
#                 type_converters.append(lambda x: str(x) if pd.notna(x) else None)

#         for i, row_tuple in enumerate(df_processed.itertuples(index=False)):
#             try:
#                 # Get values from df_processed corresponding to `insert_columns`
#                 row_values_for_insert = []
#                 for col_name in insert_columns:
#                     # Find the index of the column in the df_processed.columns
#                     # This is safer than assuming the order of row_tuple matches insert_columns directly
#                     col_idx_in_df_processed = df_processed.columns.get_loc(col_name)
#                     row_values_for_insert.append(row_tuple[col_idx_in_df_processed])

#                 converted_row_values = [type_converters[j](row_values_for_insert[j]) for j in range(len(insert_columns))]
#                 data_to_insert.append(tuple(converted_row_values))
                
#                 if i < 2:
#                     logging.debug(f"\nExemple ligne {i}:")
#                     for j, col in enumerate(insert_columns): # Use insert_columns here
#                         val = converted_row_values[j]
#                         logging.debug(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                        
#             except Exception as e:
#                 logging.error(f"\n❌ ERREUR lors de la préparation de la ligne {i}: {str(e)}")
#                 logging.error("Valeurs brutes de la ligne problématique (peut différer après conversion de type):")
#                 logging.error(str(df_processed.iloc[i].to_dict()))
#                 traceback.print_exc()
#                 continue

#         logging.info(f"\nTotal lignes préparées: {len(data_to_insert)}")
#         logging.info("="*50 + "\n")

#         if not conn:
#             raise ConnectionError(f"Connexion à la base de données non fournie ou nulle pour la station '{station}'")

#         conn.autocommit = True

#         with conn.cursor() as cursor:
#             cursor.execute("""
#                 SELECT EXISTS (
#                     SELECT FROM information_schema.tables
#                     WHERE table_schema = 'public' AND table_name = %s
#                 )
#             """, (table_name,))
#             table_exists = cursor.fetchone()[0]
            
#             if not table_exists:
#                 logging.info(f"\nℹ️ La table '{table_name}' n'existe pas, création...")
#                 if not create_station_table(station, processing_type): 
#                     raise Exception(f"Échec de la création de la table '{table_name}'")
#                 logging.info("✅ Table créée avec succès")

#             # ========== DÉTECTION ET CONSTRUCTION DE LA REQUÊTE SQL ==========
#             pk_col = None
#             cols_to_insert_in_query = [] 
            
#             if processing_type in ['before', 'after']:
#                 if 'Datetime' in columns_config: 
#                     pk_col = 'Datetime'
#                     logging.info(f"✅ Clé primaire détectée pour '{processing_type}': '{pk_col}'")
#                     cols_to_insert_in_query = expected_columns # All columns, including Datetime PK
#                 else:
#                     logging.warning(f"⚠️ 'Datetime' n'est pas configuré comme PK pour '{processing_type}'.")
#                     cols_to_insert_in_query = expected_columns # Fallback to all columns
#             elif processing_type in ['missing_before', 'missing_after']:
#                 if 'id' in columns_config: 
#                     pk_col = 'id'
#                     # Exclude 'id' from insert columns as it's SERIAL
#                     cols_to_insert_in_query = [col for col in expected_columns if col != 'id']
#                     logging.info(f"✅ Clé primaire détectée pour '{processing_type}': '{pk_col}' (SERIAL, non incluse dans l'INSERT direct)")
#                 else:
#                     logging.warning(f"⚠️ 'id' n'est pas configuré comme PK pour '{processing_type}'.")
#                     cols_to_insert_in_query = expected_columns # Fallback to all columns
            
#             # Fallback if PK not detected by the processing_type logic
#             if not pk_col:
#                 logging.warning("ℹ️ Aucune clé primaire automatique détectée. Tentative de détection par la DB (moins efficace).")
#                 try:
#                     cursor.execute("""
#                         SELECT kcu.column_name
#                         FROM information_schema.table_constraints tc
#                         JOIN information_schema.key_column_usage kcu
#                             ON tc.constraint_name = kcu.constraint_name
#                             AND tc.table_schema = kcu.table_schema
#                         WHERE tc.table_name = %s
#                         AND tc.constraint_type = 'PRIMARY KEY'
#                         LIMIT 1
#                     """, (table_name,))
#                     pk_info = cursor.fetchone()
#                     if pk_info:
#                         pk_col = pk_info[0]
#                         logging.info(f"✅ Clé primaire détectée via DB: {pk_col}")
#                         if pk_col == 'id' and 'id' in cols_to_insert_in_query: 
#                              cols_to_insert_in_query = [col for col in cols_to_insert_in_query if col != 'id'] # Ensure it's removed
#                     else:
#                         logging.info("ℹ️ Aucune clé primaire détectée par la DB.")
#                 except Exception as e:
#                     logging.error(f"❌ Erreur lors de la détection de la clé primaire via DB: {str(e)}")

#             cols_sql = ', '.join([f'"{col}"' for col in cols_to_insert_in_query])
#             placeholders = ', '.join(['%s'] * len(cols_to_insert_in_query))
            
#             query = ""
#             if pk_col == 'Datetime': 
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders})
#                     ON CONFLICT ("{pk_col}") DO NOTHING;
#                 """
#                 logging.info(f"ℹ️ Utilisation de ON CONFLICT ('{pk_col}') DO NOTHING pour '{table_name}'.")
#             elif pk_col == 'id' and processing_type in ['missing_before', 'missing_after']: # Specific case for SERIAL PK 'id'
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders});
#                 """
#                 logging.info(f"ℹ️ Insertion simple pour '{table_name}' (clé primaire '{pk_col}' SERIAL, non incluse dans l'INSERT).")
#             else: 
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders});
#                 """
#                 logging.warning(f"⚠️ Insertion simple sans ON CONFLICT pour '{table_name}' (aucune clé primaire pertinente pour ON CONFLICT).")

#             logging.info(f"\nRequête SQL générée:\n{query}")

#             logging.info("\n" + "="*50)
#             logging.info("VÉRIFICATION FINALE AVANT INSERTION")
#             logging.info("="*50)
            
#             logging.info(f"\nNombre de colonnes attendues pour l'insertion (basé sur la requête SQL): {len(cols_to_insert_in_query)}")
#             logging.info(f"Nombre de valeurs dans chaque ligne de data_to_insert: {len(data_to_insert[0]) if data_to_insert else 0}")
            
#             if data_to_insert and len(cols_to_insert_in_query) != len(data_to_insert[0]):
#                 logging.error("\n❌ ERREUR: Nombre de colonnes incompatible entre la requête SQL et les données préparées!")
#                 logging.error("Colonnes dans la requête SQL:" + str(cols_to_insert_in_query))
#                 logging.error("Première ligne de données préparées:" + str(data_to_insert[0]))
#                 raise ValueError("Incompatibilité de colonnes après préparation") 

#             if data_to_insert:
#                 first_row = data_to_insert[0]
#                 logging.info(f"\nPremière ligne de données ({len(first_row)} valeurs):")
#                 for val, col in zip(first_row, cols_to_insert_in_query): # Use cols_to_insert_in_query here
#                     logging.info(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                
#                 try:
#                     mogrified = cursor.mogrify(query, first_row).decode('utf-8')
#                     logging.info("\n✅ Test mogrify réussi:\n" + mogrified[:500] + ("..." if len(mogrified) > 500 else ""))
#                 except Exception as e:
#                     logging.error("\n❌ ERREUR mogrify:" + str(e))
#                     traceback.print_exc()
#                     raise 
#             else:
#                 logging.warning("\n⚠️ Aucune donnée à insérer!")
#                 return False

#             logging.info("="*50 + "\n")

#             batch_size = 10000
#             start_time = datetime.now()
            
#             logging.info(f"\nDébut de l'insertion de {len(data_to_insert)} lignes par lots de {batch_size}...")
            
#             for i in range(0, len(data_to_insert), batch_size):
#                 batch = data_to_insert[i:i + batch_size]
#                 try:
#                     from psycopg2 import extras
#                     extras.execute_batch(cursor, query, batch)
                    
#                     if (i // batch_size) % 10 == 0:
#                         elapsed = (datetime.now() - start_time).total_seconds()
#                         logging.info(f"Lot {i//batch_size + 1}: {len(batch)} lignes traitées en {elapsed:.2f} secondes au total...")
                        
#                 except Exception as e:
#                     conn.rollback() 
#                     logging.error(f"\n❌ ERREUR lot {i//batch_size + 1}: {str(e)}")
#                     if batch:
#                         logging.error("Exemple de ligne problématique:" + str(batch[0]))
#                     traceback.print_exc()
#                     raise 

#             total_time = (datetime.now() - start_time).total_seconds()
#             logging.info(f"\n✅ Traitement d'insertion terminé pour '{station}': {len(data_to_insert)} lignes préparées en {total_time:.2f} secondes.")
#             logging.info("Note: Le nombre exact de lignes insérées peut différer en raison de la clause ON CONFLICT.")
#             return True

#     except Exception as e:
#         logging.error(f"\n❌❌❌ ERREUR CRITIQUE lors de la sauvegarde pour '{station}': {str(e)}")
#         traceback.print_exc()
        
#         if 'df_processed' in locals():
#             logging.error("\nÉtat du DataFrame au moment de l'erreur:")
#             logging.error(f"Shape: {df_processed.shape}")
#             logging.error("5 premières lignes:\n" + df_processed.head().to_string())
            
#         return False


############################# Fin copie ############

# def save_to_database(df: pd.DataFrame, station: str, conn, processing_type: str = 'before') -> bool:
#     """
#     Sauvegarde un DataFrame dans la base de données, avec vérifications complètes et journalisation détaillée.
#     La connexion à la base de données est passée en argument.
#     """
#     try:
#         # Vérification initiale
#         if df.empty:
#             logging.warning(f"DataFrame vide reçu pour la station '{station}' ({processing_type}), aucune donnée à sauvegarder.")
#             return True

#         table_name = station.strip()

#         # Déterminer la DB Key en fonction du processing_type
#         # Note: La connexion `conn` est déjà passée en argument,
#         # mais `db_key` est utile pour `get_station_columns` et la logique PK.
#         if processing_type == 'before':
#             db_key = 'before'
#         elif processing_type == 'after':
#             db_key = 'after'
#         elif processing_type == 'missing_before':
#             db_key = 'missing_before'
#         elif processing_type == 'missing_after':
#             db_key = 'missing_after'
#         else:
#             logging.error(f"Type de traitement inconnu '{processing_type}'. Impossible de déterminer la base de données.")
#             return False

#         columns_config = get_station_columns(station, processing_type)
#         if not columns_config:
#             raise ValueError(f"Configuration des colonnes manquante pour la station '{station}' ({processing_type}). Impossible de sauvegarder.")

#         expected_columns = list(columns_config.keys())

#         # --- DÉBUT DE LA VRAIE CORRECTION POUR DATETIMEINDEX ---
#         df_processed = df.copy() 
#         if isinstance(df_processed.index, pd.DatetimeIndex):
#             df_processed = df_processed.reset_index()
#             if 'index' in df_processed.columns and 'Datetime' not in df_processed.columns:
#                 df_processed = df_processed.rename(columns={'index': 'Datetime'})
#             logging.info(f"DatetimeIndex converti en colonne 'Datetime'.")
#         else:
#             logging.info("Le DataFrame n'a pas de DatetimeIndex, aucune conversion nécessaire pour l'index.")

#         if 'Datetime' not in df_processed.columns and (processing_type == 'before' or processing_type == 'after'):
#              raise ValueError(f"La colonne 'Datetime' est introuvable après la conversion de l'index pour la station {station}, mais est attendue pour ce type de traitement.")

#         # --- CORRECTION POUR `Rel_H_%` ---
#         df_column_rename_map = {}
#         for df_col in df_processed.columns:
#             if df_col == 'Rel_H_%':
#                 df_column_rename_map[df_col] = 'Rel_H_Pct'
#             elif df_col.lower() == 'rel_h': 
#                  df_column_rename_map[df_col] = 'Rel_H_Pct'

#         if df_column_rename_map:
#             df_processed = df_processed.rename(columns=df_column_rename_map)
#             logging.debug(f"Colonnes du DataFrame renommées: {df_column_rename_map}")

#         # ========== VÉRIFICATION 1: NOMS DE COLONNES ==========
#         logging.info("\n" + "="*50)
#         logging.info("VÉRIFICATION DES NOMS DE COLONNES (CASSE ET CORRESPONDANCE)")
#         logging.info("="*50)
        
#         column_mapping_expected = {col.lower(): col for col in expected_columns}
#         new_df_columns = []
#         for col_df in df_processed.columns:
#             if col_df in expected_columns: 
#                 new_df_columns.append(col_df)
#             elif col_df.lower() in column_mapping_expected: 
#                 new_df_columns.append(column_mapping_expected[col_df.lower()])
#             else: 
#                 new_df_columns.append(col_df)
        
#         df_processed.columns = new_df_columns

#         df_cols_after_norm = df_processed.columns.tolist()
#         missing_in_df = set(expected_columns) - set(df_cols_after_norm)
#         extra_in_df = set(df_cols_after_norm) - set(expected_columns)
        
#         logging.info(f"\nColonnes attendues par la DB: {expected_columns}")
#         logging.info(f"Colonnes du DataFrame (après normalisation): {df_cols_after_norm}")

#         if missing_in_df:
#             logging.warning(f"\n⚠️ ATTENTION: Colonnes attendues mais absentes du DataFrame: {missing_in_df}. Ajoutées avec NULL.")
#             for col in missing_in_df:
#                 df_processed[col] = None
        
#         if extra_in_df:
#             logging.info(f"\nℹ️ INFO: Colonnes supplémentaires dans le DataFrame: {extra_in_df}. Suppression.")
#             df_processed = df_processed.drop(columns=list(extra_in_df))

#         if not missing_in_df and not extra_in_df and set(expected_columns) == set(df_processed.columns.tolist()):
#             logging.info("\n✅ Tous les noms de colonnes correspondent exactement")
#         else:
#             logging.warning("\n❌ Des divergences de noms de colonnes existent")

#         df_processed = df_processed[expected_columns]
#         logging.info("\nOrdre final des colonnes:" + str(df_processed.columns.tolist()))
#         logging.info("="*50 + "\n")

#         # ========== VÉRIFICATION 2: TYPES DE DONNÉES ==========
#         logging.info("\n" + "="*50)
#         logging.info("VÉRIFICATION DES TYPES DE DONNÉES ET VALEURS ANORMALES")
#         logging.info("="*50)
        
#         nan_value_strings = ['NaN', 'NAN', 'nan', '', ' ', '#VALUE!', '-', 'NULL', 'null', 'N/A', 'NA', 'None', 'NONE', 'NV', 'N.V.', '#DIV/0!']
        
#         for col, pg_type_str in columns_config.items():
#             if col not in df_processed.columns:
#                 logging.warning(f"\n⚠️ Colonne '{col}' non trouvée - ignorée")
#                 continue

#             original_dtype = str(df_processed[col].dtype)
#             pg_base_type = pg_type_str.split()[0].lower()
            
#             logging.info(f"\nColonne: {col}")
#             logging.info(f"- Type Pandas original: {original_dtype}")
#             logging.info(f"- Type PostgreSQL attendu: {pg_base_type}")
            
#             try:
#                 df_processed[col] = df_processed[col].replace(nan_value_strings, np.nan)
                
#                 if pg_base_type in ['timestamp', 'date']:
#                     df_processed[col] = pd.to_datetime(df_processed[col], errors='coerce')
#                     if pg_base_type == 'date':
#                         df_processed[col] = df_processed[col].dt.date
#                     else:
#                         df_processed[col] = df_processed[col].dt.to_pydatetime()
#                 elif pg_base_type in ['float', 'integer', 'double precision', 'real']:
#                     df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')
#                     if pg_base_type == 'integer' and pd.api.types.is_float_dtype(df_processed[col]):
#                         if (df_processed[col].notna() & (df_processed[col] % 1 != 0)).any():
#                             logging.warning(f"Colonne '{col}' contient des décimales mais le type attendu est INTEGER. Les valeurs seront tronquées ou mises à NaN.")
#                         df_processed[col] = df_processed[col].astype('Int64', errors='coerce')
#                     logging.info("- Converti en numérique")
#                 elif pg_base_type == 'boolean':
#                     df_processed[col] = df_processed[col].apply(lambda x: bool(x) if pd.notna(x) else None)
#                     logging.info("- Converti en booléen")
                
#                 null_count = df_processed[col].isna().sum()
#                 logging.info(f"- Valeurs NULL après conversion: {null_count}/{len(df_processed)} ({null_count/len(df_processed)*100:.2f}%)")
                
#                 if null_count > 0:
#                     sample_null = df_processed[df_processed[col].isna()].head(3)
#                     logging.info("- Exemple de lignes avec NULL:\n" + sample_null[[col]].to_string())
                    
#             except Exception as e:
#                 logging.error(f"\n❌ ERREUR DE CONVERSION pour '{col}': {str(e)}")
#                 problematic_vals = df_processed[col][df_processed[col].notna() & pd.to_numeric(df_processed[col], errors='coerce').isna()]
#                 if not problematic_vals.empty:
#                     logging.error("- Valeurs problématiques:" + str(problematic_vals.unique()[:5]))
#                 traceback.print_exc()

#         logging.info("\nRésumé des types après conversion:")
#         logging.info(str(df_processed.dtypes))
#         logging.info("="*50 + "\n")

#         # ========== PRÉPARATION DES DONNÉES ==========
#         logging.info("\n" + "="*50)
#         logging.info("PRÉPARATION DES DONNÉES POUR INSERTION")
#         logging.info("="*50)
        
#         data_to_insert = []
#         type_converters = []
        
#         for col in expected_columns:
#             pg_type_str = columns_config.get(col, 'text')
#             pg_base_type = pg_type_str.split()[0].lower()

#             if pg_base_type in ['timestamp', 'timestamp with time zone']:
#                 type_converters.append(lambda x: x.isoformat(timespec='microseconds') if pd.notna(x) else None)
#             elif pg_base_type == 'date':
#                 type_converters.append(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
#             elif pg_base_type in ['float', 'double precision', 'real']:
#                 type_converters.append(lambda x: float(x) if pd.notna(x) else None)
#             elif pg_base_type == 'integer':
#                 type_converters.append(lambda x: int(x) if pd.notna(x) and pd.api.types.is_integer(x) else None)
#             elif pg_base_type == 'boolean':
#                 type_converters.append(lambda x: bool(x) if pd.notna(x) else None)
#             else:
#                 type_converters.append(lambda x: str(x) if pd.notna(x) else None)

#         for i, row_tuple in enumerate(df_processed.itertuples(index=False)):
#             try:
#                 row_values = [type_converters[j](row_tuple[j]) for j in range(len(expected_columns))]
#                 data_to_insert.append(tuple(row_values))
                
#                 if i < 2:
#                     logging.debug(f"\nExemple ligne {i}:")
#                     for j, col in enumerate(expected_columns):
#                         val = row_values[j]
#                         logging.debug(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                        
#             except Exception as e:
#                 logging.error(f"\n❌ ERREUR lors de la préparation de la ligne {i}: {str(e)}")
#                 logging.error("Valeurs brutes de la ligne problématique (peut différer après conversion de type):")
#                 logging.error(str(df_processed.iloc[i].to_dict()))
#                 traceback.print_exc()
#                 continue

#         logging.info(f"\nTotal lignes préparées: {len(data_to_insert)}")
#         logging.info("="*50 + "\n")

#         # ========== CONNEXION À LA BASE (Logique existante) ==========
#         # `conn` est passé en argument. Assurez-vous qu'il est valide.
#         if not conn:
#             raise ConnectionError(f"Connexion à la base de données non fournie ou nulle pour la station '{station}'")

#         conn.autocommit = True

#         with conn.cursor() as cursor:
#             # Vérification de l'existence de la table (déjà dans votre code, utilisant votre create_station_table)
#             cursor.execute("""
#                 SELECT EXISTS (
#                     SELECT FROM information_schema.tables
#                     WHERE table_schema = 'public' AND table_name = %s
#                 )
#             """, (table_name,))
#             table_exists = cursor.fetchone()[0]
            
#             if not table_exists:
#                 logging.info(f"\nℹ️ La table '{table_name}' n'existe pas, création...")
#                 if not create_station_table(station, processing_type): # Appeler votre fonction
#                     raise Exception(f"Échec de la création de la table '{table_name}'")
#                 logging.info("✅ Table créée avec succès")

#             # ========== DÉTECTION ET CONSTRUCTION DE LA REQUÊTE SQL ==========
#             pk_col = None
#             cols_to_insert_in_query = expected_columns.copy() # Colonnes à inclure dans l'INSERT
            
#             # Détecter la clé primaire basée sur le processing_type
#             if processing_type in ['before', 'after']:
#                 if 'Datetime' in columns_config: # Vérifier si Datetime est bien dans le schéma
#                     pk_col = 'Datetime'
#                     logging.info(f"✅ Clé primaire détectée pour '{processing_type}': '{pk_col}'")
#                 else:
#                     logging.warning(f"⚠️ 'Datetime' n'est pas configuré comme PK pour '{processing_type}'.")
#             elif processing_type in ['missing_before', 'missing_after']:
#                 if 'id' in columns_config: # Vérifier si id est bien dans le schéma
#                     pk_col = 'id'
#                     # Pour les tables 'missing', 'id' est SERIAL, donc on ne l'insère pas
#                     cols_to_insert_in_query.remove('id')
#                     logging.info(f"✅ Clé primaire détectée pour '{processing_type}': '{pk_col}' (SERIAL, non incluse dans l'INSERT direct)")
#                 else:
#                     logging.warning(f"⚠️ 'id' n'est pas configuré comme PK pour '{processing_type}'.")
            
#             # Fallback si PK non détectée par la logique processing_type
#             if not pk_col:
#                 logging.warning("ℹ️ Aucune clé primaire automatique détectée. Tentative de détection par la DB (moins efficace).")
#                 try:
#                     cursor.execute("""
#                         SELECT kcu.column_name
#                         FROM information_schema.table_constraints tc
#                         JOIN information_schema.key_column_usage kcu
#                             ON tc.constraint_name = kcu.constraint_name
#                             AND tc.table_schema = kcu.table_schema
#                         WHERE tc.table_name = %s
#                         AND tc.constraint_type = 'PRIMARY KEY'
#                         LIMIT 1
#                     """, (table_name,))
#                     pk_info = cursor.fetchone()
#                     if pk_info:
#                         pk_col = pk_info[0]
#                         logging.info(f"✅ Clé primaire détectée via DB: {pk_col}")
#                         if pk_col == 'id' and 'id' in cols_to_insert_in_query: # Si 'id' est trouvé par la DB et toujours dans la liste
#                              cols_to_insert_in_query.remove('id') # S'assurer qu'il est retiré
#                     else:
#                         logging.info("ℹ️ Aucune clé primaire détectée par la DB.")
#                 except Exception as e:
#                     logging.error(f"❌ Erreur lors de la détection de la clé primaire via DB: {str(e)}")

#             cols_sql = ', '.join([f'"{col}"' for col in cols_to_insert_in_query])
#             placeholders = ', '.join(['%s'] * len(cols_to_insert_in_query))
            
#             query = ""
#             if pk_col == 'Datetime': # Votre logique originale pour Datetime
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders})
#                     ON CONFLICT ("{pk_col}") DO NOTHING;
#                 """
#                 logging.info(f"ℹ️ Utilisation de ON CONFLICT ('{pk_col}') DO NOTHING pour '{table_name}'.")
#             elif pk_col == 'id': # Votre logique pour ID (simple insert, pas de ON CONFLICT sur ID SERIAL)
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders});
#                 """
#                 logging.info(f"ℹ️ Insertion simple pour '{table_name}' (clé primaire '{pk_col}' SERIAL, non incluse dans l'INSERT).")
#             else: # Fallback pour les cas sans clé primaire détectée ou comportement inconnu
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders});
#                 """
#                 logging.warning(f"⚠️ Insertion simple sans ON CONFLICT pour '{table_name}' (aucune clé primaire pertinente pour ON CONFLICT).")

#             logging.info(f"\nRequête SQL générée:\n{query}")

#             # ========== VÉRIFICATION FINALE AVANT INSERTION ==========
#             logging.info("\n" + "="*50)
#             logging.info("VÉRIFICATION FINALE AVANT INSERTION")
#             logging.info("="*50)
            
#             # Ajuster data_to_insert pour correspondre à cols_to_insert_in_query
#             final_data_to_insert_for_batch = []
#             if data_to_insert:
#                 # Créer un mapping de l'ordre des colonnes du DataFrame original vers l'ordre de la requête SQL
#                 original_col_order_map = {col: idx for idx, col in enumerate(expected_columns)}
                
#                 for row_tuple_full in data_to_insert:
#                     temp_row_values = []
#                     for col_name in cols_to_insert_in_query:
#                         temp_row_values.append(row_tuple_full[original_col_order_map[col_name]])
#                     final_data_to_insert_for_batch.append(tuple(temp_row_values))
            
#             if not final_data_to_insert_for_batch:
#                 logging.warning("\n⚠️ Aucune donnée valide à insérer après ajustement des colonnes!")
#                 return True

#             logging.info(f"\nNombre de colonnes attendues pour l'insertion SQL: {len(cols_to_insert_in_query)}")
#             logging.info(f"Nombre de colonnes dans la première ligne du batch: {len(final_data_to_insert_for_batch[0])}")
            
#             if len(cols_to_insert_in_query) != len(final_data_to_insert_for_batch[0]):
#                  logging.error("\n❌ ERREUR: Nombre de colonnes incompatible entre la requête SQL et les données préparées!")
#                  logging.error("Colonnes de la requête SQL:" + str(cols_to_insert_in_query))
#                  logging.error("Exemple de ligne de données:" + str(final_data_to_insert_for_batch[0]))
#                  raise ValueError("Incompatibilité de colonnes pour l'insertion finale")

#             first_row_batch = final_data_to_insert_for_batch[0]
#             logging.info(f"\nPremière ligne de données pour le batch ({len(first_row_batch)} valeurs):")
#             for val, col in zip(first_row_batch, cols_to_insert_in_query):
#                 logging.info(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
            
#             try:
#                 mogrified = cursor.mogrify(query, first_row_batch).decode('utf-8')
#                 logging.info("\n✅ Test mogrify réussi:\n" + mogrified[:500] + ("..." if len(mogrified) > 500 else ""))
#             except Exception as e:
#                 logging.error("\n❌ ERREUR mogrify:" + str(e))
#                 traceback.print_exc()
#                 raise 

#             logging.info("="*50 + "\n")

#             # ========== INSERTION ==========
#             batch_size = 10000
#             start_time = datetime.now()
            
#             logging.info(f"\nDébut de l'insertion de {len(final_data_to_insert_for_batch)} lignes par lots de {batch_size}...")
            
#             for i in range(0, len(final_data_to_insert_for_batch), batch_size):
#                 batch = final_data_to_insert_for_batch[i:i + batch_size]
#                 try:
#                     extras.execute_batch(cursor, query, batch)
#                     if (i // batch_size) % 10 == 0:
#                         elapsed = (datetime.now() - start_time).total_seconds()
#                         logging.info(f"Lot {i//batch_size + 1}: {len(batch)} lignes traitées en {elapsed:.2f} secondes au total...")
#                 except Exception as e:
#                     conn.rollback() 
#                     logging.error(f"\n❌ ERREUR lot {i//batch_size + 1}: {str(e)}")
#                     if batch:
#                         logging.error(f"Exemple de ligne problématique (première ligne du lot): {batch[0]}")
#                     traceback.print_exc()
#                     raise 

#             total_time = (datetime.now() - start_time).total_seconds()
#             logging.info(f"\n✅ Traitement d'insertion terminé pour '{station}': {len(final_data_to_insert_for_batch)} lignes insérées/traitées en {total_time:.2f} secondes.")
#             logging.info("Note: Le nombre exact de lignes insérées peut différer en raison de la clause ON CONFLICT.")
#             return True

#     except Exception as e:
#         logging.error(f"\n❌❌❌ ERREUR CRITIQUE lors de la sauvegarde pour '{station}': {str(e)}")
#         traceback.print_exc()
#         if 'df_processed' in locals():
#             logging.error(f"État du DataFrame au moment de l'erreur:\nShape: {df_processed.shape}\n5 premières lignes:\n{df_processed.head().to_string()}")
#         return False
#     finally:
#         if 'conn' in locals() and conn: # S'assurer que 'conn' existe et n'est pas None avant de fermer
#             conn.close()



################ Jeudi 24 Jui 2025 ##################
# from config import STATIONS_BY_BASSIN
# def get_stations_with_data(processing_type: str = 'before') -> Dict[str, List[str]]:
#     """
#     Récupère un dictionnaire des bassins, avec une liste des stations non vides pour chaque bassin.
#     Une station est considérée "non vide" si sa table correspondante contient des enregistrements.
#     Args:
#         processing_type: 'before' ou 'after' pour choisir la base de données.
#     Returns:
#         Dictionnaire {bassin: [station1, station2, ...]} des stations non vides.
#     """
#     # Importation de STATIONS_BY_BASSIN depuis config.py
#     # Assurez-vous que config.py est dans le PYTHONPATH ou le même dossier
#     from config import STATIONS_BY_BASSIN 

#     stations_by_bassin_with_data = {bassin: [] for bassin in STATIONS_BY_BASSIN.keys()}
    
#     # Détermine le nom de la base de données cible à partir des variables d'environnement
#     target_db_name_env_var = 'DB_NAME_BEFORE' if processing_type == 'before' else 'DB_NAME_AFTER'
#     target_db_name = os.getenv(target_db_name_env_var)

#     if not target_db_name:
#         print(f"Erreur: La variable d'environnement '{target_db_name_env_var}' n'est pas définie.")
#         return {}

#     conn = None # Initialiser conn à None
#     try:
#         # Utilise la fonction get_connection existante qui prend le nom de la DB en argument
#         conn = get_connection(target_db_name)
#         if not conn:
#             print(f"Erreur: Impossible de se connecter à la base de données '{target_db_name}'")
#             return {}

#         cursor = conn.cursor()

#         for bassin, stations_in_bassin in STATIONS_BY_BASSIN.items():
#             for station_name in stations_in_bassin:
#                 # Le nom de la table dans la DB est le nom exact de la station après strip()
#                 table_name_in_db = station_name.strip()
                
#                 try:
#                     # Vérifier l'existence de la table
#                     cursor.execute(sql.SQL("""
#                         SELECT EXISTS (
#                             SELECT 1
#                             FROM information_schema.tables
#                             WHERE table_schema = 'public' AND table_name = {table_name}
#                         );
#                     """).format(table_name=sql.Literal(table_name_in_db)))

#                     table_exists = cursor.fetchone()[0]

#                     if table_exists:
#                         # Si la table existe, vérifier si elle contient des données
#                         # Utiliser sql.Identifier pour citer le nom de table car il peut contenir espaces/majuscules
#                         count_query = sql.SQL("SELECT COUNT(*) FROM {};").format(sql.Identifier(table_name_in_db))
#                         cursor.execute(count_query)
#                         count = cursor.fetchone()[0]

#                         if count > 0:
#                             stations_by_bassin_with_data[bassin].append(station_name)
                    
#                 except psycopg2.Error as e_table:
#                     print(f"Erreur lors de la vérification de la table '{table_name_in_db}': {e_table}")
#                     continue

#         for bassin in stations_by_bassin_with_data:
#             stations_by_bassin_with_data[bassin].sort()

#     except psycopg2.Error as e:
#         print(f"Erreur PostgreSQL lors de la récupération des stations: {e}")
#     except Exception as e:
#         print(f"Erreur inattendue dans get_stations_with_data: {e}")
#         traceback.print_exc()
#     finally:
#         if conn:
#             conn.close()

#     return stations_by_bassin_with_data


# 
def get_stations_with_data(processing_type: str = 'before') -> Dict[str, List[str]]:
    """
    Récupère un dictionnaire des bassins, avec une liste des stations non vides pour chaque bassin.
    Une station est considérée "non vide" si sa table correspondante contient des enregistrements.
    Args:
        processing_type: 'before' ou 'after' pour choisir la base de données.
    Returns:
        Dictionnaire {bassin: [station1, station2, ...]} des stations non vides.
    """
    # Importation de STATIONS_BY_BASSIN depuis config.py
    # Assurez-vous que config.py est dans le PYTHONPATH ou le même dossier
    from config import STATIONS_BY_BASSIN 

    stations_by_bassin_with_data = {bassin: [] for bassin in STATIONS_BY_BASSIN.keys()}
    
    # Validation du processing_type
    if processing_type not in ['raw', 'after']:
        print(f"Erreur: Le 'processing_type' '{processing_type}' est invalide. Utilisez 'before' ou 'after'.")
        return {}

    # PAS DE target_db_name_env_var ou os.getenv() ici pour la clé !
    # On passe directement la clé courte à get_connection
    db_key_for_connection = processing_type # <--- CHANGEMENT CLÉ ICI

    conn = None # Initialiser conn à None
    try:
        # Utilisez la fonction get_connection existante en lui passant la clé courte
        conn = get_connection(db_key_for_connection) # <--- UTILISEZ LA CLÉ COURTE

        if not conn:
            print(f"Erreur: Impossible de se connecter à la base de données pour la clé '{db_key_for_connection}'")
            return {}

        cursor = conn.cursor()

        for bassin, stations_in_bassin in STATIONS_BY_BASSIN.items():
            for station_name in stations_in_bassin:
                # Le nom de la table dans la DB est le nom exact de la station après strip()
                table_name_in_db = station_name.strip()
                
                try:
                    # Vérifier l'existence de la table
                    cursor.execute(sql.SQL("""
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.tables
                            WHERE table_schema = 'public' AND table_name = {table_name}
                        );
                    """).format(table_name=sql.Literal(table_name_in_db)))

                    table_exists = cursor.fetchone()[0]

                    if table_exists:
                        # Si la table existe, vérifier si elle contient des données
                        # Utiliser sql.Identifier pour citer le nom de table car il peut contenir espaces/majuscules
                        count_query = sql.SQL("SELECT COUNT(*) FROM {};").format(sql.Identifier(table_name_in_db))
                        cursor.execute(count_query)
                        count = cursor.fetchone()[0]

                        if count > 0:
                            stations_by_bassin_with_data[bassin].append(station_name)
                    
                except psycopg2.Error as e_table:
                    print(f"Erreur lors de la vérification de la table '{table_name_in_db}' dans la base de données '{db_key_for_connection}': {e_table}")
                    continue

        for bassin in stations_by_bassin_with_data:
            stations_by_bassin_with_data[bassin].sort()

    except ValueError as ve: # Capture spécifiquement l'erreur de get_connection
        print(f"Erreur de configuration de la base de données: {ve}")
        traceback.print_exc()
    except psycopg2.Error as e:
        print(f"Erreur PostgreSQL lors de la récupération des stations: {e}")
        traceback.print_exc()
    except Exception as e:
        print(f"Erreur inattendue dans get_stations_with_data: {e}")
        traceback.print_exc()
    finally:
        if conn:
            conn.close()

    return stations_by_bassin_with_data


import warnings
# Fonction pour extraire les donnees brutes  chargees dans  before_processing.py et les mettre en dataframe

# def load_raw_station_data(station_name: str, processing_type: str = 'before') -> pd.DataFrame:
#     """
#     Charge les données brutes d'une station depuis before_processing_db.
#     Renomme 'Rel_H_Pct' en 'Rel_H_%' pour le pipeline d'interpolation.
#     """

#     db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')

#     conn = None
#     df = pd.DataFrame()
#     try:
#         conn = get_connection(db_name)
#         if not conn:
#             raise ConnectionError(f"Impossible de se connecter à la base de données brute '{db_name}'.")

#         table_name = station_name.strip()
        
#         db_columns_info = get_station_columns(station_name, 'before')
#         db_columns = list(db_columns_info.keys())
        
#         column_identifiers = [sql.Identifier(col) for col in db_columns]
        
#         query = sql.SQL("SELECT {} FROM {};").format(
#             sql.SQL(', ').join(column_identifiers),
#             sql.Identifier(table_name)
#         )
        
#         df = pd.read_sql(query.as_string(conn), conn, index_col='Datetime')
        
#         df.index = pd.to_datetime(df.index, utc=True, errors='coerce')
#         initial_rows_after_load = len(df)
#         df.dropna(subset=[df.index.name], inplace=True)
#         if len(df) < initial_rows_after_load:
#             warnings.warn(f"Suppression de {initial_rows_after_load - len(df)} lignes avec index Datetime invalide après chargement pour la station {station_name}.")

#         # Renommer la colonne 'Rel_H_Pct' en 'Rel_H_%' pour le pipeline d'interpolation
#         if 'Rel_H_Pct' in df.columns and 'Rel_H_%' not in df.columns:
#             df.rename(columns={'Rel_H_Pct': 'Rel_H_%'}, inplace=True)
#             warnings.warn(f"Colonne 'Rel_H_Pct' renommée en 'Rel_H_%' pour le traitement de la station {station_name}.")

#     except Exception as e:
#         warnings.warn(f"Erreur lors du chargement des données brutes pour la station '{station_name}': {e}")
#         traceback.print_exc()
#     finally:
#         if conn:
#             conn.close()
#     return df


# Assuming this function is in db.py or a related data loading module







# Charger les variables d'environnement
load_dotenv()

# Configuration de la base de données
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    #'database': os.getenv('DB_NAME_BEFORE', 'before_processing_db'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': os.getenv('DB_PORT')
}

# Obtenir les noms des bases de données
DB_NAMES = {
    'raw': os.getenv('DB_NAME_RAW'), # Nom de la base de données pour les données brutes
    'before': os.getenv('DB_NAME_BEFORE'), # Nom de la base de données pour les données avant traitement
    'after': os.getenv('DB_NAME_AFTER'), # Nom de la base de données pour les données après traitement
    'missing_before': os.getenv('DB_NAME_MISSING_BEFORE_DB'), # Nom de la base de données pour les plages des données manquantes avant traitement
    'missing_after': os.getenv('DB_NAME_MISSING_AFTER_DB'), # Nom de la base de données pour les plages des données manquantes après traitement
    'admin_db':'postgres' # Nom de la base de données pour l'administration, généralement 'postgres' ou une autre base d'administration
}



# Fonction de connexion aux bases de donnees
def get_connection(db_key: str):
    """
    Établit une connexion à la base de données PostgreSQL en fonction d'une clé.

    Args:
        db_key (str): Clé pour déterminer la base de données (ex: 'before', 'after', 
                      'missing_before', 'missing_after').
    Returns:
        psycopg2.Connection: L'objet de connexion à la base de données.
    Raises:
        ValueError: Si la clé de base de données est inconnue.
        Exception: Si la connexion à la base de données échoue.
    """
    db_name = DB_NAMES.get(db_key)
    if not db_name:
        raise ValueError(f"Clé de base de données inconnue ou non configurée: '{db_key}'. "
                         f"Clés disponibles: {list(DB_NAMES.keys())}")

    config = DB_CONFIG.copy()
    config['database'] = db_name # Définir la base de données spécifique ici

    try:
        conn = psycopg2.connect(**config)
        return conn
    except Exception as e:
        print(f"Erreur de connexion à la base de données '{db_name}': {e}")
        raise # Relaisser l'exception pour que l'appelant la gère

def load_raw_station_data(station_name: str, processing_type: str = 'raw') -> pd.DataFrame:
    """
    Charge les données brutes d'une station depuis la base de données appropriée ('before' ou 'after').
    Renomme 'Rel_H_Pct' en 'Rel_H_%' pour le pipeline d'interpolation.
    """

    # OLD LINE (problematic):
    # db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')

    # NEW LINE: Use 'processing_type' directly, as it is already the correct short key ('before' or 'after')
    db_key_for_connection = processing_type

    conn = None
    df = pd.DataFrame()
    try:
        # CORRECTION HERE: Pass db_key_for_connection (the short key)
        conn = get_connection(db_key_for_connection)
        if not conn:
            # Better error message to indicate which key failed
            raise ConnectionError(f"Impossible de se connecter à la base de données pour la clé '{db_key_for_connection}'.")

        table_name = station_name.strip()
        
        # Ensure get_station_columns also uses the short key
        db_columns_info = get_station_columns(station_name, processing_type) # Use processing_type here too
        if not db_columns_info:
            warnings.warn(f"Impossible de récupérer la configuration des colonnes pour la station '{station_name}' avec le type de traitement '{processing_type}'. Retourne un DataFrame vide.")
            return pd.DataFrame() # Return empty DF if column config is missing

        db_columns = list(db_columns_info.keys())
        
        column_identifiers = [sql.Identifier(col) for col in db_columns]
        
        query = sql.SQL("SELECT {} FROM {};").format(
            sql.SQL(', ').join(column_identifiers),
            sql.Identifier(table_name)
        )
        
        df = pd.read_sql(query.as_string(conn), conn, index_col='Datetime')
        
        df.index = pd.to_datetime(df.index, utc=True, errors='coerce')
        initial_rows_after_load = len(df)
        df.dropna(subset=[df.index.name], inplace=True)
        if len(df) < initial_rows_after_load:
            warnings.warn(f"Suppression de {initial_rows_after_load - len(df)} lignes avec index Datetime invalide après chargement pour la station {station_name}.")

        # Renommer la colonne 'Rel_H_Pct' en 'Rel_H_%' pour le pipeline d'interpolation
        if 'Rel_H_Pct' in df.columns and 'Rel_H_%' not in df.columns:
            df.rename(columns={'Rel_H_Pct': 'Rel_H_%'}, inplace=True)
            warnings.warn(f"Colonne 'Rel_H_Pct' renommée en 'Rel_H_%' pour le traitement de la station {station_name}.")

    except ValueError as ve: # Specifically catch ValueError from get_connection
        warnings.warn(f"Erreur de configuration de la base de données lors du chargement des données brutes pour la station '{station_name}': {ve}")
        traceback.print_exc()
    except Exception as e:
        warnings.warn(f"Erreur lors du chargement des données brutes pour la station '{station_name}': {e}")
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
    return df


    ############################################################# 26 Juillet 2025 ################

def get_station_columns(station: str, processing_type: str) -> Dict[str, str]:
    """Retourne les colonnes attendues pour une station donnée selon le type de traitement"""
    
    # Nettoyage du nom de la station
    station = station.strip()

    #RAW DATA
    if processing_type == 'raw':
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
    elif   processing_type == 'before' or processing_type == 'after':
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

    elif processing_type == 'missing_before' or processing_type == 'missing_after':
        return{
            'id': 'serial primary key unique',
            'station': 'varchar(255) not null',
            'variable': 'varchar(255) not null',
            'start_time': 'timestamp(0) not null',      
            'end_time': 'timestamp(0) not null',
            'duration': 'integer',  
            'unit': 'varchar(255)',
            'count': 'integer',
        }
    
    return {}

def initialize_database():
    """Initialise simplement les bases de données si elles n'existent pas"""
    try:
        db_user = DB_CONFIG['user'] 
        if not db_user:
            raise ValueError("L'utilisateur de la base de données (DB_USER) n'est pas configuré dans DB_CONFIG.")
        
        # Utilisez 'admin_db' pour vous connecter à la base de données 'postgres'
        conn = get_connection('admin_db') # <--- PAS DE 'with' ICI POUR LA CONNEXION DIRECTE
        if not conn:
            print("Erreur : Impossible d'établir une connexion admin pour l'initialisation des bases de données.")
            return False
        
        conn.autocommit = True # Assurez-vous que autocommit est VRAIMENT activé
        
        try:
            # Récupérez les noms réels des bases de données que vous voulez créer
            target_db_names = [
                DB_NAMES['raw'],  # Base de données pour les données brutes
                DB_NAMES['before'],
                DB_NAMES['after'],
                DB_NAMES['missing_before'],
                DB_NAMES['missing_after']
            ]

            for db_name in target_db_names:
                if not db_name: 
                    print(f"Avertissement : Le nom d'une base de données est vide. Veuillez vérifier vos variables d'environnement.")
                    continue
                
                # Exécuter la vérification et la création SANS UN BLOC 'with cursor()'
                # pour le CREATE DATABASE afin d'éviter la transaction implicite.
                # Créez le curseur ici, utilisez-le, puis fermez-le immédiatement après.
                cursor = conn.cursor()
                try:
                    cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
                    if not cursor.fetchone():
                        # La commande CREATE DATABASE doit être exécutée seule,
                        # sans être englobée dans un bloc de transaction.
                        # Le autocommit sur la connexion devrait gérer ça.
                        cursor.execute(f"""
                            CREATE DATABASE {db_name}
                            WITH OWNER = {db_user}
                            ENCODING = 'UTF8'
                        """)
                        print(f"Base **{db_name}** créée avec succès")
                    else:
                        print(f"Base **{db_name}** existe déjà")
                finally:
                    # Assurez-vous de fermer le curseur après chaque opération
                    cursor.close() 

            print("Initialisation des bases terminée")
            return True
            
        finally:
            # Assurez-vous que la connexion est fermée à la fin, même en cas d'erreur
            if conn:
                conn.close()

    except Exception as e:
        print(f"Erreur initialisation DB: {e}")
        traceback.print_exc() 
        return False
    
def get_pg_type(python_type_str: str) -> str:
    """Convertit un type Python en un type PostgreSQL."""
    type_map = {
        'timestamp(0) PRIMARY KEY': 'timestamp(0) PRIMARY KEY',
        'timestamp': 'timestamp',
        'date': 'date',
        'integer': 'integer',
        'float': 'double precision',
        'boolean': 'boolean',
        'varchar(255)': 'varchar(255)',
        'varchar(255) NOT NULL': 'varchar(255) NOT NULL',
        'serial primary key unique': 'SERIAL PRIMARY KEY'
    }
    return type_map.get(python_type_str.lower(), 'text')


def create_station_table(station: str, processing_type: str = 'before'):
    """
    Crée la table dans la base de données si elle n'existe pas,
    avec une clé primaire sur 'Datetime' ou 'id' selon le type de traitement.
    """
    table_name = station.strip()

    columns_config = get_station_columns(station, processing_type)
    if not columns_config:
        print(f"Error: Configuration des colonnes manquante pour {station}. Impossible de créer la table.")
        return False
    
    db_key_for_connection = processing_type 

    try:
        with get_connection(db_key_for_connection) as conn:
            if not conn:
                print(f"Erreur: Impossible d'obtenir la connexion pour la clé '{db_key_for_connection}'.")
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
                    primary_key_defined = False

                    # Prioritize 'id' for 'missing_before/after' as SERIAL PRIMARY KEY
                    if processing_type in ['missing_before', 'missing_after'] and 'id' in columns_config:
                        # Ensure 'id' is defined as SERIAL PRIMARY KEY
                        column_defs.append(f'"id" SERIAL PRIMARY KEY UNIQUE')
                        primary_key_defined = True
                        # Add other columns from config, excluding 'id' if already added
                        for col, dtype in columns_config.items():
                            if col != 'id':
                                pg_type = get_pg_type(dtype)
                                column_defs.append(f'"{col}" {pg_type}')
                        
                    # Then 'Datetime' for 'before/after' tables
                    elif 'Datetime' in columns_config:
                        for col, dtype in columns_config.items():
                            pg_type = get_pg_type(dtype)
                            if col == 'Datetime':
                                column_defs.append(f'"{col}" {pg_type} PRIMARY KEY')
                                primary_key_defined = True
                            else:
                                column_defs.append(f'"{col}" {pg_type}')
                    else:
                        # General case: add all columns from config
                        for col, dtype in columns_config.items():
                            pg_type = get_pg_type(dtype)
                            column_defs.append(f'"{col}" {pg_type}')

                    if not primary_key_defined:
                         print(f"Attention: Aucune clé primaire (Datetime ou id) détectée pour la table '{table_name}'. "
                               "L'insertion avec ON CONFLICT pourrait échouer si aucune contrainte UNIQUE n'existe.")
                    
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
    except ValueError as ve: 
        print(f"Erreur de configuration de la base de données lors de la création de table pour '{table_name}': {ve}")
        traceback.print_exc()
        return False
    except Exception as e:
        print(f"Erreur lors de la création de la table '{table_name}': {e}")
        traceback.print_exc()
        return False


import pandas as pd
import psycopg2
from psycopg2 import errors as pg_errors
import logging
from typing import Dict, List
import numpy as np
from datetime import datetime
import traceback

##########Code pour sauvegarder un DataFrame dans la base de données avec vérifications complètes et journalisation détaillée BIEN
# def save_to_database(df: pd.DataFrame, station: str, conn, processing_type: str = 'before') -> bool:
#     """
#     Sauvegarde un DataFrame dans la base de données, avec vérifications complètes et journalisation détaillée.
#     La connexion à la base de données est passée en argument.
#     """
#     try:
#         if df.empty:
#             logging.warning(f"DataFrame vide reçu pour la station '{station}' ({processing_type}), aucune donnée à sauvegarder.")
#             return True

#         table_name = station.strip()

#         db_key = processing_type 

#         if not conn:
#             raise ConnectionError(f"Connexion à la base de données non fournie ou nulle pour '{processing_type}'")

#         columns_config = get_station_columns(station, processing_type)
#         if not columns_config:
#             raise ValueError(f"Configuration des colonnes manquante pour la station '{station}' ({processing_type}). Impossible de sauvegarder.")

#         expected_columns = list(columns_config.keys())

#         if isinstance(df.index, pd.DatetimeIndex):
#             df_processed = df.reset_index()
#             logging.info(f"DatetimeIndex converti en colonne 'Datetime'. Nouvelles colonnes: {df_processed.columns.tolist()}")
#         else:
#             df_processed = df.copy() 
#             logging.info("Le DataFrame n'a pas de DatetimeIndex, aucune conversion nécessaire pour l'index.")

#         if 'Datetime' not in df_processed.columns and 'id' not in df_processed.columns and processing_type not in ['missing_before', 'missing_after']:
#             # This check is important if Datetime is expected but not found
#             logging.warning(f"La colonne 'Datetime' est introuvable après la conversion de l'index pour la station {station}, et 'id' n'est pas non plus présent.")


#         df_column_rename_map = {}
#         for df_col in df_processed.columns: 
#             if df_col == 'Rel_H_%':
#                 df_column_rename_map[df_col] = 'Rel_H_Pct'
#             elif df_col.lower() == 'rel_h': 
#                  df_column_rename_map[df_col] = 'Rel_H_Pct'

#         if df_column_rename_map:
#             df_processed = df_processed.rename(columns=df_column_rename_map) 
#             logging.debug(f"Colonnes du DataFrame renommées: {df_column_rename_map}")

#         logging.info("\n" + "="*50)
#         logging.info("VÉRIFICATION DES NOMS DE COLONNES (CASSE ET CORRESPONDANCE)")
#         logging.info("="*50)
        
#         column_mapping_expected = {col.lower(): col for col in expected_columns}
#         new_df_columns = []
#         for col_df in df_processed.columns: 
#             if col_df in expected_columns: 
#                 new_df_columns.append(col_df)
#             elif col_df.lower() in column_mapping_expected: 
#                 new_df_columns.append(column_mapping_expected[col_df.lower()])
#             else: 
#                 new_df_columns.append(col_df)
        
#         df_processed.columns = new_df_columns 

#         df_cols_after_norm = df_processed.columns.tolist() 
#         missing_in_df = set(expected_columns) - set(df_cols_after_norm)
#         extra_in_df = set(df_cols_after_norm) - set(expected_columns)
        
#         logging.info(f"\nColonnes attendues par la DB: {expected_columns}")
#         logging.info(f"Colonnes du DataFrame (après normalisation): {df_cols_after_norm}")

#         if missing_in_df:
#             logging.warning(f"\n⚠️ ATTENTION: Colonnes attendues mais absentes du DataFrame:")
#             for col in missing_in_df:
#                 logging.warning(f"- {col}")
#                 df_processed[col] = None 
        
#         if extra_in_df:
#             logging.info(f"\nℹ️ INFO: Colonnes supplémentaires dans le DataFrame:")
#             for col in extra_in_df:
#                 logging.info(f"- {col}")
#             df_processed = df_processed.drop(columns=list(extra_in_df)) 

#         if not missing_in_df and not extra_in_df and set(expected_columns) == set(df_processed.columns.tolist()): 
#             logging.info("\n✅ Tous les noms de colonnes correspondent exactement")
#         else:
#             logging.warning("\n❌ Des divergences de noms de colonnes existent")

#         df_processed = df_processed[expected_columns] 
#         logging.info("\nOrdre final des colonnes:" + str(df_processed.columns.tolist())) 
#         logging.info("="*50 + "\n")

#         logging.info("\n" + "="*50)
#         logging.info("VÉRIFICATION DES TYPES DE DONNÉES ET VALEURS ANORMALES")
#         logging.info("="*50)
        
#         nan_value_strings = ['NaN', 'NAN', 'nan', '', ' ', '#VALUE!', '-', 'NULL', 'null', 'N/A', 'NA', 'None', 'NONE', 'NV', 'N.V.', '#DIV/0!']
        
#         for col, pg_type_str in columns_config.items():
#             if col not in df_processed.columns: 
#                 logging.warning(f"\n⚠️ Colonne '{col}' non trouvée - ignorée")
#                 continue

#             original_dtype = str(df_processed[col].dtype) 
#             pg_base_type = pg_type_str.split()[0].lower()
            
#             logging.info(f"\nColonne: {col}")
#             logging.info(f"- Type Pandas original: {original_dtype}")
#             logging.info(f"- Type PostgreSQL attendu: {pg_base_type}")
            
#             try:
#                 df_processed[col] = df_processed[col].replace(nan_value_strings, np.nan) 
                
#                 if pg_base_type in ['timestamp', 'date']:
#                     df_processed[col] = pd.to_datetime(df_processed[col], errors='coerce') 
#                     logging.info("- Converti en datetime")
#                 elif pg_base_type in ['float', 'integer', 'double precision', 'real']: 
#                     df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce') 
#                     if pg_base_type == 'integer' and pd.api.types.is_float_dtype(df_processed[col]):
#                         if (df_processed[col].notna() & (df_processed[col] % 1 != 0)).any():
#                             logging.warning(f"Colonne '{col}' contient des décimales mais le type attendu est INTEGER. Les valeurs seront tronquées ou mises à NaN.")
#                         df_processed[col] = df_processed[col].astype('Int64', errors='coerce') 
#                     logging.info("- Converti en numérique")
#                 elif pg_base_type == 'boolean':
#                     df_processed[col] = df_processed[col].apply(lambda x: bool(x) if pd.notna(x) else None) 
#                     logging.info("- Converti en booléen")
                
#                 null_count = df_processed[col].isna().sum() 
#                 logging.info(f"- Valeurs NULL après conversion: {null_count}/{len(df_processed)} ({null_count/len(df_processed)*100:.2f}%)") 
                
#                 if null_count > 0:
#                     sample_null = df_processed[df_processed[col].isna()].head(3) 
#                     logging.info("- Exemple de lignes avec NULL:\n" + sample_null[[col]].to_string())
                    
#             except Exception as e:
#                 logging.error(f"\n❌ ERREUR DE CONVERSION pour '{col}': {str(e)}")
#                 problematic_vals = df_processed[col][df_processed[col].notna() & pd.to_numeric(df_processed[col], errors='coerce').isna()]
#                 if not problematic_vals.empty:
#                     logging.error("- Valeurs problématiques:" + str(problematic_vals.unique()[:5]))
#                 traceback.print_exc()

#         logging.info("\nRésumé des types après conversion:")
#         logging.info(str(df_processed.dtypes)) 
#         logging.info("="*50 + "\n")

#         logging.info("\n" + "="*50)
#         logging.info("PRÉPARATION DES DONNÉES POUR INSERTION")
#         logging.info("="*50)
        
#         data_to_insert = []
#         type_converters = []
        
#         # Determine the columns to be actually inserted into the database
#         # This is where we ensure 'id' (SERIAL PK) is not included if it's auto-generated.
#         insert_columns = []
#         if processing_type in ['missing_before', 'missing_after']:
#             # For 'missing' tables, 'id' is SERIAL, so don't include it in INSERT statement
#             insert_columns = [col for col in expected_columns if col != 'id']
#         else:
#             insert_columns = expected_columns # For 'before'/'after', all expected columns are inserted

#         # Create type converters only for columns that will be inserted
#         for col in insert_columns:
#             pg_type_str = columns_config.get(col, 'text')
#             pg_base_type = pg_type_str.split()[0].lower()

#             if pg_base_type in ['timestamp', 'timestamp with time zone']:
#                 type_converters.append(lambda x: x.isoformat(timespec='microseconds') if pd.notna(x) else None)
#             elif pg_base_type == 'date':
#                 type_converters.append(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
#             elif pg_base_type in ['float', 'double precision', 'real']:
#                 type_converters.append(lambda x: float(x) if pd.notna(x) else None)
#             elif pg_base_type == 'integer':
#                 type_converters.append(lambda x: int(x) if pd.notna(x) and pd.api.types.is_integer(x) else None)
#             elif pg_base_type == 'boolean':
#                 type_converters.append(lambda x: bool(x) if pd.notna(x) else None)
#             else:
#                 type_converters.append(lambda x: str(x) if pd.notna(x) else None)

#         for i, row_tuple in enumerate(df_processed.itertuples(index=False)):
#             try:
#                 # Get values from df_processed corresponding to `insert_columns`
#                 row_values_for_insert = []
#                 for col_name in insert_columns:
#                     # Find the index of the column in the df_processed.columns
#                     # This is safer than assuming the order of row_tuple matches insert_columns directly
#                     col_idx_in_df_processed = df_processed.columns.get_loc(col_name)
#                     row_values_for_insert.append(row_tuple[col_idx_in_df_processed])

#                 converted_row_values = [type_converters[j](row_values_for_insert[j]) for j in range(len(insert_columns))]
#                 data_to_insert.append(tuple(converted_row_values))
                
#                 if i < 2:
#                     logging.debug(f"\nExemple ligne {i}:")
#                     for j, col in enumerate(insert_columns): # Use insert_columns here
#                         val = converted_row_values[j]
#                         logging.debug(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                        
#             except Exception as e:
#                 logging.error(f"\n❌ ERREUR lors de la préparation de la ligne {i}: {str(e)}")
#                 logging.error("Valeurs brutes de la ligne problématique (peut différer après conversion de type):")
#                 logging.error(str(df_processed.iloc[i].to_dict()))
#                 traceback.print_exc()
#                 continue

#         logging.info(f"\nTotal lignes préparées: {len(data_to_insert)}")
#         logging.info("="*50 + "\n")

#         if not conn:
#             raise ConnectionError(f"Connexion à la base de données non fournie ou nulle pour la station '{station}'")

#         conn.autocommit = True

#         with conn.cursor() as cursor:
#             cursor.execute("""
#                 SELECT EXISTS (
#                     SELECT FROM information_schema.tables
#                     WHERE table_schema = 'public' AND table_name = %s
#                 )
#             """, (table_name,))
#             table_exists = cursor.fetchone()[0]
            
#             if not table_exists:
#                 logging.info(f"\nℹ️ La table '{table_name}' n'existe pas, création...")
#                 if not create_station_table(station, processing_type): 
#                     raise Exception(f"Échec de la création de la table '{table_name}'")
#                 logging.info("✅ Table créée avec succès")

#             # ========== DÉTECTION ET CONSTRUCTION DE LA REQUÊTE SQL ==========
#             pk_col = None
#             cols_to_insert_in_query = [] 
            
#             if processing_type in ['before', 'after']:
#                 if 'Datetime' in columns_config: 
#                     pk_col = 'Datetime'
#                     logging.info(f"✅ Clé primaire détectée pour '{processing_type}': '{pk_col}'")
#                     cols_to_insert_in_query = expected_columns # All columns, including Datetime PK
#                 else:
#                     logging.warning(f"⚠️ 'Datetime' n'est pas configuré comme PK pour '{processing_type}'.")
#                     cols_to_insert_in_query = expected_columns # Fallback to all columns
#             elif processing_type in ['missing_before', 'missing_after']:
#                 if 'id' in columns_config: 
#                     pk_col = 'id'
#                     # Exclude 'id' from insert columns as it's SERIAL
#                     cols_to_insert_in_query = [col for col in expected_columns if col != 'id']
#                     logging.info(f"✅ Clé primaire détectée pour '{processing_type}': '{pk_col}' (SERIAL, non incluse dans l'INSERT direct)")
#                 else:
#                     logging.warning(f"⚠️ 'id' n'est pas configuré comme PK pour '{processing_type}'.")
#                     cols_to_insert_in_query = expected_columns # Fallback to all columns
            
#             # Fallback if PK not detected by the processing_type logic
#             if not pk_col:
#                 logging.warning("ℹ️ Aucune clé primaire automatique détectée. Tentative de détection par la DB (moins efficace).")
#                 try:
#                     cursor.execute("""
#                         SELECT kcu.column_name
#                         FROM information_schema.table_constraints tc
#                         JOIN information_schema.key_column_usage kcu
#                             ON tc.constraint_name = kcu.constraint_name
#                             AND tc.table_schema = kcu.table_schema
#                         WHERE tc.table_name = %s
#                         AND tc.constraint_type = 'PRIMARY KEY'
#                         LIMIT 1
#                     """, (table_name,))
#                     pk_info = cursor.fetchone()
#                     if pk_info:
#                         pk_col = pk_info[0]
#                         logging.info(f"✅ Clé primaire détectée via DB: {pk_col}")
#                         if pk_col == 'id' and 'id' in cols_to_insert_in_query: 
#                              cols_to_insert_in_query = [col for col in cols_to_insert_in_query if col != 'id'] # Ensure it's removed
#                     else:
#                         logging.info("ℹ️ Aucune clé primaire détectée par la DB.")
#                 except Exception as e:
#                     logging.error(f"❌ Erreur lors de la détection de la clé primaire via DB: {str(e)}")

#             cols_sql = ', '.join([f'"{col}"' for col in cols_to_insert_in_query])
#             placeholders = ', '.join(['%s'] * len(cols_to_insert_in_query))
            
#             query = ""
#             if pk_col == 'Datetime': 
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders})
#                     ON CONFLICT ("{pk_col}") DO NOTHING;
#                 """
#                 logging.info(f"ℹ️ Utilisation de ON CONFLICT ('{pk_col}') DO NOTHING pour '{table_name}'.")
#             elif pk_col == 'id' and processing_type in ['missing_before', 'missing_after']: # Specific case for SERIAL PK 'id'
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders});
#                 """
#                 logging.info(f"ℹ️ Insertion simple pour '{table_name}' (clé primaire '{pk_col}' SERIAL, non incluse dans l'INSERT).")
#             else: 
#                 query = f"""
#                     INSERT INTO "{table_name}" ({cols_sql})
#                     VALUES ({placeholders});
#                 """
#                 logging.warning(f"⚠️ Insertion simple sans ON CONFLICT pour '{table_name}' (aucune clé primaire pertinente pour ON CONFLICT).")

#             logging.info(f"\nRequête SQL générée:\n{query}")

#             logging.info("\n" + "="*50)
#             logging.info("VÉRIFICATION FINALE AVANT INSERTION")
#             logging.info("="*50)
            
#             logging.info(f"\nNombre de colonnes attendues pour l'insertion (basé sur la requête SQL): {len(cols_to_insert_in_query)}")
#             logging.info(f"Nombre de valeurs dans chaque ligne de data_to_insert: {len(data_to_insert[0]) if data_to_insert else 0}")
            
#             if data_to_insert and len(cols_to_insert_in_query) != len(data_to_insert[0]):
#                 logging.error("\n❌ ERREUR: Nombre de colonnes incompatible entre la requête SQL et les données préparées!")
#                 logging.error("Colonnes dans la requête SQL:" + str(cols_to_insert_in_query))
#                 logging.error("Première ligne de données préparées:" + str(data_to_insert[0]))
#                 raise ValueError("Incompatibilité de colonnes après préparation") 

#             if data_to_insert:
#                 first_row = data_to_insert[0]
#                 logging.info(f"\nPremière ligne de données ({len(first_row)} valeurs):")
#                 for val, col in zip(first_row, cols_to_insert_in_query): # Use cols_to_insert_in_query here
#                     logging.info(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                
#                 try:
#                     mogrified = cursor.mogrify(query, first_row).decode('utf-8')
#                     logging.info("\n✅ Test mogrify réussi:\n" + mogrified[:500] + ("..." if len(mogrified) > 500 else ""))
#                 except Exception as e:
#                     logging.error("\n❌ ERREUR mogrify:" + str(e))
#                     traceback.print_exc()
#                     raise 
#             else:
#                 logging.warning("\n⚠️ Aucune donnée à insérer!")
#                 return False

#             logging.info("="*50 + "\n")

#             batch_size = 10000
#             start_time = datetime.now()
            
#             logging.info(f"\nDébut de l'insertion de {len(data_to_insert)} lignes par lots de {batch_size}...")
            
#             for i in range(0, len(data_to_insert), batch_size):
#                 batch = data_to_insert[i:i + batch_size]
#                 try:
#                     from psycopg2 import extras
#                     extras.execute_batch(cursor, query, batch)
                    
#                     if (i // batch_size) % 10 == 0:
#                         elapsed = (datetime.now() - start_time).total_seconds()
#                         logging.info(f"Lot {i//batch_size + 1}: {len(batch)} lignes traitées en {elapsed:.2f} secondes au total...")
                        
#                 except Exception as e:
#                     conn.rollback() 
#                     logging.error(f"\n❌ ERREUR lot {i//batch_size + 1}: {str(e)}")
#                     if batch:
#                         logging.error("Exemple de ligne problématique:" + str(batch[0]))
#                     traceback.print_exc()
#                     raise 

#             total_time = (datetime.now() - start_time).total_seconds()
#             logging.info(f"\n✅ Traitement d'insertion terminé pour '{station}': {len(data_to_insert)} lignes préparées en {total_time:.2f} secondes.")
#             logging.info("Note: Le nombre exact de lignes insérées peut différer en raison de la clause ON CONFLICT.")
#             return True

#     except Exception as e:
#         logging.error(f"\n❌❌❌ ERREUR CRITIQUE lors de la sauvegarde pour '{station}': {str(e)}")
#         traceback.print_exc()
        
#         if 'df_processed' in locals():
#             logging.error("\nÉtat du DataFrame au moment de l'erreur:")
#             logging.error(f"Shape: {df_processed.shape}")
#             logging.error("5 premières lignes:\n" + df_processed.head().to_string())
            
#         return False

#############################  FFFFFFFFFF IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII  NNNNNNNNNNNNNNNNNNNNNNNNNNNN ################# 



########### Fonction permettant de  selectionner toutes les donnees dans les 4 bases de donnees pour la visualisations #################

# def load_station_data(station_name: str, processing_type: str = 'before') -> pd.DataFrame:
#     """
#     Charge les données d'une station depuis la base de données PostgreSQL spécifiée par 'processing_type'.
#     Cette fonction s'adapte automatiquement aux schémas de table :
#     - Pour 'before' et 'after': utilise 'Datetime' comme index temporel et gère les conversions.
#     - Pour 'missing_before' et 'missing_after': utilise 'id' comme clé primaire (sans index temporel)
#       et convertit 'start_time'/'end_time' en objets datetime.

#     Args:
#         station_name (str): Le nom de la station (correspond au nom de la table dans la base de données).
#         processing_type (str): La clé de la base de données à laquelle se connecter
#                                ('before', 'after', 'missing_before', 'missing_after').

#     Returns:
#         pd.DataFrame: Un DataFrame pandas contenant les données extraites.
#                       Retourne un DataFrame vide si la connexion échoue, la table est introuvable,
#                       la configuration des colonnes est absente ou le schéma est inconnu.
#     """
#     db_key_for_connection = processing_type

#     conn = None
#     df = pd.DataFrame() # Initialiser un DataFrame vide par défaut
#     try:
#         # Établir la connexion à la base de données en utilisant la clé fournie
#         conn = get_connection(db_key_for_connection)
#         if not conn:
#             raise ConnectionError(f"Impossible de se connecter à la base de données pour la clé '{db_key_for_connection}'. Vérifiez les configurations.")

#         table_name = station_name.strip()
        
#         # Récupérer les informations sur les colonnes de la table depuis la configuration db.py
#         db_columns_info = get_station_columns(station_name, processing_type)
#         if not db_columns_info:
#             warnings.warn(f"Impossible de récupérer la configuration des colonnes pour la station '{station_name}' avec le type de traitement '{processing_type}'. Retourne un DataFrame vide.")
#             return pd.DataFrame()

#         db_columns = list(db_columns_info.keys())
        
#         # Construire la requête SQL pour sélectionner toutes les colonnes
#         column_identifiers = [sql.Identifier(col) for col in db_columns]
#         query = sql.SQL("SELECT {} FROM {};").format(
#             sql.SQL(', ').join(column_identifiers),
#             sql.Identifier(table_name)
#         )
        
#         # --- Logique d'adaptation pour la lecture du DataFrame en fonction du schéma ---
#         if 'Datetime' in db_columns:
#             # Cas des tables de données de séries temporelles ('before', 'after')
#             logging.info(f"Chargement des données de séries temporelles pour '{table_name}' depuis '{processing_type}'.")
#             df = pd.read_sql(query.as_string(conn), conn, index_col='Datetime')
            
#             # Convertir l'index en DatetimeIndex et gérer les valeurs invalides
#             df.index = pd.to_datetime(df.index, utc=True, errors='coerce')
#             initial_rows_after_load = len(df)
#             df.dropna(subset=[df.index.name], inplace=True) # Supprime les lignes où Datetime est invalide
#             if len(df) < initial_rows_after_load:
#                 warnings.warn(f"Suppression de {initial_rows_after_load - len(df)} lignes avec index Datetime invalide après chargement pour la station {station_name} ({processing_type}).")

#             # Renommer la colonne 'Rel_H_Pct' en 'Rel_H_%' si elle existe (spécifique aux données météo)
#             if 'Rel_H_Pct' in df.columns and 'Rel_H_%' not in df.columns:
#                 df.rename(columns={'Rel_H_Pct': 'Rel_H_%'}, inplace=True)
#                 logging.info(f"Colonne 'Rel_H_Pct' renommée en 'Rel_H_%' pour le DataFrame de la station {station_name} ({processing_type}).")

#         elif 'id' in db_columns and 'start_time' in db_columns and 'end_time' in db_columns:
#             # Cas des tables de plages manquantes ('missing_before', 'missing_after')
#             logging.info(f"Chargement des plages manquantes pour '{table_name}' depuis '{processing_type}'.")
#             df = pd.read_sql(query.as_string(conn), conn)
            
#             # Convertir les colonnes de temps en objets datetime
#             if 'start_time' in df.columns:
#                 df['start_time'] = pd.to_datetime(df['start_time'], utc=True, errors='coerce')
#             if 'end_time' in df.columns:
#                 df['end_time'] = pd.to_datetime(df['end_time'], utc=True, errors='coerce')
#             # Les DataFrames des plages manquantes n'ont pas besoin d'un DatetimeIndex
#             # L'index par défaut de Pandas (0, 1, 2...) est suffisant, ou vous pourriez définir 'id'
#             # comme index si l'accès par ID est fréquent.

#         else:
#             # Cas où le schéma de la table n'est pas reconnu
#             warnings.warn(f"La configuration des colonnes pour la station '{station_name}' ({processing_type}) ne correspond à aucun schéma d'extraction connu ('Datetime' ou 'id'/'start_time').")
#             return pd.DataFrame() 

#     except psycopg2.errors.UndefinedTable:
#         # Gérer l'erreur si la table n'existe pas dans la base de données
#         logging.warning(f"La table '{table_name}' n'existe pas dans la base de données '{db_key_for_connection}'. Retourne un DataFrame vide.")
#     except ValueError as ve: 
#         # Capter les erreurs spécifiques de configuration de base de données
#         warnings.warn(f"Erreur de configuration de la base de données lors du chargement des données pour la station '{station_name}' ({processing_type}): {ve}")
#         traceback.print_exc() # Affiche la pile d'appels pour un débogage détaillé
#     except Exception as e:
#         # Capter toute autre erreur inattendue lors de l'extraction
#         warnings.warn(f"Erreur inattendue lors du chargement des données pour la station '{station_name}' ({processing_type}): {e}")
#         traceback.print_exc()
#     finally:
#         # S'assurer que la connexion à la base de données est toujours fermée
#         if conn:
#             conn.close()
#             logging.info(f"Connexion à la DB '{db_key_for_connection}' fermée.")
#     return df



##################### Dimanche 27 juil 2025 #####################

import pandas as pd
import psycopg2
from psycopg2 import sql
import logging
import warnings
import traceback

# def load_station_data(station_name: str, processing_type: str = 'before') -> pd.DataFrame:
#     """
#     Charge les données d'une station depuis la base de données PostgreSQL.
    
#     Args:
#         station_name: Nom de la station (correspond au nom de la table)
#         processing_type: Type de traitement ('before', 'after', 'missing_before', 'missing_after')
    
#     Returns:
#         DataFrame pandas avec les données chargées ou DataFrame vide en cas d'erreur
#     """
#     # Validation du processing_type
#     valid_types = {'before', 'after', 'missing_before', 'missing_after'}
#     if processing_type not in valid_types:
#         warnings.warn(f"Type de traitement invalide: {processing_type}")
#         return pd.DataFrame()

#     conn = None
#     df = pd.DataFrame()
    
#     try:
#         # Établir la connexion
#         conn = get_connection(processing_type)
#         if not conn:
#             raise ConnectionError(f"Connexion impossible à la base {processing_type}")

#         table_name = station_name.strip()
        
#         # Récupérer la configuration des colonnes
#         db_columns_info = get_station_columns(station_name, processing_type)
#         if not db_columns_info:
#             warnings.warn(f"Configuration des colonnes manquante pour {station_name}")
#             return pd.DataFrame()

#         db_columns = list(db_columns_info.keys())
        
#         # Construction de la requête
#         column_identifiers = [sql.Identifier(col) for col in db_columns]
#         query = sql.SQL("SELECT {} FROM {};").format(
#             sql.SQL(', ').join(column_identifiers),
#             sql.Identifier(table_name)
#         )

#         # Cas 1: Données temporelles (before/after)
#         if processing_type in {'before', 'after'}:
#             logging.info(f"Chargement données temporelles {table_name} ({processing_type})")
            
#             # Lecture sans index_col pour plus de contrôle
#             df = pd.read_sql(query.as_string(conn), conn)
            
#             if 'Datetime' not in df.columns:
#                 warnings.warn(f"Colonne 'Datetime' manquante pour {station_name}")
#                 return pd.DataFrame()
                
#             # Conversion et nettoyage
#             df['Datetime'] = pd.to_datetime(df['Datetime'], utc=True, errors='coerce')
#             df = df[~df['Datetime'].isna()]  # Supprime les dates invalides
#             df.set_index('Datetime', inplace=True)
            
#             # Renommage si nécessaire
#             if 'Rel_H_Pct' in df.columns:
#                 df.rename(columns={'Rel_H_Pct': 'Rel_H_%'}, inplace=True)

#         # Cas 2: Plages manquantes (missing_before)
#         elif processing_type == 'missing_before':
#             logging.info(f"Chargement plages manquantes {table_name} ({processing_type})")
            
#             try:
#                 df = pd.read_sql(query.as_string(conn), conn)
                
#                 # Conversion des dates
#                 for col in ['start_time', 'end_time']:
#                     if col in df.columns:
#                         df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')
                
#                 # Ajout du nom de station si absent
#                 if 'station' not in df.columns:
#                     df['station'] = station_name
                    
#             except psycopg2.errors.UndefinedTable:
#                 logging.warning(f"Table {table_name} n'existe pas dans missing_before")
#                 return pd.DataFrame()

#         # Cas 3: missing_after (gestion spéciale)
#         elif processing_type == 'missing_after':
#             # Pas de table créée si pas de données manquantes après traitement
#             logging.info(f"Tentative de chargement missing_after pour {table_name}")
#             return pd.DataFrame()

#     except psycopg2.errors.UndefinedTable:
#         logging.warning(f"Table {table_name} n'existe pas dans {processing_type}")
#         return pd.DataFrame()
        
#     except Exception as e:
#         logging.error(f"Erreur chargement {station_name} ({processing_type}): {str(e)}")
#         traceback.print_exc()
        
#     finally:
#         if conn:
#             conn.close()
#             logging.info(f"Connexion {processing_type} fermée")

#     return df



######################## Date du 28 Juil 2025 ########################

import pandas as pd
import psycopg2
from psycopg2 import sql # Importation explicite du module sql
from psycopg2 import OperationalError, ProgrammingError, Error as Psycopg2Error
import logging
import warnings
import traceback

# # --- Fonction load_station_data ---
# def load_station_data(station_name: str, processing_type: str = 'before') -> pd.DataFrame:
#     """
#     Charge les données d'une station depuis la base de données PostgreSQL spécifiée par 'processing_type'.
#     Cette fonction s'adapte automatiquement aux schémas de table :
#     - Pour 'before' et 'after': utilise 'Datetime' comme index temporel et gère les conversions.
#     - Pour 'missing_before' et 'missing_after': utilise 'id' comme clé primaire (sans index temporel)
#       et convertit 'start_time'/'end_time' en objets datetime.

#     Args:
#         station_name (str): Le nom de la station (correspond au nom de la table dans la base de données).
#         processing_type (str): La clé de la base de données à laquelle se connecter
#                                ('before', 'after', 'missing_before', 'missing_after').

#     Returns:
#         pd.DataFrame: Un DataFrame pandas contenant les données extraites.
#                       Retourne un DataFrame vide si la connexion échoue, la table est introuvable,
#                       la configuration des colonnes est absente ou le schéma est inconnu.
#     """
#     db_key_for_connection = processing_type

#     conn = None
#     df = pd.DataFrame() # Initialiser un DataFrame vide par défaut
#     try:
#         # Établir la connexion à la base de données en utilisant la clé fournie
#         conn = get_connection(db_key_for_connection)
#         if not conn:
#             warnings.warn(f"Impossible d'obtenir une connexion valide pour la clé '{db_key_for_connection}'. Retourne un DataFrame vide.")
#             return pd.DataFrame()

#         table_name = station_name.strip()
        
#         # Récupérer les informations sur les colonnes de la table depuis la configuration db.py
#         db_columns_info = get_station_columns(station_name, processing_type)
#         if not db_columns_info:
#             warnings.warn(f"Impossible de récupérer la configuration des colonnes pour la station '{station_name}' avec le type de traitement '{processing_type}'. Retourne un DataFrame vide.")
#             return pd.DataFrame()

#         db_columns = list(db_columns_info.keys())
        
#         # Construire la requête SQL pour sélectionner toutes les colonnes
#         column_identifiers = [sql.Identifier(col) for col in db_columns]
#         query = sql.SQL("SELECT {} FROM {};").format(
#             sql.SQL(', ').join(column_identifiers),
#             sql.Identifier(table_name)
#         )
        
#         # --- Logique d'adaptation pour la lecture du DataFrame en fonction du schéma ---
#         if 'Datetime' in db_columns:
#             logging.info(f"Chargement des données de séries temporelles pour '{table_name}' depuis '{processing_type}'.")
            
#             df = pd.read_sql(query.as_string(conn), conn, index_col='Datetime')
            
#             initial_rows_after_load = len(df)
#             df.index = pd.to_datetime(df.index, utc=True, errors='coerce')
            
#             if df.index.hasnans:
#                 df = df[df.index.notna()]
#                 if len(df) < initial_rows_after_load:
#                     warnings.warn(f"Suppression de {initial_rows_after_load - len(df)} lignes avec index Datetime invalide (NaT) après chargement pour la station {station_name} ({processing_type}).")

#             if 'Rel_H_Pct' in df.columns and 'Rel_H_%' not in df.columns:
#                 df.rename(columns={'Rel_H_Pct': 'Rel_H_%'}, inplace=True)
#                 logging.info(f"Colonne 'Rel_H_Pct' renommée en 'Rel_H_%%' pour le DataFrame de la station {station_name} ({processing_type}).")

#         elif 'id' in db_columns and 'start_time' in db_columns and 'end_time' in db_columns:
#             logging.info(f"Chargement des plages manquantes pour '{table_name}' depuis '{processing_type}'.")
#             df = pd.read_sql(query.as_string(conn), conn)
            
#             if 'start_time' in df.columns:
#                 df['start_time'] = pd.to_datetime(df['start_time'], utc=True, errors='coerce')
#             if 'end_time' in df.columns:
#                 df['end_time'] = pd.to_datetime(df['end_time'], utc=True, errors='coerce')

#         else:
#             warnings.warn(f"La configuration des colonnes pour la station '{station_name}' ({processing_type}) ne correspond à aucun schéma d'extraction connu ('Datetime' ou 'id'/'start_time'). Retourne un DataFrame vide.")
#             return pd.DataFrame(columns=list(db_columns_info.keys())) 

#     # --- Gestion des Exceptions ---
#     except ProgrammingError as e:
#         # Spécifique pour 'missing_after': si la table n'existe pas, c'est un cas INFO, pas une erreur.
#         if processing_type == 'missing_after' and "relation \"" in str(e) and "\" does not exist" in str(e):
#             logging.info(f"La table '{table_name}' pour '{processing_type}' n'existe pas, ce qui est attendu s'il n'y a pas de plages manquantes après traitement. Retourne un DataFrame vide avec les colonnes attendues.")
#             df = pd.DataFrame(columns=list(db_columns_info.keys()) if db_columns_info else [])
#         else:
#             # Pour toutes les autres ProgrammingError (y compris missing_before si la table n'existe pas)
#             logging.warning(f"La table '{table_name}' n'existe pas ou la requête est mal formée dans la base de données '{db_key_for_connection}'. Erreur: {e}. Retourne un DataFrame vide.")
#             df = pd.DataFrame(columns=list(db_columns_info.keys()) if db_columns_info else [])
#     except OperationalError as e:
#         logging.error(f"Erreur opérationnelle de base de données (connexion perdue, etc.) lors du chargement des données pour la station '{station_name}' ({processing_type}): {e}. Retourne un DataFrame vide.")
#         df = pd.DataFrame(columns=list(db_columns_info.keys()) if db_columns_info else [])
#     except Psycopg2Error as e:
#         logging.error(f"Erreur spécifique Psycopg2 lors du chargement des données pour la station '{station_name}' ({processing_type}): {e}")
#         traceback.print_exc()
#         df = pd.DataFrame(columns=list(db_columns_info.keys()) if db_columns_info else [])
#     except Exception as e:
#         warnings.warn(f"Erreur inattendue lors du chargement des données pour la station '{station_name}' ({processing_type}): {e}")
#         traceback.print_exc()
#         df = pd.DataFrame(columns=list(db_columns_info.keys()) if db_columns_info else [])
#     finally:
#         if conn:
#             conn.close()
#             logging.info(f"Connexion à la DB '{db_key_for_connection}' fermée.")
#     return df



def save_to_database(df: pd.DataFrame, station: str, conn, processing_type: str = 'raw') -> bool:
    """
    Sauvegarde un DataFrame dans la base de données, avec vérifications complètes et journalisation détaillée.
    La connexion à la base de données est passée en argument.
    """
    try:
        if df.empty:
            logging.warning(f"DataFrame vide reçu pour la station '{station}' ({processing_type}), aucune donnée à sauvegarder.")
            return True

        table_name = station.strip()

        db_key = processing_type 

        if not conn:
            raise ConnectionError(f"Connexion à la base de données non fournie ou nulle pour '{processing_type}'")

        columns_config = get_station_columns(station, processing_type)
        if not columns_config:
            raise ValueError(f"Configuration des colonnes manquante pour la station '{station}' ({processing_type}). Impossible de sauvegarder.")

        expected_columns = list(columns_config.keys())

        if isinstance(df.index, pd.DatetimeIndex):
            df_processed = df.reset_index()
            logging.info(f"DatetimeIndex converti en colonne 'Datetime'. Nouvelles colonnes: {df_processed.columns.tolist()}")
        else:
            df_processed = df.copy() 
            logging.info("Le DataFrame n'a pas de DatetimeIndex, aucune conversion nécessaire pour l'index.")

        if 'Datetime' not in df_processed.columns and 'id' not in df_processed.columns and processing_type not in ['missing_before', 'missing_after']:
            # This check is important if Datetime is expected but not found
            logging.warning(f"La colonne 'Datetime' est introuvable après la conversion de l'index pour la station {station}, et 'id' n'est pas non plus présent.")


        df_column_rename_map = {}
        for df_col in df_processed.columns: 
            if df_col == 'Rel_H_%':
                df_column_rename_map[df_col] = 'Rel_H_Pct'
            elif df_col.lower() == 'rel_h': 
                 df_column_rename_map[df_col] = 'Rel_H_Pct'

        if df_column_rename_map:
            df_processed = df_processed.rename(columns=df_column_rename_map) 
            logging.debug(f"Colonnes du DataFrame renommées: {df_column_rename_map}")

        logging.info("\n" + "="*50)
        logging.info("VÉRIFICATION DES NOMS DE COLONNES (CASSE ET CORRESPONDANCE)")
        logging.info("="*50)
        
        column_mapping_expected = {col.lower(): col for col in expected_columns}
        new_df_columns = []
        for col_df in df_processed.columns: 
            if col_df in expected_columns: 
                new_df_columns.append(col_df)
            elif col_df.lower() in column_mapping_expected: 
                new_df_columns.append(column_mapping_expected[col_df.lower()])
            else: 
                new_df_columns.append(col_df)
        
        df_processed.columns = new_df_columns 

        df_cols_after_norm = df_processed.columns.tolist() 
        missing_in_df = set(expected_columns) - set(df_cols_after_norm)
        extra_in_df = set(df_cols_after_norm) - set(expected_columns)
        
        logging.info(f"\nColonnes attendues par la DB: {expected_columns}")
        logging.info(f"Colonnes du DataFrame (après normalisation): {df_cols_after_norm}")

        if missing_in_df:
            logging.warning(f"\n⚠️ ATTENTION: Colonnes attendues mais absentes du DataFrame:")
            for col in missing_in_df:
                logging.warning(f"- {col}")
                df_processed[col] = None 
        
        if extra_in_df:
            logging.info(f"\nℹ️ INFO: Colonnes supplémentaires dans le DataFrame:")
            for col in extra_in_df:
                logging.info(f"- {col}")
            df_processed = df_processed.drop(columns=list(extra_in_df)) 

        if not missing_in_df and not extra_in_df and set(expected_columns) == set(df_processed.columns.tolist()): 
            logging.info("\n✅ Tous les noms de colonnes correspondent exactement")
        else:
            logging.warning("\n❌ Des divergences de noms de colonnes existent")

        df_processed = df_processed[expected_columns] 
        logging.info("\nOrdre final des colonnes:" + str(df_processed.columns.tolist())) 
        logging.info("="*50 + "\n")

        logging.info("\n" + "="*50)
        logging.info("VÉRIFICATION DES TYPES DE DONNÉES ET VALEURS ANORMALES")
        logging.info("="*50)
        
        nan_value_strings = ['NaN', 'NAN', 'nan', '', ' ', '#VALUE!', '-', 'NULL', 'null', 'N/A', 'NA', 'None', 'NONE', 'NV', 'N.V.', '#DIV/0!']
        
        for col, pg_type_str in columns_config.items():
            if col not in df_processed.columns: 
                logging.warning(f"\n⚠️ Colonne '{col}' non trouvée - ignorée")
                continue

            original_dtype = str(df_processed[col].dtype) 
            pg_base_type = pg_type_str.split()[0].lower()
            
            logging.info(f"\nColonne: {col}")
            logging.info(f"- Type Pandas original: {original_dtype}")
            logging.info(f"- Type PostgreSQL attendu: {pg_base_type}")
            
            try:
                df_processed[col] = df_processed[col].replace(nan_value_strings, np.nan) 
                
                if pg_base_type in ['timestamp', 'date']:
                    df_processed[col] = pd.to_datetime(df_processed[col], errors='coerce') 
                    logging.info("- Converti en datetime")
                elif pg_base_type in ['float', 'integer', 'double precision', 'real']: 
                    df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce') 
                    if pg_base_type == 'integer' and pd.api.types.is_float_dtype(df_processed[col]):
                        if (df_processed[col].notna() & (df_processed[col] % 1 != 0)).any():
                            logging.warning(f"Colonne '{col}' contient des décimales mais le type attendu est INTEGER. Les valeurs seront tronquées ou mises à NaN.")
                        df_processed[col] = df_processed[col].astype('Int64', errors='coerce') 
                    logging.info("- Converti en numérique")
                elif pg_base_type == 'boolean':
                    df_processed[col] = df_processed[col].apply(lambda x: bool(x) if pd.notna(x) else None) 
                    logging.info("- Converti en booléen")
                
                null_count = df_processed[col].isna().sum() 
                logging.info(f"- Valeurs NULL après conversion: {null_count}/{len(df_processed)} ({null_count/len(df_processed)*100:.2f}%)") 
                
                if null_count > 0:
                    sample_null = df_processed[df_processed[col].isna()].head(3) 
                    logging.info("- Exemple de lignes avec NULL:\n" + sample_null[[col]].to_string())
                    
            except Exception as e:
                logging.error(f"\n❌ ERREUR DE CONVERSION pour '{col}': {str(e)}")
                problematic_vals = df_processed[col][df_processed[col].notna() & pd.to_numeric(df_processed[col], errors='coerce').isna()]
                if not problematic_vals.empty:
                    logging.error("- Valeurs problématiques:" + str(problematic_vals.unique()[:5]))
                traceback.print_exc()

        logging.info("\nRésumé des types après conversion:")
        logging.info(str(df_processed.dtypes)) 
        logging.info("="*50 + "\n")

        logging.info("\n" + "="*50)
        logging.info("PRÉPARATION DES DONNÉES POUR INSERTION")
        logging.info("="*50)
        
        data_to_insert = []
        type_converters = []
        
        # Determine the columns to be actually inserted into the database
        # This is where we ensure 'id' (SERIAL PK) is not included if it's auto-generated.
        insert_columns = []
        if processing_type in ['missing_before', 'missing_after']:
            # For 'missing' tables, 'id' is SERIAL, so don't include it in INSERT statement
            insert_columns = [col for col in expected_columns if col != 'id']
        else:
            insert_columns = expected_columns # For 'before'/'after', all expected columns are inserted

        # Create type converters only for columns that will be inserted
        for col in insert_columns:
            pg_type_str = columns_config.get(col, 'text')
            pg_base_type = pg_type_str.split()[0].lower()

            if pg_base_type in ['timestamp', 'timestamp with time zone']:
                type_converters.append(lambda x: x.isoformat(timespec='microseconds') if pd.notna(x) else None)
            elif pg_base_type == 'date':
                type_converters.append(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
            elif pg_base_type in ['float', 'double precision', 'real']:
                type_converters.append(lambda x: float(x) if pd.notna(x) else None)
            elif pg_base_type == 'integer':
                type_converters.append(lambda x: int(x) if pd.notna(x) and pd.api.types.is_integer(x) else None)
            elif pg_base_type == 'boolean':
                type_converters.append(lambda x: bool(x) if pd.notna(x) else None)
            else:
                type_converters.append(lambda x: str(x) if pd.notna(x) else None)

        for i, row_tuple in enumerate(df_processed.itertuples(index=False)):
            try:
                # Get values from df_processed corresponding to `insert_columns`
                row_values_for_insert = []
                for col_name in insert_columns:
                    # Find the index of the column in the df_processed.columns
                    # This is safer than assuming the order of row_tuple matches insert_columns directly
                    col_idx_in_df_processed = df_processed.columns.get_loc(col_name)
                    row_values_for_insert.append(row_tuple[col_idx_in_df_processed])

                converted_row_values = [type_converters[j](row_values_for_insert[j]) for j in range(len(insert_columns))]
                data_to_insert.append(tuple(converted_row_values))
                
                if i < 2:
                    logging.debug(f"\nExemple ligne {i}:")
                    for j, col in enumerate(insert_columns): # Use insert_columns here
                        val = converted_row_values[j]
                        logging.debug(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                        
            except Exception as e:
                logging.error(f"\n❌ ERREUR lors de la préparation de la ligne {i}: {str(e)}")
                logging.error("Valeurs brutes de la ligne problématique (peut différer après conversion de type):")
                logging.error(str(df_processed.iloc[i].to_dict()))
                traceback.print_exc()
                continue

        logging.info(f"\nTotal lignes préparées: {len(data_to_insert)}")
        logging.info("="*50 + "\n")

        if not conn:
            raise ConnectionError(f"Connexion à la base de données non fournie ou nulle pour la station '{station}'")

        conn.autocommit = True

        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = %s
                )
            """, (table_name,))
            table_exists = cursor.fetchone()[0]

            if not table_exists:
                # TOUJOURS créer la table pour missing_before/missing_after
                if processing_type in ['missing_before', 'missing_after']:
                    if not create_station_table(station, processing_type):
                        raise Exception(f"Échec création table '{table_name}'")
                # Pour les autres types, seulement créer si le DataFrame n'est pas vide
                elif not df.empty:
                    if not create_station_table(station, processing_type):
                        raise Exception(f"Échec création table '{table_name}'")
                else:
                    return True  # Ne pas créer de table vide pour before/after
    
            
            # if not table_exists:
            #     logging.info(f"\nℹ️ La table '{table_name}' n'existe pas, création...")
            #     if not create_station_table(station, processing_type): 
            #         raise Exception(f"Échec de la création de la table '{table_name}'")
                logging.info("✅ Table créée avec succès")

            # ========== DÉTECTION ET CONSTRUCTION DE LA REQUÊTE SQL ==========
            pk_col = None
            cols_to_insert_in_query = [] 
            
            if processing_type in ['raw', 'before', 'after']:
                if 'Datetime' in columns_config: 
                    pk_col = 'Datetime'
                    logging.info(f"✅ Clé primaire détectée pour '{processing_type}': '{pk_col}'")
                    cols_to_insert_in_query = expected_columns # All columns, including Datetime PK
                else:
                    logging.warning(f"⚠️ 'Datetime' n'est pas configuré comme PK pour '{processing_type}'.")
                    cols_to_insert_in_query = expected_columns # Fallback to all columns
            elif processing_type in ['missing_before', 'missing_after']:
                if 'id' in columns_config: 
                    pk_col = 'id'
                    # Exclude 'id' from insert columns as it's SERIAL
                    cols_to_insert_in_query = [col for col in expected_columns if col != 'id']
                    logging.info(f"✅ Clé primaire détectée pour '{processing_type}': '{pk_col}' (SERIAL, non incluse dans l'INSERT direct)")
                else:
                    logging.warning(f"⚠️ 'id' n'est pas configuré comme PK pour '{processing_type}'.")
                    cols_to_insert_in_query = expected_columns # Fallback to all columns
            
            # Fallback if PK not detected by the processing_type logic
            if not pk_col:
                logging.warning("ℹ️ Aucune clé primaire automatique détectée. Tentative de détection par la DB (moins efficace).")
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
                    if pk_info:
                        pk_col = pk_info[0]
                        logging.info(f"✅ Clé primaire détectée via DB: {pk_col}")
                        if pk_col == 'id' and 'id' in cols_to_insert_in_query: 
                             cols_to_insert_in_query = [col for col in cols_to_insert_in_query if col != 'id'] # Ensure it's removed
                    else:
                        logging.info("ℹ️ Aucune clé primaire détectée par la DB.")
                except Exception as e:
                    logging.error(f"❌ Erreur lors de la détection de la clé primaire via DB: {str(e)}")

            cols_sql = ', '.join([f'"{col}"' for col in cols_to_insert_in_query])
            placeholders = ', '.join(['%s'] * len(cols_to_insert_in_query))
            
            query = ""
            if pk_col == 'Datetime': 
                query = f"""
                    INSERT INTO "{table_name}" ({cols_sql})
                    VALUES ({placeholders})
                    ON CONFLICT ("{pk_col}") DO NOTHING;
                """
                logging.info(f"ℹ️ Utilisation de ON CONFLICT ('{pk_col}') DO NOTHING pour '{table_name}'.")
            elif pk_col == 'id' and processing_type in ['missing_before', 'missing_after']: # Specific case for SERIAL PK 'id'
                query = f"""
                    INSERT INTO "{table_name}" ({cols_sql})
                    VALUES ({placeholders});
                """
                logging.info(f"ℹ️ Insertion simple pour '{table_name}' (clé primaire '{pk_col}' SERIAL, non incluse dans l'INSERT).")
            else: 
                query = f"""
                    INSERT INTO "{table_name}" ({cols_sql})
                    VALUES ({placeholders});
                """
                logging.warning(f"⚠️ Insertion simple sans ON CONFLICT pour '{table_name}' (aucune clé primaire pertinente pour ON CONFLICT).")

            logging.info(f"\nRequête SQL générée:\n{query}")

            logging.info("\n" + "="*50)
            logging.info("VÉRIFICATION FINALE AVANT INSERTION")
            logging.info("="*50)
            
            logging.info(f"\nNombre de colonnes attendues pour l'insertion (basé sur la requête SQL): {len(cols_to_insert_in_query)}")
            logging.info(f"Nombre de valeurs dans chaque ligne de data_to_insert: {len(data_to_insert[0]) if data_to_insert else 0}")
            
            if data_to_insert and len(cols_to_insert_in_query) != len(data_to_insert[0]):
                logging.error("\n❌ ERREUR: Nombre de colonnes incompatible entre la requête SQL et les données préparées!")
                logging.error("Colonnes dans la requête SQL:" + str(cols_to_insert_in_query))
                logging.error("Première ligne de données préparées:" + str(data_to_insert[0]))
                raise ValueError("Incompatibilité de colonnes après préparation") 

            if data_to_insert:
                first_row = data_to_insert[0]
                logging.info(f"\nPremière ligne de données ({len(first_row)} valeurs):")
                for val, col in zip(first_row, cols_to_insert_in_query): # Use cols_to_insert_in_query here
                    logging.info(f"- {col}: {val} ({type(val).__name__ if val is not None else 'NULL'})")
                
                try:
                    mogrified = cursor.mogrify(query, first_row).decode('utf-8')
                    logging.info("\n✅ Test mogrify réussi:\n" + mogrified[:500] + ("..." if len(mogrified) > 500 else ""))
                except Exception as e:
                    logging.error("\n❌ ERREUR mogrify:" + str(e))
                    traceback.print_exc()
                    raise 
            else:
                logging.warning("\n⚠️ Aucune donnée à insérer!")
                return False

            logging.info("="*50 + "\n")

            batch_size = 10000
            start_time = datetime.now()
            
            logging.info(f"\nDébut de l'insertion de {len(data_to_insert)} lignes par lots de {batch_size}...")
            
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i:i + batch_size]
                try:
                    from psycopg2 import extras
                    extras.execute_batch(cursor, query, batch)
                    
                    if (i // batch_size) % 10 == 0:
                        elapsed = (datetime.now() - start_time).total_seconds()
                        logging.info(f"Lot {i//batch_size + 1}: {len(batch)} lignes traitées en {elapsed:.2f} secondes au total...")
                        
                except Exception as e:
                    conn.rollback() 
                    logging.error(f"\n❌ ERREUR lot {i//batch_size + 1}: {str(e)}")
                    if batch:
                        logging.error("Exemple de ligne problématique:" + str(batch[0]))
                    traceback.print_exc()
                    raise 

            total_time = (datetime.now() - start_time).total_seconds()
            logging.info(f"\n✅ Traitement d'insertion terminé pour '{station}': {len(data_to_insert)} lignes préparées en {total_time:.2f} secondes.")
            logging.info("Note: Le nombre exact de lignes insérées peut différer en raison de la clause ON CONFLICT.")
            return True

    except Exception as e:
        logging.error(f"\n❌❌❌ ERREUR CRITIQUE lors de la sauvegarde pour '{station}': {str(e)}")
        traceback.print_exc()
        
        if 'df_processed' in locals():
            logging.error("\nÉtat du DataFrame au moment de l'erreur:")
            logging.error(f"Shape: {df_processed.shape}")
            logging.error("5 premières lignes:\n" + df_processed.head().to_string())
            
        return False

def load_station_data(station_name: str, processing_type: str = 'raw') -> pd.DataFrame:
    """
    Charge les données d'une station depuis la base de données PostgreSQL.
    Gère proprement le cas où la table n'existe pas encore.
    """
    db_key_for_connection = processing_type
    conn = None
    table_name = station_name.strip()

    try:
        conn = get_connection(db_key_for_connection)
        if not conn:
            raise ConnectionError(f"Impossible de se connecter à la base {db_key_for_connection}")

        # Vérifier d'abord si la table existe
        table_exists = False
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name = %s
                )
            """, (table_name,))
            table_exists = cursor.fetchone()[0]

        if not table_exists:
            # Pour les tables missing_*, retourner un DataFrame vide avec le bon schéma
            if processing_type in ['missing_before', 'missing_after']:
                columns_config = get_station_columns(station_name, processing_type)
                if columns_config:
                    return pd.DataFrame(columns=columns_config.keys())
                return pd.DataFrame(columns=['id', 'station', 'variable', 'start_time', 'end_time', 'duration_hours'])
            return pd.DataFrame()

        # Si la table existe, charger les données
        columns_config = get_station_columns(station_name, processing_type)
        if not columns_config:
            return pd.DataFrame()

        db_columns = list(columns_config.keys())
        column_identifiers = [sql.Identifier(col) for col in db_columns]

        query = sql.SQL("SELECT {} FROM {};").format(
            sql.SQL(', ').join(column_identifiers),
            sql.Identifier(table_name))

        # Chargement adapté au type de traitement
        if processing_type in ['raw','before', 'after']:
            df = pd.read_sql(query.as_string(conn), conn, index_col='Datetime')
            df.index = pd.to_datetime(df.index, utc=True, errors='coerce')
            
            # Gestion spécifique des colonnes
            if 'Rel_H_Pct' in df.columns and 'Rel_H_%' not in df.columns:
                df.rename(columns={'Rel_H_Pct': 'Rel_H_%'}, inplace=True)
        else:
            df = pd.read_sql(query.as_string(conn), conn)

        return df

    except Exception as e:
        logging.error(f"Erreur lors du chargement des données pour {station_name} ({processing_type}): {str(e)}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()
            logging.info(f"Connexion à la DB '{db_key_for_connection}' fermée.")