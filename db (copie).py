
# db.py
import os
from typing import Dict, List
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import logging
from datetime import datetime

# Charger les variables d'environnement
load_dotenv()

# Configuration du logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.before_db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME_BEFORE'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        self.after_db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME_AFTER'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        self.before_conn = None
        self.after_conn = None
        self.connect()

    def connect(self):
        """Établir les connexions aux bases de données"""
        try:
            self.before_conn = psycopg2.connect(**self.before_db_config)
            self.after_conn = psycopg2.connect(**self.after_db_config)
            logger.info("Connexions aux bases de données établies avec succès")
        except Exception as e:
            logger.error(f"Erreur de connexion aux bases de données: {e}")
            raise

    def close(self):
        """Fermer les connexions aux bases de données"""
        if self.before_conn:
            self.before_conn.close()
        if self.after_conn:
            self.after_conn.close()
        logger.info("Connexions aux bases de données fermées")

    def execute_query(self, query: str, params=None, fetch=False, processing_type: str = 'BEFORE'):
        """Exécuter une requête SQL sur la base appropriée"""
        conn = self.before_conn if processing_type == 'BEFORE' else self.after_conn
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params or ())
                if fetch:
                    return cursor.fetchall()
                conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Erreur lors de l'exécution de la requête: {e}")
            raise

    def create_tables_for_stations(self, stations: List[str], processing_type: str = 'BEFORE'):
        """
        Créer les tables pour les stations dans la base de données spécifiée
        selon le schéma approprié (avant ou après traitement)
        """
        conn = self.before_conn if processing_type == 'BEFORE' else self.after_conn
        try:
            with conn.cursor() as cursor:
                for station in stations:
                    columns = self._get_station_schema(station, processing_type)
                    table_name = station.replace(' ', '_').lower()
                    
                    create_table_query = sql.SQL("""
                        CREATE TABLE IF NOT EXISTS {} (
                            id SERIAL PRIMARY KEY,
                            {}
                        )
                    """).format(
                        sql.Identifier(table_name),
                        sql.SQL(',\n').join([sql.Identifier(col) + ' ' + sql.SQL(self._get_column_type(col)) 
                               for col in columns])
                    )
                    
                    cursor.execute(create_table_query)
                    logger.info(f"Table {table_name} créée dans la base {processing_type}")
                
                conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Erreur lors de la création des tables: {e}")
            raise

    def _get_station_schema(self, station: str, processing_type: str) -> List[str]:
        """
        Retourne le schéma de colonnes approprié pour une station donnée
        et un type de traitement (BEFORE ou AFTER)
        """
        # Bassin DANO
        if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
            if processing_type == 'BEFORE':
                return ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_01_mm', 'Rain_02_mm', 
                        'Air_Temp_Deg_C', 'Rel_H_%', 'Solar_R_W/m^2', 'Wind_Sp_m/sec', 'Wind_Dir_Deg']
            else:
                return ['Datetime', 'Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_01_mm', 'Rain_02_mm', 
                        'Rain_mm', 'Air_Temp_Deg_C', 'Rel_H_%', 'Solar_R_W/m^2', 'Wind_Sp_m/sec', 
                        'Wind_Dir_Deg', 'sunrise_time_utc', 'sunset_time_utc', 'Is_Daylight', 'Daylight_Duration']
        
        elif station in ['Lare', 'Tambiri 2']:
            if processing_type == 'BEFORE':
                return ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_01_mm', 'Rain_02_mm']
            else:
                return ['Datetime', 'Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_01_mm', 'Rain_02_mm', 'Rain_mm']
        
        elif station == 'Tambiri 1':
            if processing_type == 'BEFORE':
                return ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_mm', 'BP_mbar_Avg', 
                        'Air_Temp_Deg_C', 'Rel_H_%', 'Wind_Sp_m/sec', 'Wind_Dir_Deg']
            else:
                return ['Datetime', 'Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_mm', 'BP_mbar_Avg', 
                        'Air_Temp_Deg_C', 'Rel_H_%', 'Wind_Sp_m/sec', 'Wind_Dir_Deg']
        
        # Bassin DASSARI
        elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
            if processing_type == 'BEFORE':
                return ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_01_mm', 'Rain_02_mm', 
                        'Air_Temp_Deg_C', 'Rel_H_%', 'Solar_R_W/m^2', 'Wind_Sp_m/sec', 'Wind_Dir_Deg']
            else:
                return ['Datetime', 'Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_01_mm', 'Rain_02_mm', 
                        'Rain_mm', 'Air_Temp_Deg_C', 'Rel_H_%', 'Solar_R_W/m^2', 'Wind_Sp_m/sec', 
                        'Wind_Dir_Deg', 'sunrise_time_utc', 'sunset_time_utc', 'Is_Daylight', 'Daylight_Duration']
        
        elif station == 'Ouriyori 1':
            if processing_type == 'BEFORE':
                return ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_01_mm', 'Rain_02_mm', 
                        'Air_Temp_Deg_C', 'Rel_H_%', 'Solar_R_W/m^2', 'Wind_Sp_m/sec', 'Wind_Dir_Deg']
            else:
                return ['Datetime', 'Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_01_mm', 'Rain_02_mm', 
                        'Rain_mm', 'Air_Temp_Deg_C', 'Rel_H_%', 'Solar_R_W/m^2', 'Wind_Sp_m/sec', 
                        'Wind_Dir_Deg', 'sunrise_time_utc', 'sunset_time_utc', 'Is_Daylight', 'Daylight_Duration']
        
        # Bassin VEA SISSILI
        elif station in ['Oualem', 'Nebou', 'Nabugubelle', 'Manyoro', 'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabisi']:
            if processing_type == 'BEFORE':
                return ['Date', 'Rain_mm', 'Air_Temp_Deg_C', 'Rel_H_%', 
                        'Solar_R_W/m^2', 'Wind_Sp_m/sec', 'Wind_Dir_Deg']
            else:
                return ['Datetime', 'Date', 'Rain_mm',   'Air_Temp_Deg_C', 
                        'Rel_H_%', 'Solar_R_W/m^2', 'Wind_Sp_m/sec', 'Wind_Dir_Deg', 
                        'sunrise_time_utc', 'sunset_time_utc', 'Is_Daylight', 'Daylight_Duration']
        
        elif station == 'Atampisi':
            if processing_type == 'BEFORE':
                return ['Date', 'Rain_mm', 'Air_Temp_Deg_C', 'Rel_H_%', 
                        'Solar_R_W/m^2', 'Wind_Sp_m/sec', 'Wind_Dir_Deg']
            else:
                return ['Datetime', 'Date', 'Rain_mm', 'Air_Temp_Deg_C', 'Rel_H_%', 
                        'Solar_R_W/m^2', 'Wind_Sp_m/sec', 'Wind_Dir_Deg', 
                        'sunrise_time_utc', 'sunset_time_utc', 'Is_Daylight', 'Daylight_Duration']
        
        else:
            raise ValueError(f"Station non reconnue: {station}")

    def _get_column_type(self, column_name: str) -> str:
        """
        Retourne le type de colonne PostgreSQL approprié en fonction du nom de la colonne
        """
        if column_name in ['Datetime', 'Date', 'sunrise_time_utc', 'sunset_time_utc']:
            return 'TIMESTAMP'
        elif column_name in ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Is_Daylight']:
            return 'INTEGER'
        elif 'Rain' in column_name or 'Solar' in column_name or 'Wind_Sp' in column_name or 'Air_Temp' in column_name:
            return 'FLOAT'
        elif column_name == 'Rel_H_%':
            return 'FLOAT'
        elif column_name == 'Wind_Dir_Deg':
            return 'FLOAT'
        elif column_name == 'Daylight_Duration':
            return 'INTERVAL'
        elif column_name == 'BP_mbar_Avg':
            return 'FLOAT'
        else:
            return 'TEXT'

    def save_to_database(self, df: pd.DataFrame, station: str, processing_type: str = 'BEFORE'):
        """
        Sauvegarde les données dans la table appropriée de la base de données
        """
        conn = self.before_conn if processing_type == 'BEFORE' else self.after_conn
        try:
            columns = self._get_station_schema(station, processing_type)
            table_name = station.replace(' ', '_').lower()
            
            data = df[columns].to_dict('records')
            
            insert_query = sql.SQL("""
                INSERT INTO {} ({})
                VALUES ({})
                ON CONFLICT DO NOTHING
            """).format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                sql.SQL(', ').join(sql.Placeholder() * len(columns))
            )
            
            with conn.cursor() as cursor:
                execute_batch(cursor, insert_query, [tuple(record.values()) for record in data])
                conn.commit()
            
            logger.info(f"Données sauvegardées dans {table_name} (base {processing_type})")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Erreur lors de la sauvegarde des données: {e}")
            raise

    def get_station_data(self, station: str, processing_type: str = 'AFTER', 
                         start_date: datetime = None, end_date: datetime = None) -> pd.DataFrame:
        """
        Récupère les données d'une station depuis la base de données
        """
        conn = self.before_conn if processing_type == 'BEFORE' else self.after_conn
        try:
            table_name = station.replace(' ', '_').lower()
            
            query = sql.SQL("SELECT * FROM {}").format(
                sql.Identifier(table_name)
            )
            
            params = []
            if start_date or end_date:
                conditions = []
                if start_date:
                    conditions.append(sql.SQL("Datetime >= %s"))
                    params.append(start_date)
                if end_date:
                    conditions.append(sql.SQL("Datetime <= %s"))
                    params.append(end_date)
                
                query = sql.SQL("{} WHERE {}").format(
                    query,
                    sql.SQL(" AND ").join(conditions)
                )
            
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                
                if data:
                    return pd.DataFrame(data, columns=columns)
                return pd.DataFrame(columns=columns)
                
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des données: {e}")
            raise

# Singleton pour gérer les connexions aux bases de données
db_manager = DatabaseManager()



import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from typing import Dict, List, Optional
import traceback

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

# def initialize_database():
#     """Initialise les bases de données et les tables nécessaires"""
#     try:
#         # Créer les bases de données si elles n'existent pas
#         with get_connection('postgres') as conn:
#             conn.autocommit = True
#             with conn.cursor() as cursor:
#                 # Créer la base de données BEFORE_PROCESSING
#                 cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'before_processing_db'")
#                 if not cursor.fetchone():
#                     cursor.execute("CREATE DATABASE before_processing_db OWNER Wascal")
                
#                 # Créer la base de données AFTER_PROCESSING
#                 cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'after_processing_db'")
#                 if not cursor.fetchone():
#                     cursor.execute("CREATE DATABASE after_processing_db OWNER Wascal")
        
#         # Créer les schémas et tables dans chaque base de données
#         for db_name in ['before_processing_db', 'after_processing_db']:
#             with get_connection(db_name) as conn:
#                 with conn.cursor() as cursor:
#                     # Créer le schéma si nécessaire
#                     cursor.execute("CREATE SCHEMA IF NOT EXISTS meteo")
        
#         print("Bases de données initialisées avec succès")
#     except Exception as e:
#         print(f"Erreur lors de l'initialisation de la base de données: {e}")
#         raise

# def initialize_database():
#     """Initialise les bases de données et le schéma meteo"""
#     try:
#         db_user = DB_CONFIG['user']
        
#         # 1. Créer les bases de données si elles n'existent pas
#         with get_connection('postgres') as conn:
#             conn.autocommit = True
#             with conn.cursor() as cursor:
#                 for db_name in ['before_processing_db', 'after_processing_db']:
#                     # Vérifier si la base existe
#                     cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
#                     if not cursor.fetchone():
#                         cursor.execute(f"""
#                             CREATE DATABASE {db_name}
#                             WITH OWNER = {db_user}
#                             ENCODING = 'UTF8'
#                         """)
#                         print(f"Base {db_name} créée avec succès")
        
#         # 2. Créer le schéma meteo dans chaque base
#         for db_name in ['before_processing_db', 'after_processing_db']:
#             with get_connection(db_name) as conn:
#                 conn.autocommit = True
#                 with conn.cursor() as cursor:
#                     # Vérifier si le schéma existe déjà
#                     cursor.execute("""
#                         SELECT 1 FROM information_schema.schemata 
#                         WHERE schema_name = 'meteo'
#                     """)
#                     if not cursor.fetchone():
#                         cursor.execute(f"""
#                             CREATE SCHEMA meteo
#                             AUTHORIZATION {db_user}
#                         """)
#                         print(f"Schéma meteo créé dans {db_name}")
        
#         print("Initialisation de la base de données terminée avec succès")
#         return True
        
#     except Exception as e:
#         print(f"Erreur lors de l'initialisation: {e}")
#         raise


# def get_station_columns(self, station: str, processing_type: str) -> List[str]:
#         """
#         Retourne le schéma de colonnes approprié pour une station donnée
#         et un type de traitement (BEFORE ou AFTER)
#         """

#         # Nettoyer le nom de la station
#         station = station.strip()

#         # Bassin DANO
#         if station in ['Dreyer Foundation', 'Bankandi', 'Wahablé', 'Fafo', 'Yabogane']:
#             if processing_type == 'BEFORE':
#                 return ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_01_mm', 'Rain_02_mm', 
#                         'Air_Temp_Deg_C', 'Rel_H_%', 'Solar_R_W/m^2', 'Wind_Sp_m/sec', 'Wind_Dir_Deg']
#             else:
#                 return ['Datetime', 'Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_01_mm', 'Rain_02_mm', 
#                         'Rain_mm', 'Air_Temp_Deg_C', 'Rel_H_%', 'Solar_R_W/m^2', 'Wind_Sp_m/sec', 
#                         'Wind_Dir_Deg', 'sunrise_time_utc', 'sunset_time_utc', 'Is_Daylight', 'Daylight_Duration']
        
#         elif station in ['Lare', 'Tambiri 2']:
#             if processing_type == 'BEFORE':
#                 return ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_01_mm', 'Rain_02_mm']
#             else:
#                 return ['Datetime', 'Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_01_mm', 'Rain_02_mm', 'Rain_mm']
        
#         elif station == 'Tambiri 1':
#             if processing_type == 'BEFORE':
#                 return ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_mm', 'BP_mbar_Avg', 
#                         'Air_Temp_Deg_C', 'Rel_H_%', 'Wind_Sp_m/sec', 'Wind_Dir_Deg']
#             else:
#                 return ['Datetime', 'Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_mm', 'BP_mbar_Avg', 
#                         'Air_Temp_Deg_C', 'Rel_H_%', 'Wind_Sp_m/sec', 'Wind_Dir_Deg']
        
#         # Bassin DASSARI
#         elif station in ['Nagasséga', 'Koundri', 'Koupendri', 'Pouri', 'Fandohoun']:
#             if processing_type == 'BEFORE':
#                 return ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_01_mm', 'Rain_02_mm', 
#                         'Air_Temp_Deg_C', 'Rel_H_%', 'Solar_R_W/m^2', 'Wind_Sp_m/sec', 'Wind_Dir_Deg']
#             else:
#                 return ['Datetime', 'Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_01_mm', 'Rain_02_mm', 
#                         'Rain_mm', 'Air_Temp_Deg_C', 'Rel_H_%', 'Solar_R_W/m^2', 'Wind_Sp_m/sec', 
#                         'Wind_Dir_Deg', 'sunrise_time_utc', 'sunset_time_utc', 'Is_Daylight', 'Daylight_Duration']
        
#         elif station == 'Ouriyori 1':
#             if processing_type == 'BEFORE':
#                 return ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_01_mm', 'Rain_02_mm', 
#                         'Air_Temp_Deg_C', 'Rel_H_%', 'Solar_R_W/m^2', 'Wind_Sp_m/sec', 'Wind_Dir_Deg']
#             else:
#                 return ['Datetime', 'Year', 'Month', 'Day', 'Hour', 'Minute', 'Rain_01_mm', 'Rain_02_mm', 
#                         'Rain_mm', 'Air_Temp_Deg_C', 'Rel_H_%', 'Solar_R_W/m^2', 'Wind_Sp_m/sec', 
#                         'Wind_Dir_Deg', 'sunrise_time_utc', 'sunset_time_utc', 'Is_Daylight', 'Daylight_Duration']
        
#         # Bassin VEA SISSILI
#         elif station in ['Oualem', 'Nebou', 'Nabugubelle', 'Manyoro', 'Gwosi', 'Doninga', 'Bongo Soe', 'Aniabisi']:
#             if processing_type == 'BEFORE':
#                 return ['Date', 'Rain_mm', 'Air_Temp_Deg_C', 'Rel_H_%', 
#                         'Solar_R_W/m^2', 'Wind_Sp_m/sec', 'Wind_Dir_Deg']
#             else:
#                 return ['Datetime', 'Date', 'Rain_mm',   'Air_Temp_Deg_C', 
#                         'Rel_H_%', 'Solar_R_W/m^2', 'Wind_Sp_m/sec', 'Wind_Dir_Deg', 
#                         'sunrise_time_utc', 'sunset_time_utc', 'Is_Daylight', 'Daylight_Duration']
        
#         elif station == 'Atampisi':
#             if processing_type == 'BEFORE':
#                 return ['Date', 'Rain_mm', 'Air_Temp_Deg_C', 'Rel_H_%', 
#                         'Solar_R_W/m^2', 'Wind_Sp_m/sec', 'Wind_Dir_Deg']
#             else:
#                 return ['Datetime', 'Date', 'Rain_mm', 'Air_Temp_Deg_C', 'Rel_H_%', 
#                         'Solar_R_W/m^2', 'Wind_Sp_m/sec', 'Wind_Dir_Deg', 
#                         'sunrise_time_utc', 'sunset_time_utc', 'Is_Daylight', 'Daylight_Duration']
        
#         else:
#             raise ValueError(f"Station non reconnue: {station}")


def get_station_columns(station: str, processing_type: str) -> Dict[str, str]:
    """Retourne les colonnes attendues pour une station donnée selon le type de traitement"""
    
    # Nettoyer le nom de la station
    station = station.strip()

    # BEFORE_PROCESSING
    if processing_type == 'before':
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
                #'Rain_02_mm': 'float',
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
                #'Rain_02_mm': 'float',
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

# def create_station_table(station: str, processing_type: str = 'before'):
#     """Crée une table pour une station donnée dans la base de données appropriée"""
#     columns = get_station_columns(station, processing_type)
#     if not columns:
#         raise ValueError(f"Configuration des colonnes non trouvée pour la station {station}")
    
#     db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
    
#     try:
#         with get_connection(db_name) as conn:
#             with conn.cursor() as cursor:
#                 # Créer la table si elle n'existe pas
#                 column_defs = [f'"{col}" {dtype}' for col, dtype in columns.items()]
#                 create_table_query = sql.SQL("""
#                     CREATE TABLE IF NOT EXISTS meteo.{table_name} (
#                         {column_defs}
#                     )
#                 """).format(
#                     table_name=sql.Identifier(station.replace(' ', '_').replace('-', '_')),
#                     column_defs=sql.SQL(',\n').join(map(sql.SQL, column_defs)))
                
#                 cursor.execute(create_table_query)
#                 conn.commit()
                
#                 # Créer un index sur la colonne Datetime si elle existe
#                 if 'Datetime' in columns:
#                     cursor.execute(sql.SQL("""
#                         CREATE INDEX IF NOT EXISTS idx_{table_name}_datetime 
#                         ON meteo.{table_name} (Datetime)
#                     """).format(
#                         table_name=sql.Identifier(station.replace(' ', '_').replace('-', '_'))))
#                     conn.commit()
                
#                 return True
#     except Exception as e:
#         print(f"Erreur lors de la création de la table pour {station}: {e}")
#         raise



# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before'):
#     """Sauvegarde un DataFrame dans la base de données appropriée"""
#     if df.empty:
#         print("Aucune donnée à sauvegarder")
#         return False
    
#     # Créer la table si elle n'existe pas
#     create_station_table(station, processing_type)
    
#     db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
#     table_name = station.replace(' ', '_').replace('-', '_')
#     columns = get_station_columns(station, processing_type)
    
#     # Vérifier que toutes les colonnes attendues sont présentes
#     missing_cols = set(columns.keys()) - set(df.columns)
#     if missing_cols:
#         print(f"Colonnes manquantes dans le DataFrame pour {station}: {missing_cols}")
#         return False
    
#     try:
#         with get_connection(db_name) as conn:
#             with conn.cursor() as cursor:
#                 # Convertir le DataFrame en liste de tuples
#                 data = [tuple(row) for row in df[list(columns.keys())].to_records(index=False)]
                
#                 # Construire la requête d'insertion
#                 insert_query = sql.SQL("""
#                     INSERT INTO meteo.{table_name} ({columns})
#                     VALUES ({placeholders})
#                     ON CONFLICT DO NOTHING
#                 """).format(
#                     table_name=sql.Identifier(table_name),
#                     columns=sql.SQL(', ').join(map(sql.Identifier, columns.keys())),
#                     placeholders=sql.SQL(', ').join(sql.Placeholder() * len(columns))
#                 )
#                 # Exécuter la requête en batch
#                 execute_batch(cursor, insert_query, data)
#                 conn.commit()
#                 return True
#     except Exception as e:
#         print(f"Erreur lors de la sauvegarde des données pour {station}: {e}")
#         raise


# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before'):
#     """Sauvegarde un DataFrame dans la base de données appropriée"""
#     if df.empty:
#         print("Aucune donnée à sauvegarder")
#         return False
    
#     # Créer la table si elle n'existe pas
#     create_station_table(station, processing_type)
    
#     db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
#     table_name = station.replace(' ', '_').replace('-', '_')
#     columns = get_station_columns(station, processing_type)
    
#     # Vérifier que toutes les colonnes attendues sont présentes
#     missing_cols = set(columns.keys()) - set(df.columns)
#     if missing_cols:
#         print(f"Colonnes manquantes dans le DataFrame pour {station}: {missing_cols}")
#         return False
    
#     try:
#         # Convertir les types NumPy en types natifs Python
#         df = df.copy()
#         for col in df.columns:
#             if df[col].dtype == 'int64':
#                 df[col] = df[col].astype('Int64')  # Type pandas nullable integer
#             elif df[col].dtype == 'float64':
#                 df[col] = df[col].astype('float')
        
#         with get_connection(db_name) as conn:
#             with conn.cursor() as cursor:
#                 # Convertir le DataFrame en liste de tuples avec des types natifs
#                 data = [tuple(int(x) if pd.api.types.is_integer(x) else 
#                              float(x) if pd.api.types.is_float(x) else 
#                              x for x in row) 
#                        for row in df[list(columns.keys())].to_records(index=False)]
                
#                 # Construire la requête d'insertion
#                 insert_query = sql.SQL("""
#                     INSERT INTO {table_name} ({columns})
#                     VALUES ({placeholders})
#                     ON CONFLICT DO NOTHING
#                 """).format(
#                     table_name=sql.Identifier(table_name),
#                     columns=sql.SQL(', ').join(map(sql.Identifier, columns.keys())),
#                     placeholders=sql.SQL(', ').join(sql.Placeholder() * len(columns)))
                
#                 execute_batch(cursor, insert_query, data)
#                 conn.commit()
#                 return True
#     except Exception as e:
#         print(f"Erreur lors de la sauvegarde des données pour {station}: {e}")
#         raise
        

# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before'):
#     """Sauvegarde un DataFrame dans la base de données appropriée"""
#     if df.empty:
#         print("Aucune donnée à sauvegarder")
#         return False
    
#     # Créer la table si elle n'existe pas
#     create_station_table(station, processing_type)
    
#     db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
#     table_name = station.replace(' ', '_').replace('-', '_')
#     columns = get_station_columns(station, processing_type)
    
#     # Vérification approfondie des colonnes
#     print(f"Colonnes attendues pour {station} ({processing_type}): {list(columns.keys())}")
#     print(f"Colonnes disponibles dans le DataFrame: {list(df.columns)}")
    
#     # Vérifier que toutes les colonnes attendues sont présentes
#     missing_cols = set(columns.keys()) - set(df.columns)
#     if missing_cols:
#         print(f"Colonnes manquantes dans le DataFrame pour {station}: {missing_cols}")
#         # Ajouter les colonnes manquantes avec des valeurs nulles
#         for col in missing_cols:
#             df[col] = None
    
#     try:
#         # Convertir les types NumPy en types natifs Python
#         df = df.copy()
#         for col in df.columns:
#             if df[col].dtype == 'int64':
#                 df[col] = df[col].astype('Int64')  # Type pandas nullable integer
#             elif df[col].dtype == 'float64':
#                 df[col] = df[col].astype('float')
        
#         with get_connection(db_name) as conn:
#             with conn.cursor() as cursor:
#                 # Vérifier à nouveau les colonnes avant insertion
#                 available_cols = [col for col in columns.keys() if col in df.columns]
#                 print(f"Colonnes qui seront insérées: {available_cols}")
                
#                 # Convertir le DataFrame en liste de tuples avec des types natifs
#                 data = [tuple(int(x) if pd.api.types.is_integer(x) else 
#                              float(x) if pd.api.types.is_float(x) else 
#                              x for x in row) 
#                        for row in df[available_cols].to_records(index=False)]
                
#                 # Construire la requête d'insertion avec seulement les colonnes disponibles
#                 insert_query = sql.SQL("""
#                     INSERT INTO {table_name} ({columns})
#                     VALUES ({placeholders})
#                     ON CONFLICT DO NOTHING
#                 """).format(
#                     table_name=sql.Identifier(table_name),
#                     columns=sql.SQL(', ').join(map(sql.Identifier, available_cols)),
#                     placeholders=sql.SQL(', ').join(sql.Placeholder() * len(available_cols)))
                
#                 execute_batch(cursor, insert_query, data)
#                 conn.commit()
#                 return True
#     except Exception as e:
#         print(f"Erreur lors de la sauvegarde des données pour {station}: {e}")
#         traceback.print_exc()
#         raise


# def create_station_table(station: str, processing_type: str = 'before'):
#     """Crée une table pour une station donnée dans la base de données appropriée"""
#     columns = get_station_columns(station, processing_type)
#     if not columns:
#         raise ValueError(f"Configuration des colonnes non trouvée pour la station {station}")
    
#     db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
#     table_name = station.replace(' ', '_').replace('-', '_').lower()
    
#     try:
#         with get_connection(db_name) as conn:
#             conn.autocommit = True  # Important pour les opérations de DDL
#             with conn.cursor() as cursor:
#                 # Vérifier si la table existe déjà
#                 cursor.execute("""
#                     SELECT EXISTS (
#                         SELECT FROM information_schema.tables 
#                         WHERE table_schema = 'meteo' 
#                         AND table_name = %s
#                     )
#                 """, (table_name,))
#                 table_exists = cursor.fetchone()[0]
                
#                 if not table_exists:
#                     # Créer la table avec le bon schéma
#                     column_defs = []
#                     for col, dtype in columns.items():
#                         if dtype == 'integer':
#                             pg_type = 'INTEGER'
#                         elif dtype == 'float':
#                             pg_type = 'FLOAT'
#                         elif dtype == 'timestamp':
#                             pg_type = 'TIMESTAMP'
#                         elif dtype == 'boolean':
#                             pg_type = 'BOOLEAN'
#                         else:
#                             pg_type = 'TEXT'
                        
#                         if col == 'Datetime' and processing_type == 'after':
#                             column_defs.append(f'"{col}" {pg_type} PRIMARY KEY')
#                         else:
#                             column_defs.append(f'"{col}" {pg_type}')
                    
#                     create_query = f"""
#                         CREATE TABLE meteo.{table_name} (
#                             {', '.join(column_defs)}
#                         )
#                     """
#                     cursor.execute(create_query)
#                     print(f"Table meteo.{table_name} créée avec succès")
                
#                 return True
#     except Exception as e:
#         print(f"Erreur lors de la création de la table pour {station}: {e}")
#         raise

# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before'):
#     """Sauvegarde un DataFrame dans la base de données"""
#     if df.empty:
#         print("Aucune donnée à sauvegarder")
#         return False
    
#     # Créer la table si elle n'existe pas (avec vérification renforcée)
#     try:
#         if not create_station_table(station, processing_type):
#             raise ValueError(f"Impossible de créer la table pour {station}")
#     except Exception as e:
#         print(f"Échec de la création de table: {e}")
#         raise
    
#     db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
#     table_name = station.replace(' ', '_').replace('-', '_').lower()
#     columns_config = get_station_columns(station, processing_type)
#     expected_columns = list(columns_config.keys())
    
#     # Préparation des données
#     try:
#         # Alignement des colonnes
#         missing_cols = set(expected_columns) - set(df.columns)
#         for col in missing_cols:
#             df[col] = None
        
#         df = df[expected_columns]
        
#         # Conversion des types
#         for col, dtype in columns_config.items():
#             if dtype == 'integer' and col in df.columns:
#                 df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
#             elif dtype == 'float' and col in df.columns:
#                 df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
        
#         # Préparation des tuples de données
#         data = [tuple(None if pd.isna(x) else x for x in row) 
#                for row in df.to_records(index=False)]
        
#         with get_connection(db_name) as conn:
#             with conn.cursor() as cursor:
#                 # Construction de la requête
#                 columns_sql = ', '.join([f'"{col}"' for col in expected_columns])
#                 placeholders = ', '.join(['%s'] * len(expected_columns))
#                 query = f"""
#                     INSERT INTO meteo.{table_name} ({columns_sql})
#                     VALUES ({placeholders})
#                     ON CONFLICT DO NOTHING
#                 """
                
#                 # Exécution
#                 execute_batch(cursor, query, data, page_size=100)
#                 conn.commit()
                
#                 print(f"{len(data)} lignes insérées avec succès dans meteo.{table_name}")
#                 return True
                
#     except Exception as e:
#         print(f"Erreur lors de la sauvegarde pour {station}")
#         print(f"Type d'erreur: {type(e).__name__}")
#         print(f"Message: {str(e)}")
#         print(f"Colonnes attendues: {expected_columns}")
#         print(f"Colonnes reçues: {list(df.columns)}")
#         print(f"Exemple de données: {data[0] if 'data' in locals() else 'N/A'}")
#         raise


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


# def create_station_table(station: str, processing_type: str = 'before'):
#     """Crée la table directement dans le schéma public"""
#     columns = get_station_columns(station, processing_type)
#     if not columns:
#         raise ValueError(f"Configuration des colonnes manquante pour {station}")
    
#     db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
#     table_name = station.replace(' ', '_').lower()
    
#     try:
#         with get_connection(db_name) as conn:
#             conn.autocommit = True
#             with conn.cursor() as cursor:
#                 column_defs = []
#                 for col, dtype in columns.items():
#                     pg_type = 'INTEGER' if dtype == 'integer' else \
#                              'FLOAT' if dtype == 'float' else \
#                              'TIMESTAMP' if dtype == 'timestamp' else \
#                              'BOOLEAN' if dtype == 'boolean' else 'TEXT'
#                     column_defs.append(f'"{col}" {pg_type}')
                
#                 cursor.execute(f"""
#                     CREATE TABLE IF NOT EXISTS {table_name} (
#                         {', '.join(column_defs)}
#                     )
#                 """)
#                 print(f"Table {table_name} créée/mise à jour")
#                 return True
                
#     except Exception as e:
#         print(f"Erreur création table {table_name}: {e}")
#         raise


# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before'):
#     """Version simplifiée sans référence au schéma"""
#     if df.empty:
#         raise ValueError("DataFrame vide reçu")
    
#     table_name = station.replace(' ', '_').lower()
#     columns = list(get_station_columns(station, processing_type).keys())
    
#     try:
#         # Créer la table si elle n'existe pas
#         create_station_table(station, processing_type)
        
#         # Préparer les données
#         data = [tuple(row[col] if pd.notna(row[col]) else None for col in columns) 
#                for _, row in df.iterrows()]
        
#         with get_connection('before_processing_db' if processing_type == 'before' else 'after_processing_db') as conn:
#             with conn.cursor() as cursor:
#                 # Requête d'insertion simplifiée
#                 placeholders = ', '.join(['%s'] * len(columns))
#                 cursor.executemany(
#                     f"INSERT INTO {table_name} VALUES ({placeholders}) ON CONFLICT DO NOTHING",
#                     data
#                 )
#                 conn.commit()
        
#         print(f"{len(data)} lignes insérées dans {table_name}")
#         return True
        
#     except Exception as e:
#         print(f"Erreur sauvegarde {table_name}: {e}")
#         raise

def get_pg_type(python_type):
    """Convertit les types Python en types PostgreSQL"""
    return {
        'integer': 'INTEGER',
        'float': 'FLOAT',
        'timestamp': 'TIMESTAMP',
        'boolean': 'BOOLEAN'
    }.get(python_type, 'TEXT')

# def create_station_table(station: str, processing_type: str = 'before'):
#     """Crée la table avec le nom exact de la station"""
#     columns = get_station_columns(station, processing_type)
#     if not columns:
#         raise ValueError(f"Configuration des colonnes manquante pour {station}")
    
#     db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
#     table_name = station.replace(' ', '_')  # Conserve la casse
    
#     try:
#         with get_connection(db_name) as conn:
#             conn.autocommit = True
#             with conn.cursor() as cursor:
#                 # Vérification d'existence
#                 cursor.execute("""
#                     SELECT 1 FROM information_schema.tables 
#                     WHERE table_name = %s AND table_schema = 'public'
#                 """, (table_name,))
                
#                 if not cursor.fetchone():
#                     # Construction des définitions de colonnes
#                     column_defs = []
#                     for col, dtype in columns.items():
#                         pg_type = get_pg_type(dtype)
#                         column_defs.append(f'"{col}" {pg_type}')
                    
#                     # Création de la table avec guillemets
#                     cursor.execute(f"""
#                         CREATE TABLE "{table_name}" (
#                             {', '.join(column_defs)}
#                         )
#                     """)
#                     print(f"Table '{table_name}' créée avec succès")
#                 return True
#     except Exception as e:
#         print(f"Erreur création table '{table_name}': {e}")
#         raise

# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before'):
#     """Sauvegarde les données dans la table de la station"""
#     if df.empty:
#         raise ValueError("DataFrame vide reçu")
    
#     table_name = station.replace(' ', '_')  # Format: "Bankandi"
#     columns = list(get_station_columns(station, processing_type).keys())
    
#     try:
#         # Crée la table si elle n'existe pas
#         create_station_table(station, processing_type)
        
#         # Prépare les données
#         data = [tuple(None if pd.isna(row[col]) else row[col] for col in columns) 
#                for _, row in df.iterrows()]
        
#         with get_connection('before_processing_db' if processing_type == 'before' else 'after_processing_db') as conn:
#             with conn.cursor() as cursor:
#                 # Requête avec guillemets pour la casse
#                 placeholders = ', '.join(['%s'] * len(columns))
#                 cursor.executemany(
#                     f"""INSERT INTO "{table_name}" VALUES ({placeholders}) 
#                     ON CONFLICT DO NOTHING""",
#                     data
#                 )
#                 conn.commit()
        
#         print(f"{len(data)} lignes insérées avec successs sauvegardées dans '{table_name}'")
#         return True
#     except Exception as e:
#         print(f"Erreur sauvegarde dans '{table_name}': {e}")
#         raise



# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before'):
#     """Sauvegarde dans la table avec le nom exact de la station"""
#     if df.empty:
#         raise ValueError("DataFrame vide reçu")
    
#     table_name = station  # Nom exact conservé
#     columns = list(get_station_columns(station, processing_type).keys())
    
#     try:
#         create_station_table(station, processing_type)
        
#         # Préparation des données avec gestion des NaN
#         data = [tuple(None if pd.isna(row[col]) else row[col] for col in columns) 
#                for _, row in df.iterrows()]
        
#         with get_connection('before_processing_db' if processing_type == 'before' else 'after_processing_db') as conn:
#             with conn.cursor() as cursor:
#                 # Requête SQL avec guillemets pour le nom de table et colonnes
#                 cols = ', '.join([f'"{c}"' for c in columns])
#                 placeholders = ', '.join(['%s'] * len(columns))
                
#                 cursor.executemany(
#                     f"""INSERT INTO "{table_name}" ({cols}) 
#                     VALUES ({placeholders}) 
#                     ON CONFLICT DO NOTHING""",
#                     data
#                 )
#                 conn.commit()
        
#         print(f"{len(data)} lignes insérées dans '{table_name}'")
#         return True
#     except Exception as e:
#         print(f"Erreur sauvegarde dans '{table_name}': {e}")
#         raise

# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before'):
#     """Sauvegarde dans la table avec le nom exact de la station"""
#     if df.empty:
#         raise ValueError("DataFrame vide reçu")
    
#     table_name = station.strip()  # Nettoyage des espaces
#     columns_config = get_station_columns(station, processing_type)
    
#     if not columns_config:
#         raise ValueError(f"Configuration des colonnes manquante pour {station}")
    
#     # Liste des colonnes attendues
#     expected_columns = list(columns_config.keys())
    
#     try:
#         # Vérification et alignement des colonnes
#         missing_cols = set(expected_columns) - set(df.columns)
#         extra_cols = set(df.columns) - set(expected_columns)
        
#         if missing_cols:
#             print(f"Colonnes manquantes dans DataFrame pour {station}: {missing_cols}")
#             for col in missing_cols:
#                 df[col] = None  # Ajout des colonnes manquantes avec valeurs NULL
        
#         if extra_cols:
#             print(f"Colonnes en trop dans DataFrame pour {station}: {extra_cols}")
#             df = df.drop(columns=list(extra_cols))  # Suppression des colonnes non attendues
        
#         # Réordonnancement des colonnes selon la configuration
#         df = df[expected_columns]
#         df = df.reset_index(drop=True)  # Supprime l'index et évite qu'il soit considéré comme une colonne
#         # Conversion des types si nécessaire
#         if "Date" in df.columns:
#             df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        
#         # Création de la table si elle n'existe pas
#         create_station_table(station, processing_type)
        
#         # Préparation des données avec vérification
#         data = []
#         for _, row in df.iterrows():
#             try:
#                 data_tuple = tuple(None if pd.isna(row[col]) else row[col] for col in expected_columns)
#                 data.append(data_tuple)
#             except KeyError as e:
#                 print(f"Erreur: Colonne {e} manquante dans la ligne {_}")
#                 raise
        
#         with get_connection('before_processing_db' if processing_type == 'before' else 'after_processing_db') as conn:
#             with conn.cursor() as cursor:
#                 # Construction dynamique de la requête
#                 cols = ', '.join([f'"{c}"' for c in expected_columns])
#                 placeholders = ', '.join(['%s'] * len(expected_columns))
                
#                 cursor.executemany(
#                     f"""INSERT INTO "{table_name}" ({cols}) 
#                     VALUES ({placeholders}) 
#                     ON CONFLICT DO NOTHING""",
#                     data
#                 )
#                 conn.commit()
        
#         print(f"{len(data)} lignes insérées avec succès dans '{table_name}'")
#         return True
        
#     except Exception as e:
#         print(f"Erreur détaillée lors de la sauvegarde dans '{table_name}': {str(e)}")
#         print(f"Colonnes attendues: {expected_columns}")
#         print(f"Colonnes reçues: {list(df.columns)}")
#         print("5 premières lignes du DataFrame:")
#         print(df.head())
#         raise


################ code semblant bon ##############
# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before') -> bool:
#     """Sauvegarde dans la table portant exactement le nom de la station"""
#     try:
#         # Validation initiale
#         if df.empty:
#             raise ValueError("DataFrame vide reçu")
        
#         # Nettoyage strict du nom de la station/table
#         table_name = station.strip()
#         print(f"\n=== Début sauvegarde dans table '{table_name}' ===")

#         # Récupération configuration colonnes
#         columns_config = get_station_columns(station, processing_type)
#         if not columns_config:
#             raise ValueError(f"Configuration des colonnes manquante pour {table_name}")
        
#         expected_columns = [col.strip() for col in columns_config.keys()]
#         print(f"Colonnes attendues: {expected_columns}")

#         # Nettoyage des noms de colonnes dans le DataFrame
#         df.columns = [col.strip() for col in df.columns]
#         print(f"Colonnes reçues (après nettoyage): {list(df.columns)}")

#         # Vérification approfondie de l'alignement
#         mismatch = False
#         for i, (expected, actual) in enumerate(zip(expected_columns, df.columns)):
#             if expected != actual:
#                 print(f"INCOHÉRENCE colonne {i}: attendu '{expected}' ≠ reçu '{actual}'")
#                 mismatch = True
        
#         if mismatch:
#             raise ValueError("Incohérence dans les noms de colonnes")

#         # Réindexation stricte
#         df = df.reindex(columns=expected_columns)
        
#         # Conversion des types et gestion des NaN
#         df = df.where(pd.notnull(df), None)
#         if "Date" in df.columns:
#             df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d %H:%M:%S')

#         # Vérification finale des données
#         print("\nVérification des données avant insertion:")
#         print(f"Nombre de lignes: {len(df)}")
#         print("Exemple première ligne:", df.iloc[0].to_dict())

#         # Connexion à la base de données
#         with get_connection('before_processing_db' if processing_type == 'before' else 'after_processing_db') as conn:
#             with conn.cursor() as cursor:
#                 # VÉRIFICATION CRITIQUE: comptage des colonnes
#                 cursor.execute(f"SELECT * FROM \"{table_name}\" LIMIT 0")
#                 db_columns = [desc[0] for desc in cursor.description]
#                 print(f"Colonnes dans la table SQL: {db_columns}")

#                 if len(db_columns) != len(expected_columns):
#                     raise ValueError(f"Mauvais nombre de colonnes (SQL: {len(db_columns)} vs attendu: {len(expected_columns)})")

#                 # Préparation données
#                 data = [tuple(x for x in record) for record in df.to_records(index=False)]
                
#                 # Construction requête
#                 cols = ', '.join([f'"{c}"' for c in expected_columns])
#                 placeholders = ', '.join(['%s'] * len(expected_columns))
#                 query = f'INSERT INTO \"{table_name}\" ({cols}) VALUES ({placeholders})'

#                 print(f"\nRequête SQL générée:\n{query}")
#                 print("Premier tuple de données:", data[0])

#                 # Exécution avec gestion d'erreur détaillée
#                 try:
#                     cursor.executemany(query, data)
#                     conn.commit()
#                     print(f"Succès: {len(data)} lignes insérées")
#                     return True
#                 except Exception as e:
#                     conn.rollback()
#                     print(f"Échec lors de l'insertion: {str(e)}")
#                     if len(data) > 0:
#                         print("Premier tuple problématique:", data[0])
#                         print("Types dans le tuple:", [type(x) for x in data[0]])
#                     raise

#     except Exception as e:
#         print(f"\nERREUR FATALE: {str(e)}")
#         if 'df' in locals():
#             print("\nDernier état du DataFrame:")
#             print("Colonnes:", list(df.columns))
#             print("Types:", df.dtypes)
#             print("Premières lignes:\n", df.head(2))
#         raise
###################################

import numpy as np
import pandas as pd

# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before') -> bool:
#     """Sauvegarde dans la table portant exactement le nom de la station"""
#     try:
#         if df.empty:
#             raise ValueError("DataFrame vide reçu")
        
#         # Nettoyage strict du nom de la station/table
#         table_name = station.strip()
#         print(f"\n=== Début sauvegarde dans table '{table_name}' ===")

#         # Récupération configuration colonnes
#         columns_config = get_station_columns(station, processing_type)
#         if not columns_config:
#             raise ValueError(f"Configuration des colonnes manquante pour {table_name}")
        
#         expected_columns = [col.strip() for col in columns_config.keys()]
#         print(f"Colonnes attendues: {expected_columns}")

#         # Nettoyage et alignement des colonnes
#         df.columns = [col.strip() for col in df.columns]
#         df = df.reindex(columns=expected_columns)
        
#         # Conversion des types et gestion des NaN
#         df = df.replace({np.nan: None})
#         if "Date" in df.columns:
#             df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

#         # Connexion à la base de données
#         with get_connection('before_processing_db' if processing_type == 'before' else 'after_processing_db') as conn:
#             with conn.cursor() as cursor:
#                 # Vérification existence table + création si nécessaire
#                 cursor.execute("""
#                     SELECT column_name 
#                     FROM information_schema.columns 
#                     WHERE table_name = %s
#                     ORDER BY ordinal_position
#                 """, (table_name.lower(),))
                
#                 db_columns = [row[0] for row in cursor.fetchall()]
                
#                 if not db_columns:
#                     print(f"Création de la table '{table_name}'...")
#                     # Construction de la requête de création sans la colonne id
#                     columns_def = ",\n".join(
#                         [f'"{col}" {dtype}' for col, dtype in columns_config.items()]
#                     )
#                     create_query = f"""
#                     CREATE TABLE "{table_name}" (
#                         {columns_def}
#                     )
#                     """
#                     cursor.execute(create_query)
#                     conn.commit()
#                     print(f"Table '{table_name}' créée avec succès")
#                     db_columns = list(columns_config.keys())
                
#                 # Vérification colonnes (en ignorant 'id' si présent)
#                 db_data_columns = [col for col in db_columns if col != 'id']
#                 if db_data_columns != expected_columns:
#                     raise ValueError(
#                         f"Incompatibilité colonnes:\n"
#                         f"Attendu: {expected_columns}\n"
#                         f"Trouvé: {db_data_columns}"
#                     )

#                 # Préparation données (exclure 'id' si présent)
#                 data_columns = [col for col in expected_columns if col in db_columns or col != 'id']
#                 data = [tuple(row[col] for col in data_columns) for _, row in df.iterrows()]
                
#                 # Construction requête (sans 'id')
#                 cols = ', '.join([f'"{col}"' for col in data_columns])
#                 placeholders = ', '.join(['%s'] * len(data_columns))
#                 query = f'INSERT INTO "{table_name}" ({cols}) VALUES ({placeholders})'

#                 print(f"\nRequête SQL:\n{query}")
#                 print("Exemple données:", data[0] if data else "Aucune")

#                 # Exécution
#                 try:
#                     cursor.executemany(query, data)
#                     conn.commit()
#                     print(f"Succès: {len(data)} lignes insérées")
#                     return True
#                 except Exception as e:
#                     conn.rollback()
#                     print(f"Échec insertion: {str(e)}")
#                     if data:
#                         print("Exemple tuple:", data[0])
#                     raise

#     except Exception as e:
#         print(f"\nERREUR: {str(e)}")
#         if 'df' in locals():
#             print("\nÉtat final DataFrame:")
#             print("Colonnes:", list(df.columns))
#             print("Types:", df.dtypes)
#             print("5 premières lignes:\n", df.head().to_string())
#         raise


# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before') -> bool:
#     """Sauvegarde dans la table portant exactement le nom de la station"""
#     try:
#         if df.empty:
#             raise ValueError("DataFrame vide reçu")
        
#         # Nettoyage strict du nom de la station/table
#         table_name = station.strip()
#         print(f"\n=== Début sauvegarde dans table '{table_name}' ===")

#         # Récupération configuration colonnes
#         columns_config = get_station_columns(station, processing_type)
#         if not columns_config:
#             raise ValueError(f"Configuration des colonnes manquante pour {table_name}")
        
#         expected_columns = [col.strip() for col in columns_config.keys()]
#         print(f"Colonnes attendues: {expected_columns}")

#         # Nettoyage et alignement des colonnes
#         df.columns = [col.strip() for col in df.columns]
#         df = df.reindex(columns=expected_columns)
        
#         # Conversion des types et gestion des NaN
#         df = df.replace({np.nan: None})
#         if "Date" in df.columns:
#             df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d %H:%M:%S')

#         # Connexion à la base de données
#         with get_connection('before_processing_db' if processing_type == 'before' else 'after_processing_db') as conn:
#             with conn.cursor() as cursor:
#                 # Vérification existence table + création si nécessaire
#                 cursor.execute("""
#                     SELECT column_name 
#                     FROM information_schema.columns 
#                     WHERE table_name = %s
#                     ORDER BY ordinal_position
#                 """, (table_name.lower(),))
                
#                 db_columns = [row[0] for row in cursor.fetchall()]
                
#                 if not db_columns:
#                     print(f"Création de la table '{table_name}'...")
#                     # Construction de la requête de création sans la colonne id
#                     columns_def = ",\n".join(
#                         [f'"{col}" {dtype}' for col, dtype in columns_config.items()]
#                     )
#                     create_query = f"""
#                     CREATE TABLE "{table_name}" (
#                         {columns_def}
#                     )
#                     """
#                     cursor.execute(create_query)
#                     conn.commit()
#                     print(f"Table '{table_name}' créée avec succès")
#                     db_columns = list(columns_config.keys())
                
#                 # Vérification colonnes (en ignorant 'id' si présent)
#                 db_data_columns = [col for col in db_columns if col != 'id']
#                 if db_data_columns != expected_columns:
#                     raise ValueError(
#                         f"Incompatibilité colonnes:\n"
#                         f"Attendu: {expected_columns}\n"
#                         f"Trouvé: {db_data_columns}"
#                     )

#                 # Préparation données (exclure 'id' si présent)
#                 data_columns = [col for col in expected_columns if col in db_columns or col != 'id']
#                 data = [tuple(row[col] for col in data_columns) for _, row in df.iterrows()]
                
#                 # Construction requête (sans 'id')
#                 cols = ', '.join([f'"{col}"' for col in data_columns])
#                 placeholders = ', '.join(['%s'] * len(data_columns))
#                 query = f'INSERT INTO "{table_name}" ({cols}) VALUES ({placeholders})'

#                 print(f"\nRequête SQL:\n{query}")
#                 print("Exemple données:", data[0] if data else "Aucune")

#                 # Exécution
#                 try:
#                     cursor.executemany(query, data)
#                     conn.commit()
#                     print(f"Succès: {len(data)} lignes insérées")
#                     return True
#                 except Exception as e:
#                     conn.rollback()
#                     print(f"Échec insertion: {str(e)}")
#                     if data:
#                         print("Exemple tuple:", data[0])
#                     raise

#     except Exception as e:
#         print(f"\nERREUR: {str(e)}")
#         if 'df' in locals():
#             print("\nÉtat final DataFrame:")
#             print("Colonnes:", list(df.columns))
#             print("Types:", df.dtypes)
#             print("5 premières lignes:\n", df.head().to_string())
#         raise


######################  21-07-2025 ######################

# def create_station_table(station: str, processing_type: str = 'before'):
#     """Crée la table avec le nom exact de la station (espaces, accents conservés)"""
#     columns = get_station_columns(station, processing_type)
#     if not columns:
#         raise ValueError(f"Configuration des colonnes manquante pour {station}")
    
#     db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
#     table_name = station  # On conserve le nom exact tel quel

#     try:
#         with get_connection(db_name) as conn:
#             conn.autocommit = True
#             with conn.cursor() as cursor:
#                 # Vérification d'existence avec guillemets
#                 cursor.execute("""
#                     SELECT 1 FROM information_schema.tables 
#                     WHERE table_name = %s AND table_schema = 'public'
#                 """, (table_name,))
                
#                 if not cursor.fetchone():
#                     # Construction des définitions de colonnes
#                     column_defs = []
#                     for col, dtype in columns.items():
#                         pg_type = {
#                             'integer': 'INTEGER',
#                             'float': 'FLOAT',
#                             'timestamp': 'TIMESTAMP',
#                             'boolean': 'BOOLEAN',
#                             'text': 'TEXT'
#                         }.get(dtype, 'TEXT')
#                         column_defs.append(f'"{col}" {pg_type}')  # Guillemets pour les colonnes aussi
                    
#                     # Création de la table avec guillemets doubles pour le nom
#                     cursor.execute(f"""
#                         CREATE TABLE "{table_name}" (
#                             id SERIAL PRIMARY KEY,
#                             {', '.join(column_defs)}
#                         )
#                     """)
#                     print(f"Table '{table_name}' créée avec succès")
#                 return True
#     except Exception as e:
#         print(f"Erreur création table '{table_name}': {e}")
#         raise




# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before'):
#     """Sauvegarde dans la table avec le nom exact de la station"""
#     if df.empty:
#         raise ValueError("DataFrame vide reçu")
    
#     table_name = station  # Nom exact conservé
#     columns = list(get_station_columns(station, processing_type).keys())
    
#     try:
#         create_station_table(station, processing_type)
        
#         # Préparation des données avec gestion des NaN
#         data = [tuple(None if pd.isna(row[col]) else row[col] for col in columns) 
#                for _, row in df.iterrows()]
        
#         with get_connection('before_processing_db' if processing_type == 'before' else 'after_processing_db') as conn:
#             with conn.cursor() as cursor:
#                 # Requête SQL avec guillemets pour le nom de table et colonnes
#                 cols = ', '.join([f'"{c}"' for c in columns])
#                 placeholders = ', '.join(['%s'] * len(columns))
                
#                 cursor.executemany(
#                     f"""INSERT INTO "{table_name}" ({cols}) 
#                     VALUES ({placeholders}) 
#                     ON CONFLICT DO NOTHING""",
#                     data
#                 )
#                 conn.commit()
        
#         print(f"{len(data)} lignes insérées dans '{table_name}'")
#         return True
#     except Exception as e:
#         print(f"Erreur sauvegarde dans '{table_name}': {e}")
#         raise

#########################



def create_station_table(station: str, processing_type: str = 'before'):
    """Crée la table avec le nom exact de la station"""
    columns_config = get_station_columns(station, processing_type)
    if not columns_config:
        raise ValueError(f"Configuration des colonnes manquante pour {station}")
    
    db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
    table_name = station

    try:
        with get_connection(db_name) as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                # Vérification existence table
                cursor.execute("""
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = %s AND table_schema = 'public'
                """, (table_name,))
                
                if not cursor.fetchone():
                    # Construction des définitions de colonnes
                    column_defs = []
                    for col, dtype in columns_config.items():
                        pg_type = {
                            'integer': 'INTEGER',
                            'float': 'FLOAT',
                            'timestamp': 'TIMESTAMP',
                            'timestamp PRIMARY KEY': 'TIMESTAMP PRIMARY KEY',
                            'date': 'DATE',
                            'boolean': 'BOOLEAN',
                            'text': 'TEXT'
                        }.get(dtype, 'TEXT')
                        column_defs.append(f'"{col}" {pg_type}')
                    
                    # Création de la table
                    cursor.execute(f"""
                        CREATE TABLE "{table_name}" (
                            {', '.join(column_defs)}
                        )
                    """)
                    print(f"Table '{table_name}' créée avec succès")
                return True
    except Exception as e:
        print(f"Erreur création table '{table_name}': {e}")
        raise



# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before') -> bool:
#     """Sauvegarde dans la table avec vérifications complètes"""
#     try:
#         if df.empty:
#             raise ValueError("DataFrame vide reçu")
        
#         table_name = station
#         print(f"\n=== Traitement table '{table_name}' ===")

#         # 1. Vérification configuration colonnes
#         columns_config = get_station_columns(station, processing_type)
#         if not columns_config:
#             raise ValueError(f"Configuration des colonnes manquante pour {table_name}")
        
#         # On utilise directement les colonnes de la configuration (qui inclut déjà Datetime)
#         expected_columns = list(columns_config.keys())
#         print("1. Configuration colonnes OK")

#         # 2. Vérification colonnes DataFrame
#         df_columns = list(df.columns)
#         if set(df_columns) != set(expected_columns):
#             raise ValueError(
#                 f"Colonnes incorrectes:\n"
#                 f"Attendu: {expected_columns}\n"
#                 f"Reçu: {df_columns}"
#             )
#         print("2. Colonnes DataFrame OK")

#         # 3. Vérification structure table en base
#         with get_connection('before_processing_db' if processing_type == 'before' else 'after_processing_db') as conn:
#             with conn.cursor() as cursor:
#                 # Récupération structure table
#                 cursor.execute(f"""
#                     SELECT column_name, data_type 
#                     FROM information_schema.columns 
#                     WHERE table_name = %s
#                     ORDER BY ordinal_position
#                 """, (table_name,))
                
#                 db_columns = [row[0] for row in cursor.fetchall()]
#                 db_types = [row[1] for row in cursor.fetchall()]

#                 # Vérification colonnes
#                 if set(db_columns) != set(expected_columns):
#                     raise ValueError(
#                         f"Colonnes base incompatibles:\n"
#                         f"Attendu: {expected_columns}\n"
#                         f"Base: {db_columns}"
#                     )
#         print("3. Structure table OK")

#         # 4. Préparation données
#         df = df.replace({np.nan: None})
        
#         # Conversion explicite des types
#         for col, dtype in columns_config.items():
#             if dtype == 'timestamp PRIMARY KEY' or dtype == 'timestamp':
#                 df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
#             elif dtype == 'float':
#                 df[col] = df[col].astype(float)
#             elif dtype == 'integer':
#                 df[col] = df[col].astype(int)
#             elif dtype == 'date':
#                 df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')

#         # 5. Insertion avec gestion des conflits
#         with get_connection('before_processing_db' if processing_type == 'before' else 'after_processing_db') as conn:
#             with conn.cursor() as cursor:
#                 # Construction requête
#                 cols = ', '.join([f'"{col}"' for col in df.columns])
#                 placeholders = ', '.join(['%s'] * len(df.columns))
                
#                 # Requête avec gestion des conflits
#                 set_clause = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col != 'Datetime'])
                
#                 query = f"""
#                 INSERT INTO "{table_name}" ({cols}) 
#                 VALUES ({placeholders})
#                 ON CONFLICT ("Datetime") DO UPDATE SET
#                     {set_clause}
#                 """
                
#                 # Insertion complète
#                 data = [tuple(row) for _, row in df.iterrows()]
#                 cursor.executemany(query, data)
#                 conn.commit()
                
#                 print(f"\nSuccès: {len(data)} lignes insérées/mises à jour")
#                 return True

#     except Exception as e:
#         print(f"\nERREUR: {str(e)}")
#         if 'df' in locals():
#             print("\nDétails DataFrame:")
#             print("Colonnes:", list(df.columns))
#             print("Types:", df.dtypes)
#             print("5 premières lignes:\n", df.head().to_string())
#         raise










def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before') -> bool:
    """Sauvegarde dans la table avec vérifications complètes et gestion d'erreurs améliorée"""
    try:
        # Vérification DataFrame
        if df.empty:
            raise ValueError("DataFrame vide reçu")
        
        table_name = station
        print(f"\n=== Traitement table '{table_name}' ===")

        # 1. Vérification configuration colonnes
        columns_config = get_station_columns(station, processing_type)
        if not columns_config:
            raise ValueError(f"Configuration des colonnes manquante pour {table_name}")
        
        expected_columns = list(columns_config.keys())
        print("1. Configuration colonnes OK - Colonnes attendues:", expected_columns)

        # 2. Vérification colonnes DataFrame
        df_columns = list(df.columns)
        if set(df_columns) != set(expected_columns):
            missing = set(expected_columns) - set(df_columns)
            extra = set(df_columns) - set(expected_columns)
            raise ValueError(
                f"Colonnes incompatibles:\n"
                f"Manquantes: {list(missing)}\n"
                f"Supplémentaires: {list(extra)}"
            )
        print("2. Colonnes DataFrame OK")

        # 3. Vérification/création table
        db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
        with get_connection(db_name) as conn:
            with conn.cursor() as cursor:
                # Vérification existence table
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                """, (table_name,))
                
                db_columns = [row[0] for row in cursor.fetchall()]
                
                if not db_columns:
                    print("Table non trouvée, création...")
                    create_station_table(station, processing_type)
                    db_columns = list(columns_config.keys())
                
                print("Colonnes en base:", db_columns)
                
                if set(db_columns) != set(expected_columns):
                    raise ValueError(
                        f"Structure table incompatible:\n"
                        f"Attendu: {expected_columns}\n"
                        f"Base: {db_columns}"
                    )
        print("3. Structure table OK")

        # 4. Préparation données avec gestion d'erreurs
        df = df.replace({np.nan: None})
        
        # Conversion des types avec vérification
        conversion_errors = []
        for col, dtype in columns_config.items():
            try:
                if dtype in ['timestamp PRIMARY KEY', 'timestamp']:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    # Formatage pour PostgreSQL
                    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                elif dtype == 'date':
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    df[col] = df[col].dt.strftime('%Y-%m-%d')
                elif dtype == 'float':
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
                elif dtype == 'integer':
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            except Exception as e:
                conversion_errors.append(f"{col}: {str(e)}")
        
        if conversion_errors:
            raise ValueError(f"Erreurs de conversion:\n" + "\n".join(conversion_errors))

        # Vérification valeurs nulles après conversion
        null_counts = df.isnull().sum()
        if null_counts.any():
            print("Avertissement: Valeurs nulles après conversion:\n", null_counts)

        # 5. Insertion avec debug complet
        with get_connection(db_name) as conn:
            with conn.cursor() as cursor:
                # Construction requête
                cols = ', '.join([f'"{col}"' for col in df.columns])
                placeholders = ', '.join(['%s'] * len(df.columns))
                
                set_clause = ', '.join(
                    [f'"{col}" = EXCLUDED."{col}"' 
                     for col in df.columns 
                     if col != 'Datetime']
                )
                
                query = f"""
                INSERT INTO "{table_name}" ({cols}) 
                VALUES ({placeholders})
                ON CONFLICT ("Datetime") DO UPDATE SET
                    {set_clause}
                """
                
                # Debug avant insertion
                print("\nDebug avant insertion:")
                print("Requête:", query)
                print("Colonnes:", df.columns.tolist())
                print("Types:", df.dtypes)
                sample_data = df.iloc[0]
                print("Exemple données:", sample_data.to_dict())
                
                
                # Insertion complète par lots
                batch_size = 1000
                total_rows = len(df)
                inserted_rows = 0
                
                for i in range(0, total_rows, batch_size):
                    batch = df.iloc[i:i+batch_size]
                    data = [tuple(x) for x in batch.to_numpy()]
                    
                    try:
                        cursor.executemany(query, data)
                        conn.commit()
                        inserted_rows += len(batch)
                        print(f"Lot {i//batch_size + 1} inséré ({len(batch)} lignes)")
                    except Exception as batch_e:
                        conn.rollback()
                        raise ValueError(
                            f"Échec insertion lot {i//batch_size + 1}:\n"
                            f"Erreur: {str(batch_e)}\n"
                            f"Première ligne du lot: {batch.iloc[0].to_dict()}"
                        )
                
                print(f"\nSuccès: {inserted_rows}/{total_rows} lignes insérées")
                return True

    except Exception as e:
        print(f"\nERREUR: {str(e)}")
        if 'df' in locals():
            print("\nDétails DataFrame:")
            print("Colonnes:", list(df.columns))
            print("Types:", df.dtypes.to_dict())
            print("5 premières lignes:\n", df.head().to_string())
        raise


# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before') -> bool:
#     """Sauvegarde dans la table avec gestion robuste des erreurs"""
#     try:
#         # Vérification DataFrame
#         if df.empty:
#             raise ValueError("DataFrame vide reçu")
        
#         table_name = station
#         print(f"\n=== Traitement table '{table_name}' ===")

#         # 1. Vérification configuration colonnes
#         columns_config = get_station_columns(station, processing_type)
#         if not columns_config:
#             raise ValueError(f"Configuration des colonnes manquante pour {table_name}")
        
#         expected_columns = list(columns_config.keys())
#         print("1. Configuration colonnes OK - Colonnes attendues:", expected_columns)

#         # 2. Vérification colonnes DataFrame
#         df_columns = list(df.columns)
#         if set(df_columns) != set(expected_columns):
#             missing = set(expected_columns) - set(df_columns)
#             extra = set(df_columns) - set(expected_columns)
#             raise ValueError(
#                 f"Colonnes incompatibles:\n"
#                 f"Manquantes: {list(missing)}\n"
#                 f"Supplémentaires: {list(extra)}"
#             )
#         print("2. Colonnes DataFrame OK")

#         # 3. Vérification/création table
#         db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
#         with get_connection(db_name) as conn:
#             with conn.cursor() as cursor:
#                 # Vérification existence table
#                 cursor.execute("""
#                     SELECT column_name 
#                     FROM information_schema.columns 
#                     WHERE table_name = %s
#                     ORDER BY ordinal_position
#                 """, (table_name,))
                
#                 db_columns = [row[0] for row in cursor.fetchall()]
                
#                 if not db_columns:
#                     print("Table non trouvée, création...")
#                     create_station_table(station, processing_type)
#                     db_columns = list(columns_config.keys())
                
#                 print("Colonnes en base:", db_columns)
                
#                 if set(db_columns) != set(expected_columns):
#                     raise ValueError(
#                         f"Structure table incompatible:\n"
#                         f"Attendu: {expected_columns}\n"
#                         f"Base: {db_columns}"
#                     )
#         print("3. Structure table OK")

#         # 4. Préparation données
#         df = df.replace({np.nan: None})
        
#         # Conversion des types
#         for col, dtype in columns_config.items():
#             if dtype in ['timestamp PRIMARY KEY', 'timestamp']:
#                 df[col] = pd.to_datetime(df[col], errors='coerce')
#                 df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
#             elif dtype == 'date':
#                 df[col] = pd.to_datetime(df[col], errors='coerce')
#                 df[col] = df[col].dt.strftime('%Y-%m-%d')
#             elif dtype == 'float':
#                 df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
#             elif dtype == 'integer':
#                 df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

#         # 5. Insertion ligne par ligne
#         with get_connection(db_name) as conn:
#             with conn.cursor() as cursor:
#                 # Construction requête
#                 cols = ', '.join([f'"{col}"' for col in df.columns])
#                 placeholders = ', '.join(['%s'] * len(df.columns))
                
#                 # Construction de la clause SET (sans Datetime)
#                 set_columns = [col for col in df.columns if col != 'Datetime']
#                 set_clause = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in set_columns])
                
#                 query = f"""
#                 INSERT INTO "{table_name}" ({cols}) 
#                 VALUES ({placeholders})
#                 ON CONFLICT ("Datetime") DO UPDATE SET
#                     {set_clause}
#                 """
                
#                 # Debug
#                 print("\nDebug avant insertion:")
#                 print(f"Requête: {query}")
#                 print(f"Nombre colonnes: {len(df.columns)}")
#                 print(f"Exemple donnée: {tuple(df.iloc[0])}")

#                 # Insertion ligne par ligne
#                 total_rows = len(df)
#                 success_rows = 0
                
#                 for index, row in df.iterrows():
#                     try:
#                         data = tuple(row[col] for col in df.columns)
#                         cursor.execute(query, data)
#                         success_rows += 1
#                     except Exception as e:
#                         conn.rollback()
#                         raise ValueError(
#                             f"Erreur ligne {index}:\n"
#                             f"Données: {dict(row)}\n"
#                             f"Erreur: {str(e)}"
#                         )
                
#                 conn.commit()
#                 print(f"\nSuccès: {success_rows}/{total_rows} lignes insérées/mises à jour")
#                 return success_rows == total_rows

#     except Exception as e:
#         print(f"\nERREUR: {str(e)}")
#         if 'df' in locals():
#             print("\nDétails DataFrame:")
#             print("Colonnes:", list(df.columns))
#             print("Types:", df.dtypes.to_dict())
#             print("5 premières lignes:\n", df.head().to_string())
#         raise





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

# def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before') -> bool:
#     """Sauvegarde dans la table avec vérification stricte des colonnes et gestion des conflits"""
#     try:
#         if df.empty:
#             raise ValueError("DataFrame vide reçu")
        
#         table_name = station
#         columns_config = get_station_columns(station, processing_type)
#         if not columns_config:
#             raise ValueError(f"Configuration des colonnes manquante pour {table_name}")
#         expected_columns = list(columns_config.keys())
#         pk_col = 'Datetime' if 'Datetime' in expected_columns else None

#         # Nettoyage et alignement des colonnes
#         df.columns = [col.strip() for col in df.columns]
#         missing_cols = set(expected_columns) - set(df.columns)
#         for col in missing_cols:
#             df[col] = None
#         extra_cols = set(df.columns) - set(expected_columns)
#         if extra_cols:
#             df = df.drop(columns=list(extra_cols))
#         df = df[expected_columns]
#         df = df.replace({np.nan: None})

#         # Conversion des types
#         for col, dtype in columns_config.items():
#             try:
#                 if dtype in ['timestamp PRIMARY KEY', 'timestamp']:
#                     df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
#                 elif dtype == 'date':
#                     df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
#                 elif dtype == 'float':
#                     df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
#                 elif dtype == 'integer':
#                     df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
#             except Exception as e:
#                 print(f"Erreur conversion colonne {col}: {e}")

#         # Création de la table si besoin
#         create_station_table(station, processing_type)

#         db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
#         with get_connection(db_name) as conn:
#             with conn.cursor() as cursor:
#                 # Vérification structure table
#                 cursor.execute("""
#                     SELECT column_name 
#                     FROM information_schema.columns 
#                     WHERE table_name = %s
#                     ORDER BY ordinal_position
#                 """, (table_name,))
#                 db_columns = [row[0] for row in cursor.fetchall()]
#                 if set(db_columns) != set(expected_columns):
#                     raise ValueError(
#                         f"Structure table incompatible:\n"
#                         f"Attendu: {expected_columns}\n"
#                         f"Base: {db_columns}"
#                     )

#                 # Préparation des données
#                 data = [tuple(row[col] for col in expected_columns) for _, row in df.iterrows()]
#                 cols = ', '.join([f'"{col}"' for col in expected_columns])
#                 placeholders = ', '.join(['%s'] * len(expected_columns))
#                 # Gestion des conflits sur Datetime si présent
#                 if pk_col:
#                     set_clause = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in expected_columns if col != pk_col])
#                     query = f"""
#                         INSERT INTO "{table_name}" ({cols}) 
#                         VALUES ({placeholders})
#                         ON CONFLICT ("{pk_col}") DO UPDATE SET
#                             {set_clause}
#                     """
#                 else:
#                     query = f"""
#                         INSERT INTO "{table_name}" ({cols}) 
#                         VALUES ({placeholders})
#                         ON CONFLICT DO NOTHING
#                     """

#                 print("\nDebug avant insertion:")
#                 print("Requête:", query)
#                 print("Colonnes:", expected_columns)
#                 print("Premier tuple:", data[0] if data else "Aucune donnée")

#                 # Insertion par lots
#                 batch_size = 1000
#                 total_rows = len(data)
#                 inserted_rows = 0
#                 for i in range(0, total_rows, batch_size):
#                     batch = data[i:i+batch_size]
#                     try:
#                         cursor.executemany(query, batch)
#                         conn.commit()
#                         inserted_rows += len(batch)
#                         print(f"Lot {i//batch_size + 1} inséré ({len(batch)} lignes)")
#                     except Exception as batch_e:
#                         conn.rollback()
#                         print(f"Erreur lot {i//batch_size + 1}: {batch_e}")
#                         print("Premier tuple du lot:", batch[0])
#                         raise

#                 print(f"\nSuccès: {inserted_rows}/{total_rows} lignes insérées")
#                 return True

#     except Exception as e:
#         print(f"\nERREUR: {str(e)}")
#         if 'df' in locals():
#             print("\nDétails DataFrame:")
#             print("Colonnes:", list(df.columns))
#             print("Types:", df.dtypes.to_dict())
#             print("5 premières lignes:\n", df.head().to_string())
#         raise

def save_to_database(df: pd.DataFrame, station: str, processing_type: str = 'before') -> bool:
    """Sauvegarde dans la table avec gestion automatique de la structure"""
    try:
        # Vérification DataFrame
        if df.empty:
            raise ValueError("DataFrame vide reçu")
        
        table_name = station.strip()
        print(f"\n=== Traitement table '{table_name}' ===")

        # 1. Récupération configuration colonnes
        columns_config = get_station_columns(station, processing_type)
        if not columns_config:
            raise ValueError(f"Configuration des colonnes manquante pour {table_name}")
        
        expected_columns = list(columns_config.keys())
        print("Colonnes attendues:", expected_columns)

        # 2. Alignement des colonnes du DataFrame
        df = df[expected_columns].copy()
        df = df.replace({np.nan: None})

        # 3. Conversion des types
        for col, dtype in columns_config.items():
            pg_type = dtype.split()[0]  # Enlève 'PRIMARY KEY' si présent
            
            try:
                if pg_type == 'timestamp':
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                elif pg_type == 'date':
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                elif pg_type == 'float':
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
                elif pg_type == 'integer':
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            except Exception as e:
                print(f"Erreur conversion {col} en {pg_type}: {str(e)}")
                raise

        # 4. Gestion de la table (création ou mise à jour)
        db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
        with get_connection(db_name) as conn:
            with conn.cursor() as cursor:
                # Vérification existence table
                cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)", (table_name,))
                table_exists = cursor.fetchone()[0]

                if table_exists:
                    # Vérification structure existante
                    cursor.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = %s
                        ORDER BY ordinal_position
                    """, (table_name,))
                    db_columns = [row[0] for row in cursor.fetchall()]

                    # Comparaison des structures
                    if set(db_columns) != set(expected_columns):
                        print(f"Structure incompatible - recréation de la table {table_name}")
                        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                        conn.commit()
                        create_station_table(station, processing_type)
                else:
                    create_station_table(station, processing_type)

        # 5. Préparation des données
        def convert_value(val, pg_type):
            if val is None or pd.isna(val):
                return None
            if pg_type == 'timestamp' and isinstance(val, pd.Timestamp):
                return val.to_pydatetime()
            if pg_type == 'date' and hasattr(val, 'date'):
                return val.date()
            return val

        data = []
        for _, row in df.iterrows():
            row_data = []
            for col in expected_columns:
                pg_type = columns_config[col].split()[0]
                val = row[col]
                row_data.append(convert_value(val, pg_type))
            data.append(tuple(row_data))

        # 6. Insertion des données
        with get_connection(db_name) as conn:
            with conn.cursor() as cursor:
                cols = ', '.join([f'"{col}"' for col in expected_columns])
                placeholders = ', '.join(['%s'] * len(expected_columns))
                
                # Construction requête avec gestion de conflit
                pk_col = 'Datetime' if 'Datetime' in expected_columns else None
                if pk_col:
                    set_clause = ', '.join(
                        [f'"{col}" = EXCLUDED."{col}"' 
                         for col in expected_columns 
                         if col != pk_col]
                    )
                    query = f"""
                        INSERT INTO "{table_name}" ({cols}) 
                        VALUES ({placeholders})
                        ON CONFLICT ("{pk_col}") DO UPDATE SET
                            {set_clause}
                    """
                else:
                    query = f"""
                        INSERT INTO "{table_name}" ({cols}) 
                        VALUES ({placeholders})
                    """

                # Insertion par lots
                batch_size = 1000
                total_rows = len(data)
                inserted_rows = 0
                
                for i in range(0, total_rows, batch_size):
                    batch = data[i:i+batch_size]
                    try:
                        cursor.executemany(query, batch)
                        conn.commit()
                        inserted_rows += len(batch)
                        print(f"Lot {i//batch_size + 1} inséré ({len(batch)} lignes)")
                    except Exception as e:
                        conn.rollback()
                        print(f"Erreur sur le lot {i//batch_size + 1}: {str(e)}")
                        if batch:
                            print("Exemple de tuple problématique:", batch[0])
                            print("Types:", [type(x) for x in batch[0]])
                        raise

                print(f"\nSuccès: {inserted_rows}/{total_rows} lignes insérées")
                return True

    except Exception as e:
        print(f"\nERREUR: {str(e)}")
        if 'df' in locals():
            print("\nDétails DataFrame:")
            print("Colonnes:", list(df.columns))
            print("Types:", df.dtypes)
            print("5 premières lignes:\n", df.head())
        raise

def get_stations_list(processing_type: str = 'before') -> List[str]:
    """Retourne la liste des stations disponibles dans la base de données"""
    db_name = 'before_processing_db' if processing_type == 'before' else 'after_processing_db'
    
    try:
        with get_connection(db_name) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                """)
                tables = cursor.fetchall()
                return [table[0].replace('_', ' ') for table in tables]
    except Exception as e:
        print(f"Erreur lors de la récupération de la liste des stations: {e}")
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