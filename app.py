import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session, has_request_context
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import json
import traceback
from werkzeug.utils import secure_filename
from datetime import datetime
from flask_babel import Babel, _, lazy_gettext as _l, get_locale as get_current_locale

# Importations des fonctions de traitement
from data_processing import (
    create_datetime,
    create_rain_mm,
    interpolation,
    _load_and_prepare_gps_data,
    gestion_doublons,
    #calculate_daily_summary_table,
    generer_graphique_par_variable_et_periode,
    generer_graphique_comparatif,
    generate_multi_variable_station_plot,
    apply_station_specific_preprocessing,
    get_var_label,
    get_period_label,
    traiter_outliers_meteo,
    valeurs_manquantes_viz,
    outliers_viz,
    _get_missing_ranges,
    gaps_time_series_viz,
    generate_plot_stats_over_period_plotly,
)

# Importations de la configuration
from config import METADATA_VARIABLES, PALETTE_DEFAUT, DATA_LIMITS, ALLOWED_EXTENSIONS, STATIONS_BY_BASSIN, CUSTOM_STATION_COLORS

# Importations de la base de données
from db import (
    initialize_database, save_to_database, get_connection,get_stations_with_data)
   # get_stations_list, get_station_data, delete_station_data, reset_processed_data)

from dotenv import load_dotenv

app = Flask(__name__)
app.secret_key = 'votre_cle_secrete_ici'
app.config['MAX_CONTENT_LENGTH'] = 1000 * 1024 * 1024
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['STATIC_FOLDER'] = 'static'

# Configuration Flask-Babel
app.config['BABEL_DEFAULT_LOCALE'] = 'en'
app.config['BABEL_TRANSLATION_DIRECTORIES'] = 'translations'

# Initialisation de Babel
babel = Babel(app, locale_selector=lambda: select_locale())

def select_locale():
    if has_request_context():
        if 'lang' in session:
            return session['lang']
        return request.accept_languages.best_match(['en', 'fr'])
    return app.config['BABEL_DEFAULT_LOCALE']

# Initialisation de la base de données
load_dotenv()
initialize_database()
GLOBAL_GPS_DATA_DF = _load_and_prepare_gps_data()


@app.route('/set_language/<lang>')
def set_language(lang):
    if lang in ['fr', 'en']:
        session['lang'] = lang
    return redirect(request.referrer or url_for('index'))

# @app.route('/upload', methods=['POST'])
# def upload_file():
#     """Gère l'upload et le stockage des fichiers de données brutes"""
#     if not request.files:
#         flash(_('Aucun fichier reçu.'), 'error')
#         return redirect(url_for('index'))

#     uploaded_files = request.files.getlist('file[]')
#     stations = []
#     for i in range(len(uploaded_files)):
#         station_name = request.form.get(f'station_{i}')
#         if station_name:
#             stations.append(station_name)
#         else:
#             flash(_('Veuillez sélectionner une station pour chaque fichier.'), 'error')
#             return redirect(url_for('index'))

#     if len(uploaded_files) != len(stations):
#         flash(_('Nombre de fichiers et de stations incompatible.'), 'error')
#         return redirect(url_for('index'))

#     for file, station in zip(uploaded_files, stations):
#         if not file or not allowed_file(file.filename):
#             flash(_("Fichier '%s' invalide ou non autorisé.") % file.filename, 'error')
#             continue

#         try:
#             filename = secure_filename(file.filename)
#             temp_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#             file.save(temp_path)

#             if filename.lower().endswith('.csv'):
#                 df = pd.read_csv(temp_path, encoding_errors='replace')
#             else:
#                 df = pd.read_excel(temp_path)

#             df['Station'] = station
#             df = apply_station_specific_preprocessing(df, station)
            
#             # Sauvegarder les données brutes
#             save_to_database(df, station, 'before')
            
#             os.unlink(temp_path)
#             flash(_("Données brutes pour la station %s sauvegardées avec succès.") % station, 'success')

#         except Exception as e:
#             app.logger.error(f"Erreur traitement fichier {file.filename}: {str(e)}", exc_info=True)
#             flash(_("Erreur lors du traitement du fichier '%s': %s") % (file.filename, str(e)), 'error')
#             continue

#     return redirect(url_for('select_stations'))


# @app.route('/upload', methods=['POST'])
# def upload_file():
#     """Gère l'upload et le stockage des fichiers de données brutes"""
#     if not request.files:
#         flash(_('Aucun fichier reçu.'), 'error')
#         return redirect(url_for('index'))

#     uploaded_files = request.files.getlist('file[]')
#     stations = []
    
#     # Récupération des stations sélectionnées
#     for i in range(len(uploaded_files)):
#         station_name = request.form.get(f'station_{i}')
#         if station_name:
#             stations.append(station_name)
#         else:
#             flash(_('Veuillez sélectionner une station pour chaque fichier.'), 'error')
#             return redirect(url_for('index'))

#     if len(uploaded_files) != len(stations):
#         flash(_('Nombre de fichiers et de stations incompatible.'), 'error')
#         return redirect(url_for('index'))

#     for file, station in zip(uploaded_files, stations):
#         if not file or file.filename == '':
#             flash(_("Fichier vide reçu."), 'error')
#             continue

#         if not allowed_file(file.filename):
#             flash(_("Type de fichier non autorisé pour '%s'.") % file.filename, 'error')
#             continue

#         try:
#             # Sauvegarde temporaire
#             filename = secure_filename(file.filename)
#             temp_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#             file.save(temp_path)
#             app.logger.info(f"Fichier {filename} sauvegardé temporairement")

#             # Lecture du fichier
#             try:
#                 if filename.lower().endswith('.csv'):
#                     # Lecture en deux passes pour une meilleure détection des types
#                     df = pd.read_csv(temp_path, encoding_errors='replace', low_memory=False)
                    
#                     # # Conversion des types
#                     # numeric_cols = ['Rain_01_mm', 'Rain_02_mm', 'Air_Temp_Deg_C', 
#                     #               'Rel_H_%', 'Solar_R_W/m^2', 'Wind_Sp_m/sec', 'Wind_Dir_Deg', 'BP_mbar_Avg]
                    
#                     # for col in numeric_cols:
#                     #     if col in df.columns:
#                     #         df[col] = pd.to_numeric(df[col], errors='coerce')
                    
#                     # # Colonnes temporelles
#                     # time_cols = ['Year', 'Month', 'Day', 'Hour', 'Minute']
#                     # for col in time_cols:
#                     #     if col in df.columns:
#                     #         df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                
#                 else:  # Fichier Excel
#                     df = pd.read_excel(temp_path)

#                 # # Conversion des types
#                 # numeric_cols = ['Rain_01_mm', 'Rain_02_mm', 'Air_Temp_Deg_C', 
#                 #                 'Rel_H_%', 'Solar_R_W/m^2', 'Wind_Sp_m/sec', 'Wind_Dir_Deg', 'BP_mbar_Avg']
                
#                 # if 'Date' in df.columns:
#                 #             df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
                   
#                 # for col in numeric_cols:
#                 #     if col in df.columns:
#                 #         df[col] = pd.to_numeric(df[col], errors='coerce')
                
#                 # # Colonnes temporelles
#                 # time_cols = ['Year', 'Month', 'Day', 'Hour', 'Minute']
#                 # for col in time_cols:
#                 #     if col in df.columns:
#                 #         df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            
                    
#             except Exception as e:
#                 app.logger.error(f"Erreur lecture fichier {filename}: {str(e)}", exc_info=True)
#                 flash(_("Erreur de lecture du fichier '%s': %s") % (filename, str(e)), 'error')
#                 os.unlink(temp_path)
#                 continue

#             if df.empty:
#                 flash(_("Le fichier '%s' est vide ou corrompu.") % filename, 'error')
#                 os.unlink(temp_path)
#                 continue

#             # Ajout du nom de station et prétraitement
#             #df['Station'] = station
#             df = apply_station_specific_preprocessing(df, station)

#             # Vérification finale avant sauvegarde
#             app.logger.info(f"Colonnes avant sauvegarde pour {station}: {df.columns.tolist()}")
#             app.logger.info(f"Premières lignes:\n{df.head(2)}")

#             # Sauvegarde en base
#             try:
#                 success = save_to_database(df, station, 'before')
#                 if success:
#                     flash(_("Données pour %s sauvegardées avec succès!") % station, 'success')
#                 else:
#                     flash(_("Échec de sauvegarde pour %s") % station, 'error')
#             except Exception as e:
#                 app.logger.error(f"Erreur sauvegarde {station}: {str(e)}", exc_info=True)
#                 flash(_("Erreur base de données pour %s: %s") % (station, str(e)), 'error')

#             # Nettoyage
#             os.unlink(temp_path)

#         except Exception as e:
#             app.logger.error(f"Erreur globale traitement {filename}: {str(e)}", exc_info=True)
#             flash(_("Erreur traitement fichier '%s'") % file.filename, 'error')
#             if os.path.exists(temp_path):
#                 os.unlink(temp_path)
#             continue

#     return redirect(url_for('select_stations'))  



# @app.route('/upload', methods=['POST'])
# def upload_file():
#     """Gère l'upload et le stockage des fichiers de données brutes"""
#     if not request.files:
#         flash(_('Aucun fichier reçu.'), 'error')
#         return redirect(url_for('index'))

#     uploaded_files = request.files.getlist('file[]')
#     stations = []
    
#     for i in range(len(uploaded_files)):
#         station_name = request.form.get(f'station_{i}')
#         if station_name:
#             stations.append(station_name)
#         else:
#             flash(_('Veuillez sélectionner une station pour chaque fichier.'), 'error')
#             return redirect(url_for('index'))

#     if len(uploaded_files) != len(stations):
#         flash(_('Nombre de fichiers et de stations incompatible.'), 'error')
#         return redirect(url_for('index'))

#     for file, station in zip(uploaded_files, stations):
#         if not file or file.filename == '':
#             flash(_("Fichier vide reçu."), 'error')
#             continue

#         if not allowed_file(file.filename):
#             flash(_("Type de fichier non autorisé pour '%s'.") % file.filename, 'error')
#             continue

#         try:
#             # Sauvegarde temporaire
#             filename = secure_filename(file.filename)
#             temp_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#             file.save(temp_path)
#             app.logger.info(f"Fichier {filename} sauvegardé temporairement")

#             # Lecture du fichier
#             try:
#                 if filename.lower().endswith('.csv'):
#                     df = pd.read_csv(temp_path, encoding_errors='replace', low_memory=False)
#                 else:  # Fichier Excel
#                     df = pd.read_excel(temp_path)

#                 if df.empty:
#                     flash(_("Le fichier '%s' est vide ou corrompu.") % filename, 'error')
#                     os.unlink(temp_path)
#                     continue

#                 # Prétraitement spécifique
#                 df = apply_station_specific_preprocessing(df, station)

#                 # Vérification finale
#                 app.logger.info(f"Colonnes avant sauvegarde pour {station}: {df.columns.tolist()}")
#                 app.logger.info(f"Premières lignes:\n{df.head(2)}")

#                 # Sauvegarde en base
#                 try:
#                     success = save_to_database(df, station, 'before')
#                     if success:
#                         flash(_("Données pour %s sauvegardées avec succès!") % station, 'success')
#                     else:
#                         flash(_("Échec partiel de sauvegarde pour %s") % station, 'warning')
#                 except Exception as e:
#                     app.logger.error(f"Erreur sauvegarde {station}: {str(e)}", exc_info=True)
#                     flash(_("Erreur base de données pour %s: %s") % (station, str(e)), 'error')

#             except Exception as e:
#                 app.logger.error(f"Erreur lecture fichier {filename}: {str(e)}", exc_info=True)
#                 flash(_("Erreur de lecture du fichier '%s': %s") % (filename, str(e)), 'error')
#             finally:
#                 if os.path.exists(temp_path):
#                     os.unlink(temp_path)

#         except Exception as e:
#             app.logger.error(f"Erreur globale traitement {filename}: {str(e)}", exc_info=True)
#             flash(_("Erreur traitement fichier '%s'") % file.filename, 'error')
#         return render_template('select_stations.html')







# @app.route('/upload', methods=['POST'])
# def upload_file():
#     """Gère l'upload et le stockage des fichiers de données brutes"""
#     if not request.files:
#         flash(_('Aucun fichier reçu.'), 'error')
#         return redirect(url_for('index'))

#     uploaded_files = request.files.getlist('file[]')
#     stations = []
    
#     for i in range(len(uploaded_files)):
#         station_name = request.form.get(f'station_{i}')
#         if station_name:
#             stations.append(station_name)
#         else:
#             flash(_('Veuillez sélectionner une station pour chaque fichier.'), 'error')
#             return redirect(url_for('index'))

#     if len(uploaded_files) != len(stations):
#         flash(_('Nombre de fichiers et de stations incompatible.'), 'error')
#         return redirect(url_for('index'))

#     processing_type = request.form.get('processing_type', 'before')
#     db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')

#     conn = None # Initialise la connexion à None
#     try:
#         conn = get_connection(db_name) # Ouvre la connexion une seule fois pour toutes les stations
#         if not conn:
#             flash(_(f'Erreur: Impossible de se connecter à la base de données {db_name}'), 'error')
#             return redirect(url_for('index'))

#         for file, station in zip(uploaded_files, stations):
#             if not file or file.filename == '':
#                 flash(_("Fichier vide reçu."), 'error')
#                 continue

#             if not allowed_file(file.filename):
#                 flash(_("Type de fichier non autorisé pour '%s'.") % file.filename, 'error')
#                 continue

#             temp_path = None # Assurez-vous que temp_path est défini même en cas d'erreur de fichier
#             try:
#                 # Sauvegarde temporaire
#                 filename = secure_filename(file.filename)
#                 temp_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#                 file.save(temp_path)
#                 app.logger.info(f"Fichier {filename} sauvegardé temporairement")

#                 # Lecture du fichier
#                 try:
#                     if filename.lower().endswith('.csv'):
#                         df = pd.read_csv(temp_path, encoding_errors='replace', low_memory=False)
#                     else:  # Fichier Excel
#                         df = pd.read_excel(temp_path)

#                     if df.empty:
#                         flash(_("Le fichier '%s' est vide ou corrompu.") % filename, 'error')
#                         os.unlink(temp_path)
#                         continue

#                     # Prétraitement spécifique
#                     df = apply_station_specific_preprocessing(df, station)

#                     # Vérification finale
#                     app.logger.info(f"Colonnes avant sauvegarde pour {station}: {df.columns.tolist()}")
#                     app.logger.info(f"Premières lignes:\n{df.head(2).to_string()}") # .to_string() pour un meilleur affichage

#                     print(f"\n--- Traitement de la station : {station} ({len(df)} lignes à insérer) ---") # Nouvelle ligne de log
#                     # Sauvegarde en base
#                     try:
#                         # Passe la connexion à save_to_database
#                         success = save_to_database(df, station, conn, processing_type) # <-- Modifié ici
#                         if success:
#                             flash(_("Données pour %s sauvegardées avec succès!") % station, 'success')
#                         else:
#                             flash(_("Échec partiel de sauvegarde pour %s") % station, 'warning')
#                     except Exception as e:
#                         app.logger.error(f"Erreur sauvegarde {station}: {str(e)}", exc_info=True)
#                         flash(_("Erreur base de données pour %s: %s") % (station, str(e)), 'error')

#                 except Exception as e:
#                     app.logger.error(f"Erreur lecture fichier {filename}: {str(e)}", exc_info=True)
#                     flash(_("Erreur de lecture du fichier '%s': %s") % (filename, str(e)), 'error')
#                 finally:
#                     if temp_path and os.path.exists(temp_path):
#                         os.unlink(temp_path)

#             except Exception as e:
#                 app.logger.error(f"Erreur globale traitement {filename}: {str(e)}", exc_info=True)
#                 flash(_("Erreur traitement fichier '%s'") % file.filename, 'error')
#                 if temp_path and os.path.exists(temp_path): # Assurer le nettoyage même ici
#                     os.unlink(temp_path)
        
#     except Exception as e:
#         flash(_(f'Une erreur inattendue est survenue lors du traitement global: {str(e)}'), 'error')
#         traceback.print_exc()
#     finally:
#         if conn:
#             conn.close() # Ferme la connexion une seule fois à la fin
#             app.logger.info("Connexion à la base de données fermée après traitement de toutes les stations.")

#     return render_template('select_stations.html')







################### Code fonctionnant bien a la date du 23 Jui 2025 ###################

# # Routes
# @app.route('/')
# def index():
#     """Route pour la page d'accueil avec les deux options"""
#     os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
#     return render_template('index.html',
#                          bassins=sorted(STATIONS_BY_BASSIN.keys()),
#                          stations_by_bassin=STATIONS_BY_BASSIN,
#                          existing_stations=get_stations_list('before'))

# @app.route('/select_stations')
# def select_stations():
#     """Affiche la liste des stations disponibles pour traitement"""
#     stations = get_stations_list('before')
#     if not stations:
#         flash(_('Aucune donnée disponible. Veuillez télécharger des fichiers d\'abord.'), 'error')
#         return redirect(url_for('index'))
#     return render_template('select_stations.html', stations=stations)

# @app.route('/upload', methods=['POST'])
# def upload_file():
#     """Gère l'upload et le stockage des fichiers de données brutes"""
#     if not request.files:
#         flash(_('Aucun fichier reçu.'), 'error')
#         return redirect(url_for('index'))

#     uploaded_files = request.files.getlist('file[]')
#     stations = []
    
#     for i in range(len(uploaded_files)):
#         station_name = request.form.get(f'station_{i}')
#         if station_name:
#             stations.append(station_name)
#         else:
#             flash(_('Veuillez sélectionner une station pour chaque fichier.'), 'error')
#             return redirect(url_for('index'))

#     if len(uploaded_files) != len(stations):
#         flash(_('Nombre de fichiers et de stations incompatible.'), 'error')
#         return redirect(url_for('index'))

#     processing_type = request.form.get('processing_type', 'before')
#     db_name = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')

#     conn = None
#     try:
#         conn = get_connection(db_name)
#         if not conn:
#             flash(_(f'Erreur: Impossible de se connecter à la base de données {db_name}'), 'error')
#             return redirect(url_for('index'))

#         for file, station in zip(uploaded_files, stations):
#             if not file or file.filename == '':
#                 flash(_("Fichier vide reçu."), 'error')
#                 continue

#             if not allowed_file(file.filename):
#                 flash(_("Type de fichier non autorisé pour '%s'.") % file.filename, 'error')
#                 continue

#             temp_path = None
#             try:
#                 filename = secure_filename(file.filename)
#                 temp_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#                 file.save(temp_path)
#                 app.logger.info(f"Fichier {filename} sauvegardé temporairement")

#                 # --- LOGIQUE DE LECTURE CORRIGÉE ICI ---
#                 file_extension = filename.lower().rsplit('.', 1)[1]
                
#                 # Détermine le nombre de lignes à sauter
#                 # Si c'est Manyoro, on saute 1 ligne, sinon 0 (par défaut)
#                 skip_rows_count = 1 if station == 'Ouriyori 1' else 0

#                 if file_extension == 'csv':
#                     df = pd.read_csv(temp_path, encoding_errors='replace', low_memory=False, skiprows=skip_rows_count)
#                 elif file_extension == 'xlsx':
#                     df = pd.read_excel(temp_path, skiprows=skip_rows_count)
#                 else:
#                     flash(_("Type de fichier non supporté pour '%s'.") % filename, 'error')
#                     os.unlink(temp_path)
#                     continue
#                 # --- FIN LOGIQUE DE LECTURE CORRIGÉE ---

#                 if df.empty:
#                     flash(_("Le fichier '%s' est vide ou corrompu.") % filename, 'error')
#                     os.unlink(temp_path)
#                     continue

#                 # Prétraitement spécifique
#                 df = apply_station_specific_preprocessing(df, station)

#                 # Vérification finale
#                 app.logger.info(f"Colonnes avant sauvegarde pour {station}: {df.columns.tolist()}")
#                 app.logger.info(f"Premières lignes:\n{df.head(2).to_string()}")

#                 print(f"\n--- Traitement de la station : {station} ({len(df)} lignes à insérer) ---")
#                 try:
#                     success = save_to_database(df, station, conn, processing_type)
#                     if success:
#                         flash(_("Données pour %s sauvegardées avec succès!") % station, 'success')
#                         return redirect(url_for('select_stations'))

#                     else:
#                         flash(_("Échec partiel de sauvegarde pour %s") % station, 'warning')
#                 except Exception as e:
#                     app.logger.error(f"Erreur sauvegarde {station}: {str(e)}", exc_info=True)
#                     flash(_("Erreur base de données pour %s: %s") % (station, str(e)), 'error')

#             except Exception as e:
#                 app.logger.error(f"Erreur lecture fichier {filename}: {str(e)}", exc_info=True)
#                 flash(_("Erreur de lecture du fichier '%s': %s") % (filename, str(e)), 'error')
#             finally:
#                 if temp_path and os.path.exists(temp_path):
#                     os.unlink(temp_path)
        
#     except Exception as e:
#         flash(_(f'Une erreur inattendue est survenue lors du traitement global: {str(e)}'), 'error')
#         traceback.print_exc()
#     finally:
#         if conn:
#             conn.close()
#             app.logger.info("Connexion à la base de données fermée après traitement de toutes les stations.")

#     return render_template('select_stations.html')

############################## Fin du code fonctionnant bien a la date du 23 Jui 2025 ##############################







############################ Jeudi 24 Jui 2025 ############################

@app.route('/')
def index():
    """Route pour la page d'accueil avec les deux options"""
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    # Utilisez get_stations_with_data pour obtenir les stations existantes
    # Cela renverra un dict {bassin: [station1, ...]}
    stations_with_actual_data_by_bassin = get_stations_with_data('before')
    
    return render_template('index.html',
                         bassins=sorted(STATIONS_BY_BASSIN.keys()),
                         stations_by_bassin=STATIONS_BY_BASSIN, # Pour le formulaire d'upload
                         # Adaptez 'existing_stations' pour le template si nécessaire. 
                         # Pour l'instant, je passe le dict complet et il faudra l'adapter dans le JS du template
                         existing_stations_by_bassin=stations_with_actual_data_by_bassin) 

# @app.route('/select_stations')
# def select_stations():
#     """Affiche la liste des stations disponibles pour traitement"""
#     # Utilisez get_stations_with_data ici aussi
#     stations_by_bassin_for_selection = get_stations_with_data('before')
    
#     # Flatten la liste des stations pour la vérification si nécessaire
#     all_available_stations = [station for sublist in stations_by_bassin_for_selection.values() for station in sublist]

#     if not all_available_stations:
#         flash(_('Aucune donnée disponible. Veuillez télécharger des fichiers d\'abord.'), 'error')
#         return redirect(url_for('index'))
    
#     # Passe le dictionnaire complet au template
#     return render_template('select_stations.html', stations_by_bassin=stations_by_bassin_for_selection)



# @app.route('/select_stations')
# def select_stations():
#     """Affiche la liste des stations disponibles pour traitement"""
#     stations_by_bassin_for_selection = get_stations_with_data('before')
    
#     all_available_stations = [station for sublist in stations_by_bassin_for_selection.values() for station in sublist]

#     if not all_available_stations:
#         flash(_('Aucune donnée disponible. Veuillez télécharger des fichiers d\'abord.'), 'error')
#         return redirect(url_for('index'))
    
#     # Récupérer les stations récemment traitées de la session et vider la session
#     # Utilise .pop() avec une valeur par défaut [] pour vider la clé après lecture
#     recently_uploaded_stations = session.pop('recently_uploaded_stations', []) 

#     return render_template('select_stations.html', 
#                            stations_by_bassin=stations_by_bassin_for_selection,
#                            recently_uploaded_stations=recently_uploaded_stations,
#                            custom_station_colors=CUSTOM_STATION_COLORS) # Passer la liste au template




# @app.route('/upload', methods=['POST'])
# def upload_file():
#     """Gère l'upload et le stockage des fichiers de données brutes"""
#     if not request.files:
#         flash(_('Aucun fichier reçu.'), 'error')
#         return redirect(url_for('index'))

#     uploaded_files = request.files.getlist('file[]')
#     stations = []
    
#     for i in range(len(uploaded_files)):
#         station_name = request.form.get(f'station_{i}')
#         if station_name:
#             stations.append(station_name)
#         else:
#             flash(_('Veuillez sélectionner une station pour chaque fichier.'), 'error')
#             return redirect(url_for('index'))

#     if len(uploaded_files) != len(stations):
#         flash(_('Nombre de fichiers et de stations incompatible.'), 'error')
#         return redirect(url_for('index'))

#     processing_type = request.form.get('processing_type', 'before')
#     db_name_to_connect = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')

#     conn = None
#     successfully_processed_and_uploaded_stations = [] 

#     try:
#         conn = get_connection(db_name_to_connect)
#         if not conn:
#             flash(_(f'Erreur: Impossible de se connecter à la base de données {db_name_to_connect}'), 'error')
#             return redirect(url_for('index'))

#         for file, station in zip(uploaded_files, stations):
#             if not file or file.filename == '':
#                 flash(_("Fichier vide reçu."), 'error')
#                 continue

#             if not allowed_file(file.filename):
#                 flash(_("Type de fichier non autorisé pour '%s'.") % file.filename, 'error')
#                 continue

#             temp_path = None
#             df = None # Initialise df à None pour s'assurer qu'il est toujours défini
#             try:
#                 filename = secure_filename(file.filename)
#                 temp_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#                 file.save(temp_path)
#                 app.logger.info(f"Fichier {filename} sauvegardé temporairement")

#                 # --- LOGIQUE DE LECTURE CORRIGÉE ICI (réintégrée depuis ta version) ---
#                 file_extension = filename.lower().rsplit('.', 1)[1]
                
#                 # Détermine le nombre de lignes à sauter
#                 skip_rows_count = 1 if station == 'Ouriyori 1' else 0

#                 if file_extension == 'csv':
#                     df = pd.read_csv(temp_path, encoding_errors='replace', low_memory=False, skiprows=skip_rows_count)
#                 elif file_extension == 'xlsx':
#                     df = pd.read_excel(temp_path, skiprows=skip_rows_count)
#                 else:
#                     flash(_("Type de fichier non supporté pour '%s'.") % filename, 'error')
#                     os.unlink(temp_path)
#                     # continue; cela continuerait la boucle for et ignorerait le fichier courant
#                     # Mais comme tu retournes une redirection plus bas, ce n'est pas nécessaire.
#                     # Il est crucial que df ne soit pas utilisé après ce point si le type n'est pas supporté.
#                     # On peut ajouter un 'continue' ici pour passer au fichier suivant.
#                     continue 
#                 # --- FIN LOGIQUE DE LECTURE CORRIGÉE ---
                
#                 # S'assurer que df a été assigné avant d'être utilisé
#                 # Si le type de fichier n'était pas supporté, df serait encore None.
#                 if df is None or df.empty: # Vérifie si df est None OU s'il est vide
#                     flash(_("Le fichier '%s' est vide, corrompu ou d'un type non supporté.") % filename, 'error')
#                     os.unlink(temp_path)
#                     continue # Passe au fichier suivant

#                 # Prétraitement spécifique
#                 # Assurez-vous que apply_station_specific_preprocessing est importée ou définie
#                 df = apply_station_specific_preprocessing(df, station)

#                 if df.empty:
#                     flash(_("Après prétraitement, le DataFrame pour '%s' est vide. Aucune donnée à sauvegarder.") % station, 'warning')
#                     os.unlink(temp_path)
#                     continue

#                 # Vérification finale
#                 app.logger.info(f"Colonnes avant sauvegarde pour {station}: {df.columns.tolist()}")
#                 app.logger.info(f"Premières lignes:\n{df.head(2).to_string()}")

#                 print(f"\n--- Traitement de la station : {station} ({len(df)} lignes à insérer) ---")
                
#                 # # Créer la table si elle n'existe pas AVANT la sauvegarde pour cette station
#                 # if not create_station_table(station, processing_type):
#                 #     flash(_(f"Impossible de créer/vérifier la table pour la station {station}."), 'error')
#                 #     os.unlink(temp_path)
#                 #     continue

#                 try:
#                     success = save_to_database(df, station, conn, processing_type)
#                     if success:
#                         flash(_("Données pour %s sauvegardées avec succès!") % station, 'success')
#                         # Comme dans ton code original, on redirige après chaque succès.
#                         successfully_processed_and_uploaded_stations.append(station) 
#                         app.logger.info(f"DEBUG: Station '{station}' ajoutée à successfully_processed_and_uploaded_stations. Liste actuelle: {successfully_processed_and_uploaded_stations}")

#                     else:
#                         flash(_("Échec  de sauvegarde pour %s") % station, 'warning')
#                         app.logger.warning(f"DEBUG: save_to_database pour '{station}' a renvoyé False.")

#                 except Exception as e:
#                     app.logger.error(f"Erreur sauvegarde {station}: {str(e)}", exc_info=True)
#                     flash(_("Erreur base de données pour %s: %s") % (station, str(e)), 'error')

#             except Exception as e:
#                 app.logger.error(f"Erreur lecture fichier {filename}: {str(e)}", exc_info=True)
#                 flash(_("Erreur de lecture du fichier '%s': %s") % (filename, str(e)), 'error')
#             finally:
#                 if temp_path and os.path.exists(temp_path):
#                     os.unlink(temp_path)
        
#     except Exception as e:
#         flash(_(f'Une erreur inattendue est survenue lors du traitement global: {str(e)}'), 'error')
#         traceback.print_exc()
#     finally:
#         if conn:
#             conn.close()
#             app.logger.info("Connexion à la base de données fermée après traitement de toutes les stations.")
#             # --- AJOUTE CE LOG POUR DÉBOGUER ---
#             app.logger.info(f"DEBUG: Contenu final de successfully_processed_and_uploaded_stations avant session: {successfully_processed_and_uploaded_stations}")
#             # ----------------------------------
#             session['recently_uploaded_stations'] = list(set(successfully_processed_and_uploaded_stations))
#             # --- AJOUTE CE LOG POUR DÉBOGUER ---
#             app.logger.info(f"DEBUG: Contenu de 'recently_uploaded_stations' dans la session: {session.get('recently_uploaded_stations')}")
#             # ---

#     # Si le code arrive ici, cela signifie qu'aucune redirection n'a eu lieu
#     # (par exemple, si tous les fichiers ont échoué ou s'il n'y avait aucun fichier).
#     # Dans ce cas, nous redirigeons par défaut.
#     return redirect(url_for('select_stations'))


@app.route('/upload', methods=['POST'])
def upload_file():
    """Gère l'upload et le stockage des fichiers de données brutes"""
    if not request.files:
        flash(_('Aucun fichier reçu.'), 'error')
        return redirect(url_for('index'))

    uploaded_files = request.files.getlist('file[]')
    stations = []
    
    for i in range(len(uploaded_files)):
        station_name = request.form.get(f'station_{i}')
        if station_name:
            stations.append(station_name)
        else:
            flash(_('Veuillez sélectionner une station pour chaque fichier.'), 'error')
            return redirect(url_for('index'))

    if len(uploaded_files) != len(stations):
        flash(_('Nombre de fichiers et de stations incompatible.'), 'error')
        return redirect(url_for('index'))

    # 'processing_type' est déjà la clé courte que get_connection attend !
    processing_type = request.form.get('processing_type', 'raw') # Cette variable est parfaite !

    # Vérifiez que processing_type est une clé valide avant de l'utiliser
    # (Optional, but good for robustness)
    if processing_type not in ['raw', 'before', 'after', 'missing_before', 'missing_after']:
        flash(_("Erreur: Type de traitement de base de données invalide."), 'error')
        return redirect(url_for('index'))
        
    # ANCIENNE LIGNE qui causait le problème:
    # db_name_to_connect = os.getenv('DB_NAME_BEFORE') if processing_type == 'before' else os.getenv('DB_NAME_AFTER')
    
    # NOUVELLE LIGNE: Passez directement processing_type qui est la clé courte
    db_key_for_connection = processing_type # <--- C'EST LA CORRECTION ICI

    conn = None
    successfully_processed_and_uploaded_stations = [] 

    try:
        # Utilisez la clé courte db_key_for_connection ici
        conn = get_connection(db_key_for_connection) # <--- UTILISEZ LA CLÉ COURTE
        if not conn:
            flash(_(f'Erreur: Impossible de se connecter à la base de données {db_key_for_connection}'), 'error')
            return redirect(url_for('index'))

        for file, station in zip(uploaded_files, stations):
            if not file or file.filename == '':
                flash(_("Fichier vide reçu."), 'error')
                continue

            if not allowed_file(file.filename):
                flash(_("Type de fichier non autorisé pour '%s'.") % file.filename, 'error')
                continue

            temp_path = None
            df = None 
            try:
                filename = secure_filename(file.filename)
                temp_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                file.save(temp_path)
                app.logger.info(f"Fichier {filename} sauvegardé temporairement")

                file_extension = filename.lower().rsplit('.', 1)[1]
                
                skip_rows_count = 1 if station == 'Ouriyori 1' else 0

                if file_extension == 'csv':
                    df = pd.read_csv(temp_path, encoding_errors='replace', low_memory=False, skiprows=skip_rows_count)
                elif file_extension == 'xlsx':
                    df = pd.read_excel(temp_path, skiprows=skip_rows_count)
                else:
                    flash(_("Type de fichier non supporté pour '%s'.") % filename, 'error')
                    os.unlink(temp_path)
                    continue 
                
                if df is None or df.empty:
                    flash(_("Le fichier '%s' est vide, corrompu ou d'un type non supporté.") % filename, 'error')
                    os.unlink(temp_path)
                    continue 

                df = apply_station_specific_preprocessing(df, station)

                if df.empty:
                    flash(_("Après prétraitement, le DataFrame pour '%s' est vide. Aucune donnée à sauvegarder.") % station, 'warning')
                    os.unlink(temp_path)
                    continue

                app.logger.info(f"Colonnes avant sauvegarde pour {station}: {df.columns.tolist()}")
                app.logger.info(f"Premières lignes:\n{df.head(2).to_string()}")

                print(f"\n--- Traitement de la station : {station} ({len(df)} lignes à insérer) ---")
                
                try:
                    success = save_to_database(df, station, conn, processing_type)
                    if success:
                        flash(_("Données pour %s sauvegardées avec succès!") % station, 'success')
                        successfully_processed_and_uploaded_stations.append(station) 
                        app.logger.info(f"DEBUG: Station '{station}' ajoutée à successfully_processed_and_uploaded_stations. Liste actuelle: {successfully_processed_and_uploaded_stations}")

                    else:
                        flash(_("Échec  de sauvegarde pour %s") % station, 'warning')
                        app.logger.warning(f"DEBUG: save_to_database pour '{station}' a renvoyé False.")

                except Exception as e:
                    app.logger.error(f"Erreur sauvegarde {station}: {str(e)}", exc_info=True)
                    flash(_("Erreur base de données pour %s: %s") % (station, str(e)), 'error')

            except Exception as e:
                app.logger.error(f"Erreur lecture fichier {filename}: {str(e)}", exc_info=True)
                flash(_("Erreur de lecture du fichier '%s': %s") % (filename, str(e)), 'error')
            finally:
                if temp_path and os.path.exists(temp_path):
                    os.unlink(temp_path)
        
    except Exception as e:
        flash(_(f'Une erreur inattendue est survenue lors du traitement global: {str(e)}'), 'error')
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            app.logger.info("Connexion à la base de données fermée après traitement de toutes les stations.")
            app.logger.info(f"DEBUG: Contenu final de successfully_processed_and_uploaded_stations avant session: {successfully_processed_and_uploaded_stations}")
            session['recently_uploaded_stations'] = list(set(successfully_processed_and_uploaded_stations))
            app.logger.info(f"DEBUG: Contenu de 'recently_uploaded_stations' dans la session: {session.get('recently_uploaded_stations')}")

    return redirect(url_for('select_stations'))

# from  db  import load_raw_station_data
# @app.route('/process_selected_data', methods=['POST'])
# def process_selected_data():
#     selected_stations = request.form.getlist('selected_stations')
    
#     if not selected_stations:
#         flash(_("Veuillez sélectionner au moins une station pour l'analyse."), 'warning')
#         return redirect(url_for('select_stations'))

#     flash(_(f"Stations sélectionnées pour analyse : {', '.join(selected_stations)}"), 'info')
    
#     datasets = []
#     # Former le dataset de la station en question
#     for station_name in selected_stations:
#         df = load_raw_station_data(station_name, 'before')
#         datasets.append(df)

#     # Vérifier si les datasets sont vides
#     if not datasets or all(df.empty for df in datasets):
#         flash(_("Aucune donnée disponible pour les stations sélectionnées."), 'error')
#         return redirect(url_for('select_stations'))
    
#     # Traiter les données
#     # Type de traitement : traitement
#     processing_type = request.form.get('processing_type', 'after')
#     db_name_to_connect = os.getenv('DB_NAME_BEFORE') if processing_type == 'after' else os.getenv('DB_NAME_AFTER')
#     conn = None

#     try:
        

#         for station in selected_stations:
#             continue
            
#     except:
#         flash(_("Une erreur est survenue lors du traitement des stations sélectionnées."), 'error')
#         app.logger.error(f"Erreur lors du traitement des stations sélectionnées: {str(e)}", exc_info=True)
#         return redirect(url_for('select_stations'))
    
#     return redirect(url_for('index'))

from  db  import load_raw_station_data

from flask import Blueprint, render_template, request, flash, redirect, url_for
import logging

# Configuration du logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

main_bp = Blueprint('main', __name__)

# ... (les autres routes comme select_stations restent ici) ...


# @app.route('/process_selected_stations')
# def process_selected_stations():
#     """
#     Traite les données pour les stations sélectionnées par l'utilisateur.
#     Cette fonction agit comme la fonction 'process_selected_data'.
#     """
#     selected_stations_str = request.args.get('stations')
#     if not selected_stations_str:
#         flash(_('Aucune station sélectionnée pour le traitement.'), 'danger')
#         return redirect(url_for('main.select_stations'))

#     selected_stations = selected_stations_str.split(',')
    
#     processing_results = [] # Pour stocker les messages de succès/échec par station
    
#     processing_type = request.form.get('processing_type', 'after')
#     db_name_to_connect = os.getenv('DB_NAME_BEFORE') if processing_type == 'after' else os.getenv('DB_NAME_AFTER')
#     conn = None

#     try:
#         # Charger les données GPS une seule fois au début pour toutes les stations
#         # Ceci est plus efficace que de le faire pour chaque station.
#         df_gps = _load_and_prepare_gps_data()
#         if df_gps.empty:
#             flash(_("Erreur: Impossible de charger les données GPS des stations. Le traitement ne peut pas continuer."), 'danger')
#             return redirect(url_for('main.select_stations'))

#         # Itérer sur chaque station sélectionnée
#         for station_name in selected_stations:
#             logging.info(f"Début du traitement pour la station: {station_name}")
#             try:
#                 # 1. Charger les données brutes de la station depuis before_processing_db
#                 # La fonction load_raw_station_data gère déjà le renommage de 'Rel_H_Pct' en 'Rel_H_%'
#                 df_raw = load_raw_station_data(station_name)
#                 if df_raw.empty:
#                     flash(_('Aucune donnée brute trouvée pour la station %s.') % station_name, 'warning')
#                     processing_results.append({'station': station_name, 'status': 'failed', 'message': _('Aucune donnée brute.')})
#                     continue

#                 # Assurez-vous que df_raw contient une colonne 'Station'
#                 # Normalement, la DB ne stocke pas 'Station' comme une colonne de données
#                 # mais le pipeline l'attend pour le regroupement.
#                 if 'Station' not in df_raw.columns:
#                     df_raw['Station'] = station_name # Ajoute la colonne 'Station' avec le nom actuel
                
#                 # S'assurer que 'Station' est une vraie colonne si l'index est 'Datetime'
#                 if df_raw.index.name == 'Datetime' and 'Station' in df_raw.columns:
#                      # Si 'Station' est déjà une colonne et Datetime est l'index, c'est bon.
#                      # Si 'Station' était l'index et Datetime aussi, ça poserait problème, mais on a Datetime en index
#                      pass
#                 else:
#                     # Si 'Station' a été ajoutée après un reset_index, ou est manquante
#                     df_raw = df_raw.reset_index().set_index('Datetime') # Assure DatetimeIndex
#                     df_raw['Station'] = station_name
                

                
#                 # 2. Exécuter le pipeline d'interpolation
#                 # La fonction `interpolation` reçoit le DataFrame brut, les limites et les infos GPS.
#                 df_before_interp, df_after_interp, missing_before, missing_after = \
#                     interpolation(df_raw, DATA_LIMITS, df_gps) 

#                 if df_after_interp.empty:
#                     flash(_('Le pipeline d\'interpolation n\'a retourné aucune donnée pour la station %s.') % station_name, 'warning')
#                     processing_results.append({'station': station_name, 'status': 'failed', 'message': _('Aucune donnée interpolée retournée.')})
#                     continue
                
#                 conn = get_connection(db_name_to_connect)
#                 if not conn:
#                     flash(_(f'Erreur: Impossible de se connecter à la base de données {db_name_to_connect}'), 'error')
#                     return redirect(url_for('index'))
                

#                 # 3. Sauvegarder les données traitées dans after_processing_db
#                 # La fonction save_processed_data_to_db gère le renommage inverse 'Rel_H_%' en 'Rel_H_Pct'
#                 save_to_database(df_after_interp, station_name, conn, processing_type)
                
#                 flash(_('Traitement et sauvegarde réussis pour la station %s.') % station_name, 'success')
#                 processing_results.append({'station': station_name, 'status': 'success', 'message': _('Traitement réussi.')})

#                 logging.info(f"Missing ranges BEFORE interpolation for {station_name}:\n{missing_before.to_string()}")
#                 logging.info(f"Missing ranges AFTER interpolation for {station_name}:\n{missing_after.to_string()}")

#             except Exception as e:
#                 # Gérer les erreurs spécifiques à chaque station
#                 logging.error(f"Erreur lors du traitement de la station {station_name}: {e}", exc_info=True)
#                 flash(_('Erreur lors du traitement de la station %s: %s') % (station_name, str(e)), 'danger')
#                 processing_results.append({'station': station_name, 'status': 'failed', 'message': str(e)})

#     except Exception as e:
#         # Gérer les erreurs générales (ex: problème de chargement GPS global)
#         logging.error(f"Erreur générale lors du chargement des données GPS ou boucle des stations: {e}", exc_info=True)
#         flash(_('Une erreur inattendue est survenue: %s') % str(e), 'danger')

#     finally:
#             if conn:
#                 conn.close()
#                 app.logger.info("Connexion à la base de données fermée après traitement de toutes les stations.")
#         # Afficher un résumé des résultats à l'utilisateur
#     return render_template('processing_summary.html', results=processing_results)


# @app.route('/process_selected_stations') 
# def process_selected_stations():
#     """
#     Traite les données pour les stations sélectionnées par l'utilisateur.
#     """
#     selected_stations_str = request.args.get('stations')
#     if not selected_stations_str:
#         flash(_('Aucune station sélectionnée pour le traitement.'), 'danger')
#         return redirect(url_for('select_stations'))

#     selected_stations = selected_stations_str.split(',')
    
#     processing_results = []
#     conn = None 
    
#     # Nous traitons TOUJOURS des données de 'before_processing_db' pour les sauvegarder dans 'after_processing_db'.
#     # Donc, le processing_type pour la sauvegarde est 'after'.
#     db_name_to_save = os.getenv('DB_NAME_AFTER') 

#     try:
#         df_gps = _load_and_prepare_gps_data()
#         if df_gps.empty:
#             flash(_("Erreur: Impossible de charger les données GPS des stations. Le traitement ne peut pas continuer."), 'danger')
#             return redirect(url_for('select_stations'))
        
#         # Établir une seule connexion pour la sauvegarde avant la boucle sur les stations
#         # Cela est plus performant que d'ouvrir/fermer la connexion pour chaque station.
#         conn = get_connection(db_name_to_save)
#         if not conn:
#             flash(_(f'Erreur: Impossible de se connecter à la base de données de sauvegarde ({db_name_to_save}).'), 'danger')
#             return redirect(url_for('select_stations'))

#         # Assurez-vous que la connexion est en autocommit, ou gérez les commits manuellement
#         # save_to_database gère ses propres commits par lot, donc autocommit n'est pas strictement nécessaire ici,
#         # mais peut être utile si d'autres opérations DB sont ajoutées.
#         # conn.autocommit = True # Pas nécessaire si save_to_database gère ses commits internes

#         for station_name in selected_stations:
#             logging.info(f"Début du traitement pour la station: {station_name}")
#             try:
#                 df_raw = load_raw_station_data(station_name)
#                 if df_raw.empty:
#                     flash(_('Aucune donnée brute trouvée pour la station %s.') % station_name, 'warning')
#                     processing_results.append({'station': station_name, 'status': 'failed', 'message': _('Aucune donnée brute.')})
#                     continue

#                 if 'Station' not in df_raw.columns:
#                     df_raw['Station'] = station_name
                
#                 if not isinstance(df_raw.index, pd.DatetimeIndex) or 'Station' not in df_raw.columns:
#                     df_raw = df_raw.reset_index().set_index('Datetime')
#                     df_raw['Station'] = station_name
                
#                 df_before_interp, df_after_interp, missing_before, missing_after = \
#                     interpolation(df_raw, LIMITS, df_gps) 

#                 if df_after_interp.empty:
#                     flash(_('Le pipeline d\'interpolation n\'a retourné aucune donnée pour la station %s.') % station_name, 'warning')
#                     processing_results.append({'station': station_name, 'status': 'failed', 'message': _('Aucune donnée interpolée retournée.')})
#                     continue
                
#                 # <-- MODIFIÉ ICI: Appel à save_to_database
#                 # Passer la connexion ouverte et le type de traitement 'after'
#                 save_success = save_to_database(df_after_interp, station_name, conn, processing_type='after')
                
#                 if save_success:
#                     flash(_('Traitement et sauvegarde réussis pour la station %s.') % station_name, 'success')
#                     processing_results.append({'station': station_name, 'status': 'success', 'message': _('Traitement réussi.')})
#                 else:
#                     flash(_('Échec de la sauvegarde pour la station %s. Voir les logs pour plus de détails.') % station_name, 'danger')
#                     processing_results.append({'station': station_name, 'status': 'failed', 'message': _('Échec de la sauvegarde.')})


#                 logging.info(f"Missing ranges BEFORE interpolation for {station_name}:\n{missing_before.to_string()}")
#                 logging.info(f"Missing ranges AFTER interpolation for {station_name}:\n{missing_after.to_string()}")

#             except Exception as e:
#                 logging.error(f"Erreur lors du traitement de la station {station_name}: {e}", exc_info=True)
#                 flash(_('Erreur lors du traitement de la station %s: %s') % (station_name, str(e)), 'danger')
#                 processing_results.append({'station': station_name, 'status': 'failed', 'message': str(e)})

#     except Exception as e:
#         logging.error(f"Erreur générale lors du chargement des données GPS ou boucle des stations: {e}", exc_info=True)
#         flash(_('Une erreur inattendue est survenue: %s') % str(e), 'danger')

#     finally:
#         if conn:
#             conn.close()
#             app.logger.info("Connexion à la base de données fermée après traitement de toutes les stations.")
    
#     return render_template('processing_summary.html', results=processing_results)

    #return app, babel













# # --- Nouvelle fonction interne pour la logique de traitement ---
#     def _process_stations_logic(stations_to_process):
#         """
#         Logique interne de traitement des données pour une liste de stations.
#         Cette fonction n'est PAS une route Flask.
#         Elle retourne les résultats du traitement.
#         """
#         processing_results = []
#         conn = None 
#         db_name_to_save = os.getenv('DB_NAME_AFTER') 

#         try:
#             df_gps = _load_and_prepare_gps_data()
#             if df_gps.empty:
#                 # Log l'erreur mais ne flash pas ici car c'est une fonction interne
#                 logging.error("Erreur: Impossible de charger les données GPS des stations.")
#                 return [{'station': 'Global', 'status': 'failed', 'message': _('Erreur globale: Impossible de charger les données GPS.')}]
            
#             conn = get_connection(db_name_to_save)
#             if not conn:
#                 logging.error(f'Erreur: Impossible de se connecter à la base de données de sauvegarde ({db_name_to_save}).')
#                 return [{'station': 'Global', 'status': 'failed', 'message': _(f'Erreur globale: Impossible de se connecter à la base de données ({db_name_to_save}).')}]

#             for station_name in stations_to_process:
#                 logging.info(f"Début du traitement pour la station: {station_name}")
#                 try:
#                     df_raw = load_raw_station_data(station_name)
#                     if df_raw.empty:
#                         processing_results.append({'station': station_name, 'status': 'failed', 'message': _('Aucune donnée brute.')})
#                         continue

#                     if 'Station' not in df_raw.columns:
#                         df_raw['Station'] = station_name
                    
#                     if not isinstance(df_raw.index, pd.DatetimeIndex) or 'Station' not in df_raw.columns:
#                         df_raw = df_raw.reset_index().set_index('Datetime')
#                         df_raw['Station'] = station_name
                    
#                     df_before_interp, df_after_interp, missing_before, missing_after = \
#                         interpolation(df_raw, DATA_LIMITS, df_gps) 

#                     if df_after_interp.empty:
#                         processing_results.append({'station': station_name, 'status': 'failed', 'message': _('Aucune donnée interpolée retournée.')})
#                         continue
                    
#                     save_success = save_to_database(df_after_interp, station_name, conn, processing_type='after')
                    
#                     if save_success:
#                         processing_results.append({'station': station_name, 'status': 'success', 'message': _('Traitement réussi.')})
#                     else:
#                         processing_results.append({'station': station_name, 'status': 'failed', 'message': _('Échec de la sauvegarde.')})

#                     logging.info(f"Missing ranges BEFORE interpolation for {station_name}:\n{missing_before.to_string()}")
#                     logging.info(f"Missing ranges AFTER interpolation for {station_name}:\n{missing_after.to_string()}")

#                 except Exception as e:
#                     logging.error(f"Erreur lors du traitement de la station {station_name}: {e}", exc_info=True)
#                     processing_results.append({'station': station_name, 'status': 'failed', 'message': str(e)})

#         except Exception as e:
#             logging.error(f"Erreur générale lors du chargement des données GPS ou boucle des stations: {e}", exc_info=True)
#             processing_results.append({'station': 'Global', 'status': 'failed', 'message': _('Une erreur inattendue est survenue: %s') % str(e)})

#         finally:
#             if conn:
#                 conn.close()
#                 app.logger.info("Connexion à la base de données fermée après traitement de toutes les stations.")
        
#         return processing_results

#  # --- Nouvelle route qui appelle la fonction de traitement ---
# @app.route('/run_processing')
# def run_processing():
#     """
#     Déclenche le traitement des stations sélectionnées et affiche le résumé.
#     """
#     selected_stations = session.pop('selected_stations', None) # Récupérer les stations de la session

#     if not selected_stations:
#         flash(_('Aucune station sélectionnée ou session expirée. Veuillez sélectionner des stations à nouveau.'), 'danger')
#         return redirect(url_for('select_stations'))

#     # Appeler la fonction de logique de traitement
#     results = _process_stations_logic(selected_stations)
    
#     # Flash les messages globaux si la liste de résultats contient une erreur "Global"
#     for res in results:
#         if res.get('station') == 'Global' and res.get('status') == 'failed':
#             flash(res.get('message'), 'danger')

#     return render_template('processing_summary.html', results=results)

# #return app, babel



# @app.route('/select_stations', methods=['GET', 'POST'])
# def select_stations():
#     """
#     Route pour afficher les stations disponibles et gérer la sélection.
#     Lors d'un POST, stocke les stations sélectionnées en session et redirige vers la route de traitement.
#     """
#     #stations_available = get_stations_with_data(processing_type='before')
#     stations_by_bassin_for_selection = get_stations_with_data('before')

#     #     # Flatten la liste des stations pour la vérification si nécessaire
#     all_available_stations = [station for sublist in stations_by_bassin_for_selection.values() for station in sublist]

#     # Simulez des stations récemment mises à jour pour le template
#     # À adapter avec votre logique réelle si cette liste est dynamique
#     recently_uploaded_stations = [] 

#     if request.method == 'POST':
#         selected_stations = request.form.getlist('selected_stations')
#         if not selected_stations:
#             flash(_('Veuillez sélectionner au moins une station.'), 'warning')
#             return render_template('select_stations.html', 
#                                     stations_available=all_available_stations,
#                                     recently_uploaded_stations=recently_uploaded_stations) # Passer la liste même en cas d'erreur
        
#         # Stocker les stations sélectionnées dans la session
#         session['selected_stations_for_processing'] = selected_stations 
        
#         # Rediriger vers la route qui va réellement déclencher le traitement.
#         # C'est une redirection GET pour éviter les soumissions multiples.
#         return redirect(url_for('run_processing'))
        
#     return render_template('select_stations.html', 
#                             stations_available=all_available_stations,
#                             recently_uploaded_stations=recently_uploaded_stations, 
#                             stations_by_bassin=stations_by_bassin_for_selection,
#                             custom_station_colors=CUSTOM_STATION_COLORS)



# ... (votre code existant)

# @app.route('/select_stations', methods=['GET', 'POST'])
# def select_stations():
#     """
#     Route pour afficher les stations disponibles et gérer la sélection.
#     Lors d'un POST, stocke les stations sélectionnées en session et redirige vers la route de traitement.
#     """
#     stations_by_bassin_for_selection = get_stations_with_data('before')

#     all_available_stations = [station for sublist in stations_by_bassin_for_selection.values() for station in sublist]

#     # --- CORRECTION ICI ---
#     # Récupérer la liste des stations récemment mises à jour depuis la session.
#     # Si la clé n'existe pas dans la session (par exemple, au premier chargement de la page),
#     # cela retournera une liste vide.
#     recently_uploaded_stations = session.get('recently_uploaded_stations', [])
#     # ----------------------

#     if request.method == 'POST':
#         selected_stations = request.form.getlist('selected_stations')
#         if not selected_stations:
#             flash(_('Veuillez sélectionner au moins une station.'), 'warning')
#             return render_template('select_stations.html',
#                                     stations_available=all_available_stations,
#                                     recently_uploaded_stations=recently_uploaded_stations, # Passer la liste récupérée de la session
#                                     stations_by_bassin=stations_by_bassin_for_selection,
#                                     custom_station_colors=CUSTOM_STATION_COLORS) # Assurez-vous de passer tout ce qui est nécessaire

#         # Stocker les stations sélectionnées dans la session
#         session['selected_stations_for_processing'] = selected_stations

#         # Rediriger vers la route qui va réellement déclencher le traitement.
#         return redirect(url_for('process_selected_stations'))

#     return render_template('select_stations.html',
#                             stations_available=all_available_stations,
#                             recently_uploaded_stations=recently_uploaded_stations, # Passer la liste récupérée de la session
#                             stations_by_bassin=stations_by_bassin_for_selection,
#                             custom_station_colors=CUSTOM_STATION_COLORS)




# @app.route('/select_stations')
# def select_stations():
#     """Affiche la liste des stations disponibles pour traitement"""
#     stations_by_bassin_for_selection = get_stations_with_data('before')
    
#     all_available_stations = [station for sublist in stations_by_bassin_for_selection.values() for station in sublist]

#     if not all_available_stations:
#         flash(_('Aucune donnée disponible. Veuillez télécharger des fichiers d\'abord.'), 'error')
#         return redirect(url_for('index'))
    
#     # Récupérer les stations récemment traitées de la session et vider la session
#     # Utilise .pop() avec une valeur par défaut [] pour vider la clé après lecture
#     recently_uploaded_stations = session.pop('recently_uploaded_stations', []) 

#     return render_template('select_stations.html', 
#                            stations_by_bassin=stations_by_bassin_for_selection,
#                            recently_uploaded_stations=recently_uploaded_stations,
#                            custom_station_colors=CUSTOM_STATION_COLORS) # Passer la liste au template


# @app.route('/select_stations')
# def select_stations():
#     """Affiche la liste des stations disponibles pour traitement"""
#     # Utilisez get_stations_with_data ici aussi
#     stations_by_bassin_for_selection = get_stations_with_data('before')
    
#     # Flatten la liste des stations pour la vérification si nécessaire
#     all_available_stations = [station for sublist in stations_by_bassin_for_selection.values() for station in sublist]

#     if not all_available_stations:
#         flash(_('Aucune donnée disponible. Veuillez télécharger des fichiers d\'abord.'), 'error')
#         return redirect(url_for('index'))
    
#     # Passe le dictionnaire complet au template
#     return render_template('select_stations.html', stations_by_bassin=stations_by_bassin_for_selection)

import warnings
# @app.route('/process_selected_stations', methods=['POST']) # <-- CETTE ROUTE ACCEPTE MAINTENANT LE POST DU FORMULAIRE
# def process_selected_stations():
#     """
#     Traite les données pour les stations sélectionnées par l'utilisateur.
#     Cette route est directement appelée par le formulaire de sélection en POST.
#     """
#     # Récupérer les stations sélectionnées directement du formulaire POST
#     selected_stations = request.form.getlist('selected_stations') 

#     if not selected_stations:
#         flash(_('Veuillez sélectionner au moins une station.'), 'danger')
#         # Rediriger l'utilisateur vers la page de sélection s'il n'a rien choisi
#         return redirect(url_for('select_stations'))
    
#     processing_results = []
#     conn = None 
#     db_name_to_save = os.getenv('DB_NAME_AFTER') # Nous sauvegardons toujours dans la DB 'after'


#     try:
#         # Charger les données GPS une seule fois au début du traitement
#         df_gps = _load_and_prepare_gps_data()
#         if df_gps.empty:
#             flash(_("Erreur: Impossible de charger les données GPS des stations. Le traitement ne peut pas continuer."), 'danger')
#             return redirect(url_for('select_stations'))
        
#         # Établir une seule connexion à la base de données de sauvegarde pour toutes les opérations
#         conn = get_connection(db_name_to_save)
#         if not conn:
#             flash(_(f'Erreur: Impossible de se connecter à la base de données de sauvegarde ({db_name_to_save}).'), 'danger')
#             return redirect(url_for('select_stations'))

#         # Itérer sur chaque station sélectionnée et traiter ses données
#         for station_name in selected_stations:
#             logging.info(f"Début du traitement pour la station: {station_name}")
#             try:
#                 # 1. Charger les données brutes de la station depuis la DB 'before'
#                 df_raw = load_raw_station_data(station_name)
#                 if df_raw.empty:
#                     flash(_('Aucune donnée brute trouvée pour la station %s.') % station_name, 'warning')
#                     processing_results.append({'station': station_name, 'status': 'failed', 'message': _('Aucune donnée brute.')})
#                     continue

#                 # Assurez-vous que le DataFrame a les colonnes et le format attendus
#                 if 'Station' not in df_raw.columns:
#                     df_raw['Station'] = station_name
#                 if not isinstance(df_raw.index, pd.DatetimeIndex):
#                     # Si Datetime n'est pas l'index, le réinitialiser et le définir comme index
#                     if 'Datetime' in df_raw.columns:
#                         df_raw = df_raw.set_index('Datetime')
#                     else:
#                         raise ValueError(f"Colonne 'Datetime' manquante pour la station {station_name}")
                
#                 # 2. Exécuter le pipeline d'interpolation et de traitement
#                 df_before_interp, df_after_interp, missing_before, missing_after = \
#                     interpolation(df_raw, DATA_LIMITS, df_gps) 

#                 if df_after_interp.empty:
#                     flash(_('Le pipeline d\'interpolation n\'a retourné aucune donnée pour la station %s.') % station_name, 'warning')
#                     processing_results.append({'station': station_name, 'status': 'failed', 'message': _('Aucune donnée interpolée retournée.')})
#                     continue
                
#                 # appliquer la creation de la variable Rain_mm, moyenne des valeurs de pluie Rain_01_mm et Rain_02_mm
#                 #df_after_interp = create_rain_mm(df_after_interp)

#                 # Traiter les valeurs aberrantes météorologiques
#                 #df_after_interp = traiter_outliers_meteo(df_after_interp, colonnes=DATA_LIMITS.keys())
#                 # 3. Sauvegarder les données traitées dans la DB 'after'
#                 # La fonction save_to_database s'attend à la connexion et au type de traitement
#                 save_success = save_to_database(df_after_interp, station_name, conn, processing_type='after')
                
#                 if save_success:
#                     flash(_('Traitement et sauvegarde réussis pour la station %s.') % station_name, 'success')
#                     processing_results.append({'station': station_name, 'status': 'success', 'message': _('Traitement réussi.')})
#                 else:
#                     flash(_('Échec de la sauvegarde pour la station %s. Voir les logs du serveur pour plus de détails.') % station_name, 'danger')
#                     processing_results.append({'station': station_name, 'status': 'failed', 'message': _('Échec de la sauvegarde.')})

#                 # Journaliser les informations sur les données manquantes
#                 logging.info(f"Missing ranges BEFORE interpolation for {station_name}:\n{missing_before.to_string()}")
#                 logging.info(f"Missing ranges AFTER interpolation for {station_name}:\n{missing_after.to_string()}")

#             except Exception as e:
#                 # Gérer les erreurs spécifiques à chaque station
#                 logging.error(f"Erreur lors du traitement de la station {station_name}: {e}", exc_info=True)
#                 flash(_('Erreur lors du traitement de la station %s: %s') % (station_name, str(e)), 'danger')
#                 processing_results.append({'station': station_name, 'status': 'failed', 'message': str(e)})

#     except Exception as e:
#         # Gérer les erreurs générales qui pourraient survenir avant ou pendant la boucle des stations
#         logging.error(f"Erreur générale lors du chargement des données GPS ou de l'initialisation: {e}", exc_info=True)
#         flash(_('Une erreur inattendue est survenue lors de la préparation du traitement: %s') % str(e), 'danger')
#         # Ajout d'un résultat global pour le résumé si une erreur critique se produit
#         processing_results.append({'station': 'Global', 'status': 'failed', 'message': _('Erreur critique générale: %s') % str(e)})

#     finally:
#         # S'assurer que la connexion à la base de données est fermée
#         if conn:
#             conn.close()
#             app.logger.info("Connexion à la base de données fermée après traitement de toutes les stations.")
    
#     # Afficher le résumé du traitement sur une nouvelle page
#     return render_template('processing_summary.html', results=processing_results)


# Fichier: app.py

# @app.route('/process_selected_stations', methods=['POST'])
# def process_selected_stations():
#     selected_stations = request.form.getlist('selected_stations') 

#     if not selected_stations:
#         flash(_('Veuillez sélectionner au moins une station.'), 'danger')
#         return redirect(url_for('select_stations'))
    
#     processing_results = []
    
#     db_key_for_save_connection = 'after' 
#     conn = None

#     try:
#         df_gps = _load_and_prepare_gps_data()
#         if df_gps.empty:
#             flash(_("Erreur: Impossible de charger les données GPS des stations. Le traitement ne peut pas continuer."), 'danger')
#             return redirect(url_for('select_stations'))
        
#         conn = get_connection(db_key_for_save_connection) 
#         if not conn:
#             flash(_(f'Erreur: Impossible de se connecter à la base de données de sauvegarde ({db_key_for_save_connection}).'), 'danger')
#             return redirect(url_for('select_stations'))

#         for station_name in selected_stations:
#             logging.info(f"Début du traitement pour la station: {station_name}")
#             try:
#                 df_raw = load_raw_station_data(station_name, processing_type='before') 
#                 if df_raw.empty:
#                     flash(_('Aucune donnée brute trouvée pour la station %s.') % station_name, 'warning')
#                     processing_results.append({'station': station_name, 'status': 'failed', 'message': _('Aucune donnée brute.')})
#                     continue

#                 if 'Station' not in df_raw.columns:
#                     df_raw['Station'] = station_name
#                 if not isinstance(df_raw.index, pd.DatetimeIndex):
#                     if 'Datetime' in df_raw.columns:
#                         df_raw = df_raw.set_index('Datetime')
#                     else:
#                         raise ValueError(f"Colonne 'Datetime' manquante pour la station {station_name}")
                
#                 df_before_interp, df_after_interp, missing_before, missing_after = \
#                     interpolation(df_raw, DATA_LIMITS, df_gps) 

#                 if df_after_interp.empty:
#                     flash(_('Le pipeline d\'interpolation n\'a retourné aucune donnée pour la station %s.') % station_name, 'warning')
#                     processing_results.append({'station': station_name, 'status': 'failed', 'message': _('Aucune donnée interpolée retournée.')})
#                     continue
                
#                 # --- CORRECTION ICI ---
#                 # Call save_to_database directly for each dataset
#                 save_main_data_success = save_to_database(df_after_interp, station_name, conn, processing_type='after')
                
#                 # Assign the boolean results to distinct variables
#                 save_missing_before_success = save_to_database(missing_before, station_name, conn, processing_type='missing_before')
#                 save_missing_after_success = save_to_database(missing_after, station_name, conn, processing_type='missing_after')

#                 if save_main_data_success and save_missing_before_success and save_missing_after_success:
#                     flash(_('Traitement et sauvegarde réussis pour la station %s, y compris les données manquantes.') % station_name, 'success')
#                     processing_results.append({'station': station_name, 'status': 'success', 'message': _('Traitement réussi.')})
#                 elif save_main_data_success: # Main data saved, but missing data might have failed
#                     flash(_('Traitement réussi pour la station %s, mais certaines données manquantes n\'ont pas pu être sauvegardées.') % station_name, 'warning')
#                     processing_results.append({'station': station_name, 'status': 'partial_success', 'message': _('Traitement réussi, échec partiel de sauvegarde des données manquantes.')})
#                 else: # Main data save failed
#                     flash(_('Échec de la sauvegarde pour la station %s. Voir les logs du serveur pour plus de détails.') % station_name, 'danger')
#                     processing_results.append({'station': station_name, 'status': 'failed', 'message': _('Échec de la sauvegarde.')})

#                 logging.info(f"Missing ranges BEFORE interpolation for {station_name}:\n{missing_before.to_string()}")
#                 logging.info(f"Missing ranges AFTER interpolation for {station_name}:\n{missing_after.to_string()}")

#             except Exception as e:
#                 logging.error(f"Erreur lors du traitement de la station {station_name}: {e}", exc_info=True)
#                 flash(_('Erreur lors du traitement de la station %s: %s') % (station_name, str(e)), 'danger')
#                 processing_results.append({'station': station_name, 'status': 'failed', 'message': str(e)})

#     except Exception as e:
#         logging.error(f"Erreur générale lors du chargement des données GPS ou de l'initialisation: {e}", exc_info=True)
#         flash(_('Une erreur inattendue est survenue lors de la préparation du traitement: %s') % str(e), 'danger')
#         processing_results.append({'station': 'Global', 'status': 'failed', 'message': _('Erreur critique générale: %s') % str(e)})

#     finally:
#         if conn:
#             conn.close()
#             app.logger.info("Connexion à la base de données fermée après traitement de toutes les stations.")
    
#     return render_template('processing_summary.html', results=processing_results)


# ... (imports et autre code)


###########33 code  fonctionnant bien ############

# @app.route('/select_stations', methods=['GET', 'POST'])
# def select_stations():
#     """
#     Route pour afficher les stations disponibles et gérer la sélection.
#     Lors d'un POST, stocke les stations sélectionnées en session et redirige vers la route de traitement.
#     """
#     stations_by_bassin_for_selection = get_stations_with_data('before')

#     all_available_stations = [station for sublist in stations_by_bassin_for_selection.values() for station in sublist]

#     # --- CORRECTION ICI ---
#     # Récupérer la liste des stations récemment mises à jour depuis la session.
#     # Si la clé n'existe pas dans la session (par exemple, au premier chargement de la page),
#     # cela retournera une liste vide.
#     recently_uploaded_stations = session.get('recently_uploaded_stations', [])
#     # ----------------------

#     if request.method == 'POST':
#         selected_stations = request.form.getlist('selected_stations')
#         if not selected_stations:
#             flash(_('Veuillez sélectionner au moins une station.'), 'warning')
#             return render_template('select_stations.html',
#                                     stations_available=all_available_stations,
#                                     recently_uploaded_stations=recently_uploaded_stations, # Passer la liste récupérée de la session
#                                     stations_by_bassin=stations_by_bassin_for_selection,
#                                     custom_station_colors=CUSTOM_STATION_COLORS) # Assurez-vous de passer tout ce qui est nécessaire

#         # Stocker les stations sélectionnées dans la session
#         session['selected_stations_for_processing'] = selected_stations

#         # Rediriger vers la route qui va réellement déclencher le traitement.
#         return redirect(url_for('process_selected_stations'))

#     return render_template('select_stations.html',
#                             stations_available=all_available_stations,
#                             recently_uploaded_stations=recently_uploaded_stations, # Passer la liste récupérée de la session
#                             stations_by_bassin=stations_by_bassin_for_selection,
#                             custom_station_colors=CUSTOM_STATION_COLORS)

# @app.route('/process_selected_stations', methods=['POST'])
# def process_selected_stations():
#     selected_stations = request.form.getlist('selected_stations')

#     if not selected_stations:
#         flash(_('Veuillez sélectionner au moins une station.'), 'danger')
#         return redirect(url_for('select_stations'))

#     processing_results = []

#     # Initialiser les connexions à None en dehors de la boucle
#     # Chaque type de données aura sa propre connexion distincte
#     conn_after = None
#     conn_missing_before = None
#     conn_missing_after = None

#     try:
#         df_gps = _load_and_prepare_gps_data()
#         if df_gps.empty:
#             flash(_("Erreur: Impossible de charger les données GPS des stations. Le traitement ne peut pas continuer."), 'danger')
#             return redirect(url_for('select_stations'))

#         # Établir les connexions pour chaque type de base de données
#         conn_after = get_connection('after')
#         conn_missing_before = get_connection('missing_before')
#         conn_missing_after = get_connection('missing_after')

#         # Vérifier que toutes les connexions ont été établies avec succès
#         if not conn_after:
#             flash(_(f'Erreur: Impossible de se connecter à la base de données des données traitées (after).'), 'danger')
#             return redirect(url_for('select_stations'))
#         if not conn_missing_before:
#             flash(_(f'Erreur: Impossible de se connecter à la base de données des plages manquantes avant (missing_before).'), 'danger')
#             return redirect(url_for('select_stations'))
#         if not conn_missing_after:
#             flash(_(f'Erreur: Impossible de se connecter à la base de données des plages manquantes après (missing_after).'), 'danger')
#             return redirect(url_for('select_stations'))


#         for station_name in selected_stations:
#             logging.info(f"Début du traitement pour la station: {station_name}")
#             try:
#                 df_raw = load_raw_station_data(station_name, processing_type='before')
#                 if df_raw.empty:
#                     flash(_('Aucune donnée brute trouvée pour la station %s.') % station_name, 'warning')
#                     processing_results.append({'station': station_name, 'status': 'failed', 'message': _('Aucune donnée brute.')})
#                     continue

#                 if 'Station' not in df_raw.columns:
#                     df_raw['Station'] = station_name
#                 if not isinstance(df_raw.index, pd.DatetimeIndex):
#                     if 'Datetime' in df_raw.columns:
#                         df_raw = df_raw.set_index('Datetime')
#                     else:
#                         raise ValueError(f"Colonne 'Datetime' manquante pour la station {station_name}")

#                 df_before_interp, df_after_interp, missing_before, missing_after = \
#                     interpolation(df_raw, DATA_LIMITS, df_gps)

#                 if df_after_interp.empty:
#                     flash(_('Le pipeline d\'interpolation n\'a retourné aucune donnée pour la station %s.') % station_name, 'warning')
#                     processing_results.append({'station': station_name, 'status': 'failed', 'message': _('Aucune donnée interpolée retournée.')})
#                     continue

#                 # --- APPELS CORRIGÉS POUR save_to_database ---
#                 # Chaque appel utilise la connexion appropriée
#                 save_main_data_success = save_to_database(df_after_interp, station_name, conn_after, processing_type='after')
#                 save_missing_before_success = save_to_database(missing_before, station_name, conn_missing_before, processing_type='missing_before')
#                 save_missing_after_success = save_to_database(missing_after, station_name, conn_missing_after, processing_type='missing_after')

#                 if save_main_data_success and save_missing_before_success and save_missing_after_success:
#                     flash(_('Traitement et sauvegarde réussis pour la station %s, y compris les données manquantes.') % station_name, 'success')
#                     processing_results.append({'station': station_name, 'status': 'success', 'message': _('Traitement réussi.')})
#                 elif save_main_data_success: # Les données principales ont été sauvegardées, mais les données manquantes ont pu échouer
#                     flash(_('Traitement réussi pour la station %s, mais certaines données manquantes n\'ont pas pu être sauvegardées.') % station_name, 'warning')
#                     processing_results.append({'station': station_name, 'status': 'partial_success', 'message': _('Traitement réussi, échec partiel de sauvegarde des données manquantes.')})
#                 else: # La sauvegarde des données principales a échoué
#                     flash(_('Échec de la sauvegarde pour la station %s. Voir les logs du serveur pour plus de détails.') % station_name, 'danger')
#                     processing_results.append({'station': station_name, 'status': 'failed', 'message': _('Échec de la sauvegarde.')})

#                 logging.info(f"Missing ranges BEFORE interpolation for {station_name}:\n{missing_before.to_string()}")
#                 logging.info(f"Missing ranges AFTER interpolation for {station_name}:\n{missing_after.to_string()}")

#             except Exception as e:
#                 logging.error(f"Erreur lors du traitement de la station {station_name}: {e}", exc_info=True)
#                 flash(_('Erreur lors du traitement de la station %s: %s') % (station_name, str(e)), 'danger')
#                 processing_results.append({'station': station_name, 'status': 'failed', 'message': str(e)})

#     except Exception as e:
#         logging.error(f"Erreur générale lors du chargement des données GPS ou de l'initialisation: {e}", exc_info=True)
#         flash(_('Une erreur inattendue est survenue lors de la préparation du traitement: %s') % str(e), 'danger')
#         processing_results.append({'station': 'Global', 'status': 'failed', 'message': _('Erreur critique générale: %s') % str(e)})

#     finally:
#         # Fermer toutes les connexions qui ont été ouvertes
#         if conn_after:
#             conn_after.close()
#             app.logger.info("Connexion 'after' à la base de données fermée.")
#         if conn_missing_before:
#             conn_missing_before.close()
#             app.logger.info("Connexion 'missing_before' à la base de données fermée.")
#         if conn_missing_after:
#             conn_missing_after.close()
#             app.logger.info("Connexion 'missing_after' à la base de données fermée.")

#     return render_template('processing_summary.html', results=processing_results)
#    return render_template('preprocessing.html.html')

####################  code fonctionnant ###################
##################### Fin Jeudi 24 Jui 2025 #####################





##################### Dimanche 27 Juillet 2025 #####################

from db import get_station_columns, load_station_data

# Variable GLOBALE pour stocker les stations traitées avec succès pour les analyses ultérieures
# IMPORTANT : Dans Flask, pour les variables globales qui doivent persister à travers les requêtes
# et être modifiables, il est préférable d'utiliser `app.config` ou un mécanisme de cache.
# L'utilisation d'une simple variable globale Python peut causer des problèmes dans un environnement
# multi-threadé ou multi-processus (comme Gunicorn).
# app.config est sûr car il est géré par l'application Flask elle-même.
app.config['PROCESSED_STATIONS_FOR_VIZ_GLOBAL'] = []


@app.route('/select_stations', methods=['GET', 'POST'])
def select_stations():
    """
    Route pour afficher les stations disponibles et gérer la sélection.
    Lors d'un POST, stocke les stations sélectionnées en session et redirige vers la route de traitement.
    """
    stations_by_bassin_for_selection = get_stations_with_data('raw')

    all_available_stations = [station for sublist in stations_by_bassin_for_selection.values() for station in sublist]

    recently_uploaded_stations = session.get('recently_uploaded_stations', [])

    if request.method == 'POST':
        selected_stations = request.form.getlist('selected_stations')
        if not selected_stations:
            flash(_('Veuillez sélectionner au moins une station.'), 'warning')
            return render_template('select_stations.html',
                                    stations_available=all_available_stations,
                                    recently_uploaded_stations=recently_uploaded_stations,
                                    stations_by_bassin=stations_by_bassin_for_selection,
                                    custom_station_colors=CUSTOM_STATION_COLORS)

        session['selected_stations_for_processing'] = selected_stations # Stocker temporairement pour la route de traitement

        return redirect(url_for('process_selected_stations')) # Rediriger vers la route renommée

    return render_template('select_stations.html',
                            stations_available=all_available_stations,
                            recently_uploaded_stations=recently_uploaded_stations,
                            stations_by_bassin=stations_by_bassin_for_selection,
                            custom_station_colors=CUSTOM_STATION_COLORS)

   # selected_stations = request.form.getlist('selected_stations')

# ROUTE RENOMMÉE et adaptée pour utiliser la variable globale
@app.route('/process_selected_stations', methods=['POST'])
def process_selected_stations():
    # Récupérer les stations sélectionnées à partir de la session temporaire
    #selected_stations = session.pop('selected_stations_for_processing_temp', []) # Utiliser .pop pour nettoyer la session après usage
    selected_stations = request.form.getlist('selected_stations')
    if not selected_stations:
        flash(_('Aucune station sélectionnée pour le traitement.'), 'danger')
        return redirect(url_for('select_stations'))

    # Réinitialiser la variable globale pour cette nouvelle exécution
    app.config['PROCESSED_STATIONS_FOR_VIZ_GLOBAL'] = []
    processed_stations_successful_current_run = [] # Liste pour contenir les noms des stations traitées avec succès pour cette exécution

    # Initialiser les connexions à None en dehors de la boucle
    conn_before = None
    conn_after = None
    conn_missing_before = None
    conn_missing_after = None

    try:
        df_gps = GLOBAL_GPS_DATA_DF # Utiliser les données GPS globales
        if df_gps.empty:
            flash(_("Erreur: Impossible de charger les données GPS des stations. Le traitement ne peut pas continuer."), 'danger')
            return redirect(url_for('select_stations'))

        conn_before = get_connection('before')  
        conn_after = get_connection('after')
        conn_missing_before = get_connection('missing_before')
        conn_missing_after = get_connection('missing_after')

        if not conn_before or  not conn_after or not conn_missing_before or not conn_missing_after:
            flash(_(f'Erreur: Impossible d\'établir toutes les connexions aux bases de données nécessaires.'), 'danger')
            return redirect(url_for('select_stations'))


        for station_name in selected_stations:
            logging.info(f"Début du traitement pour la station: {station_name}")
            try:
                # Utiliser load_station_data pour charger les données brutes
                df_raw = load_station_data(station_name, processing_type='raw')
                if df_raw.empty:
                    flash(_('Aucune donnée brute trouvée pour la station %s.') % station_name, 'warning')
                    continue

                if 'Station' not in df_raw.columns:
                    df_raw['Station'] = station_name
                # S'assurer que Datetime est l'index pour la fonction d'interpolation
                if not isinstance(df_raw.index, pd.DatetimeIndex):
                    if 'Datetime' in df_raw.columns:
                        df_raw = df_raw.set_index('Datetime')
                    else:
                        raise ValueError(f"Colonne 'Datetime' manquante pour la station {station_name}")


                df_before_interp_temp, df_after_interp_temp, missing_before_temp, missing_after_temp = \
                    interpolation(df_raw, DATA_LIMITS, df_gps)

                if df_after_interp_temp.empty:
                    flash(_('Le pipeline d\'interpolation n\'a retourné aucune donnée pour la station %s.') % station_name, 'warning')
                    continue
                
                save_before_processing_success = save_to_database(df_before_interp_temp, station_name, conn_before, processing_type='before')
                save_main_data_success = save_to_database(df_after_interp_temp, station_name, conn_after, processing_type='after')
                save_missing_before_success = save_to_database(missing_before_temp, station_name, conn_missing_before, processing_type='missing_before')
                save_missing_after_success = save_to_database(missing_after_temp, station_name, conn_missing_after, processing_type='missing_after')

                if save_before_processing_success and save_main_data_success and save_missing_before_success and save_missing_after_success:
                    flash(_('Traitement et sauvegarde réussis pour la station %s, y compris les données manquantes.') % station_name, 'success')
                    processed_stations_successful_current_run.append(station_name) # Ajouter à la liste des stations réussies de cette exécution
                elif save_main_data_success: # Les données principales ont été sauvegardées, mais les données manquantes ont pu échouer
                    flash(_('Traitement réussi pour la station %s, mais certaines données manquantes n\'ont pas pu être sauvegardées.') % station_name, 'warning')
                    processed_stations_successful_current_run.append(station_name) # Considérer le succès partiel comme "suffisamment réussi" pour la viz
                else:
                    flash(_('Échec de la sauvegarde pour la station %s. Voir les logs du serveur pour plus de détails.') % station_name, 'danger')

                logging.info(f"Plages manquantes AVANT interpolation pour {station_name}:\n{missing_before_temp.to_string()}")
                logging.info(f"Plages manquantes APRÈS interpolation pour {station_name}:\n{missing_after_temp.to_string()}")

            except Exception as e:
                logging.error(f"Erreur lors du traitement de la station {station_name}: {e}", exc_info=True)
                flash(_('Erreur lors du traitement de la station %s: %s') % (station_name, str(e)), 'danger')

    except Exception as e:
        logging.error(f"Erreur générale lors du chargement des données GPS ou de l'initialisation: {e}", exc_info=True)
        flash(_('Une erreur inattendue est survenue lors de la préparation du traitement: %s') % str(e), 'danger')

    finally:
        if conn_before:
            conn_before.close()
            app.logger.info("Connexion 'before_processing_db' à la base de données fermée.")
        # Fermer toutes les connexions qui ont été ouvertes
        if conn_after:
            conn_after.close()
            app.logger.info("Connexion 'after_processing_db' à la base de données fermée.")
        if conn_missing_before:
            conn_missing_before.close()
            app.logger.info("Connexion 'missing_ranges_before_db' à la base de données fermée.")
        if conn_missing_after:
            conn_missing_after.close()
            app.logger.info("Connexion 'missing_ranges_after_db' à la base de données fermée.")

    # Mettre à jour la variable globale avec la liste des stations traitées avec succès
    app.config['PROCESSED_STATIONS_FOR_VIZ_GLOBAL'] = processed_stations_successful_current_run

    if not processed_stations_successful_current_run:
            flash(_("Aucune station n'a été traitée avec succès pour la visualisation."), 'warning')
            # If no stations were processed, redirect to select_stations or a summary page without viz
            return redirect(url_for('select_stations')) # Or a more generic summary page

    # Rediriger directement vers la page de visualisation
    return redirect(url_for('visualiser_resultats_pretraitement'))









########### Fin Dimanche 27 Juillet 2025 ##########

from  data_processing import (
    # generer_diagrammes_circulaires_donnees_manquantes, # Nouvelle fonction
    # generer_diagrammes_batons_outliers, # Nouvelle fonction
    # generer_graphique_chronologie_lacunes, # Nouvelle fonction
#     generate_plot_stats_over_period_plotly,
#     generer_diagrammes_circulaires_donnees_manquantes,
#     generer_diagrammes_batons_outliers,
#     generer_graphique_chronologie_lacunes,
visualize_missing_data,
visualize_outliers,
visualize_missing_ranges,
# visualize_variable_missing_ranges,
calculate_outliers,
_apply_limits_and_coercions
 )



# Variable GLOBALE pour stocker les stations traitées avec succès pour les analyses ultérieures
app.config['PROCESSED_STATIONS_FOR_VIZ_GLOBAL'] = []
# app.py - Ajout des nouvelles routes et modifications
# @app.route('/visualiser_resultats_pretraitement')
# def visualiser_resultats_pretraitement():
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
    
#     if not processed_stations:
#         flash(_("Aucune donnée à visualiser"), 'warning')
#         return redirect(url_for('index'))

#     visualizations = {}
    
#     for station in processed_stations:
#         try:
#             # Charger les données
#             df_before = load_station_data(station, 'before')
#             df_after = load_station_data(station, 'after')
            
#             if df_before.empty or df_after.empty:
#                 continue

#             # Charger les plages manquantes (tolère les tables inexistantes)
#             missing_before = load_station_data(station, 'missing_before')
#             missing_after = load_station_data(station, 'missing_after')

#             # Calcul des outliers
#             outliers_before = calculate_outliers(df_before)
#             outliers_after = calculate_outliers(df_after)

#             # Génération des visualisations
#             figs = {
#                 'missing': visualize_missing_data(df_before, df_after, station),
#                 'outliers': visualize_outliers(outliers_before, outliers_after, station),
#                 'ranges': visualize_missing_ranges(
#                     missing_after if not missing_after.empty else missing_before, 
#                     station)
#             }

#             visualizations[station] = {k: v.to_json() for k, v in figs.items()}

#         except Exception as e:
#             logging.error(f"Erreur visualisation {station}: {str(e)}")
#             continue

#     return render_template('preprocessing.html',
#                          visualizations=visualizations,
#                          stations=processed_stations)

# @app.route('/visualiser_resultats_pretraitement')
# def visualiser_resultats_pretraitement():
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
    
#     if not processed_stations:
#         flash(_("Aucune station traitée disponible pour la visualisation"), 'warning')
#         return redirect(url_for('select_stations'))

#     visualizations = {}
    
#     for station in processed_stations:
#         try:
#             # Chargement des données
#             df_before = load_station_data(station, 'before')
#             df_after = load_station_data(station, 'after')
            
#             if df_before.empty or df_after.empty:
#                 app.logger.warning(f"Données vides pour {station}")
#                 continue

#             # Chargement des plages manquantes
#             missing_before = load_station_data(station, 'missing_before')
#             missing_after = load_station_data(station, 'missing_after')

#             # Calcul des outliers
#             outliers_before = calculate_outliers(df_before)
#             outliers_after = calculate_outliers(df_after)

#             # Génération des figures
#             figs = {
#                 'missing': visualize_missing_data(df_before, df_after, station),
#                 'outliers': visualize_outliers(outliers_before, outliers_after, station),
#                 'ranges': visualize_missing_ranges(
#                     missing_after if not missing_after.empty else missing_before, 
#                     station
#                 )
#             }

#             # Vérification des figures
#             if not all(isinstance(fig, go.Figure) for fig in figs.values()):
#                 app.logger.warning(f"Figures invalides générées pour {station}")
#                 continue

#             visualizations[station] = {
#                 'missing': figs['missing'].to_json(),
#                 'outliers': figs['outliers'].to_json(),
#                 'ranges': figs['ranges'].to_json() if not missing_after.empty else None
#             }

#         except Exception as e:
#             app.logger.error(f"Erreur génération visualisation {station}: {str(e)}", exc_info=True)
#             continue

#     if not visualizations:
#         flash(_("Aucune visualisation n'a pu être générée"), 'error')
#         return redirect(url_for('select_stations'))

#     return render_template('preprocessing.html',
#                          visualizations=visualizations,
#                          stations=list(visualizations.keys()))


# @app.route('/visualiser_resultats_pretraitement')
# def visualiser_resultats_pretraitement():
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
    
#     if not processed_stations:
#         flash(_("Aucune station disponible pour visualisation"), 'warning')
#         return redirect(url_for('select_stations'))

#     station_selected = request.args.get('station', processed_stations[0])
#     variable_selected = request.args.get('variable')

#     # Vérifier que la station sélectionnée est valide
#     if station_selected not in processed_stations:
#         station_selected = processed_stations[0]

#     try:
#         df_before = load_station_data(station_selected, 'before')
#         df_after = load_station_data(station_selected, 'after')
        
#         if df_before.empty or df_after.empty:
#             raise ValueError("Données vides")

#         # Récupérer les variables disponibles
#         EXCLUDED_VARIABLES = [
#             'Datetime', 'Date', 'Year', 'Month', 'Day', 
#             'Hour', 'Minute', 'Is_Daylight', 'sunrise_time_utc',
#             'sunset_time_utc', 'Daylight_Duration', 'Station'
#         ]
#         available_variables = [
#             col for col in df_before.columns 
#             if col not in EXCLUDED_VARIABLES and pd.api.types.is_numeric_dtype(df_before[col])
#         ]

#         # Sélectionner une variable par défaut si aucune n'est sélectionnée
#         if not variable_selected or variable_selected not in available_variables:
#             variable_selected = available_variables[0] if available_variables else None

#         # Générer les figures
#         figures = {
#             'missing_before': visualize_missing_data(df_before, df_before, station_selected),
#             'missing_after': visualize_missing_data(df_before, df_after, station_selected),
#             'outliers_before': visualize_outliers(
#                 calculate_outliers(df_before),
#                 calculate_outliers(df_before),
#                 station_selected
#             ),
#             'outliers_after': visualize_outliers(
#                 calculate_outliers(df_before),
#                 calculate_outliers(df_after),
#                 station_selected
#             ),
#             'variable_ranges': visualize_missing_ranges(
#                 load_station_data(station_selected, 'missing_before'),
#                 variable_selected,
#                 station_selected
#             ) if variable_selected else go.Figure()
#         }

#         # Si requête AJAX, renvoyer uniquement les graphiques
#         if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
#             return jsonify({
#                 'missing_before': figures['missing_before'].to_html(
#                     full_html=False, include_plotlyjs=False
#                 ),
#                 'missing_after': figures['missing_after'].to_html(
#                     full_html=False, include_plotlyjs=False
#                 ),
#                 'outliers_before': figures['outliers_before'].to_html(
#                     full_html=False, include_plotlyjs=False
#                 ),
#                 'outliers_after': figures['outliers_after'].to_html(
#                     full_html=False, include_plotlyjs=False
#                 ),
#                 'variable_ranges': figures['variable_ranges'].to_html(
#                     full_html=False, include_plotlyjs=False
#                 ),
#                 'variable_selected': variable_selected
#             })

#         # Requête normale
#         return render_template('preprocessing.html',
#                             stations=processed_stations,
#                             available_variables=available_variables,
#                             station_selected=station_selected,
#                             variable_selected=variable_selected,
#                             missing_before=figures['missing_before'].to_html(
#                                 full_html=False, 
#                                 include_plotlyjs='cdn',
#                                 config={'responsive': True}
#                             ),
#                             missing_after=figures['missing_after'].to_html(
#                                 full_html=False,
#                                 include_plotlyjs=False
#                             ),
#                             outliers_before=figures['outliers_before'].to_html(
#                                 full_html=False,
#                                 include_plotlyjs=False
#                             ),
#                             outliers_after=figures['outliers_after'].to_html(
#                                 full_html=False,
#                                 include_plotlyjs=False
#                             ),
#                             variable_ranges=figures['variable_ranges'].to_html(
#                                 full_html=False,
#                                 include_plotlyjs=False
#                             ))

#     except Exception as e:
#         app.logger.error(f"Erreur génération visualisation: {str(e)}", exc_info=True)
#         if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
#             return jsonify({'error': str(e)}), 500
#         flash(_("Erreur lors de la génération des graphiques"), 'error')
#         return redirect(url_for('select_stations'))
    

# Variables à exclure de la visualisation
# Gardez cette liste cohérente avec vos besoins.
EXCLUDED_VARIABLES = [
    'Datetime', 'Date', 'Year', 'Month', 'Day',
    'Hour', 'Minute', 'Is_Daylight', 'sunrise_time_utc',
    'sunset_time_utc', 'Daylight_Duration',
    'Station' # Ajoutez 'Station' si elle est ajoutée comme colonne dans vos DFs
]

# ... (Vos routes / et /select_stations, /process_selected_stations) ...


# @app.route('/visualiser_resultats_pretraitement', methods=['GET'])
# def visualiser_resultats_pretraitement():
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
    
#     if not processed_stations:
#         flash(_("Aucune station disponible pour visualisation."), 'warning')
#         return redirect(url_for('select_stations'))

#     station_selected = request.args.get('station', processed_stations[0])
    
#     if station_selected not in processed_stations:
#         flash(_("La station sélectionnée n'a pas été traitée ou n'est pas disponible."), 'warning')
#         return redirect(url_for('visualiser_resultats_pretraitement', station=processed_stations[0])) 

#     df_before = load_station_data(station_selected, processing_type='before')
#     df_after = load_station_data(station_selected, processing_type='after')

#     if df_before.empty or df_after.empty:
#         flash(_("Impossible de charger les données 'avant' ou 'après' pour la station %s.") % station_selected, 'danger')
#         return redirect(url_for('select_stations'))

#     available_variables = [
#         col for col in df_after.columns 
#         if pd.api.types.is_numeric_dtype(df_after[col]) and col not in EXCLUDED_VARIABLES
#     ]
#     available_variables.sort() 

#     variable_selected = request.args.get('variable')
#     if variable_selected and variable_selected not in available_variables:
#         flash(_("La variable sélectionnée n'est pas disponible pour cette station."), 'warning')
#         variable_selected = None 
    
#     if not variable_selected and available_variables:
#         variable_selected = available_variables[0] 

#     # Initialiser les variables de plots HTML à None
#     missing_data_plot_html = None
#     outliers_plot_html = None
#     missing_ranges_plot_html = None

#     if variable_selected:
#         try:
#             # --- Préparation des DataFrames/Dictionnaires pour VOS fonctions de visualisation ---
#             # Pour visualize_missing_data et visualize_outliers, nous passons des DFs/dicts
#             # qui contiennent UNIQUEMENT la variable sélectionnée.
            
#             temp_df_before_single_var = pd.DataFrame({
#                 variable_selected: df_before[variable_selected]
#             })
#             temp_df_after_single_var = pd.DataFrame({
#                 variable_selected: df_after[variable_selected]
#             })
            
#             # Calcul des outliers pour la variable unique (appelé sur les DFs à une seule colonne)
#             outliers_before_dict = calculate_outliers(temp_df_before_single_var)
#             outliers_after_dict = calculate_outliers(temp_df_after_single_var)

#             # Charger le DataFrame des plages manquantes complètes et le filtrer pour la station et la variable.
#             df_missing_ranges_full = load_station_data(station_selected, 'missing_before')
#             if not df_missing_ranges_full.empty:
#                 missing_ranges_filtered_df = df_missing_ranges_full[
#                     (df_missing_ranges_full['station'] == station_selected) & 
#                     (df_missing_ranges_full['variable'] == variable_selected)
#                 ].copy()
#             else:
#                 missing_ranges_filtered_df = pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours'])


#             # Générer les figures Plotly
#             # Ces fonctions sont conçues pour gérer les DFs/dicts filtrés.
#             missing_data_fig = visualize_missing_data(temp_df_before_single_var, temp_df_after_single_var, station_selected)
#             outliers_fig = visualize_outliers(outliers_before_dict, outliers_after_dict, station_selected)
            
#             missing_ranges_fig = visualize_missing_ranges(missing_ranges_filtered_df, station_selected) 

#             # Convertir les figures en HTML pour l'inclusion directe dans le template
#             # Le premier graphique inclut 'plotly.js' depuis un CDN, les autres non.
#             missing_data_plot_html = missing_data_fig.to_html(
#                 full_html=False, include_plotlyjs='cdn', config={'responsive': True}
#             )
#             outliers_plot_html = outliers_fig.to_html(
#                 full_html=False, include_plotlyjs=False, config={'responsive': True}
#             )
#             missing_ranges_plot_html = missing_ranges_fig.to_html(
#                 full_html=False, include_plotlyjs=False, config={'responsive': True}
#             )

#         except Exception as e:
#             logging.error(f"Erreur lors de la génération des visualisations pour la station {station_selected}, variable {variable_selected}: {e}", exc_info=True)
#             flash(_("Erreur lors de la génération des visualisations pour %s - %s: %s") % (station_selected, variable_selected, str(e)), 'danger')
#             # Les variables de plots restent None si une erreur survient

#     return render_template('preprocessing.html',
#                            stations=processed_stations,
#                            station_selected=station_selected,
#                            available_variables=available_variables,
#                            variable_selected=variable_selected,
#                            missing_data_plot_html=missing_data_plot_html,
#                            outliers_plot_html=outliers_plot_html,
#                            missing_ranges_plot_html=missing_ranges_plot_html,
#                            )


# @app.route('/visualiser_resultats_pretraitement', methods=['GET'])
# def visualiser_resultats_pretraitement():
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])

#     if not processed_stations:
#         flash(_("Aucune station disponible pour visualisation."), 'warning')
#         return redirect(url_for('select_stations'))

#     station_selected = request.args.get('station', processed_stations[0])

#     if station_selected not in processed_stations:
#         flash(_("La station sélectionnée n'a pas été traitée ou n'est pas disponible."), 'warning')
#         return redirect(url_for('visualiser_resultats_pretraitement', station=processed_stations[0]))

#     df_before = load_station_data(station_selected, processing_type='before')
#     df_after = load_station_data(station_selected, processing_type='after')

#     if df_before.empty or df_after.empty:
#         flash(_("Impossible de charger les données 'avant' ou 'après' pour la station %s.") % station_selected, 'danger')
#         return redirect(url_for('select_stations'))

#     available_variables = [
#         col for col in df_after.columns
#         if pd.api.types.is_numeric_dtype(df_after[col]) and col not in EXCLUDED_VARIABLES
#     ]
#     available_variables.sort()

#     variable_selected = request.args.get('variable')
#     if variable_selected and variable_selected not in available_variables:
#         flash(_("La variable sélectionnée n'est pas disponible pour cette station."), 'warning')
#         variable_selected = None

#     if not variable_selected and available_variables:
#         variable_selected = available_variables[0]

#     # Initialiser les variables de plots HTML à None
#     missing_data_plot_html = None
#     outliers_plot_html = None
#     missing_ranges_plot_html = None

#     if variable_selected:
#         try:
#             # --- Préparation des DataFrames/Dictionnaires pour VOS fonctions de visualisation ---
#             # Pour visualize_missing_data et visualize_outliers, nous passons des DFs/dicts
#             # qui contiennent UNIQUEMENT la variable sélectionnée.

#             temp_df_before_single_var = pd.DataFrame({
#                 variable_selected: df_before[variable_selected]
#             })
#             temp_df_after_single_var = pd.DataFrame({
#                 variable_selected: df_after[variable_selected]
#             })

#             # Calcul des outliers pour la variable unique (appelé sur les DFs à une seule colonne)
#             # outliers_before_dict = calculate_outliers(temp_df_before_single_var, DATA_LIMITS, available_variables)
#             # outliers_after_dict = calculate_outliers(temp_df_after_single_var,DATA_LIMITS, available_variables)
#             # Ici, variable_selected est déjà une chaîne de caractères, nous en faisons une liste à un seul élément.
#             outliers_before_dict = calculate_outliers(temp_df_before_single_var, DATA_LIMITS, [variable_selected])
#             outliers_after_dict = calculate_outliers(temp_df_after_single_var, DATA_LIMITS, [variable_selected])

#             # Charger le DataFrame des plages manquantes complètes et le filtrer pour la station et la variable.
#             df_missing_ranges_full = load_station_data(station_selected, 'missing_before')
#             if not df_missing_ranges_full.empty:
#                 missing_ranges_filtered_df = df_missing_ranges_full[
#                     (df_missing_ranges_full['station'] == station_selected) &
#                     (df_missing_ranges_full['variable'] == variable_selected)
#                 ].copy()
#             else:
#                 missing_ranges_filtered_df = pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours'])


#             # Générer les figures Plotly
#             # Ces fonctions sont conçues pour gérer les DFs/dicts filtrés.
#             missing_data_fig = visualize_missing_data(temp_df_before_single_var, temp_df_after_single_var, station_selected)

#             # Modification ICI : Passez df_before_selected_variable et df_after_selected_variable
#             outliers_fig = visualize_outliers(
#                 outliers_before_dict,
#                 outliers_after_dict,
#                 station_selected,
#                 temp_df_before_single_var, # DataFrame avant traitement pour la variable sélectionnée
#                 temp_df_after_single_var   # DataFrame après traitement pour la variable sélectionnée
#             )

#             missing_ranges_fig = visualize_missing_ranges(missing_ranges_filtered_df, station_selected)

#             # Convertir les figures en HTML pour l'inclusion directe dans le template
#             # Le premier graphique inclut 'plotly.js' depuis un CDN, les autres non.
#             missing_data_plot_html = missing_data_fig.to_html(
#                 full_html=False, include_plotlyjs='cdn', config={'responsive': True}
#             )
#             outliers_plot_html = outliers_fig.to_html(
#                 full_html=False, include_plotlyjs=False, config={'responsive': True}
#             )
#             missing_ranges_plot_html = missing_ranges_fig.to_html(
#                 full_html=False, include_plotlyjs=False, config={'responsive': True}
#             )

#         except Exception as e:
#             logging.error(f"Erreur lors de la génération des visualisations pour la station {station_selected}, variable {variable_selected}: {e}", exc_info=True)
#             flash(_("Erreur lors de la génération des visualisations pour %s - %s: %s") % (station_selected, variable_selected, str(e)), 'danger')
#             # Les variables de plots restent None si une erreur survient

#     return render_template('preprocessing.html',
#                            stations=processed_stations,
#                            station_selected=station_selected,
#                            available_variables=available_variables,
#                            variable_selected=variable_selected,
#                            missing_data_plot_html=missing_data_plot_html,
#                            outliers_plot_html=outliers_plot_html,
#                            missing_ranges_plot_html=missing_ranges_plot_html,
#                            )


############# code fonctionnant bien pour le  traitement des données et la visualisation

# @app.route('/visualiser_resultats_pretraitement', methods=['GET'])
# def visualiser_resultats_pretraitement():
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])

#     if not processed_stations:
#         flash(_("Aucune station disponible pour visualisation."), 'warning')
#         return redirect(url_for('select_stations'))

#     station_selected = request.args.get('station', processed_stations[0])

#     if station_selected not in processed_stations:
#         flash(_("La station sélectionnée n'a pas été traitée ou n'est pas disponible."), 'warning')
#         return redirect(url_for('visualiser_resultats_pretraitement', station=processed_stations[0]))

#     df_before = load_station_data(station_selected, processing_type='before') # Vraies données brutes
#     df_after = load_station_data(station_selected, processing_type='after')   # Données finales traitées

#     if df_before.empty or df_after.empty:
#         flash(_("Impossible de charger les données 'avant' ou 'après' pour la station %s.") % station_selected, 'danger')
#         return redirect(url_for('select_stations'))

#     available_variables = [
#         col for col in df_after.columns
#         if pd.api.types.is_numeric_dtype(df_after[col]) and col not in EXCLUDED_VARIABLES
#     ]
#     available_variables.sort()

#     variable_selected = request.args.get('variable')
#     if variable_selected and variable_selected not in available_variables:
#         flash(_("La variable sélectionnée n'est pas disponible pour cette station."), 'warning')
#         variable_selected = None

#     if not variable_selected and available_variables:
#         variable_selected = available_variables[0]

#     # Initialiser les variables de plots HTML à None
#     missing_data_plot_html = None
#     outliers_plot_html = None
#     missing_ranges_plot_html = None

#     # Initialiser les dictionnaires d'outliers
#     outliers_before_dict = {}
#     outliers_after_dict = {}

#     if variable_selected:
#         try:
#             # --- Préparation des DataFrames/Dictionnaires pour VOS fonctions de visualisation ---
#             # Pour visualize_missing_data et visualize_outliers, nous passons des DFs/dicts
#             # qui contiennent UNIQUEMENT la variable sélectionnée.

#             temp_df_before_single_var = pd.DataFrame({
#                 variable_selected: df_before[variable_selected]
#             })
#             temp_df_after_single_var = pd.DataFrame({
#                 variable_selected: df_after[variable_selected]
#             })

#             # --- CALCUL DES OUTLIERS ---

#             # 1. Calculer les bornes IQR sur les données BRUTES (temp_df_before_single_var)
#             # Nous appelons calculate_outliers une première fois pour obtenir les bornes de référence
#             # temporairement.
            
#             # Appliquer les limites météo à temp_df_before_single_var avant de calculer les bornes IQR
#             # pour s'assurer que les valeurs hors limites ne faussent pas le calcul initial des bornes.
#             df_before_for_bounds = _apply_limits_and_coercions(temp_df_before_single_var.copy(), DATA_LIMITS, [variable_selected])
#             series_for_bounds = df_before_for_bounds[variable_selected].dropna()

#             if series_for_bounds.nunique() < 2:
#                 # Pas assez de données uniques pour calculer des bornes IQR significatives
#                 lower_bound_ref = None
#                 upper_bound_ref = None
#                 initial_q1 = None
#                 initial_q3 = None
#             else:
#                 initial_q1 = series_for_bounds.quantile(0.25)
#                 initial_q3 = series_for_bounds.quantile(0.75)
#                 initial_iqr = initial_q3 - initial_q1

#                 if initial_iqr == 0:
#                     lower_bound_ref = None
#                     upper_bound_ref = None
#                 else:
#                     lower_bound_ref = initial_q1 - 1.5 * initial_iqr
#                     upper_bound_ref = initial_q3 + 1.5 * initial_iqr


#             # 2. Compter les outliers dans les données BRUTES en utilisant les bornes fraîchement calculées
#             if lower_bound_ref is not None and upper_bound_ref is not None:
#                 count_before = calculate_outliers(
#                     temp_df_before_single_var,
#                     variable_selected,
#                     limits_dict=DATA_LIMITS, # Pass DATA_LIMITS pour la première application des limites
#                     lower_bound_ref=lower_bound_ref,
#                     upper_bound_ref=upper_bound_ref
#                 )
#             else:
#                 count_before = 0 # Pas d'outliers si les bornes ne peuvent pas être calculées

#             outliers_before_dict[variable_selected] = count_before

#             # 3. Compter les outliers dans les données TRAITÉES en utilisant les MÊMES BORNES de référence
#             if lower_bound_ref is not None and upper_bound_ref is not None:
#                 count_after = calculate_outliers(
#                     temp_df_after_single_var, # Utilisez le DataFrame traité
#                     variable_selected,
#                     limits_dict=DATA_LIMITS, # Pass DATA_LIMITS pour la première application des limites
#                     lower_bound_ref=lower_bound_ref, # Utilise les bornes de RÉFÉRENCE
#                     upper_bound_ref=upper_bound_ref  # Utilise les bornes de RÉFÉRENCE
#                 )
#             else:
#                 count_after = 0

#             outliers_after_dict[variable_selected] = count_after


#             # Charger le DataFrame des plages manquantes complètes et le filtrer pour la station et la variable.
#             df_missing_ranges_full = load_station_data(station_selected, 'missing_before')
#             if not df_missing_ranges_full.empty:
#                 missing_ranges_filtered_df = df_missing_ranges_full[
#                     (df_missing_ranges_full['station'] == station_selected) &
#                     (df_missing_ranges_full['variable'] == variable_selected)
#                 ].copy()
#             else:
#                 missing_ranges_filtered_df = pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours'])


#             # Générer les figures Plotly
#             missing_data_fig = visualize_missing_data(temp_df_before_single_var, temp_df_after_single_var, station_selected)
#             outliers_fig = visualize_outliers(
#                 outliers_before_dict,
#                 outliers_after_dict,
#                 station_selected,
#                 temp_df_before_single_var, # Ces DataFrames sont toujours nécessaires pour les totaux dans hovertemplate
#                 temp_df_after_single_var
#             )
#             missing_ranges_fig = visualize_missing_ranges(missing_ranges_filtered_df, station_selected)

#             # Convertir les figures en HTML pour l'inclusion directe dans le template
#             missing_data_plot_html = missing_data_fig.to_html(
#                 full_html=False, include_plotlyjs='cdn', config={'responsive': True}
#             )
#             outliers_plot_html = outliers_fig.to_html(
#                 full_html=False, include_plotlyjs=False, config={'responsive': True}
#             )
#             missing_ranges_plot_html = missing_ranges_fig.to_html(
#                 full_html=False, include_plotlyjs=False, config={'responsive': True}
#             )

#         except Exception as e:
#             logging.error(f"Erreur lors de la génération des visualisations pour la station {station_selected}, variable {variable_selected}: {e}", exc_info=True)
#             flash(_("Erreur lors de la génération des visualisations pour %s - %s: %s") % (station_selected, variable_selected, str(e)), 'danger')
#             # Les variables de plots restent None si une erreur survient

#     return render_template('preprocessing.html',
#                            stations=processed_stations,
#                            station_selected=station_selected,
#                            available_variables=available_variables,
#                            variable_selected=variable_selected,
#                            missing_data_plot_html=missing_data_plot_html,
#                            outliers_plot_html=outliers_plot_html,
#                            missing_ranges_plot_html=missing_ranges_plot_html,
#                            )
################### Fin code fonctionnant bien 


# @app.route('/visualiser_resultats_pretraitement', methods=['GET'])
# def visualiser_resultats_pretraitement():
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])

#     if not processed_stations:
#         flash("Aucune station disponible pour visualisation.", 'warning')
#         return redirect(url_for('select_stations'))

#     station_selected = request.args.get('station', processed_stations[0])

#     if station_selected not in processed_stations:
#         flash("La station sélectionnée n'a pas été traitée ou n'est pas disponible.", 'warning')
#         return redirect(url_for('visualiser_resultats_pretraitement', station=processed_stations[0]))

#     df_before = load_station_data(station_selected, processing_type='before') # Donnees avant le traitement 
#     df_after = load_station_data(station_selected, processing_type='after')   # Données apres le traitement 

#     if df_before.empty or df_after.empty:
#         flash("Impossible de charger les données 'avant' ou 'après' pour la station %s." % station_selected, 'danger')
#         return redirect(url_for('select_stations'))

#     available_variables = [
#         col for col in df_after.columns
#         if pd.api.types.is_numeric_dtype(df_after[col]) and col not in EXCLUDED_VARIABLES
#     ]
#     available_variables.sort()

#     variable_selected = request.args.get('variable')
#     if variable_selected and variable_selected not in available_variables:
#         flash("La variable sélectionnée n'est pas disponible pour cette station.", 'warning')
#         variable_selected = None

#     if not variable_selected and available_variables:
#         variable_selected = available_variables[0]

#     # Initialize plot HTML variables to None
#     missing_data_plot_html = None
#     outliers_plot_html = None
#     missing_ranges_plot_html = None
#     df_after_head_html = None

#     # Initialize outlier dictionaries
#     outliers_before_dict = {}
#     outliers_after_dict = {}

#     if variable_selected:
#         try:
#             # Check if variable_selected exists in df_before and df_after before creating single var DFs
#             # Note: This check only prevents errors for visualize_missing_data and visualize_outliers.
#             # Missing ranges might still be available even if the variable column itself isn't in df_before/after
#             # if your missing range data is recorded independently.
#             perform_missing_and_outlier_plots = True
#             if variable_selected not in df_before.columns:
#                 logging.warning(f"Variable {variable_selected} not found in df_before for station {station_selected}.")
#                 flash(f"La variable '{variable_selected}' n'est pas présente dans les données brutes pour cette station, les graphiques d'aperçu pourraient être affectés.", 'warning')
#                 perform_missing_and_outlier_plots = False

#             if variable_selected and variable_selected not in df_after.columns:
#                 logging.warning(f"Variable {variable_selected} not found in df_after for station {station_selected}.")
#                 flash(f"La variable '{variable_selected}' n'est pas présente dans les données traitées pour cette station, les graphiques d'aperçu pourraient être affectés.", 'warning')
#                 perform_missing_and_outlier_plots = False

#             if perform_missing_and_outlier_plots:
#                 temp_df_before_single_var = pd.DataFrame({
#                     variable_selected: df_before[variable_selected]
#                 })
#                 temp_df_after_single_var = pd.DataFrame({
#                     variable_selected: df_after[variable_selected]
#                 })

#                 # --- CALCUL DES OUTLIERS ---
#                 df_before_for_bounds = _apply_limits_and_coercions(temp_df_before_single_var.copy(), DATA_LIMITS, [variable_selected])
#                 series_for_bounds = df_before_for_bounds[variable_selected].dropna()

#                 lower_bound_ref, upper_bound_ref = None, None
#                 if series_for_bounds.nunique() >= 2:
#                     initial_q1 = series_for_bounds.quantile(0.25)
#                     initial_q3 = series_for_bounds.quantile(0.75)
#                     initial_iqr = initial_q3 - initial_q1
#                     if initial_iqr != 0:
#                         lower_bound_ref = initial_q1 - 1.5 * initial_iqr
#                         upper_bound_ref = initial_q3 + 1.5 * initial_iqr

#                 if lower_bound_ref is not None and upper_bound_ref is not None:
#                     count_before = calculate_outliers(temp_df_before_single_var, variable_selected, limits_dict=DATA_LIMITS, lower_bound_ref=lower_bound_ref, upper_bound_ref=upper_bound_ref)
#                     count_after = calculate_outliers(temp_df_after_single_var, variable_selected, limits_dict=DATA_LIMITS, lower_bound_ref=lower_bound_ref, upper_bound_ref=upper_bound_ref)
#                 else:
#                     count_before = 0
#                     count_after = 0

#                 outliers_before_dict[variable_selected] = count_before
#                 outliers_after_dict[variable_selected] = count_after

#                 # Generate Missing Data and Outlier plots only if perform_missing_and_outlier_plots is True
#                 missing_data_fig = visualize_missing_data(temp_df_before_single_var, temp_df_after_single_var, station_selected)
#                 outliers_fig = visualize_outliers(
#                     outliers_before_dict, outliers_after_dict, station_selected,
#                     temp_df_before_single_var, temp_df_after_single_var
#                 )
#                 missing_data_plot_html = missing_data_fig.to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True})
#                 outliers_plot_html = outliers_fig.to_html(full_html=False, include_plotlyjs=False, config={'responsive': True})
#             else:
#                 flash(f"Les graphiques des données manquantes et des outliers ne sont pas disponibles pour '{variable_selected}' en raison de données manquantes dans le dataset source.", 'info')


#             # Load the full missing ranges DataFrames and filter them for the selected station and variable.
#             # IMPORTANT: Ensure your load_station_data can fetch 'missing_before' and 'missing_after' data
#             # AND that these 'start_time'/'end_time' columns are datetime objects upon loading.
#             df_missing_ranges_before_full = load_station_data(station_selected, 'missing_before')
#             df_missing_ranges_after_full = load_station_data(station_selected, 'missing_after')

#             missing_ranges_filtered_df_before = pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours'])
#             if not df_missing_ranges_before_full.empty:
#                 missing_ranges_filtered_df_before = df_missing_ranges_before_full[
#                     (df_missing_ranges_before_full['station'] == station_selected) &
#                     (df_missing_ranges_before_full['variable'] == variable_selected)
#                 ].copy()

#             missing_ranges_filtered_df_after = pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours'])
#             if not df_missing_ranges_after_full.empty:
#                 missing_ranges_filtered_df_after = df_missing_ranges_after_full[
#                     (df_missing_ranges_after_full['station'] == station_selected) &
#                     (df_missing_ranges_after_full['variable'] == variable_selected)
#                 ].copy()

#             # Generate Missing Ranges Plot
#             # This will only be attempted if variable_selected is valid and missing ranges data is loaded
#             missing_ranges_fig = visualize_missing_ranges(
#                 missing_ranges_filtered_df_before,
#                 missing_ranges_filtered_df_after,
#                 station_selected,
#                 variable_selected
#             )
#             missing_ranges_plot_html = missing_ranges_fig.to_html(full_html=False, include_plotlyjs=False, config={'responsive': True})


#             # Generate HTML for the head of the processed DataFrame
#             if not df_after.empty:
#                 df_after_head_html = df_after.head(20).to_html(classes='table table-striped table-bordered', escape=False)
#             else:
#                 df_after_head_html = "<p>Aucune donnée traitée disponible pour cette station.</p>"

#         except Exception as e:
#             logging.error(f"Erreur lors de la génération des visualisations pour la station {station_selected}, variable {variable_selected}: {e}", exc_info=True)
#             flash("Erreur lors de la génération des visualisations pour %s - %s: %s" % (station_selected, variable_selected, str(e)), 'danger')
#             # If an error occurs, ensure plot HTML variables remain None so the template shows a message

#     return render_template('preprocessing.html',
#                            stations=processed_stations,
#                            station_selected=station_selected,
#                            available_variables=available_variables,
#                            variable_selected=variable_selected,
#                            missing_data_plot_html=missing_data_plot_html,
#                            outliers_plot_html=outliers_plot_html,
#                            missing_ranges_plot_html=missing_ranges_plot_html,
#                            df_after_head_html=df_after_head_html)



############### COde 1er aout

# @app.route('/visualiser_resultats_pretraitement', methods=['GET'])
# def visualiser_resultats_pretraitement():
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
#     current_lang = str(get_locale())

#     if not processed_stations:
#         flash(_l("Aucune station disponible pour visualisation."), 'warning')
#         return redirect(url_for('select_stations'))

#     station_selected = request.args.get('station', processed_stations[0])

#     if station_selected not in processed_stations:
#         flash(_l("La station sélectionnée n'a pas été traitée ou n'est pas disponible."), 'warning')
#         return redirect(url_for('visualiser_resultats_pretraitement', station=processed_stations[0]))

#     df_before = load_station_data(station_selected, processing_type='before')
#     df_after = load_station_data(station_selected, processing_type='after')

#     if df_before.empty or df_after.empty:
#         flash(_l("Impossible de charger les données 'avant' ou 'après' pour la station %s.") % station_selected, 'danger')
#         return redirect(url_for('select_stations'))

#     available_variables = [
#         col for col in df_after.columns
#         if pd.api.types.is_numeric_dtype(df_after[col]) and col not in EXCLUDED_VARIABLES
#     ]
#     available_variables.sort()

#     variable_colors = {var: PALETTE_DEFAUT[var] for var in available_variables if var in PALETTE_DEFAUT}

#     variable_selected = request.args.get('variable')
#     if variable_selected and variable_selected not in available_variables:
#         flash(_l("La variable sélectionnée n'est pas disponible pour cette station."), 'warning')
#         variable_selected = None

#     if not variable_selected and available_variables:
#         variable_selected = available_variables[0]

#     missing_data_plot_html = None
#     outliers_plot_html = None
#     missing_ranges_plot_html = None
#     df_after_head_html = None
#     outliers_before_dict = {}
#     outliers_after_dict = {}
#     record_count_after = df_after.shape[0] if not df_after.empty else 0

#     if variable_selected:
#         try:
#             perform_missing_and_outlier_plots = True
#             if variable_selected not in df_before.columns:
#                 logging.warning(f"Variable {variable_selected} not found in df_before for station {station_selected}.")
#                 flash(_l(f"La variable '{METADATA_VARIABLES.get(variable_selected, {}).get('Nom', {}).get(current_lang[:2], variable_selected)}' n'est pas présente dans les données brutes pour cette station, les graphiques d'aperçu pourraient être affectés."), 'warning')
#                 perform_missing_and_outlier_plots = False

#             if variable_selected and variable_selected not in df_after.columns:
#                 logging.warning(f"Variable {variable_selected} not found in df_after for station {station_selected}.")
#                 flash(_l(f"La variable '{METADATA_VARIABLES.get(variable_selected, {}).get('Nom', {}).get(current_lang[:2], variable_selected)}' n'est pas présente dans les données traitées pour cette station, les graphiques d'aperçu pourraient être affectés."), 'warning')
#                 perform_missing_and_outlier_plots = False

#             if perform_missing_and_outlier_plots:
#                 temp_df_before_single_var = pd.DataFrame({
#                     variable_selected: df_before[variable_selected]
#                 })
#                 temp_df_after_single_var = pd.DataFrame({
#                     variable_selected: df_after[variable_selected]
#                 })

#                 df_before_for_bounds = _apply_limits_and_coercions(temp_df_before_single_var.copy(), DATA_LIMITS, [variable_selected])
#                 series_for_bounds = df_before_for_bounds[variable_selected].dropna()

#                 lower_bound_ref, upper_bound_ref = None, None
#                 if series_for_bounds.nunique() >= 2:
#                     initial_q1 = series_for_bounds.quantile(0.25)
#                     initial_q3 = series_for_bounds.quantile(0.75)
#                     initial_iqr = initial_q3 - initial_q1
#                     if initial_iqr != 0:
#                         lower_bound_ref = initial_q1 - 1.5 * initial_iqr
#                         upper_bound_ref = initial_q3 + 1.5 * initial_iqr

#                 if lower_bound_ref is not None and upper_bound_ref is not None:
#                     count_before = calculate_outliers(temp_df_before_single_var, variable_selected, limits_dict=DATA_LIMITS, lower_bound_ref=lower_bound_ref, upper_bound_ref=upper_bound_ref)
#                     count_after = calculate_outliers(temp_df_after_single_var, variable_selected, limits_dict=DATA_LIMITS, lower_bound_ref=lower_bound_ref, upper_bound_ref=upper_bound_ref)
#                 else:
#                     count_before = 0
#                     count_after = 0

#                 outliers_before_dict[variable_selected] = count_before
#                 outliers_after_dict[variable_selected] = count_after

#                 missing_data_fig = visualize_missing_data(temp_df_before_single_var, temp_df_after_single_var, station_selected)
#                 outliers_fig = visualize_outliers(
#                     outliers_before_dict, outliers_after_dict, station_selected,
#                     temp_df_before_single_var, temp_df_after_single_var
#                 )
#                 missing_data_plot_html = missing_data_fig.to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True})
#                 outliers_plot_html = outliers_fig.to_html(full_html=False, include_plotlyjs=False, config={'responsive': True})
#             else:
#                 flash(_l(f"Les graphiques des données manquantes et des outliers ne sont pas disponibles pour '{METADATA_VARIABLES.get(variable_selected, {}).get('Nom', {}).get(current_lang[:2], variable_selected)}' en raison de données manquantes dans le dataset source ou traité."), 'info')

#             df_missing_ranges_before_full = load_station_data(station_selected, 'missing_before')
#             df_missing_ranges_after_full = load_station_data(station_selected, 'missing_after')

#             missing_ranges_filtered_df_before = pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours'])
#             if not df_missing_ranges_before_full.empty:
#                 missing_ranges_filtered_df_before = df_missing_ranges_before_full[
#                     (df_missing_ranges_before_full['station'] == station_selected) &
#                     (df_missing_ranges_before_full['variable'] == variable_selected)
#                 ].copy()

#             missing_ranges_filtered_df_after = pd.DataFrame(columns=['station', 'variable', 'start_time', 'end_time', 'duration_hours'])
#             if not df_missing_ranges_after_full.empty:
#                 missing_ranges_filtered_df_after = df_missing_ranges_after_full[
#                     (df_missing_ranges_after_full['station'] == station_selected) &
#                     (df_missing_ranges_after_full['variable'] == variable_selected)
#                 ].copy()

#             missing_ranges_fig = visualize_missing_ranges(
#                 missing_ranges_filtered_df_before,
#                 missing_ranges_filtered_df_after,
#                 station_selected,
#                 variable_selected
#             )
#             missing_ranges_plot_html = missing_ranges_fig.to_html(full_html=False, include_plotlyjs=False, config={'responsive': True})

#             if not df_after.empty:
#                 df_to_display = df_after.head(20).copy()
                
#                 # Gestion de l'index datetime
#                 datetime_col = None
#                 if isinstance(df_to_display.index, pd.DatetimeIndex):
#                     df_to_display = df_to_display.reset_index()
#                     if 'index' in df_to_display.columns:
#                         datetime_col = 'index'
#                 elif 'Datetime' in df_to_display.columns:
#                     datetime_col = 'Datetime'
                
#                 # Formatage de la colonne datetime
#                 if datetime_col:
#                     df_to_display[datetime_col] = pd.to_datetime(df_to_display[datetime_col])
                    
#                     if all(col in df_to_display.columns for col in ['Year', 'Month', 'Day', 'Hour', 'Minute']):
#                         df_to_display[datetime_col] = df_to_display[datetime_col].dt.strftime('%Y-%m-%d %H:%M')
#                     elif 'Date' in df_to_display.columns:
#                         df_to_display[datetime_col] = pd.to_datetime(df_to_display['Date']).dt.strftime('%Y-%m-%d')
#                     else:
#                         if not df_to_display[datetime_col].dt.time.eq(pd.Timestamp('00:00:00').time()).all():
#                             df_to_display[datetime_col] = df_to_display[datetime_col].dt.strftime('%Y-%m-%d %H:%M')
#                         else:
#                             df_to_display[datetime_col] = df_to_display[datetime_col].dt.strftime('%Y-%m-%d')
                    
#                     df_to_display = df_to_display.rename(columns={datetime_col: 'Datetime'})

#                 # Localisation des noms de colonnes
#                 display_column_names = {}
#                 for col in df_to_display.columns:
#                     if col in METADATA_VARIABLES:
#                         display_column_names[col] = str(get_var_label(METADATA_VARIABLES[col], 'Nom'))
#                     else:
#                         translations = {
#                             'Datetime': _('Date/Heure'),
#                             'Date': _('Date'),
#                             'Year': _('Année'),
#                             'Month': _('Mois'),
#                             'Day': _('Jour'),
#                             'Hour': _('Heure'),
#                             'Minute': _('Minute')
#                         }
#                         display_column_names[col] = translations.get(col, col)
                
#                 df_to_display = df_to_display.rename(columns=display_column_names)
                
#                 # Génération du HTML du tableau
#                 df_after_head_html = df_to_display.to_html(
#                     classes='table table-striped table-bordered',
#                     index=False,
#                     escape=False,
#                     border=0
#                 )
#             else:
#                 df_after_head_html = f"<p>{_('Aucune donnée traitée disponible pour cette station.')}</p>"

#         except Exception as e:
#             logging.error(f"Erreur lors de la génération des visualisations pour la station {station_selected}, variable {variable_selected}: {e}", exc_info=True)
#             flash(_l("Erreur lors de la génération des visualisations pour %s - %s: %s") % (station_selected, variable_selected, str(e)), 'danger')

#     return render_template('preprocessing.html',
#         stations=processed_stations,
#         station_selected=station_selected,
#         available_variables=available_variables,
#         variable_selected=variable_selected,
#         variable_colors=variable_colors,
#         METADATA_VARIABLES=METADATA_VARIABLES,
#         missing_data_plot_html=missing_data_plot_html,
#         outliers_plot_html=outliers_plot_html,
#         missing_ranges_plot_html=missing_ranges_plot_html,
#         df_after_head_html=df_after_head_html,
#         record_count=record_count_after)




# @app.route('/visualiser_resultats_pretraitement', methods=['GET'])
# def visualiser_resultats_pretraitement():
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
#     current_lang = str(get_locale())

#     if not processed_stations:
#         flash(_l("Aucune station disponible pour visualisation."), 'warning')
#         return redirect(url_for('select_stations'))

#     station_selected = request.args.get('station', processed_stations[0])
#     if station_selected not in processed_stations:
#         flash(_l("La station sélectionnée n'a pas été traitée ou n'est pas disponible."), 'warning')
#         return redirect(url_for('visualiser_resultats_pretraitement', station=processed_stations[0]))

#     df_before = load_station_data(station_selected, processing_type='before')
#     df_after = load_station_data(station_selected, processing_type='after')
#     if df_before.empty or df_after.empty:
#         flash(_l("Impossible de charger les données 'avant' ou 'après' pour la station %s.") % station_selected, 'danger')
#         return redirect(url_for('select_stations'))

#     available_variables = [
#         col for col in df_after.columns
#         if pd.api.types.is_numeric_dtype(df_after[col]) and col not in EXCLUDED_VARIABLES
#     ]
#     available_variables.sort()

#     variable_colors = {var: PALETTE_DEFAUT[var] for var in available_variables if var in PALETTE_DEFAUT}

#     variable_selected = request.args.get('variable')
#     if variable_selected and variable_selected not in available_variables:
#         flash(_l("La variable sélectionnée n'est pas disponible pour cette station."), 'warning')
#         variable_selected = None
#     if not variable_selected and available_variables:
#         variable_selected = available_variables[0]

#     missing_data_plot_html = None
#     outliers_plot_html = None
#     missing_ranges_plot_html = None
#     df_after_head_html = None
#     outliers_before_dict = {}
#     outliers_after_dict = {}
#     record_count_after = df_after.shape[0] if not df_after.empty else 0

#     if variable_selected:
#         try:
#             # Traitement des données manquantes et outliers (partie inchangée)
#             perform_missing_and_outlier_plots = True
#             if variable_selected not in df_before.columns:
#                 logging.warning(f"Variable {variable_selected} not found in df_before for station {station_selected}.")
#                 flash(_l(f"La variable '{get_var_label(METADATA_VARIABLES, variable_selected)}' n'est pas présente dans les données brutes pour cette station, les graphiques d'aperçu pourraient être affectés."), 'warning')
#                 perform_missing_and_outlier_plots = False
#             if variable_selected not in df_after.columns:
#                 logging.warning(f"Variable {variable_selected} not found in df_after for station {station_selected}.")
#                 flash(_l(f"La variable '{get_var_label(METADATA_VARIABLES, variable_selected)}' n'est pas présente dans les données traitées pour cette station, les graphiques d'aperçu pourraient être affectés."), 'warning')
#                 perform_missing_and_outlier_plots = False

#             if perform_missing_and_outlier_plots:
#                 temp_df_before_single_var = pd.DataFrame({variable_selected: df_before[variable_selected]})
#                 temp_df_after_single_var = pd.DataFrame({variable_selected: df_after[variable_selected]})
#                 df_before_for_bounds = _apply_limits_and_coercions(temp_df_before_single_var.copy(), DATA_LIMITS, [variable_selected])
#                 series_for_bounds = df_before_for_bounds[variable_selected].dropna()
#                 lower_bound_ref, upper_bound_ref = None, None
#                 if series_for_bounds.nunique() >= 2:
#                     initial_q1 = series_for_bounds.quantile(0.25)
#                     initial_q3 = series_for_bounds.quantile(0.75)
#                     initial_iqr = initial_q3 - initial_q1
#                     if initial_iqr != 0:
#                         lower_bound_ref = initial_q1 - 1.5 * initial_iqr
#                         upper_bound_ref = initial_q3 + 1.5 * initial_iqr

#                 if lower_bound_ref is not None and upper_bound_ref is not None:
#                     count_before = calculate_outliers(temp_df_before_single_var, variable_selected, limits_dict=DATA_LIMITS, lower_bound_ref=lower_bound_ref, upper_bound_ref=upper_bound_ref)
#                     count_after = calculate_outliers(temp_df_after_single_var, variable_selected, limits_dict=DATA_LIMITS, lower_bound_ref=lower_bound_ref, upper_bound_ref=upper_bound_ref)
#                 else:
#                     count_before = 0
#                     count_after = 0
#                 outliers_before_dict[variable_selected] = count_before
#                 outliers_after_dict[variable_selected] = count_after

#                 missing_data_fig = visualize_missing_data(temp_df_before_single_var, temp_df_after_single_var, station_selected)
#                 outliers_fig = visualize_outliers(outliers_before_dict, outliers_after_dict, station_selected, temp_df_before_single_var, temp_df_after_single_var)
#                 missing_data_plot_html = missing_data_fig.to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True})
#                 outliers_plot_html = outliers_fig.to_html(full_html=False, include_plotlyjs=False, config={'responsive': True})
#             else:
#                 flash(_l(f"Les graphiques des données manquantes et des outliers ne sont pas disponibles pour '{get_var_label(METADATA_VARIABLES, variable_selected)}' en raison de données manquantes dans le dataset source ou traité."), 'info')

#             df_missing_ranges_before_full = load_station_data(station_selected, 'missing_before')
#             df_missing_ranges_after_full = load_station_data(station_selected, 'missing_after')
#             missing_ranges_fig = visualize_missing_ranges(
#                 df_missing_ranges_before_full[(df_missing_ranges_before_full['station'] == station_selected) & (df_missing_ranges_before_full['variable'] == variable_selected)],
#                 df_missing_ranges_after_full[(df_missing_ranges_after_full['station'] == station_selected) & (df_missing_ranges_after_full['variable'] == variable_selected)],
#                 station_selected, variable_selected
#             )
#             missing_ranges_plot_html = missing_ranges_fig.to_html(full_html=False, include_plotlyjs=False, config={'responsive': True})

#             # --- Début du bloc de code du tableau (corrigé) ---
#             if not df_after.empty:
#                 df_to_display = df_after.head(20).copy()

#                 # Gérer la colonne Datetime en fonction de l'index ou de l'existence d'autres colonnes
#                 if isinstance(df_to_display.index, pd.DatetimeIndex):
#                     df_to_display = df_to_display.reset_index()
#                     df_to_display.rename(columns={'index': 'Datetime'}, inplace=True)
                
#                 if 'Datetime' in df_to_display.columns:
#                     datetime_series = pd.to_datetime(df_to_display['Datetime'])

#                     if all(col in df_after.columns for col in ['Year', 'Month', 'Day', 'Hour', 'Minute']):
#                         df_to_display['Datetime'] = datetime_series.dt.strftime('%Y-%m-%d %H:%M')
#                     elif 'Date' in df_after.columns:
#                         df_to_display['Datetime'] = datetime_series.dt.strftime('%Y-%m-%d')
#                     else:
#                         df_to_display['Datetime'] = datetime_series.dt.strftime('%Y-%m-%d %H:%M:%S')

#                     df_to_display.set_index('Datetime', inplace=True)
#                     df_to_display.index.name = _l('Datetime')
                
#                 # Création du dictionnaire de renommage avec l'appel de la fonction corrigé
#                 display_column_names = {}
#                 for col in df_to_display.columns:
#                     if col in METADATA_VARIABLES:
#                         display_column_names[col] = get_var_label(METADATA_VARIABLES[col], 'Nom')
#                     elif col == 'Station':
#                         display_column_names[col] = _l('Station')
#                     # else:
#                     #     display_column_names[col] = col.replace('_', ' ').title()

#                 df_to_display.rename(columns=display_column_names, inplace=True)
                
#                 df_after_head_html = df_to_display.to_html(
#                     classes='table table-striped table-bordered',
#                     escape=False,
#                     index=True,
#                     #border=0
#                 )
#             else:
#                 df_after_head_html = f"<div class='alert alert-info'>{_l('Aucune donnée traitée disponible pour cette station.')}</div>"
#             # --- Fin du bloc de code du tableau (corrigé) ---

#         except Exception as e:
#             logging.error(f"Erreur lors de la génération des visualisations pour la station {station_selected}, variable {variable_selected}: {e}", exc_info=True)
#             flash(_l("Erreur lors de la génération des visualisations pour %s - %s: %s") % (station_selected, variable_selected, str(e)), 'danger')

#     return render_template('preprocessing.html',
#                            stations=processed_stations,
#                            station_selected=station_selected,
#                            available_variables=available_variables,
#                            variable_selected=variable_selected,
#                            variable_colors=variable_colors,
#                            METADATA_VARIABLES=METADATA_VARIABLES,
#                            missing_data_plot_html=missing_data_plot_html,
#                            outliers_plot_html=outliers_plot_html,
#                            missing_ranges_plot_html=missing_ranges_plot_html,
#                            df_after_head_html=df_after_head_html,
#                            record_count=record_count_after)



@app.route('/visualiser_resultats_pretraitement', methods=['GET'])
def visualiser_resultats_pretraitement():
    processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
    current_lang = str(get_locale())

    if not processed_stations:
        flash(_l("Aucune station disponible pour visualisation."), 'warning')
        return redirect(url_for('select_stations'))

    station_selected = request.args.get('station', processed_stations[0])
    if station_selected not in processed_stations:
        flash(_l("La station sélectionnée n'a pas été traitée ou n'est pas disponible."), 'warning')
        return redirect(url_for('visualiser_resultats_pretraitement', station=processed_stations[0]))

    df_before = load_station_data(station_selected, processing_type='before')
    df_after = load_station_data(station_selected, processing_type='after')
    if df_before.empty or df_after.empty:
        flash(_l("Impossible de charger les données 'avant' ou 'après' pour la station %s.") % station_selected, 'danger')
        return redirect(url_for('select_stations'))

    available_variables = [
        col for col in df_after.columns
        if pd.api.types.is_numeric_dtype(df_after[col]) and col not in EXCLUDED_VARIABLES
    ]
    available_variables.sort()

    variable_colors = {var: PALETTE_DEFAUT[var] for var in available_variables if var in PALETTE_DEFAUT}

    variable_selected = request.args.get('variable')
    if variable_selected and variable_selected not in available_variables:
        flash(_l("La variable sélectionnée n'est pas disponible pour cette station."), 'warning')
        variable_selected = None
    if not variable_selected and available_variables:
        variable_selected = available_variables[0]

    missing_data_plot_html = None
    outliers_plot_html = None
    missing_ranges_plot_html = None
    df_after_head_html = None
    outliers_before_dict = {}
    outliers_after_dict = {}
    record_count_after = df_after.shape[0] if not df_after.empty else 0

    if variable_selected:
        try:
            perform_missing_and_outlier_plots = True
            if variable_selected not in df_before.columns:
                logging.warning(f"Variable {variable_selected} not found in df_before for station {station_selected}.")
                flash(_l(f"La variable '{get_var_label(METADATA_VARIABLES, variable_selected)}' n'est pas présente dans les données brutes pour cette station, les graphiques d'aperçu pourraient être affectés."), 'warning')
                perform_missing_and_outlier_plots = False
            if variable_selected not in df_after.columns:
                logging.warning(f"Variable {variable_selected} not found in df_after for station {station_selected}.")
                flash(_l(f"La variable '{get_var_label(METADATA_VARIABLES, variable_selected)}' n'est pas présente dans les données traitées pour cette station, les graphiques d'aperçu pourraient être affectés."), 'warning')
                perform_missing_and_outlier_plots = False

            if perform_missing_and_outlier_plots:
                temp_df_before_single_var = pd.DataFrame({variable_selected: df_before[variable_selected]})
                temp_df_after_single_var = pd.DataFrame({variable_selected: df_after[variable_selected]})
                df_before_for_bounds = _apply_limits_and_coercions(temp_df_before_single_var.copy(), DATA_LIMITS, [variable_selected])
                series_for_bounds = df_before_for_bounds[variable_selected].dropna()
                lower_bound_ref, upper_bound_ref = None, None
                if series_for_bounds.nunique() >= 2:
                    initial_q1 = series_for_bounds.quantile(0.25)
                    initial_q3 = series_for_bounds.quantile(0.75)
                    initial_iqr = initial_q3 - initial_q1
                    if initial_iqr != 0:
                        lower_bound_ref = initial_q1 - 1.5 * initial_iqr
                        upper_bound_ref = initial_q3 + 1.5 * initial_iqr

                if lower_bound_ref is not None and upper_bound_ref is not None:
                    count_before = calculate_outliers(
                        temp_df_before_single_var,
                        variable_selected,
                        limits_dict=DATA_LIMITS,
                        lower_bound_ref=lower_bound_ref,
                        upper_bound_ref=upper_bound_ref
                    )
                    count_after = calculate_outliers(
                        temp_df_after_single_var,
                        variable_selected,
                        limits_dict=DATA_LIMITS,
                        lower_bound_ref=lower_bound_ref,
                        upper_bound_ref=upper_bound_ref
                    )
                else:
                    count_before = 0
                    count_after = 0
                outliers_before_dict[variable_selected] = count_before
                outliers_after_dict[variable_selected] = count_after

                missing_data_fig = visualize_missing_data(temp_df_before_single_var, temp_df_after_single_var, station_selected)
                outliers_fig = visualize_outliers(
                    outliers_before_dict,
                    outliers_after_dict,
                    station_selected,
                    temp_df_before_single_var,
                    temp_df_after_single_var
                )
                missing_data_plot_html = missing_data_fig.to_html(
                    full_html=False,
                    include_plotlyjs='cdn',
                    config={'responsive': True}
                )
                outliers_plot_html = outliers_fig.to_html(
                    full_html=False,
                    include_plotlyjs=False,
                    config={'responsive': True}
                )
            else:
                flash(_l(
                    f"Les graphiques des données manquantes et des outliers ne sont pas disponibles pour '{get_var_label(METADATA_VARIABLES, variable_selected)}' en raison de données manquantes dans le dataset source ou traité."
                ), 'info')

            df_missing_ranges_before_full = load_station_data(station_selected, 'missing_before')
            df_missing_ranges_after_full = load_station_data(station_selected, 'missing_after')
            missing_ranges_fig = visualize_missing_ranges(
                df_missing_ranges_before_full[
                    (df_missing_ranges_before_full['station'] == station_selected) &
                    (df_missing_ranges_before_full['variable'] == variable_selected)
                ],
                df_missing_ranges_after_full[
                    (df_missing_ranges_after_full['station'] == station_selected) &
                    (df_missing_ranges_after_full['variable'] == variable_selected)
                ],
                station_selected,
                variable_selected
            )
            missing_ranges_plot_html = missing_ranges_fig.to_html(
                full_html=False,
                include_plotlyjs=False,
                config={'responsive': True}
            )

            # --- Bloc du tableau corrigé pour éviter le problème de lazy string sur l'index name ---
            if not df_after.empty:
                df_to_display = df_after.head(20).copy()

                # Gérer la colonne Datetime selon index ou colonnes
                if isinstance(df_to_display.index, pd.DatetimeIndex):
                    df_to_display = df_to_display.reset_index()
                    df_to_display.rename(columns={'index': 'Datetime'}, inplace=True)

                if 'Datetime' in df_to_display.columns:
                    datetime_series = pd.to_datetime(df_to_display['Datetime'])

                    if all(col in df_after.columns for col in ['Year', 'Month', 'Day', 'Hour', 'Minute']):
                        df_to_display['Datetime'] = datetime_series.dt.strftime('%Y-%m-%d %H:%M')
                    elif 'Date' in df_after.columns:
                        df_to_display['Datetime'] = datetime_series.dt.strftime('%Y-%m-%d')
                    else:
                        df_to_display['Datetime'] = datetime_series.dt.strftime('%Y-%m-%d %H:%M:%S')

                    df_to_display.set_index('Datetime', inplace=True)
                    # correction ici : forcer en str pour ne pas décomposer la lazy string
                    df_to_display.index.name = str(_l('Datetime'))

                display_column_names = {}
                for col in df_to_display.columns:
                    if col in METADATA_VARIABLES:
                        display_column_names[col] = get_var_label(METADATA_VARIABLES[col], 'Nom')
                    elif col == 'Station':
                        display_column_names[col] = str(_l('Station'))

                df_to_display.rename(columns=display_column_names, inplace=True)

                df_after_head_html = df_to_display.to_html(
                    classes='table table-striped table-bordered',
                    escape=False,
                    index=True,
                    border=True
                )
            else:
                df_after_head_html = f"<div class='alert alert-info'>{_l('Aucune donnée traitée disponible pour cette station.')}</div>"
            # --- Fin bloc tableau ---

        except Exception as e:
            logging.error(f"Erreur lors de la génération des visualisations pour la station {station_selected}, variable {variable_selected}: {e}", exc_info=True)
            flash(_l("Erreur lors de la génération des visualisations pour %s - %s: %s") % (station_selected, variable_selected, str(e)), 'danger')

    return render_template('preprocessing.html',
                           stations=processed_stations,
                           station_selected=station_selected,
                           available_variables=available_variables,
                           variable_selected=variable_selected,
                           variable_colors=variable_colors,
                           METADATA_VARIABLES=METADATA_VARIABLES,
                           missing_data_plot_html=missing_data_plot_html,
                           outliers_plot_html=outliers_plot_html,
                           missing_ranges_plot_html=missing_ranges_plot_html,
                           df_after_head_html=df_after_head_html,
                           record_count=record_count_after)
















from flask_babel import get_locale

# def get_var_label(meta, key):
#     lang = str(get_locale())
#     return meta[key].get(lang[:2], meta[key].get('en', list(meta[key].values())[0]))



from flask import Flask, render_template, redirect, url_for, flash, send_file
import pandas as pd
import io
from io import BytesIO

@app.route('/download_csv')
def download_csv():
    """Permet de télécharger les données interpolées au format CSV, sans colonnes inutiles."""
    processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])

    if GLOBAL_PROCESSED_DATA_DF.empty:
        flash(_('Aucune donnée disponible pour le téléchargement.'), 'error')
        return redirect(url_for('data_preview'))

    try:
        excluded_columns = ['sunset_time_utc', 'Is_Daylight', 'Daylight_Duration', 'sunrise_time_utc']
        filtered_df = GLOBAL_PROCESSED_DATA_DF.drop(columns=[col for col in excluded_columns if col in GLOBAL_PROCESSED_DATA_DF.columns])

        output = BytesIO()
        filtered_df.to_csv(output, index=False)
        output.seek(0)

        return send_file(
            output,
            mimetype='text/csv',
            download_name='donnees_interpolees.csv',
            as_attachment=True
        )
    except Exception as e:
        app.logger.error(f'Erreur CSV : {str(e)}')
        flash(_('Erreur lors du téléchargement CSV : %s') % str(e), 'error')
        return redirect(url_for('data_preview'))


@app.route('/download_excel')
def download_excel():
    """Permet de télécharger les données interpolées au format Excel, sans colonnes inutiles."""
    if GLOBAL_PROCESSED_DATA_DF.empty:
        flash(_('Aucune donnée disponible pour le téléchargement.'), 'error')
        return redirect(url_for('data_preview'))

    try:
        excluded_columns = ['sunset_time_utc', 'Is_Daylight', 'Daylight_Duration', 'sunrise_time_utc']
        filtered_df = GLOBAL_PROCESSED_DATA_DF.drop(columns=[col for col in excluded_columns if col in GLOBAL_PROCESSED_DATA_DF.columns])

        # Conversion datetime sans timezone
        for col in filtered_df.select_dtypes(include=['datetimetz']).columns:
            filtered_df[col] = filtered_df[col].dt.tz_localize(None)

        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            filtered_df.to_excel(writer, index=False, sheet_name='Donnees_Interpolées')

        output.seek(0)
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            download_name='donnees_interpolees.xlsx',
            as_attachment=True
        )
    except Exception as e:
        app.logger.error(f'Erreur Excel : {str(e)}')
        flash(_('Erreur lors du téléchargement Excel : %s') % str(e), 'error')
        return redirect(url_for('data_preview'))





######################


# @app.route('/visualisations_options')
# def visualisations_options():
#     """Affiche les options pour générer des visualisations."""
#     if GLOBAL_PROCESSED_DATA_DF.empty:
#         # Traduit en français, puis balise
#         flash(_('Veuillez télécharger des fichiers d\'abord pour accéder aux visualisations.'), 'error')
#         return redirect(url_for('index'))

#     # Colonnes à exclure pour les visualisations (non numériques ou non pertinentes)
#     excluded_cols = {'Station', 'Is_Daylight', 'Daylight_Duration',
#                      'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date', 'Rain_01_mm', 'Rain_02_mm'}

#     # Récupère les variables numériques disponibles dans la DataFrame
#     available_vars = [
#         col for col in GLOBAL_PROCESSED_DATA_DF.columns
#         if col not in excluded_cols and pd.api.types.is_numeric_dtype(GLOBAL_PROCESSED_DATA_DF[col])
#     ]

#     # Génère le tableau récapitulatif quotidien
#     daily_stats_df = calculate_daily_summary_table(GLOBAL_PROCESSED_DATA_DF)
#     daily_stats_html = daily_stats_df.to_html(classes='table table-striped', index=False)

#     # Récupère les paramètres de requête pour pré-remplir les champs des formulaires (si redirection)
#     variable_selectionnee = request.args.get('variable')
#     variables_selectionnees = request.args.getlist('variables[]')
#     station_selectionnee = request.args.get('station')
#     periode_selectionnee = request.args.get('periode')

#     return render_template('visualisations_options.html',
#                            stations=sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique()),
#                            variables=sorted(available_vars),
#                            METADATA_VARIABLES=METADATA_VARIABLES,
#                            PALETTE_DEFAUT=PALETTE_DEFAUT,
#                            daily_stats_table=daily_stats_html,
#                            variable_selectionnee=variable_selectionnee,
#                            variables_selectionnees=variables_selectionnees,
#                            station_selectionnee=station_selectionnee,
#                            periode_selectionnee=periode_selectionnee)


# @app.route('/visualisations_options')
# def visualisations_options():
#     return render_template('visualisations_options.html')



## 1er essai ##

# @app.route('/visualisations_options')
# def visualisations_options():
#     """Affiche les options pour générer des visualisations."""
    
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])

#     # 1. Vérifier si des stations ont été traitées
#     if not processed_stations:
#         flash(_('Veuillez d\'abord traiter des données pour accéder aux visualisations.'), 'error')
#         # Il est plus logique de rediriger vers la page de sélection de stations/fichiers
#         return redirect(url_for('select_stations'))

#     # 2. Charger les données d'une station pour déterminer les variables et le résumé
#     # On prend la première station de la liste comme référence.
#     reference_station = processed_stations[0]
#     # Charge les données traitées (type='after') pour la station de référence
#     try:
#         reference_df = load_station_data(reference_station, processing_type='after')
#         if reference_df.empty:
#             flash(_('Impossible de charger les données de la station de référence (%s).') % reference_station, 'error')
#             return redirect(url_for('select_stations'))
#     except Exception as e:
#         flash(_('Erreur lors du chargement des données pour la station de référence (%s): %s') % (reference_station, str(e)), 'error')
#         return redirect(url_for('select_stations'))

#     # 3. Définir les colonnes à exclure pour les visualisations
#     excluded_cols = {'Station', 'Is_Daylight', 'Daylight_Duration',
#                      'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date',
#                      'Rain_01_mm', 'Rain_02_mm'}
    
#     # 4. Récupérer les variables numériques disponibles à partir de la DataFrame de référence
#     available_vars = [
#         col for col in reference_df.columns
#         if col not in excluded_cols and pd.api.types.is_numeric_dtype(reference_df[col])
#     ]

#     # 5. Générer le tableau récapitulatif quotidien.
#     # Il est plus pertinent de le calculer sur toutes les stations traitées.
#     # Pour cela, il faudrait charger toutes les stations. Faisons cela avec une boucle.
#     all_processed_data = []
#     for station in processed_stations:
#         df = load_station_data(station, processing_type='after')
#         if not df.empty:
#             all_processed_data.append(df)
    
#     if all_processed_data:
#         full_df_for_summary = pd.concat(all_processed_data, ignore_index=False)
#         daily_stats_df = calculate_daily_summary_table(full_df_for_summary)
#         daily_stats_html = daily_stats_df.to_html(classes='table table-striped', index=False)
#     else:
#         # Gérer le cas où aucune donnée n'a pu être chargée même après le prétraitement
#         daily_stats_html = _('Aucune donnée disponible pour le résumé.')

#     # 6. Récupérer les paramètres de requête pour pré-remplir les champs des formulaires
#     variable_selectionnee = request.args.get('variable')
#     variables_selectionnees = request.args.getlist('variables[]')
#     station_selectionnee = request.args.get('station')
#     periode_selectionnee = request.args.get('periode')
    
#     return render_template('visualisations_options.html',
#                            stations=sorted(processed_stations),  # Utiliser la liste des stations traitées
#                            variables=sorted(available_vars),
#                            METADATA_VARIABLES=METADATA_VARIABLES,
#                            PALETTE_DEFAUT=PALETTE_DEFAUT,
#                            daily_stats_table=daily_stats_html,
#                            variable_selectionnee=variable_selectionnee,
#                            variables_selectionnees=variables_selectionnees,
#                            station_selectionnee=station_selectionnee,
#                            periode_selectionnee=periode_selectionnee)





# @app.route('/visualisations_options')
# def visualisations_options():
#     """
#     Affiche les options pour générer des visualisations.
#     Utilise le nouveau système de gestion de l'état avec app.config.
#     """
#     # 1. Vérifier si des stations ont été traitées.
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
#     if not processed_stations:
#         flash(_('Veuillez télécharger des fichiers d\'abord pour accéder aux visualisations.'), 'error')
#         return redirect(url_for('index'))
    
#     # 2. Récupérer un DataFrame pour obtenir les colonnes et la table récapitulative.
#     # On choisit la première station traitée pour inspecter les colonnes disponibles,
#     # car on suppose que toutes les stations ont les mêmes colonnes après traitement.
#     # On charge les données traitées ('after').
#     df_sample = load_station_data(processed_stations[0], processing_type='after')
    
#     # Gérer le cas où le chargement de la première station échoue.
#     if df_sample.empty:
#         flash(_('Impossible de charger les données traitées pour la visualisation.'), 'error')
#         return redirect(url_for('select_stations'))

#     # 3. Définir les colonnes à exclure pour les visualisations
#     excluded_cols = {'Station', 'Is_Daylight', 'Daylight_Duration',
#                      'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date', 'Rain_01_mm', 'Rain_02_mm'}

#     # 4. Récupérer les variables numériques disponibles à partir de l'échantillon de DataFrame
#     available_vars = [
#         col for col in df_sample.columns
#         if col not in excluded_cols and pd.api.types.is_numeric_dtype(df_sample[col])
#     ]

#     # 5. Récupérer le DataFrame complet des données traitées pour le tableau récapitulatif
#     # Cette partie est délicate si vous ne voulez pas charger toutes les données en mémoire.
#     # Si la table récapitulative est basée sur les données complètes de TOUTES les stations,
#     # il faudrait soit la charger ici, soit la stocker dans app.config après le traitement.
#     # Je vais supposer pour le moment que `calculate_daily_summary_table` peut prendre
#     # une liste de stations et charger les données une par une si nécessaire,
#     # ou que vous avez une autre méthode.
#     # Pour l'instant, je vais prendre une approche simple qui charge les données de la première station
#     # pour générer le tableau, en notant que cela pourrait être amélioré.
    
#     # Une meilleure approche serait de stocker le daily_stats_df complet après l'upload
#     # et le traitement initial.
    
#     # Supposons ici que 'calculate_daily_summary_table' charge les données nécessaires.
#     # Si cette fonction a besoin d'un DataFrame unique de toutes les stations,
#     # il faudra le construire ici en bouclant sur `processed_stations` et en concaténant les DFs.
    
#     # Example (si vous avez besoin de la concaténation) :
#     # all_processed_dfs = [load_station_data(station, processing_type='after') for station in processed_stations]
#     # all_processed_df = pd.concat(all_processed_dfs, ignore_index=False)
#     # daily_stats_df = calculate_daily_summary_table(all_processed_df)

#     # Pour une version simple, on va prendre les données de la première station comme exemple.
#     # daily_stats_df = calculate_daily_summary_table(df_sample) 
#     # daily_stats_html = daily_stats_df.to_html(classes='table table-striped', index=False)


#     # 6. Récupérer les paramètres de requête pour pré-remplir les champs des formulaires (si redirection)
#     variable_selectionnee = request.args.get('variable')
#     variables_selectionnees = request.args.getlist('variables[]')
#     station_selectionnee = request.args.get('station')
#     periode_selectionnee = request.args.get('periode')

#     return render_template('visualisations_options.html',
#                            stations=sorted(processed_stations),
#                            variables=sorted(available_vars),
#                            METADATA_VARIABLES=METADATA_VARIABLES,
#                            PALETTE_DEFAUT=PALETTE_DEFAUT,
#                            #daily_stats_table=daily_stats_html,
#                            variable_selectionnee=variable_selectionnee,
#                            variables_selectionnees=variables_selectionnees,
#                            station_selectionnee=station_selectionnee,
#                            periode_selectionnee=periode_selectionnee)









# ... (le reste du code est inchangé)



# @app.route('/visualisations_options')
# def visualisations_options():
#     """
#     Affiche les options pour générer des visualisations.
#     Utilise le nouveau système de gestion de l'état avec app.config.
#     """
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
#     if not processed_stations:
#         flash(_('Veuillez télécharger des fichiers d\'abord pour accéder aux visualisations.'), 'error')
#         return redirect(url_for('index'))
    
#     station_selected_for_sample = processed_stations[0]
#     df_sample = load_station_data(station_selected_for_sample, processing_type='after')
    
#     if df_sample.empty:
#         flash(_('Impossible de charger les données traitées pour la visualisation.'), 'error')
#         return redirect(url_for('index'))

#     if 'Station' not in df_sample.columns:
#         if 'Station' in df_sample.index.names:
#             df_sample = df_sample.reset_index(level='Station')
#         else:
#             df_sample['Station'] = station_selected_for_sample
    
#     excluded_cols = {'Station', 'Is_Daylight', 'Daylight_Duration',
#                      'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date', 'Rain_01_mm', 'Rain_02_mm'}

#     available_vars = [
#         col for col in df_sample.columns
#         if col not in excluded_cols and pd.api.types.is_numeric_dtype(df_sample[col])
#     ]

#     # Pour l'instant, on génère le tableau récapitulatif sur l'échantillon de la première station
#     # Une meilleure implémentation pourrait concaténer toutes les stations si nécessaire.
#     # daily_stats_df = calculate_daily_summary_table(df_sample)
#     # daily_stats_html = daily_stats_df.to_html(classes='table table-striped', index=False)

#     variable_selectionnee = request.args.get('variable')
#     variables_selectionnees = request.args.getlist('variables[]')
#     station_selectionnee = request.args.get('station')
#     periode_selectionnee = request.args.get('periode')

#     return render_template('visualisations_options.html',
#                            stations=sorted(processed_stations),
#                            variables=sorted(available_vars),
#                            METADATA_VARIABLES=METADATA_VARIABLES,
#                            PALETTE_DEFAUT=PALETTE_DEFAUT,
#                            #daily_stats_table=daily_stats_html,
#                            variable_selectionnee=variable_selectionnee,
#                            variables_selectionnees=variables_selectionnees,
#                            station_selectionnee=station_selectionnee,
#                            periode_selectionnee=periode_selectionnee)


# @app.route('/statistiques', methods=['GET', 'POST'])
# def statistiques():
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
#     if not processed_stations:
#         flash(_('Aucune donnée disponible'), 'error')
#         return redirect(url_for('index'))

#     try:
#         variable = request.args.get('variable', request.form.get('variable'))
#         if not variable or variable not in METADATA_VARIABLES:
#             flash(_('Variable invalide'), 'error')
#             return redirect(url_for('visualisations_options'))
        
#         # Charger et concaténer les données traitées de toutes les stations
#         all_processed_dfs = [load_station_data(station, processing_type='after') for station in processed_stations]
#         df_processed_full = pd.concat(all_processed_dfs, ignore_index=False)
        
#         # Charger et concaténer les données brutes (si nécessaire)
#         df_before_interpolation_full = pd.DataFrame()
#         try:
#             all_before_dfs = [load_station_data(station, processing_type='before') for station in processed_stations]
#             df_before_interpolation_full = pd.concat(all_before_dfs, ignore_index=False)
#         except Exception as e:
#             app.logger.warning(f"Impossible de charger les données brutes pour les statistiques : {e}")
        
#         var_meta = METADATA_VARIABLES[variable]
#         is_rain = var_meta.get('is_rain', False)
#         stations = sorted(df_processed_full['Station'].unique())
#         station_count = len(stations)

#         context = {
#             'variable_name': get_var_label(var_meta, 'Nom'),
#             'unit': get_var_label(var_meta, 'Unite'),
#             'stations': stations,
#             'variable_selectionnee': variable,
#             'CUSTOM_STATION_COLORS': CUSTOM_STATION_COLORS,
#             'is_rain_variable': is_rain,
#             'plots_html': {},
#             'plots_html_rain_yearly_detailed': None,
#             'plots_html_rain_yearly_summary': []
#         }

#         yearly_plots = generate_plot_stats_over_period_plotly(
#             df=df_processed_full,
#             variable=variable,
#             station_colors=CUSTOM_STATION_COLORS,
#             time_frequency='yearly',
#             df_original=df_before_interpolation_full if not df_before_interpolation_full.empty else None,
#             logger=app.logger
#         )

#         if is_rain and isinstance(yearly_plots, tuple):
#             detailed, summary = yearly_plots

#             if detailed and isinstance(detailed, go.Figure):
#                 detailed.update_layout(
#                     xaxis_title="Années",
#                     yaxis_title="Précipitations (mm)",
#                     plot_bgcolor='white',
#                     xaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                     yaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                     hovermode='closest',
#                     showlegend=True,
#                     legend=dict(
#                         title="Stations",
#                         font=dict(size=12),
#                         orientation="h",
#                         yanchor="bottom",
#                         y=-0.3,
#                         xanchor="center",
#                         x=0.5
#                     )
#                 )
#                 context['plots_html_rain_yearly_detailed'] = detailed.to_html(full_html=False, include_plotlyjs='cdn')

#             if summary and isinstance(summary, go.Figure):
#                 METRIC_CONFIG = {
#                     'Cumul Annuel': {'y_title': 'Précipitations (mm)', 'unit': 'mm'},
#                     'Moyenne Jours Pluvieux': {'y_title': 'Précipitations (mm)', 'unit': 'mm'},
#                     'Moyenne Saison': {'y_title': 'Précipitations (mm)', 'unit': 'mm'},
#                     'Durée Saison': {'y_title': 'Jours', 'unit': 'jours'},
#                     'Jours Sans Pluie': {'y_title': 'Jours', 'unit': 'jours'},
#                     'Durée Sécheresse': {'y_title': 'Jours', 'unit': 'jours'}
#                 }

#                 METRIC_ORDER = list(METRIC_CONFIG.keys())
#                 all_traces = summary.data
#                 traces_per_metric = 3 * station_count

#                 for metric_name in METRIC_ORDER:
#                     config = METRIC_CONFIG[metric_name]
#                     plots = []

#                     for plot_type in ['mean', 'high', 'low']:
#                         fig = go.Figure()
#                         metric_index = METRIC_ORDER.index(metric_name)
#                         base_index = metric_index * traces_per_metric
#                         offset = {'mean': 0, 'high': station_count, 'low': 2 * station_count}[plot_type]

#                         for i in range(station_count):
#                             trace_idx = base_index + offset + i
#                             if trace_idx < len(all_traces):
#                                 trace = all_traces[trace_idx]
#                                 new_trace = go.Bar(
#                                     x=trace.x,
#                                     y=trace.y,
#                                     name=trace.name,
#                                     marker=trace.marker,
#                                     hovertemplate=getattr(trace, 'hovertemplate', None),
#                                     hovertext=getattr(trace, 'hovertext', None),
#                                     customdata=getattr(trace, 'customdata', None)
#                                 )
#                                 fig.add_trace(new_trace)

#                         fig.update_layout(
#                             xaxis_title="Stations",
#                             yaxis_title=config['y_title'],
#                             plot_bgcolor='white',
#                             xaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                             yaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                             hovermode='closest',
#                             showlegend=False,
#                             margin=dict(t=50, b=50, l=80, r=40),
#                             height=400
#                         )

#                         plots.append(fig.to_html(full_html=False, include_plotlyjs='cdn'))

#                     context['plots_html_rain_yearly_summary'].append({
#                         'title': f"{metric_name} ({config['unit']})",
#                         'plots': plots
#                     })

#         else:
#             if yearly_plots and isinstance(yearly_plots, go.Figure):
#                 yearly_plots.update_layout(
#                     xaxis_title="Stations",
#                     yaxis_title=f"{context['variable_name']} ({context['unit']})",
#                     plot_bgcolor='white',
#                     xaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                     yaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                     hovermode='closest',
#                     showlegend=False
#                 )
#                 context['plots_html']['yearly'] = yearly_plots.to_html(full_html=False, include_plotlyjs='cdn')

#         for freq in ['monthly', 'weekly', 'daily']:
#             fig = generate_plot_stats_over_period_plotly(
#                 df=df_processed_full,
#                 variable=variable,
#                 station_colors=CUSTOM_STATION_COLORS,
#                 time_frequency=freq,
#                 logger=app.logger
#             )
#             if fig and isinstance(fig, go.Figure):
#                 fig.update_layout(
#                     xaxis_title="Stations",
#                     yaxis_title=f"{context['variable_name']} ({context['unit']})",
#                     plot_bgcolor='white',
#                     xaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                     yaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                     hovermode='closest',
#                     showlegend=False
#                 )
#                 context['plots_html'][freq] = fig.to_html(full_html=False, include_plotlyjs='cdn')

#         return render_template('statistiques.html', **context)

#     except Exception as e:
#         app.logger.error(f"Erreur: {str(e)}", exc_info=True)
#         flash(_('Erreur technique'), 'error')
#         return redirect(url_for('visualisations_options'))


# @app.route('/generate_plot', methods=['GET', 'POST'])
# def generate_plot():
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
#     if not processed_stations:
#         flash(_('Aucune donnée disponible. Veuillez télécharger des fichiers d\'abord.'), 'error')
#         return redirect(url_for('index'))
#     try:
#         is_comparative = 'comparative' in request.form
#         periode_key = request.form.get('periode')

#         if not periode_key:
#             flash(_('Veuillez sélectionner une période d\'analyse.'), 'error')
#             return redirect(url_for('visualisations_options'))

#         translated_periode = get_period_label(periode_key)

#         if is_comparative:
#             variable = request.form.get('variable')
#             if not variable:
#                 flash(_('Veuillez sélectionner une variable pour la comparaison.'), 'error')
#                 return redirect(url_for('visualisations_options'))
            
#             all_processed_dfs = [load_station_data(station, processing_type='after') for station in processed_stations]
#             df_processed_full = pd.concat(all_processed_dfs, ignore_index=False)
            
#             all_before_dfs = [load_station_data(station, processing_type='before') for station in processed_stations]
#             df_before_interpolation_full = pd.concat(all_before_dfs, ignore_index=False)

#             fig = generer_graphique_comparatif(
#                 df=df_processed_full,
#                 variable=variable,
#                 periode=periode_key,
#                 colors=CUSTOM_STATION_COLORS,
#                 metadata=METADATA_VARIABLES,
#                 before_interpolation_df=df_before_interpolation_full
#             )
            
#             var_meta = METADATA_VARIABLES.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
#             var_label = str(get_var_label(var_meta, 'Nom')) 
            
#             title = _("Comparaison de %(variable_name)s (%(period)s)") % {
#                 'variable_name': var_label,
#                 'period': translated_periode
#             }

#         else:
#             station = request.form.get('station')
#             variables = request.form.getlist('variables[]')

#             if not station:
#                 flash(_('Veuillez sélectionner une station.'), 'error')
#                 return redirect(url_for('visualisations_options'))

#             if not variables:
#                 flash(_('Veuillez sélectionner au moins une variable à visualiser.'), 'error')
#                 return redirect(url_for('visualisations_options'))

#             df_processed_single = load_station_data(station, processing_type='after')
#             df_before_interpolation_single = load_station_data(station, processing_type='before')

#             fig = generer_graphique_par_variable_et_periode(
#                 df=df_processed_single,
#                 station=station,
#                 variables=variables,
#                 periode=periode_key,
#                 colors=PALETTE_DEFAUT,
#                 metadata=METADATA_VARIABLES,
#                 before_interpolation_df=df_before_interpolation_single
#             )
#             title = _("Évolution des variables pour %(station)s (%(period)s)") % {
#                 'station': station,
#                 'period': translated_periode
#             }

#         if not fig or not fig.data:
#             flash(_('Aucune donnée disponible pour les critères sélectionnés. Veuillez affiner votre sélection.'), 'warning')
#             return redirect(url_for('visualisations_options'))

#         plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')
#         return render_template('plot_display.html',
#                             plot_html=plot_html,
#                             title=title)

#     except Exception as e:
#         app.logger.error(f"Erreur lors de la génération du graphique: {str(e)}", exc_info=True)
#         flash(_("Erreur lors de la génération du graphique : %s") % str(e), 'error')
#         return redirect(url_for('visualisations_options'))

# @app.route('/generate_multi_variable_plot_route', methods=['GET', 'POST'])
# def generate_multi_variable_plot_route():
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
#     if not processed_stations:
#         flash(_('Aucune donnée disponible. Veuillez télécharger des fichiers d\'abord.'), 'error')
#         return redirect(url_for('index'))
#     try:
#         station = request.form['station']
#         variables = request.form.getlist('variables[]')
#         periode_key = request.form.get('periode', '')

#         if not station or not variables:
#             flash(_('Veuillez sélectionner une station et au moins une variable pour l\'analyse normalisée.'), 'error')
#             return redirect(url_for('visualisations_options'))
        
#         translated_periode = get_period_label(periode_key)
        
#         df_processed_single = load_station_data(station, processing_type='after')
#         df_before_interpolation_single = load_station_data(station, processing_type='before')

#         fig = generate_multi_variable_station_plot(
#             df=df_processed_single,
#             station=station,
#             variables=variables,
#             periode=periode_key,
#             colors=PALETTE_DEFAUT,
#             metadata=METADATA_VARIABLES,
#             before_interpolation_df=df_before_interpolation_single
#         )
        
#         if not fig or not fig.data:
#             flash(_('Aucune donnée disponible pour les critères sélectionnés. Veuillez affiner votre sélection.'), 'warning')
#             return redirect(url_for('visualisations_options'))

#         plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

#         return render_template('plot_display.html',
#                                plot_html=plot_html,
#                                title=_("Analyse multi-variables normalisée - %(station)s (%(period)s)") % {
#                                    'station': station,
#                                    'period': translated_periode
#                                })

#     except Exception as e:
#         app.logger.error(f"Erreur lors de la génération du graphique normalisé: {str(e)}", exc_info=True)
#         flash(_("Erreur lors de la génération du graphique normalisé : %s") % str(e), 'error')
#         return redirect(url_for('visualisations_options'))



@app.route('/visualisations_options')
def visualisations_options():
    """
    Affiche les options pour générer des visualisations.
    Utilise le nouveau système de gestion de l'état avec app.config.
    """
    processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
    if not processed_stations:
        flash(_('Veuillez télécharger des fichiers d\'abord pour accéder aux visualisations.'), 'error')
        return redirect(url_for('select_stations'))
    
    station_selected_for_sample = processed_stations[0]
    df_sample = load_station_data(station_selected_for_sample, processing_type='after')
    
    if df_sample.empty:
        flash(_('Impossible de charger les données traitées pour la visualisation.'), 'error')
        return redirect(url_for('index'))

    if 'Station' not in df_sample.columns:
        if 'Station' in df_sample.index.names:
            df_sample = df_sample.reset_index(level='Station')
        else:
            df_sample['Station'] = station_selected_for_sample
    
    excluded_cols = {'Station', 'Is_Daylight', 'Daylight_Duration',
                     'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date', 'Rain_01_mm', 'Rain_02_mm'}

    available_vars = [
        col for col in df_sample.columns
        if col not in excluded_cols and pd.api.types.is_numeric_dtype(df_sample[col])
    ]

    # daily_stats_df = calculate_daily_summary_table(df_sample)
    # daily_stats_html = daily_stats_df.to_html(classes='table table-striped', index=False)

    variable_selectionnee = request.args.get('variable')
    variables_selectionnees = request.args.getlist('variables[]')
    station_selectionnee = request.args.get('station')
    periode_selectionnee = request.args.get('periode')

    return render_template('visualisations_options.html',
                           stations=sorted(processed_stations),
                           variables=sorted(available_vars),
                           METADATA_VARIABLES=METADATA_VARIABLES,
                           PALETTE_DEFAUT=PALETTE_DEFAUT,
                           #daily_stats_table=daily_stats_html,
                           variable_selectionnee=variable_selectionnee,
                           variables_selectionnees=variables_selectionnees,
                           station_selectionnee=station_selectionnee,
                           periode_selectionnee=periode_selectionnee)


################## code bien fonctionnnant
@app.route('/statistiques', methods=['GET', 'POST'])
def statistiques():
    processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
    if not processed_stations:
        flash(_('Aucune donnée disponible'), 'error')
        return redirect(url_for('index'))

    try:
        variable = request.args.get('variable', request.form.get('variable'))
        if not variable or variable not in METADATA_VARIABLES:
            flash(_('Variable invalide'), 'error')
            return redirect(url_for('visualisations_options'))
        
        # Charger et ajouter la colonne 'Station' à chaque DF avant de les concaténer
        all_processed_dfs = []
        for station in processed_stations:
            df = load_station_data(station, processing_type='after')
            if not df.empty:
                if 'Station' not in df.columns:
                    df['Station'] = station
                all_processed_dfs.append(df)
        df_processed_full = pd.concat(all_processed_dfs, ignore_index=False)
        
        all_before_dfs = []
        try:
            for station in processed_stations:
                df = load_station_data(station, processing_type='before')
                if not df.empty:
                    if 'Station' not in df.columns:
                        df['Station'] = station
                    all_before_dfs.append(df)
            df_before_interpolation_full = pd.concat(all_before_dfs, ignore_index=False)
        except Exception as e:
            app.logger.warning(f"Impossible de charger les données brutes pour les statistiques : {e}")
            df_before_interpolation_full = pd.DataFrame()
        
        var_meta = METADATA_VARIABLES[variable]
        is_rain = var_meta.get('is_rain', False)
        stations = sorted(df_processed_full['Station'].unique())
        station_count = len(stations)

        context = {
            'variable_name': get_var_label(var_meta, 'Nom'),
            'unit': get_var_label(var_meta, 'Unite'),
            'stations': stations,
            'variable_selectionnee': variable,
            'CUSTOM_STATION_COLORS': CUSTOM_STATION_COLORS,
            'is_rain_variable': is_rain,
            'plots_html': {},
            'plots_html_rain_yearly_detailed': None,
            'plots_html_rain_yearly_summary': []
        }

        yearly_plots = generate_plot_stats_over_period_plotly(
            df=df_processed_full,
            variable=variable,
            station_colors=CUSTOM_STATION_COLORS,
            time_frequency='yearly',
            df_original=df_before_interpolation_full if not df_before_interpolation_full.empty else None,
            logger=app.logger
        )

        if is_rain and isinstance(yearly_plots, tuple):
            detailed, summary = yearly_plots

            if detailed and isinstance(detailed, go.Figure):
                detailed.update_layout(
                    xaxis_title="Années",
                    yaxis_title="Précipitations (mm)",
                    plot_bgcolor='white',
                    xaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
                    yaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
                    hovermode='closest',
                    showlegend=True,
                    legend=dict(
                        title="Stations",
                        font=dict(size=12),
                        orientation="h",
                        yanchor="bottom",
                        y=-0.3,
                        xanchor="center",
                        x=0.5
                    )
                )
                context['plots_html_rain_yearly_detailed'] = detailed.to_html(full_html=False, include_plotlyjs='cdn')

            if summary and isinstance(summary, go.Figure):
                METRIC_CONFIG = {
                    'Cumul Annuel': {'y_title': 'Précipitations (mm)', 'unit': 'mm'},
                    'Moyenne Jours Pluvieux': {'y_title': 'Précipitations (mm)', 'unit': 'mm'},
                    'Moyenne Saison': {'y_title': 'Précipitations (mm)', 'unit': 'mm'},
                    'Durée Saison': {'y_title': 'Jours', 'unit': 'jours'},
                    'Jours Sans Pluie': {'y_title': 'Jours', 'unit': 'jours'},
                    'Durée Sécheresse': {'y_title': 'Jours', 'unit': 'jours'}
                }

                METRIC_ORDER = list(METRIC_CONFIG.keys())
                all_traces = summary.data
                traces_per_metric = 3 * station_count

                for metric_name in METRIC_ORDER:
                    config = METRIC_CONFIG[metric_name]
                    plots = []

                    for plot_type in ['mean', 'high', 'low']:
                        fig = go.Figure()
                        metric_index = METRIC_ORDER.index(metric_name)
                        base_index = metric_index * traces_per_metric
                        offset = {'mean': 0, 'high': station_count, 'low': 2 * station_count}[plot_type]

                        for i in range(station_count):
                            trace_idx = base_index + offset + i
                            if trace_idx < len(all_traces):
                                trace = all_traces[trace_idx]
                                new_trace = go.Bar(
                                    x=trace.x,
                                    y=trace.y,
                                    name=trace.name,
                                    marker=trace.marker,
                                    hovertemplate=getattr(trace, 'hovertemplate', None),
                                    hovertext=getattr(trace, 'hovertext', None),
                                    customdata=getattr(trace, 'customdata', None)
                                )
                                fig.add_trace(new_trace)

                        fig.update_layout(
                            xaxis_title="Stations",
                            yaxis_title=config['y_title'],
                            plot_bgcolor='white',
                            xaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
                            yaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
                            hovermode='closest',
                            showlegend=False,
                            margin=dict(t=50, b=50, l=80, r=40),
                            height=400
                        )

                        plots.append(fig.to_html(full_html=False, include_plotlyjs='cdn'))

                    context['plots_html_rain_yearly_summary'].append({
                        'title': f"{metric_name} ({config['unit']})",
                        'plots': plots
                    })

        else:
            if yearly_plots and isinstance(yearly_plots, go.Figure):
                yearly_plots.update_layout(
                    xaxis_title="Stations",
                    yaxis_title=f"{context['variable_name']} ({context['unit']})",
                    plot_bgcolor='white',
                    xaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
                    yaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
                    hovermode='closest',
                    showlegend=False
                )
                context['plots_html']['yearly'] = yearly_plots.to_html(full_html=False, include_plotlyjs='cdn')

        for freq in ['monthly', 'weekly', 'daily']:
            fig = generate_plot_stats_over_period_plotly(
                df=df_processed_full,
                variable=variable,
                station_colors=CUSTOM_STATION_COLORS,
                time_frequency=freq,
                logger=app.logger
            )
            if fig and isinstance(fig, go.Figure):
                fig.update_layout(
                    xaxis_title="Stations",
                    yaxis_title=f"{context['variable_name']} ({context['unit']})",
                    plot_bgcolor='white',
                    xaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
                    yaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
                    hovermode='closest',
                    showlegend=False
                )
                context['plots_html'][freq] = fig.to_html(full_html=False, include_plotlyjs='cdn')

        return render_template('statistiques.html', **context)

    except Exception as e:
        app.logger.error(f"Erreur: {str(e)}", exc_info=True)
        flash(_('Erreur technique'), 'error')
        return redirect(url_for('visualisations_options'))


@app.route('/generate_plot', methods=['GET', 'POST'])
def generate_plot():
    processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
    if not processed_stations:
        flash(_('Aucune donnée disponible. Veuillez télécharger des fichiers d\'abord.'), 'error')
        return redirect(url_for('index'))
    try:
        is_comparative = 'comparative' in request.form
        periode_key = request.form.get('periode')

        if not periode_key:
            flash(_('Veuillez sélectionner une période d\'analyse.'), 'error')
            return redirect(url_for('visualisations_options'))

        translated_periode = get_period_label(periode_key)

        if is_comparative:
            variable = request.form.get('variable')
            if not variable:
                flash(_('Veuillez sélectionner une variable pour la comparaison.'), 'error')
                return redirect(url_for('visualisations_options'))
            
            all_processed_dfs = []
            for station in processed_stations:
                df = load_station_data(station, processing_type='after')
                if not df.empty:
                    if 'Station' not in df.columns:
                        df['Station'] = station
                    all_processed_dfs.append(df)
            df_processed_full = pd.concat(all_processed_dfs, ignore_index=False)
            
            all_before_dfs = []
            for station in processed_stations:
                df = load_station_data(station, processing_type='before')
                if not df.empty:
                    if 'Station' not in df.columns:
                        df['Station'] = station
                    all_before_dfs.append(df)
            df_before_interpolation_full = pd.concat(all_before_dfs, ignore_index=False)

            fig = generer_graphique_comparatif(
                df=df_processed_full,
                variable=variable,
                periode=periode_key,
                colors=CUSTOM_STATION_COLORS,
                metadata=METADATA_VARIABLES,
                before_interpolation_df=df_before_interpolation_full
            )
            
            var_meta = METADATA_VARIABLES.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
            var_label = str(get_var_label(var_meta, 'Nom')) 
            
            title = _("Comparaison de %(variable_name)s (%(period)s)") % {
                'variable_name': var_label,
                'period': translated_periode
            }

        else:
            station = request.form.get('station')
            variables = request.form.getlist('variables[]')

            if not station:
                flash(_('Veuillez sélectionner une station.'), 'error')
                return redirect(url_for('visualisations_options'))

            if not variables:
                flash(_('Veuillez sélectionner au moins une variable à visualiser.'), 'error')
                return redirect(url_for('visualisations_options'))

            df_processed_single = load_station_data(station, processing_type='after')
            if not df_processed_single.empty and 'Station' not in df_processed_single.columns:
                df_processed_single['Station'] = station
                
            df_before_interpolation_single = load_station_data(station, processing_type='before')
            if not df_before_interpolation_single.empty and 'Station' not in df_before_interpolation_single.columns:
                df_before_interpolation_single['Station'] = station

            fig = generer_graphique_par_variable_et_periode(
                df=df_processed_single,
                station=station,
                variables=variables,
                periode=periode_key,
                colors=PALETTE_DEFAUT,
                metadata=METADATA_VARIABLES,
                before_interpolation_df=df_before_interpolation_single
            )
            title = _("Évolution des variables pour %(station)s (%(period)s)") % {
                'station': station,
                'period': translated_periode
            }

        if not fig or not fig.data:
            flash(_('Aucune donnée disponible pour les critères sélectionnés. Veuillez affiner votre sélection.'), 'warning')
            return redirect(url_for('visualisations_options'))

        plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')
        return render_template('plot_display.html',
                            plot_html=plot_html,
                            title=title)

    except Exception as e:
        app.logger.error(f"Erreur lors de la génération du graphique: {str(e)}", exc_info=True)
        flash(_("Erreur lors de la génération du graphique : %s") % str(e), 'error')
        return redirect(url_for('visualisations_options'))

@app.route('/generate_multi_variable_plot_route', methods=['GET', 'POST'])
def generate_multi_variable_plot_route():
    processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
    if not processed_stations:
        flash(_('Aucune donnée disponible. Veuillez télécharger des fichiers d\'abord.'), 'error')
        return redirect(url_for('index'))
    try:
        station = request.form['station']
        variables = request.form.getlist('variables[]')
        periode_key = request.form.get('periode', '')

        if not station or not variables:
            flash(_('Veuillez sélectionner une station et au moins une variable pour l\'analyse normalisée.'), 'error')
            return redirect(url_for('visualisations_options'))
        
        translated_periode = get_period_label(periode_key)
        
        df_processed_single = load_station_data(station, processing_type='after')
        if not df_processed_single.empty and 'Station' not in df_processed_single.columns:
            df_processed_single['Station'] = station
            
        df_before_interpolation_single = load_station_data(station, processing_type='before')
        if not df_before_interpolation_single.empty and 'Station' not in df_before_interpolation_single.columns:
            df_before_interpolation_single['Station'] = station

        fig = generate_multi_variable_station_plot(
            df=df_processed_single,
            station=station,
            variables=variables,
            periode=periode_key,
            colors=PALETTE_DEFAUT,
            metadata=METADATA_VARIABLES,
            before_interpolation_df=df_before_interpolation_single
        )
        
        if not fig or not fig.data:
            flash(_('Aucune donnée disponible pour les critères sélectionnés. Veuillez affiner votre sélection.'), 'warning')
            return redirect(url_for('visualisations_options'))

        plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

        return render_template('plot_display.html',
                               plot_html=plot_html,
                               title=_("Analyse multi-variables normalisée - %(station)s (%(period)s)") % {
                                   'station': station,
                                   'period': translated_periode
                               })

    except Exception as e:
        app.logger.error(f"Erreur lors de la génération du graphique normalisé: {str(e)}", exc_info=True)
        flash(_("Erreur lors de la génération du graphique normalisé : %s") % str(e), 'error')
        return redirect(url_for('visualisations_options'))






# ################## code bien fonctionnnant
# @app.route('/statistiques', methods=['GET', 'POST'])
# def statistiques():
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
#     if not processed_stations:
#         flash(_('Aucune donnée disponible'), 'error')
#         return redirect(url_for('index'))

#     try:
#         variable = request.args.get('variable', request.form.get('variable'))
#         if not variable or variable not in METADATA_VARIABLES:
#             flash(_('Variable invalide'), 'error')
#             return redirect(url_for('visualisations_options'))
        
#         # Charger et ajouter la colonne 'Station' à chaque DF avant de les concaténer
#         all_processed_dfs = []
#         for station in processed_stations:
#             df = load_station_data(station, processing_type='after')
#             if not df.empty:
#                 if 'Station' not in df.columns:
#                     df['Station'] = station
#                 all_processed_dfs.append(df)
#         df_processed_full = pd.concat(all_processed_dfs, ignore_index=False)
        
#         all_before_dfs = []
#         try:
#             for station in processed_stations:
#                 df = load_station_data(station, processing_type='before')
#                 if not df.empty:
#                     if 'Station' not in df.columns:
#                         df['Station'] = station
#                     all_before_dfs.append(df)
#             df_before_interpolation_full = pd.concat(all_before_dfs, ignore_index=False)
#         except Exception as e:
#             app.logger.warning(f"Impossible de charger les données brutes pour les statistiques : {e}")
#             df_before_interpolation_full = pd.DataFrame()
        
#         var_meta = METADATA_VARIABLES[variable]
#         is_rain = var_meta.get('is_rain', False)
#         stations = sorted(df_processed_full['Station'].unique())
#         station_count = len(stations)

#         context = {
#             'variable_name': get_var_label(var_meta, 'Nom'),
#             'unit': get_var_label(var_meta, 'Unite'),
#             'stations': stations,
#             'variable_selectionnee': variable,
#             'CUSTOM_STATION_COLORS': CUSTOM_STATION_COLORS,
#             'is_rain_variable': is_rain,
#             'plots_html': {},
#             'plots_html_rain_yearly_detailed': None,
#             'plots_html_rain_yearly_summary': []
#         }

#         yearly_plots = generate_plot_stats_over_period_plotly(
#             df=df_processed_full,
#             variable=variable,
#             station_colors=CUSTOM_STATION_COLORS,
#             time_frequency='yearly',
#             df_original=df_before_interpolation_full if not df_before_interpolation_full.empty else None,
#             logger=app.logger
#         )

#         if is_rain and isinstance(yearly_plots, tuple):
#             detailed, summary = yearly_plots

#             if detailed and isinstance(detailed, go.Figure):
#                 detailed.update_layout(
#                     xaxis_title="Années",
#                     yaxis_title="Précipitations (mm)",
#                     plot_bgcolor='white',
#                     xaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                     yaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                     hovermode='closest',
#                     showlegend=True,
#                     legend=dict(
#                         title="Stations",
#                         font=dict(size=12),
#                         orientation="h",
#                         yanchor="bottom",
#                         y=-0.3,
#                         xanchor="center",
#                         x=0.5
#                     )
#                 )
#                 context['plots_html_rain_yearly_detailed'] = detailed.to_html(full_html=False, include_plotlyjs='cdn')

#             if summary and isinstance(summary, go.Figure):
#                 METRIC_CONFIG = {
#                     'Cumul Annuel': {'y_title': 'Précipitations (mm)', 'unit': 'mm'},
#                     'Moyenne Jours Pluvieux': {'y_title': 'Précipitations (mm)', 'unit': 'mm'},
#                     'Moyenne Saison': {'y_title': 'Précipitations (mm)', 'unit': 'mm'},
#                     'Durée Saison': {'y_title': 'Jours', 'unit': 'jours'},
#                     'Jours Sans Pluie': {'y_title': 'Jours', 'unit': 'jours'},
#                     'Durée Sécheresse': {'y_title': 'Jours', 'unit': 'jours'}
#                 }

#                 METRIC_ORDER = list(METRIC_CONFIG.keys())
#                 all_traces = summary.data
#                 traces_per_metric = 3 * station_count

#                 for metric_name in METRIC_ORDER:
#                     config = METRIC_CONFIG[metric_name]
#                     plots = []

#                     for plot_type in ['mean', 'high', 'low']:
#                         fig = go.Figure()
#                         metric_index = METRIC_ORDER.index(metric_name)
#                         base_index = metric_index * traces_per_metric
#                         offset = {'mean': 0, 'high': station_count, 'low': 2 * station_count}[plot_type]

#                         for i in range(station_count):
#                             trace_idx = base_index + offset + i
#                             if trace_idx < len(all_traces):
#                                 trace = all_traces[trace_idx]
#                                 new_trace = go.Bar(
#                                     x=trace.x,
#                                     y=trace.y,
#                                     name=trace.name,
#                                     marker=trace.marker,
#                                     hovertemplate=getattr(trace, 'hovertemplate', None),
#                                     hovertext=getattr(trace, 'hovertext', None),
#                                     customdata=getattr(trace, 'customdata', None)
#                                 )
#                                 fig.add_trace(new_trace)

#                         fig.update_layout(
#                             xaxis_title="Stations",
#                             yaxis_title=config['y_title'],
#                             plot_bgcolor='white',
#                             xaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                             yaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                             hovermode='closest',
#                             showlegend=False,
#                             margin=dict(t=50, b=50, l=80, r=40),
#                             height=400
#                         )

#                         plots.append(fig.to_html(full_html=False, include_plotlyjs='cdn'))

#                     context['plots_html_rain_yearly_summary'].append({
#                         'title': f"{metric_name} ({config['unit']})",
#                         'plots': plots
#                     })

#         else:
#             if yearly_plots and isinstance(yearly_plots, go.Figure):
#                 yearly_plots.update_layout(
#                     xaxis_title="Stations",
#                     yaxis_title=f"{context['variable_name']} ({context['unit']})",
#                     plot_bgcolor='white',
#                     xaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                     yaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                     hovermode='closest',
#                     showlegend=False
#                 )
#                 context['plots_html']['yearly'] = yearly_plots.to_html(full_html=False, include_plotlyjs='cdn')

#         for freq in ['monthly', 'weekly', 'daily']:
#             fig = generate_plot_stats_over_period_plotly(
#                 df=df_processed_full,
#                 variable=variable,
#                 station_colors=CUSTOM_STATION_COLORS,
#                 time_frequency=freq,
#                 logger=app.logger
#             )
#             if fig and isinstance(fig, go.Figure):
#                 fig.update_layout(
#                     xaxis_title="Stations",
#                     yaxis_title=f"{context['variable_name']} ({context['unit']})",
#                     plot_bgcolor='white',
#                     xaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                     yaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                     hovermode='closest',
#                     showlegend=False
#                 )
#                 context['plots_html'][freq] = fig.to_html(full_html=False, include_plotlyjs='cdn')

#         return render_template('statistiques.html', **context)

#     except Exception as e:
#         app.logger.error(f"Erreur: {str(e)}", exc_info=True)
#         flash(_('Erreur technique'), 'error')
#         return redirect(url_for('visualisations_options'))
# from flask import render_template, request, flash, redirect, url_for

############ Fin  du code fonctionnel ###################





###Nouveau essai

# @app.route('/statistiques', methods=['GET', 'POST'])
# def statistiques():
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
#     if not processed_stations:
#         flash(_('Aucune donnée disponible'), 'error')
#         return redirect(url_for('index'))

#     try:
#         variable = request.args.get('variable', request.form.get('variable'))
#         if not variable or variable not in METADATA_VARIABLES:
#             flash(_('Variable invalide'), 'error')
#             return redirect(url_for('visualisations_options'))
        
#         # Charger et ajouter la colonne 'Station' à chaque DF avant de les concaténer
#         all_processed_dfs = []
#         for station in processed_stations:
#             df = load_station_data(station, processing_type='after')
#             if not df.empty:
#                 if 'Station' not in df.columns:
#                     df['Station'] = station
#                 all_processed_dfs.append(df)
#         df_processed_full = pd.concat(all_processed_dfs, ignore_index=False)
        
#         all_before_dfs = []
#         try:
#             for station in processed_stations:
#                 df = load_station_data(station, processing_type='before')
#                 if not df.empty:
#                     if 'Station' not in df.columns:
#                         df['Station'] = station
#                     all_before_dfs.append(df)
#             df_before_interpolation_full = pd.concat(all_before_dfs, ignore_index=False)
#         except Exception as e:
#             app.logger.warning(f"Impossible de charger les données brutes pour les statistiques : {e}")
#             df_before_interpolation_full = pd.DataFrame()
        
#         var_meta = METADATA_VARIABLES[variable]
#         is_rain = var_meta.get('is_rain', False)
#         stations = sorted(df_processed_full['Station'].unique())
#         station_count = len(stations)

#         context = {
#             'variable_name': get_var_label(var_meta, 'Nom'),
#             'unit': get_var_label(var_meta, 'Unite'),
#             'stations': stations,
#             'variable_selectionnee': variable,
#             'CUSTOM_STATION_COLORS': CUSTOM_STATION_COLORS,
#             'is_rain_variable': is_rain,
#             'plots_html': {},
#             'plots_html_rain_yearly_detailed': None,
#             'plots_html_rain_yearly_summary': []
#         }

#         yearly_plots = generate_plot_stats_over_period_plotly(
#             df=df_processed_full,
#             variable=variable,
#             station_colors=CUSTOM_STATION_COLORS,
#             time_frequency='yearly',
#             df_original=df_before_interpolation_full if not df_before_interpolation_full.empty else None,
#             logger=app.logger
#         )

#         if is_rain and isinstance(yearly_plots, tuple):
#             detailed, summary = yearly_plots

#             if detailed and isinstance(detailed, go.Figure):
#                 detailed.update_layout(
#                     xaxis_title="Années",
#                     yaxis_title="Précipitations (mm)",
#                     plot_bgcolor='white',
#                     xaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                     yaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                     hovermode='closest',
#                     showlegend=True,
#                     legend=dict(
#                         title="Stations",
#                         font=dict(size=12),
#                         orientation="h",
#                         yanchor="bottom",
#                         y=-0.3,
#                         xanchor="center",
#                         x=0.5
#                     )
#                 )
#                 context['plots_html_rain_yearly_detailed'] = detailed.to_html(full_html=False, include_plotlyjs='cdn')

#             if summary and isinstance(summary, go.Figure):
#                 METRIC_CONFIG = {
#                     'Cumul Annuel': {'y_title': 'Précipitations (mm)', 'unit': 'mm'},
#                     'Moyenne Jours Pluvieux': {'y_title': 'Précipitations (mm)', 'unit': 'mm'},
#                     'Moyenne Saison': {'y_title': 'Précipitations (mm)', 'unit': 'mm'},
#                     'Durée Saison': {'y_title': 'Jours', 'unit': 'jours'},
#                     'Jours Sans Pluie': {'y_title': 'Jours', 'unit': 'jours'},
#                     'Durée Sécheresse': {'y_title': 'Jours', 'unit': 'jours'}
#                 }

#                 METRIC_ORDER = list(METRIC_CONFIG.keys())
#                 all_traces = summary.data
#                 traces_per_metric = 3 * station_count

#                 for metric_name in METRIC_ORDER:
#                     config = METRIC_CONFIG[metric_name]
#                     plots = []

#                     for plot_type in ['mean', 'high', 'low']:
#                         fig = go.Figure()
#                         metric_index = METRIC_ORDER.index(metric_name)
#                         base_index = metric_index * traces_per_metric
#                         offset = {'mean': 0, 'high': station_count, 'low': 2 * station_count}[plot_type]

#                         for i in range(station_count):
#                             trace_idx = base_index + offset + i
#                             if trace_idx < len(all_traces):
#                                 trace = all_traces[trace_idx]
#                                 new_trace = go.Bar(
#                                     x=trace.x,
#                                     y=trace.y,
#                                     name=trace.name,
#                                     marker=trace.marker,
#                                     hovertemplate=getattr(trace, 'hovertemplate', None),
#                                     hovertext=getattr(trace, 'hovertext', None),
#                                     customdata=getattr(trace, 'customdata', None)
#                                 )
#                                 fig.add_trace(new_trace)

#                         fig.update_layout(
#                             xaxis_title="Stations",
#                             yaxis_title=config['y_title'],
#                             plot_bgcolor='white',
#                             xaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                             yaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                             hovermode='closest',
#                             showlegend=False,
#                             margin=dict(t=50, b=50, l=80, r=40),
#                             height=400
#                         )

#                         plots.append(fig.to_html(full_html=False, include_plotlyjs='cdn'))

#                     context['plots_html_rain_yearly_summary'].append({
#                         'title': f"{metric_name} ({config['unit']})",
#                         'plots': plots
#                     })

#         else:
#             if yearly_plots and isinstance(yearly_plots, go.Figure):
#                 yearly_plots.update_layout(
#                     xaxis_title="Stations",
#                     yaxis_title=f"{context['variable_name']} ({context['unit']})",
#                     plot_bgcolor='white',
#                     xaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                     yaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                     hovermode='closest',
#                     showlegend=False
#                 )
#                 context['plots_html']['yearly'] = yearly_plots.to_html(full_html=False, include_plotlyjs='cdn')

#         for freq in ['monthly', 'weekly', 'daily']:
#             fig = generate_plot_stats_over_period_plotly(
#                 df=df_processed_full,
#                 variable=variable,
#                 station_colors=CUSTOM_STATION_COLORS,
#                 time_frequency=freq,
#                 logger=app.logger
#             )
#             if fig and isinstance(fig, go.Figure):
#                 fig.update_layout(
#                     xaxis_title="Stations",
#                     yaxis_title=f"{context['variable_name']} ({context['unit']})",
#                     plot_bgcolor='white',
#                     xaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                     yaxis=dict(showgrid=False, title_font=dict(size=14), tickfont=dict(size=12)),
#                     hovermode='closest',
#                     showlegend=False
#                 )
#                 context['plots_html'][freq] = fig.to_html(full_html=False, include_plotlyjs='cdn')

#         return render_template('statistiques.html', **context)

#     except Exception as e:
#         app.logger.error(f"Erreur: {str(e)}", exc_info=True)
#         flash(_('Erreur technique'), 'error')
#         return redirect(url_for('visualisations_options'))


# from data_processing import (
#     _ensure_datetime_index, available_years_from_df, available_months_for_year, available_dates_for_hourly, common_years_across_stations, filter_df_by_period
# )
# @app.route('/visualisations_options')
# def visualisations_options():
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
#     if not processed_stations:
#         flash(_('Veuillez télécharger des fichiers d\'abord pour accéder aux visualisations.'), 'error')
#         return redirect(url_for('select_stations'))

#     station_sample = processed_stations[0]
#     df_sample = load_station_data(station_sample, processing_type='after')
#     if df_sample.empty:
#         flash(_('Impossible de charger les données traitées pour la visualisation.'), 'error')
#         return redirect(url_for('visualiser_resultats_pretraitement'))

#     df_sample = _ensure_datetime_index(df_sample)
#     if 'Station' not in df_sample.columns:
#         if 'Station' in df_sample.index.names:
#             df_sample = df_sample.reset_index(level='Station')
#         else:
#             df_sample['Station'] = station_sample

#     #excluded_cols = {'Station', 'Is_Daylight', 'Daylight_Duration', 'Rain_01_mm', 'Rain_02_mm'}
#     excluded_cols = {'Station', 'Is_Daylight', 'Daylight_Duration',
#                      'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date', 'Rain_01_mm', 'Rain_02_mm'}

#     available_vars = [
#         col for col in df_sample.columns
#         if col not in excluded_cols and pd.api.types.is_numeric_dtype(df_sample[col])
#     ]

#     available_vars = [
#         col for col in df_sample.columns
#         if col not in excluded_cols and pd.api.types.is_numeric_dtype(df_sample[col])
#     ]

#     variable_selectionnee = request.args.get('variable')
#     variables_selectionnees = request.args.getlist('variables[]')
#     station_selectionnee = request.args.get('station', station_sample)
#     periode_selectionnee = request.args.get('periode')
#     year_sel = request.args.get('year')
#     month_sel = request.args.get('month')
#     day_sel = request.args.get('day')

#     common_years = common_years_across_stations(processed_stations, processing_type='after')
#     available_years_sample = available_years_from_df(df_sample)
#     available_months_sample = {y: available_months_for_year(df_sample, y) for y in available_years_sample}
#     available_dates_per_station = {
#         station: available_dates_for_hourly(_ensure_datetime_index(load_station_data(station, processing_type='after')))
#         for station in processed_stations
#     }

#     return render_template(
#         'visualisations_options.html',
#         stations=sorted(processed_stations),
#         variables=sorted(available_vars),
#         METADATA_VARIABLES=METADATA_VARIABLES,
#         PALETTE_DEFAUT=PALETTE_DEFAUT,
#         variable_selectionnee=variable_selectionnee,
#         variables_selectionnees=variables_selectionnees,
#         station_selectionnee=station_selectionnee,
#         periode_selectionnee=periode_selectionnee,
#         year_selectionnee=year_sel,
#         month_selectionnee=month_sel,
#         day_selectionnee=day_sel,
#         common_years_comparative=common_years,
#         available_years_sample=available_years_sample,
#         available_months_sample=available_months_sample,
#         available_dates_per_station=available_dates_per_station,
#         babel_locale=str(get_locale())
#     )

# # -- Exemple d'intégration dans generate_plot (multi-variable) --

# @app.route('/generate_plot', methods=['GET', 'POST'])
# def generate_plot():
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
#     if not processed_stations:
#         flash(_('Aucune donnée disponible. Veuillez télécharger des fichiers d\'abord.'), 'error')
#         return redirect(url_for('index'))
#     try:
#         is_comparative = 'comparative' in request.form
#         periode_key = request.form.get('periode')
#         year = request.form.get('year')
#         month = request.form.get('month')
#         day = request.form.get('day')

#         if not periode_key:
#             flash(_('Veuillez sélectionner une période d\'analyse.'), 'error')
#             return redirect(url_for('visualisations_options'))

#         translated_periode = get_period_label(periode_key)

#         if is_comparative:
#             variable = request.form.get('variable')
#             if not variable:
#                 flash(_('Veuillez sélectionner une variable pour la comparaison.'), 'error')
#                 return redirect(url_for('visualisations_options'))

#             # concaténation et filtrage
#             all_processed_dfs = []
#             for station in processed_stations:
#                 df = load_station_data(station, processing_type='after')
#                 if not df.empty:
#                     df = _ensure_datetime_index(df)
#                     if 'Station' not in df.columns:
#                         df['Station'] = station
#                     all_processed_dfs.append(filter_df_by_period(df, periode_key, year=year, month=month, day=day))
#             df_processed_full = pd.concat(all_processed_dfs, ignore_index=False)

#             all_before_dfs = []
#             for station in processed_stations:
#                 df = load_station_data(station, processing_type='before')
#                 if not df.empty:
#                     df = _ensure_datetime_index(df)
#                     if 'Station' not in df.columns:
#                         df['Station'] = station
#                     all_before_dfs.append(filter_df_by_period(df, periode_key, year=year, month=month, day=day))
#             df_before_interpolation_full = pd.concat(all_before_dfs, ignore_index=False)

#             fig = generer_graphique_comparatif(
#                 df=df_processed_full,
#                 variable=variable,
#                 periode=periode_key,
#                 colors=CUSTOM_STATION_COLORS,
#                 metadata=METADATA_VARIABLES,
#                 before_interpolation_df=df_before_interpolation_full
#             )

#             var_meta = METADATA_VARIABLES.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
#             var_label = str(get_var_label(var_meta, 'Nom'))

#             title = _("Comparaison de %(variable_name)s (%(period)s)") % {
#                 'variable_name': var_label,
#                 'period': translated_periode
#             }

#         else:
#             station = request.form.get('station')
#             variables = request.form.getlist('variables[]')
#             if not station:
#                 flash(_('Veuillez sélectionner une station.'), 'error')
#                 return redirect(url_for('visualisations_options'))
#             if not variables:
#                 flash(_('Veuillez sélectionner au moins une variable à visualiser.'), 'error')
#                 return redirect(url_for('visualisations_options'))

#             df_processed_single = load_station_data(station, processing_type='after')
#             df_processed_single = _ensure_datetime_index(df_processed_single)
#             if 'Station' not in df_processed_single.columns:
#                 df_processed_single['Station'] = station
#             df_filtered = filter_df_by_period(df_processed_single, periode_key, year=year, month=month, day=day)

#             df_before_interpolation_single = load_station_data(station, processing_type='before')
#             df_before_interpolation_single = _ensure_datetime_index(df_before_interpolation_single)
#             if 'Station' not in df_before_interpolation_single.columns:
#                 df_before_interpolation_single['Station'] = station
#             df_before_filtered = filter_df_by_period(df_before_interpolation_single, periode_key, year=year, month=month, day=day)

#             fig = generer_graphique_par_variable_et_periode(
#                 df=df_filtered,
#                 station=station,
#                 variables=variables,
#                 periode=periode_key,
#                 colors=PALETTE_DEFAUT,
#                 metadata=METADATA_VARIABLES,
#                 before_interpolation_df=df_before_filtered
#             )
#             title = _("Évolution des variables pour %(station)s (%(period)s)") % {
#                 'station': station,
#                 'period': translated_periode
#             }

#         if not fig or not fig.data:
#             flash(_('Aucune donnée disponible pour les critères sélectionnés. Veuillez affiner votre sélection.'), 'warning')
#             return redirect(url_for('visualisations_options'))

#         plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')
#         return render_template('plot_display.html',
#                                plot_html=plot_html,
#                                title=title)

#     except Exception as e:
#         app.logger.error(f"Erreur lors de la génération du graphique: {str(e)}", exc_info=True)
#         flash(_("Erreur lors de la génération du graphique : %s") % str(e), 'error')
#         return redirect(url_for('visualisations_options'))

# @app.route('/generate_multi_variable_plot_route', methods=['GET', 'POST'])
# def generate_multi_variable_plot_route():
#     processed_stations = app.config.get('PROCESSED_STATIONS_FOR_VIZ_GLOBAL', [])
#     if not processed_stations:
#         flash(_('Aucune donnée disponible. Veuillez télécharger des fichiers d\'abord.'), 'error')
#         return redirect(url_for('index'))

#     try:
#         station = request.form.get('station')
#         variables = request.form.getlist('variables[]')
#         periode_key = request.form.get('periode', '')
#         year = request.form.get('year')
#         month = request.form.get('month')
#         day = request.form.get('day')  # utilisé pour "Horaire"

#         if not station or not variables:
#             flash(_('Veuillez sélectionner une station et au moins une variable pour l\'analyse normalisée.'), 'error')
#             return redirect(url_for('visualisations_options'))

#         translated_periode = get_period_label(periode_key)

#         # Charger les données traitées pour la station (appliquer index datetime)
#         df_processed_single = load_station_data(station, processing_type='after')
#         df_processed_single = _ensure_datetime_index(df_processed_single)
#         if df_processed_single.empty:
#             flash(_('Les données traitées pour la station sélectionnée sont vides.'), 'warning')
#             return redirect(url_for('visualisations_options'))
#         if 'Station' not in df_processed_single.columns:
#             df_processed_single['Station'] = station

#         # Appliquer le filtre de période
#         df_filtered = filter_df_by_period(df_processed_single, periode_key, year=year, month=month, day=day)

#         # Charger les données avant interpolation
#         df_before_interpolation_single = load_station_data(station, processing_type='before')
#         df_before_interpolation_single = _ensure_datetime_index(df_before_interpolation_single)
#         if not df_before_interpolation_single.empty and 'Station' not in df_before_interpolation_single.columns:
#             df_before_interpolation_single['Station'] = station
#         df_before_filtered = filter_df_by_period(df_before_interpolation_single, periode_key, year=year, month=month, day=day) if not df_before_interpolation_single.empty else None

#         # Générer le graphique normalisé
#         fig = generate_multi_variable_station_plot(
#             df=df_filtered,
#             station=station,
#             variables=variables,
#             periode=periode_key,
#             colors=PALETTE_DEFAUT,
#             metadata=METADATA_VARIABLES,
#             before_interpolation_df=df_before_filtered
#         )

#         if not fig or not fig.data:
#             flash(_('Aucune donnée disponible pour les critères sélectionnés. Veuillez affiner votre sélection.'), 'warning')
#             return redirect(url_for('visualisations_options'))

#         plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

#         return render_template(
#             'plot_display.html',
#             plot_html=plot_html,
#             title=_("Analyse multi-variables normalisée - %(station)s (%(period)s)") % {
#                 'station': station,
#                 'period': translated_periode
#             }
#         )

#     except Exception as e:
#         app.logger.error(f"Erreur lors de la génération du graphique normalisé: {str(e)}", exc_info=True)
#         flash(_("Erreur lors de la génération du graphique normalisé : %s") % str(e), 'error')
#         return redirect(url_for('visualisations_options'))









@app.route('/preview')
def data_preview():
    """Affiche un aperçu des données traitées"""
    stations = get_stations_with_data('after')
    if not stations:
        flash(_('Aucune donnée disponible. Veuillez traiter des stations d\'abord.'), 'error')
        return redirect(url_for('index'))
    
    try:
        preview_dfs = []
        for station in stations:
            station_df = get_stations_with_data(station, 'after').head(10)
            preview_dfs.append(station_df)
        
        preview_df = pd.concat(preview_dfs)
        preview_html = preview_df.to_html(
            classes='table table-striped table-hover',
            index=False,
            border=0,
            justify='left',
            na_rep='NaN',
            max_rows=None
        )

        return render_template('preview.html',
                           preview_table=preview_html,
                           preview_type=_("10 lignes × %(stations_count)s stations") % {'stations_count': len(stations)},
                           dataset_shape=_("%(rows)s lignes × %(cols)s colonnes") % {'rows': len(preview_df), 'cols': len(preview_df.columns)},
                           stations_count=len(stations))

    except Exception as e:
        app.logger.error(f'Erreur lors de la préparation de l\'aperçu: {str(e)}', exc_info=True)
        flash(_('Erreur lors de la préparation de l\'aperçu : %s') % str(e), 'error')
        return redirect(url_for('index'))

@app.route('/reset_data', methods=['POST'])
def reset_data():
    """Réinitialise toutes les données traitées"""
    try:
        if reset_processed_data():
            flash(_('Toutes les données traitées ont été réinitialisées avec succès.'), 'success')
        else:
            flash(_('Erreur lors de la réinitialisation des données traitées.'), 'error')
        
        return jsonify(success=True, redirect_url=url_for('index'))
    except Exception as e:
        app.logger.error(f"Erreur critique lors de la réinitialisation des données: {str(e)}", exc_info=True)
        flash(_('Erreur lors de la réinitialisation des données : %s') % str(e), 'error')
        return jsonify(success=False, message=_('Erreur interne lors de la réinitialisation : %s') % str(e), redirect_url=url_for('index')), 500

# Fonctions utilitaires
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.context_processor
def inject_globals():
    return {
        'data_available': len(get_stations_with_data('after')) > 0,
        'now': datetime.now(),
        'babel_locale': str(get_current_locale()),
        'get_stations_list': get_stations_with_data
    }

@app.template_filter('number_format')
def number_format(value):
    try:
        return "{:,}".format(int(value)).replace(",", " ")
    except (ValueError, TypeError):
        return str(value)

if __name__ == '__main__':
    app.run(debug=True)