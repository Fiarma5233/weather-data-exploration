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
    interpolation,
    _load_and_prepare_gps_data,
    gestion_doublons,
    calculate_daily_summary_table,
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
    initialize_database, save_to_database, get_stations_list, 
    get_station_data, delete_station_data, reset_processed_data)

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

# Routes
@app.route('/')
def index():
    """Route pour la page d'accueil avec les deux options"""
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    return render_template('index.html',
                         bassins=sorted(STATIONS_BY_BASSIN.keys()),
                         stations_by_bassin=STATIONS_BY_BASSIN,
                         existing_stations=get_stations_list('before'))

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

@app.route('/select_stations')
def select_stations():
    """Affiche la liste des stations disponibles pour traitement"""
    stations = get_stations_list('before')
    if not stations:
        flash(_('Aucune donnée disponible. Veuillez télécharger des fichiers d\'abord.'), 'error')
        return redirect(url_for('index'))
    return render_template('select_stations.html', stations=stations)


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

    for file, station in zip(uploaded_files, stations):
        if not file or file.filename == '':
            flash(_("Fichier vide reçu."), 'error')
            continue

        if not allowed_file(file.filename):
            flash(_("Type de fichier non autorisé pour '%s'.") % file.filename, 'error')
            continue

        try:
            # Sauvegarde temporaire
            filename = secure_filename(file.filename)
            temp_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(temp_path)
            app.logger.info(f"Fichier {filename} sauvegardé temporairement")

            # Lecture du fichier
            try:
                if filename.lower().endswith('.csv'):
                    df = pd.read_csv(temp_path, encoding_errors='replace', low_memory=False)
                else:  # Fichier Excel
                    df = pd.read_excel(temp_path)

                if df.empty:
                    flash(_("Le fichier '%s' est vide ou corrompu.") % filename, 'error')
                    os.unlink(temp_path)
                    continue

                # Prétraitement spécifique
                df = apply_station_specific_preprocessing(df, station)

                # Vérification finale
                app.logger.info(f"Colonnes avant sauvegarde pour {station}: {df.columns.tolist()}")
                app.logger.info(f"Premières lignes:\n{df.head(2)}")

                # Sauvegarde en base
                try:
                    success = save_to_database(df, station, 'before')
                    if success:
                        flash(_("Données pour %s sauvegardées avec succès!") % station, 'success')
                    else:
                        flash(_("Échec partiel de sauvegarde pour %s") % station, 'warning')
                except Exception as e:
                    app.logger.error(f"Erreur sauvegarde {station}: {str(e)}", exc_info=True)
                    flash(_("Erreur base de données pour %s: %s") % (station, str(e)), 'error')

            except Exception as e:
                app.logger.error(f"Erreur lecture fichier {filename}: {str(e)}", exc_info=True)
                flash(_("Erreur de lecture du fichier '%s': %s") % (filename, str(e)), 'error')
            finally:
                if os.path.exists(temp_path):
                    os.unlink(temp_path)

        except Exception as e:
            app.logger.error(f"Erreur globale traitement {filename}: {str(e)}", exc_info=True)
            flash(_("Erreur traitement fichier '%s'") % file.filename, 'error')
        return render_template('select_stations.html')


@app.route('/process_stations', methods=['POST'])
def process_stations():
    """Traite les stations sélectionnées et sauvegarde les résultats"""
    selected_stations = request.form.getlist('stations')
    if not selected_stations:
        flash(_('Veuillez sélectionner au moins une station.'), 'error')
        return redirect(url_for('select_stations'))

    try:
        for station in selected_stations:
            # Récupérer les données brutes
            raw_df = get_station_data(station, 'before')
            if raw_df is None or raw_df.empty:
                flash(_('Aucune donnée trouvée pour la station %s.') % station, 'warning')
                continue
            
            # Appliquer le traitement
            raw_df = create_datetime(raw_df)
            raw_df = gestion_doublons(raw_df)
            raw_df = raw_df.set_index('Datetime').sort_index()
            
            # Interpolation et autres traitements
            before_interp_df, processed_df, missing_before, missing_after = interpolation(
                raw_df, DATA_LIMITS, GLOBAL_GPS_DATA_DF
            )
            processed_df = traiter_outliers_meteo(processed_df, colonnes=DATA_LIMITS.keys())
            
            # Sauvegarder les données traitées
            save_to_database(processed_df.reset_index(), station, 'after')
        
        flash(_('Traitement terminé pour %d stations.') % len(selected_stations), 'success')
        return redirect(url_for('data_preview'))

    except Exception as e:
        app.logger.error(f"Erreur critique lors du traitement des stations: {str(e)}", exc_info=True)
        flash(_('Erreur critique lors du traitement des données : %s') % str(e), 'error')
        return redirect(url_for('select_stations'))

@app.route('/preview')
def data_preview():
    """Affiche un aperçu des données traitées"""
    stations = get_stations_list('after')
    if not stations:
        flash(_('Aucune donnée disponible. Veuillez traiter des stations d\'abord.'), 'error')
        return redirect(url_for('index'))
    
    try:
        preview_dfs = []
        for station in stations:
            station_df = get_station_data(station, 'after').head(10)
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
        'data_available': len(get_stations_list('after')) > 0,
        'now': datetime.now(),
        'babel_locale': str(get_current_locale()),
        'get_stations_list': get_stations_list
    }

@app.template_filter('number_format')
def number_format(value):
    try:
        return "{:,}".format(int(value)).replace(",", " ")
    except (ValueError, TypeError):
        return str(value)

if __name__ == '__main__':
    app.run(debug=True)