
######### Apres le probleme:

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
from flask_babel import get_locale

# Importations pour Flask-Babel
from flask_babel import Babel, _, lazy_gettext as _l, get_locale as get_current_locale

# Importations des fonctions de traitement
# Assurez-vous que ces fonctions sont correctement définies dans votre module data_processing.py
from data_processing import (
    create_datetime,
    interpolation,
    _load_and_prepare_gps_data,
    gestion_doublons,
    calculate_daily_summary_table,
    generer_graphique_par_variable_et_periode,
    generer_graphique_comparatif,
    generate_multi_variable_station_plot,
    # generate_variable_summary_plots_for_web, # Cette fonction n'était pas définie dans votre data_processing.py récent
    #generate_stats_plots,
    apply_station_specific_preprocessing,
    generate_daily_stats_plot_plotly,
    get_var_label,
    get_period_label,
    traiter_outliers_meteo,
    valeurs_manquantes_viz,
    outliers_viz,
    _get_missing_ranges,
    gaps_time_series_viz
    
)

# Importations de la configuration
# Assurez-vous que ces variables sont correctement définies dans votre module config.py
# IMPORTANT : Les valeurs de METADATA_VARIABLES doivent aussi être balisées si elles sont affichées
# à l'utilisateur. Je les ai balisées dans cet exemple.
from config import METADATA_VARIABLES, PALETTE_DEFAUT, DATA_LIMITS, ALLOWED_EXTENSIONS, STATIONS_BY_BASSIN, CUSTOM_STATION_COLORS

app = Flask(__name__)
# N'oubliez pas de changer cette clé secrète en production!
app.secret_key = 'votre_cle_secrete_ici'
app.config['MAX_CONTENT_LENGTH'] = 1000 * 1024 * 1024 # Limite de taille des fichiers à 64 Mo
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['STATIC_FOLDER'] = 'static'

# --- Configuration et Initialisation de Flask-Babel ---
app.config['BABEL_DEFAULT_LOCALE'] = 'en' # Langue par défaut de l'application (passée de 'en' à 'fr')
# Spécifie le dossier où Flask-Babel trouvera vos fichiers de traduction
app.config['BABEL_TRANSLATION_DIRECTORIES'] = 'translations'

# Initialisation de Babel, en lui passant la fonction get_locale
# Cette ligne doit être AVANT la définition de get_locale()
# Utilisez une lambda pour retarder l'exécution de get_locale()
babel = Babel(app, locale_selector=lambda: select_locale())


# Cette fonction est utilisée par Flask-Babel pour déterminer la locale.
# LE DÉCORATEUR @babel.localeselector A ÉTÉ DÉLIBÉRÉMENT SUPPRIMÉ ICI.
# Il est crucial qu'il ne soit PAS présent au-dessus de cette fonction,
# car la fonction est déjà passée via le paramètre 'locale_selector' ci-dessus.
def select_locale():
    # Vérifie si nous sommes dans un contexte de requête HTTP
    if has_request_context():
        # 1. Vérifie si la langue est explicitement définie dans la session de l'utilisateur
        if 'lang' in session:
            return session['lang']
        # 2. Sinon, utilise la langue préférée envoyée par le navigateur de l'utilisateur
        return request.accept_languages.best_match(['en', 'fr']) # Liste des langues supportées
    
    # Si nous ne sommes pas dans un contexte de requête (par exemple, lors de l'exécution de Babel extract)
    # retourne une langue par défaut pour éviter l'erreur.
    return app.config['BABEL_DEFAULT_LOCALE'] # Ou 'fr' si vous préférez une valeur fixe


# Route pour changer la langue
@app.route('/set_language/<lang>')
def set_language(lang):
    # Vérifie si la langue est supportée avant de la définir
    if lang in ['fr', 'en']:
        session['lang'] = lang
    # Redirige l'utilisateur vers la page d'où il vient, ou vers l'index si l'en-tête Referer n'est pas disponible
    return redirect(request.referrer or url_for('index'))

# --- Fin de la configuration Flask-Babel ---


# Variables globales initialisées comme DataFrames vides
GLOBAL_RAW_DATA_DF = pd.DataFrame() 
GLOBAL_BEFORE_INTERPOLATION_DATA_DF = pd.DataFrame() 
GLOBAL_PROCESSED_DATA_DF = pd.DataFrame() 
GLOBAL_MISSING_VALUES_BEFORE_INTERPOLATION_DF = pd.DataFrame() # Pour stocker les valeurs manquantes avant interpolation
GLOBAL_MISSING_VALUES_AFTER_INTERPOLATION_DF = pd.DataFrame() # Pour stocker les valeurs manquantes après interpolation
GLOBAL_GPS_DATA_DF = pd.DataFrame()


with app.app_context():
    GLOBAL_GPS_DATA_DF = _load_and_prepare_gps_data()

@app.context_processor
def inject_globals():
    return {
        'GLOBAL_RAW_DATA_DF': GLOBAL_RAW_DATA_DF,
        'GLOBAL_BEFORE_INTERPOLATION_DATA_DF': GLOBAL_BEFORE_INTERPOLATION_DATA_DF,
        'GLOBAL_PROCESSED_DATA_DF': GLOBAL_PROCESSED_DATA_DF,
        'GLOBAL_GPS_DATA_DF': GLOBAL_GPS_DATA_DF,
        'data_available': not GLOBAL_PROCESSED_DATA_DF.empty, 
        'now': datetime.now(),
        'babel_locale': str(get_current_locale())
    }

# # Variables globales pour stocker les DataFrames en mémoire
# GLOBAL_PROCESSED_DATA_DF = pd.DataFrame()
# GLOBAL_GPS_DATA_DF = pd.DataFrame()
# GLOBAL_BEFORE_INTERPOLATION_DATA_DF = pd.DataFrame()  # Pour stocker les données avant interpolation    

# # Chargement des données GPS au démarrage de l'application
# # Ce bloc s'exécute dans un contexte d'application, mais PAS de requête.
# # La fonction get_locale() doit donc pouvoir gérer l'absence de 'request' et 'session' à ce moment.
# with app.app_context():
#     GLOBAL_GPS_DATA_DF = _load_and_prepare_gps_data()

# @app.context_processor
# def inject_globals():
#     """Injecte des variables globales et Babel dans le contexte de tous les templates."""
#     return {
#         'GLOBAL_PROCESSED_DATA_DF': GLOBAL_PROCESSED_DATA_DF,
#         'GLOBAL_GPS_DATA_DF': GLOBAL_GPS_DATA_DF,
#         'data_available': not GLOBAL_PROCESSED_DATA_DF.empty, # Indique si des données traitées sont chargées
#         'now': datetime.now(),
#         # Utiliser la fonction get_current_locale() de Flask-Babel
#         'babel_locale': str(get_current_locale()) # Injecte la locale actuelle de Babel pour l'attribut lang des balises HTML
#     }

@app.template_filter('number_format')
def number_format(value):
    """Filtre Jinja pour formater les nombres avec des espaces comme séparateurs de milliers."""
    try:
        # Utilise une locale pour le formatage des nombres si nécessaire, sinon le format général
        return "{:,}".format(int(value)).replace(",", " ")
    except (ValueError, TypeError):
        return str(value)

def allowed_file(filename):
    """Vérifie si l'extension du fichier est autorisée."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    """Route pour la page d'accueil, affichant le formulaire d'upload."""
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True) # S'assure que le dossier d'upload existe
    return render_template('index.html',
                           bassins=sorted(STATIONS_BY_BASSIN.keys()),
                           stations_by_bassin=STATIONS_BY_BASSIN)

# @app.route('/upload', methods=['POST'])
# def upload_file():
#     """Gère l'upload et le traitement des fichiers de données."""
#     global GLOBAL_PROCESSED_DATA_DF

#     if not request.files:
#         # Traduit en français, puis balise
#         flash(_('Aucun fichier reçu.'), 'error')
#         return redirect(url_for('index'))

#     uploaded_files = request.files.getlist('file[]')
#     stations = []
#     # Collecter les noms de station pour chaque fichier
#     for i in range(len(uploaded_files)):
#         station_name = request.form.get(f'station_{i}')
#         if station_name:
#             stations.append(station_name)
#         else:
#             # Traduit en français, puis balise
#             flash(_('Veuillez sélectionner une station pour chaque fichier.'), 'error')
#             return redirect(url_for('index'))

#     if len(uploaded_files) != len(stations):
#         # Traduit en français, puis balise
#         flash(_('Nombre de fichiers et de stations incompatible.'), 'error')
#         return redirect(url_for('index'))

#     batch_dfs = [] # Liste pour stocker les DataFrames de chaque fichier uploadé

#     for file, station in zip(uploaded_files, stations):
#         if not file or not allowed_file(file.filename):
#             # Traduit en français, puis balise (avec interpolation)
#             flash(_("Fichier '%s' invalide ou non autorisé.") % file.filename, 'error')
#             continue # Passe au fichier suivant

#         try:
#             filename = secure_filename(file.filename)
#             temp_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#             file.save(temp_path) # Sauvegarde temporaire du fichier

#             # Lire le fichier selon son extension
#             if filename.lower().endswith('.csv'):
#                 df = pd.read_csv(temp_path, encoding_errors='replace')
#             else: # Supposons .xlsx ou .xls
#                 df = pd.read_excel(temp_path)

#             df['Station'] = station # Ajoute la colonne 'Station'
#             # Correction: apply_station_specific_preprocessing ne prend pas 'bassin' comme argument direct
#             # Il déduit le bassin de la station à l'intérieur de la fonction
#             df = apply_station_specific_preprocessing(df, station) # Applique le prétraitement spécifique à la station
#             batch_dfs.append(df) # Ajoute la DataFrame traitée à la liste

#             os.unlink(temp_path) # Supprime le fichier temporaire après lecture et traitement

#         except Exception as e:
#             app.logger.error(f"Erreur traitement fichier {file.filename}: {str(e)}", exc_info=True)
#             # Traduit en français, puis balise (avec interpolation)
#             flash(_("Erreur lors du traitement du fichier '%s': %s") % (file.filename, str(e)), 'error')
#             continue

#     if not batch_dfs:
#         # Traduit en français, puis balise
#         flash(_('Aucune donnée valide traitée. Veuillez vérifier vos fichiers.'), 'error')
#         return redirect(url_for('index'))

#     try:
#         # Utiliser un merge au lieu de concat
#         batch_df = pd.concat(batch_dfs, ignore_index=True) # Concatène toutes les DataFrames du lot
#         batch_df = create_datetime(batch_df) # Crée la colonne Datetime
#         batch_df = gestion_doublons(batch_df) # Gère les doublons
#         batch_df = batch_df.set_index('Datetime').sort_index() # Définit Datetime comme index

#         # Met à jour GLOBAL_PROCESSED_DATA_DF
#         if GLOBAL_PROCESSED_DATA_DF.empty:
#             GLOBAL_PROCESSED_DATA_DF = batch_df
#         else:
#             # Supprime les données existantes pour les stations mises à jour avant de concaténer
#             stations_to_update = batch_df['Station'].unique()
#             GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF[
#                 ~GLOBAL_PROCESSED_DATA_DF['Station'].isin(stations_to_update)
#             ]
#             GLOBAL_PROCESSED_DATA_DF = pd.concat(
#                 [GLOBAL_PROCESSED_DATA_DF, batch_df]
#             ).sort_index()

#         # Applique l'interpolation et la gestion des limites
#         GLOBAL_PROCESSED_DATA_DF = interpolation(
#             GLOBAL_PROCESSED_DATA_DF,
#             DATA_LIMITS,
#             GLOBAL_GPS_DATA_DF
#         )

#         new_rows = len(batch_df)
#         stations_added = ', '.join(batch_df['Station'].unique())
#         # Traduit en français, puis balise (avec variables nommées)
#         flash(
#             _("%(new_rows)s nouvelles lignes traitées pour les stations : %(stations_added)s") % {'new_rows': new_rows, 'stations_added': stations_added},
#             'success'
#         )

#         return redirect(url_for('data_preview')) # Redirige vers la prévisualisation des données

#     except Exception as e:
#         app.logger.error(f"Erreur critique lors du traitement global des données: {str(e)}", exc_info=True)
#         # Traduit en français, puis balise (avec interpolation)
#         flash(_('Erreur critique lors du traitement des données : %s') % str(e), 'error')
#         return redirect(url_for('index'))

import pandas as pd
import os
from flask import Flask, request, redirect, url_for, flash
from werkzeug.utils import secure_filename
from flask_babel import Babel, _

# --- Fonctions et configuration Flask (assurez-vous qu'elles sont définies) ---
# app = Flask(__name__)
# app.config['UPLOAD_FOLDER'] = 'uploads'
# app.config['SECRET_KEY'] = 'votre_cle_secrete_ici'
# babel = Babel(app)

# Variables Globales
# GLOBAL_PROCESSED_DATA_DF = pd.DataFrame()
# DATA_LIMITS = {}
# GLOBAL_GPS_DATA_DF = pd.DataFrame()

# Fonctions utilitaires : Assurez-vous que create_datetime, gestion_doublons,
# apply_station_specific_preprocessing, interpolation sont correctement définies
# et fonctionnent comme prévu.

# --- IMPORTANT : Liste des colonnes identifiées comme 'Datetime components' qui doivent être retirées ---
# Adaptez cette liste aux noms exacts des colonnes de vos fichiers sources
# qui sont utilisées uniquement pour construire la colonne 'Datetime'.
DATETIME_COMPONENTS = ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date', 'Time']
# 'Date' et 'Time' sont inclus si votre `create_datetime` les utilise puis ne les supprime pas.

# @app.route('/upload', methods=['POST'])
# def upload_file():
#     """Gère l'upload et le traitement des fichiers de données en utilisant pd.merge
#        et en nettoyant les noms de colonnes."""
#     global GLOBAL_PROCESSED_DATA_DF

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

#     processed_dfs_for_merge = []

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
            
#             # 1. Créer 'Datetime' et supprimer les colonnes de composants
#             df = create_datetime(df)
#             # Supprime les colonnes de composants de Datetime pour éviter qu'elles soient fusionnées
#             cols_to_drop = [col for col in DATETIME_COMPONENTS if col in df.columns]
#             df = df.drop(columns=cols_to_drop, errors='ignore')

#             df = gestion_doublons(df)
#             df = apply_station_specific_preprocessing(df, station)

#             processed_dfs_for_merge.append(df)

#             os.unlink(temp_path)

#         except Exception as e:
#             app.logger.error(f"Erreur traitement fichier {file.filename}: {str(e)}", exc_info=True)
#             flash(_("Erreur lors du traitement du fichier '%s': %s") % (file.filename, str(e)), 'error')
#             continue

#     if not processed_dfs_for_merge:
#         flash(_('Aucune donnée valide traitée ou préparée pour le merge.'), 'error')
#         return redirect(url_for('index'))

#     try:
#         # 2. Fusion des DataFrames du lot et consolidation des colonnes
#         # Commence avec la première DataFrame
#         merged_batch_df = processed_dfs_for_merge[0]

#         # Boucle pour fusionner les DataFrames suivantes
#         for i in range(1, len(processed_dfs_for_merge)):
#             current_df_to_merge = processed_dfs_for_merge[i]
            
#             # Identifier les colonnes communes (autres que 'Datetime' et 'Station')
#             # Celles-ci devront être consolidées
#             common_cols = [
#                 col for col in merged_batch_df.columns
#                 if col in current_df_to_merge.columns and col not in ['Datetime', 'Station']
#             ]
            
#             # Effectuer la fusion avec des suffixes pour les colonnes communes
#             temp_merged_df = pd.merge(
#                 merged_batch_df,
#                 current_df_to_merge,
#                 on=['Datetime', 'Station'],
#                 how='outer',
#                 suffixes=('_x', '_y') # Suffixes génériques pour cette étape de fusion de lot
#             )
            
#             # Consolider les colonnes : préférer la valeur de '_y' (le fichier le plus récent dans le lot)
#             # si elle est disponible, sinon prendre celle de '_x'.
#             for col in common_cols:
#                 col_x = f"{col}_x"
#                 col_y = f"{col}_y"
#                 if col_x in temp_merged_df.columns and col_y in temp_merged_df.columns:
#                     # Utiliser coalesce (fillna) pour prendre la première valeur non-NaN
#                     temp_merged_df[col] = temp_merged_df[col_y].fillna(temp_merged_df[col_x])
#                     # Supprimer les colonnes suffixées après consolidation
#                     temp_merged_df = temp_merged_df.drop(columns=[col_x, col_y])
#                 elif col_y in temp_merged_df.columns: # Si la colonne n'était que dans le second df avec suffixe
#                     temp_merged_df.rename(columns={col_y: col}, inplace=True)
#                 elif col_x in temp_merged_df.columns: # Si la colonne n'était que dans le premier df avec suffixe
#                     temp_merged_df.rename(columns={col_x: col}, inplace=True)

#             # Gérer les colonnes uniques au second DataFrame du merge (sans suffixe)
#             # Elles seront ajoutées automatiquement par l'outer merge
            
#             merged_batch_df = temp_merged_df
#             app.logger.info(f"Fusionné {i+1} DataFrames dans le lot. Forme actuelle: {merged_batch_df.shape}")

#         # Nettoyage final du DataFrame du lot (s'assurer que Datetime est l'index et trié)
#         if 'Datetime' in merged_batch_df.columns:
#             batch_df = merged_batch_df.set_index('Datetime').sort_index()
#         else:
#             flash(_('Erreur: La colonne Datetime est manquante après le merge du lot.'), 'error')
#             return redirect(url_for('index'))

#         # S'assurer que 'Station' est une colonne si nécessaire pour les étapes suivantes
#         if 'Station' in batch_df.index.names:
#             batch_df = batch_df.reset_index(level='Station')

#         # 3. Mise à jour de GLOBAL_PROCESSED_DATA_DF et consolidation finale
#         if GLOBAL_PROCESSED_DATA_DF.empty:
#             GLOBAL_PROCESSED_DATA_DF = batch_df
#         else:
#             # Préparer les DataFrames pour la fusion globale
#             global_df_reset = GLOBAL_PROCESSED_DATA_DF.reset_index() if 'Datetime' in GLOBAL_PROCESSED_DATA_DF.index.names else GLOBAL_PROCESSED_DATA_DF
#             batch_df_reset = batch_df.reset_index() if 'Datetime' in batch_df.index.names else batch_df

#             # Identifier les colonnes communes entre l'ancien global et le nouveau lot
#             common_global_cols = [
#                 col for col in global_df_reset.columns
#                 if col in batch_df_reset.columns and col not in ['Datetime', 'Station']
#             ]

#             temp_global_merged_df = pd.merge(
#                 global_df_reset,
#                 batch_df_reset,
#                 on=['Datetime', 'Station'],
#                 how='outer',
#                 suffixes=('_old', '_new') # Suffixes pour la fusion globale
#             )
            
#             # Consolidation des colonnes dans la fusion globale:
#             # Préférer les valeurs '_new' (du nouveau lot) si disponibles, sinon '_old'
#             for col in common_global_cols:
#                 col_old = f"{col}_old"
#                 col_new = f"{col}_new"
#                 if col_old in temp_global_merged_df.columns and col_new in temp_global_merged_df.columns:
#                     temp_global_merged_df[col] = temp_global_merged_df[col_new].fillna(temp_global_merged_df[col_old])
#                     temp_global_merged_df = temp_global_merged_df.drop(columns=[col_old, col_new])
#                 elif col_new in temp_global_merged_df.columns: # Si la colonne est unique au nouveau lot et suffixée
#                     temp_global_merged_df.rename(columns={col_new: col}, inplace=True)
#                 elif col_old in temp_global_merged_df.columns: # Si la colonne est unique à l'ancien global et suffixée
#                     temp_global_merged_df.rename(columns={col_old: col}, inplace=True)

#             # Assurer que toutes les colonnes qui n'étaient pas en commun mais existent dans l'un ou l'autre
#             # et qui ont pu avoir des suffixes (si pd.merge les a ajoutés sans col commune) sont renommées.
#             # C'est un cas moins fréquent avec 'outer' mais utile si les colonnes ne sont pas toujours présentes.
#             final_columns = set()
#             for col in temp_global_merged_df.columns:
#                 if col.endswith('_old'):
#                     final_columns.add(col[:-4])
#                 elif col.endswith('_new'):
#                     final_columns.add(col[:-4])
#                 else:
#                     final_columns.add(col)
            
#             # Crée une nouvelle DataFrame avec les noms de colonnes propres
#             clean_df_cols = {}
#             for col in temp_global_merged_df.columns:
#                 if col.endswith('_old'):
#                     if col[:-4] not in clean_df_cols:
#                         clean_df_cols[col[:-4]] = temp_global_merged_df[col]
#                     # Si la version non suffixée existe déjà (consolidée), on ne fait rien
#                 elif col.endswith('_new'):
#                     if col[:-4] not in clean_df_cols:
#                         clean_df_cols[col[:-4]] = temp_global_merged_df[col]
#                     # Si la version non suffixée existe déjà (consolidée), on ne fait rien
#                 else:
#                     if col not in clean_df_cols:
#                         clean_df_cols[col] = temp_global_merged_df[col]

#             # Reconstruire la DataFrame avec les noms propres
#             GLOBAL_PROCESSED_DATA_DF = pd.DataFrame(clean_df_cols).set_index('Datetime').sort_index()


#         # 4. Application des traitements finaux
#         # Cette fonction doit gérer les NaNs générés par le merge
#         GLOBAL_PROCESSED_DATA_DF = interpolation(
#             GLOBAL_PROCESSED_DATA_DF,
#             DATA_LIMITS,
#             GLOBAL_GPS_DATA_DF
#         )

#         new_rows = len(batch_df) # Cette métrique est moins précise avec des DF larges
#         stations_added = ', '.join(batch_df['Station'].unique())
#         flash(
#             _("%(new_rows)s nouvelles lignes traitées pour les stations : %(stations_added)s") % {'new_rows': new_rows, 'stations_added': stations_added},
#             'success'
#         )

#         return redirect(url_for('data_preview'))

#     except Exception as e:
#         app.logger.error(f"Erreur critique lors du traitement global des données: {str(e)}", exc_info=True)
#         flash(_('Erreur critique lors du traitement des données : %s') % str(e), 'error')
#         return redirect(url_for('index'))


from flask import request, redirect, url_for, flash
import os
import pandas as pd
import functools
import re

# @app.route('/upload', methods=['POST'])
# def upload_file():
#     """Gère l'upload et le traitement des fichiers de données en utilisant pd.merge optimisé."""
#     global GLOBAL_PROCESSED_DATA_DF

#     # Vérifie qu'au moins un fichier a été envoyé
#     if not request.files:
#         flash(_('Aucun fichier reçu.'), 'error')
#         return redirect(url_for('index'))

#     # Récupère la liste des fichiers uploadés
#     uploaded_files = request.files.getlist('file[]')
#     stations = []
#     # Récupère le nom de la station pour chaque fichier
#     for i in range(len(uploaded_files)):
#         station_name = request.form.get(f'station_{i}')
#         if station_name:
#             stations.append(station_name)
#         else:
#             flash(_('Veuillez sélectionner une station pour chaque fichier.'), 'error')
#             return redirect(url_for('index'))

#     # Vérifie que chaque fichier a bien une station associée
#     if len(uploaded_files) != len(stations):
#         flash(_('Nombre de fichiers et de stations incompatible.'), 'error')
#         return redirect(url_for('index'))

#     # Liste pour stocker les DataFrames prétraités
#     processed_dfs_for_merge = []

#     # Boucle sur chaque fichier et station associée
#     for file, station in zip(uploaded_files, stations):
#         # Vérifie que le fichier est autorisé
#         if not file or not allowed_file(file.filename):
#             flash(_("Fichier '%s' invalide ou non autorisé.") % file.filename, 'error')
#             continue

#         try:
#             # Sécurise le nom du fichier et le sauvegarde temporairement
#             filename = secure_filename(file.filename)
#             temp_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#             file.save(temp_path)

#             # Lit le fichier en DataFrame (CSV ou Excel)
#             if filename.lower().endswith('.csv'):
#                 df = pd.read_csv(temp_path, encoding_errors='replace', low_memory=False)
#             else:
#                 df = pd.read_excel(temp_path)

#             # Ajoute la colonne 'Station'
#             df['Station'] = station
#             # Crée la colonne 'Datetime' à partir des composantes si besoin
#             df = create_datetime(df)
#             # Supprime les colonnes de composantes de date pour éviter les conflits lors du merge
#             cols_to_drop = [col for col in DATETIME_COMPONENTS if col in df.columns]
#             df = df.drop(columns=cols_to_drop, errors='ignore')
#             # Supprime les doublons
#             df = gestion_doublons(df)
#             # Applique le prétraitement spécifique à la station
#             df = apply_station_specific_preprocessing(df, station)
#             # Ajoute le DataFrame à la liste
#             processed_dfs_for_merge.append(df)
#             # Supprime le fichier temporaire
#             os.unlink(temp_path)

#         except Exception as e:
#             # Gestion des erreurs lors du traitement d'un fichier
#             app.logger.error(f"Erreur traitement fichier {file.filename}: {str(e)}", exc_info=True)
#             flash(_("Erreur lors du traitement du fichier '%s': %s") % (file.filename, str(e)), 'error')
#             continue

#     # Vérifie qu'au moins un DataFrame a été préparé
#     if not processed_dfs_for_merge:
#         flash(_('Aucune donnée valide traitée ou préparée pour le merge.'), 'error')
#         return redirect(url_for('index'))

#     try:
#         # Fusionne tous les DataFrames du lot sur 'Datetime' et 'Station' en une seule passe rapide
#         def merge_two(left, right):
#             return pd.merge(left, right, on=['Datetime', 'Station'], how='outer', suffixes=('', '_dup'))

#         merged_batch_df = functools.reduce(merge_two, processed_dfs_for_merge)

#         # Supprime toutes les colonnes suffixées '_dup' générées par le merge
#         merged_batch_df = merged_batch_df.loc[:, ~merged_batch_df.columns.str.endswith('_dup')]

#         # Met 'Datetime' en index et trie
#         if 'Datetime' in merged_batch_df.columns:
#             batch_df = merged_batch_df.set_index('Datetime').sort_index()
#         else:
#             flash(_('Erreur: La colonne Datetime est manquante après le merge du lot.'), 'error')
#             return redirect(url_for('index'))

#         # Remet 'Station' en colonne si elle est passée en index
#         if 'Station' in batch_df.index.names:
#             batch_df = batch_df.reset_index(level='Station')

#         # Si le DataFrame global est vide, on le remplit avec le batch
#         if GLOBAL_PROCESSED_DATA_DF.empty:
#             GLOBAL_PROCESSED_DATA_DF = batch_df
#         else:
#             # Prépare les DataFrames pour la fusion globale
#             global_df_reset = GLOBAL_PROCESSED_DATA_DF.reset_index() if 'Datetime' in GLOBAL_PROCESSED_DATA_DF.index.names else GLOBAL_PROCESSED_DATA_DF
#             batch_df_reset = batch_df.reset_index() if 'Datetime' in batch_df.index.names else batch_df

#             # Fusionne le DataFrame du lot avec le DataFrame global sur 'Datetime' et 'Station'
#             merged_global_df = pd.merge(
#                 global_df_reset,
#                 batch_df_reset,
#                 on=['Datetime', 'Station'],
#                 how='outer',
#                 suffixes=('_old', '_new')
#             )

#             # Consolide les colonnes dupliquées : garde la valeur la plus récente (_new), sinon l’ancienne (_old)
#             for col in merged_global_df.columns:
#                 if col.endswith('_new'):
#                     base_col = col[:-4]
#                     old_col = base_col + '_old'
#                     if old_col in merged_global_df.columns:
#                         merged_global_df[base_col] = merged_global_df[col].combine_first(merged_global_df[old_col])
#                         merged_global_df.drop([col, old_col], axis=1, inplace=True)
#                     else:
#                         merged_global_df.rename(columns={col: base_col}, inplace=True)
#                 elif col.endswith('_old'):
#                     base_col = col[:-4]
#                     if base_col not in merged_global_df.columns:
#                         merged_global_df.rename(columns={col: base_col}, inplace=True)

#             # Met à jour le DataFrame global avec l'index 'Datetime'
#             GLOBAL_PROCESSED_DATA_DF = merged_global_df.set_index('Datetime').sort_index()

#         # Applique l’interpolation et les traitements finaux sur le DataFrame global
#         GLOBAL_PROCESSED_DATA_DF = interpolation(
#             GLOBAL_PROCESSED_DATA_DF,
#             DATA_LIMITS,
#             GLOBAL_GPS_DATA_DF
#         )

#         # Calcule le nombre de nouvelles lignes et les stations ajoutées
#         new_rows = len(batch_df)
#         stations_added = ', '.join(batch_df['Station'].unique())
#         # Affiche un message de succès à l'utilisateur
#         flash(
#             _("%(new_rows)s nouvelles lignes traitées pour les stations : %(stations_added)s") % {'new_rows': new_rows, 'stations_added': stations_added},
#             'success'
#         )

#         # Redirige vers la page de prévisualisation des données
#         return redirect(url_for('data_preview'))

#     except Exception as e:
#         # Gestion des erreurs globales
#         app.logger.error(f"Erreur critique lors du traitement global des données: {str(e)}", exc_info=True)
#         flash(_('Erreur critique lors du traitement des données : %s') % str(e), 'error')
#         return redirect(url_for('index'))



@app.route('/upload', methods=['POST'])
def upload_file():
    """Gère l'upload et le traitement des fichiers de données."""
    global GLOBAL_PROCESSED_DATA_DF, GLOBAL_BEFORE_INTERPOLATION_DATA_DF, GLOBAL_RAW_DATA_DF, GLOBAL_MISSING_VALUES_AFTER_INTERPOLATION_DF, GLOBAL_MISSING_VALUES_BEFORE_INTERPOLATION_DF

    if not request.files:
        # Traduit en français, puis balise
        flash(_('Aucun fichier reçu.'), 'error')
        return redirect(url_for('index'))

    uploaded_files = request.files.getlist('file[]')
    stations = []
    # Collecter les noms de station pour chaque fichier
    for i in range(len(uploaded_files)):
        station_name = request.form.get(f'station_{i}')
        if station_name:
            stations.append(station_name)
        else:
            # Traduit en français, puis balise
            flash(_('Veuillez sélectionner une station pour chaque fichier.'), 'error')
            return redirect(url_for('index'))

    if len(uploaded_files) != len(stations):
        # Traduit en français, puis balise
        flash(_('Nombre de fichiers et de stations incompatible.'), 'error')
        return redirect(url_for('index'))

    batch_dfs = [] # Liste pour stocker les DataFrames de chaque fichier uploadé

    for file, station in zip(uploaded_files, stations):
        if not file or not allowed_file(file.filename):
            # Traduit en français, puis balise (avec interpolation)
            flash(_("Fichier '%s' invalide ou non autorisé.") % file.filename, 'error')
            continue # Passe au fichier suivant

        try:
            filename = secure_filename(file.filename)
            temp_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(temp_path) # Sauvegarde temporaire du fichier

            # Lire le fichier selon son extension
            if filename.lower().endswith('.csv'):
                df = pd.read_csv(temp_path, encoding_errors='replace')
            else: # Supposons .xlsx ou .xls
                df = pd.read_excel(temp_path)

            df['Station'] = station # Ajoute la colonne 'Station'
            # Correction: apply_station_specific_preprocessing ne prend pas 'bassin' comme argument direct
            # Il déduit le bassin de la station à l'intérieur de la fonction
            df = apply_station_specific_preprocessing(df, station) # Applique le prétraitement spécifique à la station
            batch_dfs.append(df) # Ajoute la DataFrame traitée à la liste

            os.unlink(temp_path) # Supprime le fichier temporaire après lecture et traitement

        except Exception as e:
            app.logger.error(f"Erreur traitement fichier {file.filename}: {str(e)}", exc_info=True)
            # Traduit en français, puis balise (avec interpolation)
            flash(_("Erreur lors du traitement du fichier '%s': %s") % (file.filename, str(e)), 'error')
            continue

    if not batch_dfs:
        # Traduit en français, puis balise
        flash(_('Aucune donnée valide traitée. Veuillez vérifier vos fichiers.'), 'error')
        return redirect(url_for('index'))

    try:
        # Utiliser un merge au lieu de concat
        batch_df = pd.concat(batch_dfs, ignore_index=True) # Concatène toutes les DataFrames du lot
        batch_df = create_datetime(batch_df) # Crée la colonne Datetime
        batch_df = gestion_doublons(batch_df) # Gère les doublons
        batch_df = batch_df.set_index('Datetime').sort_index() # Définit Datetime comme index

        # Met à jour GLOBAL_PROCESSED_DATA_DF
        if GLOBAL_RAW_DATA_DF.empty:
            GLOBAL_RAW_DATA_DF = batch_df
        else:
            # Supprime les données existantes pour les stations mises à jour avant de concaténer
            stations_to_update = batch_df['Station'].unique()
            GLOBAL_RAW_DATA_DF = GLOBAL_RAW_DATA_DF[
                ~GLOBAL_RAW_DATA_DF['Station'].isin(stations_to_update)
            ]
            GLOBAL_RAW_DATA_DF = pd.concat(
                [GLOBAL_RAW_DATA_DF, batch_df]
            ).sort_index()

        # # Met à jour GLOBAL_B pour les données avant interpolation
        # GLOBAL_BEFORE_INTERPOLATION_DATA_DF , GLOBAL_PROCESSED_DATA_DF = preproce
        # Applique l'interpolation et la gestion des limites

        GLOBAL_BEFORE_INTERPOLATION_DATA_DF , GLOBAL_PROCESSED_DATA_DF, GLOBAL_MISSING_VALUES_BEFORE_INTERPOLATION_DF, GLOBAL_MISSING_VALUES_AFTER_INTERPOLATION_DF= interpolation(
            GLOBAL_RAW_DATA_DF,
            DATA_LIMITS,
            GLOBAL_GPS_DATA_DF
        )

        # Appliquer la fonction de traitement des outliers
        GLOBAL_PROCESSED_DATA_DF = traiter_outliers_meteo(GLOBAL_PROCESSED_DATA_DF, colonnes=DATA_LIMITS.keys())

        new_rows = len(batch_df)
        stations_added = ', '.join(batch_df['Station'].unique())
        # Traduit en français, puis balise (avec variables nommées)
        flash(
            _("%(new_rows)s nouvelles lignes traitées pour les stations : %(stations_added)s") % {'new_rows': new_rows, 'stations_added': stations_added},
            'success'
        )

        #return redirect(url_for('preprocessing')) # Redirige vers la prévisualisation des données
        return redirect(url_for('data_preview')) # Redirige vers la prévisualisation des données


    except Exception as e:
        app.logger.error(f"Erreur critique lors du traitement global des données: {str(e)}", exc_info=True)
        # Traduit en français, puis balise (avec interpolation)
        flash(_('Erreur critique lors du traitement des données : %s') % str(e), 'error')
        return redirect(url_for('index'))


# @app.route('/upload', methods=['POST'])
# def upload_file():
#     """Version optimisée de la fonction d'upload"""
#     global GLOBAL_PROCESSED_DATA_DF, GLOBAL_BEFORE_INTERPOLATION_DATA_DF, GLOBAL_RAW_DATA_DF
#     global GLOBAL_MISSING_VALUES_AFTER_INTERPOLATION_DF, GLOBAL_MISSING_VALUES_BEFORE_INTERPOLATION_DF

#     # Vérification rapide des fichiers
#     if not request.files:
#         flash(_('Aucun fichier reçu.'), 'error')
#         return redirect(url_for('index'))

#     # Récupération des fichiers et stations en une passe
#     uploaded_files = request.files.getlist('file[]')
#     stations = [request.form.get(f'station_{i}') for i in range(len(uploaded_files))]
    
#     # Validation des entrées
#     if None in stations:
#         flash(_('Veuillez sélectionner une station pour chaque fichier.'), 'error')
#         return redirect(url_for('index'))
    
#     if len(uploaded_files) != len(stations):
#         flash(_('Nombre de fichiers et de stations incompatible.'), 'error')
#         return redirect(url_for('index'))

#     # Traitement parallèle des fichiers (si nombreux)
#     batch_dfs = []
#     temp_paths = []
    
#     for file, station in zip(uploaded_files, stations):
#         if not file or not allowed_file(file.filename):
#             flash(_("Fichier '%s' invalide ou non autorisé.") % file.filename, 'error')
#             continue

#         try:
#             filename = secure_filename(file.filename)
#             temp_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#             file.save(temp_path)
#             temp_paths.append(temp_path)
            
#             # Lecture optimisée selon l'extension
#             reader = pd.read_csv if filename.lower().endswith('.csv') else pd.read_excel
#             df = reader(temp_path, encoding_errors='replace')
            
#             # Prétraitement
#             df['Station'] = station
#             df = apply_station_specific_preprocessing(df, station)
#             batch_dfs.append(df)

#         except Exception as e:
#             app.logger.error(f"Erreur traitement fichier {file.filename}: {str(e)}", exc_info=True)
#             flash(_("Erreur lors du traitement du fichier '%s': %s") % (file.filename, str(e)), 'error')
    
#     # Nettoyage des fichiers temporaires
#     for path in temp_paths:
#         try:
#             os.unlink(path)
#         except:
#             pass

#     if not batch_dfs:
#         flash(_('Aucune donnée valide traitée. Veuillez vérifier vos fichiers.'), 'error')
#         return redirect(url_for('index'))

#     try:
#         # Concaténation et traitement optimisé
#         batch_df = pd.concat(batch_dfs, ignore_index=True)
#         batch_df = create_datetime(batch_df)
#         batch_df = gestion_doublons(batch_df).set_index('Datetime').sort_index()

#         # Mise à jour des données globales
#         stations_to_update = batch_df['Station'].unique()
#         if not GLOBAL_RAW_DATA_DF.empty:
#             GLOBAL_RAW_DATA_DF = GLOBAL_RAW_DATA_DF[~GLOBAL_RAW_DATA_DF['Station'].isin(stations_to_update)]
        
#         GLOBAL_RAW_DATA_DF = pd.concat([GLOBAL_RAW_DATA_DF, batch_df]).sort_index()

#         # Interpolation optimisée
#         (GLOBAL_BEFORE_INTERPOLATION_DATA_DF, 
#          GLOBAL_PROCESSED_DATA_DF,
#          GLOBAL_MISSING_VALUES_BEFORE_INTERPOLATION_DF,
#          GLOBAL_MISSING_VALUES_AFTER_INTERPOLATION_DF) = interpolation(
#              GLOBAL_RAW_DATA_DF,
#              DATA_LIMITS,
#              GLOBAL_GPS_DATA_DF
#          )

#         # Traitement des outliers
#         GLOBAL_PROCESSED_DATA_DF = traiter_outliers_meteo(GLOBAL_PROCESSED_DATA_DF, colonnes=DATA_LIMITS.keys())

#         # Feedback utilisateur
#         new_rows = len(batch_df)
#         stations_added = ', '.join(batch_df['Station'].unique())
#         flash(
#             _("%(new_rows)s nouvelles lignes traitées pour les stations : %(stations_added)s") % {
#                 'new_rows': new_rows, 
#                 'stations_added': stations_added
#             },
#             'success'
#         )

#         return redirect(url_for('preprocessing'))

#     except Exception as e:
#         app.logger.error(f"Erreur critique lors du traitement global: {str(e)}", exc_info=True)
#         flash(_('Erreur critique lors du traitement des données : %s') % str(e), 'error')
#         return redirect(url_for('index'))
    

print("--- In preprocessing route (GLOBAL_PROCESSED_DATA_DF) ---")
print(GLOBAL_PROCESSED_DATA_DF.columns.tolist()) 

@app.route('/preview')
def data_preview():
    """Affiche un aperçu des données chargées."""
    if GLOBAL_PROCESSED_DATA_DF.empty:
        # Traduit en français, puis balise
        flash(_('Aucune donnée disponible. Veuillez télécharger des fichiers d\'abord.'), 'error')
        return redirect(url_for('index'))
    try:
        stations = GLOBAL_PROCESSED_DATA_DF['Station'].unique()

        if len(stations) == 1:
            preview_df = GLOBAL_PROCESSED_DATA_DF.head(20).reset_index()
            # Traduit en français, puis balise
            preview_type = _("20 premières lignes")
        else:
            preview_dfs = []
            for station in stations:
                # Prend les 10 premières lignes par station
                station_df = GLOBAL_PROCESSED_DATA_DF[GLOBAL_PROCESSED_DATA_DF['Station'] == station].head(10).reset_index()
                preview_dfs.append(station_df)
            preview_df = pd.concat(preview_dfs)
            # Traduit en français, puis balise (avec variables nommées)
            preview_type = _("10 lignes × %(stations_count)s stations") % {'stations_count': len(stations)}

        # Convertit la DataFrame en HTML pour l'affichage
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
                               preview_type=preview_type,
                               # Traduit en français, puis balise (avec variables nommées)
                               dataset_shape=_("%(rows)s lignes × %(cols)s colonnes") % {'rows': GLOBAL_PROCESSED_DATA_DF.shape[0], 'cols': GLOBAL_PROCESSED_DATA_DF.shape[1]},
                               stations_count=len(stations))

    except Exception as e:
        app.logger.error(f'Erreur lors de la préparation de l\'aperçu: {str(e)}', exc_info=True)
        # Traduit en français, puis balise (avec interpolation)
        flash(_('Erreur lors de la préparation de l\'aperçu : %s') % str(e), 'error')
        return redirect(url_for('index'))

@app.route('/visualisations_options')
def visualisations_options():
    """Affiche les options pour générer des visualisations."""
    if GLOBAL_PROCESSED_DATA_DF.empty:
        # Traduit en français, puis balise
        flash(_('Veuillez télécharger des fichiers d\'abord pour accéder aux visualisations.'), 'error')
        return redirect(url_for('index'))

    # Colonnes à exclure pour les visualisations (non numériques ou non pertinentes)
    excluded_cols = {'Station', 'Is_Daylight', 'Daylight_Duration',
                     'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date', 'Rain_01_mm', 'Rain_02_mm'}

    # Récupère les variables numériques disponibles dans la DataFrame
    available_vars = [
        col for col in GLOBAL_PROCESSED_DATA_DF.columns
        if col not in excluded_cols and pd.api.types.is_numeric_dtype(GLOBAL_PROCESSED_DATA_DF[col])
    ]

    # Génère le tableau récapitulatif quotidien
    daily_stats_df = calculate_daily_summary_table(GLOBAL_PROCESSED_DATA_DF)
    daily_stats_html = daily_stats_df.to_html(classes='table table-striped', index=False)

    # Récupère les paramètres de requête pour pré-remplir les champs des formulaires (si redirection)
    variable_selectionnee = request.args.get('variable')
    variables_selectionnees = request.args.getlist('variables[]')
    station_selectionnee = request.args.get('station')
    periode_selectionnee = request.args.get('periode')

    return render_template('visualisations_options.html',
                           stations=sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique()),
                           variables=sorted(available_vars),
                           METADATA_VARIABLES=METADATA_VARIABLES,
                           PALETTE_DEFAUT=PALETTE_DEFAUT,
                           daily_stats_table=daily_stats_html,
                           variable_selectionnee=variable_selectionnee,
                           variables_selectionnees=variables_selectionnees,
                           station_selectionnee=station_selectionnee,
                           periode_selectionnee=periode_selectionnee)

from flask_babel import get_locale

def get_var_label(meta, key):
    lang = str(get_locale())
    return meta[key].get(lang[:2], meta[key].get('en', list(meta[key].values())[0]))


    
@app.route('/generate_plot', methods=['GET', 'POST'])
def generate_plot():
    """Génère un graphique basé sur les sélections de l'utilisateur."""
    if GLOBAL_PROCESSED_DATA_DF.empty:
        flash(_('Aucune donnée disponible. Veuillez télécharger des fichiers d\'abord.'), 'error')
        return redirect(url_for('index'))
    try:
        is_comparative = 'comparative' in request.form
        periode_key = request.form.get('periode') # Renamed to periode_key for clarity

        if not periode_key:
            flash(_('Veuillez sélectionner une période d\'analyse.'), 'error')
            return redirect(url_for('visualisations_options'))

        # --- Translate the period key for display ---
        translated_periode = get_period_label(periode_key)

        if is_comparative:
            variable = request.form.get('variable')
            if not variable:
                flash(_('Veuillez sélectionner une variable pour la comparaison.'), 'error')
                return redirect(url_for('visualisations_options'))

            fig = generer_graphique_comparatif(
                df=GLOBAL_PROCESSED_DATA_DF,
                variable=variable,
                periode=periode_key, # Pass the untranslated key to the plotting function
                colors=CUSTOM_STATION_COLORS,
                metadata=METADATA_VARIABLES
            )

            var_meta = METADATA_VARIABLES.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
            var_label = str(get_var_label(var_meta, 'Nom')) 
            
            # Use the translated_periode for the title
            title = _("Comparaison de %(variable_name)s (%(period)s)") % {
                'variable_name': var_label,
                'period': translated_periode # <-- Use the TRANSLATED period here
            }


        else: # Graphique multi-variables pour une station
            station = request.form.get('station')
            variables = request.form.getlist('variables[]')

            if not station:
                flash(_('Veuillez sélectionner une station.'), 'error')
                return redirect(url_for('visualisations_options'))

            if not variables:
                flash(_('Veuillez sélectionner au moins une variable à visualiser.'), 'error')
                return redirect(url_for('visualisations_options'))

            fig = generer_graphique_par_variable_et_periode(
                df=GLOBAL_PROCESSED_DATA_DF,
                station=station,
                variables=variables,
                periode=periode_key, # Pass the untranslated key to the plotting function
                colors=PALETTE_DEFAUT,
                metadata=METADATA_VARIABLES
            )
            # Use the translated_periode for the title
            title = _("Évolution des variables pour %(station)s (%(period)s)") % {
                'station': station,
                'period': translated_periode # <-- Use the TRANSLATED period here
            }

        # Vérifie si la figure a été générée avec des données
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
    """Génère un graphique d'analyse multi-variables normalisées."""
    if GLOBAL_PROCESSED_DATA_DF.empty:
        # Traduit en français, puis balise
        flash(_('Aucune donnée disponible. Veuillez télécharger des fichiers d\'abord.'), 'error')
        return redirect(url_for('index'))
    try:
        station = request.form['station']
        variables = request.form.getlist('variables[]')
        #periode = request.form.get('periode', 'Brutes') # Valeur par défaut 'Brutes'
        periode_key = request.form.get('periode', '') # Renamed to periode_key for clarity


        if not station or not variables:
            # Traduit en français, puis balise
            flash(_('Veuillez sélectionner une station et au moins une variable pour l\'analyse normalisée.'), 'error')
            return redirect(url_for('visualisations_options'))
        
        translated_periode = get_period_label(periode_key)

        # var_meta = METADATA_VARIABLES.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
        # var_label = str(get_var_label(var_meta, 'Nom')) 
        # var_unit = str(get_var_label(var_meta, 'Unite')) 

            

        fig = generate_multi_variable_station_plot(
            df=GLOBAL_PROCESSED_DATA_DF,
            station=station,
            variables=variables,
            periode=periode_key,
            colors=PALETTE_DEFAUT,
            metadata=METADATA_VARIABLES
        )


       
        # Vérifie si la figure a été générée avec des données
        if not fig or not fig.data:
            # Traduit en français, puis balise
            flash(_('Aucune donnée disponible pour les critères sélectionnés. Veuillez affiner votre sélection.'), 'warning')
            return redirect(url_for('visualisations_options'))

        plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

        # Traduit en français, puis balise
        return render_template('plot_display.html',
                               plot_html=plot_html,
                               title=_("Analyse multi-variables normalisée - %(station)s (%(period)s)") % {
                                   'station': station,
                                   'period': translated_periode
                               })

    except Exception as e:
        app.logger.error(f"Erreur lors de la génération du graphique normalisé: {str(e)}", exc_info=True)
        # Traduit en français, puis balise (avec interpolation)
        flash(_("Erreur lors de la génération du graphique normalisé : %s") % str(e), 'error')
        return redirect(url_for('visualisations_options'))


@app.route('/reset_data', methods=['POST'])
def reset_data():
    """Réinitialise toutes les données chargées en mémoire et supprime les fichiers temporaires."""
    global GLOBAL_PROCESSED_DATA_DF
    try:
        # 1. Réinitialiser la DataFrame globale en mémoire
        GLOBAL_PROCESSED_DATA_DF = pd.DataFrame()

        # 2. Nettoyer le dossier d'upload (supprime tout fichier résiduel)
        for filename in os.listdir(app.config['UPLOAD_FOLDER']):
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path) # Supprime le fichier
            except Exception as e:
                # Log l'erreur mais ne bloque pas la réinitialisation principale
                app.logger.warning(f"Impossible de supprimer le fichier résiduel {file_path}: {e}")

        # 3. Flasher un message de succès
        # Traduit en français, puis balise
        flash(_('Toutes les données ont été réinitialisées avec succès. Vous pouvez maintenant télécharger de nouveaux jeux de données.'), 'success')

        # 4. Répondre avec une URL de redirection pour le frontend
        return jsonify(success=True, redirect_url=url_for('index'))

    except Exception as e:
        app.logger.error(f"Erreur critique lors de la réinitialisation des données: {str(e)}", exc_info=True)
        # Traduit en français, puis balise
        flash(_('Erreur lors de la réinitialisation des données : %s') % str(e), 'error')
        # En cas d'erreur, tenter quand même de rediriger l'utilisateur vers la page d'accueil
        # Traduit en français, puis balise
        return jsonify(success=False, message=_('Erreur interne lors de la réinitialisation : %s') % str(e), redirect_url=url_for('index')), 500


# @app.route('/statistiques', methods=['GET', 'POST'])
# def statistiques():
#     """Affiche des statistiques quotidiennes pour une variable sélectionnée."""
#     if GLOBAL_PROCESSED_DATA_DF.empty:
#         # Traduit en français, puis balise
#         flash(_('Aucune donnée disponible. Veuillez télécharger des fichiers d\'abord.'), 'error')
#         return redirect(url_for('index'))
#     try:
#         variable = request.args.get('variable') if request.method == 'GET' else request.form.get('variable')

#         if not variable or variable not in GLOBAL_PROCESSED_DATA_DF.columns:
#             # Traduit en français, puis balise
#             flash(_('Variable invalide ou introuvable. Veuillez sélectionner une variable valide.'), 'error')
#             return redirect(url_for('visualisations_options'))

#         fig = generate_daily_stats_plot_plotly(
#             df=GLOBAL_PROCESSED_DATA_DF,
#             variable=variable,
#             station_colors=CUSTOM_STATION_COLORS
#         )

#         # Vérifie si la figure a été générée avec des données
#         if not fig or not fig.data:
#             # Traduit en français, puis balise
#             flash(_('Aucune statistique disponible pour cette variable. Veuillez affiner votre sélection ou vérifier les données.'), 'warning')
#             return redirect(url_for('visualisations_options'))

#         plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

#         #var_meta = METADATA_VARIABLES.get(variable, {'Nom': variable, 'Unite': ''})

#         var_meta = METADATA_VARIABLES.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
#         var_label = str(get_var_label(var_meta, 'Nom'))   # Conversion explicite en str
#         var_unit = str(get_var_label(var_meta, 'Unite'))  # Conversion explicite en str
#         return render_template('statistiques.html',
#                                variable_name=var_label,
#                                unit=var_unit,
#                                plot_html=plot_html,
#                                variable_selectionnee=variable)

#     except Exception as e:
#         app.logger.error(f"ERREUR dans /statistiques: {str(e)}", exc_info=True)
#         traceback.print_exc() # Affiche la trace complète de l'erreur dans les logs du serveur
#         # Traduit en français, puis balise
#         flash(_('Erreur technique lors de la génération des statistiques.'), 'error')
#         return redirect(url_for('visualisations_options'))




@app.route('/statistiques', methods=['GET', 'POST'])
def statistiques():
    """Affiche des statistiques quotidiennes pour une variable sélectionnée."""
    # Vérifie si le DataFrame des données traitées est vide
    if GLOBAL_PROCESSED_DATA_DF.empty:
        flash(_('Aucune donnée disponible. Veuillez télécharger des fichiers d\'abord.'), 'error')
        return redirect(url_for('index'))

    # Vérifie si le DataFrame des données brutes est disponible, même s'il peut être vide
    # Cela permet à generate_daily_stats_plot_plotly de gérer l'absence si nécessaire
    if GLOBAL_BEFORE_INTERPOLATION_DATA_DF.empty:
        # Optionnel: Vous pouvez choisir d'afficher un message flash ici
        # ou laisser la fonction de plotting gérer le cas où df_original est vide.
        # Pour le moment, nous allons le passer même s'il est vide, la fonction le gérera.
        app.logger.warning("GLOBAL_BEFORE_INTERPOLATION_DATA_DF est vide. Les Max/Min ne seront pas vérifiés par rapport aux données brutes.")
        df_original_to_pass = None # Ou GLOBAL_BEFORE_INTERPOLATION_DATA_DF si la fonction le gère déjà comme un DataFrame vide
    else:
        df_original_to_pass = GLOBAL_BEFORE_INTERPOLATION_DATA_DF

    try:
        variable = request.args.get('variable') if request.method == 'GET' else request.form.get('variable')

        if not variable or variable not in GLOBAL_PROCESSED_DATA_DF.columns:
            flash(_('Variable invalide ou introuvable. Veuillez sélectionner une variable valide.'), 'error')
            return redirect(url_for('visualisations_options'))

        # Appel de la fonction de visualisation en passant df_original
        fig = generate_daily_stats_plot_plotly(
            df=GLOBAL_PROCESSED_DATA_DF,
            variable=variable,
            station_colors=CUSTOM_STATION_COLORS,
            df_original=df_original_to_pass # Passe le DataFrame original ici
        )

        # Vérifie si la figure a été générée avec des données
        if not fig or not fig.data:
            flash(_('Aucune statistique disponible pour cette variable. Veuillez affiner votre sélection ou vérifier les données.'), 'warning')
            return redirect(url_for('visualisations_options'))

        plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

        var_meta = METADATA_VARIABLES.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
        var_label = str(get_var_label(var_meta, 'Nom'))
        var_unit = str(get_var_label(var_meta, 'Unite'))

        return render_template('statistiques.html',
                               variable_name=var_label,
                               unit=var_unit,
                               plot_html=plot_html,
                               variable_selectionnee=variable)

    except Exception as e:
        app.logger.error(f"ERREUR dans /statistiques: {str(e)}", exc_info=True)
        traceback.print_exc()
        flash(_('Erreur technique lors de la génération des statistiques.'), 'error')
        return redirect(url_for('visualisations_options'))


 #Processing fonctionnel -Debut -#

# @app.route('/preprocessing', methods=['POST',  'GET'])
# def preprocessing():
#     stations = GLOBAL_PROCESSED_DATA_DF['Station'].unique()

#     station_selected = request.args.get('station')

#     print("--- In preprocessing route (GLOBAL_PROCESSED_DATA_DF) ---")
#     print(GLOBAL_PROCESSED_DATA_DF.columns.tolist()) 

#     # Liste des stations à afficher dans le select
#     #stations = sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique())

#     if not station_selected:
#         station_selected = stations[0]  # par défaut

#     # Filtrer les données selon la station sélectionnée
#     df_raw = GLOBAL_BEFORE_INTERPOLATION_DATA_DF[GLOBAL_BEFORE_INTERPOLATION_DATA_DF['Station'] == station_selected]
#     df_clean = GLOBAL_PROCESSED_DATA_DF[GLOBAL_PROCESSED_DATA_DF['Station'] == station_selected]

#     # Générer les figures Plotly
#     fig_missing_before = valeurs_manquantes_viz(df_raw)
#     fig_outliers_before = outliers_viz(df_raw)

#     fig_missing_after = valeurs_manquantes_viz(df_clean)
#     fig_outliers_after = outliers_viz(df_clean)

#     # Convertir en HTML
#     return render_template(
#         'preprocessing.html',
#         stations=stations,
#         station_selected=station_selected,
#         missing_before=fig_missing_before.to_html(full_html=False),
#         outliers_before=fig_outliers_before.to_html(full_html=False),
#         missing_after=fig_missing_after.to_html(full_html=False),
#         outliers_after=fig_outliers_after.to_html(full_html=False),
#     )

 #Processing fonctionnel -Fin -#
 

 # COmmenter poiur eviter  le traitement des donnnees explicitement  dans le templates
# @app.route('/preprocessing', methods=['GET', 'POST'])
# def preprocessing():
#     """
#     Affiche la page de prévisualisation du prétraitement avec les graphiques.
#     """
#     global GLOBAL_PROCESSED_DATA_DF, GLOBAL_BEFORE_INTERPOLATION_DATA_DF, GLOBAL_MISSING_VALUES_BEFORE_INTERPOLATION_DF, GLOBAL_MISSING_VALUES_AFTER_INTERPOLATION_DF

#     if GLOBAL_PROCESSED_DATA_DF.empty:
#         flash(_("Veuillez d'abord uploader des données pour accéder à la page de prétraitement."), 'info')
#         return redirect(url_for('index'))

#     # Assure que la liste des stations est triée et unique
#     stations = sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist())
#     station_selected = request.args.get('station')

#     # Sélectionne la première station si aucune n'est spécifiée
#     if not station_selected and stations:
#         station_selected = stations[0]
#     elif not stations: # Aucune station disponible après traitement
#         flash(_("Aucune station disponible pour la visualisation après le traitement."), 'warning')
#         return redirect(url_for('index'))

#     # Filtrer les données par la station sélectionnée
#     df_raw_station = GLOBAL_BEFORE_INTERPOLATION_DATA_DF[GLOBAL_BEFORE_INTERPOLATION_DATA_DF['Station'] == station_selected]
#     df_clean_station = GLOBAL_PROCESSED_DATA_DF[GLOBAL_PROCESSED_DATA_DF['Station'] == station_selected]

#     # Filtrer les DataFrames des valeurs manquantes pour la station sélectionnée
#     df_missing_before_station = GLOBAL_MISSING_VALUES_BEFORE_INTERPOLATION_DF[
#         GLOBAL_MISSING_VALUES_BEFORE_INTERPOLATION_DF['station'] == station_selected
#     ]
#     df_missing_after_station = GLOBAL_MISSING_VALUES_AFTER_INTERPOLATION_DF[
#         GLOBAL_MISSING_VALUES_AFTER_INTERPOLATION_DF['station'] == station_selected
#     ]

#     # Générer les figures Plotly
#     # Les pie charts des pourcentages de manquants
#     fig_missing_percent_before = valeurs_manquantes_viz(df_raw_station)
#     fig_missing_percent_after = valeurs_manquantes_viz(df_clean_station)

#     # Les graphiques de séries temporelles avec les gaps visualisés
#     fig_gaps_before = gaps_time_series_viz(df_raw_station, df_missing_before_station, station_selected, _("Avant Interpolation"))
#     fig_gaps_after = gaps_time_series_viz(df_clean_station, df_missing_after_station, station_selected, _("Après Interpolation"))

#     # Placeholders pour les graphiques d'outliers
#     # Remplacez ceci par vos vrais appels à `outliers_viz` si vous l'avez implémentée
#     fig_outliers_before = go.Figure().add_annotation(
#         x=0.5, y=0.5, text=_("Graphique des outliers avant interpolation (à implémenter)"),
#         showarrow=False, font=dict(size=14)
#     )
#     fig_outliers_after = go.Figure().add_annotation(
#         x=0.5, y=0.5, text=_("Graphique des outliers après interpolation (à implémenter)"),
#         showarrow=False, font=dict(size=14)
#     )


#     # Convertir les figures Plotly en HTML et les passer au template
#     return render_template(
#         'preprocessing.html',
#         stations=stations,
#         station_selected=station_selected,
        
#         missing_percent_before=fig_missing_percent_before.to_html(full_html=False),
#         missing_percent_after=fig_missing_percent_after.to_html(full_html=False),

#         gaps_before=fig_gaps_before.to_html(full_html=False),
#         gaps_after=fig_gaps_after.to_html(full_html=False),

#         outliers_before=fig_outliers_before.to_html(full_html=False),
#         outliers_after=fig_outliers_after.to_html(full_html=False),
#     )


if __name__ == '__main__':
    app.run(debug=True) # Exécute l'application Flask en mode débogage
