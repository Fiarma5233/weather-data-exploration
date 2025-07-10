
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
    #generate_daily_stats_plot_plotly,
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



############ Debut de code fonctionnel  ###########################

# @app.route('/upload', methods=['POST'])
# def upload_file():
#     """Gère l'upload et le traitement des fichiers de données."""
#     global GLOBAL_PROCESSED_DATA_DF, GLOBAL_BEFORE_INTERPOLATION_DATA_DF, GLOBAL_RAW_DATA_DF, GLOBAL_MISSING_VALUES_AFTER_INTERPOLATION_DF, GLOBAL_MISSING_VALUES_BEFORE_INTERPOLATION_DF

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
#         if GLOBAL_RAW_DATA_DF.empty:
#             GLOBAL_RAW_DATA_DF = batch_df
#         else:
#             # Supprime les données existantes pour les stations mises à jour avant de concaténer
#             stations_to_update = batch_df['Station'].unique()
#             GLOBAL_RAW_DATA_DF = GLOBAL_RAW_DATA_DF[
#                 ~GLOBAL_RAW_DATA_DF['Station'].isin(stations_to_update)
#             ]
#             GLOBAL_RAW_DATA_DF = pd.concat(
#                 [GLOBAL_RAW_DATA_DF, batch_df]
#             ).sort_index()

#         GLOBAL_BEFORE_INTERPOLATION_DATA_DF , GLOBAL_PROCESSED_DATA_DF, GLOBAL_MISSING_VALUES_BEFORE_INTERPOLATION_DF, GLOBAL_MISSING_VALUES_AFTER_INTERPOLATION_DF= interpolation(
#             GLOBAL_RAW_DATA_DF,
#             DATA_LIMITS,
#             GLOBAL_GPS_DATA_DF
#         )

#         # Appliquer la fonction de traitement des outliers
#         GLOBAL_PROCESSED_DATA_DF = traiter_outliers_meteo(GLOBAL_PROCESSED_DATA_DF, colonnes=DATA_LIMITS.keys())

#         new_rows = len(batch_df)
#         stations_added = ', '.join(batch_df['Station'].unique())
#         # Traduit en français, puis balise (avec variables nommées)
#         flash(
#             _("%(new_rows)s nouvelles lignes traitées pour les stations : %(stations_added)s") % {'new_rows': new_rows, 'stations_added': stations_added},
#             'success'
#         )

#         #return redirect(url_for('preprocessing')) # Redirige vers la prévisualisation des données
#         return redirect(url_for('preprocessing')) # Redirige vers la prévisualisation des données


#     except Exception as e:
#         app.logger.error(f"Erreur critique lors du traitement global des données: {str(e)}", exc_info=True)
#         # Traduit en français, puis balise (avec interpolation)
#         flash(_('Erreur critique lors du traitement des données : %s') % str(e), 'error')
#         return redirect(url_for('index'))




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

            # Passage de GLOBAL_BEFORE_INTERPOLATION_DATA_DF
            fig = generer_graphique_comparatif(
                df=GLOBAL_PROCESSED_DATA_DF,
                variable=variable,
                periode=periode_key,
                colors=CUSTOM_STATION_COLORS,
                metadata=METADATA_VARIABLES,
                before_interpolation_df=GLOBAL_BEFORE_INTERPOLATION_DATA_DF
            )

            var_meta = METADATA_VARIABLES.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
            var_label = str(get_var_label(var_meta, 'Nom')) 
            
            title = _("Comparaison de %(variable_name)s (%(period)s)") % {
                'variable_name': var_label,
                'period': translated_periode
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

            # Passage de GLOBAL_BEFORE_INTERPOLATION_DATA_DF
            fig = generer_graphique_par_variable_et_periode(
                df=GLOBAL_PROCESSED_DATA_DF,
                station=station,
                variables=variables,
                periode=periode_key,
                colors=PALETTE_DEFAUT,
                metadata=METADATA_VARIABLES,
                before_interpolation_df=GLOBAL_BEFORE_INTERPOLATION_DATA_DF
            )
            title = _("Évolution des variables pour %(station)s (%(period)s)") % {
                'station': station,
                'period': translated_periode
            }

        # ... (le reste du code pour la gestion des erreurs et le rendu)
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

        # Passage de GLOBAL_BEFORE_INTERPOLATION_DATA_DF
        fig = generate_multi_variable_station_plot(
            df=GLOBAL_PROCESSED_DATA_DF,
            station=station,
            variables=variables,
            periode=periode_key,
            colors=PALETTE_DEFAUT,
            metadata=METADATA_VARIABLES,
            before_interpolation_df=GLOBAL_BEFORE_INTERPOLATION_DATA_DF
        )
        
        # ... (le reste du code pour la gestion des erreurs et le rendu)
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

# ... (le reste du code est inchangé)
############ Fin  du code fonctionnel ###################


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


from flask import Flask, render_template, redirect, url_for, flash, send_file
import pandas as pd
import io
from io import BytesIO

@app.route('/download_csv')
def download_csv():
    """Permet de télécharger les données interpolées au format CSV, sans colonnes inutiles."""
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



# ########## Debut de la route pour la visualisation des statistiques quotidiennes ###########



# @app.route('/statistiques', methods=['GET', 'POST'])
# # @login_required # Uncomment if you use a login system
# def statistiques():
#     """Displays statistics for a selected variable, aggregated by different periods."""

#     if GLOBAL_PROCESSED_DATA_DF.empty:
#         flash(_('No data available. Please upload files first.'), 'error')
#         return redirect(url_for('index'))

#     df_original_to_pass = None
#     if not GLOBAL_BEFORE_INTERPOLATION_DATA_DF.empty:
#         df_original_to_pass = GLOBAL_BEFORE_INTERPOLATION_DATA_DF
#     else:
#         app.logger.warning("GLOBAL_BEFORE_INTERPOLATION_DATA_DF is empty. Max/Min will not be verified against raw data, and some annual rain stats might be inaccurate.")

#     try:
#         variable = request.args.get('variable') if request.method == 'GET' else request.form.get('variable')

#         if not variable or variable not in METADATA_VARIABLES:
#             flash(_('Invalid or unknown variable. Please select a valid variable.'), 'error')
#             return redirect(url_for('visualisations_options'))

#         var_meta = METADATA_VARIABLES.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
#         var_label = str(get_var_label(var_meta, 'Nom'))
#         var_unit = str(get_var_label(var_meta, 'Unite'))
#         is_rain_variable = var_meta.get('is_rain', False)

#         plots_html = {}
#         plots_html_rain_yearly_detailed = None
#         plots_html_rain_yearly_summary = None

#         # --- Handle Yearly Plot Generation ---
#         yearly_plot_output = generate_plot_stats_over_period_plotly(
#             df=GLOBAL_PROCESSED_DATA_DF,
#             variable=variable,
#             station_colors=CUSTOM_STATION_COLORS,
#             time_frequency='yearly',
#             df_original=df_original_to_pass,
#             logger=app.logger
#         )

#         if is_rain_variable and isinstance(yearly_plot_output, tuple):
#             fig_yearly_detailed_rain, fig_yearly_summary_rain = yearly_plot_output
#             app.logger.info(f"Unpacked two figures for yearly rain data: {variable}")

#             if fig_yearly_detailed_rain and (fig_yearly_detailed_rain.data and any(trace.visible != 'legendonly' for trace in fig_yearly_detailed_rain.data)):
#                 plots_html_rain_yearly_detailed = fig_yearly_detailed_rain.to_html(full_html=False, include_plotlyjs='cdn')
#                 app.logger.info(f"Detailed yearly rain plot for {variable} generated successfully.")
#             else:
#                 app.logger.info(f"No visible data traces for detailed yearly rain plot of {variable}.")

#             if fig_yearly_summary_rain and (fig_yearly_summary_rain.data and any(trace.x is not None for trace in fig_yearly_summary_rain.data)):
#                 plots_html_rain_yearly_summary = fig_yearly_summary_rain.to_html(full_html=False, include_plotlyjs='cdn')
#                 app.logger.info(f"Summary yearly rain plot for {variable} generated successfully.")
#             else:
#                 app.logger.info(f"No visible data traces for summary yearly rain plot of {variable}.")

#         else: # Generic yearly plot (single 3-subplot figure)
#             fig_yearly_generic = yearly_plot_output
#             app.logger.info(f"Received single generic figure for yearly data: {variable}")
#             if fig_yearly_generic and (fig_yearly_generic.data and any(trace.x is not None for trace in fig_yearly_generic.data)):
#                 plots_html['yearly'] = fig_yearly_generic.to_html(full_html=False, include_plotlyjs='cdn')
#                 app.logger.info(f"Generic yearly plot for {variable} generated successfully.")
#             else:
#                 app.logger.info(f"No visible data traces for generic yearly plot of {variable}.")


#         # 6. Generate plots for other frequencies (Monthly, Weekly, Daily)
#         for freq in ['monthly', 'weekly', 'daily']:
#             fig_freq = generate_plot_stats_over_period_plotly(
#                 df=GLOBAL_PROCESSED_DATA_DF, variable=variable, station_colors=CUSTOM_STATION_COLORS, time_frequency=freq, logger=app.logger
#             )
#             if fig_freq and (fig_freq.data and any(trace.x is not None for trace in fig_freq.data)):
#                 plots_html[freq] = fig_freq.to_html(full_html=False, include_plotlyjs='cdn')
#                 app.logger.info(f"Plot for {variable} at {freq} frequency generated successfully.")
#             else:
#                 app.logger.info(f"No visible data traces for plot of {variable} at {freq} frequency.")

#         if not (any(plots_html.values()) or plots_html_rain_yearly_detailed or plots_html_rain_yearly_summary):
#             flash(_('No statistics available for this variable. Please refine your selection or check the data.'), 'warning')
#             return redirect(url_for('visualisations_options'))

#         return render_template('statistiques.html',
#                                variable_name=var_label,
#                                unit=var_unit,
#                                plots_html=plots_html,
#                                plots_html_rain_yearly_detailed=plots_html_rain_yearly_detailed,
#                                plots_html_rain_yearly_summary=plots_html_rain_yearly_summary,
#                                variable_selectionnee=variable,
#                                is_rain_variable=is_rain_variable)

#     except Exception as e:
#         app.logger.error(f"ERROR in /statistiques: {str(e)}", exc_info=True)
#         flash(_('Technical error occurred while generating statistics.'), 'error')
#         return redirect(url_for('visualisations_options'))


# @app.route('/statistiques', methods=['GET', 'POST'])
# def statistiques():
#     """Displays statistics for a selected variable, aggregated by different periods."""

#     if GLOBAL_PROCESSED_DATA_DF.empty:
#         flash(_('No data available. Please upload files first.'), 'error')
#         return redirect(url_for('index'))

#     df_original_to_pass = None
#     if not GLOBAL_BEFORE_INTERPOLATION_DATA_DF.empty:
#         df_original_to_pass = GLOBAL_BEFORE_INTERPOLATION_DATA_DF
#     else:
#         app.logger.warning("GLOBAL_BEFORE_INTERPOLATION_DATA_DF is empty. Max/Min will not be verified against raw data, and some annual rain stats might be inaccurate.")

#     try:
#         variable = request.args.get('variable') if request.method == 'GET' else request.form.get('variable')

#         if not variable or variable not in METADATA_VARIABLES:
#             flash(_('Invalid or unknown variable. Please select a valid variable.'), 'error')
#             return redirect(url_for('visualisations_options'))

#         var_meta = METADATA_VARIABLES.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
#         var_label = str(get_var_label(var_meta, 'Nom'))
#         var_unit = str(get_var_label(var_meta, 'Unite'))
#         is_rain_variable = var_meta.get('is_rain', False)

#         plots_html = {}
#         plots_html_rain_yearly_detailed = None
#         plots_html_rain_yearly_summary = []
        
#         # Get all unique stations
#         stations = sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique())
#         if not stations:
#             flash(_('No stations found in the data.'), 'error')
#             return redirect(url_for('visualisations_options'))

#         # --- Handle Yearly Plot Generation ---
#         yearly_plot_output = generate_plot_stats_over_period_plotly(
#             df=GLOBAL_PROCESSED_DATA_DF,
#             variable=variable,
#             station_colors=CUSTOM_STATION_COLORS,
#             time_frequency='yearly',
#             df_original=df_original_to_pass,
#             logger=app.logger
#         )

#         if is_rain_variable and isinstance(yearly_plot_output, tuple):
#             fig_yearly_detailed_rain, fig_yearly_summary_rain = yearly_plot_output
#             app.logger.info(f"Unpacked two figures for yearly rain data: {variable}")

#             if fig_yearly_detailed_rain and (fig_yearly_detailed_rain.data and any(trace.visible != 'legendonly' for trace in fig_yearly_detailed_rain.data)):
#                 plots_html_rain_yearly_detailed = fig_yearly_detailed_rain.to_html(full_html=False, include_plotlyjs='cdn')
#                 app.logger.info(f"Detailed yearly rain plot for {variable} generated successfully.")
#             else:
#                 app.logger.info(f"No visible data traces for detailed yearly rain plot of {variable}.")

#             if fig_yearly_summary_rain and (fig_yearly_summary_rain.data and any(trace.x is not None for trace in fig_yearly_summary_rain.data)):
#                 # Create separate figures for each metric row (6 rows x 3 plots)
#                 metric_info = [
#                     {'title': _('Cumul Annuel des Précipitations'), 'unit': var_unit},
#                     {'title': _('Précipitation Moyenne des Jours Pluvieux'), 'unit': var_unit},
#                     {'title': _('Précipitation Moyenne de la Saison Pluvieuse'), 'unit': var_unit},
#                     {'title': _('Durée de la Saison Pluvieuse'), 'unit': _('jours')},
#                     {'title': _('Plus Longue Durée des Jours Sans Pluie'), 'unit': _('jours')},
#                     {'title': _('Durée de la Sécheresse Définie'), 'unit': _('jours')}
#                 ]

#                 for row_idx in range(6):
#                     row_plots = []
#                     for col_idx in range(3):  # 3 columns: Mean, Max, Min
#                         fig = go.Figure()
                        
#                         # Add all stations' data for this subplot
#                         for station in stations:
#                             trace_idx = row_idx * 3 * len(stations) + col_idx * len(stations) + list(stations).index(station)
#                             if trace_idx < len(fig_yearly_summary_rain.data):
#                                 trace = fig_yearly_summary_rain.data[trace_idx]
#                                 fig.add_trace(trace)
                        
#                         if fig.data:
#                             # Update layout for the subplot
#                             subplot_titles = [
#                                 _('Moyenne sur la période'),
#                                 _('Valeur maximale'),
#                                 _('Valeur minimale')
#                             ]
                            
#                             fig.update_layout(
#                                 title=f"{subplot_titles[col_idx]}",
#                                 plot_bgcolor='white',
#                                 paper_bgcolor='white',
#                                 margin=dict(l=50, r=50, b=80, t=50, pad=4),
#                                 height=400,
#                                 yaxis_title=f"{metric_info[row_idx]['title']} ({metric_info[row_idx]['unit']})",
#                                 legend_title=_('Station')
#                             )
                            
#                             row_plots.append(fig.to_html(
#                                 full_html=False, 
#                                 include_plotlyjs='cdn' if row_idx == 0 and col_idx == 0 else False
#                             ))
                    
#                     if row_plots:
#                         plots_html_rain_yearly_summary.append({
#                             'title': metric_info[row_idx]['title'],
#                             'unit': metric_info[row_idx]['unit'],
#                             'plots': row_plots
#                         })
                
#                 app.logger.info(f"Summary yearly rain plot for {variable} separated into individual rows with all stations.")
#             else:
#                 app.logger.info(f"No visible data traces for summary yearly rain plot of {variable}.")

#         else:  # Generic yearly plot (single 3-subplot figure)
#             fig_yearly_generic = yearly_plot_output
#             app.logger.info(f"Received single generic figure for yearly data: {variable}")
#             if fig_yearly_generic and (fig_yearly_generic.data and any(trace.x is not None for trace in fig_yearly_generic.data)):
#                 plots_html['yearly'] = fig_yearly_generic.to_html(full_html=False, include_plotlyjs='cdn')
#                 app.logger.info(f"Generic yearly plot for {variable} generated successfully.")
#             else:
#                 app.logger.info(f"No visible data traces for generic yearly plot of {variable}.")

#         # Generate plots for other frequencies (Monthly, Weekly, Daily)
#         for freq in ['monthly', 'weekly', 'daily']:
#             fig_freq = generate_plot_stats_over_period_plotly(
#                 df=GLOBAL_PROCESSED_DATA_DF, 
#                 variable=variable, 
#                 station_colors=CUSTOM_STATION_COLORS, 
#                 time_frequency=freq, 
#                 logger=app.logger
#             )
#             if fig_freq and (fig_freq.data and any(trace.x is not None for trace in fig_freq.data)):
#                 plots_html[freq] = fig_freq.to_html(full_html=False, include_plotlyjs='cdn')
#                 app.logger.info(f"Plot for {variable} at {freq} frequency generated successfully.")
#             else:
#                 app.logger.info(f"No visible data traces for plot of {variable} at {freq} frequency.")

#         if not (any(plots_html.values()) or plots_html_rain_yearly_detailed or plots_html_rain_yearly_summary):
#             flash(_('No statistics available for this variable. Please refine your selection or check the data.'), 'warning')
#             return redirect(url_for('visualisations_options'))

#         return render_template('statistiques.html',
#                                variable_name=var_label,
#                                unit=var_unit,
#                                plots_html=plots_html,
#                                plots_html_rain_yearly_detailed=plots_html_rain_yearly_detailed,
#                                plots_html_rain_yearly_summary=plots_html_rain_yearly_summary,
#                                stations=stations,
#                                variable_selectionnee=variable,
#                                CUSTOM_STATION_COLORS=CUSTOM_STATION_COLORS , # Ajoutez cette ligne
#                                is_rain_variable=is_rain_variable)

#     except Exception as e:
#         app.logger.error(f"ERROR in /statistiques: {str(e)}", exc_info=True)
#         flash(_('Technical error occurred while generating statistics.'), 'error')
#         return redirect(url_for('visualisations_options'))
 

@app.route('/statistiques', methods=['GET', 'POST'])
def statistiques():
    """Displays statistics for a selected variable, aggregated by different periods."""

    if GLOBAL_PROCESSED_DATA_DF.empty:
        flash(_('No data available. Please upload files first.'), 'error')
        return redirect(url_for('index'))

    df_original_to_pass = None
    if not GLOBAL_BEFORE_INTERPOLATION_DATA_DF.empty:
        df_original_to_pass = GLOBAL_BEFORE_INTERPOLATION_DATA_DF
    else:
        app.logger.warning("GLOBAL_BEFORE_INTERPOLATION_DATA_DF is empty. Max/Min will not be verified against raw data, and some annual rain stats might be inaccurate.")

    try:
        variable = request.args.get('variable') if request.method == 'GET' else request.form.get('variable')

        if not variable or variable not in METADATA_VARIABLES:
            flash(_('Invalid or unknown variable. Please select a valid variable.'), 'error')
            return redirect(url_for('visualisations_options'))

        var_meta = METADATA_VARIABLES.get(variable, {'Nom': {'fr': variable, 'en': variable}, 'Unite': {'fr': '', 'en': ''}})
        var_label = str(get_var_label(var_meta, 'Nom'))
        var_unit = str(get_var_label(var_meta, 'Unite'))
        is_rain_variable = var_meta.get('is_rain', False)

        plots_html = {}
        plots_html_rain_yearly_detailed = None
        plots_html_rain_yearly_summary = []
        
        stations = sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique())
        if not stations:
            flash(_('No stations found in the data.'), 'error')
            return redirect(url_for('visualisations_options'))

        yearly_plot_output = generate_plot_stats_over_period_plotly(
            df=GLOBAL_PROCESSED_DATA_DF,
            variable=variable,
            station_colors=CUSTOM_STATION_COLORS,
            time_frequency='yearly',
            df_original=df_original_to_pass,
            logger=app.logger
        )

        if is_rain_variable and isinstance(yearly_plot_output, tuple):
            fig_yearly_detailed_rain, fig_yearly_summary_rain = yearly_plot_output
            app.logger.info(f"Unpacked two figures for yearly rain data: {variable}")

            if fig_yearly_detailed_rain and (fig_yearly_detailed_rain.data and any(trace.visible != 'legendonly' for trace in fig_yearly_detailed_rain.data)):
                plots_html_rain_yearly_detailed = fig_yearly_detailed_rain.to_html(full_html=False, include_plotlyjs='cdn')
                app.logger.info(f"Detailed yearly rain plot for {variable} generated successfully.")
            else:
                app.logger.info(f"No visible data traces for detailed yearly rain plot of {variable}.")

            if fig_yearly_summary_rain and (fig_yearly_summary_rain.data and any(trace.x is not None for trace in fig_yearly_summary_rain.data)):
                metric_info = [
                    {'title': _('Cumul Annuel des Précipitations'), 'unit': var_unit},
                    {'title': _('Précipitation Moyenne des Jours Pluvieux'), 'unit': var_unit},
                    {'title': _('Précipitation Moyenne de la Saison Pluvieuse'), 'unit': var_unit},
                    {'title': _('Durée de la Saison Pluvieuse'), 'unit': _('jours')},
                    {'title': _('Plus Longue Durée des Jours Sans Pluie'), 'unit': _('jours')},
                    {'title': _('Durée de la Sécheresse Définie'), 'unit': _('jours')}
                ]

                for row_idx in range(6):
                    row_plots = []
                    for col_idx in range(3):
                        fig = go.Figure()
                        
                        for station in stations:
                            trace_idx = row_idx * 3 * len(stations) + col_idx * len(stations) + list(stations).index(station)
                            if trace_idx < len(fig_yearly_summary_rain.data):
                                trace = fig_yearly_summary_rain.data[trace_idx]
                                fig.add_trace(trace)
                        
                        if fig.data:
                            subplot_titles = [
                                _('Moyenne sur la période'),
                                _('Valeur maximale'),
                                _('Valeur minimale')
                            ]
                            
                            fig.update_layout(
                                title=f"{subplot_titles[col_idx]}",
                                plot_bgcolor='white',
                                paper_bgcolor='white',
                                margin=dict(l=50, r=50, b=80, t=50, pad=4),
                                height=400,
                                yaxis_title=f"{metric_info[row_idx]['title']} ({metric_info[row_idx]['unit']})",
                                legend_title=_('Station')
                            )
                            
                            row_plots.append(fig.to_html(
                                full_html=False, 
                                include_plotlyjs='cdn' if row_idx == 0 and col_idx == 0 else False
                            ))
                    
                    if row_plots:
                        plots_html_rain_yearly_summary.append({
                            'title': metric_info[row_idx]['title'],
                            'unit': metric_info[row_idx]['unit'],
                            'plots': row_plots
                        })
                
                app.logger.info(f"Summary yearly rain plot for {variable} separated into individual rows with all stations.")
            else:
                app.logger.info(f"No visible data traces for summary yearly rain plot of {variable}.")

        else:
            fig_yearly_generic = yearly_plot_output
            app.logger.info(f"Received single generic figure for yearly data: {variable}")
            if fig_yearly_generic and (fig_yearly_generic.data and any(trace.x is not None for trace in fig_yearly_generic.data)):
                plots_html['yearly'] = fig_yearly_generic.to_html(full_html=False, include_plotlyjs='cdn')
                app.logger.info(f"Generic yearly plot for {variable} generated successfully.")
            else:
                app.logger.info(f"No visible data traces for generic yearly plot of {variable}.")

        for freq in ['monthly', 'weekly', 'daily']:
            fig_freq = generate_plot_stats_over_period_plotly(
                df=GLOBAL_PROCESSED_DATA_DF, 
                variable=variable, 
                station_colors=CUSTOM_STATION_COLORS, 
                time_frequency=freq, 
                logger=app.logger
            )
            if fig_freq and (fig_freq.data and any(trace.x is not None for trace in fig_freq.data)):
                plots_html[freq] = fig_freq.to_html(full_html=False, include_plotlyjs='cdn')
                app.logger.info(f"Plot for {variable} at {freq} frequency generated successfully.")
            else:
                app.logger.info(f"No visible data traces for plot of {variable} at {freq} frequency.")

        if not (any(plots_html.values()) or plots_html_rain_yearly_detailed or plots_html_rain_yearly_summary):
            flash(_('No statistics available for this variable. Please refine your selection or check the data.'), 'warning')
            return redirect(url_for('visualisations_options'))

        return render_template('statistiques.html',
                           variable_name=var_label,
                           unit=var_unit,
                           plots_html=plots_html,
                           plots_html_rain_yearly_detailed=plots_html_rain_yearly_detailed,
                           plots_html_rain_yearly_summary=plots_html_rain_yearly_summary,
                           stations=stations,
                           variable_selectionnee=variable,
                           CUSTOM_STATION_COLORS=CUSTOM_STATION_COLORS,
                           is_rain_variable=is_rain_variable)

    except Exception as e:
        app.logger.error(f"ERROR in /statistiques: {str(e)}", exc_info=True)
        flash(_('Technical error occurred while generating statistics.'), 'error')
        return redirect(url_for('visualisations_options'))
    
########### Fin de la route pour la visualisation des statistiques quotidiennes ###########


 #Processing fonctionnel -Debut -#

@app.route('/preprocessing', methods=['POST',  'GET'])
def preprocessing():
    stations = GLOBAL_PROCESSED_DATA_DF['Station'].unique()

    station_selected = request.args.get('station')

    print("--- In preprocessing route (GLOBAL_PROCESSED_DATA_DF) ---")
    print(GLOBAL_PROCESSED_DATA_DF.columns.tolist()) 

    # Liste des stations à afficher dans le select
    #stations = sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique())

    if not station_selected:
        station_selected = stations[0]  # par défaut

    # Filtrer les données selon la station sélectionnée
    df_raw = GLOBAL_BEFORE_INTERPOLATION_DATA_DF[GLOBAL_BEFORE_INTERPOLATION_DATA_DF['Station'] == station_selected]
    df_clean = GLOBAL_PROCESSED_DATA_DF[GLOBAL_PROCESSED_DATA_DF['Station'] == station_selected]

    # Générer les figures Plotly
    fig_missing_before = valeurs_manquantes_viz(df_raw)
    fig_outliers_before = outliers_viz(df_raw)

    fig_missing_after = valeurs_manquantes_viz(df_clean)
    fig_outliers_after = outliers_viz(df_clean)

    # Convertir en HTML
    return render_template(
        'preprocessing.html',
        stations=stations,
        station_selected=station_selected,
        missing_before=fig_missing_before.to_html(full_html=False),
        outliers_before=fig_outliers_before.to_html(full_html=False),
        missing_after=fig_missing_after.to_html(full_html=False),
        outliers_after=fig_outliers_after.to_html(full_html=False),
    )

 #Processing fonctionnel -Fin -#

@app.route('/upload', methods=['POST'])
def upload_file():
    """Gère l'upload et le traitement des fichiers de données."""
    global GLOBAL_PROCESSED_DATA_DF, GLOBAL_BEFORE_INTERPOLATION_DATA_DF, GLOBAL_RAW_DATA_DF, GLOBAL_MISSING_VALUES_AFTER_INTERPOLATION_DF, GLOBAL_MISSING_VALUES_BEFORE_INTERPOLATION_DF

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

    batch_dfs = []

    for file, station in zip(uploaded_files, stations):
        if not file or not allowed_file(file.filename):
            flash(_("Fichier '%s' invalide ou non autorisé.") % file.filename, 'error')
            continue

        try:
            filename = secure_filename(file.filename)
            temp_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(temp_path)

            if filename.lower().endswith('.csv'):
                df = pd.read_csv(temp_path, encoding_errors='replace')
            else:
                df = pd.read_excel(temp_path)

            df['Station'] = station
            df = apply_station_specific_preprocessing(df, station)
            batch_dfs.append(df)

            os.unlink(temp_path)

        except Exception as e:
            app.logger.error(f"Erreur traitement fichier {file.filename}: {str(e)}", exc_info=True)
            flash(_("Erreur lors du traitement du fichier '%s': %s") % (file.filename, str(e)), 'error')
            continue

    if not batch_dfs:
        flash(_('Aucune donnée valide traitée. Veuillez vérifier vos fichiers.'), 'error')
        return redirect(url_for('index'))

    try:
        batch_df = pd.concat(batch_dfs, ignore_index=True)
        batch_df = create_datetime(batch_df)
        batch_df = gestion_doublons(batch_df)
        batch_df = batch_df.set_index('Datetime').sort_index()

        if GLOBAL_RAW_DATA_DF.empty:
            GLOBAL_RAW_DATA_DF = batch_df
        else:
            stations_to_update = batch_df['Station'].unique()
            GLOBAL_RAW_DATA_DF = GLOBAL_RAW_DATA_DF[
                ~GLOBAL_RAW_DATA_DF['Station'].isin(stations_to_update)
            ]
            GLOBAL_RAW_DATA_DF = pd.concat(
                [GLOBAL_RAW_DATA_DF, batch_df]
            ).sort_index()

        GLOBAL_BEFORE_INTERPOLATION_DATA_DF , GLOBAL_PROCESSED_DATA_DF, GLOBAL_MISSING_VALUES_BEFORE_INTERPOLATION_DF, GLOBAL_MISSING_VALUES_AFTER_INTERPOLATION_DF= interpolation(
            GLOBAL_RAW_DATA_DF,
            DATA_LIMITS,
            GLOBAL_GPS_DATA_DF
        )

        GLOBAL_PROCESSED_DATA_DF = traiter_outliers_meteo(GLOBAL_PROCESSED_DATA_DF, colonnes=DATA_LIMITS.keys())

        new_rows = len(batch_df)
        stations_added = ', '.join(batch_df['Station'].unique())
        flash(
            _("%(new_rows)s nouvelles lignes traitées pour les stations : %(stations_added)s") % {'new_rows': new_rows, 'stations_added': stations_added},
            'success'
        )

        return redirect(url_for('data_preview'))

    except Exception as e:
        app.logger.error(f"Erreur critique lors du traitement global des données: {str(e)}", exc_info=True)
        flash(_('Erreur critique lors du traitement des données : %s') % str(e), 'error')
        return redirect(url_for('index'))


if __name__ == '__main__':
    #app.run(debug=True) # Exécute l'application Flask en mode débogage
    app.run(host='0.0.0.0', port=8089, debug=True)

