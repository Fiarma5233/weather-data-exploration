import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import json
import traceback
from werkzeug.utils import secure_filename
from datetime import datetime

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
    generate_variable_summary_plots_for_web,
    apply_station_specific_preprocessing,
    generate_stats_plots,
    generate_daily_stats_plot_plotly,
    traiter_outliers_meteo
)
from data_processing import generate_missing_data_plot, generate_outliers_plot, preprocess_data_for_viz, interpolation

# Importations de la configuration
# Assurez-vous que ces variables sont correctement définies dans votre module config.py
from config import METADATA_VARIABLES, PALETTE_DEFAUT, DATA_LIMITS, ALLOWED_EXTENSIONS, STATIONS_BY_BASSIN, CUSTOM_STATION_COLORS

app = Flask(__name__)
app.secret_key = 'votre_cle_secrete_ici' # Utilisez une clé secrète forte en production
app.config['MAX_CONTENT_LENGTH'] = 64 * 1024 * 1024 # Limite de taille des fichiers à 64 Mo
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['STATIC_FOLDER'] = 'static'

# Variables globales pour stocker les DataFrames en mémoire
GLOBAL_PROCESSED_DATA_DF = pd.DataFrame()
GLOBAL_GPS_DATA_DF = pd.DataFrame()
GLOBAL_RAW_DATA_DF = pd.DataFrame() # Données après chargement et prétraitement spécifique station, AVANT create_datetime/gestion_doublons


# Chargement des données GPS au démarrage de l'application
with app.app_context():
    GLOBAL_GPS_DATA_DF = _load_and_prepare_gps_data()

@app.context_processor
def inject_globals():
    """Injecte des variables globales dans le contexte de tous les templates."""
    return {
        'GLOBAL_PROCESSED_DATA_DF': GLOBAL_PROCESSED_DATA_DF,
        'GLOBAL_GPS_DATA_DF': GLOBAL_GPS_DATA_DF,
        'data_available': not GLOBAL_PROCESSED_DATA_DF.empty, # Indique si des données traitées sont chargées
        'now': datetime.now()
    }

@app.template_filter('number_format')
def number_format(value):
    """Filtre Jinja pour formater les nombres avec des espaces comme séparateurs de milliers."""
    try:
        return "{:,}".format(int(value)).replace(",", " ")
    except (ValueError, TypeError):
        return str(value)

def allowed_file(filename):
    """Vérifie si l'extension du fichier est autorisée."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

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
#         flash('Aucun fichier reçu', 'error')
#         return redirect(url_for('index'))

#     uploaded_files = request.files.getlist('file[]')
#     stations = []
#     # Collecter les noms de station pour chaque fichier
#     for i in range(len(uploaded_files)):
#         station_name = request.form.get(f'station_{i}')
#         if station_name:
#             stations.append(station_name)
#         else:
#             flash('Veuillez sélectionner une station pour chaque fichier.', 'error')
#             return redirect(url_for('index'))

#     if len(uploaded_files) != len(stations):
#         flash('Nombre de fichiers et de stations incompatible', 'error')
#         return redirect(url_for('index'))

#     batch_dfs = [] # Liste pour stocker les DataFrames de chaque fichier uploadé
    
#     for file, station in zip(uploaded_files, stations):
#         if not file or not allowed_file(file.filename):
#             flash(f"Fichier '{file.filename}' invalide ou non autorisé.", 'error')
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
#             flash(f"Erreur traitement fichier '{file.filename}': {str(e)}", 'error')
#             continue

#     if not batch_dfs:
#         flash('Aucune donnée valide traitée. Veuillez vérifier vos fichiers.', 'error')
#         return redirect(url_for('index'))

#     try:
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
#         flash(
#             f"{new_rows} nouvelles lignes traitées pour les stations : {stations_added}",
#             'success'
#         )
        
#         return redirect(url_for('data_preview')) # Redirige vers la prévisualisation des données
        
#     except Exception as e:
#         app.logger.error(f"Erreur critique lors du traitement global des données: {str(e)}", exc_info=True)
#         flash(f'Erreur critique lors du traitement des données: {str(e)}', 'error')
#         return redirect(url_for('index'))


@app.route('/upload', methods=['POST'])
def upload_file():
    """Gère l'upload et le traitement des fichiers de données."""
    global GLOBAL_RAW_DATA_DF, GLOBAL_PROCESSED_DATA_DF 

    if not request.files:
        flash('Aucun fichier reçu', 'error')
        return redirect(url_for('index'))

    uploaded_files = request.files.getlist('file[]')
    stations = []
    for i in range(len(uploaded_files)):
        station_name = request.form.get(f'station_{i}')
        if station_name:
            stations.append(station_name)
        else:
            flash('Veuillez sélectionner une station pour chaque fichier.', 'error')
            return redirect(url_for('index'))

    if len(uploaded_files) != len(stations):
        flash('Nombre de fichiers et de stations incompatible', 'error')
        return redirect(url_for('index'))

    batch_dfs_initial_load = [] # Liste pour stocker les DataFrames après apply_station_specific_preprocessing
    
    for file, station in zip(uploaded_files, stations):
        if not file or not allowed_file(file.filename):
            flash(f"Fichier '{file.filename}' invalide ou non autorisé.", 'error')
            continue
            
        try:
            filename = secure_filename(file.filename)
            temp_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(temp_path)

            if filename.lower().endswith('.csv'):
                df_station = pd.read_csv(temp_path, encoding_errors='replace')
            else: # Supposons .xlsx ou .xls
                df_station = pd.read_excel(temp_path)
            
            df_station['Station'] = station 
            df_station = apply_station_specific_preprocessing(df_station, station)
            batch_dfs_initial_load.append(df_station)
            
            os.unlink(temp_path)
            
        except Exception as e:
            app.logger.error(f"Erreur traitement fichier {file.filename}: {str(e)}", exc_info=True)
            flash(f"Erreur traitement fichier '{file.filename}': {str(e)}", 'error')
            continue

    if not batch_dfs_initial_load:
        flash('Aucune donnée valide traitée. Veuillez vérifier vos fichiers.', 'error')
        return redirect(url_for('index'))

    try:
        # Concatène toutes les DataFrames après apply_station_specific_preprocessing
        new_combined_raw_df = pd.concat(batch_dfs_initial_load, ignore_index=True)
        
        # Applique create_datetime et gestion_doublons pour obtenir le GLOBAL_RAW_DATA_DF
        # C'est l'état des données juste avant l'interpolation
        new_combined_raw_df = create_datetime(new_combined_raw_df)
        new_combined_raw_df = gestion_doublons(new_combined_raw_df)
        new_combined_raw_df = new_combined_raw_df.set_index('Datetime').sort_index()

        # Met à jour GLOBAL_RAW_DATA_DF
        if GLOBAL_RAW_DATA_DF.empty:
            GLOBAL_RAW_DATA_DF = new_combined_raw_df
        else:
            stations_to_update = new_combined_raw_df['Station'].unique()
            GLOBAL_RAW_DATA_DF = GLOBAL_RAW_DATA_DF[
                ~GLOBAL_RAW_DATA_DF['Station'].isin(stations_to_update)
            ]
            GLOBAL_RAW_DATA_DF = pd.concat(
                [GLOBAL_RAW_DATA_DF, new_combined_raw_df]
            ).sort_index()
        
        # Applique l'interpolation complète pour créer GLOBAL_PROCESSED_DATA_DF
        # Nous appelons la fonction originale `interpolation` ici.
        GLOBAL_PROCESSED_DATA_DF = interpolation(
            GLOBAL_RAW_DATA_DF.copy(), # Important: travailler sur une copie
            DATA_LIMITS,
            GLOBAL_GPS_DATA_DF
        )
        
        new_rows = len(new_combined_raw_df)
        stations_added = ', '.join(new_combined_raw_df['Station'].unique())
        flash(
            f"{new_rows} nouvelles lignes traitées pour les stations : {stations_added}",
            'success'
        )
        
        return redirect(url_for('preprocessing')) 
        
    except Exception as e:
        app.logger.error(f"Erreur critique lors du traitement global des données: {str(e)}", exc_info=True)
        flash(f'Erreur critique lors du traitement des données: {str(e)}', 'error')
        return redirect(url_for('index'))


@app.route('/preview')
def data_preview():
    """Affiche un aperçu des données chargées."""
    if GLOBAL_PROCESSED_DATA_DF.empty:
        flash('Aucune donnée disponible. Veuillez uploader des fichiers d\'abord.', 'error')
        return redirect(url_for('index'))
    try:
        stations = GLOBAL_PROCESSED_DATA_DF['Station'].unique()
        
        if len(stations) == 1:
            preview_df = GLOBAL_PROCESSED_DATA_DF.head(20).reset_index()
            preview_type = "20 premières lignes"
        else:
            preview_dfs = []
            for station in stations:
                # Prend les 10 premières lignes par station
                station_df = GLOBAL_PROCESSED_DATA_DF[GLOBAL_PROCESSED_DATA_DF['Station'] == station].head(10).reset_index()
                preview_dfs.append(station_df)
            preview_df = pd.concat(preview_dfs)
            preview_type = f"10 lignes × {len(stations)} stations"
        
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
                            dataset_shape=f"{GLOBAL_PROCESSED_DATA_DF.shape[0]} lignes × {GLOBAL_PROCESSED_DATA_DF.shape[1]} colonnes",
                            stations_count=len(stations))
    
    except Exception as e:
        app.logger.error(f'Erreur lors de la préparation de l\'aperçu: {str(e)}', exc_info=True)
        flash(f'Erreur lors de la préparation de l\'aperçu: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/visualisations_options')
def visualisations_options():
    """Affiche les options pour générer des visualisations."""
    if GLOBAL_PROCESSED_DATA_DF.empty:
        flash('Veuillez uploader des fichiers d\'abord pour accéder aux visualisations.', 'error')
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

@app.route('/generate_plot', methods=['POST'])
def generate_plot():
    """Génère un graphique basé sur les sélections de l'utilisateur."""
    if GLOBAL_PROCESSED_DATA_DF.empty:
        flash('Aucune donnée disponible. Veuillez uploader des fichiers d\'abord.', 'error')
        return redirect(url_for('index'))
    try:
        is_comparative = 'comparative' in request.form
        periode = request.form.get('periode')

        if not periode:
            flash('Veuillez sélectionner une période d\'analyse.', 'error')
            return redirect(url_for('visualisations_options'))

        if is_comparative:
            variable = request.form.get('variable')
            if not variable:
                flash('Veuillez sélectionner une variable pour la comparaison.', 'error')
                return redirect(url_for('visualisations_options'))

            fig = generer_graphique_comparatif(
                df=GLOBAL_PROCESSED_DATA_DF,
                variable=variable,
                periode=periode,
                colors=CUSTOM_STATION_COLORS,
                metadata=METADATA_VARIABLES
            )
            title = f"Comparaison de {METADATA_VARIABLES.get(variable, {}).get('Nom', variable)} ({periode})"
            
        else: # Graphique multi-variables pour une station
            station = request.form.get('station')
            variables = request.form.getlist('variables[]')
            
            if not station:
                flash('Veuillez sélectionner une station.', 'error')
                return redirect(url_for('visualisations_options'))
                
            if not variables:
                flash('Veuillez sélectionner au moins une variable à visualiser.', 'error')
                return redirect(url_for('visualisations_options'))

            fig = generer_graphique_par_variable_et_periode(
                df=GLOBAL_PROCESSED_DATA_DF,
                station=station,
                variables=variables,
                periode=periode,
                colors=PALETTE_DEFAUT,
                metadata=METADATA_VARIABLES
            )
            title = f"Évolution des variables pour {station} ({periode})"

        # Vérifie si la figure a été générée avec des données
        if not fig or not fig.data:
            flash('Aucune donnée disponible pour les critères sélectionnés. Veuillez affiner votre sélection.', 'warning')
            return redirect(url_for('visualisations_options'))

        plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')
        return render_template('plot_display.html',
                            plot_html=plot_html,
                            title=title)

    except Exception as e:
        app.logger.error(f"Erreur lors de la génération du graphique: {str(e)}", exc_info=True)
        flash(f"Erreur lors de la génération du graphique: {str(e)}", 'error')
        return redirect(url_for('visualisations_options'))
    
@app.route('/generate_multi_variable_plot_route', methods=['POST'])
def generate_multi_variable_plot_route():
    """Génère un graphique d'analyse multi-variables normalisées."""
    if GLOBAL_PROCESSED_DATA_DF.empty:
        flash('Aucune donnée disponible. Veuillez uploader des fichiers d\'abord.', 'error')
        return redirect(url_for('index'))
    try:
        station = request.form['station']
        variables = request.form.getlist('variables[]')
        periode = request.form.get('periode', 'Brutes') # Valeur par défaut 'Brutes'
        
        if not station or not variables:
            flash('Veuillez sélectionner une station et au moins une variable pour l\'analyse normalisée.', 'error')
            return redirect(url_for('visualisations_options'))

        fig = generate_multi_variable_station_plot(
            df=GLOBAL_PROCESSED_DATA_DF,
            station=station,
            variables=variables,
            periode=periode,
            colors=PALETTE_DEFAUT,
            metadata=METADATA_VARIABLES
        )
        
        # Vérifie si la figure a été générée avec des données
        if not fig or not fig.data:
            flash('Aucune donnée disponible pour les critères sélectionnés. Veuillez affiner votre sélection.', 'warning')
            return redirect(url_for('visualisations_options'))

        plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')
        
        return render_template('plot_display.html',
                            plot_html=plot_html,
                            title=f"Analyse Multi-Variables Normalisées - {station} ({periode})")
    
    except Exception as e:
        app.logger.error(f"Erreur lors de la génération du graphique normalisé: {str(e)}", exc_info=True)
        flash(f"Erreur lors de la génération du graphique normalisé: {str(e)}", 'error')
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
        flash('Toutes les données ont été réinitialisées avec succès. Vous pouvez maintenant télécharger de nouveaux datasets.', 'success')
        
        # 4. Répondre avec une URL de redirection pour le frontend
        return jsonify(success=True, redirect_url=url_for('index'))

    except Exception as e:
        app.logger.error(f"Erreur critique lors de la réinitialisation des données: {str(e)}", exc_info=True)
        flash(f'Erreur lors de la réinitialisation des données: {str(e)}', 'error')
        # En cas d'erreur, tenter quand même de rediriger l'utilisateur vers la page d'accueil
        return jsonify(success=False, message=f'Erreur interne lors de la réinitialisation: {str(e)}', redirect_url=url_for('index')), 500
        

@app.route('/statistiques', methods=['GET', 'POST'])
def statistiques():
    """Affiche des statistiques quotidiennes pour une variable sélectionnée."""
    if GLOBAL_PROCESSED_DATA_DF.empty:
        flash('Aucune donnée disponible. Veuillez uploader des fichiers d\'abord.', 'error')
        return redirect(url_for('index'))
    try:
        variable = request.args.get('variable') if request.method == 'GET' else request.form.get('variable')
        
        if not variable or variable not in GLOBAL_PROCESSED_DATA_DF.columns:
            flash('Variable invalide ou non trouvée. Veuillez sélectionner une variable valide.', 'error')
            return redirect(url_for('visualisations_options'))

        fig = generate_daily_stats_plot_plotly(
            df=GLOBAL_PROCESSED_DATA_DF,
            variable=variable,
            station_colors=CUSTOM_STATION_COLORS
        )
        
        # Vérifie si la figure a été générée avec des données
        if not fig or not fig.data:
            flash('Aucune statistique disponible pour cette variable. Veuillez affiner votre sélection ou vérifier les données.', 'warning')
            return redirect(url_for('visualisations_options'))

        plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

        var_meta = METADATA_VARIABLES.get(variable, {'Nom': variable, 'Unite': ''})
        return render_template('statistiques.html',
                            variable_name=var_meta.get('Nom', variable),
                            unit=var_meta.get('Unite', ''),
                            plot_html=plot_html,
                            variable_selectionnee=variable)

    except Exception as e:
        app.logger.error(f"ERREUR dans /statistiques: {str(e)}", exc_info=True)
        traceback.print_exc() # Affiche la trace complète de l'erreur dans les logs du serveur
        flash('Erreur technique lors de la génération des statistiques.', 'error')
        return redirect(url_for('visualisations_options'))


from data_processing import generate_missing_data_plot, generate_outliers_plot, preprocess_data_for_viz, interpolation

@app.route('/preprocessing')
def preprocessing():
    """Affiche les visualisations avant/après prétraitement (interpolation et outliers)."""
    if GLOBAL_RAW_DATA_DF.empty: # Utilise GLOBAL_RAW_DATA_DF pour vérifier la présence de données brutes
        flash('Veuillez uploader des fichiers d\'abord pour accéder aux visualisations de prétraitement.', 'error')
        return redirect(url_for('index'))
    
    stations = sorted(GLOBAL_RAW_DATA_DF['Station'].unique().tolist())
    station_selected = request.args.get('station', stations[0] if stations else None)

    # --- Préparation des données AVANT interpolation ---
    # Nous utilisons GLOBAL_RAW_DATA_DF, qui a déjà 'Datetime' comme index et les doublons gérés.
    # Ensuite, nous appliquons 'preprocess_data_for_viz' pour obtenir Is_Daylight, Rain_mm,
    # et remplacer les outliers par NaN, mais SANS interpoler.
    df_for_before_plots = preprocess_data_for_viz(
        GLOBAL_RAW_DATA_DF.copy(), # Travailler sur une copie
        DATA_LIMITS,
        GLOBAL_GPS_DATA_DF
    )
    
    # Génère les graphiques AVANT interpolation
    missing_before_fig = generate_missing_data_plot(df_for_before_plots, station_selected)
    outliers_before_fig = generate_outliers_plot(df_for_before_plots, DATA_LIMITS, station_selected)
    
    # --- Préparation des données APRÈS interpolation ---
    # Ces données sont GLOBAL_PROCESSED_DATA_DF, qui est déjà passé par la fonction `interpolation` complète lors de l'upload
    df_for_after_plots = GLOBAL_PROCESSED_DATA_DF.copy()

    # Génère les graphiques APRÈS interpolation
    missing_after_fig = generate_missing_data_plot(df_for_after_plots, station_selected)

    outliers_traitment = traiter_outliers_meteo(df_for_after_plots, DATA_LIMITS)
    outliers_after_fig = generate_outliers_plot(outliers_traitment, DATA_LIMITS, station_selected)
    
    return render_template('preprocessing.html',
                         stations=stations,
                         station_selected=station_selected,
                         missing_before=missing_before_fig.to_html(full_html=False, include_plotlyjs='cdn'),
                         outliers_before=outliers_before_fig.to_html(full_html=False, include_plotlyjs='cdn'),
                         missing_after=missing_after_fig.to_html(full_html=False, include_plotlyjs='cdn'),
                         outliers_after=outliers_after_fig.to_html(full_html=False, include_plotlyjs='cdn'))

if __name__ == '__main__':
    app.run(debug=True) # Exécute l'application Flask en mode débogage















# # app.py

# import os
# import pandas as pd
# from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
# import plotly.graph_objects as go
# import plotly.express as px
# from plotly.subplots import make_subplots
# import json
# import traceback
# from werkzeug.utils import secure_filename
# from datetime import datetime

# # Importations des fonctions de traitement
# from data_processing import (
#     create_datetime,
#     interpolation, # Gardons le nom original, cette fonction fait l'interpolation complète
#     _load_and_prepare_gps_data,
#     gestion_doublons,
#     calculate_daily_summary_table,
#     generer_graphique_par_variable_et_periode,
#     generer_graphique_comparatif,
#     generate_multi_variable_station_plot,
#     generate_variable_summary_plots_for_web,
#     apply_station_specific_preprocessing,
#     generate_stats_plots,
#     generate_daily_stats_plot_plotly,
#     traiter_outliers_meteo,
    
#     # Fonctions mises à jour/nouvelles pour le prétraitement et la visualisation
#     generate_missing_data_plot, 
#     generate_outliers_plot,
#     preprocess_data_for_viz # La nouvelle fonction sans interpolation active
# )

# # Importations de la configuration
# from config import METADATA_VARIABLES, PALETTE_DEFAUT, DATA_LIMITS, ALLOWED_EXTENSIONS, STATIONS_BY_BASSIN, CUSTOM_STATION_COLORS

# app = Flask(__name__)
# app.secret_key = 'votre_cle_secrete_ici' 
# app.config['MAX_CONTENT_LENGTH'] = 64 * 1024 * 1024 
# app.config['UPLOAD_FOLDER'] = 'uploads'
# app.config['STATIC_FOLDER'] = 'static'

# # Variables globales pour stocker les DataFrames en mémoire
# GLOBAL_RAW_DATA_DF = pd.DataFrame() # Données après chargement et prétraitement spécifique station, AVANT create_datetime/gestion_doublons
# GLOBAL_PROCESSED_DATA_DF = pd.DataFrame() # Données après tout le prétraitement (date, doublons, interpolation)
# GLOBAL_GPS_DATA_DF = pd.DataFrame()

# # Chargement des données GPS au démarrage de l'application
# with app.app_context():
#     GLOBAL_GPS_DATA_DF = _load_and_prepare_gps_data()

# @app.context_processor
# def inject_globals():
#     """Injecte des variables globales dans le contexte de tous les templates."""
#     return {
#         'GLOBAL_PROCESSED_DATA_DF': GLOBAL_PROCESSED_DATA_DF,
#         'GLOBAL_GPS_DATA_DF': GLOBAL_GPS_DATA_DF,
#         'data_available': not GLOBAL_PROCESSED_DATA_DF.empty, 
#         'now': datetime.now()
#     }

# @app.template_filter('number_format')
# def number_format(value):
#     """Filtre Jinja pour formater les nombres avec des espaces comme séparateurs de milliers."""
#     try:
#         return "{:,}".format(int(value)).replace(",", " ")
#     except (ValueError, TypeError):
#         return str(value)

# def allowed_file(filename):
#     """Vérifie si l'extension du fichier est autorisée."""
#     return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# @app.route('/')
# def index():
#     """Route pour la page d'accueil, affichant le formulaire d'upload."""
#     os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True) 
#     return render_template('index.html',
#                          bassins=sorted(STATIONS_BY_BASSIN.keys()),
#                          stations_by_bassin=STATIONS_BY_BASSIN)

# @app.route('/upload', methods=['POST'])
# def upload_file():
#     """Gère l'upload et le traitement des fichiers de données."""
#     global GLOBAL_RAW_DATA_DF, GLOBAL_PROCESSED_DATA_DF 

#     if not request.files:
#         flash('Aucun fichier reçu', 'error')
#         return redirect(url_for('index'))

#     uploaded_files = request.files.getlist('file[]')
#     stations = []
#     for i in range(len(uploaded_files)):
#         station_name = request.form.get(f'station_{i}')
#         if station_name:
#             stations.append(station_name)
#         else:
#             flash('Veuillez sélectionner une station pour chaque fichier.', 'error')
#             return redirect(url_for('index'))

#     if len(uploaded_files) != len(stations):
#         flash('Nombre de fichiers et de stations incompatible', 'error')
#         return redirect(url_for('index'))

#     batch_dfs_initial_load = [] # Liste pour stocker les DataFrames après apply_station_specific_preprocessing
    
#     for file, station in zip(uploaded_files, stations):
#         if not file or not allowed_file(file.filename):
#             flash(f"Fichier '{file.filename}' invalide ou non autorisé.", 'error')
#             continue
            
#         try:
#             filename = secure_filename(file.filename)
#             temp_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#             file.save(temp_path)

#             if filename.lower().endswith('.csv'):
#                 df_station = pd.read_csv(temp_path, encoding_errors='replace')
#             else: # Supposons .xlsx ou .xls
#                 df_station = pd.read_excel(temp_path)
            
#             df_station['Station'] = station 
#             df_station = apply_station_specific_preprocessing(df_station, station)
#             batch_dfs_initial_load.append(df_station)
            
#             os.unlink(temp_path)
            
#         except Exception as e:
#             app.logger.error(f"Erreur traitement fichier {file.filename}: {str(e)}", exc_info=True)
#             flash(f"Erreur traitement fichier '{file.filename}': {str(e)}", 'error')
#             continue

#     if not batch_dfs_initial_load:
#         flash('Aucune donnée valide traitée. Veuillez vérifier vos fichiers.', 'error')
#         return redirect(url_for('index'))

#     try:
#         # Concatène toutes les DataFrames après apply_station_specific_preprocessing
#         new_combined_raw_df = pd.concat(batch_dfs_initial_load, ignore_index=True)
        
#         # Applique create_datetime et gestion_doublons pour obtenir le GLOBAL_RAW_DATA_DF
#         # C'est l'état des données juste avant l'interpolation
#         new_combined_raw_df = create_datetime(new_combined_raw_df)
#         new_combined_raw_df = gestion_doublons(new_combined_raw_df)
#         new_combined_raw_df = new_combined_raw_df.set_index('Datetime').sort_index()

#         # Met à jour GLOBAL_RAW_DATA_DF
#         if GLOBAL_RAW_DATA_DF.empty:
#             GLOBAL_RAW_DATA_DF = new_combined_raw_df
#         else:
#             stations_to_update = new_combined_raw_df['Station'].unique()
#             GLOBAL_RAW_DATA_DF = GLOBAL_RAW_DATA_DF[
#                 ~GLOBAL_RAW_DATA_DF['Station'].isin(stations_to_update)
#             ]
#             GLOBAL_RAW_DATA_DF = pd.concat(
#                 [GLOBAL_RAW_DATA_DF, new_combined_raw_df]
#             ).sort_index()
        
#         # Applique l'interpolation complète pour créer GLOBAL_PROCESSED_DATA_DF
#         # Nous appelons la fonction originale `interpolation` ici.
#         GLOBAL_PROCESSED_DATA_DF = interpolation(
#             GLOBAL_RAW_DATA_DF.copy(), # Important: travailler sur une copie
#             DATA_LIMITS,
#             GLOBAL_GPS_DATA_DF
#         )
        
#         new_rows = len(new_combined_raw_df)
#         stations_added = ', '.join(new_combined_raw_df['Station'].unique())
#         flash(
#             f"{new_rows} nouvelles lignes traitées pour les stations : {stations_added}",
#             'success'
#         )
        
#         return redirect(url_for('preprocessing')) 
        
#     except Exception as e:
#         app.logger.error(f"Erreur critique lors du traitement global des données: {str(e)}", exc_info=True)
#         flash(f'Erreur critique lors du traitement des données: {str(e)}', 'error')
#         return redirect(url_for('index'))

# @app.route('/preview')
# def data_preview():
#     """Affiche un aperçu des données brutes chargées."""
#     if GLOBAL_RAW_DATA_DF.empty: 
#         flash('Aucune donnée brute disponible. Veuillez uploader des fichiers d\'abord.', 'error')
#         return redirect(url_for('index'))
#     try:
#         stations = GLOBAL_RAW_DATA_DF['Station'].unique()
        
#         if len(stations) == 1:
#             preview_df = GLOBAL_RAW_DATA_DF.head(20).reset_index() # .reset_index() pour afficher Datetime comme colonne
#             preview_type = "20 premières lignes"
#         else:
#             preview_dfs = []
#             for station in stations:
#                 station_df = GLOBAL_RAW_DATA_DF[GLOBAL_RAW_DATA_DF['Station'] == station].head(10).reset_index()
#                 preview_dfs.append(station_df)
#             preview_df = pd.concat(preview_dfs)
#             preview_type = f"10 lignes × {len(stations)} stations"
        
#         preview_html = preview_df.to_html(
#             classes='table table-striped table-hover',
#             index=False,
#             border=0,
#             justify='left',
#             na_rep='NaN',
#             max_rows=None
#         )
        
#         return render_template('preview.html',
#                             preview_table=preview_html,
#                             preview_type=preview_type,
#                             dataset_shape=f"{GLOBAL_RAW_DATA_DF.shape[0]} lignes × {GLOBAL_RAW_DATA_DF.shape[1]} colonnes",
#                             stations_count=len(stations))
    
#     except Exception as e:
#         app.logger.error(f'Erreur lors de la préparation de l\'aperçu: {str(e)}', exc_info=True)
#         flash(f'Erreur lors de la préparation de l\'aperçu: {str(e)}', 'error')
#         return redirect(url_for('index'))

# @app.route('/visualisations_options')
# def visualisations_options():
#     """Affiche les options pour générer des visualisations."""
#     if GLOBAL_PROCESSED_DATA_DF.empty:
#         flash('Veuillez uploader des fichiers d\'abord pour accéder aux visualisations.', 'error')
#         return redirect(url_for('index'))

#     excluded_cols = {'Station', 'Is_Daylight', 'Daylight_Duration',
#                    'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date', 'Rain_01_mm', 'Rain_02_mm'}
    
#     available_vars = [
#         col for col in GLOBAL_PROCESSED_DATA_DF.columns
#         if col not in excluded_cols and pd.api.types.is_numeric_dtype(GLOBAL_PROCESSED_DATA_DF[col])
#     ]

#     daily_stats_df = calculate_daily_summary_table(GLOBAL_PROCESSED_DATA_DF)
#     daily_stats_html = daily_stats_df.to_html(classes='table table-striped', index=False)

#     variable_selectionnee = request.args.get('variable')
#     variables_selectionnees = request.args.getlist('variables[]')
#     station_selectionnee = request.args.get('station')
#     periode_selectionnee = request.args.get('periode')

#     return render_template('visualisations_options.html',
#                         stations=sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique()),
#                         variables=sorted(available_vars),
#                         METADATA_VARIABLES=METADATA_VARIABLES,
#                         PALETTE_DEFAUT=PALETTE_DEFAUT,
#                         daily_stats_table=daily_stats_html,
#                         variable_selectionnee=variable_selectionnee,
#                         variables_selectionnees=variables_selectionnees,
#                         station_selectionnee=station_selectionnee,
#                         periode_selectionnee=periode_selectionnee)

# @app.route('/generate_plot', methods=['POST'])
# def generate_plot():
#     """Génère un graphique basé sur les sélections de l'utilisateur."""
#     if GLOBAL_PROCESSED_DATA_DF.empty:
#         flash('Aucune donnée disponible. Veuillez uploader des fichiers d\'abord.', 'error')
#         return redirect(url_for('index'))
#     try:
#         is_comparative = 'comparative' in request.form
#         periode = request.form.get('periode')

#         if not periode:
#             flash('Veuillez sélectionner une période d\'analyse.', 'error')
#             return redirect(url_for('visualisations_options'))

#         if is_comparative:
#             variable = request.form.get('variable')
#             if not variable:
#                 flash('Veuillez sélectionner une variable pour la comparaison.', 'error')
#                 return redirect(url_for('visualisations_options'))

#             fig = generer_graphique_comparatif(
#                 df=GLOBAL_PROCESSED_DATA_DF,
#                 variable=variable,
#                 periode=periode,
#                 colors=CUSTOM_STATION_COLORS,
#                 metadata=METADATA_VARIABLES
#             )
#             title = f"Comparaison de {METADATA_VARIABLES.get(variable, {}).get('Nom', variable)} ({periode})"
            
#         else: # Graphique multi-variables pour une station
#             station = request.form.get('station')
#             variables = request.form.getlist('variables[]')
            
#             if not station:
#                 flash('Veuillez sélectionner une station.', 'error')
#                 return redirect(url_for('visualisations_options'))
                
#             if not variables:
#                 flash('Veuillez sélectionner au moins une variable à visualiser.', 'error')
#                 return redirect(url_for('visualisations_options'))

#             fig = generer_graphique_par_variable_et_periode(
#                 df=GLOBAL_PROCESSED_DATA_DF,
#                 station=station,
#                 variables=variables,
#                 periode=periode,
#                 colors=PALETTE_DEFAUT,
#                 metadata=METADATA_VARIABLES
#             )
#             title = f"Évolution des variables pour {station} ({periode})"

#         if not fig or not fig.data:
#             flash('Aucune donnée disponible pour les critères sélectionnés. Veuillez affiner votre sélection.', 'warning')
#             return redirect(url_for('visualisations_options'))

#         plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')
#         return render_template('plot_display.html',
#                             plot_html=plot_html,
#                             title=title)

#     except Exception as e:
#         app.logger.error(f"Erreur lors de la génération du graphique: {str(e)}", exc_info=True)
#         flash(f"Erreur lors de la génération du graphique: {str(e)}", 'error')
#         return redirect(url_for('visualisations_options'))
    
# @app.route('/generate_multi_variable_plot_route', methods=['POST'])
# def generate_multi_variable_plot_route():
#     """Génère un graphique d'analyse multi-variables normalisées."""
#     if GLOBAL_PROCESSED_DATA_DF.empty:
#         flash('Aucune donnée disponible. Veuillez uploader des fichiers d\'abord.', 'error')
#         return redirect(url_for('index'))
#     try:
#         station = request.form['station']
#         variables = request.form.getlist('variables[]')
#         periode = request.form.get('periode', 'Brutes')
        
#         if not station or not variables:
#             flash('Veuillez sélectionner une station et au moins une variable pour l\'analyse normalisée.', 'error')
#             return redirect(url_for('visualisations_options'))

#         fig = generate_multi_variable_station_plot(
#             df=GLOBAL_PROCESSED_DATA_DF,
#             station=station,
#             variables=variables,
#             periode=periode,
#             colors=PALETTE_DEFAUT,
#             metadata=METADATA_VARIABLES
#         )
        
#         if not fig or not fig.data:
#             flash('Aucune donnée disponible pour les critères sélectionnés. Veuillez affiner votre sélection.', 'warning')
#             return redirect(url_for('visualisations_options'))

#         plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')
        
#         return render_template('plot_display.html',
#                             plot_html=plot_html,
#                             title=f"Analyse Multi-Variables Normalisées - {station} ({periode})")
    
#     except Exception as e:
#         app.logger.error(f"Erreur lors de la génération du graphique normalisé: {str(e)}", exc_info=True)
#         flash(f"Erreur lors de la génération du graphique normalisé: {str(e)}", 'error')
#         return redirect(url_for('visualisations_options'))
    

# @app.route('/reset_data', methods=['POST'])
# def reset_data():
#     """Réinitialise toutes les données chargées en mémoire et supprime les fichiers temporaires."""
#     global GLOBAL_PROCESSED_DATA_DF, GLOBAL_RAW_DATA_DF 
#     try:
#         GLOBAL_PROCESSED_DATA_DF = pd.DataFrame()
#         GLOBAL_RAW_DATA_DF = pd.DataFrame() 

#         for filename in os.listdir(app.config['UPLOAD_FOLDER']):
#             file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#             try:
#                 if os.path.isfile(file_path):
#                     os.unlink(file_path)
#             except Exception as e:
#                 app.logger.warning(f"Impossible de supprimer le fichier résiduel {file_path}: {e}")

#         flash('Toutes les données ont été réinitialisées avec succès. Vous pouvez maintenant télécharger de nouveaux datasets.', 'success')
        
#         return jsonify(success=True, redirect_url=url_for('index'))

#     except Exception as e:
#         app.logger.error(f"Erreur critique lors de la réinitialisation des données: {str(e)}", exc_info=True)
#         flash(f'Erreur lors de la réinitialisation des données: {str(e)}', 'error')
#         return jsonify(success=False, message=f'Erreur interne lors de la réinitialisation: {str(e)}', redirect_url=url_for('index')), 500
        

# @app.route('/statistiques', methods=['GET', 'POST'])
# def statistiques():
#     """Affiche des statistiques quotidiennes pour une variable sélectionnée."""
#     if GLOBAL_PROCESSED_DATA_DF.empty:
#         flash('Aucune donnée disponible. Veuillez uploader des fichiers d\'abord.', 'error')
#         return redirect(url_for('index'))
#     try:
#         variable = request.args.get('variable') if request.method == 'GET' else request.form.get('variable')
        
#         if not variable or variable not in GLOBAL_PROCESSED_DATA_DF.columns:
#             flash('Variable invalide ou non trouvée. Veuillez sélectionner une variable valide.', 'error')
#             return redirect(url_for('visualisations_options'))

#         fig = generate_daily_stats_plot_plotly(
#             df=GLOBAL_PROCESSED_DATA_DF,
#             variable=variable,
#             station_colors=CUSTOM_STATION_COLORS
#         )
        
#         if not fig or not fig.data:
#             flash('Aucune statistique disponible pour cette variable. Veuillez affiner votre sélection ou vérifier les données.', 'warning')
#             return redirect(url_for('visualisations_options'))

#         plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

#         var_meta = METADATA_VARIABLES.get(variable, {'Nom': variable, 'Unite': ''})
#         return render_template('statistiques.html',
#                             variable_name=var_meta.get('Nom', variable),
#                             unit=var_meta.get('Unite', ''),
#                             plot_html=plot_html,
#                             variable_selectionnee=variable)

#     except Exception as e:
#         app.logger.error(f"ERREUR dans /statistiques: {str(e)}", exc_info=True)
#         traceback.print_exc() 
#         flash('Erreur technique lors de la génération des statistiques.', 'error')
#         return redirect(url_for('visualisations_options'))


# # --- ROUTE POUR LE PRÉTRAITEMENT (MIS À JOUR) ---
# # Importez les fonctions mises à jour de data_processing
# from data_processing import generate_missing_data_plot, generate_outliers_plot, preprocess_data_for_viz, interpolation

# @app.route('/preprocessing')
# def preprocessing():
#     """Affiche les visualisations avant/après prétraitement (interpolation et outliers)."""
#     if GLOBAL_RAW_DATA_DF.empty: # Utilise GLOBAL_RAW_DATA_DF pour vérifier la présence de données brutes
#         flash('Veuillez uploader des fichiers d\'abord pour accéder aux visualisations de prétraitement.', 'error')
#         return redirect(url_for('index'))
    
#     stations = sorted(GLOBAL_RAW_DATA_DF['Station'].unique().tolist())
#     station_selected = request.args.get('station', stations[0] if stations else None)

#     # --- Préparation des données AVANT interpolation ---
#     # Nous utilisons GLOBAL_RAW_DATA_DF, qui a déjà 'Datetime' comme index et les doublons gérés.
#     # Ensuite, nous appliquons 'preprocess_data_for_viz' pour obtenir Is_Daylight, Rain_mm,
#     # et remplacer les outliers par NaN, mais SANS interpoler.
#     df_for_before_plots = preprocess_data_for_viz(
#         GLOBAL_RAW_DATA_DF.copy(), # Travailler sur une copie
#         DATA_LIMITS,
#         GLOBAL_GPS_DATA_DF
#     )
    
#     # Génère les graphiques AVANT interpolation
#     missing_before_fig = generate_missing_data_plot(df_for_before_plots, station_selected)
#     outliers_before_fig = generate_outliers_plot(df_for_before_plots, DATA_LIMITS, station_selected)
    
#     # --- Préparation des données APRÈS interpolation ---
#     # Ces données sont GLOBAL_PROCESSED_DATA_DF, qui est déjà passé par la fonction `interpolation` complète lors de l'upload
#     df_for_after_plots = GLOBAL_PROCESSED_DATA_DF.copy()

#     # Génère les graphiques APRÈS interpolation
#     missing_after_fig = generate_missing_data_plot(df_for_after_plots, station_selected)

#     outliers_traitment = traiter_outliers_meteo(df_for_after_plots, DATA_LIMITS)
#     outliers_after_fig = generate_outliers_plot(outliers_traitment, DATA_LIMITS, station_selected)
    
#     return render_template('preprocessing.html',
#                          stations=stations,
#                          station_selected=station_selected,
#                          missing_before=missing_before_fig.to_html(full_html=False, include_plotlyjs='cdn'),
#                          outliers_before=outliers_before_fig.to_html(full_html=False, include_plotlyjs='cdn'),
#                          missing_after=missing_after_fig.to_html(full_html=False, include_plotlyjs='cdn'),
#                          outliers_after=outliers_after_fig.to_html(full_html=False, include_plotlyjs='cdn'))


# if __name__ == '__main__':
#     app.run(debug=True)