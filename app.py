# # # # # # # # # # import pandas as pd
# # # # # # # # # # from flask import Flask, render_template, request, redirect, url_for, flash, session
# # # # # # # # # # import os
# # # # # # # # # # import io
# # # # # # # # # # import re # Importation pour les expressions régulières

# # # # # # # # # # # Importation des fonctions de traitement et de visualisation des données
# # # # # # # # # # from data_processing import (
# # # # # # # # # #     create_datetime,
# # # # # # # # # #     gestion_doublons,
# # # # # # # # # #     interpolation,
# # # # # # # # # #     traiter_outliers_meteo,
# # # # # # # # # #     generer_graphique_par_variable_et_periode,
# # # # # # # # # #     generer_graphique_comparatif,
# # # # # # # # # #     generate_multi_variable_station_plot,
# # # # # # # # # #     daily_stats,
# # # # # # # # # #     _load_and_prepare_gps_data
# # # # # # # # # # )

# # # # # # # # # # # Importation des configurations globales
# # # # # # # # # # import config # Importation du module config entier
# # # # # # # # # # from config import (
# # # # # # # # # #     STATIONS_BY_BASSIN,
# # # # # # # # # #     LIMITS_METEO,
# # # # # # # # # #     CUSTOM_STATION_COLORS,
# # # # # # # # # #     CUSTOM_VARIABLE_COLORS,
# # # # # # # # # #     METADATA_VARIABLES
# # # # # # # # # # )

# # # # # # # # # # # Ajout d'une option pour éviter les avertissements de downcasting de Pandas
# # # # # # # # # # pd.set_option('future.no_silent_downcasting', True)

# # # # # # # # # # app = Flask(__name__)
# # # # # # # # # # app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'une_cle_secrete_par_defaut_tres_forte_pour_dev')

# # # # # # # # # # # Variable globale pour stocker les données du DataFrame après traitement
# # # # # # # # # # GLOBAL_PROCESSED_DATA_DF = None

# # # # # # # # # # # Initialisation de GLOBAL_DF_GPS_INFO au démarrage de l'application
# # # # # # # # # # try:
# # # # # # # # # #     config.GLOBAL_DF_GPS_INFO = _load_and_prepare_gps_data()
# # # # # # # # # # except Exception as e:
# # # # # # # # # #     print(f"Erreur fatale lors du chargement des données GPS des stations au démarrage: {e}. "
# # # # # # # # # #           "L'application pourrait ne pas fonctionner correctement sans ces données.")
# # # # # # # # # #     config.GLOBAL_DF_GPS_INFO = pd.DataFrame({
# # # # # # # # # #         'Station': [],
# # # # # # # # # #         'Lat': [],
# # # # # # # # # #         'Long': [],
# # # # # # # # # #         'Timezone': []
# # # # # # # # # #     })


# # # # # # # # # # @app.route('/')
# # # # # # # # # # def index():
# # # # # # # # # #     """
# # # # # # # # # #     Route principale affichant le formulaire d'upload de fichiers.
# # # # # # # # # #     """
# # # # # # # # # #     return render_template('index.html', stations_by_bassin=STATIONS_BY_BASSIN)

# # # # # # # # # # @app.route('/upload', methods=['GET', 'POST'])
# # # # # # # # # # def upload_file():
# # # # # # # # # #     """
# # # # # # # # # #     Gère l'upload et le traitement des fichiers CSV/Excel.
# # # # # # # # # #     Permet l'upload de multiples fichiers, chacun associé à un bassin et une station,
# # # # # # # # # #     en vérifiant leur compatibilité et en les ajoutant au DataFrame global.
# # # # # # # # # #     Si la méthode est GET, redirige vers la page d'accueil.
# # # # # # # # # #     """
# # # # # # # # # #     if request.method == 'GET':
# # # # # # # # # #         flash('Accès direct à la page d\'upload non autorisé. Veuillez utiliser le formulaire.', 'warning')
# # # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # #     global GLOBAL_PROCESSED_DATA_DF

# # # # # # # # # #     upload_groups = []
    
# # # # # # # # # #     # Collecter tous les indices de groupe présents dans le formulaire
# # # # # # # # # #     all_input_indices = set()
# # # # # # # # # #     for key in request.form.keys():
# # # # # # # # # #         match = re.search(r'_((\d+))$', key)
# # # # # # # # # #         if match:
# # # # # # # # # #             all_input_indices.add(int(match.group(1)))
# # # # # # # # # #     for key in request.files.keys():
# # # # # # # # # #         match = re.search(r'_(\d+)$', key)
# # # # # # # # # #         if match:
# # # # # # # # # #             all_input_indices.add(int(match.group(1)))
            
# # # # # # # # # #     sorted_indices = sorted(list(all_input_indices))

# # # # # # # # # #     if not sorted_indices:
# # # # # # # # # #         flash('Aucun dataset n\'a été soumis. Veuillez ajouter au moins un dataset.', 'error')
# # # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # #     print(f"DEBUG (upload_file): Nombre de groupes d'upload détectés: {len(sorted_indices)}")
# # # # # # # # # #     for index in sorted_indices:
# # # # # # # # # #         bassin_name = request.form.get(f'bassin_{index}')
# # # # # # # # # #         station_name = request.form.get(f'station_{index}')
# # # # # # # # # #         file_obj = request.files.get(f'file_input_{index}')

# # # # # # # # # #         if not bassin_name or not station_name or not file_obj or file_obj.filename == '':
# # # # # # # # # #             flash(f'Le dataset pour le groupe {index+1} est incomplet (bassin, station ou fichier manquant). Veuillez compléter toutes les informations.', 'error')
# # # # # # # # # #             return redirect(url_for('index'))

# # # # # # # # # #         upload_groups.append({
# # # # # # # # # #             'bassin': bassin_name,
# # # # # # # # # #             'station': station_name,
# # # # # # # # # #             'file': file_obj,
# # # # # # # # # #             'index': index
# # # # # # # # # #         })
    
# # # # # # # # # #     if not upload_groups:
# # # # # # # # # #         flash('Aucun dataset valide n\'a été trouvé pour le traitement. Veuillez vérifier vos sélections.', 'error')
# # # # # # # # # #         return redirect(url_for('index'))


# # # # # # # # # #     processed_dataframes_for_batch = []
    
# # # # # # # # # #     expected_raw_data_columns_for_comparison = None
# # # # # # # # # #     expected_raw_time_columns_for_comparison = None 

    
# # # # # # # # # #     for group_info in upload_groups:
# # # # # # # # # #         file = group_info['file']
# # # # # # # # # #         bassin = group_info['bassin']
# # # # # # # # # #         station = group_info['station']

# # # # # # # # # #         file_extension = os.path.splitext(file.filename)[1].lower()
# # # # # # # # # #         df_temp = None

# # # # # # # # # #         try:
# # # # # # # # # #             if file_extension == '.csv':
# # # # # # # # # #                 df_temp = pd.read_csv(io.BytesIO(file.read()))
# # # # # # # # # #             elif file_extension in ['.xls', '.xlsx']:
# # # # # # # # # #                 df_temp = pd.read_excel(io.BytesIO(file.read()))
# # # # # # # # # #             else:
# # # # # # # # # #                 flash(f'Extension de fichier non supportée pour "{file.filename}". Seuls les fichiers CSV ou Excel sont acceptés.', 'error')
# # # # # # # # # #                 return redirect(url_for('index'))
            
# # # # # # # # # #             if df_temp is not None:
# # # # # # # # # #                 current_file_columns = df_temp.columns.tolist()
                
# # # # # # # # # #                 current_raw_time_cols = []
# # # # # # # # # #                 if 'Date' in current_file_columns:
# # # # # # # # # #                     current_raw_time_cols.append('Date')
# # # # # # # # # #                 if all(col in current_file_columns for col in ['Year', 'Month', 'Day', 'Hour', 'Minute']):
# # # # # # # # # #                     current_raw_time_cols.extend(['Year', 'Month', 'Day', 'Hour', 'Minute'])
# # # # # # # # # #                 current_raw_time_cols_sorted = sorted(list(set(current_raw_time_cols)))

# # # # # # # # # #                 current_raw_data_cols_sorted = sorted([
# # # # # # # # # #                     col for col in current_file_columns 
# # # # # # # # # #                     if col != 'Station' and col not in ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
# # # # # # # # # #                 ])

# # # # # # # # # #                 if expected_raw_data_columns_for_comparison is None:
# # # # # # # # # #                     expected_raw_data_columns_for_comparison = current_raw_data_cols_sorted
# # # # # # # # # #                     expected_raw_time_columns_for_comparison = current_raw_time_cols_sorted
# # # # # # # # # #                 else:
# # # # # # # # # #                     if current_raw_data_cols_sorted != expected_raw_data_columns_for_comparison:
# # # # # # # # # #                         flash(f"Le fichier '{file.filename}' (Station: {station}) a des colonnes de données incompatibles avec les données déjà chargées. "
# # # # # # # # # #                               f"Attendues: {expected_raw_data_columns_for_comparison}, Obtenues: {current_raw_data_cols_sorted}. Fusion annulée.", 'error')
# # # # # # # # # #                         return redirect(url_for('index'))
                    
# # # # # # # # # #                     if current_raw_time_cols_sorted != expected_raw_time_columns_for_comparison:
# # # # # # # # # #                         flash(f"Le fichier '{file.filename}' (Station: {station}) a des colonnes temporelles incompatibles avec les données déjà chargées. "
# # # # # # # # # #                               f"Attendues: {expected_raw_time_columns_for_comparison}, Obtenues: {current_raw_time_cols_sorted}. Fusion annulée.", 'error')
# # # # # # # # # #                         return redirect(url_for('index'))
                
# # # # # # # # # #                 df_temp['Station'] = station
# # # # # # # # # #                 print(f"DEBUG (upload_file): Fichier '{file.filename}' - Station assignée: '{station}'. Premières 5 lignes du DF temporaire:\n{df_temp.head(5)}")
# # # # # # # # # #                 processed_dataframes_for_batch.append(df_temp)

# # # # # # # # # #         except Exception as e:
# # # # # # # # # #             flash(f'Erreur lors de la lecture ou du traitement du fichier "{file.filename}": {e}', 'error')
# # # # # # # # # #             print(f"DEBUG (upload_file): Erreur lors du traitement du fichier '{file.filename}': {e}")
# # # # # # # # # #             return redirect(url_for('index'))

# # # # # # # # # #     if not processed_dataframes_for_batch:
# # # # # # # # # #         flash('Aucun fichier valide n\'a pu être traité à partir de ce lot après les vérifications de contenu.', 'error')
# # # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # #     try:
# # # # # # # # # #         # Concaténation de tous les DataFrames du batch
# # # # # # # # # #         df_current_batch_raw_merged = pd.concat(processed_dataframes_for_batch, ignore_index=True)
# # # # # # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_raw_merged APRÈS CONCATÉNATION DU BATCH:\n{df_current_batch_raw_merged['Station'].unique().tolist()}")
        
# # # # # # # # # #         # Étape 1: Créer la colonne Datetime AVANT la gestion des doublons
# # # # # # # # # #         df_current_batch_with_datetime = create_datetime(df_current_batch_raw_merged, bassin=upload_groups[0]['bassin'])
# # # # # # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_with_datetime APRÈS CREATE_DATETIME DU BATCH:\n{df_current_batch_with_datetime['Station'].unique().tolist()}")

# # # # # # # # # #         # Étape 2: Gérer les doublons maintenant que 'Datetime' est une colonne
# # # # # # # # # #         df_current_batch_cleaned = gestion_doublons(df_current_batch_with_datetime)
# # # # # # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_cleaned APRÈS GESTION DOUBLONS DU BATCH:\n{df_current_batch_cleaned['Station'].unique().tolist()}")

# # # # # # # # # #         if 'Datetime' not in df_current_batch_cleaned.columns or df_current_batch_cleaned['Datetime'].isnull().all():
# # # # # # # # # #             flash("Erreur: La colonne 'Datetime' est manquante ou vide après le nettoyage. Impossible de traiter les données.", 'error')
# # # # # # # # # #             return redirect(url_for('index'))
        
# # # # # # # # # #         # Assurez-vous que 'Datetime' est bien de type datetime et nettoyez les NaT
# # # # # # # # # #         df_current_batch_cleaned['Datetime'] = pd.to_datetime(df_current_batch_cleaned['Datetime'], errors='coerce')
# # # # # # # # # #         df_current_batch_cleaned.dropna(subset=['Datetime'], inplace=True) 

# # # # # # # # # #         if df_current_batch_cleaned.empty:
# # # # # # # # # #             flash("Erreur: Toutes les données du lot actuel ont été supprimées en raison de dates invalides après nettoyage. Traitement annulé.", 'error')
# # # # # # # # # #             return redirect(url_for('index'))

# # # # # # # # # #         # Définir Datetime comme index
# # # # # # # # # #         df_current_batch_processed = df_current_batch_cleaned.set_index('Datetime').sort_index()
# # # # # # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_processed APRÈS SET_INDEX ET SORT_INDEX DU BATCH:\n{df_current_batch_processed['Station'].unique().tolist()}")


# # # # # # # # # #         if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # # # # #             GLOBAL_PROCESSED_DATA_DF = df_current_batch_processed
# # # # # # # # # #             flash(f'{len(processed_dataframes_for_batch)} fichier(s) téléchargé(s) et initialisé(s).', 'success')
# # # # # # # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF initialisé. Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
# # # # # # # # # #         else:
# # # # # # # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF EXISTANT AVANT FUSION: Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
            
# # # # # # # # # #             new_batch_stations = df_current_batch_processed['Station'].unique().tolist()
# # # # # # # # # #             print(f"DEBUG (upload_file): Stations dans le batch actuel à fusionner: {new_batch_stations}")

# # # # # # # # # #             # Filtrer les données existantes pour les stations du lot actuel du DataFrame global
# # # # # # # # # #             df_global_filtered = GLOBAL_PROCESSED_DATA_DF[~GLOBAL_PROCESSED_DATA_DF['Station'].isin(new_batch_stations)].copy()
# # # # # # # # # #             print(f"DEBUG (upload_file): Stations dans GLOBAL_PROCESSED_DATA_DF après filtrage des stations du batch actuel:\n{df_global_filtered['Station'].unique().tolist() if not df_global_filtered.empty else 'DataFrame global vide après filtrage'}")

# # # # # # # # # #             # Concaténer le DataFrame global filtré avec les données du nouveau lot
# # # # # # # # # #             # Assurez-vous que les deux DataFrames ont des index compatibles (DatetimeIndex)
# # # # # # # # # #             combined_df = pd.concat([df_global_filtered, df_current_batch_processed], ignore_index=False) # Important: ignore_index=False pour préserver les DatetimeIndex et les aligner
# # # # # # # # # #             print(f"DEBUG (upload_file): Stations uniques dans combined_df APRÈS CONCATINATION GLOBAL_FILTRÉ+BATCH:\n{combined_df['Station'].unique().tolist()}")

# # # # # # # # # #             # Pas besoin de to_datetime/dropna/set_index ici si les index sont déjà corrects.
# # # # # # # # # #             # Cependant, on peut re-trier par sécurité.
# # # # # # # # # #             GLOBAL_PROCESSED_DATA_DF = combined_df.sort_index()

# # # # # # # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF mis à jour. Stations uniques finales: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

# # # # # # # # # #             flash(f'{len(processed_dataframes_for_batch)} fichier(s) ajouté(s). Total de stations uniques: {len(GLOBAL_PROCESSED_DATA_DF["Station"].unique())}.', 'success')

# # # # # # # # # #         # --- VÉRIFICATION FINALE ET DÉFENSIVE DE L'INDEX DE GLOBAL_PROCESSED_DATA_DF AVANT TOUT APPEL EXTERNE ---
# # # # # # # # # #         print(f"DEBUG: Type de l'index de GLOBAL_PROCESSED_DATA_DF avant vérification finale: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# # # # # # # # # #         try:
# # # # # # # # # #             # Cette étape est cruciale si l'index pourrait être cassé, mais elle doit être transparente si tout va bien
# # # # # # # # # #             if not isinstance(GLOBAL_PROCESSED_DATA_DF.index, pd.DatetimeIndex):
# # # # # # # # # #                 # Si l'index n'est pas un DatetimeIndex, essayez de le forcer ou de le recréer à partir d'une colonne Datetime
# # # # # # # # # #                 if 'Datetime' in GLOBAL_PROCESSED_DATA_DF.columns:
# # # # # # # # # #                     GLOBAL_PROCESSED_DATA_DF['Datetime'] = pd.to_datetime(GLOBAL_PROCESSED_DATA_DF['Datetime'], errors='coerce')
# # # # # # # # # #                     GLOBAL_PROCESSED_DATA_DF.dropna(subset=['Datetime'], inplace=True)
# # # # # # # # # #                     GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.set_index('Datetime').sort_index()
# # # # # # # # # #                 else:
# # # # # # # # # #                     raise TypeError("L'index n'est pas DatetimeIndex et aucune colonne 'Datetime' n'est disponible pour la recréation.")
            
# # # # # # # # # #             GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.sort_index() # Re-trier au cas où

# # # # # # # # # #             print("DEBUG: Index de GLOBAL_PROCESSED_DATA_DF VALIDÉ et RE-CONVERTI en DatetimeIndex (vérification finale réussie).")

# # # # # # # # # #         except Exception as e:
# # # # # # # # # #             flash(f"Erreur critique finale: Impossible de garantir un DatetimeIndex pour l'interpolation. Le format des dates dans vos fichiers est incompatible. Détails: {e}", 'error')
# # # # # # # # # #             print(f"DEBUG: Erreur critique finale lors de la conversion de l'index: {e}")
# # # # # # # # # #             return redirect(url_for('index'))
        
# # # # # # # # # #         if GLOBAL_PROCESSED_DATA_DF.empty:
# # # # # # # # # #             flash("Erreur: Le DataFrame global est vide après le traitement des dates. Il n'y a plus de données valides à analyser.", 'error')
# # # # # # # # # #             return redirect(url_for('index'))


# # # # # # # # # #         print(f"DEBUG: Type de l'index de GLOBAL_PROCESSED_DATA_DF AVANT interpolation: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# # # # # # # # # #         print(f"DEBUG: Dtypes de GLOBAL_PROCESSED_DATA_DF (après vérif index): \n{GLOBAL_PROCESSED_DATA_DF.dtypes}")


# # # # # # # # # #         # Traitement d'interpolation et des outliers sur le DataFrame global unifié
# # # # # # # # # #         # Ces fonctions attendent un DatetimeIndex
# # # # # # # # # #         GLOBAL_PROCESSED_DATA_DF = interpolation(GLOBAL_PROCESSED_DATA_DF, LIMITS_METEO, config.GLOBAL_DF_GPS_INFO)
# # # # # # # # # #         GLOBAL_PROCESSED_DATA_DF = traiter_outliers_meteo(GLOBAL_PROCESSED_DATA_DF, LIMITS_METEO)
        
# # # # # # # # # #         # Nettoyage des colonnes temporaires
# # # # # # # # # #         cols_to_drop_after_process = [
# # # # # # # # # #             'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date',
# # # # # # # # # #             'sunrise_time_local', 'sunset_time_local'
# # # # # # # # # #         ]
# # # # # # # # # #         GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.drop(columns=cols_to_drop_after_process, errors='ignore')

# # # # # # # # # #         # Mettre à jour les informations de session pour les options de visualisation
# # # # # # # # # #         session['available_stations'] = sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist())
# # # # # # # # # #         session['available_variables'] = sorted([col for col in GLOBAL_PROCESSED_DATA_DF.columns if col in METADATA_VARIABLES.keys()])
# # # # # # # # # #         session['can_compare_stations'] = len(session['available_stations']) >= 2

# # # # # # # # # #         flash('Données traitées et fusionnées avec succès ! Vous pouvez maintenant visualiser les résultats.', 'success')
# # # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # # #     except Exception as e:
# # # # # # # # # #         flash(f"Une erreur inattendue s'est produite lors du traitement des données: {e}", 'error')
# # # # # # # # # #         print(f"DEBUG: Erreur inattendue dans /upload: {e}")
# # # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # # @app.route('/visualizations_options')
# # # # # # # # # # def show_visualizations_options():
# # # # # # # # # #     """
# # # # # # # # # #     Affiche la page des options de visualisation après le traitement des données.
# # # # # # # # # #     """
# # # # # # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # #     print(f"DEBUG (app.py/visualizations_options): Stations disponibles dans GLOBAL_PROCESSED_DATA_DF: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

# # # # # # # # # #     daily_stats_df = daily_stats(GLOBAL_PROCESSED_DATA_DF)
    
# # # # # # # # # #     print(f"DEBUG (app.py/visualizations_options): Stations présentes dans daily_stats_df: {daily_stats_df['Station'].unique().tolist() if not daily_stats_df.empty else 'DataFrame vide'}")

# # # # # # # # # #     initial_data_html = None
# # # # # # # # # #     data_summary = f"Lignes: {len(GLOBAL_PROCESSED_DATA_DF)}, Colonnes: {len(GLOBAL_PROCESSED_DATA_DF.columns)}"
    
# # # # # # # # # #     unique_stations_count = len(GLOBAL_PROCESSED_DATA_DF['Station'].unique())

# # # # # # # # # #     if unique_stations_count > 1:
# # # # # # # # # #         top_n_data = GLOBAL_PROCESSED_DATA_DF.groupby('Station').head(10)
# # # # # # # # # #         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
# # # # # # # # # #     else:
# # # # # # # # # #         top_n_data = GLOBAL_PROCESSED_DATA_DF.head(20)
# # # # # # # # # #         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
    
# # # # # # # # # #     if initial_data_html is None:
# # # # # # # # # #         initial_data_html = "<p class='text-center text-red-500'>Aucune donnée à afficher pour la prévisualisation.</p>"


# # # # # # # # # #     daily_stats_html = daily_stats_df.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=False, border=0) if not daily_stats_df.empty else None

# # # # # # # # # #     stations = session.get('available_stations', [])
# # # # # # # # # #     variables = session.get('available_variables', [])
# # # # # # # # # #     can_compare_stations = session.get('can_compare_stations', False)

# # # # # # # # # #     periodes = ['Journalière', 'Hebdomadaire', 'Mensuelle', 'Annuelle']

# # # # # # # # # #     return render_template(
# # # # # # # # # #         'visualizations_options.html',
# # # # # # # # # #         initial_data_html=initial_data_html,
# # # # # # # # # #         data_summary=data_summary,
# # # # # # # # # #         daily_stats_html=daily_stats_html,
# # # # # # # # # #         stations=stations,
# # # # # # # # # #         variables=variables,
# # # # # # # # # #         periodes=periodes,
# # # # # # # # # #         can_compare_stations=can_compare_stations,
# # # # # # # # # #         METADATA_VARIABLES=METADATA_VARIABLES
# # # # # # # # # #     )

# # # # # # # # # # @app.route('/generate_single_plot', methods=['GET', 'POST'])
# # # # # # # # # # def generate_single_plot():
# # # # # # # # # #     """
# # # # # # # # # #     Génère et affiche un graphique pour une seule variable et une seule station.
# # # # # # # # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # # # # # # # #     """
# # # # # # # # # #     if request.method == 'GET':
# # # # # # # # # #         flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
# # # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # #     station_select = request.form.get('station_select_single')
# # # # # # # # # #     variable_select = request.form.get('variable_select_single')
# # # # # # # # # #     periode_select = request.form.get('periode_select_single')

# # # # # # # # # #     if not station_select or not variable_select or not periode_select:
# # # # # # # # # #         flash('Veuillez sélectionner une station, une variable et une période pour le graphique.', 'error')
# # # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # # #     try:
# # # # # # # # # #         plot_data = generer_graphique_par_variable_et_periode(
# # # # # # # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # # # # # # #             station_select,
# # # # # # # # # #             variable_select,
# # # # # # # # # #             periode_select,
# # # # # # # # # #             CUSTOM_STATION_COLORS,
# # # # # # # # # #             METADATA_VARIABLES
# # # # # # # # # #         )

# # # # # # # # # #         if plot_data:
# # # # # # # # # #             meta = METADATA_VARIABLES.get(variable_select, {})
# # # # # # # # # #             plot_title = f"Évolution de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) pour la station {station_select} ({periode_select})"
# # # # # # # # # #             return render_template('plot_display.html', plot_data=plot_data, plot_title=plot_title)
# # # # # # # # # #         else:
# # # # # # # # # #             flash("Impossible de générer le graphique. Vérifiez les données ou la sélection.", 'error')
# # # # # # # # # #             return redirect(url_for('show_visualizations_options'))
# # # # # # # # # #     except TypeError as e: # Capture spécifique de TypeError
# # # # # # # # # #         flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
# # # # # # # # # #         print(f"DEBUG: Erreur de type dans generate_single_plot: {e}")
# # # # # # # # # #         return redirect(url_for('show_visualizations_options'))
# # # # # # # # # #     except Exception as e:
# # # # # # # # # #         flash(f"Erreur lors de la génération du graphique: {e}", 'error')
# # # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # # # @app.route('/generate_comparative_plot', methods=['GET', 'POST'])
# # # # # # # # # # def generate_comparative_plot():
# # # # # # # # # #     """
# # # # # # # # # #     Génère et affiche un graphique comparatif pour une variable à travers toutes les stations.
# # # # # # # # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # # # # # # # #     """
# # # # # # # # # #     if request.method == 'GET':
# # # # # # # # # #         flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
# # # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # #     variable_select = request.form.get('variable_select_comparative')
# # # # # # # # # #     periode_select = request.form.get('periode_select_comparative')

# # # # # # # # # #     if not variable_select or not periode_select:
# # # # # # # # # #         flash('Veuillez sélectionner une variable et une période pour le graphique comparatif.', 'error')
# # # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # # #     try:
# # # # # # # # # #         plot_data = generer_graphique_comparatif(
# # # # # # # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # # # # # # #             variable_select,
# # # # # # # # # #             periode_select,
# # # # # # # # # #             CUSTOM_STATION_COLORS,
# # # # # # # # # #             METADATA_VARIABLES
# # # # # # # # # #         )

# # # # # # # # # #         if plot_data:
# # # # # # # # # #             meta = METADATA_VARIABLES.get(variable_select, {})
# # # # # # # # # #             plot_title = f"Comparaison de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) entre stations ({periode_select})"
# # # # # # # # # #             return render_template('plot_display.html', plot_data=plot_data, plot_title=plot_title)
# # # # # # # # # #         else:
# # # # # # # # # #             flash("Impossible de générer le graphique comparatif. Vérifiez les données ou la sélection.", 'error')
# # # # # # # # # #             return redirect(url_for('show_visualizations_options'))
# # # # # # # # # #     except TypeError as e: # Capture spécifique de TypeError
# # # # # # # # # #         flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
# # # # # # # # # #         print(f"DEBUG: Erreur de type dans generate_comparative_plot: {e}")
# # # # # # # # # #         return redirect(url_for('show_visualizations_options'))
# # # # # # # # # #     except Exception as e:
# # # # # # # # # #         flash(f"Erreur lors de la génération du graphique comparatif: {e}", 'error')
# # # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # # # @app.route('/generate_multi_variable_plot', methods=['GET', 'POST'])
# # # # # # # # # # def generate_multi_variable_plot_route():
# # # # # # # # # #     """
# # # # # # # # # #     Génère et affiche un graphique d'évolution multi-variables normalisé pour une station.
# # # # # # # # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # # # # # # # #     """
# # # # # # # # # #     if request.method == 'GET':
# # # # # # # # # #         flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
# # # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # #     station_select = request.form.get('station_select_multi_var')

# # # # # # # # # #     if not station_select:
# # # # # # # # # #         flash('Veuillez sélectionner une station pour le graphique multi-variables.', 'error')
# # # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # # #     try:
# # # # # # # # # #         plot_data = generate_multi_variable_station_plot(
# # # # # # # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # # # # # # #             station_select,
# # # # # # # # # #             CUSTOM_VARIABLE_COLORS,
# # # # # # # # # #             METADATA_VARIABLES
# # # # # # # # # #         )

# # # # # # # # # #         if plot_data:
# # # # # # # # # #             plot_title = f"Évolution Normalisée des Variables Météorologiques pour la station {station_select}"
# # # # # # # # # #             return render_template('plot_display.html', plot_data=plot_data, plot_title=plot_title)
# # # # # # # # # #         else:
# # # # # # # # # #             flash("Impossible de générer le graphique multi-variables. Vérifiez les données ou la sélection.", 'error')
# # # # # # # # # #             return redirect(url_for('show_visualizations_options'))
# # # # # # # # # #     except TypeError as e: # Capture spécifique de TypeError
# # # # # # # # # #         flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
# # # # # # # # # #         print(f"DEBUG: Erreur de type dans generate_multi_variable_plot_route: {e}")
# # # # # # # # # #         return redirect(url_for('show_visualizations_options'))
# # # # # # # # # #     except Exception as e:
# # # # # # # # # #         flash(f"Erreur lors de la génération du graphique multi-variables: {e}", 'error')
# # # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # # # @app.route('/reset_data', methods=['GET', 'POST'])
# # # # # # # # # # def reset_data():
# # # # # # # # # #     """
# # # # # # # # # #     Réinitialise les données globales traitées et redirige vers la page d'accueil.
# # # # # # # # # #     Si la méthode est GET, redirige simplement vers la page d'accueil.
# # # # # # # # # #     """
# # # # # # # # # #     if request.method == 'GET':
# # # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # #     global GLOBAL_PROCESSED_DATA_DF
# # # # # # # # # #     GLOBAL_PROCESSED_DATA_DF = None
# # # # # # # # # #     session.pop('available_stations', None)
# # # # # # # # # #     session.pop('available_variables', None)
# # # # # # # # # #     session.pop('can_compare_stations', None)
# # # # # # # # # #     flash('Toutes les données chargées ont été réinitialisées. Vous pouvez commencer une nouvelle analyse.', 'info')
# # # # # # # # # #     return redirect(url_for('index'))


# # # # # # # # # # if __name__ == '__main__':
# # # # # # # # # #     app.run(debug=True)



# # # # # # # # # ###################################
# # # # # # # # # # import pandas as pd
# # # # # # # # # # from flask import Flask, render_template, request, redirect, url_for, flash, session
# # # # # # # # # # import os
# # # # # # # # # # import io
# # # # # # # # # # import warnings
# # # # # # # # # # import config # Importe le module config pour accéder aux variables globales et constantes
# # # # # # # # # # import data_processing # Importe les fonctions de traitement de données
# # # # # # # # # # import json # Pour charger le fichier JSON des coordonnées
# # # # # # # # # # import plotly.graph_objects as go # Importation pour gérer les objets Figure

# # # # # # # # # # app = Flask(__name__)
# # # # # # # # # # app.secret_key = 'votre_cle_secrete_tres_secrete' # Remplacez par une clé secrète forte pour la production

# # # # # # # # # # # Chemin vers le fichier JSON des coordonnées de station
# # # # # # # # # # STATION_COORDS_PATH = os.path.join('data', 'station_coordinates.json')

# # # # # # # # # # # Initialiser GLOBAL_PROCESSED_DATA_DF
# # # # # # # # # # # Ce DataFrame stockera toutes les données fusionnées et prétraitées.
# # # # # # # # # # GLOBAL_PROCESSED_DATA_DF = pd.DataFrame()

# # # # # # # # # # # Variables pour le suivi des fichiers téléchargés et des stations
# # # # # # # # # # uploaded_files_info = [] # Stocke le nom du fichier et la station assignée pour le lot actuel

# # # # # # # # # # # Initialisation des données GPS au démarrage de l'application
# # # # # # # # # # # Ceci garantit que GLOBAL_DF_GPS_INFO est chargé une seule fois
# # # # # # # # # # with app.app_context():
# # # # # # # # # #     try:
# # # # # # # # # #         # Assurez-vous que le dossier 'data' existe
# # # # # # # # # #         data_dir = 'data'
# # # # # # # # # #         os.makedirs(data_dir, exist_ok=True)
        
# # # # # # # # # #         # Vérifiez si le fichier JSON des coordonnées existe. S'il n'existe pas,
# # # # # # # # # #         # appelez la fonction de préparation des données.
# # # # # # # # # #         if not os.path.exists(STATION_COORDS_PATH) or os.stat(STATION_COORDS_PATH).st_size == 0:
# # # # # # # # # #             print(f"Le fichier '{STATION_COORDS_PATH}' n'existe pas ou est vide. Exécution de data_processing._load_and_prepare_gps_data()...")
# # # # # # # # # #             config.GLOBAL_DF_GPS_INFO = data_processing._load_and_prepare_gps_data()
# # # # # # # # # #             # Sauvegarder le DataFrame GPS après l'avoir chargé/préparé pour les exécutions futures
# # # # # # # # # #             if not config.GLOBAL_DF_GPS_INFO.empty: # Sauvegarder uniquement si le DF n'est pas vide
# # # # # # # # # #                 config.GLOBAL_DF_GPS_INFO.to_json(STATION_COORDS_PATH, orient='records', indent=4)
# # # # # # # # # #                 print(f"Données GPS des stations sauvegardées dans '{STATION_COORDS_PATH}'.")
# # # # # # # # # #             else:
# # # # # # # # # #                 print("Avertissement: Les données GPS sont vides, le fichier JSON ne sera pas créé.")
# # # # # # # # # #         else:
# # # # # # # # # #             config.GLOBAL_DF_GPS_INFO = pd.read_json(STATION_COORDS_PATH, orient='records')
        
# # # # # # # # # #         if config.GLOBAL_DF_GPS_INFO.empty:
# # # # # # # # # #             raise ValueError("Le DataFrame des données GPS est vide après le chargement/la préparation.")
        
# # # # # # # # # #         print("Préparation des données de coordonnées des stations terminée.")
# # # # # # # # # #     except Exception as e:
# # # # # # # # # #         print(f"Erreur lors du chargement ou de la préparation des données GPS: {e}")
# # # # # # # # # #         config.GLOBAL_DF_GPS_INFO = pd.DataFrame() # Assurez-vous qu'il s'agit d'un DataFrame vide en cas d'échec

# # # # # # # # # # @app.route('/')
# # # # # # # # # # def index():
# # # # # # # # # #     """
# # # # # # # # # #     Route principale pour télécharger les fichiers et choisir le bassin/station.
# # # # # # # # # #     """
# # # # # # # # # #     bassins = list(config.STATIONS_BY_BASSIN.keys())
# # # # # # # # # #     # Réinitialise les informations de session lors de l'accès à la page d'accueil
# # # # # # # # # #     session.pop('uploaded_files_info', None)
# # # # # # # # # #     session.pop('global_data_df', None)
# # # # # # # # # #     global GLOBAL_PROCESSED_DATA_DF
# # # # # # # # # #     GLOBAL_PROCESSED_DATA_DF = pd.DataFrame() # Réinitialiser le DF global

# # # # # # # # # #     # Récupérer les messages flash
# # # # # # # # # #     messages = session.pop('_flashes', [])
# # # # # # # # # #     # Passer stations_by_bassin pour le formulaire d'upload dans index.html
# # # # # # # # # #     return render_template('index.html', bassins=bassins, messages=messages, stations_by_bassin=config.STATIONS_BY_BASSIN)

# # # # # # # # # # @app.route('/upload', methods=['POST'])
# # # # # # # # # # def upload_file():
# # # # # # # # # #     """
# # # # # # # # # #     Gère le téléchargement des fichiers Excel/CSV, leur prétraitement
# # # # # # # # # #     et leur fusion dans un DataFrame global.
# # # # # # # # # #     """
# # # # # # # # # #     global GLOBAL_PROCESSED_DATA_DF
# # # # # # # # # #     global uploaded_files_info

# # # # # # # # # #     if 'file' not in request.files:
# # # # # # # # # #         flash('Aucun fichier sélectionné.', 'error')
# # # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # #     files = request.files.getlist('file')
# # # # # # # # # #     if not files:
# # # # # # # # # #         flash('Aucun fichier sélectionné.', 'error')
# # # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # #     bassin = request.form.get('bassin')
# # # # # # # # # #     if not bassin:
# # # # # # # # # #         flash('Veuillez sélectionner un bassin.', 'error')
# # # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # #     # Récupérer le mappage station_assignee des données du formulaire
# # # # # # # # # #     num_files = len(files)
# # # # # # # # # #     current_batch_assigned_stations = []
# # # # # # # # # #     for i in range(num_files):
# # # # # # # # # #         station_name = request.form.get(f'station_assignee_{i}')
# # # # # # # # # #         if not station_name:
# # # # # # # # # #             flash(f"La station assignée pour le fichier {files[i].filename} est manquante.", "error")
# # # # # # # # # #             return redirect(url_for('index'))
# # # # # # # # # #         current_batch_assigned_stations.append(station_name)
    
# # # # # # # # # #     print(f"DEBUG (upload_file): Nombre de groupes d'upload détectés: {len(files)}")

# # # # # # # # # #     # Assurez-vous que config.GLOBAL_DF_GPS_INFO est chargé et non vide
# # # # # # # # # #     if config.GLOBAL_DF_GPS_INFO is None or config.GLOBAL_DF_GPS_INFO.empty:
# # # # # # # # # #         flash("Erreur: Les données GPS des stations ne sont pas chargées ou sont vides. Veuillez vérifier la configuration et les fichiers de coordonnées.", "error")
# # # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # #     df_list_current_batch = []
# # # # # # # # # #     processed_files_count = 0
# # # # # # # # # #     batch_stations_processed = set()

# # # # # # # # # #     for i, file in enumerate(files):
# # # # # # # # # #         if file.filename == '':
# # # # # # # # # #             continue

# # # # # # # # # #         filename = file.filename
# # # # # # # # # #         station_assignee = current_batch_assigned_stations[i]

# # # # # # # # # #         print(f"DEBUG (upload_file): Traitement du fichier '{filename}' assigné à la station '{station_assignee}'.")
# # # # # # # # # #         print(f"DEBUG (upload_file): Stations GPS connues: {config.GLOBAL_DF_GPS_INFO['Station'].unique().tolist()}")

# # # # # # # # # #         # Vérifier si la station assignée existe dans les données GPS
# # # # # # # # # #         if station_assignee not in config.GLOBAL_DF_GPS_INFO['Station'].values:
# # # # # # # # # #             flash(f"La station '{station_assignee}' assignée au fichier '{filename}' n'est pas reconnue dans les données GPS. Vérifiez l'orthographe.", "warning")
# # # # # # # # # #             print(f"DEBUG (upload_file): IGNORÉ - Station '{station_assignee}' du fichier '{filename}' non trouvée dans les données GPS.")
# # # # # # # # # #             continue # Passe au fichier suivant

# # # # # # # # # #         try:
# # # # # # # # # #             # Détecter le type de fichier et lire
# # # # # # # # # #             if filename.endswith('.xlsx'):
# # # # # # # # # #                 df_temp = pd.read_excel(io.BytesIO(file.read()))
# # # # # # # # # #             elif filename.endswith('.csv'):
# # # # # # # # # #                 df_temp = pd.read_csv(io.BytesIO(file.read()))
# # # # # # # # # #             else:
# # # # # # # # # #                 flash(f"Format de fichier non supporté pour {filename}. Utilisez .xlsx ou .csv", "error")
# # # # # # # # # #                 continue

# # # # # # # # # #             # Assignation de la station au DataFrame
# # # # # # # # # #             df_temp['Station'] = station_assignee
# # # # # # # # # #             print(f"DEBUG (upload_file): Fichier '{filename}' - Station assignée: '{station_assignee}'. Premières 5 lignes du DF temporaire:\n{df_temp.head()}")
            
# # # # # # # # # #             df_list_current_batch.append(df_temp)
# # # # # # # # # #             batch_stations_processed.add(station_assignee)
# # # # # # # # # #             processed_files_count += 1

# # # # # # # # # #         except Exception as e:
# # # # # # # # # #             flash(f"Erreur lors du traitement du fichier {filename}: {e}", "error")
# # # # # # # # # #             warnings.warn(f"Erreur lors du traitement du fichier {filename}: {e}")
# # # # # # # # # #             continue

# # # # # # # # # #     if not df_list_current_batch:
# # # # # # # # # #         flash("Aucun fichier n'a pu être traité dans ce lot.", "error")
# # # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # #     # Concaténation de tous les DataFrames du lot actuel
# # # # # # # # # #     df_current_batch_raw_merged = pd.concat(df_list_current_batch, ignore_index=True)
# # # # # # # # # #     print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_raw_merged APRÈS CONCATÉNATION DU BATCH:\n{df_current_batch_raw_merged['Station'].unique().tolist()}")

# # # # # # # # # #     # 1. Création de la colonne Datetime
# # # # # # # # # #     df_current_batch_with_datetime = data_processing.create_datetime(df_current_batch_raw_merged, bassin)
# # # # # # # # # #     print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_with_datetime APRÈS CREATE_DATETIME DU BATCH:\n{df_current_batch_with_datetime['Station'].unique().tolist()}")
    
# # # # # # # # # #     if df_current_batch_with_datetime.empty or 'Datetime' not in df_current_batch_with_datetime.columns:
# # # # # # # # # #         flash("Erreur: La colonne 'Datetime' n'a pas pu être créée. Vérifiez les formats de date/heure dans vos fichiers.", "error")
# # # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # #     # 2. Gestion des doublons
# # # # # # # # # #     df_current_batch_cleaned = data_processing.gestion_doublons(df_current_batch_with_datetime)
# # # # # # # # # #     print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_cleaned APRÈS GESTION DOUBLONS DU BATCH:\n{df_current_batch_cleaned['Station'].unique().tolist()}")

# # # # # # # # # #     # 3. Définir 'Datetime' comme index et trier
# # # # # # # # # #     # IMPORTANT: Convertir 'Datetime' en datetime avant de définir l'index
# # # # # # # # # #     df_current_batch_cleaned['Datetime'] = pd.to_datetime(df_current_batch_cleaned['Datetime'], errors='coerce')
# # # # # # # # # #     df_current_batch_cleaned.dropna(subset=['Datetime'], inplace=True)
    
# # # # # # # # # #     if df_current_batch_cleaned.empty:
# # # # # # # # # #         flash("Erreur: Le DataFrame est vide après nettoyage des dates invalides. Aucun graphique ne peut être généré.", "error")
# # # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # #     df_current_batch_processed = df_current_batch_cleaned.set_index('Datetime').sort_index()
    
# # # # # # # # # #     print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_processed APRÈS SET_INDEX ET SORT_INDEX DU BATCH:\n{df_current_batch_processed['Station'].unique().tolist()}")
# # # # # # # # # #     print(f"DEBUG: Type de l'index de df_current_batch_processed AVANT interpolation: {type(df_current_batch_processed.index)}")
# # # # # # # # # #     print(f"DEBUG: Dtypes de df_current_batch_processed (après vérif index):\n{df_current_batch_processed.dtypes.to_string()}")

# # # # # # # # # #     try:
# # # # # # # # # #         # 4. Interpolation des données et calcul du jour/nuit
# # # # # # # # # #         df_current_batch_interpolated = data_processing.interpolation(df_current_batch_processed, config.LIMITS_METEO, config.GLOBAL_DF_GPS_INFO)
        
# # # # # # # # # #         # 5. Traitement des outliers (déjà intégré dans interpolation, mais peut être séparé si nécessaire)
# # # # # # # # # #         # df_current_batch_final = data_processing.traiter_outliers_meteo(df_current_batch_interpolated, config.LIMITS_METEO)
# # # # # # # # # #         df_current_batch_final = df_current_batch_interpolated # Si traiter_outliers_meteo est appelé séparément ou intégré

# # # # # # # # # #         # Nettoyage des colonnes temporaires si elles existent encore après le traitement
# # # # # # # # # #         cols_to_drop_after_process = ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
# # # # # # # # # #         df_current_batch_final = df_current_batch_final.drop(columns=cols_to_drop_after_process, errors='ignore')

# # # # # # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_final APRÈS INTERPOLATION ET TRAITEMENT: {df_current_batch_final['Station'].unique().tolist()}")

# # # # # # # # # #         # Fusion avec le DataFrame global existant
# # # # # # # # # #         if GLOBAL_PROCESSED_DATA_DF.empty:
# # # # # # # # # #             GLOBAL_PROCESSED_DATA_DF = df_current_batch_final
# # # # # # # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF initialisé. Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
# # # # # # # # # #         else:
# # # # # # # # # #             # Concaténation et suppression des doublons sur (Datetime, Station) pour éviter les entrées dupliquées
# # # # # # # # # #             df_to_concat = df_current_batch_final.copy()
            
# # # # # # # # # #             # Assurer que les dtypes sont compatibles avant la concaténation
# # # # # # # # # #             # C'est une étape cruciale pour éviter les problèmes de dtype 'object' qui peuvent cacher des incohérences.
# # # # # # # # # #             for col in GLOBAL_PROCESSED_DATA_DF.columns:
# # # # # # # # # #                 if col in df_to_concat.columns and GLOBAL_PROCESSED_DATA_DF[col].dtype != df_to_concat[col].dtype:
# # # # # # # # # #                     try:
# # # # # # # # # #                         # Tenter de convertir la colonne dans df_to_concat au type de GLOBAL_PROCESSED_DATA_DF
# # # # # # # # # #                         df_to_concat[col] = df_to_concat[col].astype(GLOBAL_PROCESSED_DATA_DF[col].dtype)
# # # # # # # # # #                     except Exception as e:
# # # # # # # # # #                         warnings.warn(f"Impossible de convertir la colonne '{col}' dans le nouveau lot au type du DataFrame global: {e}. Des incohérences pourraient persister.")
            
# # # # # # # # # #             combined_df = pd.concat([GLOBAL_PROCESSED_DATA_DF, df_to_concat])
            
# # # # # # # # # #             # Gestion des doublons après concaténation (sur index et Station)
# # # # # # # # # #             # L'index est DatetimeIndex, donc on réinitialise pour utiliser Datetime comme colonne pour drop_duplicates
# # # # # # # # # #             combined_df_reset = combined_df.reset_index()
# # # # # # # # # #             combined_df_deduplicated = combined_df_reset.drop_duplicates(subset=['Datetime', 'Station'], keep='first')
            
# # # # # # # # # #             # Re-définir l'index après déduplication
# # # # # # # # # #             GLOBAL_PROCESSED_DATA_DF = combined_df_deduplicated.set_index('Datetime').sort_index()

# # # # # # # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF mis à jour. Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
        
# # # # # # # # # #         # Vérification finale du DatetimeIndex
# # # # # # # # # #         if not isinstance(GLOBAL_PROCESSED_DATA_DF.index, pd.DatetimeIndex):
# # # # # # # # # #             warnings.warn("L'index de GLOBAL_PROCESSED_DATA_DF n'est pas un DatetimeIndex après fusion finale. Tentative de correction...")
# # # # # # # # # #             GLOBAL_PROCESSED_DATA_DF.index = pd.to_datetime(GLOBAL_PROCESSED_DATA_DF.index, errors='coerce')
# # # # # # # # # #             GLOBAL_PROCESSED_DATA_DF.dropna(inplace=True)
# # # # # # # # # #             GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.sort_index()
# # # # # # # # # #             if not isinstance(GLOBAL_PROCESSED_DATA_DF.index, pd.DatetimeIndex):
# # # # # # # # # #                 raise TypeError("Impossible de garantir un DatetimeIndex pour GLOBAL_PROCESSED_DATA_DF après fusion et vérification finale.")
# # # # # # # # # #         else:
# # # # # # # # # #             # S'assurer que l'index est correctement typé et trié
# # # # # # # # # #             if GLOBAL_PROCESSED_DATA_DF.index.dtype != 'datetime64[ns, UTC]':
# # # # # # # # # #                 try:
# # # # # # # # # #                     GLOBAL_PROCESSED_DATA_DF.index = GLOBAL_PROCESSED_DATA_DF.index.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
# # # # # # # # # #                 except TypeError: # Already localized but maybe to a different TZ
# # # # # # # # # #                     GLOBAL_PROCESSED_DATA_DF.index = GLOBAL_PROCESSED_DATA_DF.index.tz_convert('UTC')
# # # # # # # # # #             GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.sort_index()

# # # # # # # # # #         print(f"DEBUG: Type de l'index de GLOBAL_PROCESSED_DATA_DF après vérification finale: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# # # # # # # # # #         print(f"DEBUG: Index de GLOBAL_PROCESSED_DATA_DF VALIDÉ et RE-CONVERTI en DatetimeIndex (vérification finale réussie).")
# # # # # # # # # #         print(f"DEBUG: Dtypes de GLOBAL_PROCESSED_DATA_DF (après vérif index):\n{GLOBAL_PROCESSED_DATA_DF.dtypes.to_string()}")


# # # # # # # # # #     except Exception as e:
# # # # # # # # # #         flash(f"Une erreur inattendue s'est produite lors du traitement des données: {e}", "error")
# # # # # # # # # #         warnings.warn(f"Erreur inattendue dans /upload: {e}")
# # # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # #     flash(f"{processed_files_count} fichier(s) téléchargé(s) et traité(s) avec succès pour le bassin {bassin}.", "success")
# # # # # # # # # #     return redirect(url_for('show_visualizations_options'))


# # # # # # # # # # @app.route('/visualizations_options')
# # # # # # # # # # def show_visualizations_options():
# # # # # # # # # #     """
# # # # # # # # # #     Affiche la page des options de visualisation.
# # # # # # # # # #     """
# # # # # # # # # #     if GLOBAL_PROCESSED_DATA_DF.empty:
# # # # # # # # # #         flash("Aucune donnée disponible pour la visualisation. Veuillez télécharger des fichiers.", "info")
# # # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # #     # Récupérer toutes les stations uniques
# # # # # # # # # #     all_stations = GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()
# # # # # # # # # #     all_variables = [col for col in GLOBAL_PROCESSED_DATA_DF.columns if col not in ['Station', 'Is_Daylight', 'Daylight_Duration']]
    
# # # # # # # # # #     # Calcul des statistiques journalières
# # # # # # # # # #     daily_stats_df = data_processing.daily_stats(GLOBAL_PROCESSED_DATA_DF)

# # # # # # # # # #     print(f"DEBUG (app.py/visualizations_options): Stations disponibles dans GLOBAL_PROCESSED_DATA_DF: {all_stations}")
# # # # # # # # # #     print(f"DEBUG (app.py/visualizations_options): Stations présentes dans daily_stats_df: {daily_stats_df['Station'].unique().tolist()}")

# # # # # # # # # #     messages = session.pop('_flashes', []) # Récupère les messages flash pour affichage
# # # # # # # # # #     return render_template(
# # # # # # # # # #         'visualizations_options.html',
# # # # # # # # # #         stations=all_stations,
# # # # # # # # # #         variables=all_variables,
# # # # # # # # # #         daily_stats=daily_stats_df,
# # # # # # # # # #         metadata_variables=config.METADATA_VARIABLES,
# # # # # # # # # #         messages=messages
# # # # # # # # # #     )

# # # # # # # # # # @app.route('/generate_plot', methods=['POST'])
# # # # # # # # # # def generate_plot():
# # # # # # # # # #     """
# # # # # # # # # #     Génère et affiche le graphique demandé par l'utilisateur.
# # # # # # # # # #     """
# # # # # # # # # #     station = request.form.get('station')
# # # # # # # # # #     variable = request.form.get('variable')
# # # # # # # # # #     periode = request.form.get('periode')
# # # # # # # # # #     plot_type = request.form.get('plot_type')

# # # # # # # # # #     if GLOBAL_PROCESSED_DATA_DF.empty:
# # # # # # # # # #         flash("Aucune donnée disponible pour générer le graphique.", "error")
# # # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # # #     fig = go.Figure() # Initialiser une figure Plotly vide
# # # # # # # # # #     plot_title = "Graphique Météorologique"

# # # # # # # # # #     if plot_type == 'single_station_variable':
# # # # # # # # # #         if not station or not variable or not periode:
# # # # # # # # # #             flash("Veuillez sélectionner une station, une variable et une période pour le graphique.", "error")
# # # # # # # # # #             return redirect(url_for('show_visualizations_options'))
# # # # # # # # # #         fig = data_processing.generer_graphique_par_variable_et_periode(
# # # # # # # # # #             GLOBAL_PROCESSED_DATA_DF, station, variable, periode,
# # # # # # # # # #             config.CUSTOM_STATION_COLORS, config.METADATA_VARIABLES
# # # # # # # # # #         )
# # # # # # # # # #         variable_name = config.METADATA_VARIABLES.get(variable, {}).get('Nom', variable)
# # # # # # # # # #         plot_title = f"Évolution de {variable_name} pour {station} ({periode})"

# # # # # # # # # #     elif plot_type == 'comparative_variable':
# # # # # # # # # #         if not variable or not periode:
# # # # # # # # # #             flash("Veuillez sélectionner une variable et une période pour le graphique comparatif.", "error")
# # # # # # # # # #             return redirect(url_for('show_visualizations_options'))
# # # # # # # # # #         fig = data_processing.generer_graphique_comparatif(
# # # # # # # # # #             GLOBAL_PROCESSED_DATA_DF, variable, periode,
# # # # # # # # # #             config.CUSTOM_STATION_COLORS, config.METADATA_VARIABLES
# # # # # # # # # #         )
# # # # # # # # # #         variable_name = config.METADATA_VARIABLES.get(variable, {}).get('Nom', variable)
# # # # # # # # # #         plot_title = f"Comparaison de {variable_name} entre stations ({periode})"

# # # # # # # # # #     elif plot_type == 'multi_variable_station':
# # # # # # # # # #         if not station:
# # # # # # # # # #             flash("Veuillez sélectionner une station pour le graphique multi-variables.", "error")
# # # # # # # # # #             return redirect(url_for('show_visualizations_options'))
# # # # # # # # # #         fig = data_processing.generate_multi_variable_station_plot(
# # # # # # # # # #             GLOBAL_PROCESSED_DATA_DF, station,
# # # # # # # # # #             config.CUSTOM_VARIABLE_COLORS, config.METADATA_VARIABLES
# # # # # # # # # #         )
# # # # # # # # # #         plot_title = f"Évolution Normalisée des Variables pour {station}"

# # # # # # # # # #     # Convertir l'objet Figure Plotly en chaîne JSON
# # # # # # # # # #     plot_json_data = fig.to_json()
    
# # # # # # # # # #     # Debug print: Vérifier si plot_json_data est bien généré et sa taille
# # # # # # # # # #     if plot_json_data is not None:
# # # # # # # # # #         print(f"DEBUG (generate_plot): Longueur du plot_json_data généré: {len(plot_json_data)} caractères.")
# # # # # # # # # #     else:
# # # # # # # # # #         print("DEBUG (generate_plot): plot_json_data est None.")

# # # # # # # # # #     if not fig.data: # Vérifier si la figure a des traces de données
# # # # # # # # # #         flash("Impossible de générer le graphique avec les sélections actuelles. Il se peut qu'il n'y ait pas de données pour cette sélection.", "warning")
# # # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # # #     return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)

# # # # # # # # # # if __name__ == '__main__':
# # # # # # # # # #     app.run(debug=True)

# # # # # # # # # ##############################

# # # # # # # # # import pandas as pd
# # # # # # # # # from flask import Flask, render_template, request, redirect, url_for, flash, session
# # # # # # # # # import os
# # # # # # # # # import io
# # # # # # # # # import re # Importation pour les expressions régulières
# # # # # # # # # import json # Pour charger le fichier JSON des coordonnées
# # # # # # # # # import plotly.graph_objects as go # Importation pour gérer les objets Figure

# # # # # # # # # # Importation des fonctions de traitement et de visualisation des données
# # # # # # # # # from data_processing import (
# # # # # # # # #     create_datetime,
# # # # # # # # #     gestion_doublons,
# # # # # # # # #     interpolation,
# # # # # # # # #     traiter_outliers_meteo,
# # # # # # # # #     generer_graphique_par_variable_et_periode,
# # # # # # # # #     generer_graphique_comparatif,
# # # # # # # # #     generate_multi_variable_station_plot,
# # # # # # # # #     daily_stats,
# # # # # # # # #     _load_and_prepare_gps_data
# # # # # # # # # )

# # # # # # # # # # Importation des configurations globales
# # # # # # # # # import config # Importation du module config entier
# # # # # # # # # from config import (
# # # # # # # # #     STATIONS_BY_BASSIN,
# # # # # # # # #     LIMITS_METEO,
# # # # # # # # #     CUSTOM_STATION_COLORS,
# # # # # # # # #     CUSTOM_VARIABLE_COLORS,
# # # # # # # # #     METADATA_VARIABLES
# # # # # # # # # )

# # # # # # # # # # Ajout d'une option pour éviter les avertissements de downcasting de Pandas
# # # # # # # # # pd.set_option('future.no_silent_downcasting', True)

# # # # # # # # # app = Flask(__name__)
# # # # # # # # # app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'une_cle_secrete_par_defaut_tres_forte_pour_dev')

# # # # # # # # # # Variable globale pour stocker les données du DataFrame après traitement
# # # # # # # # # GLOBAL_PROCESSED_DATA_DF = None

# # # # # # # # # # Chemin vers le fichier JSON des coordonnées de station
# # # # # # # # # STATION_COORDS_PATH = os.path.join('data', 'station_coordinates.json')

# # # # # # # # # # Initialisation de config.GLOBAL_DF_GPS_INFO au démarrage de l'application
# # # # # # # # # # Assurez-vous que le dossier 'data' existe
# # # # # # # # # data_dir = 'data'
# # # # # # # # # os.makedirs(data_dir, exist_ok=True)

# # # # # # # # # try:
# # # # # # # # #     # Vérifiez si le fichier JSON des coordonnées existe ou est vide.
# # # # # # # # #     # Si non, appelez la fonction de préparation des données et sauvegardez-les.
# # # # # # # # #     if not os.path.exists(STATION_COORDS_PATH) or os.stat(STATION_COORDS_PATH).st_size == 0:
# # # # # # # # #         print(f"Le fichier '{STATION_COORDS_PATH}' n'existe pas ou est vide. Exécution de data_processing._load_and_prepare_gps_data()...")
# # # # # # # # #         config.GLOBAL_DF_GPS_INFO = _load_and_prepare_gps_data()
# # # # # # # # #         if not config.GLOBAL_DF_GPS_INFO.empty:
# # # # # # # # #             config.GLOBAL_DF_GPS_INFO.to_json(STATION_COORDS_PATH, orient='records', indent=4)
# # # # # # # # #             print(f"Données GPS des stations sauvegardées dans '{STATION_COORDS_PATH}'.")
# # # # # # # # #         else:
# # # # # # # # #             print("Avertissement: Les données GPS sont vides après la préparation, le fichier JSON ne sera pas créé ou sera vide.")
# # # # # # # # #     else:
# # # # # # # # #         config.GLOBAL_DF_GPS_INFO = pd.read_json(STATION_COORDS_PATH, orient='records')
    
# # # # # # # # #     if config.GLOBAL_DF_GPS_INFO.empty:
# # # # # # # # #         raise ValueError("Le DataFrame des données GPS est vide après le chargement/la préparation.")
    
# # # # # # # # #     print("Préparation des données de coordonnées des stations terminée.")
# # # # # # # # # except Exception as e:
# # # # # # # # #     print(f"Erreur fatale lors du chargement ou de la préparation des données GPS des stations au démarrage: {e}. "
# # # # # # # # #           "L'application pourrait ne pas fonctionner correctement sans ces données.")
# # # # # # # # #     config.GLOBAL_DF_GPS_INFO = pd.DataFrame({
# # # # # # # # #         'Station': [],
# # # # # # # # #         'Lat': [],
# # # # # # # # #         'Long': [],
# # # # # # # # #         'Timezone': []
# # # # # # # # #     })


# # # # # # # # # @app.route('/')
# # # # # # # # # def index():
# # # # # # # # #     """
# # # # # # # # #     Route principale affichant le formulaire d'upload de fichiers.
# # # # # # # # #     """
# # # # # # # # #     # Passer stations_by_bassin pour le formulaire d'upload dans index.html
# # # # # # # # #     return render_template('index.html', stations_by_bassin=STATIONS_BY_BASSIN)

# # # # # # # # # @app.route('/upload', methods=['GET', 'POST'])
# # # # # # # # # def upload_file():
# # # # # # # # #     """
# # # # # # # # #     Gère l'upload et le traitement des fichiers CSV/Excel.
# # # # # # # # #     Permet l'upload de multiples fichiers, chacun associé à un bassin et une station,
# # # # # # # # #     en vérifiant leur compatibilité et en les ajoutant au DataFrame global.
# # # # # # # # #     Si la méthode est GET, redirige vers la page d'accueil.
# # # # # # # # #     """
# # # # # # # # #     if request.method == 'GET':
# # # # # # # # #         flash('Accès direct à la page d\'upload non autorisé. Veuillez utiliser le formulaire.', 'warning')
# # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # #     global GLOBAL_PROCESSED_DATA_DF

# # # # # # # # #     upload_groups = []
    
# # # # # # # # #     # Collecter tous les indices de groupe présents dans le formulaire
# # # # # # # # #     all_input_indices = set()
# # # # # # # # #     for key in request.form.keys():
# # # # # # # # #         match = re.search(r'_((\d+))$', key)
# # # # # # # # #         if match:
# # # # # # # # #             all_input_indices.add(int(match.group(1)))
# # # # # # # # #     for key in request.files.keys():
# # # # # # # # #         match = re.search(r'_(\d+)$', key)
# # # # # # # # #         if match:
# # # # # # # # #             all_input_indices.add(int(match.group(1)))
            
# # # # # # # # #     sorted_indices = sorted(list(all_input_indices))

# # # # # # # # #     if not sorted_indices:
# # # # # # # # #         flash('Aucun dataset n\'a été soumis. Veuillez ajouter au moins un dataset.', 'error')
# # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # #     print(f"DEBUG (upload_file): Nombre de groupes d'upload détectés: {len(sorted_indices)}")
# # # # # # # # #     for index in sorted_indices:
# # # # # # # # #         bassin_name = request.form.get(f'bassin_{index}')
# # # # # # # # #         station_name = request.form.get(f'station_{index}')
# # # # # # # # #         file_obj = request.files.get(f'file_input_{index}')

# # # # # # # # #         if not bassin_name or not station_name or not file_obj or file_obj.filename == '':
# # # # # # # # #             flash(f'Le dataset pour le groupe {index+1} est incomplet (bassin, station ou fichier manquant). Veuillez compléter toutes les informations.', 'error')
# # # # # # # # #             return redirect(url_for('index'))

# # # # # # # # #         upload_groups.append({
# # # # # # # # #             'bassin': bassin_name,
# # # # # # # # #             'station': station_name,
# # # # # # # # #             'file': file_obj,
# # # # # # # # #             'index': index
# # # # # # # # #         })
    
# # # # # # # # #     if not upload_groups:
# # # # # # # # #         flash('Aucun dataset valide n\'a été trouvé pour le traitement. Veuillez vérifier vos sélections.', 'error')
# # # # # # # # #         return redirect(url_for('index'))


# # # # # # # # #     processed_dataframes_for_batch = []
    
# # # # # # # # #     expected_raw_data_columns_for_comparison = None
# # # # # # # # #     expected_raw_time_columns_for_comparison = None 

    
# # # # # # # # #     for group_info in upload_groups:
# # # # # # # # #         file = group_info['file']
# # # # # # # # #         bassin = group_info['bassin']
# # # # # # # # #         station = group_info['station']

# # # # # # # # #         file_extension = os.path.splitext(file.filename)[1].lower()
# # # # # # # # #         df_temp = None

# # # # # # # # #         try:
# # # # # # # # #             if file_extension == '.csv':
# # # # # # # # #                 df_temp = pd.read_csv(io.BytesIO(file.read()))
# # # # # # # # #             elif file_extension in ['.xls', '.xlsx']:
# # # # # # # # #                 df_temp = pd.read_excel(io.BytesIO(file.read()))
# # # # # # # # #             else:
# # # # # # # # #                 flash(f'Extension de fichier non supportée pour "{file.filename}". Seuls les fichiers CSV ou Excel sont acceptés.', 'error')
# # # # # # # # #                 return redirect(url_for('index'))
            
# # # # # # # # #             if df_temp is not None:
# # # # # # # # #                 current_file_columns = df_temp.columns.tolist()
                
# # # # # # # # #                 current_raw_time_cols = []
# # # # # # # # #                 if 'Date' in current_file_columns:
# # # # # # # # #                     current_raw_time_cols.append('Date')
# # # # # # # # #                 if all(col in current_file_columns for col in ['Year', 'Month', 'Day', 'Hour', 'Minute']):
# # # # # # # # #                     current_raw_time_cols.extend(['Year', 'Month', 'Day', 'Hour', 'Minute'])
# # # # # # # # #                 current_raw_time_cols_sorted = sorted(list(set(current_raw_time_cols)))

# # # # # # # # #                 current_raw_data_cols_sorted = sorted([
# # # # # # # # #                     col for col in current_file_columns 
# # # # # # # # #                     if col != 'Station' and col not in ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
# # # # # # # # #                 ])

# # # # # # # # #                 if expected_raw_data_columns_for_comparison is None:
# # # # # # # # #                     expected_raw_data_columns_for_comparison = current_raw_data_cols_sorted
# # # # # # # # #                     expected_raw_time_columns_for_comparison = current_raw_time_cols_sorted
# # # # # # # # #                 else:
# # # # # # # # #                     if current_raw_data_cols_sorted != expected_raw_data_columns_for_comparison:
# # # # # # # # #                         flash(f"Le fichier '{file.filename}' (Station: {station}) a des colonnes de données incompatibles avec les données déjà chargées. "
# # # # # # # # #                               f"Attendues: {expected_raw_data_columns_for_comparison}, Obtenues: {current_raw_data_cols_sorted}. Fusion annulée.", 'error')
# # # # # # # # #                         return redirect(url_for('index'))
                    
# # # # # # # # #                     if current_raw_time_cols_sorted != expected_raw_time_columns_for_comparison:
# # # # # # # # #                         flash(f"Le fichier '{file.filename}' (Station: {station}) a des colonnes temporelles incompatibles avec les données déjà chargées. "
# # # # # # # # #                               f"Attendues: {expected_raw_time_columns_for_comparison}, Obtenues: {current_raw_time_cols_sorted}. Fusion annulée.", 'error')
# # # # # # # # #                         return redirect(url_for('index'))
                
# # # # # # # # #                 df_temp['Station'] = station
# # # # # # # # #                 print(f"DEBUG (upload_file): Fichier '{file.filename}' - Station assignée: '{station}'. Premières 5 lignes du DF temporaire:\n{df_temp.head(5)}")
# # # # # # # # #                 processed_dataframes_for_batch.append(df_temp)

# # # # # # # # #         except Exception as e:
# # # # # # # # #             flash(f'Erreur lors de la lecture ou du traitement du fichier "{file.filename}": {e}', 'error')
# # # # # # # # #             print(f"DEBUG (upload_file): Erreur lors du traitement du fichier '{file.filename}': {e}")
# # # # # # # # #             return redirect(url_for('index'))

# # # # # # # # #     if not processed_dataframes_for_batch:
# # # # # # # # #         flash('Aucun fichier valide n\'a pu être traité à partir de ce lot après les vérifications de contenu.', 'error')
# # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # #     try:
# # # # # # # # #         # Concaténation de tous les DataFrames du batch
# # # # # # # # #         df_current_batch_raw_merged = pd.concat(processed_dataframes_for_batch, ignore_index=True)
# # # # # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_raw_merged APRÈS CONCATÉNATION DU BATCH:\n{df_current_batch_raw_merged['Station'].unique().tolist()}")
        
# # # # # # # # #         # Étape 1: Créer la colonne Datetime AVANT la gestion des doublons
# # # # # # # # #         df_current_batch_with_datetime = create_datetime(df_current_batch_raw_merged, bassin=upload_groups[0]['bassin'])
# # # # # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_with_datetime APRÈS CREATE_DATETIME DU BATCH:\n{df_current_batch_with_datetime['Station'].unique().tolist()}")

# # # # # # # # #         # Étape 2: Gérer les doublons maintenant que 'Datetime' est une colonne
# # # # # # # # #         df_current_batch_cleaned = gestion_doublons(df_current_batch_with_datetime)
# # # # # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_cleaned APRÈS GESTION DOUBLONS DU BATCH:\n{df_current_batch_cleaned['Station'].unique().tolist()}")

# # # # # # # # #         if 'Datetime' not in df_current_batch_cleaned.columns or df_current_batch_cleaned['Datetime'].isnull().all():
# # # # # # # # #             flash("Erreur: La colonne 'Datetime' est manquante ou vide après le nettoyage. Impossible de traiter les données.", 'error')
# # # # # # # # #             return redirect(url_for('index'))
        
# # # # # # # # #         # Assurez-vous que 'Datetime' est bien de type datetime et nettoyez les NaT
# # # # # # # # #         df_current_batch_cleaned['Datetime'] = pd.to_datetime(df_current_batch_cleaned['Datetime'], errors='coerce')
# # # # # # # # #         df_current_batch_cleaned.dropna(subset=['Datetime'], inplace=True) 

# # # # # # # # #         if df_current_batch_cleaned.empty:
# # # # # # # # #             flash("Erreur: Toutes les données du lot actuel ont été supprimées en raison de dates invalides. Traitement annulé.", 'error')
# # # # # # # # #             return redirect(url_for('index'))

# # # # # # # # #         # Définir Datetime comme index
# # # # # # # # #         df_current_batch_processed = df_current_batch_cleaned.set_index('Datetime').sort_index()
# # # # # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_processed APRÈS SET_INDEX ET SORT_INDEX DU BATCH:\n{df_current_batch_processed['Station'].unique().tolist()}")


# # # # # # # # #         if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # # # #             GLOBAL_PROCESSED_DATA_DF = df_current_batch_processed
# # # # # # # # #             flash(f'{len(processed_dataframes_for_batch)} fichier(s) téléchargé(s) et initialisé(s).', 'success')
# # # # # # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF initialisé. Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
# # # # # # # # #         else:
# # # # # # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF EXISTANT AVANT FUSION: Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
            
# # # # # # # # #             new_batch_stations = df_current_batch_processed['Station'].unique().tolist()
# # # # # # # # #             print(f"DEBUG (upload_file): Stations dans le batch actuel à fusionner: {new_batch_stations}")

# # # # # # # # #             # Filtrer les données existantes pour les stations du lot actuel du DataFrame global
# # # # # # # # #             df_global_filtered = GLOBAL_PROCESSED_DATA_DF[~GLOBAL_PROCESSED_DATA_DF['Station'].isin(new_batch_stations)].copy()
# # # # # # # # #             print(f"DEBUG (upload_file): Stations dans GLOBAL_PROCESSED_DATA_DF après filtrage des stations du batch actuel:\n{df_global_filtered['Station'].unique().tolist() if not df_global_filtered.empty else 'DataFrame global vide après filtrage'}")

# # # # # # # # #             # Concaténer le DataFrame global filtré avec les données du nouveau lot
# # # # # # # # #             # Assurez-vous que les deux DataFrames ont des index compatibles (DatetimeIndex)
# # # # # # # # #             combined_df = pd.concat([df_global_filtered, df_current_batch_processed], ignore_index=False) # Important: ignore_index=False pour préserver les DatetimeIndex et les aligner
# # # # # # # # #             print(f"DEBUG (upload_file): Stations uniques dans combined_df APRÈS CONCATINATION GLOBAL_FILTRÉ+BATCH:\n{combined_df['Station'].unique().tolist()}")

# # # # # # # # #             # Pas besoin de to_datetime/dropna/set_index ici si les index sont déjà corrects.
# # # # # # # # #             # Cependant, on peut re-trier par sécurité.
# # # # # # # # #             GLOBAL_PROCESSED_DATA_DF = combined_df.sort_index()

# # # # # # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF mis à jour. Stations uniques finales: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

# # # # # # # # #             flash(f'{len(processed_dataframes_for_batch)} fichier(s) ajouté(s). Total de stations uniques: {len(GLOBAL_PROCESSED_DATA_DF["Station"].unique())}.', 'success')

# # # # # # # # #         # --- VÉRIFICATION FINALE ET DÉFENSIVE DE L'INDEX DE GLOBAL_PROCESSED_DATA_DF AVANT TOUT APPEL EXTERNE ---
# # # # # # # # #         print(f"DEBUG: Type de l'index de GLOBAL_PROCESSED_DATA_DF avant vérification finale: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# # # # # # # # #         try:
# # # # # # # # #             # Cette étape est cruciale si l'index pourrait être cassé, mais elle doit être transparente si tout va bien
# # # # # # # # #             if not isinstance(GLOBAL_PROCESSED_DATA_DF.index, pd.DatetimeIndex):
# # # # # # # # #                 # Si l'index n'est pas un DatetimeIndex, essayez de le forcer ou de le recréer à partir d'une colonne Datetime
# # # # # # # # #                 if 'Datetime' in GLOBAL_PROCESSED_DATA_DF.columns:
# # # # # # # # #                     GLOBAL_PROCESSED_DATA_DF['Datetime'] = pd.to_datetime(GLOBAL_PROCESSED_DATA_DF['Datetime'], errors='coerce')
# # # # # # # # #                     GLOBAL_PROCESSED_DATA_DF.dropna(subset=['Datetime'], inplace=True)
# # # # # # # # #                     GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.set_index('Datetime').sort_index()
# # # # # # # # #                 else:
# # # # # # # # #                     raise TypeError("L'index n'est pas DatetimeIndex et aucune colonne 'Datetime' n'est disponible pour la recréation.")
            
# # # # # # # # #             GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.sort_index() # Re-trier au cas où

# # # # # # # # #             print("DEBUG: Index de GLOBAL_PROCESSED_DATA_DF VALIDÉ et RE-CONVERTI en DatetimeIndex (vérification finale réussie).")

# # # # # # # # #         except Exception as e:
# # # # # # # # #             flash(f"Erreur critique finale: Impossible de garantir un DatetimeIndex pour l'interpolation. Le format des dates dans vos fichiers est incompatible. Détails: {e}", 'error')
# # # # # # # # #             print(f"DEBUG: Erreur critique finale lors de la conversion de l'index: {e}")
# # # # # # # # #             return redirect(url_for('index'))
        
# # # # # # # # #         if GLOBAL_PROCESSED_DATA_DF.empty:
# # # # # # # # #             flash("Erreur: Le DataFrame global est vide après le traitement des dates. Il n'y a plus de données valides à analyser.", 'error')
# # # # # # # # #             return redirect(url_for('index'))


# # # # # # # # #         print(f"DEBUG: Type de l'index de GLOBAL_PROCESSED_DATA_DF AVANT interpolation: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# # # # # # # # #         print(f"DEBUG: Dtypes de GLOBAL_PROCESSED_DATA_DF (après vérif index): \n{GLOBAL_PROCESSED_DATA_DF.dtypes}")


# # # # # # # # #         # Traitement d'interpolation et des outliers sur le DataFrame global unifié
# # # # # # # # #         # Ces fonctions attendent un DatetimeIndex
# # # # # # # # #         GLOBAL_PROCESSED_DATA_DF = interpolation(GLOBAL_PROCESSED_DATA_DF, LIMITS_METEO, config.GLOBAL_DF_GPS_INFO)
# # # # # # # # #         GLOBAL_PROCESSED_DATA_DF = traiter_outliers_meteo(GLOBAL_PROCESSED_DATA_DF, LIMITS_METEO)
        
# # # # # # # # #         # Nettoyage des colonnes temporaires
# # # # # # # # #         cols_to_drop_after_process = [
# # # # # # # # #             'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date',
# # # # # # # # #             'sunrise_time_local', 'sunset_time_local'
# # # # # # # # #         ]
# # # # # # # # #         GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.drop(columns=cols_to_drop_after_process, errors='ignore')

# # # # # # # # #         # Mettre à jour les informations de session pour les options de visualisation
# # # # # # # # #         session['available_stations'] = sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist())
# # # # # # # # #         session['available_variables'] = sorted([col for col in GLOBAL_PROCESSED_DATA_DF.columns if col in METADATA_VARIABLES.keys()])
# # # # # # # # #         session['can_compare_stations'] = len(session['available_stations']) >= 2

# # # # # # # # #         flash('Données traitées et fusionnées avec succès ! Vous pouvez maintenant visualiser les résultats.', 'success')
# # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # #     except Exception as e:
# # # # # # # # #         flash(f"Une erreur inattendue s'est produite lors du traitement des données: {e}", 'error')
# # # # # # # # #         print(f"DEBUG: Erreur inattendue dans /upload: {e}")
# # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # # @app.route('/visualizations_options')
# # # # # # # # # def show_visualizations_options():
# # # # # # # # #     """
# # # # # # # # #     Affiche la page des options de visualisation après le traitement des données.
# # # # # # # # #     """
# # # # # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # #     print(f"DEBUG (app.py/visualizations_options): Stations disponibles dans GLOBAL_PROCESSED_DATA_DF: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

# # # # # # # # #     daily_stats_df = daily_stats(GLOBAL_PROCESSED_DATA_DF)
    
# # # # # # # # #     print(f"DEBUG (app.py/visualizations_options): Stations présentes dans daily_stats_df: {daily_stats_df['Station'].unique().tolist() if not daily_stats_df.empty else 'DataFrame vide'}")

# # # # # # # # #     initial_data_html = None
# # # # # # # # #     data_summary = f"Lignes: {len(GLOBAL_PROCESSED_DATA_DF)}, Colonnes: {len(GLOBAL_PROCESSED_DATA_DF.columns)}"
    
# # # # # # # # #     unique_stations_count = len(GLOBAL_PROCESSED_DATA_DF['Station'].unique())

# # # # # # # # #     if unique_stations_count > 1:
# # # # # # # # #         top_n_data = GLOBAL_PROCESSED_DATA_DF.groupby('Station').head(10)
# # # # # # # # #         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
# # # # # # # # #     else:
# # # # # # # # #         top_n_data = GLOBAL_PROCESSED_DATA_DF.head(20)
# # # # # # # # #         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
    
# # # # # # # # #     if initial_data_html is None:
# # # # # # # # #         initial_data_html = "<p class='text-center text-red-500'>Aucune donnée à afficher pour la prévisualisation.</p>"


# # # # # # # # #     daily_stats_html = daily_stats_df.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=False, border=0) if not daily_stats_df.empty else None

# # # # # # # # #     stations = session.get('available_stations', [])
# # # # # # # # #     variables = session.get('available_variables', [])
# # # # # # # # #     can_compare_stations = session.get('can_compare_stations', False)

# # # # # # # # #     periodes = ['Journalière', 'Hebdomadaire', 'Mensuelle', 'Annuelle']

# # # # # # # # #     return render_template(
# # # # # # # # #         'visualizations_options.html',
# # # # # # # # #         initial_data_html=initial_data_html,
# # # # # # # # #         data_summary=data_summary,
# # # # # # # # #         daily_stats_html=daily_stats_html,
# # # # # # # # #         stations=stations,
# # # # # # # # #         variables=variables,
# # # # # # # # #         periodes=periodes,
# # # # # # # # #         can_compare_stations=can_compare_stations,
# # # # # # # # #         METADATA_VARIABLES=METADATA_VARIABLES
# # # # # # # # #     )

# # # # # # # # # @app.route('/generate_single_plot', methods=['GET', 'POST'])
# # # # # # # # # def generate_single_plot():
# # # # # # # # #     """
# # # # # # # # #     Génère et affiche un graphique pour une seule variable et une seule station.
# # # # # # # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # # # # # # #     """
# # # # # # # # #     if request.method == 'GET':
# # # # # # # # #         flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
# # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # #     station_select = request.form.get('station_select_single')
# # # # # # # # #     variable_select = request.form.get('variable_select_single')
# # # # # # # # #     periode_select = request.form.get('periode_select_single')

# # # # # # # # #     if not station_select or not variable_select or not periode_select:
# # # # # # # # #         flash('Veuillez sélectionner une station, une variable et une période pour le graphique.', 'error')
# # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # #     try:
# # # # # # # # #         # La fonction retourne maintenant un objet Figure Plotly
# # # # # # # # #         fig = generer_graphique_par_variable_et_periode(
# # # # # # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # # # # # #             station_select,
# # # # # # # # #             variable_select,
# # # # # # # # #             periode_select,
# # # # # # # # #             CUSTOM_STATION_COLORS,
# # # # # # # # #             METADATA_VARIABLES
# # # # # # # # #         )

# # # # # # # # #         if fig: # Vérifier si la figure a été générée (pas vide)
# # # # # # # # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # # # # # # # #             meta = METADATA_VARIABLES.get(variable_select, {})
# # # # # # # # #             plot_title = f"Évolution de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) pour la station {station_select} ({periode_select})"
# # # # # # # # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # # # # # # # #         else:
# # # # # # # # #             flash("Impossible de générer le graphique. Vérifiez les données ou la sélection.", 'error')
# # # # # # # # #             return redirect(url_for('show_visualizations_options'))
# # # # # # # # #     except TypeError as e: # Capture spécifique de TypeError
# # # # # # # # #         flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
# # # # # # # # #         print(f"DEBUG: Erreur de type dans generate_single_plot: {e}")
# # # # # # # # #         return redirect(url_for('show_visualizations_options'))
# # # # # # # # #     except Exception as e:
# # # # # # # # #         flash(f"Erreur lors de la génération du graphique: {e}", 'error')
# # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # # @app.route('/generate_comparative_plot', methods=['GET', 'POST'])
# # # # # # # # # def generate_comparative_plot():
# # # # # # # # #     """
# # # # # # # # #     Génère et affiche un graphique comparatif pour une variable à travers toutes les stations.
# # # # # # # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # # # # # # #     """
# # # # # # # # #     if request.method == 'GET':
# # # # # # # # #         flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
# # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # #     variable_select = request.form.get('variable_select_comparative')
# # # # # # # # #     periode_select = request.form.get('periode_select_comparative')

# # # # # # # # #     if not variable_select or not periode_select:
# # # # # # # # #         flash('Veuillez sélectionner une variable et une période pour le graphique comparatif.', 'error')
# # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # #     try:
# # # # # # # # #         # La fonction retourne maintenant un objet Figure Plotly
# # # # # # # # #         fig = generer_graphique_comparatif(
# # # # # # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # # # # # #             variable_select,
# # # # # # # # #             periode_select,
# # # # # # # # #             CUSTOM_STATION_COLORS,
# # # # # # # # #             METADATA_VARIABLES
# # # # # # # # #         )

# # # # # # # # #         if fig: # Vérifier si la figure a été générée (pas vide)
# # # # # # # # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # # # # # # # #             meta = METADATA_VARIABLES.get(variable_select, {})
# # # # # # # # #             plot_title = f"Comparaison de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) entre stations ({periode_select})"
# # # # # # # # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # # # # # # # #         else:
# # # # # # # # #             flash("Impossible de générer le graphique comparatif. Vérifiez les données ou la sélection.", 'error')
# # # # # # # # #             return redirect(url_for('show_visualizations_options'))
# # # # # # # # #     except TypeError as e: # Capture spécifique de TypeError
# # # # # # # # #         flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
# # # # # # # # #         print(f"DEBUG: Erreur de type dans generate_comparative_plot: {e}")
# # # # # # # # #         return redirect(url_for('show_visualizations_options'))
# # # # # # # # #     except Exception as e:
# # # # # # # # #         flash(f"Erreur lors de la génération du graphique comparatif: {e}", 'error')
# # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # # @app.route('/generate_multi_variable_plot', methods=['GET', 'POST'])
# # # # # # # # # def generate_multi_variable_plot_route():
# # # # # # # # #     """
# # # # # # # # #     Génère et affiche un graphique d'évolution multi-variables normalisé pour une station.
# # # # # # # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # # # # # # #     """
# # # # # # # # #     if request.method == 'GET':
# # # # # # # # #         flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
# # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # #     station_select = request.form.get('station_select_multi_var')

# # # # # # # # #     if not station_select:
# # # # # # # # #         flash('Veuillez sélectionner une station pour le graphique multi-variables.', 'error')
# # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # #     try:
# # # # # # # # #         # La fonction retourne maintenant un objet Figure Plotly
# # # # # # # # #         fig = generate_multi_variable_station_plot(
# # # # # # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # # # # # #             station_select,
# # # # # # # # #             CUSTOM_VARIABLE_COLORS,
# # # # # # # # #             METADATA_VARIABLES
# # # # # # # # #         )

# # # # # # # # #         if fig: # Vérifier si la figure a été générée (pas vide)
# # # # # # # # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # # # # # # # #             plot_title = f"Évolution Normalisée des Variables Météorologiques pour la station {station_select}"
# # # # # # # # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # # # # # # # #         else:
# # # # # # # # #             flash("Impossible de générer le graphique multi-variables. Vérifiez les données ou la sélection.", 'error')
# # # # # # # # #             return redirect(url_for('show_visualizations_options'))
# # # # # # # # #     except TypeError as e: # Capture spécifique de TypeError
# # # # # # # # #         flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
# # # # # # # # #         print(f"DEBUG: Erreur de type dans generate_multi_variable_plot_route: {e}")
# # # # # # # # #         return redirect(url_for('show_visualizations_options'))
# # # # # # # # #     except Exception as e:
# # # # # # # # #         flash(f"Erreur lors de la génération du graphique multi-variables: {e}", 'error')
# # # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # # @app.route('/reset_data', methods=['GET', 'POST'])
# # # # # # # # # def reset_data():
# # # # # # # # #     """
# # # # # # # # #     Réinitialise les données globales traitées et redirige vers la page d'accueil.
# # # # # # # # #     Si la méthode est GET, redirige simplement vers la page d'accueil.
# # # # # # # # #     """
# # # # # # # # #     if request.method == 'GET':
# # # # # # # # #         return redirect(url_for('index'))

# # # # # # # # #     global GLOBAL_PROCESSED_DATA_DF
# # # # # # # # #     GLOBAL_PROCESSED_DATA_DF = None
# # # # # # # # #     session.pop('available_stations', None)
# # # # # # # # #     session.pop('available_variables', None)
# # # # # # # # #     session.pop('can_compare_stations', None)
# # # # # # # # #     flash('Toutes les données chargées ont été réinitialisées. Vous pouvez commencer une nouvelle analyse.', 'info')
# # # # # # # # #     return redirect(url_for('index'))


# # # # # # # # # if __name__ == '__main__':
# # # # # # # # #     app.run(debug=True)


# # # # # # # # #################

# # # # # # # # import pandas as pd
# # # # # # # # from flask import Flask, render_template, request, redirect, url_for, flash, session
# # # # # # # # import os
# # # # # # # # import io
# # # # # # # # import re # Importation pour les expressions régulières
# # # # # # # # import json # Pour charger le fichier JSON des coordonnées
# # # # # # # # import plotly.graph_objects as go # Importation pour gérer les objets Figure

# # # # # # # # # Importation des fonctions de traitement et de visualisation des données
# # # # # # # # from data_processing import (
# # # # # # # #     create_datetime,
# # # # # # # #     gestion_doublons,
# # # # # # # #     interpolation,
# # # # # # # #     traiter_outliers_meteo,
# # # # # # # #     generer_graphique_par_variable_et_periode,
# # # # # # # #     generer_graphique_comparatif,
# # # # # # # #     generate_multi_variable_station_plot,
# # # # # # # #     calculate_daily_summary_table, # Nouvelle fonction pour le tableau de stats
# # # # # # # #     generate_daily_summary_plot_for_variable, # Nouvelle fonction pour les figures de stats
# # # # # # # #     _load_and_prepare_gps_data
# # # # # # # # )

# # # # # # # # # Importation des configurations globales
# # # # # # # # import config # Importation du module config entier
# # # # # # # # from config import (
# # # # # # # #     STATIONS_BY_BASSIN,
# # # # # # # #     LIMITS_METEO,
# # # # # # # #     CUSTOM_STATION_COLORS,
# # # # # # # #     CUSTOM_VARIABLE_COLORS,
# # # # # # # #     METADATA_VARIABLES,
# # # # # # # #     PALETTE_DEFAUT # Importation de la palette de couleurs
# # # # # # # # )

# # # # # # # # # Ajout d'une option pour éviter les avertissements de downcasting de Pandas
# # # # # # # # pd.set_option('future.no_silent_downcasting', True)

# # # # # # # # app = Flask(__name__)
# # # # # # # # app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'une_cle_secrete_par_defaut_tres_forte_pour_dev')

# # # # # # # # # Variable globale pour stocker les données du DataFrame après traitement
# # # # # # # # GLOBAL_PROCESSED_DATA_DF = None

# # # # # # # # # Chemin vers le fichier JSON des coordonnées de station
# # # # # # # # STATION_COORDS_PATH = os.path.join('data', 'station_coordinates.json')

# # # # # # # # # Initialisation de config.GLOBAL_DF_GPS_INFO au démarrage de l'application
# # # # # # # # # Assurez-vous que le dossier 'data' existe
# # # # # # # # data_dir = 'data'
# # # # # # # # os.makedirs(data_dir, exist_ok=True)

# # # # # # # # try:
# # # # # # # #     # Vérifiez si le fichier JSON des coordonnées existe ou est vide.
# # # # # # # #     # Si non, appelez la fonction de préparation des données et sauvegardez-les.
# # # # # # # #     if not os.path.exists(STATION_COORDS_PATH) or os.stat(STATION_COORDS_PATH).st_size == 0:
# # # # # # # #         print(f"Le fichier '{STATION_COORDS_PATH}' n'existe pas ou est vide. Exécution de data_processing._load_and_prepare_gps_data()...")
# # # # # # # #         config.GLOBAL_DF_GPS_INFO = _load_and_prepare_gps_data()
# # # # # # # #         if not config.GLOBAL_DF_GPS_INFO.empty:
# # # # # # # #             config.GLOBAL_DF_GPS_INFO.to_json(STATION_COORDS_PATH, orient='records', indent=4)
# # # # # # # #             print(f"Données GPS des stations sauvegardées dans '{STATION_COORDS_PATH}'.")
# # # # # # # #         else:
# # # # # # # #             print("Avertissement: Les données GPS sont vides après la préparation, le fichier JSON ne sera pas créé ou sera vide.")
# # # # # # # #     else:
# # # # # # # #         config.GLOBAL_DF_GPS_INFO = pd.read_json(STATION_COORDS_PATH, orient='records')
    
# # # # # # # #     if config.GLOBAL_DF_GPS_INFO.empty:
# # # # # # # #         raise ValueError("Le DataFrame des données GPS est vide après le chargement/la préparation.")
    
# # # # # # # #     print("Préparation des données de coordonnées des stations terminée.")
# # # # # # # # except Exception as e:
# # # # # # # #     print(f"Erreur fatale lors du chargement ou de la préparation des données GPS des stations au démarrage: {e}. "
# # # # # # # #           "L'application pourrait ne pas fonctionner correctement sans ces données.")
# # # # # # # #     config.GLOBAL_DF_GPS_INFO = pd.DataFrame({
# # # # # # # #         'Station': [],
# # # # # # # #         'Lat': [],
# # # # # # # #         'Long': [],
# # # # # # # #         'Timezone': []
# # # # # # # #     })


# # # # # # # # @app.route('/')
# # # # # # # # def index():
# # # # # # # #     """
# # # # # # # #     Route principale affichant le formulaire d'upload de fichiers.
# # # # # # # #     """
# # # # # # # #     # Passer stations_by_bassin pour le formulaire d'upload dans index.html
# # # # # # # #     return render_template('index.html', stations_by_bassin=STATIONS_BY_BASSIN)

# # # # # # # # @app.route('/upload', methods=['GET', 'POST'])
# # # # # # # # def upload_file():
# # # # # # # #     """
# # # # # # # #     Gère l'upload et le traitement des fichiers CSV/Excel.
# # # # # # # #     Permet l'upload de multiples fichiers, chacun associé à un bassin et une station,
# # # # # # # #     en vérifiant leur compatibilité et en les ajoutant au DataFrame global.
# # # # # # # #     Si la méthode est GET, redirige vers la page d'accueil.
# # # # # # # #     """
# # # # # # # #     if request.method == 'GET':
# # # # # # # #         flash('Accès direct à la page d\'upload non autorisé. Veuillez utiliser le formulaire.', 'warning')
# # # # # # # #         return redirect(url_for('index'))

# # # # # # # #     global GLOBAL_PROCESSED_DATA_DF

# # # # # # # #     upload_groups = []
    
# # # # # # # #     # Collecter tous les indices de groupe présents dans le formulaire
# # # # # # # #     all_input_indices = set()
# # # # # # # #     for key in request.form.keys():
# # # # # # # #         match = re.search(r'_((\d+))$', key)
# # # # # # # #         if match:
# # # # # # # #             all_input_indices.add(int(match.group(1)))
# # # # # # # #     for key in request.files.keys():
# # # # # # # #         match = re.search(r'_(\d+)$', key)
# # # # # # # #         if match:
# # # # # # # #             all_input_indices.add(int(match.group(1)))
            
# # # # # # # #     sorted_indices = sorted(list(all_input_indices))

# # # # # # # #     if not sorted_indices:
# # # # # # # #         flash('Aucun dataset n\'a été soumis. Veuillez ajouter au moins un dataset.', 'error')
# # # # # # # #         return redirect(url_for('index'))

# # # # # # # #     print(f"DEBUG (upload_file): Nombre de groupes d'upload détectés: {len(sorted_indices)}")
# # # # # # # #     for index in sorted_indices:
# # # # # # # #         bassin_name = request.form.get(f'bassin_{index}')
# # # # # # # #         station_name = request.form.get(f'station_{index}')
# # # # # # # #         file_obj = request.files.get(f'file_input_{index}')

# # # # # # # #         if not bassin_name or not station_name or not file_obj or file_obj.filename == '':
# # # # # # # #             flash(f'Le dataset pour le groupe {index+1} est incomplet (bassin, station ou fichier manquant). Veuillez compléter toutes les informations.', 'error')
# # # # # # # #             return redirect(url_for('index'))

# # # # # # # #         upload_groups.append({
# # # # # # # #             'bassin': bassin_name,
# # # # # # # #             'station': station_name,
# # # # # # # #             'file': file_obj,
# # # # # # # #             'index': index
# # # # # # # #         })
    
# # # # # # # #     if not upload_groups:
# # # # # # # #         flash('Aucun dataset valide n\'a été trouvé pour le traitement. Veuillez vérifier vos sélections.', 'error')
# # # # # # # #         return redirect(url_for('index'))


# # # # # # # #     processed_dataframes_for_batch = []
    
# # # # # # # #     expected_raw_data_columns_for_comparison = None
# # # # # # # #     expected_raw_time_columns_for_comparison = None 

    
# # # # # # # #     for group_info in upload_groups:
# # # # # # # #         file = group_info['file']
# # # # # # # #         bassin = group_info['bassin']
# # # # # # # #         station = group_info['station']

# # # # # # # #         file_extension = os.path.splitext(file.filename)[1].lower()
# # # # # # # #         df_temp = None

# # # # # # # #         try:
# # # # # # # #             if file_extension == '.csv':
# # # # # # # #                 df_temp = pd.read_csv(io.BytesIO(file.read()))
# # # # # # # #             elif file_extension in ['.xls', '.xlsx']:
# # # # # # # #                 df_temp = pd.read_excel(io.BytesIO(file.read()))
# # # # # # # #             else:
# # # # # # # #                 flash(f'Extension de fichier non supportée pour "{file.filename}". Seuls les fichiers CSV ou Excel sont acceptés.', 'error')
# # # # # # # #                 return redirect(url_for('index'))
            
# # # # # # # #             if df_temp is not None:
# # # # # # # #                 current_file_columns = df_temp.columns.tolist()
                
# # # # # # # #                 current_raw_time_cols = []
# # # # # # # #                 if 'Date' in current_file_columns:
# # # # # # # #                     current_raw_time_cols.append('Date')
# # # # # # # #                 if all(col in current_file_columns for col in ['Year', 'Month', 'Day', 'Hour', 'Minute']):
# # # # # # # #                     current_raw_time_cols.extend(['Year', 'Month', 'Day', 'Hour', 'Minute'])
# # # # # # # #                 current_raw_time_cols_sorted = sorted(list(set(current_raw_time_cols)))

# # # # # # # #                 current_raw_data_cols_sorted = sorted([
# # # # # # # #                     col for col in current_file_columns 
# # # # # # # #                     if col != 'Station' and col not in ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
# # # # # # # #                 ])

# # # # # # # #                 if expected_raw_data_columns_for_comparison is None:
# # # # # # # #                     expected_raw_data_columns_for_comparison = current_raw_data_cols_sorted
# # # # # # # #                     expected_raw_time_columns_for_comparison = current_raw_time_cols_sorted
# # # # # # # #                 else:
# # # # # # # #                     if current_raw_data_cols_sorted != expected_raw_data_columns_for_comparison:
# # # # # # # #                         flash(f"Le fichier '{file.filename}' (Station: {station}) a des colonnes de données incompatibles avec les données déjà chargées. "
# # # # # # # #                               f"Attendues: {expected_raw_data_columns_for_comparison}, Obtenues: {current_raw_data_cols_sorted}. Fusion annulée.", 'error')
# # # # # # # #                         return redirect(url_for('index'))
                    
# # # # # # # #                     if current_raw_time_cols_sorted != expected_raw_time_columns_for_comparison:
# # # # # # # #                         flash(f"Le fichier '{file.filename}' (Station: {station}) a des colonnes temporelles incompatibles avec les données déjà chargées. "
# # # # # # # #                               f"Attendues: {expected_raw_time_columns_for_comparison}, Obtenues: {current_raw_time_cols_sorted}. Fusion annulée.", 'error')
# # # # # # # #                         return redirect(url_for('index'))
                
# # # # # # # #                 df_temp['Station'] = station
# # # # # # # #                 print(f"DEBUG (upload_file): Fichier '{file.filename}' - Station assignée: '{station}'. Premières 5 lignes du DF temporaire:\n{df_temp.head(5)}")
# # # # # # # #                 processed_dataframes_for_batch.append(df_temp)

# # # # # # # #         except Exception as e:
# # # # # # # #             flash(f'Erreur lors de la lecture ou du traitement du fichier "{file.filename}": {e}', 'error')
# # # # # # # #             print(f"DEBUG (upload_file): Erreur lors du traitement du fichier '{file.filename}': {e}")
# # # # # # # #             return redirect(url_for('index'))

# # # # # # # #     if not processed_dataframes_for_batch:
# # # # # # # #         flash('Aucun fichier valide n\'a pu être traité à partir de ce lot après les vérifications de contenu.', 'error')
# # # # # # # #         return redirect(url_for('index'))

# # # # # # # #     try:
# # # # # # # #         # Concaténation de tous les DataFrames du batch
# # # # # # # #         df_current_batch_raw_merged = pd.concat(processed_dataframes_for_batch, ignore_index=True)
# # # # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_raw_merged APRÈS CONCATÉNATION DU BATCH:\n{df_current_batch_raw_merged['Station'].unique().tolist()}")
        
# # # # # # # #         # Étape 1: Créer la colonne Datetime AVANT la gestion des doublons
# # # # # # # #         df_current_batch_with_datetime = create_datetime(df_current_batch_raw_merged, bassin=upload_groups[0]['bassin'])
# # # # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_with_datetime APRÈS CREATE_DATETIME DU BATCH:\n{df_current_batch_with_datetime['Station'].unique().tolist()}")

# # # # # # # #         # Étape 2: Gérer les doublons maintenant que 'Datetime' est une colonne
# # # # # # # #         df_current_batch_cleaned = gestion_doublons(df_current_batch_with_datetime)
# # # # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_cleaned APRÈS GESTION DOUBLONS DU BATCH:\n{df_current_batch_cleaned['Station'].unique().tolist()}")

# # # # # # # #         if 'Datetime' not in df_current_batch_cleaned.columns or df_current_batch_cleaned['Datetime'].isnull().all():
# # # # # # # #             flash("Erreur: La colonne 'Datetime' est manquante ou vide après le nettoyage. Impossible de traiter les données.", 'error')
# # # # # # # #             return redirect(url_for('index'))
        
# # # # # # # #         # Assurez-vous que 'Datetime' est bien de type datetime et nettoyez les NaT
# # # # # # # #         df_current_batch_cleaned['Datetime'] = pd.to_datetime(df_current_batch_cleaned['Datetime'], errors='coerce')
# # # # # # # #         df_current_batch_cleaned.dropna(subset=['Datetime'], inplace=True) 

# # # # # # # #         if df_current_batch_cleaned.empty:
# # # # # # # #             flash("Erreur: Toutes les données du lot actuel ont été supprimées en raison de dates invalides. Traitement annulé.", 'error')
# # # # # # # #             return redirect(url_for('index'))

# # # # # # # #         # Définir Datetime comme index
# # # # # # # #         df_current_batch_processed = df_current_batch_cleaned.set_index('Datetime').sort_index()
# # # # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_processed APRÈS SET_INDEX ET SORT_INDEX DU BATCH:\n{df_current_batch_processed['Station'].unique().tolist()}")


# # # # # # # #         if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # # #             GLOBAL_PROCESSED_DATA_DF = df_current_batch_processed
# # # # # # # #             flash(f'{len(processed_dataframes_for_batch)} fichier(s) téléchargé(s) et initialisé(s).', 'success')
# # # # # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF initialisé. Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
# # # # # # # #         else:
# # # # # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF EXISTANT AVANT FUSION: Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
            
# # # # # # # #             new_batch_stations = df_current_batch_processed['Station'].unique().tolist()
# # # # # # # #             print(f"DEBUG (upload_file): Stations dans le batch actuel à fusionner: {new_batch_stations}")

# # # # # # # #             # Filtrer les données existantes pour les stations du lot actuel du DataFrame global
# # # # # # # #             df_global_filtered = GLOBAL_PROCESSED_DATA_DF[~GLOBAL_PROCESSED_DATA_DF['Station'].isin(new_batch_stations)].copy()
# # # # # # # #             print(f"DEBUG (upload_file): Stations dans GLOBAL_PROCESSED_DATA_DF après filtrage des stations du batch actuel:\n{df_global_filtered['Station'].unique().tolist() if not df_global_filtered.empty else 'DataFrame global vide après filtrage'}")

# # # # # # # #             # Concaténer le DataFrame global filtré avec les données du nouveau lot
# # # # # # # #             # Assurez-vous que les deux DataFrames ont des index compatibles (DatetimeIndex)
# # # # # # # #             combined_df = pd.concat([df_global_filtered, df_current_batch_processed], ignore_index=False) # Important: ignore_index=False pour préserver les DatetimeIndex et les aligner
# # # # # # # #             print(f"DEBUG (upload_file): Stations uniques dans combined_df APRÈS CONCATINATION GLOBAL_FILTRÉ+BATCH:\n{combined_df['Station'].unique().tolist()}")

# # # # # # # #             # Pas besoin de to_datetime/dropna/set_index ici si les index sont déjà corrects.
# # # # # # # #             # Cependant, on peut re-trier par sécurité.
# # # # # # # #             GLOBAL_PROCESSED_DATA_DF = combined_df.sort_index()

# # # # # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF mis à jour. Stations uniques finales: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

# # # # # # # #             flash(f'{len(processed_dataframes_for_batch)} fichier(s) ajouté(s). Total de stations uniques: {len(GLOBAL_PROCESSED_DATA_DF["Station"].unique())}.', 'success')

# # # # # # # #         # --- VÉRIFICATION FINALE ET DÉFENSIVE DE L'INDEX DE GLOBAL_PROCESSED_DATA_DF AVANT TOUT APPEL EXTERNE ---
# # # # # # # #         print(f"DEBUG: Type de l'index de GLOBAL_PROCESSED_DATA_DF avant vérification finale: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# # # # # # # #         try:
# # # # # # # #             # Cette étape est cruciale si l'index pourrait être cassé, mais elle doit être transparente si tout va bien
# # # # # # # #             if not isinstance(GLOBAL_PROCESSED_DATA_DF.index, pd.DatetimeIndex):
# # # # # # # #                 # Si l'index n'est pas un DatetimeIndex, essayez de le forcer ou de le recréer à partir d'une colonne Datetime
# # # # # # # #                 if 'Datetime' in GLOBAL_PROCESSED_DATA_DF.columns:
# # # # # # # #                     GLOBAL_PROCESSED_DATA_DF['Datetime'] = pd.to_datetime(GLOBAL_PROCESSED_DATA_DF['Datetime'], errors='coerce')
# # # # # # # #                     GLOBAL_PROCESSED_DATA_DF.dropna(subset=['Datetime'], inplace=True)
# # # # # # # #                     GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.set_index('Datetime').sort_index()
# # # # # # # #                 else:
# # # # # # # #                     raise TypeError("L'index n'est pas DatetimeIndex et aucune colonne 'Datetime' n'est disponible pour la recréation.")
            
# # # # # # # #             GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.sort_index() # Re-trier au cas où

# # # # # # # #             print("DEBUG: Index de GLOBAL_PROCESSED_DATA_DF VALIDÉ et RE-CONVERTI en DatetimeIndex (vérification finale réussie).")

# # # # # # # #         except Exception as e:
# # # # # # # #             flash(f"Erreur critique finale: Impossible de garantir un DatetimeIndex pour l'interpolation. Le format des dates dans vos fichiers est incompatible. Détails: {e}", 'error')
# # # # # # # #             print(f"DEBUG: Erreur critique finale lors de la conversion de l'index: {e}")
# # # # # # # #             return redirect(url_for('index'))
        
# # # # # # # #         if GLOBAL_PROCESSED_DATA_DF.empty:
# # # # # # # #             flash("Erreur: Le DataFrame global est vide après le traitement des dates. Il n'y a plus de données valides à analyser.", 'error')
# # # # # # # #             return redirect(url_for('index'))


# # # # # # # #         print(f"DEBUG: Type de l'index de GLOBAL_PROCESSED_DATA_DF AVANT interpolation: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# # # # # # # #         print(f"DEBUG: Dtypes de GLOBAL_PROCESSED_DATA_DF (après vérif index): \n{GLOBAL_PROCESSED_DATA_DF.dtypes}")


# # # # # # # #         # Traitement d'interpolation et des outliers sur le DataFrame global unifié
# # # # # # # #         # Ces fonctions attendent un DatetimeIndex
# # # # # # # #         GLOBAL_PROCESSED_DATA_DF = interpolation(GLOBAL_PROCESSED_DATA_DF, LIMITS_METEO, config.GLOBAL_DF_GPS_INFO)
# # # # # # # #         GLOBAL_PROCESSED_DATA_DF = traiter_outliers_meteo(GLOBAL_PROCESSED_DATA_DF, LIMITS_METEO)
        
# # # # # # # #         # Nettoyage des colonnes temporaires
# # # # # # # #         cols_to_drop_after_process = [
# # # # # # # #             'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date',
# # # # # # # #             'sunrise_time_local', 'sunset_time_local'
# # # # # # # #         ]
# # # # # # # #         GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.drop(columns=cols_to_drop_after_process, errors='ignore')

# # # # # # # #         # Mettre à jour les informations de session pour les options de visualisation
# # # # # # # #         session['available_stations'] = sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist())
# # # # # # # #         session['available_variables'] = sorted([col for col in GLOBAL_PROCESSED_DATA_DF.columns if col in METADATA_VARIABLES.keys()])
# # # # # # # #         session['can_compare_stations'] = len(session['available_stations']) >= 2
        
# # # # # # # #         # Filtrer les variables qui ont des métadonnées pour les statistiques journalières détaillées
# # # # # # # #         # Ces variables sont celles que nous pouvons visualiser avec les barres Max/Min/Moyenne etc.
# # # # # # # #         daily_stat_variables_available = [
# # # # # # # #             var for var in METADATA_VARIABLES.keys() if var in GLOBAL_PROCESSED_DATA_DF.columns and 
# # # # # # # #             # Exclure les variables qui ne sont pas numériques après traitement ou n'ont pas de sens pour ces stats
# # # # # # # #             pd.api.types.is_numeric_dtype(GLOBAL_PROCESSED_DATA_DF[var])
# # # # # # # #         ]
# # # # # # # #         session['daily_stat_variables'] = sorted(daily_stat_variables_available)


# # # # # # # #         flash('Données traitées et fusionnées avec succès ! Vous pouvez maintenant visualiser les résultats.', 'success')
# # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # #     except Exception as e:
# # # # # # # #         flash(f"Une erreur inattendue s'est produite lors du traitement des données: {e}", 'error')
# # # # # # # #         print(f"DEBUG: Erreur inattendue dans /upload: {e}")
# # # # # # # #         return redirect(url_for('index'))

# # # # # # # # @app.route('/visualizations_options')
# # # # # # # # def show_visualizations_options():
# # # # # # # #     """
# # # # # # # #     Affiche la page des options de visualisation après le traitement des données.
# # # # # # # #     """
# # # # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # # # #         return redirect(url_for('index'))

# # # # # # # #     print(f"DEBUG (app.py/visualizations_options): Stations disponibles dans GLOBAL_PROCESSED_DATA_DF: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

# # # # # # # #     # Appel de la fonction de calcul des statistiques journalières pour le tableau HTML
# # # # # # # #     daily_stats_df = calculate_daily_summary_table(GLOBAL_PROCESSED_DATA_DF)
    
# # # # # # # #     print(f"DEBUG (app.py/visualizations_options): Stations présentes dans daily_stats_df: {daily_stats_df['Station'].unique().tolist() if not daily_stats_df.empty else 'DataFrame vide'}")

# # # # # # # #     initial_data_html = None
# # # # # # # #     data_summary = f"Lignes: {len(GLOBAL_PROCESSED_DATA_DF)}, Colonnes: {len(GLOBAL_PROCESSED_DATA_DF.columns)}"
    
# # # # # # # #     unique_stations_count = len(GLOBAL_PROCESSED_DATA_DF['Station'].unique())

# # # # # # # #     if unique_stations_count > 1:
# # # # # # # #         top_n_data = GLOBAL_PROCESSED_DATA_DF.groupby('Station').head(10)
# # # # # # # #         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
# # # # # # # #     else:
# # # # # # # #         top_n_data = GLOBAL_PROCESSED_DATA_DF.head(20)
# # # # # # # #         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
    
# # # # # # # #     if initial_data_html is None:
# # # # # # # #         initial_data_html = "<p class='text-center text-red-500'>Aucune donnée à afficher pour la prévisualisation.</p>"


# # # # # # # #     daily_stats_html = daily_stats_df.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=False, border=0) if not daily_stats_df.empty else None

# # # # # # # #     # Récupérer les informations de session pour les menus déroulants
# # # # # # # #     stations = session.get('available_stations', [])
# # # # # # # #     variables = session.get('available_variables', [])
# # # # # # # #     can_compare_stations = session.get('can_compare_stations', False)
# # # # # # # #     daily_stat_variables = session.get('daily_stat_variables', []) # Les variables pertinentes pour les stats journalières

# # # # # # # #     periodes = ['Brute', 'Journalière', 'Hebdomadaire', 'Mensuelle', 'Annuelle']

# # # # # # # #     return render_template(
# # # # # # # #         'visualizations_options.html',
# # # # # # # #         initial_data_html=initial_data_html,
# # # # # # # #         data_summary=data_summary,
# # # # # # # #         daily_stats_html=daily_stats_html,
# # # # # # # #         stations=stations,
# # # # # # # #         variables=variables,
# # # # # # # #         periodes=periodes,
# # # # # # # #         can_compare_stations=can_compare_stations,
# # # # # # # #         METADATA_VARIABLES=METADATA_VARIABLES,
# # # # # # # #         daily_stat_variables=daily_stat_variables # Passer les variables pour les stats journalières
# # # # # # # #     )

# # # # # # # # @app.route('/generate_single_plot', methods=['GET', 'POST'])
# # # # # # # # def generate_single_plot():
# # # # # # # #     """
# # # # # # # #     Génère et affiche un graphique pour une seule variable et une seule station.
# # # # # # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # # # # # #     """
# # # # # # # #     if request.method == 'GET':
# # # # # # # #         flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
# # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # # # #         return redirect(url_for('index'))

# # # # # # # #     station_select = request.form.get('station_select_single')
# # # # # # # #     variable_select = request.form.get('variable_select_single')
# # # # # # # #     periode_select = request.form.get('periode_select_single')

# # # # # # # #     if not station_select or not variable_select or not periode_select:
# # # # # # # #         flash('Veuillez sélectionner une station, une variable et une période pour le graphique.', 'error')
# # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # #     try:
# # # # # # # #         # La fonction retourne maintenant un objet Figure Plotly
# # # # # # # #         fig = generer_graphique_par_variable_et_periode(
# # # # # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # # # # #             station_select,
# # # # # # # #             variable_select,
# # # # # # # #             periode_select,
# # # # # # # #             CUSTOM_STATION_COLORS,
# # # # # # # #             METADATA_VARIABLES
# # # # # # # #         )

# # # # # # # #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# # # # # # # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # # # # # # #             meta = METADATA_VARIABLES.get(variable_select, {})
# # # # # # # #             plot_title = f"Évolution de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) pour la station {station_select} ({periode_select})"
# # # # # # # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # # # # # # #         else:
# # # # # # # #             flash("Impossible de générer le graphique. Vérifiez les données ou la sélection.", 'error')
# # # # # # # #             return redirect(url_for('show_visualizations_options'))
# # # # # # # #     except TypeError as e: # Capture spécifique de TypeError
# # # # # # # #         flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
# # # # # # # #         print(f"DEBUG: Erreur de type dans generate_single_plot: {e}")
# # # # # # # #         return redirect(url_for('show_visualizations_options'))
# # # # # # # #     except Exception as e:
# # # # # # # #         flash(f"Erreur lors de la génération du graphique: {e}", 'error')
# # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # @app.route('/generate_comparative_plot', methods=['GET', 'POST'])
# # # # # # # # def generate_comparative_plot():
# # # # # # # #     """
# # # # # # # #     Génère et affiche un graphique comparatif pour une variable à travers toutes les stations.
# # # # # # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # # # # # #     """
# # # # # # # #     if request.method == 'GET':
# # # # # # # #         flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
# # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # # # #         return redirect(url_for('index'))

# # # # # # # #     variable_select = request.form.get('variable_select_comparative')
# # # # # # # #     periode_select = request.form.get('periode_select_comparative')

# # # # # # # #     if not variable_select or not periode_select:
# # # # # # # #         flash('Veuillez sélectionner une variable et une période pour le graphique comparatif.', 'error')
# # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # #     try:
# # # # # # # #         # La fonction retourne maintenant un objet Figure Plotly
# # # # # # # #         fig = generer_graphique_comparatif(
# # # # # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # # # # #             variable_select,
# # # # # # # #             periode_select,
# # # # # # # #             CUSTOM_STATION_COLORS,
# # # # # # # #             METADATA_VARIABLES
# # # # # # # #         )

# # # # # # # #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# # # # # # # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # # # # # # #             meta = METADATA_VARIABLES.get(variable_select, {})
# # # # # # # #             plot_title = f"Comparaison de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) entre stations ({periode_select})"
# # # # # # # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # # # # # # #         else:
# # # # # # # #             flash("Impossible de générer le graphique comparatif. Vérifiez les données ou la sélection.", 'error')
# # # # # # # #             return redirect(url_for('show_visualizations_options'))
# # # # # # # #     except TypeError as e: # Capture spécifique de TypeError
# # # # # # # #         flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
# # # # # # # #         print(f"DEBUG: Erreur de type dans generate_comparative_plot: {e}")
# # # # # # # #         return redirect(url_for('show_visualizations_options'))
# # # # # # # #     except Exception as e:
# # # # # # # #         flash(f"Erreur lors de la génération du graphique comparatif: {e}", 'error')
# # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # @app.route('/generate_multi_variable_plot', methods=['GET', 'POST'])
# # # # # # # # def generate_multi_variable_plot_route():
# # # # # # # #     """
# # # # # # # #     Génère et affiche un graphique d'évolution multi-variables normalisé pour une station.
# # # # # # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # # # # # #     """
# # # # # # # #     if request.method == 'GET':
# # # # # # # #         flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
# # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # # # #         return redirect(url_for('index'))

# # # # # # # #     station_select = request.form.get('station_select_multi_var')

# # # # # # # #     if not station_select:
# # # # # # # #         flash('Veuillez sélectionner une station pour le graphique multi-variables.', 'error')
# # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # #     try:
# # # # # # # #         # La fonction retourne maintenant un objet Figure Plotly
# # # # # # # #         fig = generate_multi_variable_station_plot(
# # # # # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # # # # #             station_select,
# # # # # # # #             CUSTOM_VARIABLE_COLORS,
# # # # # # # #             METADATA_VARIABLES
# # # # # # # #         )

# # # # # # # #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# # # # # # # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # # # # # # #             plot_title = f"Évolution Normalisée des Variables Météorologiques pour la station {station_select}"
# # # # # # # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # # # # # # #         else:
# # # # # # # #             flash("Impossible de générer le graphique multi-variables. Vérifiez les données ou la sélection.", 'error')
# # # # # # # #             return redirect(url_for('show_visualizations_options'))
# # # # # # # #     except TypeError as e: # Capture spécifique de TypeError
# # # # # # # #         flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
# # # # # # # #         print(f"DEBUG: Erreur de type dans generate_multi_variable_plot_route: {e}")
# # # # # # # #         return redirect(url_for('show_visualizations_options'))
# # # # # # # #     except Exception as e:
# # # # # # # #         flash(f"Erreur lors de la génération du graphique multi-variables: {e}", 'error')
# # # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # # @app.route('/get_daily_stats_plot_json/<station_name>/<variable_name>', methods=['GET'])
# # # # # # # # def get_daily_stats_plot_json(station_name, variable_name):
# # # # # # # #     """
# # # # # # # #     Route AJAX pour obtenir le JSON Plotly d'un graphique de statistiques journalières
# # # # # # # #     pour une station et une variable données.
# # # # # # # #     """
# # # # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # # #         return json.dumps({'error': 'Aucune donnée disponible pour les statistiques journalières.'}), 404

# # # # # # # #     try:
# # # # # # # #         # Appel de la nouvelle fonction dans data_processing pour générer la figure Plotly
# # # # # # # #         fig = generate_daily_summary_plot_for_variable(
# # # # # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # # # # #             station_name,
# # # # # # # #             variable_name,
# # # # # # # #             METADATA_VARIABLES,
# # # # # # # #             PALETTE_DEFAUT
# # # # # # # #         )
        
# # # # # # # #         if fig and fig.data:
# # # # # # # #             return json.dumps(fig.to_dict()) # Retourne le dictionnaire Plotly de la figure
# # # # # # # #         else:
# # # # # # # #             return json.dumps({'error': 'Graphique de statistiques journalières vide pour cette sélection.'}), 404

# # # # # # # #     except Exception as e:
# # # # # # # #         print(f"Erreur lors de la génération du graphique de stats journalières (API): {e}")
# # # # # # # #         return json.dumps({'error': f'Erreur lors de la génération du graphique de stats journalières: {e}'}), 500

# # # # # # # # @app.route('/reset_data', methods=['GET', 'POST'])
# # # # # # # # def reset_data():
# # # # # # # #     """
# # # # # # # #     Réinitialise les données globales traitées et redirige vers la page d'accueil.
# # # # # # # #     Si la méthode est GET, redirige simplement vers la page d'accueil.
# # # # # # # #     """
# # # # # # # #     if request.method == 'GET':
# # # # # # # #         return redirect(url_for('index'))

# # # # # # # #     global GLOBAL_PROCESSED_DATA_DF
# # # # # # # #     GLOBAL_PROCESSED_DATA_DF = None
# # # # # # # #     session.pop('available_stations', None)
# # # # # # # #     session.pop('available_variables', None)
# # # # # # # #     session.pop('can_compare_stations', None)
# # # # # # # #     session.pop('daily_stat_variables', None) # Réinitialiser aussi cette session
# # # # # # # #     flash('Toutes les données chargées ont été réinitialisées. Vous pouvez commencer une nouvelle analyse.', 'info')
# # # # # # # #     return redirect(url_for('index'))


# # # # # # # # if __name__ == '__main__':
# # # # # # # #     app.run(debug=True)


# # # # # # # import pandas as pd
# # # # # # # from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
# # # # # # # import os
# # # # # # # import io
# # # # # # # import re # Importation pour les expressions régulières
# # # # # # # import json # Pour charger le fichier JSON des coordonnées
# # # # # # # import plotly.graph_objects as go # Importation pour gérer les objets Figure Plotly
# # # # # # # import base64 # Pour encoder les images Matplotlib en base64

# # # # # # # # Importation des fonctions de traitement et de visualisation des données
# # # # # # # from data_processing import (
# # # # # # #     create_datetime,
# # # # # # #     gestion_doublons,
# # # # # # #     interpolation,
# # # # # # #     traiter_outliers_meteo,
# # # # # # #     generer_graphique_par_variable_et_periode,
# # # # # # #     generer_graphique_comparatif,
# # # # # # #     generate_multi_variable_station_plot,
# # # # # # #     calculate_daily_summary_table, # Fonction pour le tableau de stats
# # # # # # #     generate_daily_summary_plot_for_variable, # Fonction pour les figures de stats (maintenant Matplotlib)
# # # # # # #     _load_and_prepare_gps_data
# # # # # # # )

# # # # # # # # Importation des configurations globales
# # # # # # # import config # Importation du module config entier
# # # # # # # from config import (
# # # # # # #     STATIONS_BY_BASSIN,
# # # # # # #     LIMITS_METEO,
# # # # # # #     CUSTOM_STATION_COLORS,
# # # # # # #     CUSTOM_VARIABLE_COLORS,
# # # # # # #     METADATA_VARIABLES,
# # # # # # #     PALETTE_DEFAUT # Importation de la palette de couleurs
# # # # # # # )

# # # # # # # # Ajout d'une option pour éviter les avertissements de downcasting de Pandas
# # # # # # # pd.set_option('future.no_silent_downcasting', True)

# # # # # # # app = Flask(__name__)
# # # # # # # app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'une_cle_secrete_par_defaut_tres_forte_pour_dev')

# # # # # # # # Variable globale pour stocker les données du DataFrame après traitement
# # # # # # # GLOBAL_PROCESSED_DATA_DF = None

# # # # # # # # Chemin vers le fichier JSON des coordonnées de station
# # # # # # # STATION_COORDS_PATH = os.path.join('data', 'station_coordinates.json')

# # # # # # # # Initialisation de config.GLOBAL_DF_GPS_INFO au démarrage de l'application
# # # # # # # # Assurez-vous que le dossier 'data' existe
# # # # # # # data_dir = 'data'
# # # # # # # os.makedirs(data_dir, exist_ok=True)

# # # # # # # try:
# # # # # # #     # Vérifiez si le fichier JSON des coordonnées existe ou est vide.
# # # # # # #     # Si non, appelez la fonction de préparation des données et sauvegardez-les.
# # # # # # #     if not os.path.exists(STATION_COORDS_PATH) or os.stat(STATION_COORDS_PATH).st_size == 0:
# # # # # # #         print(f"Le fichier '{STATION_COORDS_PATH}' n'existe pas ou est vide. Exécution de data_processing._load_and_prepare_gps_data()...")
# # # # # # #         config.GLOBAL_DF_GPS_INFO = _load_and_prepare_gps_data()
# # # # # # #         if not config.GLOBAL_DF_GPS_INFO.empty:
# # # # # # #             config.GLOBAL_DF_GPS_INFO.to_json(STATION_COORDS_PATH, orient='records', indent=4)
# # # # # # #             print(f"Données GPS des stations sauvegardées dans '{STATION_COORDS_PATH}'.")
# # # # # # #         else:
# # # # # # #             print("Avertissement: Les données GPS sont vides après la préparation, le fichier JSON ne sera pas créé ou sera vide.")
# # # # # # #     else:
# # # # # # #         config.GLOBAL_DF_GPS_INFO = pd.read_json(STATION_COORDS_PATH, orient='records')
    
# # # # # # #     if config.GLOBAL_DF_GPS_INFO.empty:
# # # # # # #         raise ValueError("Le DataFrame des données GPS est vide après le chargement/la préparation.")
    
# # # # # # #     print("Préparation des données de coordonnées des stations terminée.")
# # # # # # # except Exception as e:
# # # # # # #     print(f"Erreur fatale lors du chargement ou de la préparation des données GPS des stations au démarrage: {e}. "
# # # # # # #           "L'application pourrait ne pas fonctionner correctement sans ces données.")
# # # # # # #     config.GLOBAL_DF_GPS_INFO = pd.DataFrame({
# # # # # # #         'Station': [],
# # # # # # #         'Lat': [],
# # # # # # #         'Long': [],
# # # # # # #         'Timezone': []
# # # # # # #     })


# # # # # # # @app.route('/')
# # # # # # # def index():
# # # # # # #     """
# # # # # # #     Route principale affichant le formulaire d'upload de fichiers.
# # # # # # #     """
# # # # # # #     # Passer stations_by_bassin pour le formulaire d'upload dans index.html
# # # # # # #     return render_template('index.html', stations_by_bassin=STATIONS_BY_BASSIN)

# # # # # # # @app.route('/upload', methods=['GET', 'POST'])
# # # # # # # def upload_file():
# # # # # # #     """
# # # # # # #     Gère l'upload et le traitement des fichiers CSV/Excel.
# # # # # # #     Permet l'upload de multiples fichiers, chacun associé à un bassin et une station,
# # # # # # #     en vérifiant leur compatibilité et en les ajoutant au DataFrame global.
# # # # # # #     Si la méthode est GET, redirige vers la page d'accueil.
# # # # # # #     """
# # # # # # #     if request.method == 'GET':
# # # # # # #         flash('Accès direct à la page d\'upload non autorisé. Veuillez utiliser le formulaire.', 'warning')
# # # # # # #         return redirect(url_for('index'))

# # # # # # #     global GLOBAL_PROCESSED_DATA_DF

# # # # # # #     upload_groups = []
    
# # # # # # #     # Collecter tous les indices de groupe présents dans le formulaire
# # # # # # #     all_input_indices = set()
# # # # # # #     for key in request.form.keys():
# # # # # # #         match = re.search(r'_((\d+))$', key)
# # # # # # #         if match:
# # # # # # #             all_input_indices.add(int(match.group(1)))
# # # # # # #     for key in request.files.keys():
# # # # # # #         match = re.search(r'_(\d+)$', key)
# # # # # # #         if match:
# # # # # # #             all_input_indices.add(int(match.group(1)))
            
# # # # # # #     sorted_indices = sorted(list(all_input_indices))

# # # # # # #     if not sorted_indices:
# # # # # # #         flash('Aucun dataset n\'a été soumis. Veuillez ajouter au moins un dataset.', 'error')
# # # # # # #         return redirect(url_for('index'))

# # # # # # #     print(f"DEBUG (upload_file): Nombre de groupes d'upload détectés: {len(sorted_indices)}")
# # # # # # #     for index in sorted_indices:
# # # # # # #         bassin_name = request.form.get(f'bassin_{index}')
# # # # # # #         station_name = request.form.get(f'station_{index}')
# # # # # # #         # Correction de la faute de frappe : 'index手間' est remplacé par 'index'
# # # # # # #         file_obj = request.files.get(f'file_input_{index}') 

# # # # # # #         if not bassin_name or not station_name or not file_obj or file_obj.filename == '':
# # # # # # #             flash(f'Le dataset pour le groupe {index+1} est incomplet (bassin, station ou fichier manquant). Veuillez compléter toutes les informations.', 'error')
# # # # # # #             return redirect(url_for('index'))

# # # # # # #         upload_groups.append({
# # # # # # #             'bassin': bassin_name,
# # # # # # #             'station': station_name,
# # # # # # #             'file': file_obj,
# # # # # # #             'index': index
# # # # # # #         })
    
# # # # # # #     if not upload_groups:
# # # # # # #         flash('Aucun dataset valide n\'a été trouvé pour le traitement. Veuillez vérifier vos sélections.', 'error')
# # # # # # #         return redirect(url_for('index'))


# # # # # # #     processed_dataframes_for_batch = []
    
# # # # # # #     expected_raw_data_columns_for_comparison = None
# # # # # # #     expected_raw_time_columns_for_comparison = None 

    
# # # # # # #     for group_info in upload_groups:
# # # # # # #         file = group_info['file']
# # # # # # #         bassin = group_info['bassin']
# # # # # # #         station = group_info['station']

# # # # # # #         file_extension = os.path.splitext(file.filename)[1].lower()
# # # # # # #         df_temp = None

# # # # # # #         try:
# # # # # # #             if file_extension == '.csv':
# # # # # # #                 df_temp = pd.read_csv(io.BytesIO(file.read()))
# # # # # # #             elif file_extension in ['.xls', '.xlsx']:
# # # # # # #                 df_temp = pd.read_excel(io.BytesIO(file.read()))
# # # # # # #             else:
# # # # # # #                 flash(f'Extension de fichier non supportée pour "{file.filename}". Seuls les fichiers CSV ou Excel sont acceptés.', 'error')
# # # # # # #                 continue
            
# # # # # # #             if df_temp is not None:
# # # # # # #                 current_file_columns = df_temp.columns.tolist()
                
# # # # # # #                 current_raw_time_cols = []
# # # # # # #                 if 'Date' in current_file_columns:
# # # # # # #                     current_raw_time_cols.append('Date')
# # # # # # #                 if all(col in current_file_columns for col in ['Year', 'Month', 'Day', 'Hour', 'Minute']):
# # # # # # #                     current_raw_time_cols.extend(['Year', 'Month', 'Day', 'Hour', 'Minute'])
# # # # # # #                 current_raw_time_cols_sorted = sorted(list(set(current_raw_time_cols)))

# # # # # # #                 current_raw_data_cols_sorted = sorted([
# # # # # # #                     col for col in current_file_columns 
# # # # # # #                     if col != 'Station' and col not in ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
# # # # # # #                 ])

# # # # # # #                 if expected_raw_data_columns_for_comparison is None:
# # # # # # #                     expected_raw_data_columns_for_comparison = current_raw_data_cols_sorted
# # # # # # #                     expected_raw_time_columns_for_comparison = current_raw_time_cols_sorted
# # # # # # #                 else:
# # # # # # #                     if current_raw_data_cols_sorted != expected_raw_data_columns_for_comparison:
# # # # # # #                         flash(f"Le fichier '{file.filename}' (Station: {station}) a des colonnes de données incompatibles avec les données déjà chargées. "
# # # # # # #                               f"Attendues: {expected_raw_data_columns_for_comparison}, Obtenues: {current_raw_data_cols_sorted}. Fusion annulée.", 'error')
# # # # # # #                         return redirect(url_for('index'))
                    
# # # # # # #                     if current_raw_time_cols_sorted != expected_raw_time_columns_for_comparison:
# # # # # # #                         flash(f"Le fichier '{file.filename}' (Station: {station}) a des colonnes temporelles incompatibles avec les données déjà chargées. "
# # # # # # #                               f"Attendues: {expected_raw_time_columns_for_comparison}, Obtenues: {current_raw_time_cols_sorted}. Fusion annulée.", 'error')
# # # # # # #                         return redirect(url_for('index'))
                
# # # # # # #                 df_temp['Station'] = station
# # # # # # #                 print(f"DEBUG (upload_file): Fichier '{file.filename}' - Station assignée: '{station}'. Premières 5 lignes du DF temporaire:\n{df_temp.head(5)}")
# # # # # # #                 processed_dataframes_for_batch.append(df_temp)

# # # # # # #         except Exception as e:
# # # # # # #             flash(f'Erreur lors de la lecture ou du traitement du fichier "{file.filename}": {e}', 'error')
# # # # # # #             print(f"DEBUG (upload_file): Erreur lors du traitement du fichier '{file.filename}': {e}")
# # # # # # #             return redirect(url_for('index'))

# # # # # # #     if not processed_dataframes_for_batch:
# # # # # # #         flash('Aucun fichier valide n\'a pu être traité à partir de ce lot après les vérifications de contenu.', 'error')
# # # # # # #         return redirect(url_for('index'))

# # # # # # #     try:
# # # # # # #         # Concaténation de tous les DataFrames du batch
# # # # # # #         df_current_batch_raw_merged = pd.concat(processed_dataframes_for_batch, ignore_index=True)
# # # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_raw_merged APRÈS CONCATÉNATION DU BATCH:\n{df_current_batch_raw_merged['Station'].unique().tolist()}")
        
# # # # # # #         # Étape 1: Créer la colonne Datetime AVANT la gestion des doublons
# # # # # # #         df_current_batch_with_datetime = create_datetime(df_current_batch_raw_merged, bassin=upload_groups[0]['bassin'])
# # # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_with_datetime APRÈS CREATE_DATETIME DU BATCH:\n{df_current_batch_with_datetime['Station'].unique().tolist()}")

# # # # # # #         # Étape 2: Gérer les doublons maintenant que 'Datetime' est une colonne
# # # # # # #         df_current_batch_cleaned = gestion_doublons(df_current_batch_with_datetime)
# # # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_cleaned APRÈS GESTION DOUBLONS DU BATCH:\n{df_current_batch_cleaned['Station'].unique().tolist()}")

# # # # # # #         if 'Datetime' not in df_current_batch_cleaned.columns or df_current_batch_cleaned['Datetime'].isnull().all():
# # # # # # #             flash("Erreur: La colonne 'Datetime' est manquante ou vide après le nettoyage. Impossible de traiter les données.", 'error')
# # # # # # #             return redirect(url_for('index'))
        
# # # # # # #         # Assurez-vous que 'Datetime' est bien de type datetime et nettoyez les NaT
# # # # # # #         df_current_batch_cleaned['Datetime'] = pd.to_datetime(df_current_batch_cleaned['Datetime'], errors='coerce')
# # # # # # #         df_current_batch_cleaned.dropna(subset=['Datetime'], inplace=True) 

# # # # # # #         if df_current_batch_cleaned.empty:
# # # # # # #             flash("Erreur: Toutes les données du lot actuel ont été supprimées en raison de dates invalides. Traitement annulé.", 'error')
# # # # # # #             return redirect(url_for('index'))

# # # # # # #         # Définir Datetime comme index
# # # # # # #         df_current_batch_processed = df_current_batch_cleaned.set_index('Datetime').sort_index()
# # # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_processed APRÈS SET_INDEX ET SORT_INDEX DU BATCH:\n{df_current_batch_processed['Station'].unique().tolist()}")


# # # # # # #         if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # #             GLOBAL_PROCESSED_DATA_DF = df_current_batch_processed
# # # # # # #             flash(f'{len(processed_dataframes_for_batch)} fichier(s) téléchargé(s) et initialisé(s).', 'success')
# # # # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF initialisé. Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
# # # # # # #         else:
# # # # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF EXISTANT AVANT FUSION: Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
            
# # # # # # #             new_batch_stations = df_current_batch_processed['Station'].unique().tolist()
# # # # # # #             print(f"DEBUG (upload_file): Stations dans le batch actuel à fusionner: {new_batch_stations}")

# # # # # # #             # Filtrer les données existantes pour les stations du lot actuel du DataFrame global
# # # # # # #             df_global_filtered = GLOBAL_PROCESSED_DATA_DF[~GLOBAL_PROCESSED_DATA_DF['Station'].isin(new_batch_stations)].copy()
# # # # # # #             print(f"DEBUG (upload_file): Stations dans GLOBAL_PROCESSED_DATA_DF après filtrage des stations du batch actuel:\n{df_global_filtered['Station'].unique().tolist() if not df_global_filtered.empty else 'DataFrame global vide après filtrage'}")

# # # # # # #             # Concaténer le DataFrame global filtré avec les données du nouveau lot
# # # # # # #             # Assurez-vous que les deux DataFrames ont des index compatibles (DatetimeIndex)
# # # # # # #             combined_df = pd.concat([df_global_filtered, df_current_batch_processed], ignore_index=False) # Important: ignore_index=False pour préserver les DatetimeIndex et les aligner
# # # # # # #             print(f"DEBUG (upload_file): Stations uniques dans combined_df APRÈS CONCATINATION GLOBAL_FILTRÉ+BATCH:\n{combined_df['Station'].unique().tolist()}")

# # # # # # #             # Pas besoin de to_datetime/dropna/set_index ici si les index sont déjà corrects.
# # # # # # #             # Cependant, on peut re-trier par sécurité.
# # # # # # #             GLOBAL_PROCESSED_DATA_DF = combined_df.sort_index()

# # # # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF mis à jour. Stations uniques finales: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

# # # # # # #             flash(f'{len(processed_dataframes_for_batch)} fichier(s) ajouté(s). Total de stations uniques: {len(GLOBAL_PROCESSED_DATA_DF["Station"].unique())}.', 'success')

# # # # # # #         # --- VÉRIFICATION FINALE ET DÉFENSIVE DE L'INDEX DE GLOBAL_PROCESSED_DATA_DF AVANT TOUT APPEL EXTERNE ---
# # # # # # #         print(f"DEBUG: Type de l'index de GLOBAL_PROCESSED_DATA_DF avant vérification finale: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# # # # # # #         try:
# # # # # # #             # Cette étape est cruciale si l'index pourrait être cassé, mais elle doit être transparente si tout va bien
# # # # # # #             if not isinstance(GLOBAL_PROCESSED_DATA_DF.index, pd.DatetimeIndex):
# # # # # # #                 # Si l'index n'est pas un DatetimeIndex, essayez de le forcer ou de le recréer à partir d'une colonne Datetime
# # # # # # #                 if 'Datetime' in GLOBAL_PROCESSED_DATA_DF.columns:
# # # # # # #                     GLOBAL_PROCESSED_DATA_DF['Datetime'] = pd.to_datetime(GLOBAL_PROCESSED_DATA_DF['Datetime'], errors='coerce')
# # # # # # #                     GLOBAL_PROCESSED_DATA_DF.dropna(subset=['Datetime'], inplace=True)
# # # # # # #                     GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.set_index('Datetime').sort_index()
# # # # # # #                 else:
# # # # # # #                     raise TypeError("L'index n'est pas DatetimeIndex et aucune colonne 'Datetime' n'est disponible pour la recréation.")
            
# # # # # # #             GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.sort_index() # Re-trier au cas où

# # # # # # #             print("DEBUG: Index de GLOBAL_PROCESSED_DATA_DF VALIDÉ et RE-CONVERTI en DatetimeIndex (vérification finale réussie).")

# # # # # # #         except Exception as e:
# # # # # # #             flash(f"Erreur critique finale: Impossible de garantir un DatetimeIndex pour l'interpolation. Le format des dates dans vos fichiers est incompatible. Détails: {e}", 'error')
# # # # # # #             print(f"DEBUG: Erreur critique finale lors de la conversion de l'index: {e}")
# # # # # # #             return redirect(url_for('index'))
        
# # # # # # #         if GLOBAL_PROCESSED_DATA_DF.empty:
# # # # # # #             flash("Erreur: Le DataFrame global est vide après le traitement des dates. Il n'y a plus de données valides à analyser.", 'error')
# # # # # # #             return redirect(url_for('index'))


# # # # # # #         print(f"DEBUG: Type de l'index de GLOBAL_PROCESSED_DATA_DF AVANT interpolation: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# # # # # # #         print(f"DEBUG: Dtypes de GLOBAL_PROCESSED_DATA_DF (après vérif index): \n{GLOBAL_PROCESSED_DATA_DF.dtypes}")


# # # # # # #         # Traitement d'interpolation et des outliers sur le DataFrame global unifié
# # # # # # #         # Ces fonctions attendent un DatetimeIndex
# # # # # # #         GLOBAL_PROCESSED_DATA_DF = interpolation(GLOBAL_PROCESSED_DATA_DF, LIMITS_METEO, config.GLOBAL_DF_GPS_INFO)
# # # # # # #         GLOBAL_PROCESSED_DATA_DF = traiter_outliers_meteo(GLOBAL_PROCESSED_DATA_DF, LIMITS_METEO)
        
# # # # # # #         # Nettoyage des colonnes temporaires
# # # # # # #         cols_to_drop_after_process = [
# # # # # # #             'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date',
# # # # # # #             'sunrise_time_local', 'sunset_time_local'
# # # # # # #         ]
# # # # # # #         GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.drop(columns=cols_to_drop_after_process, errors='ignore')

# # # # # # #         # Mettre à jour les informations de session pour les options de visualisation
# # # # # # #         session['available_stations'] = sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist())
# # # # # # #         session['available_variables'] = sorted([col for col in GLOBAL_PROCESSED_DATA_DF.columns if col in METADATA_VARIABLES.keys()])
# # # # # # #         session['can_compare_stations'] = len(session['available_stations']) >= 2
        
# # # # # # #         # Filtrer les variables qui ont des métadonnées pour les statistiques journalières détaillées
# # # # # # #         # Ces variables sont celles que nous pouvons visualiser avec les barres Max/Min/Moyenne etc.
# # # # # # #         daily_stat_variables_available = [
# # # # # # #             var for var in METADATA_VARIABLES.keys() if var in GLOBAL_PROCESSED_DATA_DF.columns and 
# # # # # # #             # Exclure les variables qui ne sont pas numériques après traitement ou n'ont pas de sens pour ces stats
# # # # # # #             pd.api.types.is_numeric_dtype(GLOBAL_PROCESSED_DATA_DF[var])
# # # # # # #         ]
# # # # # # #         session['daily_stat_variables'] = sorted(daily_stat_variables_available)


# # # # # # #         flash('Données traitées et fusionnées avec succès ! Vous pouvez maintenant visualiser les résultats.', 'success')
# # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # #     except Exception as e:
# # # # # # #         flash(f"Une erreur inattendue s'est produite lors du traitement des données: {e}", 'error')
# # # # # # #         print(f"DEBUG: Erreur inattendue dans /upload: {e}")
# # # # # # #         return redirect(url_for('index'))

# # # # # # # @app.route('/visualizations_options')
# # # # # # # def show_visualizations_options():
# # # # # # #     """
# # # # # # #     Affiche la page des options de visualisation après le traitement des données.
# # # # # # #     """
# # # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # # #         return redirect(url_for('index'))

# # # # # # #     print(f"DEBUG (app.py/visualizations_options): Stations disponibles dans GLOBAL_PROCESSED_DATA_DF: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

# # # # # # #     # Appel de la fonction de calcul des statistiques journalières pour le tableau HTML
# # # # # # #     daily_stats_df = calculate_daily_summary_table(GLOBAL_PROCESSED_DATA_DF)
    
# # # # # # #     print(f"DEBUG (app.py/visualizations_options): Stations présentes dans daily_stats_df: {daily_stats_df['Station'].unique().tolist() if not daily_stats_df.empty else 'DataFrame vide'}")

# # # # # # #     initial_data_html = None
# # # # # # #     data_summary = f"Lignes: {len(GLOBAL_PROCESSED_DATA_DF)}, Colonnes: {len(GLOBAL_PROCESSED_DATA_DF.columns)}"
    
# # # # # # #     unique_stations_count = len(GLOBAL_PROCESSED_DATA_DF['Station'].unique())

# # # # # # #     if unique_stations_count > 1:
# # # # # # #         top_n_data = GLOBAL_PROCESSED_DATA_DF.groupby('Station').head(10)
# # # # # # #         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
# # # # # # #     else:
# # # # # # #         top_n_data = GLOBAL_PROCESSED_DATA_DF.head(20)
# # # # # # #         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
    
# # # # # # #     if initial_data_html is None:
# # # # # # #         initial_data_html = "<p class='text-center text-red-500'>Aucune donnée à afficher pour la prévisualisation.</p>"


# # # # # # #     daily_stats_html = daily_stats_df.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=False, border=0) if not daily_stats_df.empty else None

# # # # # # #     # Récupérer les informations de session pour les menus déroulants
# # # # # # #     stations = session.get('available_stations', [])
# # # # # # #     variables = session.get('available_variables', [])
# # # # # # #     can_compare_stations = session.get('can_compare_stations', False)
# # # # # # #     daily_stat_variables = session.get('daily_stat_variables', []) # Les variables pertinentes pour les stats journalières

# # # # # # #     periodes = ['Brute', 'Journalière', 'Hebdomadaire', 'Mensuelle', 'Annuelle']

# # # # # # #     return render_template(
# # # # # # #         'visualizations_options.html',
# # # # # # #         initial_data_html=initial_data_html,
# # # # # # #         data_summary=data_summary,
# # # # # # #         daily_stats_html=daily_stats_html,
# # # # # # #         stations=stations,
# # # # # # #         variables=variables,
# # # # # # #         periodes=periodes,
# # # # # # #         can_compare_stations=can_compare_stations,
# # # # # # #         METADATA_VARIABLES=METADATA_VARIABLES,
# # # # # # #         daily_stat_variables=daily_stat_variables # Passer les variables pour les stats journalières
# # # # # # #     )

# # # # # # # @app.route('/generate_single_plot', methods=['GET', 'POST'])
# # # # # # # def generate_single_plot():
# # # # # # #     """
# # # # # # #     Génère et affiche un graphique pour une seule variable et une seule station.
# # # # # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # # # # #     """
# # # # # # #     if request.method == 'GET':
# # # # # # #         flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
# # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # # #         return redirect(url_for('index'))

# # # # # # #     station_select = request.form.get('station_select_single')
# # # # # # #     variable_select = request.form.get('variable_select_single')
# # # # # # #     periode_select = request.form.get('periode_select_single')

# # # # # # #     if not station_select or not variable_select or not periode_select:
# # # # # # #         flash('Veuillez sélectionner une station, une variable et une période pour le graphique.', 'error')
# # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # #     try:
# # # # # # #         # La fonction retourne maintenant un objet Figure Plotly
# # # # # # #         fig = generer_graphique_par_variable_et_periode(
# # # # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # # # #             station_select,
# # # # # # #             variable_select,
# # # # # # #             periode_select,
# # # # # # #             CUSTOM_STATION_COLORS,
# # # # # # #             METADATA_VARIABLES
# # # # # # #         )

# # # # # # #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# # # # # # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # # # # # #             meta = METADATA_VARIABLES.get(variable_select, {})
# # # # # # #             plot_title = f"Évolution de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) pour la station {station_select} ({periode_select})"
# # # # # # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # # # # # #         else:
# # # # # # #             flash("Impossible de générer le graphique. Vérifiez les données ou la sélection.", 'error')
# # # # # # #             return redirect(url_for('show_visualizations_options'))
# # # # # # #     except TypeError as e: # Capture spécifique de TypeError
# # # # # # #         flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
# # # # # # #         print(f"DEBUG: Erreur de type dans generate_single_plot: {e}")
# # # # # # #         return redirect(url_for('show_visualizations_options'))
# # # # # # #     except Exception as e:
# # # # # # #         flash(f"Erreur lors de la génération du graphique: {e}", 'error')
# # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # @app.route('/generate_comparative_plot', methods=['GET', 'POST'])
# # # # # # # def generate_comparative_plot():
# # # # # # #     """
# # # # # # #     Génère et affiche un graphique comparatif pour une variable à travers toutes les stations.
# # # # # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # # # # #     """
# # # # # # #     if request.method == 'GET':
# # # # # # #         flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
# # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # # #         return redirect(url_for('index'))

# # # # # # #     variable_select = request.form.get('variable_select_comparative')
# # # # # # #     periode_select = request.form.get('periode_select_comparative')

# # # # # # #     if not variable_select or not periode_select:
# # # # # # #         flash('Veuillez sélectionner une variable et une période pour le graphique comparatif.', 'error')
# # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # #     try:
# # # # # # #         # La fonction retourne maintenant un objet Figure Plotly
# # # # # # #         fig = generer_graphique_comparatif(
# # # # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # # # #             variable_select,
# # # # # # #             periode_select,
# # # # # # #             CUSTOM_STATION_COLORS,
# # # # # # #             METADATA_VARIABLES
# # # # # # #         )

# # # # # # #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# # # # # # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # # # # # #             meta = METADATA_VARIABLES.get(variable_select, {})
# # # # # # #             plot_title = f"Comparaison de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) entre stations ({periode})"
# # # # # # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # # # # # #         else:
# # # # # # #             flash("Impossible de générer le graphique comparatif. Vérifiez les données ou la sélection.", 'error')
# # # # # # #             return redirect(url_for('show_visualizations_options'))
# # # # # # #     except TypeError as e: # Capture spécifique de TypeError
# # # # # # #         flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
# # # # # # #         print(f"DEBUG: Erreur de type dans generate_comparative_plot: {e}")
# # # # # # #         return redirect(url_for('show_visualizations_options'))
# # # # # # #     except Exception as e:
# # # # # # #         flash(f"Erreur lors de la génération du graphique comparatif: {e}", 'error')
# # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # @app.route('/generate_multi_variable_plot', methods=['GET', 'POST'])
# # # # # # # def generate_multi_variable_plot_route():
# # # # # # #     """
# # # # # # #     Génère et affiche un graphique d'évolution multi-variables normalisé pour une station.
# # # # # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # # # # #     """
# # # # # # #     if request.method == 'GET':
# # # # # # #         flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
# # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # # #         return redirect(url_for('index'))

# # # # # # #     station_select = request.form.get('station_select_multi_var')

# # # # # # #     if not station_select:
# # # # # # #         flash('Veuillez sélectionner une station pour le graphique multi-variables.', 'error')
# # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # #     try:
# # # # # # #         # La fonction retourne maintenant un objet Figure Plotly
# # # # # # #         fig = generate_multi_variable_station_plot(
# # # # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # # # #             station_select,
# # # # # # #             CUSTOM_VARIABLE_COLORS,
# # # # # # #             METADATA_VARIABLES
# # # # # # #         )

# # # # # # #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# # # # # # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # # # # # #             plot_title = f"Évolution Normalisée des Variables Météorologiques pour la station {station_select}"
# # # # # # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # # # # # #         else:
# # # # # # #             flash("Impossible de générer le graphique multi-variables. Vérifiez les données ou la sélection.", 'error')
# # # # # # #             return redirect(url_for('show_visualizations_options'))
# # # # # # #     except TypeError as e: # Capture spécifique de TypeError
# # # # # # #         flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
# # # # # # #         print(f"DEBUG: Erreur de type dans generate_multi_variable_plot_route: {e}")
# # # # # # #         return redirect(url_for('show_visualizations_options'))
# # # # # # #     except Exception as e:
# # # # # # #         flash(f"Erreur lors de la génération du graphique multi-variables: {e}", 'error')
# # # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # # @app.route('/get_daily_stats_plot_json/<station_name>/<variable_name>', methods=['GET'])
# # # # # # # def get_daily_stats_plot_json(station_name, variable_name):
# # # # # # #     """
# # # # # # #     Route AJAX pour obtenir le JSON Plotly d'un graphique de statistiques journalières
# # # # # # #     pour une station et une variable données.
# # # # # # #     """
# # # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # # #         return jsonify({'error': 'Aucune donnée disponible pour les statistiques journalières.'}), 404

# # # # # # #     try:
# # # # # # #         # Appel de la fonction de traitement de données pour générer la figure Matplotlib
# # # # # # #         fig = generate_daily_summary_plot_for_variable(
# # # # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # # # #             station_name,
# # # # # # #             variable_name,
# # # # # # #             METADATA_VARIABLES,
# # # # # # #             PALETTE_DEFAUT
# # # # # # #         )
        
# # # # # # #         if fig:
# # # # # # #             # Sauvegarder la figure dans un buffer BytesIO
# # # # # # #             img_buffer = io.BytesIO()
# # # # # # #             fig.savefig(img_buffer, format='png', bbox_inches='tight')
# # # # # # #             img_buffer.seek(0)
            
# # # # # # #             # Encoder l'image en base64
# # # # # # #             encoded_image = base64.b64encode(img_buffer.read()).decode('utf-8')
            
# # # # # # #             # Fermer la figure pour libérer la mémoire
# # # # # # #             plt.close(fig)

# # # # # # #             return jsonify({'image_data': encoded_image, 'mime_type': 'image/png'})
# # # # # # #         else:
# # # # # # #             return jsonify({'error': 'Graphique de statistiques journalières vide pour cette sélection.'}), 404

# # # # # # #     except Exception as e:
# # # # # # #         print(f"Erreur lors de la génération du graphique de stats journalières (API): {e}")
# # # # # # #         return jsonify({'error': f'Erreur lors de la génération du graphique de stats journalières: {e}'}), 500

# # # # # # # @app.route('/reset_data', methods=['GET', 'POST'])
# # # # # # # def reset_data():
# # # # # # #     """
# # # # # # #     Réinitialise les données globales traitées et redirige vers la page d'accueil.
# # # # # # #     Si la méthode est GET, redirige simplement vers la page d'accueil.
# # # # # # #     """
# # # # # # #     if request.method == 'GET':
# # # # # # #         return redirect(url_for('index'))

# # # # # # #     global GLOBAL_PROCESSED_DATA_DF
# # # # # # #     GLOBAL_PROCESSED_DATA_DF = None
# # # # # # #     session.pop('available_stations', None)
# # # # # # #     session.pop('available_variables', None)
# # # # # # #     session.pop('can_compare_stations', None)
# # # # # # #     session.pop('daily_stat_variables', None) # Réinitialiser aussi cette session
# # # # # # #     flash('Toutes les données chargées ont été réinitialisées. Vous pouvez commencer une nouvelle analyse.', 'info')
# # # # # # #     return redirect(url_for('index'))


# # # # # # # if __name__ == '__main__':
# # # # # # #     app.run(debug=True)

# # # # # # ##############
# # # # # # import pandas as pd
# # # # # # from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
# # # # # # import os
# # # # # # import io
# # # # # # import re # Importation pour les expressions régulières
# # # # # # import json # Pour charger le fichier JSON des coordonnées
# # # # # # import plotly.graph_objects as go # Importation pour gérer les objets Figure Plotly
# # # # # # import base64 # Pour encoder les images Matplotlib en base64
# # # # # # import matplotlib.pyplot as plt # NOUVEL IMPORT : Ajout de Matplotlib pour la gestion des figures

# # # # # # # Importation des fonctions de traitement et de visualisation des données
# # # # # # from data_processing import (
# # # # # #     create_datetime,
# # # # # #     gestion_doublons,
# # # # # #     interpolation,
# # # # # #     traiter_outliers_meteo,
# # # # # #     generer_graphique_par_variable_et_periode,
# # # # # #     generer_graphique_comparatif,
# # # # # #     generate_multi_variable_station_plot,
# # # # # #     calculate_daily_summary_table, # Fonction pour le tableau de stats
# # # # # #     generate_daily_summary_plot_for_variable, # Fonction pour les figures de stats (maintenant Matplotlib)
# # # # # #     _load_and_prepare_gps_data
# # # # # # )

# # # # # # # Importation des configurations globales
# # # # # # import config # Importation du module config entier
# # # # # # from config import (
# # # # # #     STATIONS_BY_BASSIN,
# # # # # #     LIMITS_METEO,
# # # # # #     CUSTOM_STATION_COLORS,
# # # # # #     CUSTOM_VARIABLE_COLORS,
# # # # # #     METADATA_VARIABLES,
# # # # # #     PALETTE_DEFAUT # Importation de la palette de couleurs
# # # # # # )

# # # # # # # Ajout d'une option pour éviter les avertissements de downcasting de Pandas
# # # # # # pd.set_option('future.no_silent_downcasting', True)

# # # # # # app = Flask(__name__)
# # # # # # app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'une_cle_secrete_par_defaut_tres_forte_pour_dev')

# # # # # # # Variable globale pour stocker les données du DataFrame après traitement
# # # # # # GLOBAL_PROCESSED_DATA_DF = None

# # # # # # # Chemin vers le fichier JSON des coordonnées de station
# # # # # # STATION_COORDS_PATH = os.path.join('data', 'station_coordinates.json')

# # # # # # # Initialisation de config.GLOBAL_DF_GPS_INFO au démarrage de l'application
# # # # # # # Assurez-vous que le dossier 'data' existe
# # # # # # data_dir = 'data'
# # # # # # os.makedirs(data_dir, exist_ok=True)

# # # # # # try:
# # # # # #     # Vérifiez si le fichier JSON des coordonnées existe ou est vide.
# # # # # #     # Si non, appelez la fonction de préparation des données et sauvegardez-les.
# # # # # #     if not os.path.exists(STATION_COORDS_PATH) or os.stat(STATION_COORDS_PATH).st_size == 0:
# # # # # #         print(f"Le fichier '{STATION_COORDS_PATH}' n'existe pas ou est vide. Exécution de data_processing._load_and_prepare_gps_data()...")
# # # # # #         config.GLOBAL_DF_GPS_INFO = _load_and_prepare_gps_data()
# # # # # #         if not config.GLOBAL_DF_GPS_INFO.empty:
# # # # # #             config.GLOBAL_DF_GPS_INFO.to_json(STATION_COORDS_PATH, orient='records', indent=4)
# # # # # #             print(f"Données GPS des stations sauvegardées dans '{STATION_COORDS_PATH}'.")
# # # # # #         else:
# # # # # #             print("Avertissement: Les données GPS sont vides après la préparation, le fichier JSON ne sera pas créé ou sera vide.")
# # # # # #     else:
# # # # # #         config.GLOBAL_DF_GPS_INFO = pd.read_json(STATION_COORDS_PATH, orient='records')
    
# # # # # #     if config.GLOBAL_DF_GPS_INFO.empty:
# # # # # #         raise ValueError("Le DataFrame des données GPS est vide après le chargement/la préparation.")
    
# # # # # #     print("Préparation des données de coordonnées des stations terminée.")
# # # # # # except Exception as e:
# # # # # #     print(f"Erreur fatale lors du chargement ou de la préparation des données GPS des stations au démarrage: {e}. "
# # # # # #           "L'application pourrait ne pas fonctionner correctement sans ces données.")
# # # # # #     config.GLOBAL_DF_GPS_INFO = pd.DataFrame({
# # # # # #         'Station': [],
# # # # # #         'Lat': [],
# # # # # #         'Long': [],
# # # # # #         'Timezone': []
# # # # # #     })


# # # # # # @app.route('/')
# # # # # # def index():
# # # # # #     """
# # # # # #     Route principale affichant le formulaire d'upload de fichiers.
# # # # # #     """
# # # # # #     # Passer stations_by_bassin pour le formulaire d'upload dans index.html
# # # # # #     return render_template('index.html', stations_by_bassin=STATIONS_BY_BASSIN)

# # # # # # @app.route('/upload', methods=['GET', 'POST'])
# # # # # # def upload_file():
# # # # # #     """
# # # # # #     Gère l'upload et le traitement des fichiers CSV/Excel.
# # # # # #     Permet l'upload de multiples fichiers, chacun associé à un bassin et une station,
# # # # # #     en vérifiant leur compatibilité et en les ajoutant au DataFrame global.
# # # # # #     Si la méthode est GET, redirige vers la page d'accueil.
# # # # # #     """
# # # # # #     if request.method == 'GET':
# # # # # #         flash('Accès direct à la page d\'upload non autorisé. Veuillez utiliser le formulaire.', 'warning')
# # # # # #         return redirect(url_for('index'))

# # # # # #     global GLOBAL_PROCESSED_DATA_DF

# # # # # #     upload_groups = []
    
# # # # # #     # Collecter tous les indices de groupe présents dans le formulaire
# # # # # #     all_input_indices = set()
# # # # # #     for key in request.form.keys():
# # # # # #         match = re.search(r'_((\d+))$', key)
# # # # # #         if match:
# # # # # #             all_input_indices.add(int(match.group(1)))
# # # # # #     for key in request.files.keys():
# # # # # #         match = re.search(r'_(\d+)$', key)
# # # # # #         if match:
# # # # # #             all_input_indices.add(int(match.group(1)))
            
# # # # # #     sorted_indices = sorted(list(all_input_indices))

# # # # # #     if not sorted_indices:
# # # # # #         flash('Aucun dataset n\'a été soumis. Veuillez ajouter au moins un dataset.', 'error')
# # # # # #         return redirect(url_for('index'))

# # # # # #     print(f"DEBUG (upload_file): Nombre de groupes d'upload détectés: {len(sorted_indices)}")
# # # # # #     for index in sorted_indices:
# # # # # #         bassin_name = request.form.get(f'bassin_{index}')
# # # # # #         station_name = request.form.get(f'station_{index}')
# # # # # #         file_obj = request.files.get(f'file_input_{index}') 

# # # # # #         if not bassin_name or not station_name or not file_obj or file_obj.filename == '':
# # # # # #             flash(f'Le dataset pour le groupe {index+1} est incomplet (bassin, station ou fichier manquant). Veuillez compléter toutes les informations.', 'error')
# # # # # #             return redirect(url_for('index'))

# # # # # #         upload_groups.append({
# # # # # #             'bassin': bassin_name,
# # # # # #             'station': station_name,
# # # # # #             'file': file_obj,
# # # # # #             'index': index
# # # # # #         })
    
# # # # # #     if not upload_groups:
# # # # # #         flash('Aucun dataset valide n\'a été trouvé pour le traitement. Veuillez vérifier vos sélections.', 'error')
# # # # # #         return redirect(url_for('index'))


# # # # # #     processed_dataframes_for_batch = []
    
# # # # # #     expected_raw_data_columns_for_comparison = None
# # # # # #     expected_raw_time_columns_for_comparison = None 

    
# # # # # #     for group_info in upload_groups:
# # # # # #         file = group_info['file']
# # # # # #         bassin = group_info['bassin']
# # # # # #         station = group_info['station']

# # # # # #         file_extension = os.path.splitext(file.filename)[1].lower()
# # # # # #         df_temp = None

# # # # # #         try:
# # # # # #             if file_extension == '.csv':
# # # # # #                 df_temp = pd.read_csv(io.BytesIO(file.read()))
# # # # # #             elif file_extension in ['.xls', '.xlsx']:
# # # # # #                 df_temp = pd.read_excel(io.BytesIO(file.read()))
# # # # # #             else:
# # # # # #                 flash(f'Extension de fichier non supportée pour "{file.filename}". Seuls les fichiers CSV ou Excel sont acceptés.', 'error')
# # # # # #                 continue
            
# # # # # #             if df_temp is not None:
# # # # # #                 current_file_columns = df_temp.columns.tolist()
                
# # # # # #                 current_raw_time_cols = []
# # # # # #                 if 'Date' in current_file_columns:
# # # # # #                     current_raw_time_cols.append('Date')
# # # # # #                 if all(col in current_file_columns for col in ['Year', 'Month', 'Day', 'Hour', 'Minute']):
# # # # # #                     current_raw_time_cols.extend(['Year', 'Month', 'Day', 'Hour', 'Minute'])
# # # # # #                 current_raw_time_cols_sorted = sorted(list(set(current_raw_time_cols)))

# # # # # #                 current_raw_data_cols_sorted = sorted([
# # # # # #                     col for col in current_file_columns 
# # # # # #                     if col != 'Station' and col not in ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
# # # # # #                 ])

# # # # # #                 if expected_raw_data_columns_for_comparison is None:
# # # # # #                     expected_raw_data_columns_for_comparison = current_raw_data_cols_sorted
# # # # # #                     expected_raw_time_columns_for_comparison = current_raw_time_cols_sorted
# # # # # #                 else:
# # # # # #                     if current_raw_data_cols_sorted != expected_raw_data_columns_for_comparison:
# # # # # #                         flash(f"Le fichier '{file.filename}' (Station: {station}) a des colonnes de données incompatibles avec les données déjà chargées. "
# # # # # #                               f"Attendues: {expected_raw_data_columns_for_comparison}, Obtenues: {current_raw_data_cols_sorted}. Fusion annulée.", 'error')
# # # # # #                         return redirect(url_for('index'))
                    
# # # # # #                     if current_raw_time_cols_sorted != expected_raw_time_columns_for_comparison:
# # # # # #                         flash(f"Le fichier '{file.filename}' (Station: {station}) a des colonnes temporelles incompatibles avec les données déjà chargées. "
# # # # # #                               f"Attendues: {expected_raw_time_columns_for_comparison}, Obtenues: {current_raw_time_cols_sorted}. Fusion annulée.", 'error')
# # # # # #                         return redirect(url_for('index'))
                
# # # # # #                 df_temp['Station'] = station
# # # # # #                 print(f"DEBUG (upload_file): Fichier '{file.filename}' - Station assignée: '{station}'. Premières 5 lignes du DF temporaire:\n{df_temp.head(5)}")
# # # # # #                 processed_dataframes_for_batch.append(df_temp)

# # # # # #         except Exception as e:
# # # # # #             flash(f'Erreur lors de la lecture ou du traitement du fichier "{file.filename}": {e}', 'error')
# # # # # #             print(f"DEBUG (upload_file): Erreur lors du traitement du fichier '{file.filename}': {e}")
# # # # # #             return redirect(url_for('index'))

# # # # # #     if not processed_dataframes_for_batch:
# # # # # #         flash('Aucun fichier valide n\'a pu être traité à partir de ce lot après les vérifications de contenu.', 'error')
# # # # # #         return redirect(url_for('index'))

# # # # # #     try:
# # # # # #         # Concaténation de tous les DataFrames du batch
# # # # # #         df_current_batch_raw_merged = pd.concat(processed_dataframes_for_batch, ignore_index=True)
# # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_raw_merged APRÈS CONCATÉNATION DU BATCH:\n{df_current_batch_raw_merged['Station'].unique().tolist()}")
        
# # # # # #         # Étape 1: Créer la colonne Datetime AVANT la gestion des doublons
# # # # # #         df_current_batch_with_datetime = create_datetime(df_current_batch_raw_merged, bassin=upload_groups[0]['bassin'])
# # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_with_datetime APRÈS CREATE_DATETIME DU BATCH:\n{df_current_batch_with_datetime['Station'].unique().tolist()}")

# # # # # #         # Étape 2: Gérer les doublons maintenant que 'Datetime' est une colonne
# # # # # #         df_current_batch_cleaned = gestion_doublons(df_current_batch_with_datetime)
# # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_cleaned APRÈS GESTION DOUBLONS DU BATCH:\n{df_current_batch_cleaned['Station'].unique().tolist()}")

# # # # # #         if 'Datetime' not in df_current_batch_cleaned.columns or df_current_batch_cleaned['Datetime'].isnull().all():
# # # # # #             flash("Erreur: La colonne 'Datetime' est manquante ou vide après le nettoyage. Impossible de traiter les données.", 'error')
# # # # # #             return redirect(url_for('index'))
        
# # # # # #         # Assurez-vous que 'Datetime' est bien de type datetime et nettoyez les NaT
# # # # # #         df_current_batch_cleaned['Datetime'] = pd.to_datetime(df_current_batch_cleaned['Datetime'], errors='coerce')
# # # # # #         df_current_batch_cleaned.dropna(subset=['Datetime'], inplace=True) 

# # # # # #         if df_current_batch_cleaned.empty:
# # # # # #             flash("Erreur: Toutes les données du lot actuel ont été supprimées en raison de dates invalides. Traitement annulé.", 'error')
# # # # # #             return redirect(url_for('index'))

# # # # # #         # Définir Datetime comme index
# # # # # #         df_current_batch_processed = df_current_batch_cleaned.set_index('Datetime').sort_index()
# # # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_processed APRÈS SET_INDEX ET SORT_INDEX DU BATCH:\n{df_current_batch_processed['Station'].unique().tolist()}")


# # # # # #         if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # #             GLOBAL_PROCESSED_DATA_DF = df_current_batch_processed
# # # # # #             flash(f'{len(processed_dataframes_for_batch)} fichier(s) téléchargé(s) et initialisé(s).', 'success')
# # # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF initialisé. Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
# # # # # #         else:
# # # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF EXISTANT AVANT FUSION: Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
            
# # # # # #             new_batch_stations = df_current_batch_processed['Station'].unique().tolist()
# # # # # #             print(f"DEBUG (upload_file): Stations dans le batch actuel à fusionner: {new_batch_stations}")

# # # # # #             # Filtrer les données existantes pour les stations du lot actuel du DataFrame global
# # # # # #             df_global_filtered = GLOBAL_PROCESSED_DATA_DF[~GLOBAL_PROCESSED_DATA_DF['Station'].isin(new_batch_stations)].copy()
# # # # # #             print(f"DEBUG (upload_file): Stations dans GLOBAL_PROCESSED_DATA_DF après filtrage des stations du batch actuel:\n{df_global_filtered['Station'].unique().tolist() if not df_global_filtered.empty else 'DataFrame global vide après filtrage'}")

# # # # # #             # Concaténer le DataFrame global filtré avec les données du nouveau lot
# # # # # #             # Assurez-vous que les deux DataFrames ont des index compatibles (DatetimeIndex)
# # # # # #             combined_df = pd.concat([df_global_filtered, df_current_batch_processed], ignore_index=False) # Important: ignore_index=False pour préserver les DatetimeIndex et les aligner
# # # # # #             print(f"DEBUG (upload_file): Stations uniques dans combined_df APRÈS CONCATINATION GLOBAL_FILTRÉ+BATCH:\n{combined_df['Station'].unique().tolist()}")

# # # # # #             # Pas besoin de to_datetime/dropna/set_index ici si les index sont déjà corrects.
# # # # # #             # Cependant, on peut re-trier par sécurité.
# # # # # #             GLOBAL_PROCESSED_DATA_DF = combined_df.sort_index()

# # # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF mis à jour. Stations uniques finales: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

# # # # # #             flash(f'{len(processed_dataframes_for_batch)} fichier(s) ajouté(s). Total de stations uniques: {len(GLOBAL_PROCESSED_DATA_DF["Station"].unique())}.', 'success')

# # # # # #         # --- VÉRIFICATION FINALE ET DÉFENSIVE DE L'INDEX DE GLOBAL_PROCESSED_DATA_DF AVANT TOUT APPEL EXTERNE ---
# # # # # #         print(f"DEBUG: Type de l'index de GLOBAL_PROCESSED_DATA_DF avant vérification finale: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# # # # # #         try:
# # # # # #             # Cette étape est cruciale si l'index pourrait être cassé, mais elle doit être transparente si tout va bien
# # # # # #             if not isinstance(GLOBAL_PROCESSED_DATA_DF.index, pd.DatetimeIndex):
# # # # # #                 # Si l'index n'est pas un DatetimeIndex, essayez de le forcer ou de le recréer à partir d'une colonne Datetime
# # # # # #                 if 'Datetime' in GLOBAL_PROCESSED_DATA_DF.columns:
# # # # # #                     GLOBAL_PROCESSED_DATA_DF['Datetime'] = pd.to_datetime(GLOBAL_PROCESSED_DATA_DF['Datetime'], errors='coerce')
# # # # # #                     GLOBAL_PROCESSED_DATA_DF.dropna(subset=['Datetime'], inplace=True)
# # # # # #                     GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.set_index('Datetime').sort_index()
# # # # # #                 else:
# # # # # #                     raise TypeError("L'index n'est pas DatetimeIndex et aucune colonne 'Datetime' n'est disponible pour la recréation.")
            
# # # # # #             GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.sort_index() # Re-trier au cas où

# # # # # #             print("DEBUG: Index de GLOBAL_PROCESSED_DATA_DF VALIDÉ et RE-CONVERTI en DatetimeIndex (vérification finale réussie).")

# # # # # #         except Exception as e:
# # # # # #             flash(f"Erreur critique finale: Impossible de garantir un DatetimeIndex pour l'interpolation. Le format des dates dans vos fichiers est incompatible. Détails: {e}", 'error')
# # # # # #             print(f"DEBUG: Erreur critique finale lors de la conversion de l'index: {e}")
# # # # # #             return redirect(url_for('index'))
        
# # # # # #         if GLOBAL_PROCESSED_DATA_DF.empty:
# # # # # #             flash("Erreur: Le DataFrame global est vide après le traitement des dates. Il n'y a plus de données valides à analyser.", 'error')
# # # # # #             return redirect(url_for('index'))


# # # # # #         print(f"DEBUG: Type de l'index de GLOBAL_PROCESSED_DATA_DF AVANT interpolation: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# # # # # #         print(f"DEBUG: Dtypes de GLOBAL_PROCESSED_DATA_DF (après vérif index): \n{GLOBAL_PROCESSED_DATA_DF.dtypes}")


# # # # # #         # Traitement d'interpolation et des outliers sur le DataFrame global unifié
# # # # # #         # Ces fonctions attendent un DatetimeIndex
# # # # # #         GLOBAL_PROCESSED_DATA_DF = interpolation(GLOBAL_PROCESSED_DATA_DF, LIMITS_METEO, config.GLOBAL_DF_GPS_INFO)
# # # # # #         GLOBAL_PROCESSED_DATA_DF = traiter_outliers_meteo(GLOBAL_PROCESSED_DATA_DF, LIMITS_METEO)
        
# # # # # #         # Nettoyage des colonnes temporaires
# # # # # #         cols_to_drop_after_process = [
# # # # # #             'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date',
# # # # # #             'sunrise_time_local', 'sunset_time_local'
# # # # # #         ]
# # # # # #         GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.drop(columns=cols_to_drop_after_process, errors='ignore')

# # # # # #         # Mettre à jour les informations de session pour les options de visualisation
# # # # # #         session['available_stations'] = sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist())
# # # # # #         session['available_variables'] = sorted([col for col in GLOBAL_PROCESSED_DATA_DF.columns if col in METADATA_VARIABLES.keys()])
# # # # # #         session['can_compare_stations'] = len(session['available_stations']) >= 2
        
# # # # # #         # Filtrer les variables qui ont des métadonnées pour les statistiques journalières détaillées
# # # # # #         # Ces variables sont celles que nous pouvons visualiser avec les barres Max/Min/Moyenne etc.
# # # # # #         daily_stat_variables_available = [
# # # # # #             var for var in METADATA_VARIABLES.keys() if var in GLOBAL_PROCESSED_DATA_DF.columns and 
# # # # # #             # Exclure les variables qui ne sont pas numériques après traitement ou n'ont pas de sens pour ces stats
# # # # # #             pd.api.types.is_numeric_dtype(GLOBAL_PROCESSED_DATA_DF[var])
# # # # # #         ]
# # # # # #         session['daily_stat_variables'] = sorted(daily_stat_variables_available)


# # # # # #         flash('Données traitées et fusionnées avec succès ! Vous pouvez maintenant visualiser les résultats.', 'success')
# # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # #     except Exception as e:
# # # # # #         flash(f"Une erreur inattendue s'est produite lors du traitement des données: {e}", 'error')
# # # # # #         print(f"DEBUG: Erreur inattendue dans /upload: {e}")
# # # # # #         return redirect(url_for('index'))

# # # # # # @app.route('/visualizations_options')
# # # # # # def show_visualizations_options():
# # # # # #     """
# # # # # #     Affiche la page des options de visualisation après le traitement des données.
# # # # # #     """
# # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # #         return redirect(url_for('index'))

# # # # # #     print(f"DEBUG (app.py/visualizations_options): Stations disponibles dans GLOBAL_PROCESSED_DATA_DF: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

# # # # # #     # Appel de la fonction de calcul des statistiques journalières pour le tableau HTML
# # # # # #     daily_stats_df = calculate_daily_summary_table(GLOBAL_PROCESSED_DATA_DF)
    
# # # # # #     print(f"DEBUG (app.py/visualizations_options): Stations présentes dans daily_stats_df: {daily_stats_df['Station'].unique().tolist() if not daily_stats_df.empty else 'DataFrame vide'}")

# # # # # #     initial_data_html = None
# # # # # #     data_summary = f"Lignes: {len(GLOBAL_PROCESSED_DATA_DF)}, Colonnes: {len(GLOBAL_PROCESSED_DATA_DF.columns)}"
    
# # # # # #     unique_stations_count = len(GLOBAL_PROCESSED_DATA_DF['Station'].unique())

# # # # # #     if unique_stations_count > 1:
# # # # # #         top_n_data = GLOBAL_PROCESSED_DATA_DF.groupby('Station').head(10)
# # # # # #         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
# # # # # #     else:
# # # # # #         top_n_data = GLOBAL_PROCESSED_DATA_DF.head(20)
# # # # # #         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
    
# # # # # #     if initial_data_html is None:
# # # # # #         initial_data_html = "<p class='text-center text-red-500'>Aucune donnée à afficher pour la prévisualisation.</p>"


# # # # # #     daily_stats_html = daily_stats_df.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=False, border=0) if not daily_stats_df.empty else None

# # # # # #     # Récupérer les informations de session pour les menus déroulants
# # # # # #     stations = session.get('available_stations', [])
# # # # # #     variables = session.get('available_variables', [])
# # # # # #     can_compare_stations = session.get('can_compare_stations', False)
# # # # # #     daily_stat_variables = session.get('daily_stat_variables', []) # Les variables pertinentes pour les stats journalières

# # # # # #     periodes = ['Brute', 'Journalière', 'Hebdomadaire', 'Mensuelle', 'Annuelle']

# # # # # #     return render_template(
# # # # # #         'visualizations_options.html',
# # # # # #         initial_data_html=initial_data_html,
# # # # # #         data_summary=data_summary,
# # # # # #         daily_stats_html=daily_stats_html,
# # # # # #         stations=stations,
# # # # # #         variables=variables,
# # # # # #         periodes=periodes,
# # # # # #         can_compare_stations=can_compare_stations,
# # # # # #         METADATA_VARIABLES=METADATA_VARIABLES,
# # # # # #         daily_stat_variables=daily_stat_variables # Passer les variables pour les stats journalières
# # # # # #     )

# # # # # # @app.route('/generate_single_plot', methods=['GET', 'POST'])
# # # # # # def generate_single_plot():
# # # # # #     """
# # # # # #     Génère et affiche un graphique pour une seule variable et une seule station.
# # # # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # # # #     """
# # # # # #     if request.method == 'GET':
# # # # # #         flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
# # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # #         return redirect(url_for('index'))

# # # # # #     station_select = request.form.get('station_select_single')
# # # # # #     variable_select = request.form.get('variable_select_single')
# # # # # #     periode_select = request.form.get('periode_select_single')

# # # # # #     if not station_select or not variable_select or not periode_select:
# # # # # #         flash('Veuillez sélectionner une station, une variable et une période pour le graphique.', 'error')
# # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # #     try:
# # # # # #         # La fonction retourne maintenant un objet Figure Plotly
# # # # # #         fig = generer_graphique_par_variable_et_periode(
# # # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # # #             station_select,
# # # # # #             variable_select,
# # # # # #             periode_select,
# # # # # #             CUSTOM_STATION_COLORS,
# # # # # #             METADATA_VARIABLES
# # # # # #         )

# # # # # #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# # # # # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # # # # #             meta = METADATA_VARIABLES.get(variable_select, {})
# # # # # #             plot_title = f"Évolution de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) pour la station {station_select} ({periode_select})"
# # # # # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # # # # #         else:
# # # # # #             flash("Impossible de générer le graphique. Vérifiez les données ou la sélection.", 'error')
# # # # # #             return redirect(url_for('show_visualizations_options'))
# # # # # #     except TypeError as e: # Capture spécifique de TypeError
# # # # # #         flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
# # # # # #         print(f"DEBUG: Erreur de type dans generate_single_plot: {e}")
# # # # # #         return redirect(url_for('show_visualizations_options'))
# # # # # #     except Exception as e:
# # # # # #         flash(f"Erreur lors de la génération du graphique: {e}", 'error')
# # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # @app.route('/generate_comparative_plot', methods=['GET', 'POST'])
# # # # # # def generate_comparative_plot():
# # # # # #     """
# # # # # #     Génère et affiche un graphique comparatif pour une variable à travers toutes les stations.
# # # # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # # # #     """
# # # # # #     if request.method == 'GET':
# # # # # #         flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
# # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # #         return redirect(url_for('index'))

# # # # # #     variable_select = request.form.get('variable_select_comparative')
# # # # # #     periode_select = request.form.get('periode_select_comparative')

# # # # # #     if not variable_select or not periode_select:
# # # # # #         flash('Veuillez sélectionner une variable et une période pour le graphique comparatif.', 'error')
# # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # #     try:
# # # # # #         # La fonction retourne maintenant un objet Figure Plotly
# # # # # #         fig = generer_graphique_comparatif(
# # # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # # #             variable_select,
# # # # # #             periode_select,
# # # # # #             CUSTOM_STATION_COLORS,
# # # # # #             METADATA_VARIABLES
# # # # # #         )

# # # # # #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# # # # # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # # # # #             meta = METADATA_VARIABLES.get(variable_select, {})
# # # # # #             plot_title = f"Comparaison de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) entre stations ({periode})"
# # # # # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # # # # #         else:
# # # # # #             flash("Impossible de générer le graphique comparatif. Vérifiez les données ou la sélection.", 'error')
# # # # # #             return redirect(url_for('show_visualizations_options'))
# # # # # #     except TypeError as e: # Capture spécifique de TypeError
# # # # # #         flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
# # # # # #         print(f"DEBUG: Erreur de type dans generate_comparative_plot: {e}")
# # # # # #         return redirect(url_for('show_visualizations_options'))
# # # # # #     except Exception as e:
# # # # # #         flash(f"Erreur lors de la génération du graphique comparatif: {e}", 'error')
# # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # @app.route('/generate_multi_variable_plot', methods=['GET', 'POST'])
# # # # # # def generate_multi_variable_plot_route():
# # # # # #     """
# # # # # #     Génère et affiche un graphique d'évolution multi-variables normalisé pour une station.
# # # # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # # # #     """
# # # # # #     if request.method == 'GET':
# # # # # #         flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
# # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # #         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
# # # # # #         return redirect(url_for('index'))

# # # # # #     station_select = request.form.get('station_select_multi_var')

# # # # # #     if not station_select:
# # # # # #         flash('Veuillez sélectionner une station pour le graphique multi-variables.', 'error')
# # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # #     try:
# # # # # #         # La fonction retourne maintenant un objet Figure Plotly
# # # # # #         fig = generate_multi_variable_station_plot(
# # # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # # #             station_select,
# # # # # #             CUSTOM_VARIABLE_COLORS,
# # # # # #             METADATA_VARIABLES
# # # # # #         )

# # # # # #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# # # # # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # # # # #             plot_title = f"Évolution Normalisée des Variables Météorologiques pour la station {station_select}"
# # # # # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # # # # #         else:
# # # # # #             flash("Impossible de générer le graphique multi-variables. Vérifiez les données ou la sélection.", 'error')
# # # # # #             return redirect(url_for('show_visualizations_options'))
# # # # # #     except TypeError as e: # Capture spécifique de TypeError
# # # # # #         flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
# # # # # #         print(f"DEBUG: Erreur de type dans generate_multi_variable_plot_route: {e}")
# # # # # #         return redirect(url_for('show_visualizations_options'))
# # # # # #     except Exception as e:
# # # # # #         flash(f"Erreur lors de la génération du graphique multi-variables: {e}", 'error')
# # # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # # @app.route('/get_daily_stats_plot_json/<station_name>/<variable_name>', methods=['GET'])
# # # # # # def get_daily_stats_plot_json(station_name, variable_name):
# # # # # #     """
# # # # # #     Route AJAX pour obtenir le JSON Plotly d'un graphique de statistiques journalières
# # # # # #     pour une station et une variable données.
# # # # # #     """
# # # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # # #         return jsonify({'error': 'Aucune donnée disponible pour les statistiques journalières.'}), 404

# # # # # #     try:
# # # # # #         # Appel de la fonction de traitement de données pour générer la figure Matplotlib
# # # # # #         fig = generate_daily_summary_plot_for_variable(
# # # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # # #             station_name,
# # # # # #             variable_name,
# # # # # #             METADATA_VARIABLES,
# # # # # #             PALETTE_DEFAUT
# # # # # #         )
        
# # # # # #         if fig:
# # # # # #             # Sauvegarder la figure dans un buffer BytesIO
# # # # # #             img_buffer = io.BytesIO()
# # # # # #             fig.savefig(img_buffer, format='png', bbox_inches='tight')
# # # # # #             img_buffer.seek(0)
            
# # # # # #             # Encoder l'image en base64
# # # # # #             encoded_image = base64.b64encode(img_buffer.read()).decode('utf-8')
            
# # # # # #             # Fermer la figure pour libérer la mémoire
# # # # # #             plt.close(fig)

# # # # # #             return jsonify({'image_data': encoded_image, 'mime_type': 'image/png'})
# # # # # #         else:
# # # # # #             return jsonify({'error': 'Graphique de statistiques journalières vide pour cette sélection.'}), 404

# # # # # #     except Exception as e:
# # # # # #         print(f"Erreur lors de la génération du graphique de stats journalières (API): {e}")
# # # # # #         return jsonify({'error': f'Erreur lors de la génération du graphique de stats journalières: {e}'}), 500

# # # # # # @app.route('/reset_data', methods=['GET', 'POST'])
# # # # # # def reset_data():
# # # # # #     """
# # # # # #     Réinitialise les données globales traitées et redirige vers la page d'accueil.
# # # # # #     Si la méthode est GET, redirige simplement vers la page d'accueil.
# # # # # #     """
# # # # # #     if request.method == 'GET':
# # # # # #         return redirect(url_for('index'))

# # # # # #     global GLOBAL_PROCESSED_DATA_DF
# # # # # #     GLOBAL_PROCESSED_DATA_DF = None
# # # # # #     session.pop('available_stations', None)
# # # # # #     session.pop('available_variables', None)
# # # # # #     session.pop('can_compare_stations', None)
# # # # # #     session.pop('daily_stat_variables', None) # Réinitialiser aussi cette session
# # # # # #     flash('Toutes les données chargées ont été réinitialisées. Vous pouvez commencer une nouvelle analyse.', 'info')
# # # # # #     return redirect(url_for('index'))


# # # # # # if __name__ == '__main__':
# # # # # #     app.run(debug=True)


# # # # # import pandas as pd
# # # # # from flask import Flask, render_template, request, jsonify, redirect, url_for
# # # # # from werkzeug.utils import secure_filename
# # # # # import os
# # # # # import io
# # # # # import base64
# # # # # from datetime import datetime
# # # # # import json
# # # # # import plotly.graph_objects as go
# # # # # import matplotlib.pyplot as plt # Importation de Matplotlib
# # # # # import seaborn as sns # Importation de Seaborn
# # # # # import traceback # Importation de traceback pour les messages d'erreur détaillés
# # # # # import re # Importation du module re pour les expressions régulières

# # # # # # Importation des fonctions de traitement de données
# # # # # from data_processing import (
# # # # #     create_datetime, interpolation, _load_and_prepare_gps_data,
# # # # #     gestion_doublons, traiter_outliers_meteo, create_rain_mm,
# # # # #     generer_graphique_par_variable_et_periode, generer_graphique_comparatif,
# # # # #     generate_multi_variable_station_plot, calculate_daily_summary_table,
# # # # #     generate_daily_summary_plot_for_variable
# # # # # )

# # # # # # Importation des configurations globales
# # # # # from config import METADATA_VARIABLES, PALETTE_DEFAUT, DATA_LIMITS, STATIONS_BY_BASSIN

# # # # # app = Flask(__name__)
# # # # # app.config['UPLOAD_FOLDER'] = 'uploads'
# # # # # app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 # 16 MB max-upload size
# # # # # os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# # # # # # Variable globale pour stocker les données du DataFrame après traitement
# # # # # GLOBAL_PROCESSED_DATA_DF = None
# # # # # GLOBAL_DF_GPS_INFO = None # Variable globale pour les données GPS

# # # # # # Chemin vers le fichier JSON des coordonnées de station
# # # # # STATION_COORDS_PATH = os.path.join('data', 'station_coordinates.json')

# # # # # # Initialisation de GLOBAL_DF_GPS_INFO au démarrage de l'application
# # # # # # Assurez-vous que le dossier 'data' existe
# # # # # data_dir_for_gps = 'data'
# # # # # os.makedirs(data_dir_for_gps, exist_ok=True)

# # # # # try:
# # # # #     if not os.path.exists(STATION_COORDS_PATH) or os.stat(STATION_COORDS_PATH).st_size == 0:
# # # # #         print(f"DEBUG: Le fichier '{STATION_COORDS_PATH}' n'existe pas ou est vide. Exécution de data_processing._load_and_prepare_gps_data()...")
# # # # #         GLOBAL_DF_GPS_INFO = _load_and_prepare_gps_data()
# # # # #         if not GLOBAL_DF_GPS_INFO.empty:
# # # # #             GLOBAL_DF_GPS_INFO.to_json(STATION_COORDS_PATH, orient='records', indent=4)
# # # # #             print(f"DEBUG: Données GPS des stations sauvegardées dans '{STATION_COORDS_PATH}'.")
# # # # #         else:
# # # # #             print("DEBUG: Avertissement: Les données GPS sont vides après la préparation, le fichier JSON ne sera pas créé ou sera vide.")
# # # # #     else:
# # # # #         GLOBAL_DF_GPS_INFO = pd.read_json(STATION_COORDS_PATH, orient='records')
# # # # #         print(f"DEBUG: Données GPS des stations chargées depuis '{STATION_COORDS_PATH}'.")
    
# # # # #     if GLOBAL_DF_GPS_INFO.empty:
# # # # #         raise ValueError("Le DataFrame des données GPS est vide après le chargement/la préparation.")
    
# # # # #     print("DEBUG: Préparation des données de coordonnées des stations terminée.")
# # # # # except Exception as e:
# # # # #     print(f"DEBUG: Erreur fatale lors du chargement ou de la préparation des données GPS des stations au démarrage: {e}. "
# # # # #           "L'application pourrait ne pas fonctionner correctement sans ces données.")
# # # # #     GLOBAL_DF_GPS_INFO = pd.DataFrame({
# # # # #         'Station': [],
# # # # #         'Lat': [],
# # # # #         'Long': [],
# # # # #         'Timezone': []
# # # # #     })
# # # # #     traceback.print_exc()


# # # # # @app.route('/')
# # # # # def index():
# # # # #     """
# # # # #     Route principale affichant le formulaire d'upload de fichiers.
# # # # #     """
# # # # #     # Utiliser STATIONS_BY_BASSIN importé de config.py
# # # # #     return render_template('index.html', stations_by_bassin=STATIONS_BY_BASSIN)


# # # # # @app.route('/upload', methods=['GET', 'POST'])
# # # # # def upload_file():
# # # # #     """
# # # # #     Gère l'upload et le traitement des fichiers CSV/Excel.
# # # # #     Permet l'upload de multiples fichiers, chacun associé à un bassin et une station,
# # # # #     en vérifiant leur compatibilité et en les ajoutant au DataFrame global.
# # # # #     Si la méthode est GET, redirige vers la page d'accueil.
# # # # #     """
# # # # #     if request.method == 'GET':
# # # # #         return redirect(url_for('index'))

# # # # #     global GLOBAL_PROCESSED_DATA_DF

# # # # #     upload_groups = []
    
# # # # #     # Collecter tous les indices de groupe présents dans le formulaire
# # # # #     all_input_indices = set()
# # # # #     for key in request.form.keys():
# # # # #         match = re.search(r'_((\d+))$', key)
# # # # #         if match:
# # # # #             all_input_indices.add(int(match.group(1)))
# # # # #     for key in request.files.keys():
# # # # #         match = re.search(r'_(\d+)$', key)
# # # # #         if match:
# # # # #             all_input_indices.add(int(match.group(1)))
            
# # # # #     sorted_indices = sorted(list(all_input_indices))

# # # # #     if not sorted_indices:
# # # # #         # Aucun dataset n'a été soumis
# # # # #         print("DEBUG: Aucun groupe d'upload détecté.")
# # # # #         return redirect(url_for('index')) # Redirection pour rester sur la page d'upload

# # # # #     print(f"DEBUG (upload_file): Nombre de groupes d'upload détectés: {len(sorted_indices)}")
# # # # #     for index in sorted_indices:
# # # # #         bassin_name = request.form.get(f'bassin_{index}')
# # # # #         station_name = request.form.get(f'station_{index}')
# # # # #         file_obj = request.files.get(f'file_input_{index}') 

# # # # #         if not bassin_name or not station_name or not file_obj or file_obj.filename == '':
# # # # #             # Un groupe est incomplet, gérer l'erreur ou ignorer
# # # # #             print(f"DEBUG: Groupe d'upload {index} incomplet. Ignoré.")
# # # # #             continue # Ignorer ce groupe incomplet et passer au suivant

# # # # #         upload_groups.append({
# # # # #             'bassin': bassin_name,
# # # # #             'station': station_name,
# # # # #             'file': file_obj,
# # # # #             'index': index
# # # # #         })
    
# # # # #     if not upload_groups:
# # # # #         # Aucun dataset valide n'a été trouvé pour le traitement
# # # # #         print("DEBUG: Aucun groupe d'upload valide trouvé après filtrage.")
# # # # #         return redirect(url_for('index')) # Redirection si aucun groupe n'est valide


# # # # #     processed_dataframes_for_batch = []
    
# # # # #     expected_raw_data_columns_for_comparison = None
# # # # #     expected_raw_time_columns_for_comparison = None 

    
# # # # #     for group_info in upload_groups:
# # # # #         file = group_info['file']
# # # # #         bassin = group_info['bassin']
# # # # #         station = group_info['station']

# # # # #         file_extension = os.path.splitext(file.filename)[1].lower()
# # # # #         df_temp = None

# # # # #         try:
# # # # #             if file_extension == '.csv':
# # # # #                 df_temp = pd.read_csv(io.BytesIO(file.read()))
# # # # #             elif file_extension in ['.xls', '.xlsx']:
# # # # #                 df_temp = pd.read_excel(io.BytesIO(file.read()))
# # # # #             else:
# # # # #                 print(f"DEBUG: Type de fichier non supporté pour '{file.filename}'.")
# # # # #                 continue
            
# # # # #             if df_temp is not None:
# # # # #                 current_file_columns = df_temp.columns.tolist()
                
# # # # #                 current_raw_time_cols = []
# # # # #                 if 'Date' in current_file_columns:
# # # # #                     current_raw_time_cols.append('Date')
# # # # #                 if all(col in current_file_columns for col in ['Year', 'Month', 'Day', 'Hour', 'Minute']):
# # # # #                     current_raw_time_cols.extend(['Year', 'Month', 'Day', 'Hour', 'Minute'])
# # # # #                 current_raw_time_cols_sorted = sorted(list(set(current_raw_time_cols)))

# # # # #                 current_raw_data_cols_sorted = sorted([
# # # # #                     col for col in current_file_columns 
# # # # #                     if col != 'Station' and col not in ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
# # # # #                 ])

# # # # #                 if expected_raw_data_columns_for_comparison is None:
# # # # #                     expected_raw_data_columns_for_comparison = current_raw_data_cols_sorted
# # # # #                     expected_raw_time_columns_for_comparison = current_raw_time_cols_sorted
# # # # #                 else:
# # # # #                     if current_raw_data_cols_sorted != expected_raw_data_columns_for_comparison:
# # # # #                         print(f"DEBUG: Mismatch de colonnes de données pour '{file.filename}'. Attendu: {expected_raw_data_columns_for_comparison}, Trouvé: {current_raw_data_cols_sorted}")
# # # # #                         return redirect(url_for('index'))
                    
# # # # #                     if current_raw_time_cols_sorted != expected_raw_time_columns_for_comparison:
# # # # #                         print(f"DEBUG: Mismatch de colonnes temporelles pour '{file.filename}'. Attendu: {expected_raw_time_columns_for_comparison}, Trouvé: {current_raw_time_cols_sorted}")
# # # # #                         return redirect(url_for('index'))
                
# # # # #                 df_temp['Station'] = station
# # # # #                 print(f"DEBUG (upload_file): Fichier '{file.filename}' - Station assignée: '{station}'. Premières 5 lignes du DF temporaire:\n{df_temp.head(5)}")
# # # # #                 processed_dataframes_for_batch.append(df_temp)

# # # # #         except Exception as e:
# # # # #             print(f"DEBUG (upload_file): Erreur lors de la lecture ou du traitement du fichier '{file.filename}': {e}")
# # # # #             traceback.print_exc()
# # # # #             return redirect(url_for('index'))

# # # # #     if not processed_dataframes_for_batch:
# # # # #         print("DEBUG: La liste des DataFrames traités pour le batch est vide.")
# # # # #         return redirect(url_for('index'))

# # # # #     try:
# # # # #         # Concaténation de tous les DataFrames du batch
# # # # #         df_current_batch_raw_merged = pd.concat(processed_dataframes_for_batch, ignore_index=True)
# # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_raw_merged APRÈS CONCATÉNATION DU BATCH:\n{df_current_batch_raw_merged['Station'].unique().tolist()}")
        
# # # # #         # Étape 1: Créer la colonne Datetime AVANT la gestion des doublons
# # # # #         df_current_batch_with_datetime = create_datetime(df_current_batch_raw_merged, bassin=upload_groups[0]['bassin'])
# # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_with_datetime APRÈS CREATE_DATETIME DU BATCH:\n{df_current_batch_with_datetime['Station'].unique().tolist()}")

# # # # #         # Étape 2: Gérer les doublons maintenant que 'Datetime' est une colonne
# # # # #         df_current_batch_cleaned = gestion_doublons(df_current_batch_with_datetime)
# # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_cleaned APRÈS GESTION DOUBLONS DU BATCH:\n{df_current_batch_cleaned['Station'].unique().tolist()}")

# # # # #         if 'Datetime' not in df_current_batch_cleaned.columns or df_current_batch_cleaned['Datetime'].isnull().all():
# # # # #             print("DEBUG: La colonne 'Datetime' est manquante ou vide après le nettoyage.")
# # # # #             return redirect(url_for('index'))
        
# # # # #         # Assurez-vous que 'Datetime' est bien de type datetime et nettoyez les NaT
# # # # #         df_current_batch_cleaned['Datetime'] = pd.to_datetime(df_current_batch_cleaned['Datetime'], errors='coerce')
# # # # #         df_current_batch_cleaned.dropna(subset=['Datetime'], inplace=True) 

# # # # #         if df_current_batch_cleaned.empty:
# # # # #             print("DEBUG: Le DataFrame est vide après le nettoyage des dates invalides.")
# # # # #             return redirect(url_for('index'))

# # # # #         # Définir Datetime comme index
# # # # #         df_current_batch_processed = df_current_batch_cleaned.set_index('Datetime').sort_index()
# # # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_processed APRÈS SET_INDEX ET SORT_INDEX DU BATCH:\n{df_current_batch_processed['Station'].unique().tolist()}")


# # # # #         if GLOBAL_PROCESSED_DATA_DF is None:
# # # # #             GLOBAL_PROCESSED_DATA_DF = df_current_batch_processed
# # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF initialisé. Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
# # # # #         else:
# # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF EXISTANT AVANT FUSION: Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
            
# # # # #             new_batch_stations = df_current_batch_processed['Station'].unique().tolist()
# # # # #             print(f"DEBUG (upload_file): Stations dans le batch actuel à fusionner: {new_batch_stations}")

# # # # #             # Filtrer les données existantes pour les stations du lot actuel du DataFrame global
# # # # #             df_global_filtered = GLOBAL_PROCESSED_DATA_DF[~GLOBAL_PROCESSED_DATA_DF['Station'].isin(new_batch_stations)].copy()
# # # # #             print(f"DEBUG (upload_file): Stations dans GLOBAL_PROCESSED_DATA_DF après filtrage des stations du batch actuel:\n{df_global_filtered['Station'].unique().tolist() if not df_global_filtered.empty else 'DataFrame global vide après filtrage'}")

# # # # #             # Concaténer le DataFrame global filtré avec les données du nouveau lot
# # # # #             # Assurez-vous que les deux DataFrames ont des index compatibles (DatetimeIndex)
# # # # #             combined_df = pd.concat([df_global_filtered, df_current_batch_processed], ignore_index=False) # Important: ignore_index=False pour préserver les DatetimeIndex et les aligner
# # # # #             print(f"DEBUG (upload_file): Stations uniques dans combined_df APRÈS CONCATINATION GLOBAL_FILTRÉ+BATCH:\n{combined_df['Station'].unique().tolist()}")

# # # # #             # Pas besoin de to_datetime/dropna/set_index ici si les index sont déjà corrects.
# # # # #             # Cependant, on peut re-trier par sécurité.
# # # # #             GLOBAL_PROCESSED_DATA_DF = combined_df.sort_index()

# # # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF mis à jour. Stations uniques finales: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

# # # # #         # --- VÉRIFICATION FINALE ET DÉFENSIVE DE L'INDEX ET DE LA COLONNE 'STATION' DE GLOBAL_PROCESSED_DATA_DF AVANT TOUT APPEL EXTERNE ---
# # # # #         print(f"DEBUG_STATION_NAMES (upload_file): Type de l'index de GLOBAL_PROCESSED_DATA_DF avant vérification finale: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# # # # #         if 'Station' not in GLOBAL_PROCESSED_DATA_DF.columns:
# # # # #             print("DEBUG_STATION_NAMES (upload_file): ERREUR: La colonne 'Station' est manquante dans GLOBAL_PROCESSED_DATA_DF après traitement.")
# # # # #             return redirect(url_for('index')) # Redirection car données invalides

# # # # #         try:
# # # # #             if not isinstance(GLOBAL_PROCESSED_DATA_DF.index, pd.DatetimeIndex):
# # # # #                 if 'Datetime' in GLOBAL_PROCESSED_DATA_DF.columns:
# # # # #                     GLOBAL_PROCESSED_DATA_DF['Datetime'] = pd.to_datetime(GLOBAL_PROCESSED_DATA_DF['Datetime'], errors='coerce')
# # # # #                     GLOBAL_PROCESSED_DATA_DF.dropna(subset=['Datetime'], inplace=True)
# # # # #                     GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.set_index('Datetime').sort_index()
# # # # #                 else:
# # # # #                     raise TypeError("L'index n'est pas DatetimeIndex et aucune colonne 'Datetime' n'est disponible pour la recréation.")
            
# # # # #             GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.sort_index()

# # # # #             print(f"DEBUG_STATION_NAMES (upload_file): Index de GLOBAL_PROCESSED_DATA_DF VALIDÉ. Stations uniques finales après vérification finale: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

# # # # #         except Exception as e:
# # # # #             print(f"DEBUG_STATION_NAMES (upload_file): Erreur critique finale: Impossible de garantir un DatetimeIndex pour l'interpolation. Détails: {e}")
# # # # #             traceback.print_exc()
# # # # #             return redirect(url_for('index'))
        
# # # # #         if GLOBAL_PROCESSED_DATA_DF.empty:
# # # # #             print("DEBUG_STATION_NAMES (upload_file): GLOBAL_PROCESSED_DATA_DF est vide après la vérification finale.")
# # # # #             return redirect(url_for('index'))


# # # # #         print(f"DEBUG (upload_file): Type de l'index de GLOBAL_PROCESSED_DATA_DF AVANT interpolation: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# # # # #         print(f"DEBUG (upload_file): Dtypes de GLOBAL_PROCESSED_DATA_DF (après vérif index): \n{GLOBAL_PROCESSED_DATA_DF.dtypes}")


# # # # #         # Traitement d'interpolation et des outliers sur le DataFrame global unifié
# # # # #         # Ces fonctions attendent un DatetimeIndex
# # # # #         # S'assurer que GLOBAL_DF_GPS_INFO est valide avant de le passer
# # # # #         if GLOBAL_DF_GPS_INFO is None or GLOBAL_DF_GPS_INFO.empty:
# # # # #             print("DEBUG: GLOBAL_DF_GPS_INFO est vide ou None. Tente de le recharger/préparer.")
# # # # #             try:
# # # # #                 # Tente de recharger ou de générer les données GPS ici si elles sont manquantes
# # # # #                 # Cela peut arriver si le chargement initial a échoué.
# # # # #                 GLOBAL_DF_GPS_INFO = _load_and_prepare_gps_data()
# # # # #                 if GLOBAL_DF_GPS_INFO.empty:
# # # # #                     raise ValueError("Re-chargement des données GPS échoué, DataFrame vide.")
# # # # #             except Exception as e:
# # # # #                 print(f"DEBUG: Erreur lors du rechargement des données GPS durant l'upload: {e}")
# # # # #                 traceback.print_exc()
# # # # #                 # Créer un DataFrame GPS vide pour éviter les erreurs plus tard
# # # # #                 GLOBAL_DF_GPS_INFO = pd.DataFrame({'Station': [], 'Lat': [], 'Long': [], 'Timezone': []})


# # # # #         GLOBAL_PROCESSED_DATA_DF = interpolation(GLOBAL_PROCESSED_DATA_DF, DATA_LIMITS, GLOBAL_DF_GPS_INFO)
# # # # #         GLOBAL_PROCESSED_DATA_DF = traiter_outliers_meteo(GLOBAL_PROCESSED_DATA_DF, DATA_LIMITS)
        
# # # # #         # Nettoyage des colonnes temporaires
# # # # #         cols_to_drop_after_process = [
# # # # #             'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date',
# # # # #             'sunrise_time_local', 'sunset_time_local'
# # # # #         ]
# # # # #         GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.drop(columns=cols_to_drop_after_process, errors='ignore')

# # # # #         print(f"DEBUG_STATION_NAMES (upload_file - FIN): GLOBAL_PROCESSED_DATA_DF FINAL après interpolation et outliers. Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
        
# # # # #         # Pour cet exemple, nous allons juste rediriger vers la page d'options
# # # # #         return redirect(url_for('show_visualizations_options'))

# # # # #     except Exception as e:
# # # # #         print(f"DEBUG: Une erreur inattendue s'est produite lors du traitement des données: {e}")
# # # # #         traceback.print_exc() # Ajout du traceback en cas d'erreur inattendue
# # # # #         return redirect(url_for('index'))

# # # # # @app.route('/visualizations_options')
# # # # # def show_visualizations_options():
# # # # #     """
# # # # #     Affiche la page des options de visualisation après le traitement des données.
# # # # #     """
# # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # #         print("DEBUG_STATION_NAMES (show_visualizations_options): GLOBAL_PROCESSED_DATA_DF est None. Redirection vers l'index.")
# # # # #         return redirect(url_for('index'))

# # # # #     # DEBUG: Vérifier les stations avant de les passer au template
# # # # #     # Assurez-vous que la colonne 'Station' existe avant d'essayer d'y accéder
# # # # #     if 'Station' not in GLOBAL_PROCESSED_DATA_DF.columns:
# # # # #         print("DEBUG_STATION_NAMES (show_visualizations_options): ERREUR: La colonne 'Station' est manquante dans GLOBAL_PROCESSED_DATA_DF lors de l l'affichage des options.")
# # # # #         return redirect(url_for('index')) # Ou afficher une erreur appropriée
    
# # # # #     stations_in_df = GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()
# # # # #     print(f"DEBUG_STATION_NAMES (show_visualizations_options): Stations détectées dans GLOBAL_PROCESSED_DATA_DF (pour le template): {stations_in_df}")
    
# # # # #     # Appel de la fonction de calcul des statistiques journalières pour le tableau HTML
# # # # #     daily_stats_df = calculate_daily_summary_table(GLOBAL_PROCESSED_DATA_DF)
    
# # # # #     print(f"DEBUG (app.py/visualizations_options): Stations présentes dans daily_stats_df: {daily_stats_df['Station'].unique().tolist() if not daily_stats_df.empty else 'DataFrame vide'}")

# # # # #     initial_data_html = None
# # # # #     data_summary = f"Lignes: {len(GLOBAL_PROCESSED_DATA_DF)}, Colonnes: {len(GLOBAL_PROCESSED_DATA_DF.columns)}"
    
# # # # #     unique_stations_count = len(stations_in_df) # Utiliser la liste déjà extraite

# # # # #     if unique_stations_count > 1:
# # # # #         top_n_data = GLOBAL_PROCESSED_DATA_DF.groupby('Station').head(10)
# # # # #         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
# # # # #     else:
# # # # #         top_n_data = GLOBAL_PROCESSED_DATA_DF.head(20)
# # # # #         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
    
# # # # #     if initial_data_html is None:
# # # # #         initial_data_html = "<p class='text-center text-red-500'>Aucune donnée à afficher pour la prévisualisation.</p>"


# # # # #     daily_stats_html = daily_stats_df.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=False, border=0) if not daily_stats_df.empty else None

# # # # #     # Récupérer les informations pour les menus déroulants dynamiquement
# # # # #     stations = sorted(stations_in_df) # Utiliser la variable déjà vérifiée
# # # # #     variables = sorted([col for col in GLOBAL_PROCESSED_DATA_DF.columns if col in METADATA_VARIABLES.keys()])
# # # # #     can_compare_stations = len(stations) >= 2
    
# # # # #     daily_stat_variables_available = [
# # # # #         var for var in METADATA_VARIABLES.keys() if var in GLOBAL_PROCESSED_DATA_DF.columns and
# # # # #         pd.api.types.is_numeric_dtype(GLOBAL_PROCESSED_DATA_DF[var])
# # # # #     ]
# # # # #     daily_stat_variables = sorted(daily_stat_variables_available)


# # # # #     periodes = ['Brute', 'Journalière', 'Hebdomadaire', 'Mensuelle', 'Annuelle']

# # # # #     return render_template(
# # # # #         'visualizations_options.html',
# # # # #         initial_data_html=initial_data_html,
# # # # #         data_summary=data_summary,
# # # # #         daily_stats_html=daily_stats_html,
# # # # #         stations=stations,
# # # # #         variables=variables,
# # # # #         periodes=periodes,
# # # # #         can_compare_stations=can_compare_stations,
# # # # #         METADATA_VARIABLES=METADATA_VARIABLES,
# # # # #         daily_stat_variables=daily_stat_variables # Passer les variables pour les stats journalières
# # # # #     )

# # # # # @app.route('/generate_single_plot', methods=['GET', 'POST'])
# # # # # def generate_single_plot():
# # # # #     """
# # # # #     Génère et affiche un graphique pour une seule variable et une seule station.
# # # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # # #     """
# # # # #     if request.method == 'GET':
# # # # #         return redirect(url_for('show_visualizations_options'))

# # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # #         return redirect(url_for('index'))

# # # # #     station_select = request.form.get('station_select_single')
# # # # #     variable_select = request.form.get('variable_select_single')
# # # # #     periode_select = request.form.get('periode_select_single')

# # # # #     if not station_select or not variable_select or not periode_select:
# # # # #         return redirect(url_for('show_visualizations_options'))

# # # # #     try:
# # # # #         fig = generer_graphique_par_variable_et_periode(
# # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # #             station_select,
# # # # #             variable_select,
# # # # #             periode_select,
# # # # #             PALETTE_DEFAUT, # Utilisez PALETTE_DEFAUT pour les couleurs
# # # # #             METADATA_VARIABLES
# # # # #         )

# # # # #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# # # # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # # # #             meta = METADATA_VARIABLES.get(variable_select, {})
# # # # #             plot_title = f"Évolution de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) pour la station {station_select} ({periode_select})"
# # # # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # # # #         else:
# # # # #             return redirect(url_for('show_visualizations_options'))
# # # # #     except TypeError as e:
# # # # #         print(f"DEBUG: Erreur de type dans generate_single_plot: {e}")
# # # # #         traceback.print_exc()
# # # # #         return redirect(url_for('show_visualizations_options'))
# # # # #     except Exception as e:
# # # # #         print(f"DEBUG: Erreur lors de la génération du graphique: {e}")
# # # # #         traceback.print_exc()
# # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # @app.route('/generate_comparative_plot', methods=['GET', 'POST'])
# # # # # def generate_comparative_plot():
# # # # #     """
# # # # #     Génère et affiche un graphique comparatif pour une variable à travers toutes les stations.
# # # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # # #     """
# # # # #     if request.method == 'GET':
# # # # #         return redirect(url_for('show_visualizations_options'))

# # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # #         return redirect(url_for('index'))

# # # # #     variable_select = request.form.get('variable_select_comparative')
# # # # #     periode_select = request.form.get('periode_select_comparative')

# # # # #     if not variable_select or not periode_select:
# # # # #         return redirect(url_for('show_visualizations_options'))

# # # # #     try:
# # # # #         fig = generer_graphique_comparatif(
# # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # #             variable_select,
# # # # #             periode_select,
# # # # #             PALETTE_DEFAUT, # Utilisez PALETTE_DEFAUT pour les couleurs
# # # # #             METADATA_VARIABLES
# # # # #         )

# # # # #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# # # # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # # # #             meta = METADATA_VARIABLES.get(variable_select, {})
# # # # #             plot_title = f"Comparaison de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) entre stations ({periode_select})"
# # # # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # # # #         else:
# # # # #             return redirect(url_for('show_visualizations_options'))
# # # # #     except TypeError as e:
# # # # #         print(f"DEBUG: Erreur de type dans generate_comparative_plot: {e}")
# # # # #         traceback.print_exc()
# # # # #         return redirect(url_for('show_visualizations_options'))
# # # # #     except Exception as e:
# # # # #         print(f"DEBUG: Erreur lors de la génération du graphique comparatif: {e}")
# # # # #         traceback.print_exc()
# # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # @app.route('/generate_multi_variable_plot', methods=['GET', 'POST'])
# # # # # def generate_multi_variable_plot_route():
# # # # #     """
# # # # #     Génère et affiche un graphique d'évolution multi-variables normalisé pour une station.
# # # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # # #     """
# # # # #     if request.method == 'GET':
# # # # #         return redirect(url_for('show_visualizations_options'))

# # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # #         return redirect(url_for('index'))

# # # # #     station_select = request.form.get('station_select_multi_var')

# # # # #     if not station_select:
# # # # #         return redirect(url_for('show_visualizations_options'))

# # # # #     try:
# # # # #         fig = generate_multi_variable_station_plot(
# # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # #             station_select,
# # # # #             PALETTE_DEFAUT, # Utilisez PALETTE_DEFAUT pour les couleurs
# # # # #             METADATA_VARIABLES
# # # # #         )

# # # # #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# # # # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # # # #             plot_title = f"Évolution Normalisée des Variables Météorologiques pour la station {station_select}"
# # # # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # # # #         else:
# # # # #             return redirect(url_for('show_visualizations_options'))
# # # # #     except TypeError as e:
# # # # #         print(f"DEBUG: Erreur de type dans generate_multi_variable_plot_route: {e}")
# # # # #         traceback.print_exc()
# # # # #         return redirect(url_for('show_visualizations_options'))
# # # # #     except Exception as e:
# # # # #         print(f"DEBUG: Erreur lors de la génération du graphique multi-variables: {e}")
# # # # #         traceback.print_exc()
# # # # #         return redirect(url_for('show_visualizations_options'))

# # # # # @app.route('/get_daily_stats_summary_table', methods=['GET'])
# # # # # def get_daily_stats_summary_table():
# # # # #     """
# # # # #     Route AJAX pour obtenir les statistiques journalières récapitulatives.
# # # # #     """
# # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # #         return jsonify({'error': 'Aucune donnée traitée pour les statistiques journalières.'}), 404
    
# # # # #     try:
# # # # #         summary_table = calculate_daily_summary_table(GLOBAL_PROCESSED_DATA_DF)
# # # # #         # Convertir les Timestamps en chaînes pour la sérialisation JSON
# # # # #         for col in summary_table.columns:
# # # # #             if pd.api.types.is_datetime64_any_dtype(summary_table[col]):
# # # # #                 summary_table[col] = summary_table[col].dt.strftime('%Y-%m-%d %H:%M:%S')
# # # # #             elif summary_table[col].dtype == 'object' and summary_table[col].apply(lambda x: isinstance(x, (datetime, pd.Timestamp))).any():
# # # # #                  summary_table[col] = summary_table[col].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, (datetime, pd.Timestamp)) else x)


# # # # #         return jsonify(summary_table.to_dict(orient='records'))
# # # # #     except Exception as e:
# # # # #         print(f"Erreur lors de la génération du tableau de statistiques journalières: {e}")
# # # # #         traceback.print_exc()
# # # # #         return jsonify({'error': f"Erreur lors de la génération du tableau de statistiques journalières: {str(e)}"}), 500


# # # # # @app.route('/get_daily_stats_plot_json/<station_name>/<path:variable_name>', methods=['GET'])
# # # # # def get_daily_stats_plot_json(station_name, variable_name):
# # # # #     """
# # # # #     Route AJAX pour obtenir le JSON Plotly d'un graphique de statistiques journalières
# # # # #     pour une station et une variable données.
# # # # #     """
# # # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # # #         return jsonify({'error': 'Aucune donnée disponible pour les statistiques journalières.'}), 404

# # # # #     try:
# # # # #         # Appel de la fonction de traitement de données pour générer la figure Matplotlib
# # # # #         fig = generate_daily_summary_plot_for_variable(
# # # # #             GLOBAL_PROCESSED_DATA_DF,
# # # # #             station_name,
# # # # #             variable_name,
# # # # #             METADATA_VARIABLES,
# # # # #             PALETTE_DEFAUT
# # # # #         )
        
# # # # #         if fig:
# # # # #             # Sauvegarder la figure dans un buffer BytesIO
# # # # #             img_buffer = io.BytesIO()
# # # # #             fig.savefig(img_buffer, format='png', bbox_inches='tight')
# # # # #             img_buffer.seek(0)
            
# # # # #             # Encoder l'image en base64
# # # # #             encoded_image = base64.b64encode(img_buffer.read()).decode('utf-8')
            
# # # # #             # Fermer la figure pour libérer la mémoire
# # # # #             plt.close(fig)

# # # # #             return jsonify({'image_data': encoded_image, 'mime_type': 'image/png'})
# # # # #         else:
# # # # #             return jsonify({'error': 'Graphique de statistiques journalières vide pour cette sélection.'}), 404

# # # # #     except Exception as e:
# # # # #         print(f"Erreur lors de la génération du graphique de stats journalières (API): {e}")
# # # # #         traceback.print_exc()
# # # # #         return jsonify({'error': f'Erreur lors de la génération du graphique de stats journalières: {e}'}), 500


# # # # # @app.route('/reset_data', methods=['GET', 'POST'])
# # # # # def reset_data():
# # # # #     """
# # # # #     Réinitialise les données globales traitées et redirige vers la page d'accueil.
# # # # #     Si la méthode est GET, redirige simplement vers la page d'accueil.
# # # # #     """
# # # # #     if request.method == 'GET':
# # # # #         return redirect(url_for('index'))

# # # # #     global GLOBAL_PROCESSED_DATA_DF
# # # # #     GLOBAL_PROCESSED_DATA_DF = None
# # # # #     # Vous devriez également gérer la réinitialisation des variables de session si elles sont utilisées
# # # # #     # par exemple, session.pop('available_stations', None)
# # # # #     return redirect(url_for('index'))


# # # # # if __name__ == '__main__':
# # # # #     app.run(debug=True)

# # # # import pandas as pd
# # # # from flask import Flask, render_template, request, jsonify, redirect, url_for
# # # # from werkzeug.utils import secure_filename
# # # # import os
# # # # import io
# # # # import base64
# # # # from datetime import datetime
# # # # import json
# # # # import plotly.graph_objects as go
# # # # import matplotlib.pyplot as plt # Importation de Matplotlib
# # # # import seaborn as sns # Importation de Seaborn
# # # # import traceback # Importation de traceback pour les messages d'erreur détaillés
# # # # import re # Importation du module re pour les expressions régulières

# # # # # Importation des fonctions de traitement de données
# # # # from data_processing import (
# # # #     create_datetime, interpolation, _load_and_prepare_gps_data,
# # # #     gestion_doublons, traiter_outliers_meteo, create_rain_mm,
# # # #     generer_graphique_par_variable_et_periode, generer_graphique_comparatif,
# # # #     generate_multi_variable_station_plot, calculate_daily_summary_table,
# # # #     generate_daily_summary_plot_for_variable
# # # # )

# # # # # Importation des configurations globales
# # # # from config import METADATA_VARIABLES, PALETTE_DEFAUT, DATA_LIMITS, STATIONS_BY_BASSIN

# # # # app = Flask(__name__)
# # # # app.config['UPLOAD_FOLDER'] = 'uploads'
# # # # app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 # 16 MB max-upload size
# # # # os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# # # # # Variable globale pour stocker les données du DataFrame après traitement
# # # # GLOBAL_PROCESSED_DATA_DF = None
# # # # GLOBAL_DF_GPS_INFO = None # Variable globale pour les données GPS

# # # # # Chemin vers le fichier JSON des coordonnées de station
# # # # STATION_COORDS_PATH = os.path.join('data', 'station_coordinates.json')

# # # # # Initialisation de GLOBAL_DF_GPS_INFO au démarrage de l'application
# # # # # Assurez-vous que le dossier 'data' existe
# # # # data_dir_for_gps = 'data'
# # # # os.makedirs(data_dir_for_gps, exist_ok=True)

# # # # try:
# # # #     if not os.path.exists(STATION_COORDS_PATH) or os.stat(STATION_COORDS_PATH).st_size == 0:
# # # #         print(f"DEBUG: Le fichier '{STATION_COORDS_PATH}' n'existe pas ou est vide. Exécution de data_processing._load_and_prepare_gps_data()...")
# # # #         GLOBAL_DF_GPS_INFO = _load_and_prepare_gps_data()
# # # #         if not GLOBAL_DF_GPS_INFO.empty:
# # # #             GLOBAL_DF_GPS_INFO.to_json(STATION_COORDS_PATH, orient='records', indent=4)
# # # #             print(f"DEBUG: Données GPS des stations sauvegardées dans '{STATION_COORDS_PATH}'.")
# # # #         else:
# # # #             print("DEBUG: Avertissement: Les données GPS sont vides après la préparation, le fichier JSON ne sera pas créé ou sera vide.")
# # # #     else:
# # # #         GLOBAL_DF_GPS_INFO = pd.read_json(STATION_COORDS_PATH, orient='records')
# # # #         print(f"DEBUG: Données GPS des stations chargées depuis '{STATION_COORDS_PATH}'.")
    
# # # #     if GLOBAL_DF_GPS_INFO.empty:
# # # #         raise ValueError("Le DataFrame des données GPS est vide après le chargement/la préparation.")
    
# # # #     print("DEBUG: Préparation des données de coordonnées des stations terminée.")
# # # # except Exception as e:
# # # #     print(f"DEBUG: Erreur fatale lors du chargement ou de la préparation des données GPS des stations au démarrage: {e}. "
# # # #           "L'application pourrait ne pas fonctionner correctement sans ces données.")
# # # #     GLOBAL_DF_GPS_INFO = pd.DataFrame({
# # # #         'Station': [],
# # # #         'Lat': [],
# # # #         'Long': [],
# # # #         'Timezone': []
# # # #     })
# # # #     traceback.print_exc()


# # # # @app.route('/')
# # # # def index():
# # # #     """
# # # #     Route principale affichant le formulaire d'upload de fichiers.
# # # #     """
# # # #     # Utiliser STATIONS_BY_BASSIN importé de config.py
# # # #     return render_template('index.html', stations_by_bassin=STATIONS_BY_BASSIN)


# # # # @app.route('/upload', methods=['GET', 'POST'])
# # # # def upload_file():
# # # #     """
# # # #     Gère l'upload et le traitement des fichiers CSV/Excel.
# # # #     Permet l'upload de multiples fichiers, chacun associé à un bassin et une station,
# # # #     en vérifiant leur compatibilité et en les ajoutant au DataFrame global.
# # # #     Si la méthode est GET, redirige vers la page d'accueil.
# # # #     """
# # # #     if request.method == 'GET':
# # # #         return redirect(url_for('index'))

# # # #     global GLOBAL_PROCESSED_DATA_DF
# # # #     global GLOBAL_DF_GPS_INFO # Déclaration pour utiliser la variable globale

# # # #     upload_groups = []
    
# # # #     # Collecter tous les indices de groupe présents dans le formulaire
# # # #     all_input_indices = set()
# # # #     for key in request.form.keys():
# # # #         match = re.search(r'_((\d+))$', key)
# # # #         if match:
# # # #             all_input_indices.add(int(match.group(1)))
# # # #     for key in request.files.keys():
# # # #         match = re.search(r'_(\d+)$', key)
# # # #         if match:
# # # #             all_input_indices.add(int(match.group(1)))
            
# # # #     sorted_indices = sorted(list(all_input_indices))

# # # #     if not sorted_indices:
# # # #         # Aucun dataset n'a été soumis
# # # #         print("DEBUG: Aucun groupe d'upload détecté.")
# # # #         return redirect(url_for('index')) # Redirection pour rester sur la page d'upload

# # # #     print(f"DEBUG (upload_file): Nombre de groupes d'upload détectés: {len(sorted_indices)}")
# # # #     for index in sorted_indices:
# # # #         bassin_name = request.form.get(f'bassin_{index}')
# # # #         station_name = request.form.get(f'station_{index}')
# # # #         file_obj = request.files.get(f'file_input_{index}') 

# # # #         if not bassin_name or not station_name or not file_obj or file_obj.filename == '':
# # # #             # Un groupe est incomplet, gérer l'erreur ou ignorer
# # # #             print(f"DEBUG: Groupe d'upload {index} incomplet. Ignoré.")
# # # #             continue # Ignorer ce groupe incomplet et passer au suivant

# # # #         upload_groups.append({
# # # #             'bassin': bassin_name,
# # # #             'station': station_name,
# # # #             'file': file_obj,
# # # #             'index': index
# # # #         })
    
# # # #     if not upload_groups:
# # # #         # Aucun dataset valide n'a été trouvé pour le traitement
# # # #         print("DEBUG: Aucun groupe d'upload valide trouvé après filtrage.")
# # # #         return redirect(url_for('index')) # Redirection si aucun groupe n'est valide


# # # #     processed_dataframes_for_batch = []
    
# # # #     expected_raw_data_columns_for_comparison = None
# # # #     expected_raw_time_columns_for_comparison = None 

    
# # # #     for group_info in upload_groups:
# # # #         file = group_info['file']
# # # #         bassin = group_info['bassin']
# # # #         station = group_info['station']

# # # #         file_extension = os.path.splitext(file.filename)[1].lower()
# # # #         df_temp = None

# # # #         try:
# # # #             if file_extension == '.csv':
# # # #                 df_temp = pd.read_csv(io.BytesIO(file.read()))
# # # #             elif file_extension in ['.xls', '.xlsx']:
# # # #                 df_temp = pd.read_excel(io.BytesIO(file.read()))
# # # #             else:
# # # #                 print(f"DEBUG: Type de fichier non supporté pour '{file.filename}'.")
# # # #                 continue
            
# # # #             if df_temp is not None:
# # # #                 current_file_columns = df_temp.columns.tolist()
                
# # # #                 current_raw_time_cols = []
# # # #                 if 'Date' in current_file_columns:
# # # #                     current_raw_time_cols.append('Date')
# # # #                 if all(col in current_file_columns for col in ['Year', 'Month', 'Day', 'Hour', 'Minute']):
# # # #                     current_raw_time_cols.extend(['Year', 'Month', 'Day', 'Hour', 'Minute'])
# # # #                 current_raw_time_cols_sorted = sorted(list(set(current_raw_time_cols)))

# # # #                 current_raw_data_cols_sorted = sorted([
# # # #                     col for col in current_file_columns 
# # # #                     if col != 'Station' and col not in ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
# # # #                 ])

# # # #                 if expected_raw_data_columns_for_comparison is None:
# # # #                     expected_raw_data_columns_for_comparison = current_raw_data_cols_sorted
# # # #                     expected_raw_time_columns_for_comparison = current_raw_time_cols_sorted
# # # #                 else:
# # # #                     if current_raw_data_cols_sorted != expected_raw_data_columns_for_comparison:
# # # #                         print(f"DEBUG: Mismatch de colonnes de données pour '{file.filename}'. Attendu: {expected_raw_data_columns_for_comparison}, Trouvé: {current_raw_data_cols_sorted}")
# # # #                         return redirect(url_for('index'))
                    
# # # #                     if current_raw_time_cols_sorted != expected_raw_time_columns_for_comparison:
# # # #                         print(f"DEBUG: Mismatch de colonnes temporelles pour '{file.filename}'. Attendu: {expected_raw_time_columns_for_comparison}, Trouvé: {current_raw_time_cols_sorted}")
# # # #                         return redirect(url_for('index'))
                
# # # #                 df_temp['Station'] = station
# # # #                 print(f"DEBUG (upload_file): Fichier '{file.filename}' - Station assignée: '{station}'. Premières 5 lignes du DF temporaire:\n{df_temp.head(5)}")
# # # #                 processed_dataframes_for_batch.append(df_temp)

# # # #         except Exception as e:
# # # #             print(f"DEBUG (upload_file): Erreur lors de la lecture ou du traitement du fichier '{file.filename}': {e}")
# # # #             traceback.print_exc()
# # # #             return redirect(url_for('index'))

# # # #     if not processed_dataframes_for_batch:
# # # #         print("DEBUG: La liste des DataFrames traités pour le batch est vide.")
# # # #         return redirect(url_for('index'))

# # # #     try:
# # # #         # Concaténation de tous les DataFrames du batch
# # # #         df_current_batch_raw_merged = pd.concat(processed_dataframes_for_batch, ignore_index=True)
# # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_raw_merged APRÈS CONCATÉNATION DU BATCH:\n{df_current_batch_raw_merged['Station'].unique().tolist()}")
        
# # # #         # Étape 1: Créer la colonne Datetime AVANT la gestion des doublons
# # # #         df_current_batch_with_datetime = create_datetime(df_current_batch_raw_merged, bassin=upload_groups[0]['bassin'])
# # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_with_datetime APRÈS CREATE_DATETIME DU BATCH:\n{df_current_batch_with_datetime['Station'].unique().tolist()}")

# # # #         # Étape 2: Gérer les doublons maintenant que 'Datetime' est une colonne
# # # #         df_current_batch_cleaned = gestion_doublons(df_current_batch_with_datetime)
# # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_cleaned APRÈS GESTION DOUBLONS DU BATCH:\n{df_current_batch_cleaned['Station'].unique().tolist()}")

# # # #         if 'Datetime' not in df_current_batch_cleaned.columns or df_current_batch_cleaned['Datetime'].isnull().all():
# # # #             print("DEBUG: La colonne 'Datetime' est manquante ou vide après le nettoyage.")
# # # #             return redirect(url_for('index'))
        
# # # #         # Assurez-vous que 'Datetime' est bien de type datetime et nettoyez les NaT
# # # #         df_current_batch_cleaned['Datetime'] = pd.to_datetime(df_current_batch_cleaned['Datetime'], errors='coerce')
# # # #         df_current_batch_cleaned.dropna(subset=['Datetime'], inplace=True) 

# # # #         if df_current_batch_cleaned.empty:
# # # #             print("DEBUG: Le DataFrame est vide après le nettoyage des dates invalides.")
# # # #             return redirect(url_for('index'))

# # # #         # Définir Datetime comme index
# # # #         df_current_batch_processed = df_current_batch_cleaned.set_index('Datetime').sort_index()
# # # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_processed APRÈS SET_INDEX ET SORT_INDEX DU BATCH:\n{df_current_batch_processed['Station'].unique().tolist()}")


# # # #         if GLOBAL_PROCESSED_DATA_DF is None:
# # # #             GLOBAL_PROCESSED_DATA_DF = df_current_batch_processed
# # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF initialisé. Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
# # # #         else:
# # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF EXISTANT AVANT FUSION: Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
            
# # # #             new_batch_stations = df_current_batch_processed['Station'].unique().tolist()
# # # #             print(f"DEBUG (upload_file): Stations dans le batch actuel à fusionner: {new_batch_stations}")

# # # #             # Filtrer les données existantes pour les stations du lot actuel du DataFrame global
# # # #             df_global_filtered = GLOBAL_PROCESSED_DATA_DF[~GLOBAL_PROCESSED_DATA_DF['Station'].isin(new_batch_stations)].copy()
# # # #             print(f"DEBUG (upload_file): Stations dans GLOBAL_PROCESSED_DATA_DF après filtrage des stations du batch actuel:\n{df_global_filtered['Station'].unique().tolist() if not df_global_filtered.empty else 'DataFrame global vide après filtrage'}")

# # # #             # Concaténer le DataFrame global filtré avec les données du nouveau lot
# # # #             # Assurez-vous que les deux DataFrames ont des index compatibles (DatetimeIndex)
# # # #             combined_df = pd.concat([df_global_filtered, df_current_batch_processed], ignore_index=False) # Important: ignore_index=False pour préserver les DatetimeIndex et les aligner
# # # #             print(f"DEBUG (upload_file): Stations uniques dans combined_df APRÈS CONCATINATION GLOBAL_FILTRÉ+BATCH:\n{combined_df['Station'].unique().tolist()}")

# # # #             # Pas besoin de to_datetime/dropna/set_index ici si les index sont déjà corrects.
# # # #             # Cependant, on peut re-trier par sécurité.
# # # #             GLOBAL_PROCESSED_DATA_DF = combined_df.sort_index()

# # # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF mis à jour. Stations uniques finales: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

# # # #         # --- VÉRIFICATION FINALE ET DÉFENSIVE DE L'INDEX ET DE LA COLONNE 'STATION' DE GLOBAL_PROCESSED_DATA_DF AVANT TOUT APPEL EXTERNE ---
# # # #         print(f"DEBUG_STATION_NAMES (upload_file): Type de l'index de GLOBAL_PROCESSED_DATA_DF avant vérification finale: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# # # #         if 'Station' not in GLOBAL_PROCESSED_DATA_DF.columns:
# # # #             print("DEBUG_STATION_NAMES (upload_file): ERREUR: La colonne 'Station' est manquante dans GLOBAL_PROCESSED_DATA_DF après traitement.")
# # # #             return redirect(url_for('index')) # Redirection car données invalides

# # # #         try:
# # # #             if not isinstance(GLOBAL_PROCESSED_DATA_DF.index, pd.DatetimeIndex):
# # # #                 if 'Datetime' in GLOBAL_PROCESSED_DATA_DF.columns:
# # # #                     GLOBAL_PROCESSED_DATA_DF['Datetime'] = pd.to_datetime(GLOBAL_PROCESSED_DATA_DF['Datetime'], errors='coerce')
# # # #                     GLOBAL_PROCESSED_DATA_DF.dropna(subset=['Datetime'], inplace=True)
# # # #                     GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.set_index('Datetime').sort_index()
# # # #                 else:
# # # #                     raise TypeError("L'index n'est pas DatetimeIndex et aucune colonne 'Datetime' n'est disponible pour la recréation.")
            
# # # #             GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.sort_index()

# # # #             print(f"DEBUG_STATION_NAMES (upload_file): Index de GLOBAL_PROCESSED_DATA_DF VALIDÉ. Stations uniques finales après vérification finale: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

# # # #         except Exception as e:
# # # #             print(f"DEBUG_STATION_NAMES (upload_file): Erreur critique finale: Impossible de garantir un DatetimeIndex pour l'interpolation. Détails: {e}")
# # # #             traceback.print_exc()
# # # #             return redirect(url_for('index'))
        
# # # #         if GLOBAL_PROCESSED_DATA_DF.empty:
# # # #             print("DEBUG_STATION_NAMES (upload_file): GLOBAL_PROCESSED_DATA_DF est vide après la vérification finale.")
# # # #             return redirect(url_for('index'))


# # # #         print(f"DEBUG (upload_file): Type de l'index de GLOBAL_PROCESSED_DATA_DF AVANT interpolation: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# # # #         print(f"DEBUG (upload_file): Dtypes de GLOBAL_PROCESSED_DATA_DF (après vérif index): \n{GLOBAL_PROCESSED_DATA_DF.dtypes}")


# # # #         # Traitement d'interpolation et des outliers sur le DataFrame global unifié
# # # #         # Ces fonctions attendent un DatetimeIndex
# # # #         # S'assurer que GLOBAL_DF_GPS_INFO est valide avant de le passer
# # # #         if GLOBAL_DF_GPS_INFO is None or GLOBAL_DF_GPS_INFO.empty:
# # # #             print("DEBUG: GLOBAL_DF_GPS_INFO est vide ou None. Tente de le recharger/préparer.")
# # # #             try:
# # # #                 # Tente de recharger ou de générer les données GPS ici si elles sont manquantes
# # # #                 # Cela peut arriver si le chargement initial a échoué.
# # # #                 GLOBAL_DF_GPS_INFO = _load_and_prepare_gps_data()
# # # #                 if GLOBAL_DF_GPS_INFO.empty:
# # # #                     raise ValueError("Re-chargement des données GPS échoué, DataFrame vide.")
# # # #             except Exception as e:
# # # #                 print(f"DEBUG: Erreur lors du rechargement des données GPS durant l'upload: {e}")
# # # #                 traceback.print_exc()
# # # #                 # Créer un DataFrame GPS vide pour éviter les erreurs plus tard
# # # #                 GLOBAL_DF_GPS_INFO = pd.DataFrame({'Station': [], 'Lat': [], 'Long': [], 'Timezone': []})


# # # #         GLOBAL_PROCESSED_DATA_DF = interpolation(GLOBAL_PROCESSED_DATA_DF, DATA_LIMITS, GLOBAL_DF_GPS_INFO)
# # # #         GLOBAL_PROCESSED_DATA_DF = traiter_outliers_meteo(GLOBAL_PROCESSED_DATA_DF, DATA_LIMITS)
        
# # # #         # Nettoyage des colonnes temporaires
# # # #         cols_to_drop_after_process = [
# # # #             'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date',
# # # #             'sunrise_time_local', 'sunset_time_local'
# # # #         ]
# # # #         GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.drop(columns=cols_to_drop_after_process, errors='ignore')

# # # #         print(f"DEBUG_STATION_NAMES (upload_file - FIN): GLOBAL_PROCESSED_DATA_DF FINAL après interpolation et outliers. Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
        
# # # #         # Pour cet exemple, nous allons juste rediriger vers la page d'options
# # # #         return redirect(url_for('show_visualizations_options'))

# # # #     except Exception as e:
# # # #         print(f"DEBUG: Une erreur inattendue s'est produite lors du traitement des données: {e}")
# # # #         traceback.print_exc() # Ajout du traceback en cas d'erreur inattendue
# # # #         return redirect(url_for('index'))

# # # # @app.route('/visualizations_options')
# # # # def show_visualizations_options():
# # # #     """
# # # #     Affiche la page des options de visualisation après le traitement des données.
# # # #     """
# # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # #         print("DEBUG_STATION_NAMES (show_visualizations_options): GLOBAL_PROCESSED_DATA_DF est None. Redirection vers l'index.")
# # # #         return redirect(url_for('index'))

# # # #     # DEBUG: Vérifier les stations avant de les passer au template
# # # #     # Assurez-vous que la colonne 'Station' existe avant d'essayer d'y accéder
# # # #     if 'Station' not in GLOBAL_PROCESSED_DATA_DF.columns:
# # # #         print("DEBUG_STATION_NAMES (show_visualizations_options): ERREUR: La colonne 'Station' est manquante dans GLOBAL_PROCESSED_DATA_DF lors de l l'affichage des options.")
# # # #         return redirect(url_for('index')) # Ou afficher une erreur appropriée
    
# # # #     stations_in_df = GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()
# # # #     print(f"DEBUG_STATION_NAMES (show_visualizations_options): Stations détectées dans GLOBAL_PROCESSED_DATA_DF (pour le template): {stations_in_df}")
    
# # # #     # Appel de la fonction de calcul des statistiques journalières pour le tableau HTML
# # # #     daily_stats_df = calculate_daily_summary_table(GLOBAL_PROCESSED_DATA_DF)
    
# # # #     print(f"DEBUG (app.py/visualizations_options): Stations présentes dans daily_stats_df: {daily_stats_df['Station'].unique().tolist() if not daily_stats_df.empty else 'DataFrame vide'}")

# # # #     initial_data_html = None
# # # #     data_summary = f"Lignes: {len(GLOBAL_PROCESSED_DATA_DF)}, Colonnes: {len(GLOBAL_PROCESSED_DATA_DF.columns)}"
    
# # # #     unique_stations_count = len(stations_in_df) # Utiliser la liste déjà extraite

# # # #     if unique_stations_count > 1:
# # # #         top_n_data = GLOBAL_PROCESSED_DATA_DF.groupby('Station').head(10)
# # # #         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
# # # #     else:
# # # #         top_n_data = GLOBAL_PROCESSED_DATA_DF.head(20)
# # # #         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
    
# # # #     if initial_data_html is None:
# # # #         initial_data_html = "<p class='text-center text-red-500'>Aucune donnée à afficher pour la prévisualisation.</p>"


# # # #     daily_stats_html = daily_stats_df.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=False, border=0) if not daily_stats_df.empty else None

# # # #     # Récupérer les informations pour les menus déroulants dynamiquement
# # # #     stations = sorted(stations_in_df) # Utiliser la variable déjà vérifiée
# # # #     variables = sorted([col for col in GLOBAL_PROCESSED_DATA_DF.columns if col in METADATA_VARIABLES.keys()])
# # # #     can_compare_stations = len(stations) >= 2
    
# # # #     daily_stat_variables_available = [
# # # #         var for var in METADATA_VARIABLES.keys() if var in GLOBAL_PROCESSED_DATA_DF.columns and
# # # #         pd.api.types.is_numeric_dtype(GLOBAL_PROCESSED_DATA_DF[var])
# # # #     ]
# # # #     daily_stat_variables = sorted(daily_stat_variables_available)


# # # #     periodes = ['Brute', 'Journalière', 'Hebdomadaire', 'Mensuelle', 'Annuelle']

# # # #     return render_template(
# # # #         'visualizations_options.html',
# # # #         initial_data_html=initial_data_html,
# # # #         data_summary=data_summary,
# # # #         daily_stats_html=daily_stats_html,
# # # #         stations=stations,
# # # #         variables=variables,
# # # #         periodes=periodes,
# # # #         can_compare_stations=can_compare_stations,
# # # #         METADATA_VARIABLES=METADATA_VARIABLES,
# # # #         daily_stat_variables=daily_stat_variables # Passer les variables pour les stats journalières
# # # #     )

# # # # @app.route('/generate_single_plot', methods=['GET', 'POST'])
# # # # def generate_single_plot():
# # # #     """
# # # #     Génère et affiche un graphique pour une seule variable et une seule station.
# # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # #     """
# # # #     if request.method == 'GET':
# # # #         return redirect(url_for('show_visualizations_options'))

# # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # #         return redirect(url_for('index'))

# # # #     station_select = request.form.get('station_select_single')
# # # #     variable_select = request.form.get('variable_select_single')
# # # #     periode_select = request.form.get('periode_select_single')

# # # #     if not station_select or not variable_select or not periode_select:
# # # #         return redirect(url_for('show_visualizations_options'))

# # # #     try:
# # # #         fig = generer_graphique_par_variable_et_periode(
# # # #             GLOBAL_PROCESSED_DATA_DF,
# # # #             station_select,
# # # #             variable_select,
# # # #             periode_select,
# # # #             PALETTE_DEFAUT, # Utilisez PALETTE_DEFAUT pour les couleurs
# # # #             METADATA_VARIABLES
# # # #         )

# # # #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# # # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # # #             meta = METADATA_VARIABLES.get(variable_select, {})
# # # #             plot_title = f"Évolution de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) pour la station {station_select} ({periode_select})"
# # # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # # #         else:
# # # #             return redirect(url_for('show_visualizations_options'))
# # # #     except TypeError as e:
# # # #         print(f"DEBUG: Erreur de type dans generate_single_plot: {e}")
# # # #         traceback.print_exc()
# # # #         return redirect(url_for('show_visualizations_options'))
# # # #     except Exception as e:
# # # #         print(f"DEBUG: Erreur lors de la génération du graphique: {e}")
# # # #         traceback.print_exc()
# # # #         return redirect(url_for('show_visualizations_options'))

# # # # @app.route('/generate_comparative_plot', methods=['GET', 'POST'])
# # # # def generate_comparative_plot():
# # # #     """
# # # #     Génère et affiche un graphique comparatif pour une variable à travers toutes les stations.
# # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # #     """
# # # #     if request.method == 'GET':
# # # #         return redirect(url_for('show_visualizations_options'))

# # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # #         return redirect(url_for('index'))

# # # #     variable_select = request.form.get('variable_select_comparative')
# # # #     periode_select = request.form.get('periode_select_comparative')

# # # #     if not variable_select or not periode_select:
# # # #         return redirect(url_for('show_visualizations_options'))

# # # #     try:
# # # #         fig = generer_graphique_comparatif(
# # # #             GLOBAL_PROCESSED_DATA_DF,
# # # #             variable_select,
# # # #             periode_select,
# # # #             PALETTE_DEFAUT, # Utilisez PALETTE_DEFAUT pour les couleurs
# # # #             METADATA_VARIABLES
# # # #         )

# # # #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# # # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # # #             meta = METADATA_VARIABLES.get(variable_select, {})
# # # #             plot_title = f"Comparaison de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) entre stations ({periode_select})"
# # # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # # #         else:
# # # #             return redirect(url_for('show_visualizations_options'))
# # # #     except TypeError as e:
# # # #         print(f"DEBUG: Erreur de type dans generate_comparative_plot: {e}")
# # # #         traceback.print_exc()
# # # #         return redirect(url_for('show_visualizations_options'))
# # # #     except Exception as e:
# # # #         print(f"DEBUG: Erreur lors de la génération du graphique comparatif: {e}")
# # # #         traceback.print_exc()
# # # #         return redirect(url_for('show_visualizations_options'))

# # # # @app.route('/generate_multi_variable_plot', methods=['GET', 'POST'])
# # # # def generate_multi_variable_plot_route():
# # # #     """
# # # #     Génère et affiche un graphique d'évolution multi-variables normalisé pour une station.
# # # #     Si la méthode est GET, redirige vers les options de visualisation.
# # # #     """
# # # #     if request.method == 'GET':
# # # #         return redirect(url_for('show_visualizations_options'))

# # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # #         return redirect(url_for('index'))

# # # #     station_select = request.form.get('station_select_multi_var')

# # # #     if not station_select:
# # # #         return redirect(url_for('show_visualizations_options'))

# # # #     try:
# # # #         fig = generate_multi_variable_station_plot(
# # # #             GLOBAL_PROCESSED_DATA_DF,
# # # #             station_select,
# # # #             PALETTE_DEFAUT, # Utilisez PALETTE_DEFAUT pour les couleurs
# # # #             METADATA_VARIABLES
# # # #         )

# # # #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# # # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # # #             plot_title = f"Évolution Normalisée des Variables Météorologiques pour la station {station_select}"
# # # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # # #         else:
# # # #             return redirect(url_for('show_visualizations_options'))
# # # #     except TypeError as e:
# # # #         print(f"DEBUG: Erreur de type dans generate_multi_variable_plot_route: {e}")
# # # #         traceback.print_exc()
# # # #         return redirect(url_for('show_visualizations_options'))
# # # #     except Exception as e:
# # # #         print(f"DEBUG: Erreur lors de la génération du graphique multi-variables: {e}")
# # # #         traceback.print_exc()
# # # #         return redirect(url_for('show_visualizations_options'))

# # # # @app.route('/get_daily_stats_summary_table', methods=['GET'])
# # # # def get_daily_stats_summary_table():
# # # #     """
# # # #     Route AJAX pour obtenir les statistiques journalières récapitulatives.
# # # #     """
# # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # #         return jsonify({'error': 'Aucune donnée traitée pour les statistiques journalières.'}), 404
    
# # # #     try:
# # # #         summary_table = calculate_daily_summary_table(GLOBAL_PROCESSED_DATA_DF)
# # # #         # Convertir les Timestamps en chaînes pour la sérialisation JSON
# # # #         for col in summary_table.columns:
# # # #             if pd.api.types.is_datetime64_any_dtype(summary_table[col]):
# # # #                 summary_table[col] = summary_table[col].dt.strftime('%Y-%m-%d %H:%M:%S')
# # # #             elif summary_table[col].dtype == 'object' and summary_table[col].apply(lambda x: isinstance(x, (datetime, pd.Timestamp))).any():
# # # #                  summary_table[col] = summary_table[col].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, (datetime, pd.Timestamp)) else x)


# # # #         return jsonify(summary_table.to_dict(orient='records'))
# # # #     except Exception as e:
# # # #         print(f"Erreur lors de la génération du tableau de statistiques journalières: {e}")
# # # #         traceback.print_exc()
# # # #         return jsonify({'error': f"Erreur lors de la génération du tableau de statistiques journalières: {str(e)}"}), 500


# # # # @app.route('/get_daily_stats_plot_json/<station_name>/<path:variable_name>', methods=['GET'])
# # # # def get_daily_stats_plot_json(station_name, variable_name):
# # # #     """
# # # #     Route AJAX pour obtenir le JSON Plotly d'un graphique de statistiques journalières
# # # #     pour une station et une variable données.
# # # #     """
# # # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # # #         return jsonify({'error': 'Aucune donnée disponible pour les statistiques journalières.'}), 404

# # # #     try:
# # # #         # Appel de la fonction de traitement de données pour générer la figure Matplotlib
# # # #         fig = generate_daily_summary_plot_for_variable(
# # # #             GLOBAL_PROCESSED_DATA_DF,
# # # #             station_name,
# # # #             variable_name,
# # # #             METADATA_VARIABLES,
# # # #             PALETTE_DEFAUT
# # # #         )
        
# # # #         if fig:
# # # #             # Sauvegarder la figure dans un buffer BytesIO
# # # #             img_buffer = io.BytesIO()
# # # #             fig.savefig(img_buffer, format='png', bbox_inches='tight')
# # # #             img_buffer.seek(0)
            
# # # #             # Encoder l'image en base64
# # # #             encoded_image = base64.b64encode(img_buffer.read()).decode('utf-8')
            
# # # #             # Fermer la figure pour libérer la mémoire
# # # #             plt.close(fig)

# # # #             return jsonify({'image_data': encoded_image, 'mime_type': 'image/png'})
# # # #         else:
# # # #             return jsonify({'error': 'Graphique de statistiques journalières vide pour cette sélection.'}), 404

# # # #     except Exception as e:
# # # #         print(f"Erreur lors de la génération du graphique de stats journalières (API): {e}")
# # # #         traceback.print_exc()
# # # #         return jsonify({'error': f'Erreur lors de la génération du graphique de stats journalières: {e}'}), 500


# # # # @app.route('/reset_data', methods=['GET', 'POST'])
# # # # def reset_data():
# # # #     """
# # # #     Réinitialise les données globales traitées et redirige vers la page d'accueil.
# # # #     Si la méthode est GET, redirige simplement vers la page d'accueil.
# # # #     """
# # # #     if request.method == 'GET':
# # # #         return redirect(url_for('index'))

# # # #     global GLOBAL_PROCESSED_DATA_DF
# # # #     GLOBAL_PROCESSED_DATA_DF = None
# # # #     # Vous devriez également gérer la réinitialisation des variables de session si elles sont utilisées
# # # #     # par exemple, session.pop('available_stations', None)
# # # #     return redirect(url_for('index'))


# # # # if __name__ == '__main__':
# # # #     app.run(debug=True)

# # # import pandas as pd
# # # from flask import Flask, render_template, request, jsonify, redirect, url_for
# # # from werkzeug.utils import secure_filename
# # # import os
# # # import io
# # # import base64
# # # from datetime import datetime
# # # import json
# # # import plotly.graph_objects as go
# # # import matplotlib.pyplot as plt # Importation de Matplotlib
# # # import seaborn as sns # Importation de Seaborn
# # # import traceback # Importation de traceback pour les messages d'erreur détaillés
# # # import re # Importation du module re pour les expressions régulières

# # # # Importation des fonctions de traitement de données
# # # from data_processing import (
# # #     create_datetime, interpolation, _load_and_prepare_gps_data,
# # #     gestion_doublons, traiter_outliers_meteo, create_rain_mm,
# # #     generer_graphique_par_variable_et_periode, generer_graphique_comparatif,
# # #     generate_multi_variable_station_plot, calculate_daily_summary_table, # Keep calculate_daily_summary_table for the HTML table
# # #     generate_variable_summary_plots_for_web # Renamed function
# # # )

# # # # Importation des configurations globales
# # # from config import METADATA_VARIABLES, PALETTE_DEFAUT, DATA_LIMITS, STATIONS_BY_BASSIN

# # # app = Flask(__name__)
# # # app.config['UPLOAD_FOLDER'] = 'uploads'
# # # app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 # 16 MB max-upload size
# # # os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# # # # Variable globale pour stocker les données du DataFrame après traitement
# # # GLOBAL_PROCESSED_DATA_DF = None
# # # GLOBAL_DF_GPS_INFO = None # Variable globale pour les données GPS

# # # # Chemin vers le fichier JSON des coordonnées de station
# # # STATION_COORDS_PATH = os.path.join('data', 'station_coordinates.json')

# # # # Initialisation de GLOBAL_DF_GPS_INFO au démarrage de l'application
# # # # Assurez-vous que le dossier 'data' existe
# # # data_dir_for_gps = 'data'
# # # os.makedirs(data_dir_for_gps, exist_ok=True)

# # # try:
# # #     if not os.path.exists(STATION_COORDS_PATH) or os.stat(STATION_COORDS_PATH).st_size == 0:
# # #         print(f"DEBUG: Le fichier '{STATION_COORDS_PATH}' n'existe pas ou est vide. Exécution de data_processing._load_and_prepare_gps_data()...")
# # #         GLOBAL_DF_GPS_INFO = _load_and_prepare_gps_data()
# # #         if not GLOBAL_DF_GPS_INFO.empty:
# # #             GLOBAL_DF_GPS_INFO.to_json(STATION_COORDS_PATH, orient='records', indent=4)
# # #             print(f"DEBUG: Données GPS des stations sauvegardées dans '{STATION_COORDS_PATH}'.")
# # #         else:
# # #             print("DEBUG: Avertissement: Les données GPS sont vides après la préparation, le fichier JSON ne sera pas créé ou sera vide.")
# # #     else:
# # #         GLOBAL_DF_GPS_INFO = pd.read_json(STATION_COORDS_PATH, orient='records')
# # #         print(f"DEBUG: Données GPS des stations chargées depuis '{STATION_COORDS_PATH}'.")
    
# # #     if GLOBAL_DF_GPS_INFO.empty:
# # #         raise ValueError("Le DataFrame des données GPS est vide après le chargement/la préparation.")
    
# # #     print("DEBUG: Préparation des données de coordonnées des stations terminée.")
# # # except Exception as e:
# # #     print(f"DEBUG: Erreur fatale lors du chargement ou de la préparation des données GPS des stations au démarrage: {e}. "
# # #           "L'application pourrait ne pas fonctionner correctement sans ces données.")
# # #     GLOBAL_DF_GPS_INFO = pd.DataFrame({
# # #         'Station': [],
# # #         'Lat': [],
# # #         'Long': [],
# # #         'Timezone': []
# # #     })
# # #     traceback.print_exc()


# # # @app.route('/')
# # # def index():
# # #     """
# # #     Route principale affichant le formulaire d'upload de fichiers.
# # #     """
# # #     # Utiliser STATIONS_BY_BASSIN importé de config.py
# # #     return render_template('index.html', stations_by_bassin=STATIONS_BY_BASSIN)


# # # @app.route('/upload', methods=['GET', 'POST'])
# # # def upload_file():
# # #     """
# # #     Gère l'upload et le traitement des fichiers CSV/Excel.
# # #     Permet l'upload de multiples fichiers, chacun associé à un bassin et une station,
# # #     en vérifiant leur compatibilité et en les ajoutant au DataFrame global.
# # #     Si la méthode est GET, redirige vers la page d'accueil.
# # #     """
# # #     if request.method == 'GET':
# # #         return redirect(url_for('index'))

# # #     global GLOBAL_PROCESSED_DATA_DF
# # #     global GLOBAL_DF_GPS_INFO # Déclaration pour utiliser la variable globale

# # #     upload_groups = []
    
# # #     # Collecter tous les indices de groupe présents dans le formulaire
# # #     all_input_indices = set()
# # #     for key in request.form.keys():
# # #         match = re.search(r'_((\d+))$', key)
# # #         if match:
# # #             all_input_indices.add(int(match.group(1)))
# # #     for key in request.files.keys():
# # #         match = re.search(r'_(\d+)$', key)
# # #         if match:
# # #             all_input_indices.add(int(match.group(1)))
            
# # #     sorted_indices = sorted(list(all_input_indices))

# # #     if not sorted_indices:
# # #         # Aucun dataset n'a été soumis
# # #         print("DEBUG: Aucun groupe d'upload détecté.")
# # #         return redirect(url_for('index')) # Redirection pour rester sur la page d'upload

# # #     print(f"DEBUG (upload_file): Nombre de groupes d'upload détectés: {len(sorted_indices)}")
# # #     for index in sorted_indices:
# # #         bassin_name = request.form.get(f'bassin_{index}')
# # #         station_name = request.form.get(f'station_{index}')
# # #         file_obj = request.files.get(f'file_input_{index}') 

# # #         if not bassin_name or not station_name or not file_obj or file_obj.filename == '':
# # #             # Un groupe est incomplet, gérer l'erreur ou ignorer
# # #             print(f"DEBUG: Groupe d'upload {index} incomplet. Ignoré.")
# # #             continue # Ignorer ce groupe incomplet et passer au suivant

# # #         upload_groups.append({
# # #             'bassin': bassin_name,
# # #             'station': station_name,
# # #             'file': file_obj,
# # #             'index': index
# # #         })
    
# # #     if not upload_groups:
# # #         # Aucun dataset valide n'a été trouvé pour le traitement
# # #         print("DEBUG: Aucun groupe d'upload valide trouvé après filtrage.")
# # #         return redirect(url_for('index')) # Redirection si aucun groupe n'est valide


# # #     processed_dataframes_for_batch = []
    
# # #     expected_raw_data_columns_for_comparison = None
# # #     expected_raw_time_columns_for_comparison = None 

    
# # #     for group_info in upload_groups:
# # #         file = group_info['file']
# # #         bassin = group_info['bassin']
# # #         station = group_info['station']

# # #         file_extension = os.path.splitext(file.filename)[1].lower()
# # #         df_temp = None

# # #         try:
# # #             if file_extension == '.csv':
# # #                 df_temp = pd.read_csv(io.BytesIO(file.read()))
# # #             elif file_extension in ['.xls', '.xlsx']:
# # #                 df_temp = pd.read_excel(io.BytesIO(file.read()))
# # #             else:
# # #                 print(f"DEBUG: Type de fichier non supporté pour '{file.filename}'.")
# # #                 continue
            
# # #             if df_temp is not None:
# # #                 current_file_columns = df_temp.columns.tolist()
                
# # #                 current_raw_time_cols = []
# # #                 if 'Date' in current_file_columns:
# # #                     current_raw_time_cols.append('Date')
# # #                 if all(col in current_file_columns for col in ['Year', 'Month', 'Day', 'Hour', 'Minute']):
# # #                     current_raw_time_cols.extend(['Year', 'Month', 'Day', 'Hour', 'Minute'])
# # #                 current_raw_time_cols_sorted = sorted(list(set(current_raw_time_cols)))

# # #                 current_raw_data_cols_sorted = sorted([
# # #                     col for col in current_file_columns 
# # #                     if col != 'Station' and col not in ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
# # #                 ])

# # #                 if expected_raw_data_columns_for_comparison is None:
# # #                     expected_raw_data_columns_for_comparison = current_raw_data_cols_sorted
# # #                     expected_raw_time_columns_for_comparison = current_raw_time_cols_sorted
# # #                 else:
# # #                     if current_raw_data_cols_sorted != expected_raw_data_columns_for_comparison:
# # #                         print(f"DEBUG: Mismatch de colonnes de données pour '{file.filename}'. Attendu: {expected_raw_data_columns_for_comparison}, Trouvé: {current_raw_data_cols_sorted}")
# # #                         return redirect(url_for('index'))
                    
# # #                     if current_raw_time_cols_sorted != expected_raw_time_columns_for_comparison:
# # #                         print(f"DEBUG: Mismatch de colonnes temporelles pour '{file.filename}'. Attendu: {expected_raw_time_columns_for_comparison}, Trouvé: {current_raw_time_cols_sorted}")
# # #                         return redirect(url_for('index'))
                
# # #                 df_temp['Station'] = station
# # #                 print(f"DEBUG (upload_file): Fichier '{file.filename}' - Station assignée: '{station}'. Premières 5 lignes du DF temporaire:\n{df_temp.head(5)}")
# # #                 processed_dataframes_for_batch.append(df_temp)

# # #         except Exception as e:
# # #             print(f"DEBUG (upload_file): Erreur lors de la lecture ou du traitement du fichier '{file.filename}': {e}")
# # #             traceback.print_exc()
# # #             return redirect(url_for('index'))

# # #     if not processed_dataframes_for_batch:
# # #         print("DEBUG: La liste des DataFrames traités pour le batch est vide.")
# # #         return redirect(url_for('index'))

# # #     try:
# # #         # Concaténation de tous les DataFrames du batch
# # #         df_current_batch_raw_merged = pd.concat(processed_dataframes_for_batch, ignore_index=True)
# # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_raw_merged APRÈS CONCATÉNATION DU BATCH:\n{df_current_batch_raw_merged['Station'].unique().tolist()}")
        
# # #         # Étape 1: Créer la colonne Datetime AVANT la gestion des doublons
# # #         df_current_batch_with_datetime = create_datetime(df_current_batch_raw_merged, bassin=upload_groups[0]['bassin'])
# # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_with_datetime APRÈS CREATE_DATETIME DU BATCH:\n{df_current_batch_with_datetime['Station'].unique().tolist()}")

# # #         # Étape 2: Gérer les doublons maintenant que 'Datetime' est une colonne
# # #         df_current_batch_cleaned = gestion_doublons(df_current_batch_with_datetime)
# # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_cleaned APRÈS GESTION DOUBLONS DU BATCH:\n{df_current_batch_cleaned['Station'].unique().tolist()}")

# # #         if 'Datetime' not in df_current_batch_cleaned.columns or df_current_batch_cleaned['Datetime'].isnull().all():
# # #             print("DEBUG: La colonne 'Datetime' est manquante ou vide après le nettoyage.")
# # #             return redirect(url_for('index'))
        
# # #         # Assurez-vous que 'Datetime' est bien de type datetime et nettoyez les NaT
# # #         df_current_batch_cleaned['Datetime'] = pd.to_datetime(df_current_batch_cleaned['Datetime'], errors='coerce')
# # #         df_current_batch_cleaned.dropna(subset=['Datetime'], inplace=True) 

# # #         if df_current_batch_cleaned.empty:
# # #             print("DEBUG: Le DataFrame est vide après le nettoyage des dates invalides.")
# # #             return redirect(url_for('index'))

# # #         # Définir Datetime comme index
# # #         df_current_batch_processed = df_current_batch_cleaned.set_index('Datetime').sort_index()
# # #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_processed APRÈS SET_INDEX ET SORT_INDEX DU BATCH: {df_current_batch_processed['Station'].unique().tolist()}")


# # #         if GLOBAL_PROCESSED_DATA_DF is None:
# # #             GLOBAL_PROCESSED_DATA_DF = df_current_batch_processed
# # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF initialisé. Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
# # #         else:
# # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF EXISTANT AVANT FUSION: Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
            
# # #             new_batch_stations = df_current_batch_processed['Station'].unique().tolist()
# # #             print(f"DEBUG (upload_file): Stations dans le batch actuel à fusionner: {new_batch_stations}")

# # #             # Filtrer les données existantes pour les stations du lot actuel du DataFrame global
# # #             df_global_filtered = GLOBAL_PROCESSED_DATA_DF[~GLOBAL_PROCESSED_DATA_DF['Station'].isin(new_batch_stations)].copy()
# # #             print(f"DEBUG (upload_file): Stations dans GLOBAL_PROCESSED_DATA_DF après filtrage des stations du batch actuel: {df_global_filtered['Station'].unique().tolist() if not df_global_filtered.empty else 'DataFrame global vide après filtrage'}")

# # #             # Concaténer le DataFrame global filtré avec les données du nouveau lot
# # #             # Assurez-vous que les deux DataFrames ont des index compatibles (DatetimeIndex)
# # #             combined_df = pd.concat([df_global_filtered, df_current_batch_processed], ignore_index=False) # Important: ignore_index=False pour préserver les DatetimeIndex et les aligner
# # #             print(f"DEBUG (upload_file): Stations uniques dans combined_df APRÈS CONCATINATION GLOBAL_FILTRÉ+BATCH: {combined_df['Station'].unique().tolist()}")

# # #             # Pas besoin de to_datetime/dropna/set_index ici si les index sont déjà corrects.
# # #             # Cependant, on peut re-trier par sécurité.
# # #             GLOBAL_PROCESSED_DATA_DF = combined_df.sort_index()

# # #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF mis à jour. Stations uniques finales: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

# # #         # --- VÉRIFICATION FINALE ET DÉFENSIVE DE L'INDEX ET DE LA COLONNE 'STATION' DE GLOBAL_PROCESSED_DATA_DF AVANT TOUT APPEL EXTERNE ---
# # #         print(f"DEBUG_STATION_NAMES (upload_file): Type de l'index de GLOBAL_PROCESSED_DATA_DF avant vérification finale: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# # #         if 'Station' not in GLOBAL_PROCESSED_DATA_DF.columns:
# # #             print("DEBUG_STATION_NAMES (upload_file): ERREUR: La colonne 'Station' est manquante dans GLOBAL_PROCESSED_DATA_DF après traitement.")
# # #             return redirect(url_for('index')) # Redirection car données invalides

# # #         try:
# # #             if not isinstance(GLOBAL_PROCESSED_DATA_DF.index, pd.DatetimeIndex):
# # #                 if 'Datetime' in GLOBAL_PROCESSED_DATA_DF.columns:
# # #                     GLOBAL_PROCESSED_DATA_DF['Datetime'] = pd.to_datetime(GLOBAL_PROCESSED_DATA_DF['Datetime'], errors='coerce')
# # #                     GLOBAL_PROCESSED_DATA_DF.dropna(subset=['Datetime'], inplace=True)
# # #                     GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.set_index('Datetime').sort_index()
# # #                 else:
# # #                     raise TypeError("L'index n'est pas DatetimeIndex et aucune colonne 'Datetime' n'est disponible pour la recréation.")
            
# # #             GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.sort_index()

# # #             print(f"DEBUG_STATION_NAMES (upload_file): Index de GLOBAL_PROCESSED_DATA_DF VALIDÉ. Stations uniques finales après vérification finale: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

# # #         except Exception as e:
# # #             print(f"DEBUG_STATION_NAMES (upload_file): Erreur critique finale: Impossible de garantir un DatetimeIndex pour l'interpolation. Détails: {e}")
# # #             traceback.print_exc()
# # #             return redirect(url_for('index'))
        
# # #         if GLOBAL_PROCESSED_DATA_DF.empty:
# # #             print("DEBUG_STATION_NAMES (upload_file): GLOBAL_PROCESSED_DATA_DF est vide après la vérification finale.")
# # #             return redirect(url_for('index'))


# # #         print(f"DEBUG (upload_file): Type de l'index de GLOBAL_PROCESSED_DATA_DF AVANT interpolation: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# # #         print(f"DEBUG (upload_file): Dtypes de GLOBAL_PROCESSED_DATA_DF (après vérif index): \n{GLOBAL_PROCESSED_DATA_DF.dtypes}")


# # #         # Traitement d'interpolation et des outliers sur le DataFrame global unifié
# # #         # Ces fonctions attendent un DatetimeIndex
# # #         # S'assurer que GLOBAL_DF_GPS_INFO est valide avant de le passer
# # #         if GLOBAL_DF_GPS_INFO is None or GLOBAL_DF_GPS_INFO.empty:
# # #             print("DEBUG: GLOBAL_DF_GPS_INFO est vide ou None. Tente de le recharger/préparer.")
# # #             try:
# # #                 # Tente de recharger ou de générer les données GPS ici si elles sont manquantes
# # #                 # Cela peut arriver si le chargement initial a échoué.
# # #                 GLOBAL_DF_GPS_INFO = _load_and_prepare_gps_data()
# # #                 if GLOBAL_DF_GPS_INFO.empty:
# # #                     raise ValueError("Re-chargement des données GPS échoué, DataFrame vide.")
# # #             except Exception as e:
# # #                 print(f"DEBUG: Erreur lors du rechargement des données GPS durant l'upload: {e}")
# # #                 traceback.print_exc()
# # #                 # Créer un DataFrame GPS vide pour éviter les erreurs plus tard
# # #                 GLOBAL_DF_GPS_INFO = pd.DataFrame({'Station': [], 'Lat': [], 'Long': [], 'Timezone': []})


# # #         GLOBAL_PROCESSED_DATA_DF = interpolation(GLOBAL_PROCESSED_DATA_DF, DATA_LIMITS, GLOBAL_DF_GPS_INFO)
# # #         GLOBAL_PROCESSED_DATA_DF = traiter_outliers_meteo(GLOBAL_PROCESSED_DATA_DF, DATA_LIMITS)
        
# # #         # Nettoyage des colonnes temporaires
# # #         cols_to_drop_after_process = [
# # #             'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date',
# # #             'sunrise_time_local', 'sunset_time_local'
# # #         ]
# # #         GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.drop(columns=cols_to_drop_after_process, errors='ignore')

# # #         print(f"DEBUG_STATION_NAMES (upload_file - FIN): GLOBAL_PROCESSED_DATA_DF FINAL après interpolation et outliers. Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
        
# # #         # Pour cet exemple, nous allons juste rediriger vers la page d'options
# # #         return redirect(url_for('show_visualizations_options'))

# # #     except Exception as e:
# # #         print(f"DEBUG: Une erreur inattendue s'est produite lors du traitement des données: {e}")
# # #         traceback.print_exc() # Ajout du traceback en cas d'erreur inattendue
# # #         return redirect(url_for('index'))

# # # @app.route('/visualizations_options')
# # # def show_visualizations_options():
# # #     """
# # #     Affiche la page des options de visualisation après le traitement des données.
# # #     """
# # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # #         print("DEBUG_STATION_NAMES (show_visualizations_options): GLOBAL_PROCESSED_DATA_DF est None. Redirection vers l'index.")
# # #         return redirect(url_for('index'))

# # #     # DEBUG: Vérifier les stations avant de les passer au template
# # #     # Assurez-vous que la colonne 'Station' existe avant d'essayer d'y accéder
# # #     if 'Station' not in GLOBAL_PROCESSED_DATA_DF.columns:
# # #         print("DEBUG_STATION_NAMES (show_visualizations_options): ERREUR: La colonne 'Station' est manquante dans GLOBAL_PROCESSED_DATA_DF lors de l l'affichage des options.")
# # #         return redirect(url_for('index')) # Ou afficher une erreur appropriée
    
# # #     stations_in_df = GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()
# # #     print(f"DEBUG_STATION_NAMES (show_visualizations_options): Stations détectées dans GLOBAL_PROCESSED_DATA_DF (pour le template): {stations_in_df}")
    
# # #     daily_stats_df = calculate_daily_summary_table(GLOBAL_PROCESSED_DATA_DF)
    
# # #     print(f"DEBUG (app.py/visualizations_options): Stations présentes dans daily_stats_df: {daily_stats_df['Station'].unique().tolist() if not daily_stats_df.empty else 'DataFrame vide'}")

# # #     initial_data_html = None
# # #     data_summary = f"Lignes: {len(GLOBAL_PROCESSED_DATA_DF)}, Colonnes: {len(GLOBAL_PROCESSED_DATA_DF.columns)}"
    
# # #     unique_stations_count = len(stations_in_df) # Utiliser la liste déjà extraite

# # #     if unique_stations_count > 1:
# # #         top_n_data = GLOBAL_PROCESSED_DATA_DF.groupby('Station').head(10)
# # #         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
# # #     else:
# # #         top_n_data = GLOBAL_PROCESSED_DATA_DF.head(20)
# # #         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
    
# # #     if initial_data_html is None:
# # #         initial_data_html = "<p class='text-center text-red-500'>Aucune donnée à afficher pour la prévisualisation.</p>"


# # #     daily_stats_html = daily_stats_df.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=False, border=0) if not daily_stats_df.empty else None

# # #     # Récupérer les informations pour les menus déroulants dynamiquement
# # #     stations = sorted(stations_in_df) # Utiliser la variable déjà vérifiée
# # #     variables = sorted([col for col in GLOBAL_PROCESSED_DATA_DF.columns if col in METADATA_VARIABLES.keys()])
# # #     can_compare_stations = len(stations) >= 2
    
# # #     daily_stat_variables_available = [
# # #         var for var in METADATA_VARIABLES.keys() if var in GLOBAL_PROCESSED_DATA_DF.columns and
# # #         pd.api.types.is_numeric_dtype(GLOBAL_PROCESSED_DATA_DF[var])
# # #     ]
# # #     daily_stat_variables = sorted(daily_stat_variables_available)

# # #     periodes = ['Brute', 'Journalière', 'Hebdomadaire', 'Mensuelle', 'Annuelle']

# # #     # For the new detailed stats plots, we need to pass all available variables
# # #     # and the list of stations. The JS will then handle the AJAX calls.
# # #     all_numeric_variables = sorted([v for v in METADATA_VARIABLES.keys() if v in GLOBAL_PROCESSED_DATA_DF.columns and pd.api.types.is_numeric_dtype(GLOBAL_PROCESSED_DATA_DF[v])])
    
# # #     return render_template(
# # #         'visualizations_options.html',
# # #         initial_data_html=initial_data_html,
# # #         data_summary=data_summary,
# # #         daily_stats_html=daily_stats_html,
# # #         stations=stations,
# # #         variables=variables,
# # #         periodes=periodes,
# # #         can_compare_stations=can_compare_stations,
# # #         METADATA_VARIABLES=METADATA_VARIABLES,
# # #         daily_stat_variables=daily_stat_variables, # This is for the previous daily stats table/plot if still used.
# # #         stations_for_summary_plots=stations, # Use the same 'stations' list for consistency in dropdown
# # #         all_available_variables_for_summary=all_numeric_variables # Pass to JS via template
# # #     )

# # # @app.route('/generate_single_plot', methods=['GET', 'POST'])
# # # def generate_single_plot():
# # #     """
# # #     Génère et affiche un graphique pour une seule variable et une seule station.
# # #     Si la méthode est GET, redirige vers les options de visualisation.
# # #     """
# # #     if request.method == 'GET':
# # #         return redirect(url_for('show_visualizations_options'))

# # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # #         return redirect(url_for('index'))

# # #     station_select = request.form.get('station_select_single')
# # #     variable_select = request.form.get('variable_select_single')
# # #     periode_select = request.form.get('periode_select_single')

# # #     if not station_select or not variable_select or not periode_select:
# # #         return redirect(url_for('show_visualizations_options'))

# # #     try:
# # #         fig = generer_graphique_par_variable_et_periode(
# # #             GLOBAL_PROCESSED_DATA_DF,
# # #             station_select,
# # #             variable_select,
# # #             periode_select,
# # #             PALETTE_DEFAUT, # Utilisez PALETTE_DEFAUT pour les couleurs
# # #             METADATA_VARIABLES
# # #         )

# # #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # #             meta = METADATA_VARIABLES.get(variable_select, {})
# # #             plot_title = f"Évolution de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) pour la station {station_select} ({periode_select})"
# # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # #         else:
# # #             return redirect(url_for('show_visualizations_options'))
# # #     except TypeError as e:
# # #         print(f"DEBUG: Erreur de type dans generate_single_plot: {e}")
# # #         traceback.print_exc()
# # #         return redirect(url_for('show_visualizations_options'))
# # #     except Exception as e:
# # #         print(f"DEBUG: Erreur lors de la génération du graphique: {e}")
# # #         traceback.print_exc()
# # #         return redirect(url_for('show_visualizations_options'))

# # # @app.route('/generate_comparative_plot', methods=['GET', 'POST'])
# # # def generate_comparative_plot():
# # #     """
# # #     Génère et affiche un graphique comparatif pour une variable à travers toutes les stations.
# # #     Si la méthode est GET, redirige vers les options de visualisation.
# # #     """
# # #     if request.method == 'GET':
# # #         return redirect(url_for('show_visualizations_options'))

# # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # #         return redirect(url_for('index'))

# # #     variable_select = request.form.get('variable_select_comparative')
# # #     periode_select = request.form.get('periode_select_comparative')

# # #     if not variable_select or not periode_select:
# # #         return redirect(url_for('show_visualizations_options'))

# # #     try:
# # #         fig = generer_graphique_comparatif(
# # #             GLOBAL_PROCESSED_DATA_DF,
# # #             variable_select,
# # #             periode_select,
# # #             PALETTE_DEFAUT, # Utilisez PALETTE_DEFAUT pour les couleurs
# # #             METADATA_VARIABLES
# # #         )

# # #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # #             meta = METADATA_VARIABLES.get(variable_select, {})
# # #             plot_title = f"Comparaison de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) entre stations ({periode_select})"
# # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # #         else:
# # #             return redirect(url_for('show_visualizations_options'))
# # #     except TypeError as e:
# # #         print(f"DEBUG: Erreur de type dans generate_comparative_plot: {e}")
# # #         traceback.print_exc()
# # #         return redirect(url_for('show_visualizations_options'))
# # #     except Exception as e:
# # #         print(f"DEBUG: Erreur lors de la génération du graphique comparatif: {e}")
# # #         traceback.print_exc()
# # #         return redirect(url_for('show_visualizations_options'))

# # # @app.route('/generate_multi_variable_plot', methods=['GET', 'POST'])
# # # def generate_multi_variable_plot_route():
# # #     """
# # #     Génère et affiche un graphique d'évolution multi-variables normalisé pour une station.
# # #     Si la méthode est GET, redirige vers les options de visualisation.
# # #     """
# # #     if request.method == 'GET':
# # #         return redirect(url_for('show_visualizations_options'))

# # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # #         return redirect(url_for('index'))

# # #     station_select = request.form.get('station_select_multi_var')

# # #     if not station_select:
# # #         return redirect(url_for('show_visualizations_options'))

# # #     try:
# # #         fig = generate_multi_variable_station_plot(
# # #             GLOBAL_PROCESSED_DATA_DF,
# # #             station_select,
# # #             PALETTE_DEFAUT, # Utilisez PALETTE_DEFAUT pour les couleurs
# # #             METADATA_VARIABLES
# # #         )

# # #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# # #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# # #             plot_title = f"Évolution Normalisée des Variables Météorologiques pour la station {station_select}"
# # #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# # #         else:
# # #             return redirect(url_for('show_visualizations_options'))
# # #     except TypeError as e:
# # #         print(f"DEBUG: Erreur de type dans generate_multi_variable_plot_route: {e}")
# # #         traceback.print_exc()
# # #         return redirect(url_for('show_visualizations_options'))
# # #     except Exception as e:
# # #         print(f"DEBUG: Erreur lors de la génération du graphique multi-variables: {e}")
# # #         traceback.print_exc()
# # #         return redirect(url_for('show_visualizations_options'))

# # # @app.route('/get_daily_stats_summary_table', methods=['GET'])
# # # def get_daily_stats_summary_table():
# # #     """
# # #     Route AJAX pour obtenir les statistiques journalières récapitulatives.
# # #     """
# # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # #         return jsonify({'error': 'Aucune donnée traitée pour les statistiques journalières.'}), 404
    
# # #     try:
# # #         summary_table = calculate_daily_summary_table(GLOBAL_PROCESSED_DATA_DF)
# # #         # Convertir les Timestamps en chaînes pour la sérialisation JSON
# # #         for col in summary_table.columns:
# # #             if pd.api.types.is_datetime64_any_dtype(summary_table[col]):
# # #                 summary_table[col] = summary_table[col].dt.strftime('%Y-%m-%d %H:%M:%S')
# # #             elif summary_table[col].dtype == 'object' and summary_table[col].apply(lambda x: isinstance(x, (datetime, pd.Timestamp))).any():
# # #                  summary_table[col] = summary_table[col].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, (datetime, pd.Timestamp)) else x)


# # #         return jsonify(summary_table.to_dict(orient='records'))
# # #     except Exception as e:
# # #         print(f"Erreur lors de la génération du tableau de statistiques journalières: {e}")
# # #         traceback.print_exc()
# # #         return jsonify({'error': f"Erreur lors de la génération du tableau de statistiques journalières: {str(e)}"}), 500


# # # @app.route('/get_variable_summary_plot/<station_name>/<path:variable_name>', methods=['GET'])
# # # def get_variable_summary_plot(station_name, variable_name):
# # #     """
# # #     Route AJAX pour obtenir l'image base64 d'un graphique de statistiques détaillées
# # #     pour une station et une variable données.
# # #     """
# # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # #         return jsonify({'error': 'Aucune donnée disponible.'}), 404

# # #     try:
# # #         fig = generate_variable_summary_plots_for_web(
# # #             GLOBAL_PROCESSED_DATA_DF,
# # #             station_name,
# # #             variable_name,
# # #             METADATA_VARIABLES, # Use global metadata
# # #             PALETTE_DEFAUT # Use global palette
# # #         )
        
# # #         if fig:
# # #             img_buffer = io.BytesIO()
# # #             fig.savefig(img_buffer, format='png', bbox_inches='tight')
# # #             img_buffer.seek(0)
# # #             encoded_image = base64.b64encode(img_buffer.read()).decode('utf-8')
# # #             plt.close(fig) # Close the figure to free memory

# # #             return jsonify({'image_data': encoded_image, 'mime_type': 'image/png'})
# # #         else:
# # #             return jsonify({'error': f'Graphique vide pour {variable_name} à {station_name}.'}), 404

# # #     except Exception as e:
# # #         print(f"Erreur lors de la génération du graphique récapitulatif (API): {e}")
# # #         traceback.print_exc()
# # #         return jsonify({'error': f'Erreur lors de la génération du graphique récapitulatif: {e}'}), 500

# # # # New route to serve the detailed summary plots page
# # # @app.route('/show_detailed_variable_stats', methods=['POST'])
# # # def show_detailed_variable_stats():
# # #     if GLOBAL_PROCESSED_DATA_DF is None:
# # #         return redirect(url_for('index'))

# # #     station_select = request.form.get('station_select_summary_plots')
# # #     if not station_select:
# # #         return redirect(url_for('show_visualizations_options'))

# # #     # Prepare list of variables to pass to the JS template
# # #     # Ensure 'Rain_mm' is first, then others sorted alphabetically
# # #     all_numeric_variables = sorted([
# # #         v for v in METADATA_VARIABLES.keys() if v in GLOBAL_PROCESSED_DATA_DF.columns and pd.api.types.is_numeric_dtype(GLOBAL_PROCESSED_DATA_DF[v])
# # #     ])
# # #     ordered_variables = ['Rain_mm'] + [v for v in all_numeric_variables if v != 'Rain_mm']
    
# # #     return render_template(
# # #         'detailed_variable_stats.html',
# # #         station_name=station_select,
# # #         ordered_variables=ordered_variables,
# # #         metadata=METADATA_VARIABLES # Pass metadata for display names
# # #     )


# # # @app.route('/reset_data', methods=['GET', 'POST'])
# # # def reset_data():
# # #     """a
# # #     Réinitialise les données globales traitées et redirige vers la page d'accueil.
# # #     Si la méthode est GET, redirige simplement vers la page d'accueil.
# # #     """
# # #     if request.method == 'GET':
# # #         return redirect(url_for('index'))

# # #     global GLOBAL_PROCESSED_DATA_DF
# # #     GLOBAL_PROCESSED_DATA_DF = None
# # #     # Vous devriez également gérer la réinitialisation des variables de session si elles sont utilisées
# # #     # par exemple, session.pop('available_stations', None)
# # #     return redirect(url_for('index'))


# # # if __name__ == '__main__':
# # #     app.run(debug=True)

# # import pandas as pd
# # from flask import Flask, render_template, request, jsonify, redirect, url_for
# # from werkzeug.utils import secure_filename
# # import os
# # import io
# # import base64
# # from datetime import datetime
# # import json
# # import plotly.graph_objects as go
# # import matplotlib.pyplot as plt # Importation de Matplotlib
# # import seaborn as sns # Importation de Seaborn
# # import traceback # Importation de traceback pour les messages d'erreur détaillés
# # import re # Importation du module re pour les expressions régulières

# # # Importation des fonctions de traitement de données
# # from data_processing import (
# #     create_datetime, interpolation, _load_and_prepare_gps_data,
# #     gestion_doublons, traiter_outliers_meteo, create_rain_mm,
# #     generer_graphique_par_variable_et_periode, generer_graphique_comparatif,
# #     generate_multi_variable_station_plot, calculate_daily_summary_table, # Keep calculate_daily_summary_table for the HTML table
# #     generate_variable_summary_plots_for_web # Renamed function
# # )

# # # Importation des configurations globales
# # from config import METADATA_VARIABLES, PALETTE_DEFAUT, DATA_LIMITS, STATIONS_BY_BASSIN

# # app = Flask(__name__)
# # app.config['UPLOAD_FOLDER'] = 'uploads'
# # app.config['MAX_CONTENT_LENGTH'] = 64 * 1024 * 1024 # 64 MB max-upload size (Increased from 16 MB)
# # os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# # # Variable globale pour stocker les données du DataFrame après traitement
# # GLOBAL_PROCESSED_DATA_DF = None
# # GLOBAL_DF_GPS_INFO = None # Variable globale pour les données GPS

# # # Chemin vers le fichier JSON des coordonnées de station
# # STATION_COORDS_PATH = os.path.join('data', 'station_coordinates.json')

# # # Initialisation de GLOBAL_DF_GPS_INFO au démarrage de l'application
# # # Assurez-vous que le dossier 'data' existe
# # data_dir_for_gps = 'data'
# # os.makedirs(data_dir_for_gps, exist_ok=True)

# # try:
# #     if not os.path.exists(STATION_COORDS_PATH) or os.stat(STATION_COORDS_PATH).st_size == 0:
# #         print(f"DEBUG: Le fichier '{STATION_COORDS_PATH}' n'existe pas ou est vide. Exécution de data_processing._load_and_prepare_gps_data()...")
# #         GLOBAL_DF_GPS_INFO = _load_and_prepare_gps_data()
# #         if not GLOBAL_DF_GPS_INFO.empty:
# #             GLOBAL_DF_GPS_INFO.to_json(STATION_COORDS_PATH, orient='records', indent=4)
# #             print(f"DEBUG: Données GPS des stations sauvegardées dans '{STATION_COORDS_PATH}'.")
# #         else:
# #             print("DEBUG: Avertissement: Les données GPS sont vides après la préparation, le fichier JSON ne sera pas créé ou sera vide.")
# #     else:
# #         GLOBAL_DF_GPS_INFO = pd.read_json(STATION_COORDS_PATH, orient='records')
# #         print(f"DEBUG: Données GPS des stations chargées depuis '{STATION_COORDS_PATH}'.")
    
# #     if GLOBAL_DF_GPS_INFO.empty:
# #         raise ValueError("Le DataFrame des données GPS est vide après le chargement/la préparation.")
    
# #     print("DEBUG: Préparation des données de coordonnées des stations terminée.")
# # except Exception as e:
# #     print(f"DEBUG: Erreur fatale lors du chargement ou de la préparation des données GPS des stations au démarrage: {e}. "
# #           "L'application pourrait ne pas fonctionner correctement sans ces données.")
# #     GLOBAL_DF_GPS_INFO = pd.DataFrame({
# #         'Station': [],
# #         'Lat': [],
# #         'Long': [],
# #         'Timezone': []
# #     })
# #     traceback.print_exc()


# # @app.route('/')
# # def index():
# #     """
# #     Route principale affichant le formulaire d'upload de fichiers.
# #     """
# #     # Utiliser STATIONS_BY_BASSIN importé de config.py
# #     return render_template('index.html', stations_by_bassin=STATIONS_BY_BASSIN)


# # @app.route('/upload', methods=['GET', 'POST'])
# # def upload_file():
# #     """
# #     Gère l'upload et le traitement des fichiers CSV/Excel.
# #     Permet l'upload de multiples fichiers, chacun associé à un bassin et une station,
# #     en vérifiant leur compatibilité et en les ajoutant au DataFrame global.
# #     Si la méthode est GET, redirige vers la page d'accueil.
# #     """
# #     if request.method == 'GET':
# #         return redirect(url_for('index'))

# #     global GLOBAL_PROCESSED_DATA_DF
# #     global GLOBAL_DF_GPS_INFO # Déclaration pour utiliser la variable globale

# #     upload_groups = []
    
# #     # Collecter tous les indices de groupe présents dans le formulaire
# #     all_input_indices = set()
# #     for key in request.form.keys():
# #         match = re.search(r'_((\d+))$', key)
# #         if match:
# #             all_input_indices.add(int(match.group(1)))
# #     for key in request.files.keys():
# #         match = re.search(r'_(\d+)$', key)
# #         if match:
# #             all_input_indices.add(int(match.group(1)))
            
# #     sorted_indices = sorted(list(all_input_indices))

# #     if not sorted_indices:
# #         # Aucun dataset n'a été soumis
# #         print("DEBUG: Aucun groupe d'upload détecté.")
# #         return redirect(url_for('index')) # Redirection pour rester sur la page d'upload

# #     print(f"DEBUG (upload_file): Nombre de groupes d'upload détectés: {len(sorted_indices)}")
# #     for index in sorted_indices:
# #         bassin_name = request.form.get(f'bassin_{index}')
# #         station_name = request.form.get(f'station_{index}')
# #         file_obj = request.files.get(f'file_input_{index}') 

# #         if not bassin_name or not station_name or not file_obj or file_obj.filename == '':
# #             # Un groupe est incomplet, gérer l'erreur ou ignorer
# #             print(f"DEBUG: Groupe d'upload {index} incomplet. Ignoré.")
# #             continue # Ignorer ce groupe incomplet et passer au suivant

# #         upload_groups.append({
# #             'bassin': bassin_name,
# #             'station': station_name,
# #             'file': file_obj,
# #             'index': index
# #         })
    
# #     if not upload_groups:
# #         # Aucun dataset valide n'a été trouvé pour le traitement
# #         print("DEBUG: Aucun groupe d'upload valide trouvé après filtrage.")
# #         return redirect(url_for('index')) # Redirection si aucun groupe n'est valide


# #     processed_dataframes_for_batch = []
    
# #     expected_raw_data_columns_for_comparison = None
# #     expected_raw_time_columns_for_comparison = None 

    
# #     for group_info in upload_groups:
# #         file = group_info['file']
# #         bassin = group_info['bassin']
# #         station = group_info['station']

# #         file_extension = os.path.splitext(file.filename)[1].lower()
# #         df_temp = None

# #         try:
# #             if file_extension == '.csv':
# #                 df_temp = pd.read_csv(io.BytesIO(file.read()))
# #             elif file_extension in ['.xls', '.xlsx']:
# #                 df_temp = pd.read_excel(io.BytesIO(file.read()))
# #             else:
# #                 print(f"DEBUG: Type de fichier non supporté pour '{file.filename}'.")
# #                 continue
            
# #             if df_temp is not None:
# #                 current_file_columns = df_temp.columns.tolist()
                
# #                 current_raw_time_cols = []
# #                 if 'Date' in current_file_columns:
# #                     current_raw_time_cols.append('Date')
# #                 if all(col in current_file_columns for col in ['Year', 'Month', 'Day', 'Hour', 'Minute']):
# #                     current_raw_time_cols.extend(['Year', 'Month', 'Day', 'Hour', 'Minute'])
# #                 current_raw_time_cols_sorted = sorted(list(set(current_raw_time_cols)))

# #                 current_raw_data_cols_sorted = sorted([
# #                     col for col in current_file_columns 
# #                     if col != 'Station' and col not in ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
# #                 ])

# #                 if expected_raw_data_columns_for_comparison is None:
# #                     expected_raw_data_columns_for_comparison = current_raw_data_cols_sorted
# #                     expected_raw_time_columns_for_comparison = current_raw_time_cols_sorted
# #                 else:
# #                     if current_raw_data_cols_sorted != expected_raw_data_columns_for_comparison:
# #                         print(f"DEBUG: Mismatch de colonnes de données pour '{file.filename}'. Attendu: {expected_raw_data_columns_for_comparison}, Trouvé: {current_raw_data_cols_sorted}")
# #                         return redirect(url_for('index'))
                    
# #                     if current_raw_time_cols_sorted != expected_raw_time_columns_for_comparison:
# #                         print(f"DEBUG: Mismatch de colonnes temporelles pour '{file.filename}'. Attendu: {expected_raw_time_columns_for_comparison}, Trouvé: {current_raw_time_cols_sorted}")
# #                         return redirect(url_for('index'))
                
# #                 df_temp['Station'] = station
# #                 print(f"DEBUG (upload_file): Fichier '{file.filename}' - Station assignée: '{station}'. Premières 5 lignes du DF temporaire:\n{df_temp.head(5)}")
# #                 processed_dataframes_for_batch.append(df_temp)

# #         except Exception as e:
# #             print(f"DEBUG (upload_file): Erreur lors de la lecture ou du traitement du fichier '{file.filename}': {e}")
# #             traceback.print_exc()
# #             return redirect(url_for('index'))

# #     if not processed_dataframes_for_batch:
# #         print("DEBUG: La liste des DataFrames traités pour le batch est vide.")
# #         return redirect(url_for('index'))

# #     try:
# #         # Concaténation de tous les DataFrames du batch
# #         df_current_batch_raw_merged = pd.concat(processed_dataframes_for_batch, ignore_index=True)
# #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_raw_merged APRÈS CONCATÉNATION DU BATCH:\n{df_current_batch_raw_merged['Station'].unique().tolist()}")
        
# #         # Étape 1: Créer la colonne Datetime AVANT la gestion des doublons
# #         df_current_batch_with_datetime = create_datetime(df_current_batch_raw_merged, bassin=upload_groups[0]['bassin'])
# #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_with_datetime APRÈS CREATE_DATETIME DU BATCH:\n{df_current_batch_with_datetime['Station'].unique().tolist()}")

# #         # Étape 2: Gérer les doublons maintenant que 'Datetime' est une colonne
# #         df_current_batch_cleaned = gestion_doublons(df_current_batch_with_datetime)
# #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_cleaned APRÈS GESTION DOUBLONS DU BATCH:\n{df_current_batch_cleaned['Station'].unique().tolist()}")

# #         if 'Datetime' not in df_current_batch_cleaned.columns or df_current_batch_cleaned['Datetime'].isnull().all():
# #             print("DEBUG: La colonne 'Datetime' est manquante ou vide après le nettoyage.")
# #             return redirect(url_for('index'))
        
# #         # Assurez-vous que 'Datetime' est bien de type datetime et nettoyez les NaT
# #         df_current_batch_cleaned['Datetime'] = pd.to_datetime(df_current_batch_cleaned['Datetime'], errors='coerce')
# #         df_current_batch_cleaned.dropna(subset=['Datetime'], inplace=True) 

# #         if df_current_batch_cleaned.empty:
# #             print("DEBUG: Le DataFrame est vide après le nettoyage des dates invalides.")
# #             return redirect(url_for('index'))

# #         # Définir Datetime comme index
# #         df_current_batch_processed = df_current_batch_cleaned.set_index('Datetime').sort_index()
# #         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_processed APRÈS SET_INDEX ET SORT_INDEX DU BATCH: {df_current_batch_processed['Station'].unique().tolist()}")


# #         if GLOBAL_PROCESSED_DATA_DF is None:
# #             GLOBAL_PROCESSED_DATA_DF = df_current_batch_processed
# #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF initialisé. Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
# #         else:
# #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF EXISTANT AVANT FUSION: Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
            
# #             new_batch_stations = df_current_batch_processed['Station'].unique().tolist()
# #             print(f"DEBUG (upload_file): Stations dans le batch actuel à fusionner: {new_batch_stations}")

# #             # Filtrer les données existantes pour les stations du lot actuel du DataFrame global
# #             df_global_filtered = GLOBAL_PROCESSED_DATA_DF[~GLOBAL_PROCESSED_DATA_DF['Station'].isin(new_batch_stations)].copy()
# #             print(f"DEBUG (upload_file): Stations dans GLOBAL_PROCESSED_DATA_DF après filtrage des stations du batch actuel: {df_global_filtered['Station'].unique().tolist() if not df_global_filtered.empty else 'DataFrame global vide après filtrage'}")

# #             # Concaténer le DataFrame global filtré avec les données du nouveau lot
# #             # Assurez-vous que les deux DataFrames ont des index compatibles (DatetimeIndex)
# #             combined_df = pd.concat([df_global_filtered, df_current_batch_processed], ignore_index=False) # Important: ignore_index=False pour préserver les DatetimeIndex et les aligner
# #             print(f"DEBUG (upload_file): Stations uniques dans combined_df APRÈS CONCATINATION GLOBAL_FILTRÉ+BATCH: {combined_df['Station'].unique().tolist()}")

# #             # Pas besoin de to_datetime/dropna/set_index ici si les index sont déjà corrects.
# #             # Cependant, on peut re-trier par sécurité.
# #             GLOBAL_PROCESSED_DATA_DF = combined_df.sort_index()

# #             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF mis à jour. Stations uniques finales: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

# #         # --- VÉRIFICATION FINALE ET DÉFENSIVE DE L'INDEX ET DE LA COLONNE 'STATION' DE GLOBAL_PROCESSED_DATA_DF AVANT TOUT APPEL EXTERNE ---
# #         print(f"DEBUG_STATION_NAMES (upload_file): Type de l'index de GLOBAL_PROCESSED_DATA_DF avant vérification finale: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# #         if 'Station' not in GLOBAL_PROCESSED_DATA_DF.columns:
# #             print("DEBUG_STATION_NAMES (upload_file): ERREUR: La colonne 'Station' est manquante dans GLOBAL_PROCESSED_DATA_DF après traitement.")
# #             return redirect(url_for('index')) # Redirection car données invalides

# #         try:
# #             if not isinstance(GLOBAL_PROCESSED_DATA_DF.index, pd.DatetimeIndex):
# #                 if 'Datetime' in GLOBAL_PROCESSED_DATA_DF.columns:
# #                     GLOBAL_PROCESSED_DATA_DF['Datetime'] = pd.to_datetime(GLOBAL_PROCESSED_DATA_DF['Datetime'], errors='coerce')
# #                     GLOBAL_PROCESSED_DATA_DF.dropna(subset=['Datetime'], inplace=True)
# #                     GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.set_index('Datetime').sort_index()
# #                 else:
# #                     raise TypeError("L'index n'est pas DatetimeIndex et aucune colonne 'Datetime' n'est disponible pour la recréation.")
            
# #             GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.sort_index()

# #             print(f"DEBUG_STATION_NAMES (upload_file): Index de GLOBAL_PROCESSED_DATA_DF VALIDÉ. Stations uniques finales après vérification finale: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

# #         except Exception as e:
# #             print(f"DEBUG_STATION_NAMES (upload_file): Erreur critique finale: Impossible de garantir un DatetimeIndex pour l'interpolation. Détails: {e}")
# #             traceback.print_exc()
# #             return redirect(url_for('index'))
        
# #         if GLOBAL_PROCESSED_DATA_DF.empty:
# #             print("DEBUG_STATION_NAMES (upload_file): GLOBAL_PROCESSED_DATA_DF est vide après la vérification finale.")
# #             return redirect(url_for('index'))


# #         print(f"DEBUG (upload_file): Type de l'index de GLOBAL_PROCESSED_DATA_DF AVANT interpolation: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
# #         print(f"DEBUG (upload_file): Dtypes de GLOBAL_PROCESSED_DATA_DF (après vérif index): \n{GLOBAL_PROCESSED_DATA_DF.dtypes}")


# #         # Traitement d'interpolation et des outliers sur le DataFrame global unifié
# #         # Ces fonctions attendent un DatetimeIndex
# #         # S'assurer que GLOBAL_DF_GPS_INFO est valide avant de le passer
# #         if GLOBAL_DF_GPS_INFO is None or GLOBAL_DF_GPS_INFO.empty:
# #             print("DEBUG: GLOBAL_DF_GPS_INFO est vide ou None. Tente de le recharger/préparer.")
# #             try:
# #                 # Tente de recharger ou de générer les données GPS ici si elles sont manquantes
# #                 # Cela peut arriver si le chargement initial a échoué.
# #                 GLOBAL_DF_GPS_INFO = _load_and_prepare_gps_data()
# #                 if GLOBAL_DF_GPS_INFO.empty:
# #                     raise ValueError("Re-chargement des données GPS échoué, DataFrame vide.")
# #             except Exception as e:
# #                 print(f"DEBUG: Erreur lors du rechargement des données GPS durant l'upload: {e}")
# #                 traceback.print_exc()
# #                 # Créer un DataFrame GPS vide pour éviter les erreurs plus tard
# #                 GLOBAL_DF_GPS_INFO = pd.DataFrame({'Station': [], 'Lat': [], 'Long': [], 'Timezone': []})


# #         GLOBAL_PROCESSED_DATA_DF = interpolation(GLOBAL_PROCESSED_DATA_DF, DATA_LIMITS, GLOBAL_DF_GPS_INFO)
# #         GLOBAL_PROCESSED_DATA_DF = traiter_outliers_meteo(GLOBAL_PROCESSED_DATA_DF, DATA_LIMITS)
        
# #         # Nettoyage des colonnes temporaires
# #         cols_to_drop_after_process = [
# #             'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date',
# #             'sunrise_time_local', 'sunset_time_local'
# #         ]
# #         GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.drop(columns=cols_to_drop_after_process, errors='ignore')

# #         print(f"DEBUG_STATION_NAMES (upload_file - FIN): GLOBAL_PROCESSED_DATA_DF FINAL après interpolation et outliers. Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
        
# #         # Pour cet exemple, nous allons juste rediriger vers la page d'options
# #         return redirect(url_for('show_visualizations_options'))

# #     except Exception as e:
# #         print(f"DEBUG: Une erreur inattendue s'est produite lors du traitement des données: {e}")
# #         traceback.print_exc() # Ajout du traceback en cas d'erreur inattendue
# #         return redirect(url_for('index'))

# # @app.route('/visualizations_options')
# # def show_visualizations_options():
# #     """
# #     Affiche la page des options de visualisation après le traitement des données.
# #     """
# #     if GLOBAL_PROCESSED_DATA_DF is None:
# #         print("DEBUG_STATION_NAMES (show_visualizations_options): GLOBAL_PROCESSED_DATA_DF est None. Redirection vers l'index.")
# #         return redirect(url_for('index'))

# #     # DEBUG: Vérifier les stations avant de les passer au template
# #     # Assurez-vous que la colonne 'Station' existe avant d'essayer d'y accéder
# #     if 'Station' not in GLOBAL_PROCESSED_DATA_DF.columns:
# #         print("DEBUG_STATION_NAMES (show_visualizations_options): ERREUR: La colonne 'Station' est manquante dans GLOBAL_PROCESSED_DATA_DF lors de l l'affichage des options.")
# #         return redirect(url_for('index')) # Ou afficher une erreur appropriée
    
# #     stations_in_df = GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()
# #     print(f"DEBUG_STATION_NAMES (show_visualizations_options): Stations détectées dans GLOBAL_PROCESSED_DATA_DF (pour le template): {stations_in_df}")
    
# #     daily_stats_df = calculate_daily_summary_table(GLOBAL_PROCESSED_DATA_DF)
    
# #     print(f"DEBUG (app.py/visualizations_options): Stations présentes dans daily_stats_df: {daily_stats_df['Station'].unique().tolist() if not daily_stats_df.empty else 'DataFrame vide'}")

# #     initial_data_html = None
# #     data_summary = f"Lignes: {len(GLOBAL_PROCESSED_DATA_DF)}, Colonnes: {len(GLOBAL_PROCESSED_DATA_DF.columns)}"
    
# #     unique_stations_count = len(stations_in_df) # Utiliser la liste déjà extraite

# #     if unique_stations_count > 1:
# #         top_n_data = GLOBAL_PROCESSED_DATA_DF.groupby('Station').head(10)
# #         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
# #     else:
# #         top_n_data = GLOBAL_PROCESSED_DATA_DF.head(20)
# #         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
    
# #     if initial_data_html is None:
# #         initial_data_html = "<p class='text-center text-red-500'>Aucune donnée à afficher pour la prévisualisation.</p>"


# #     daily_stats_html = daily_stats_df.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=False, border=0) if not daily_stats_df.empty else None

# #     # Récupérer les informations pour les menus déroulants dynamiquement
# #     stations = sorted(stations_in_df) # Utiliser la variable déjà vérifiée
# #     variables = sorted([col for col in GLOBAL_PROCESSED_DATA_DF.columns if col in METADATA_VARIABLES.keys()])
# #     can_compare_stations = len(stations) >= 2
    
# #     daily_stat_variables_available = [
# #         var for var in METADATA_VARIABLES.keys() if var in GLOBAL_PROCESSED_DATA_DF.columns and
# #         pd.api.types.is_numeric_dtype(GLOBAL_PROCESSED_DATA_DF[var])
# #     ]
# #     daily_stat_variables = sorted(daily_stat_variables_available)

# #     periodes = ['Brute', 'Journalière', 'Hebdomadaire', 'Mensuelle', 'Annuelle']

# #     # For the new detailed stats plots, we need to pass all available variables
# #     # and the list of stations. The JS will then handle the AJAX calls.
# #     all_numeric_variables = sorted([v for v in METADATA_VARIABLES.keys() if v in GLOBAL_PROCESSED_DATA_DF.columns and pd.api.types.is_numeric_dtype(GLOBAL_PROCESSED_DATA_DF[v])])
    
# #     return render_template(
# #         'visualizations_options.html',
# #         initial_data_html=initial_data_html,
# #         data_summary=data_summary,
# #         daily_stats_html=daily_stats_html,
# #         stations=stations,
# #         variables=variables,
# #         periodes=periodes,
# #         can_compare_stations=can_compare_stations,
# #         METADATA_VARIABLES=METADATA_VARIABLES,
# #         daily_stat_variables=daily_stat_variables, # This is for the previous daily stats table/plot if still used.
# #         stations_for_summary_plots=stations, # Use the same 'stations' list for consistency in dropdown
# #         all_available_variables_for_summary=all_numeric_variables # Pass to JS via template
# #     )

# # @app.route('/generate_single_plot', methods=['GET', 'POST'])
# # def generate_single_plot():
# #     """
# #     Génère et affiche un graphique pour une seule variable et une seule station.
# #     Si la méthode est GET, redirige vers les options de visualisation.
# #     """
# #     if request.method == 'GET':
# #         return redirect(url_for('show_visualizations_options'))

# #     if GLOBAL_PROCESSED_DATA_DF is None:
# #         return redirect(url_for('index'))

# #     station_select = request.form.get('station_select_single')
# #     variable_select = request.form.get('variable_select_single')
# #     periode_select = request.form.get('periode_select_single')

# #     if not station_select or not variable_select or not periode_select:
# #         return redirect(url_for('show_visualizations_options'))

# #     try:
# #         fig = generer_graphique_par_variable_et_periode(
# #             GLOBAL_PROCESSED_DATA_DF,
# #             station_select,
# #             variable_select,
# #             periode_select,
# #             PALETTE_DEFAUT, # Utilisez PALETTE_DEFAUT pour les couleurs
# #             METADATA_VARIABLES
# #         )

# #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# #             meta = METADATA_VARIABLES.get(variable_select, {})
# #             plot_title = f"Évolution de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) pour la station {station_select} ({periode_select})"
# #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# #         else:
# #             return redirect(url_for('show_visualizations_options'))
# #     except TypeError as e:
# #         print(f"DEBUG: Erreur de type dans generate_single_plot: {e}")
# #         traceback.print_exc()
# #         return redirect(url_for('show_visualizations_options'))
# #     except Exception as e:
# #         print(f"DEBUG: Erreur lors de la génération du graphique: {e}")
# #         traceback.print_exc()
# #         return redirect(url_for('show_visualizations_options'))

# # @app.route('/generate_comparative_plot', methods=['GET', 'POST'])
# # def generate_comparative_plot():
# #     """
# #     Génère et affiche un graphique comparatif pour une variable à travers toutes les stations.
# #     Si la méthode est GET, redirige vers les options de visualisation.
# #     """
# #     if request.method == 'GET':
# #         return redirect(url_for('show_visualizations_options'))

# #     if GLOBAL_PROCESSED_DATA_DF is None:
# #         return redirect(url_for('index'))

# #     variable_select = request.form.get('variable_select_comparative')
# #     periode_select = request.form.get('periode_select_comparative')

# #     if not variable_select or not periode_select:
# #         return redirect(url_for('show_visualizations_options'))

# #     try:
# #         fig = generer_graphique_comparatif(
# #             GLOBAL_PROCESSED_DATA_DF,
# #             variable_select,
# #             periode_select,
# #             PALETTE_DEFAUT, # Utilisez PALETTE_DEFAUT pour les couleurs
# #             METADATA_VARIABLES
# #         )

# #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# #             meta = METADATA_VARIABLES.get(variable_select, {})
# #             plot_title = f"Comparaison de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) entre stations ({periode_select})"
# #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# #         else:
# #             return redirect(url_for('show_visualizations_options'))
# #     except TypeError as e:
# #         print(f"DEBUG: Erreur de type dans generate_comparative_plot: {e}")
# #         traceback.print_exc()
# #         return redirect(url_for('show_visualizations_options'))
# #     except Exception as e:
# #         print(f"DEBUG: Erreur lors de la génération du graphique comparatif: {e}")
# #         traceback.print_exc()
# #         return redirect(url_for('show_visualizations_options'))

# # @app.route('/generate_multi_variable_plot', methods=['GET', 'POST'])
# # def generate_multi_variable_plot_route():
# #     """
# #     Génère et affiche un graphique d'évolution multi-variables normalisé pour une station.
# #     Si la méthode est GET, redirige vers les options de visualisation.
# #     """
# #     if request.method == 'GET':
# #         return redirect(url_for('show_visualizations_options'))

# #     if GLOBAL_PROCESSED_DATA_DF is None:
# #         return redirect(url_for('index'))

# #     station_select = request.form.get('station_select_multi_var')

# #     if not station_select:
# #         return redirect(url_for('show_visualizations_options'))

# #     try:
# #         fig = generate_multi_variable_station_plot(
# #             GLOBAL_PROCESSED_DATA_DF,
# #             station_select,
# #             PALETTE_DEFAUT, # Utilisez PALETTE_DEFAUT pour les couleurs
# #             METADATA_VARIABLES
# #         )

# #         if fig and fig.data: # Vérifier si la figure a été générée et contient des traces
# #             plot_json_data = fig.to_json() # Convertir la figure en JSON
# #             plot_title = f"Évolution Normalisée des Variables Météorologiques pour la station {station_select}"
# #             return render_template('plot_display.html', plot_json_data=plot_json_data, plot_title=plot_title)
# #         else:
# #             return redirect(url_for('show_visualizations_options'))
# #     except TypeError as e:
# #         print(f"DEBUG: Erreur de type dans generate_multi_variable_plot_route: {e}")
# #         traceback.print_exc()
# #         return redirect(url_for('show_visualizations_options'))
# #     except Exception as e:
# #         print(f"DEBUG: Erreur lors de la génération du graphique multi-variables: {e}")
# #         traceback.print_exc()
# #         return redirect(url_for('show_visualizations_options'))

# # @app.route('/get_daily_stats_summary_table', methods=['GET'])
# # def get_daily_stats_summary_table():
# #     """
# #     Route AJAX pour obtenir les statistiques journalières récapitulatives.
# #     """
# #     if GLOBAL_PROCESSED_DATA_DF is None:
# #         return jsonify({'error': 'Aucune donnée traitée pour les statistiques journalières.'}), 404
    
# #     try:
# #         summary_table = calculate_daily_summary_table(GLOBAL_PROCESSED_DATA_DF)
# #         # Convertir les Timestamps en chaînes pour la sérialisation JSON
# #         for col in summary_table.columns:
# #             if pd.api.types.is_datetime64_any_dtype(summary_table[col]):
# #                 summary_table[col] = summary_table[col].dt.strftime('%Y-%m-%d %H:%M:%S')
# #             elif summary_table[col].dtype == 'object' and summary_table[col].apply(lambda x: isinstance(x, (datetime, pd.Timestamp))).any():
# #                  summary_table[col] = summary_table[col].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, (datetime, pd.Timestamp)) else x)


# #         return jsonify(summary_table.to_dict(orient='records'))
# #     except Exception as e:
# #         print(f"Erreur lors de la génération du tableau de statistiques journalières: {e}")
# #         traceback.print_exc()
# #         return jsonify({'error': f"Erreur lors de la génération du tableau de statistiques journalières: {str(e)}"}), 500


# # @app.route('/get_variable_summary_plot/<station_name>/<path:variable_name>', methods=['GET'])
# # def get_variable_summary_plot(station_name, variable_name):
# #     """
# #     Route AJAX pour obtenir l'image base64 d'un graphique de statistiques détaillées
# #     pour une station et une variable données.
# #     """
# #     if GLOBAL_PROCESSED_DATA_DF is None:
# #         return jsonify({'error': 'Aucune donnée disponible.'}), 404

# #     try:
# #         fig = generate_variable_summary_plots_for_web(
# #             GLOBAL_PROCESSED_DATA_DF,
# #             station_name,
# #             variable_name,
# #             METADATA_VARIABLES, # Use global metadata
# #             PALETTE_DEFAUT # Use global palette
# #         )
        
# #         if fig:
# #             img_buffer = io.BytesIO()
# #             fig.savefig(img_buffer, format='png', bbox_inches='tight')
# #             img_buffer.seek(0)
# #             encoded_image = base64.b64encode(img_buffer.read()).decode('utf-8')
# #             plt.close(fig) # Close the figure to free memory

# #             return jsonify({'image_data': encoded_image, 'mime_type': 'image/png'})
# #         else:
# #             return jsonify({'error': f'Graphique vide pour {variable_name} à {station_name}.'}), 404

# #     except Exception as e:
# #         print(f"Erreur lors de la génération du graphique récapitulatif (API): {e}")
# #         traceback.print_exc()
# #         return jsonify({'error': f'Erreur lors de la génération du graphique récapitulatif: {e}'}), 500

# # # New route to serve the detailed summary plots page
# # @app.route('/show_detailed_variable_stats', methods=['POST'])
# # def show_detailed_variable_stats():
# #     if GLOBAL_PROCESSED_DATA_DF is None:
# #         return redirect(url_for('index'))

# #     station_select = request.form.get('station_select_summary_plots')
# #     if not station_select:
# #         return redirect(url_for('show_visualizations_options'))

# #     # Prepare list of variables to pass to the JS template
# #     # Ensure 'Rain_mm' is first, then others sorted alphabetically
# #     all_numeric_variables = sorted([
# #         v for v in METADATA_VARIABLES.keys() if v in GLOBAL_PROCESSED_DATA_DF.columns and pd.api.types.is_numeric_dtype(GLOBAL_PROCESSED_DATA_DF[v])
# #     ])
# #     ordered_variables = ['Rain_mm'] + [v for v in all_numeric_variables if v != 'Rain_mm']
    
# #     return render_template(
# #         'detailed_variable_stats.html',
# #         station_name=station_select,
# #         ordered_variables=ordered_variables,
# #         metadata=METADATA_VARIABLES # Pass metadata for display names
# #     )


# # @app.route('/reset_data', methods=['GET', 'POST'])
# # def reset_data():
# #     """
# #     Réinitialise les données globales traitées et redirige vers la page d'accueil.
# #     Si la méthode est GET, redirige simplement vers la page d'accueil.
# #     """
# #     if request.method == 'GET':
# #         return redirect(url_for('index'))

# #     global GLOBAL_PROCESSED_DATA_DF
# #     GLOBAL_PROCESSED_DATA_DF = None
# #     # Vous devriez également gérer la réinitialisation des variables de session si elles sont utilisées
# #     # par exemple, session.pop('available_stations', None)
# #     return redirect(url_for('index'))


# # if __name__ == '__main__':
# #     app.run(debug=True)

# #voila pour app.py:


# import pandas as pd
# from flask import Flask, render_template, request, redirect, url_for, flash, session
# import os
# import io
# import re # Importation pour les expressions régulières

# # Importation des fonctions de traitement et de visualisation des données
# from data_processing import (
#     create_datetime,
#     gestion_doublons,
#     interpolation,
#     traiter_outliers_meteo,
#     generer_graphique_par_variable_et_periode,
#     generer_graphique_comparatif,
#     generate_multi_variable_station_plot,
#     daily_stats,
#     _load_and_prepare_gps_data
# )

# # Importation des configurations globales
# import config # Importation du module config entier
# from config import (
#     STATIONS_BY_BASSIN,
#     DATA_LIMITS,
#     CUSTOM_STATION_COLORS,
#     PALETTE_DEFAUT,
#     METADATA_VARIABLES
# )

# # Ajout d'une option pour éviter les avertissements de downcasting de Pandas
# pd.set_option('future.no_silent_downcasting', True)

# app = Flask(__name__)
# app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'une_cle_secrete_par_defaut_tres_forte_pour_dev')

# # Variable globale pour stocker les données du DataFrame après traitement
# GLOBAL_PROCESSED_DATA_DF = None

# # Initialisation de GLOBAL_DF_GPS_INFO au démarrage de l'application
# try:
#     config.GLOBAL_DF_GPS_INFO = _load_and_prepare_gps_data()
# except Exception as e:
#     print(f"Erreur fatale lors du chargement des données GPS des stations au démarrage: {e}. "
#           "L'application pourrait ne pas fonctionner correctement sans ces données.")
#     config.GLOBAL_DF_GPS_INFO = pd.DataFrame({
#         'Station': [],
#         'Lat': [],
#         'Long': [],
#         'Timezone': []
#     })


# @app.route('/')
# def index():
#     """
#     Route principale affichant le formulaire d'upload de fichiers.
#     """
#     return render_template('index.html', stations_by_bassin=STATIONS_BY_BASSIN)

# @app.route('/upload', methods=['GET', 'POST'])
# def upload_file():
#     """
#     Gère l'upload et le traitement des fichiers CSV/Excel.
#     Permet l'upload de multiples fichiers, chacun associé à un bassin et une station,
#     en vérifiant leur compatibilité et en les ajoutant au DataFrame global.
#     Si la méthode est GET, redirige vers la page d'accueil.
#     """
#     if request.method == 'GET':
#         flash('Accès direct à la page d\'upload non autorisé. Veuillez utiliser le formulaire.', 'warning')
#         return redirect(url_for('index'))

#     global GLOBAL_PROCESSED_DATA_DF

#     upload_groups = []

#     # Collecter tous les indices de groupe présents dans le formulaire
#     all_input_indices = set()
#     for key in request.form.keys():
#         match = re.search(r'_((\d+))$', key)
#         if match:
#             all_input_indices.add(int(match.group(1)))
#     for key in request.files.keys():
#         match = re.search(r'_(\d+)$', key)
#         if match:
#             all_input_indices.add(int(match.group(1)))

#     sorted_indices = sorted(list(all_input_indices))

#     if not sorted_indices:
#         flash('Aucun dataset n\'a été soumis. Veuillez ajouter au moins un dataset.', 'error')
#         return redirect(url_for('index'))

#     print(f"DEBUG (upload_file): Nombre de groupes d'upload détectés: {len(sorted_indices)}")
#     for index in sorted_indices:
#         bassin_name = request.form.get(f'bassin_{index}')
#         station_name = request.form.get(f'station_{index}')
#         file_obj = request.files.get(f'file_input_{index}')

#         if not bassin_name or not station_name or not file_obj or file_obj.filename == '':
#             flash(f'Le dataset pour le groupe {index+1} est incomplet (bassin, station ou fichier manquant). Veuillez compléter toutes les informations.', 'error')
#             return redirect(url_for('index'))

#         upload_groups.append({
#             'bassin': bassin_name,
#             'station': station_name,
#             'file': file_obj,
#             'index': index
#         })

#     if not upload_groups:
#         flash('Aucun dataset valide n\'a été trouvé pour le traitement. Veuillez vérifier vos sélections.', 'error')
#         return redirect(url_for('index'))


#     processed_dataframes_for_batch = []

#     expected_raw_data_columns_for_comparison = None
#     expected_raw_time_columns_for_comparison = None


#     for group_info in upload_groups:
#         file = group_info['file']
#         bassin = group_info['bassin']
#         station = group_info['station']

#         file_extension = os.path.splitext(file.filename)[1].lower()
#         df_temp = None

#         try:
#             if file_extension == '.csv':
#                 df_temp = pd.read_csv(io.BytesIO(file.read()))
#             elif file_extension in ['.xls', '.xlsx']:
#                 df_temp = pd.read_excel(io.BytesIO(file.read()))
#             else:
#                 flash(f'Extension de fichier non supportée pour "{file.filename}". Seuls les fichiers CSV ou Excel sont acceptés.', 'error')
#                 return redirect(url_for('index'))

#             if df_temp is not None:
#                 current_file_columns = df_temp.columns.tolist()

#                 current_raw_time_cols = []
#                 if 'Date' in current_file_columns:
#                     current_raw_time_cols.append('Date')
#                 if all(col in current_file_columns for col in ['Year', 'Month', 'Day', 'Hour', 'Minute']):
#                     current_raw_time_cols.extend(['Year', 'Month', 'Day', 'Hour', 'Minute'])
#                 current_raw_time_cols_sorted = sorted(list(set(current_raw_time_cols)))

#                 current_raw_data_cols_sorted = sorted([
#                     col for col in current_file_columns
#                     if col != 'Station' and col not in ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
#                 ])

#                 if expected_raw_data_columns_for_comparison is None:
#                     expected_raw_data_columns_for_comparison = current_raw_data_cols_sorted
#                     expected_raw_time_columns_for_comparison = current_raw_time_cols_sorted
#                 else:
#                     if current_raw_data_cols_sorted != expected_raw_data_columns_for_comparison:
#                         flash(f"Le fichier '{file.filename}' (Station: {station}) a des colonnes de données incompatibles avec les données déjà chargées. "
#                               f"Attendues: {expected_raw_data_columns_for_comparison}, Obtenues: {current_raw_data_cols_sorted}. Fusion annulée.", 'error')
#                         return redirect(url_for('index'))

#                     if current_raw_time_cols_sorted != expected_raw_time_columns_for_comparison:
#                         flash(f"Le fichier '{file.filename}' (Station: {station}) a des colonnes temporelles incompatibles avec les données déjà chargées. "
#                               f"Attendues: {expected_raw_time_columns_for_comparison}, Obtenues: {current_raw_time_cols_sorted}. Fusion annulée.", 'error')
#                         return redirect(url_for('index'))

#                 df_temp['Station'] = station
#                 print(f"DEBUG (upload_file): Fichier '{file.filename}' - Station assignée: '{station}'. Premières 5 lignes du DF temporaire:\n{df_temp.head(5)}")
#                 processed_dataframes_for_batch.append(df_temp)

#         except Exception as e:
#             flash(f'Erreur lors de la lecture ou du traitement du fichier "{file.filename}": {e}', 'error')
#             print(f"DEBUG (upload_file): Erreur lors du traitement du fichier '{file.filename}': {e}")
#             return redirect(url_for('index'))

#     if not processed_dataframes_for_batch:
#         flash('Aucun fichier valide n\'a pu être traité à partir de ce lot après les vérifications de contenu.', 'error')
#         return redirect(url_for('index'))

#     try:
#         # Concaténation de tous les DataFrames du batch
#         df_current_batch_raw_merged = pd.concat(processed_dataframes_for_batch, ignore_index=True)
#         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_raw_merged APRÈS CONCATÉNATION DU BATCH:\n{df_current_batch_raw_merged['Station'].unique().tolist()}")

#         # Étape 1: Créer la colonne Datetime AVANT la gestion des doublons
#         df_current_batch_with_datetime = create_datetime(df_current_batch_raw_merged, bassin=upload_groups[0]['bassin'])
#         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_with_datetime APRÈS CREATE_DATETIME DU BATCH:\n{df_current_batch_with_datetime['Station'].unique().tolist()}")

#         # Étape 2: Gérer les doublons maintenant que 'Datetime' est une colonne
#         df_current_batch_cleaned = gestion_doublons(df_current_batch_with_datetime)
#         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_cleaned APRÈS GESTION DOUBLONS DU BATCH:\n{df_current_batch_cleaned['Station'].unique().tolist()}")

#         if 'Datetime' not in df_current_batch_cleaned.columns or df_current_batch_cleaned['Datetime'].isnull().all():
#             flash("Erreur: La colonne 'Datetime' est manquante ou vide après le nettoyage. Impossible de traiter les données.", 'error')
#             return redirect(url_for('index'))

#         # Assurez-vous que 'Datetime' est bien de type datetime et nettoyez les NaT
#         df_current_batch_cleaned['Datetime'] = pd.to_datetime(df_current_batch_cleaned['Datetime'], errors='coerce')
#         df_current_batch_cleaned.dropna(subset=['Datetime'], inplace=True)

#         if df_current_batch_cleaned.empty:
#             flash("Erreur: Toutes les données du lot actuel ont été supprimées en raison de dates invalides après nettoyage. Traitement annulé.", 'error')
#             return redirect(url_for('index'))

#         # Définir Datetime comme index
#         df_current_batch_processed = df_current_batch_cleaned.set_index('Datetime').sort_index()
#         print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_processed APRÈS SET_INDEX ET SORT_INDEX DU BATCH:\n{df_current_batch_processed['Station'].unique().tolist()}")


#         if GLOBAL_PROCESSED_DATA_DF is None:
#             GLOBAL_PROCESSED_DATA_DF = df_current_batch_processed
#             flash(f'{len(processed_dataframes_for_batch)} fichier(s) téléchargé(s) et initialisé(s).', 'success')
#             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF initialisé. Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
#         else:
#             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF EXISTANT AVANT FUSION: Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

#             new_batch_stations = df_current_batch_processed['Station'].unique().tolist()
#             print(f"DEBUG (upload_file): Stations dans le batch actuel à fusionner: {new_batch_stations}")

#             # Filtrer les données existantes pour les stations du lot actuel du DataFrame global
#             df_global_filtered = GLOBAL_PROCESSED_DATA_DF[~GLOBAL_PROCESSED_DATA_DF['Station'].isin(new_batch_stations)].copy()
#             print(f"DEBUG (upload_file): Stations dans GLOBAL_PROCESSED_DATA_DF après filtrage des stations du batch actuel:\n{df_global_filtered['Station'].unique().tolist() if not df_global_filtered.empty else 'DataFrame global vide après filtrage'}")

#             # Concaténer le DataFrame global filtré avec les données du nouveau lot
#             # Assurez-vous que les deux DataFrames ont des index compatibles (DatetimeIndex)
#             combined_df = pd.concat([df_global_filtered, df_current_batch_processed], ignore_index=False) # Important: ignore_index=False pour préserver les DatetimeIndex et les aligner
#             print(f"DEBUG (upload_file): Stations uniques dans combined_df APRÈS CONCATINATION GLOBAL_FILTRÉ+BATCH:\n{combined_df['Station'].unique().tolist()}")

#             # Pas besoin de to_datetime/dropna/set_index ici si les index sont déjà corrects.
#             # Cependant, on peut re-trier par sécurité.
#             GLOBAL_PROCESSED_DATA_DF = combined_df.sort_index()

#             print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF mis à jour. Stations uniques finales: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

#             flash(f'{len(processed_dataframes_for_batch)} fichier(s) ajouté(s). Total de stations uniques: {len(GLOBAL_PROCESSED_DATA_DF["Station"].unique())}.', 'success')

#         # --- VÉRIFICATION FINALE ET DÉFENSIVE DE L'INDEX DE GLOBAL_PROCESSED_DATA_DF AVANT TOUT APPEL EXTERNE ---
#         print(f"DEBUG: Type de l'index de GLOBAL_PROCESSED_DATA_DF avant vérification finale: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
#         try:
#             # Cette étape est cruciale si l'index pourrait être cassé, mais elle doit être transparente si tout va bien
#             if not isinstance(GLOBAL_PROCESSED_DATA_DF.index, pd.DatetimeIndex):
#                 # Si l'index n'est pas un DatetimeIndex, essayez de le forcer ou de le recréer à partir d'une colonne Datetime
#                 if 'Datetime' in GLOBAL_PROCESSED_DATA_DF.columns:
#                     GLOBAL_PROCESSED_DATA_DF['Datetime'] = pd.to_datetime(GLOBAL_PROCESSED_DATA_DF['Datetime'], errors='coerce')
#                     GLOBAL_PROCESSED_DATA_DF.dropna(subset=['Datetime'], inplace=True)
#                     GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.set_index('Datetime').sort_index()
#                 else:
#                     raise TypeError("L'index n'est pas DatetimeIndex et aucune colonne 'Datetime' n'est disponible pour la recréation.")

#             GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.sort_index() # Re-trier au cas où

#             print("DEBUG: Index de GLOBAL_PROCESSED_DATA_DF VALIDÉ et RE-CONVERTI en DatetimeIndex (vérification finale réussie).")

#         except Exception as e:
#             flash(f"Erreur critique finale: Impossible de garantir un DatetimeIndex pour l'interpolation. Le format des dates dans vos fichiers est incompatible. Détails: {e}", 'error')
#             print(f"DEBUG: Erreur critique finale lors de la conversion de l'index: {e}")
#             return redirect(url_for('index'))

#         if GLOBAL_PROCESSED_DATA_DF.empty:
#             flash("Erreur: Le DataFrame global est vide après le traitement des dates. Il n'y a plus de données valides à analyser.", 'error')
#             return redirect(url_for('index'))


#         print(f"DEBUG: Type de l'index de GLOBAL_PROCESSED_DATA_DF AVANT interpolation: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
#         print(f"DEBUG: Dtypes de GLOBAL_PROCESSED_DATA_DF (après vérif index): \n{GLOBAL_PROCESSED_DATA_DF.dtypes}")


#         # Traitement d'interpolation et des outliers sur le DataFrame global unifié
#         # Ces fonctions attendent un DatetimeIndex
#         GLOBAL_PROCESSED_DATA_DF = interpolation(GLOBAL_PROCESSED_DATA_DF, DATA_LIMITS, config.GLOBAL_DF_GPS_INFO)
#         GLOBAL_PROCESSED_DATA_DF = traiter_outliers_meteo(GLOBAL_PROCESSED_DATA_DF, DATA_LIMITS)

#         # Nettoyage des colonnes temporaires
#         cols_to_drop_after_process = [
#             'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date',
#             'sunrise_time_local', 'sunset_time_local'
#         ]
#         GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.drop(columns=cols_to_drop_after_process, errors='ignore')

#         # Mettre à jour les informations de session pour les options de visualisation
#         session['available_stations'] = sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist())
#         session['available_variables'] = sorted([col for col in GLOBAL_PROCESSED_DATA_DF.columns if col in METADATA_VARIABLES.keys()])
#         session['can_compare_stations'] = len(session['available_stations']) >= 2

#         flash('Données traitées et fusionnées avec succès ! Vous pouvez maintenant visualiser les résultats.', 'success')
#         return redirect(url_for('show_visualizations_options'))

#     except Exception as e:
#         flash(f"Une erreur inattendue s'est produite lors du traitement des données: {e}", 'error')
#         print(f"DEBUG: Erreur inattendue dans /upload: {e}")
#         return redirect(url_for('index'))

# @app.route('/visualizations_options')
# def show_visualizations_options():
#     """
#     Affiche la page des options de visualisation après le traitement des données.
#     """
#     if GLOBAL_PROCESSED_DATA_DF is None:
#         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
#         return redirect(url_for('index'))

#     print(f"DEBUG (app.py/visualizations_options): Stations disponibles dans GLOBAL_PROCESSED_DATA_DF: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

#     daily_stats_df = daily_stats(GLOBAL_PROCESSED_DATA_DF)

#     print(f"DEBUG (app.py/visualizations_options): Stations présentes dans daily_stats_df: {daily_stats_df['Station'].unique().tolist() if not daily_stats_df.empty else 'DataFrame vide'}")

#     initial_data_html = None
#     data_summary = f"Lignes: {len(GLOBAL_PROCESSED_DATA_DF)}, Colonnes: {len(GLOBAL_PROCESSED_DATA_DF.columns)}"

#     unique_stations_count = len(GLOBAL_PROCESSED_DATA_DF['Station'].unique())

#     if unique_stations_count > 1:
#         top_n_data = GLOBAL_PROCESSED_DATA_DF.groupby('Station').head(10)
#         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
#     else:
#         top_n_data = GLOBAL_PROCESSED_DATA_DF.head(20)
#         initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)

#     if initial_data_html is None:
#         initial_data_html = "<p class='text-center text-red-500'>Aucune donnée à afficher pour la prévisualisation.</p>"


#     daily_stats_html = daily_stats_df.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=False, border=0) if not daily_stats_df.empty else None

#     stations = session.get('available_stations', [])
#     variables = session.get('available_variables', [])
#     can_compare_stations = session.get('can_compare_stations', False)

#     periodes = ['Journalière', 'Hebdomadaire', 'Mensuelle', 'Annuelle']

#     return render_template(
#         'visualizations_options.html',
#         initial_data_html=initial_data_html,
#         data_summary=data_summary,
#         daily_stats_html=daily_stats_html,
#         stations=stations,
#         variables=variables,
#         periodes=periodes,
#         can_compare_stations=can_compare_stations,
#         METADATA_VARIABLES=METADATA_VARIABLES
#     )

# @app.route('/generate_single_plot', methods=['GET', 'POST'])
# def generate_single_plot():
#     """
#     Génère et affiche un graphique pour une seule variable et une seule station.
#     Si la méthode est GET, redirige vers les options de visualisation.
#     """
#     if request.method == 'GET':
#         flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
#         return redirect(url_for('show_visualizations_options'))

#     if GLOBAL_PROCESSED_DATA_DF is None:
#         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
#         return redirect(url_for('index'))

#     station_select = request.form.get('station_select_single')
#     variable_select = request.form.get('variable_select_single')
#     periode_select = request.form.get('periode_select_single')

#     if not station_select or not variable_select or not periode_select:
#         flash('Veuillez sélectionner une station, une variable et une période pour le graphique.', 'error')
#         return redirect(url_for('show_visualizations_options'))

#     try:
#         plot_data = generer_graphique_par_variable_et_periode(
#             GLOBAL_PROCESSED_DATA_DF,
#             station_select,
#             variable_select,
#             periode_select,
#             CUSTOM_STATION_COLORS,
#             METADATA_VARIABLES
#         )

#         if plot_data:
#             meta = METADATA_VARIABLES.get(variable_select, {})
#             plot_title = f"Évolution de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) pour la station {station_select} ({periode_select})"
#             return render_template('plot_display.html', plot_data=plot_data, plot_title=plot_title)
#         else:
#             flash("Impossible de générer le graphique. Vérifiez les données ou la sélection.", 'error')
#             return redirect(url_for('show_visualizations_options'))
#     except TypeError as e: # Capture spécifique de TypeError
#         flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
#         print(f"DEBUG: Erreur de type dans generate_single_plot: {e}")
#         return redirect(url_for('show_visualizations_options'))
#     except Exception as e:
#         flash(f"Erreur lors de la génération du graphique: {e}", 'error')
#         return redirect(url_for('show_visualizations_options'))

# @app.route('/generate_comparative_plot', methods=['GET', 'POST'])
# def generate_comparative_plot():
#     """
#     Génère et affiche un graphique comparatif pour une variable à travers toutes les stations.
#     Si la méthode est GET, redirige vers les options de visualisation.
#     """
#     if request.method == 'GET':
#         flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
#         return redirect(url_for('show_visualizations_options'))

#     if GLOBAL_PROCESSED_DATA_DF is None:
#         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
#         return redirect(url_for('index'))

#     variable_select = request.form.get('variable_select_comparative')
#     periode_select = request.form.get('periode_select_comparative')

#     if not variable_select or not periode_select:
#         flash('Veuillez sélectionner une variable et une période pour le graphique comparatif.', 'error')
#         return redirect(url_for('show_visualizations_options'))

#     try:
#         plot_data = generer_graphique_comparatif(
#             GLOBAL_PROCESSED_DATA_DF,
#             variable_select,
#             periode_select,
#             CUSTOM_STATION_COLORS,
#             METADATA_VARIABLES
#         )

#         if plot_data:
#             meta = METADATA_VARIABLES.get(variable_select, {})
#             plot_title = f"Comparaison de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) entre stations ({periode_select})"
#             return render_template('plot_display.html', plot_data=plot_data, plot_title=plot_title)
#         else:
#             flash("Impossible de générer le graphique comparatif. Vérifiez les données ou la sélection.", 'error')
#             return redirect(url_for('show_visualizations_options'))
#     except TypeError as e: # Capture spécifique de TypeError
#         flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
#         print(f"DEBUG: Erreur de type dans generate_comparative_plot: {e}")
#         return redirect(url_for('show_visualizations_options'))
#     except Exception as e:
#         flash(f"Erreur lors de la génération du graphique comparatif: {e}", 'error')
#         return redirect(url_for('show_visualizations_options'))

# @app.route('/generate_multi_variable_plot', methods=['GET', 'POST'])
# def generate_multi_variable_plot_route():
#     """
#     Génère et affiche un graphique d'évolution multi-variables normalisé pour une station.
#     Si la méthode est GET, redirige vers les options de visualisation.
#     """
#     if request.method == 'GET':
#         flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
#         return redirect(url_for('show_visualizations_options'))

#     if GLOBAL_PROCESSED_DATA_DF is None:
#         flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
#         return redirect(url_for('index'))

#     station_select = request.form.get('station_select_multi_var')

#     if not station_select:
#         flash('Veuillez sélectionner une station pour le graphique multi-variables.', 'error')
#         return redirect(url_for('show_visualizations_options'))

#     try:
#         plot_data = generate_multi_variable_station_plot(
#             GLOBAL_PROCESSED_DATA_DF,
#             station_select,
#             PALETTE_DEFAUT,
#             METADATA_VARIABLES
#         )

#         if plot_data:
#             plot_title = f"Évolution Normalisée des Variables Météorologiques pour la station {station_select}"
#             return render_template('plot_display.html', plot_data=plot_data, plot_title=plot_title)
#         else:
#             flash("Impossible de générer le graphique multi-variables. Vérifiez les données ou la sélection.", 'error')
#             return redirect(url_for('show_visualizations_options'))
#     except TypeError as e: # Capture spécifique de TypeError
#         flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
#         print(f"DEBUG: Erreur de type dans generate_multi_variable_plot_route: {e}")
#         return redirect(url_for('show_visualizations_options'))
#     except Exception as e:
#         flash(f"Erreur lors de la génération du graphique multi-variables: {e}", 'error')
#         return redirect(url_for('show_visualizations_options'))

# @app.route('/reset_data', methods=['GET', 'POST'])
# def reset_data():
#     """
#     Réinitialise les données globales traitées et redirige vers la page d'accueil.
#     Si la méthode est GET, redirige simplement vers la page d'accueil.
#     """
#     if request.method == 'GET':
#         return redirect(url_for('index'))

#     global GLOBAL_PROCESSED_DATA_DF
#     GLOBAL_PROCESSED_DATA_DF = None
#     session.pop('available_stations', None)
#     session.pop('available_variables', None)
#     session.pop('can_compare_stations', None)
#     flash('Toutes les données chargées ont été réinitialisées. Vous pouvez commencer une nouvelle analyse.', 'info')
#     return redirect(url_for('index'))


# if __name__ == '__main__':
#     app.run(debug=True)

# #Alors donnes toutes les corrections necessaires sans me demander de completer quoi que ce soit par moi-meme
# import os
# import pandas as pd
# from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
# import plotly.graph_objects as go
# import plotly.express as px
# import json
# import matplotlib.pyplot as plt
# import io
# import base64
# import numpy as np
# import traceback
# from werkzeug.utils import secure_filename


# # Importations des fonctions de traitement
# from data_processing import (
#     create_datetime,
#     interpolation,
#     _load_and_prepare_gps_data,
#     gestion_doublons,
#     calculate_daily_summary_table,
#     generer_graphique_par_variable_et_periode,
#     generer_graphique_comparatif,
#     generate_multi_variable_station_plot,
#     generate_variable_summary_plots_for_web,
#     #preprocess_station_data , # Changé depuis apply_station_specific_preprocessing
#     apply_station_specific_preprocessing
# )

# from config import METADATA_VARIABLES, PALETTE_DEFAUT, DATA_LIMITS, ALLOWED_EXTENSIONS, STATIONS_BY_BASSIN
# #
# #from config import METADATA_VARIABLES, PALETTE_DEFAUT, DATA_LIMITS, ALLOWED_EXTENSIONS, STATIONS_BY_BASSIN

# app = Flask(__name__)
# app.secret_key = 'votre_cle_secrete_ici'
# app.config['MAX_CONTENT_LENGTH'] = 64 * 1024 * 1024
# app.config['UPLOAD_FOLDER'] = 'uploads'



# @app.context_processor
# def inject_globals():
#     return {
#         'METADATA_VARIABLES': METADATA_VARIABLES,
#         'PALETTE_DEFAUT': PALETTE_DEFAUT,
#         'STATIONS_BY_BASSIN': STATIONS_BY_BASSIN
#     }
# # Variables globales
# GLOBAL_PROCESSED_DATA_DF = pd.DataFrame()
# GLOBAL_GPS_DATA_DF = pd.DataFrame()

# # Chargement des données GPS
# with app.app_context():
#     GLOBAL_GPS_DATA_DF = _load_and_prepare_gps_data()

# def allowed_file(filename):
#     return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# @app.route('/')
# def index():
#     os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
#     return render_template('index.html', 
#                          bassins=sorted(STATIONS_BY_BASSIN.keys()),
#                          stations_by_bassin=STATIONS_BY_BASSIN)

# @app.route('/upload', methods=['POST'])
# def upload_file():
#     global GLOBAL_PROCESSED_DATA_DF

#     if not request.files:
#         flash('Aucun fichier reçu', 'error')
#         return redirect(url_for('index'))

#     # Récupération des fichiers et des stations
#     uploaded_files = request.files.getlist('file[]')
#     stations = [request.form.get(f'station_{i}') for i in range(len(uploaded_files)) 
#                if request.form.get(f'station_{i}')]

#     if len(uploaded_files) != len(stations):
#         flash('Nombre de fichiers et de stations incompatible', 'error')
#         return redirect(url_for('index'))

#     df_current_batch_raw_merged = pd.DataFrame()

#     for file, station in zip(uploaded_files, stations):
#         if file and allowed_file(file.filename):
#             try:
#                 # Sauvegarde temporaire du fichier
#                 filename = secure_filename(file.filename)
#                 filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#                 file.save(filepath)

#                 # Lecture du fichier
#                 if filename.endswith('.csv'):
#                     df_temp = pd.read_csv(filepath)
#                 else:
#                     df_temp = pd.read_excel(filepath)

#                 # Prétraitement
#                 df_temp['Station'] = station
#                 df_temp = apply_station_specific_preprocessing(df_temp, station)
#                 df_current_batch_raw_merged = pd.concat([df_current_batch_raw_merged, df_temp], ignore_index=True)

#                 # Suppression du fichier temporaire
#                 os.remove(filepath)

#             except Exception as e:
#                 flash(f"Erreur traitement {file.filename}: {str(e)}", 'error')
#                 continue

#     if df_current_batch_raw_merged.empty:
#         flash('Aucune donnée valide traitée', 'error')
#         return redirect(url_for('index'))

#     # Traitement des données
#     try:
#         df_with_datetime = create_datetime(df_current_batch_raw_merged)
#         df_cleaned = gestion_doublons(df_with_datetime)
#         df_processed = df_cleaned.set_index('Datetime').sort_index()

#         # Mise à jour des données globales
#         if GLOBAL_PROCESSED_DATA_DF.empty:
#             GLOBAL_PROCESSED_DATA_DF = df_processed
#         else:
#             stations_to_update = df_processed['Station'].unique()
#             GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF[~GLOBAL_PROCESSED_DATA_DF['Station'].isin(stations_to_update)]
#             GLOBAL_PROCESSED_DATA_DF = pd.concat([GLOBAL_PROCESSED_DATA_DF, df_processed]).sort_index()

#         # Interpolation
#         GLOBAL_PROCESSED_DATA_DF = interpolation(GLOBAL_PROCESSED_DATA_DF, DATA_LIMITS, GLOBAL_GPS_DATA_DF)
#         flash('Fichiers traités avec succès', 'success')

#     except Exception as e:
#         flash(f'Erreur traitement données: {str(e)}', 'error')
#         return redirect(url_for('index'))

#     return redirect(url_for('visualizations_options'))

# @app.route('/visualizations_options')
# # def visualizations_options():
# #     if GLOBAL_PROCESSED_DATA_DF.empty:
# #         flash('Veuillez uploader des fichiers d\'abord', 'error')
# #         return redirect(url_for('index'))

# #     excluded_cols = ['Station', 'Is_Daylight', 'Daylight_Duration', 'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
# #     available_variables = [col for col in GLOBAL_PROCESSED_DATA_DF.columns 
# #                          if pd.api.types.is_numeric_dtype(GLOBAL_PROCESSED_DATA_DF[col]) and col not in excluded_cols]

# #     daily_stats_df = calculate_daily_summary_table(GLOBAL_PROCESSED_DATA_DF)
# #     daily_stats_html = daily_stats_df.to_html(classes='table table-striped', index=False)

# #     return render_template('visualizations_options.html',
# #                          stations=sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique()),
# #                          variables=sorted(available_variables),
# #                          daily_stats_table=daily_stats_html)


# @app.route('/visualizations_options')
# def visualizations_options():
#     if GLOBAL_PROCESSED_DATA_DF.empty:
#         flash('Veuillez uploader des fichiers d\'abord', 'error')
#         return redirect(url_for('index'))

#     excluded_cols = ['Station', 'Is_Daylight', 'Daylight_Duration', 
#                    'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
#     available_variables = [col for col in GLOBAL_PROCESSED_DATA_DF.columns 
#                          if pd.api.types.is_numeric_dtype(GLOBAL_PROCESSED_DATA_DF[col]) 
#                          and col not in excluded_cols]

#     daily_stats_df = calculate_daily_summary_table(GLOBAL_PROCESSED_DATA_DF)
#     daily_stats_html = daily_stats_df.to_html(classes='table table-striped', index=False)

#     return render_template('visualizations_options.html',
#                          stations=sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique()),
#                          variables=sorted(available_variables),
#                          daily_stats_table=daily_stats_html)

# @app.route('/generate_plot', methods=['POST'])
# def generate_plot():
#     # [Votre code existant pour generate_plot reste inchangé]
#     pass

# @app.route('/generate_multi_variable_plot', methods=['POST'])
# def generate_multi_variable_plot_route():
#     """Génère un graphique multi-variables pour une station"""
#     try:
#         station = request.form['station']
#         variables = request.form.getlist('variables[]')
        
#         if not station or not variables:
#             flash('Veuillez sélectionner une station et au moins une variable', 'error')
#             return redirect(url_for('visualizations_options'))

#         fig = generate_multi_variable_station_plot(
#             df=GLOBAL_PROCESSED_DATA_DF,
#             station=station,
#             colors=PALETTE_DEFAUT,
#             metadata=METADATA_VARIABLES
#         )
        
#         # Convertir la figure matplotlib en image
#         img = io.BytesIO()
#         fig.savefig(img, format='png', bbox_inches='tight')
#         img.seek(0)
#         plot_url = base64.b64encode(img.getvalue()).decode('utf8')
#         plt.close(fig)
        
#         return render_template('plot_display.html', 
#                             plot_url=plot_url,
#                             title=f"Graphique multi-variables pour {station}")
    
#     except Exception as e:
#         flash(f"Erreur lors de la génération du graphique: {str(e)}", 'error')
#         return redirect(url_for('visualizations_options'))
    
# @app.route('/generate_plot', methods=['POST'])
# def generate_plot():
#     """Génère un graphique pour une variable ou une comparaison"""
#     try:
#         # Récupération des données du formulaire
#         station = request.form.get('station')
#         variable = request.form.get('variable')
#         periode = request.form.get('periode')
#         is_comparative = 'comparative' in request.form

#         # Validation des données
#         if not variable or not periode:
#             flash('Veuillez sélectionner une variable et une période', 'error')
#             return redirect(url_for('visualizations_options'))

#         if not is_comparative and not station:
#             flash('Veuillez sélectionner une station', 'error')
#             return redirect(url_for('visualizations_options'))

#         # Génération du graphique
#         if is_comparative:
#             fig = generer_graphique_comparatif(
#                 df=GLOBAL_PROCESSED_DATA_DF,
#                 variable=variable,
#                 periode=periode,
#                 colors=PALETTE_DEFAUT,
#                 metadata=METADATA_VARIABLES
#             )
#             title = f"Comparaison de {METADATA_VARIABLES.get(variable, {}).get('Nom', variable)} ({periode})"
#         else:
#             fig = generer_graphique_par_variable_et_periode(
#                 df=GLOBAL_PROCESSED_DATA_DF,
#                 station=station,
#                 variable=variable,
#                 periode=periode,
#                 colors=PALETTE_DEFAUT,
#                 metadata=METADATA_VARIABLES
#             )
#             title = f"{METADATA_VARIABLES.get(variable, {}).get('Nom', variable)} à {station} ({periode})"

#         # Conversion de la figure Plotly en HTML
#         plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

#         return render_template('plot_display.html', 
#                             plot_html=plot_html,
#                             title=title)

#     except Exception as e:
#         flash(f"Erreur lors de la génération du graphique: {str(e)}", 'error')
#         return redirect(url_for('visualizations_options'))
    

# @app.route('/preview')
# def data_preview():
#     if GLOBAL_PROCESSED_DATA_DF.empty:
#         flash('Aucune donnée disponible. Veuillez uploader des fichiers d\'abord.', 'error')
#         return redirect(url_for('index'))
    
#     # Préparer l'aperçu
#     stations = GLOBAL_PROCESSED_DATA_DF['Station'].unique()
    
#     if len(stations) == 1:
#         # Si une seule station, afficher 20 lignes
#         preview_df = GLOBAL_PROCESSED_DATA_DF.head(20)
#     else:
#         # Si plusieurs stations, afficher 10 lignes par station
#         preview_dfs = []
#         for station in stations:
#             station_df = GLOBAL_PROCESSED_DATA_DF[GLOBAL_PROCESSED_DATA_DF['Station'] == station].head(10)
#             preview_dfs.append(station_df)
#         preview_df = pd.concat(preview_dfs)
    
#     # Obtenir les dimensions
#     rows, cols = GLOBAL_PROCESSED_DATA_DF.shape
    
#     return render_template('preview.html',
#                          preview_table=preview_df.to_html(classes='table table-striped'),
#                          dataset_shape=f"{rows} lignes × {cols} colonnes",
#                          stations_count=len(stations))

# if __name__ == '__main__':
#     app.run(debug=True)

# import os
# import pandas as pd
# from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
# import plotly.graph_objects as go
# import plotly.express as px
# import json
# import matplotlib.pyplot as plt
# import io
# import base64
# import numpy as np
# import traceback
# from werkzeug.utils import secure_filename

# # Importations des fonctions de traitement
# from data_processing import (
#     create_datetime,
#     interpolation,
#     _load_and_prepare_gps_data,
#     gestion_doublons,
#     calculate_daily_summary_table,
#     generer_graphique_par_variable_et_periode,
#     generer_graphique_comparatif,
#     generate_multi_variable_station_plot,
#     generate_variable_summary_plots_for_web,
#     apply_station_specific_preprocessing
# )

# from config import METADATA_VARIABLES, PALETTE_DEFAUT, DATA_LIMITS, ALLOWED_EXTENSIONS, STATIONS_BY_BASSIN

# app = Flask(__name__)
# app.secret_key = 'votre_cle_secrete_ici'
# app.config['MAX_CONTENT_LENGTH'] = 64 * 1024 * 1024
# app.config['UPLOAD_FOLDER'] = 'uploads'

# @app.context_processor
# def inject_globals():
#     return {
#         'METADATA_VARIABLES': METADATA_VARIABLES,
#         'PALETTE_DEFAUT': PALETTE_DEFAUT,
#         'STATIONS_BY_BASSIN': STATIONS_BY_BASSIN
#     }

# # Variables globales
# GLOBAL_PROCESSED_DATA_DF = pd.DataFrame()
# GLOBAL_GPS_DATA_DF = pd.DataFrame()

# # Chargement des données GPS
# with app.app_context():
#     GLOBAL_GPS_DATA_DF = _load_and_prepare_gps_data()

# def allowed_file(filename):
#     return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# @app.route('/')
# def index():
#     os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
#     return render_template('index.html', 
#                          bassins=sorted(STATIONS_BY_BASSIN.keys()),
#                          stations_by_bassin=STATIONS_BY_BASSIN)

# @app.route('/upload', methods=['POST'])
# def upload_file():
#     global GLOBAL_PROCESSED_DATA_DF

#     if not request.files:
#         flash('Aucun fichier reçu', 'error')
#         return redirect(url_for('index'))

#     # Récupération des fichiers et des stations
#     uploaded_files = request.files.getlist('file[]')
#     stations = [request.form.get(f'station_{i}') for i in range(len(uploaded_files)) 
#                if request.form.get(f'station_{i}')]

#     if len(uploaded_files) != len(stations):
#         flash('Nombre de fichiers et de stations incompatible', 'error')
#         return redirect(url_for('index'))

#     df_current_batch_raw_merged = pd.DataFrame()

#     for file, station in zip(uploaded_files, stations):
#         if file and allowed_file(file.filename):
#             try:
#                 # Sauvegarde temporaire du fichier
#                 filename = secure_filename(file.filename)
#                 filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#                 file.save(filepath)

#                 # Lecture du fichier
#                 if filename.endswith('.csv'):
#                     df_temp = pd.read_csv(filepath)
#                 else:
#                     df_temp = pd.read_excel(filepath)

#                 # Prétraitement
#                 df_temp['Station'] = station
#                 df_temp = apply_station_specific_preprocessing(df_temp, station)
#                 df_current_batch_raw_merged = pd.concat([df_current_batch_raw_merged, df_temp], ignore_index=True)

#                 # Suppression du fichier temporaire
#                 os.remove(filepath)

#             except Exception as e:
#                 flash(f"Erreur traitement {file.filename}: {str(e)}", 'error')
#                 continue

#     if df_current_batch_raw_merged.empty:
#         flash('Aucune donnée valide traitée', 'error')
#         return redirect(url_for('index'))

#     # Traitement des données
#     try:
#         df_with_datetime = create_datetime(df_current_batch_raw_merged)
#         df_cleaned = gestion_doublons(df_with_datetime)
#         df_processed = df_cleaned.set_index('Datetime').sort_index()

#         # Mise à jour des données globales
#         if GLOBAL_PROCESSED_DATA_DF.empty:
#             GLOBAL_PROCESSED_DATA_DF = df_processed
#         else:
#             stations_to_update = df_processed['Station'].unique()
#             GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF[~GLOBAL_PROCESSED_DATA_DF['Station'].isin(stations_to_update)]
#             GLOBAL_PROCESSED_DATA_DF = pd.concat([GLOBAL_PROCESSED_DATA_DF, df_processed]).sort_index()

#         # Interpolation
#         GLOBAL_PROCESSED_DATA_DF = interpolation(GLOBAL_PROCESSED_DATA_DF, DATA_LIMITS, GLOBAL_GPS_DATA_DF)
#         flash('Fichiers traités avec succès', 'success')

#     except Exception as e:
#         flash(f'Erreur traitement données: {str(e)}', 'error')
#         return redirect(url_for('index'))

#     return redirect(url_for('visualizations_options'))

# @app.route('/visualizations_options')
# def visualizations_options():
#     if GLOBAL_PROCESSED_DATA_DF.empty:
#         flash('Veuillez uploader des fichiers d\'abord', 'error')
#         return redirect(url_for('index'))

#     excluded_cols = ['Station', 'Is_Daylight', 'Daylight_Duration', 
#                    'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
#     available_variables = [col for col in GLOBAL_PROCESSED_DATA_DF.columns 
#                          if pd.api.types.is_numeric_dtype(GLOBAL_PROCESSED_DATA_DF[col]) 
#                          and col not in excluded_cols]

#     daily_stats_df = calculate_daily_summary_table(GLOBAL_PROCESSED_DATA_DF)
#     daily_stats_html = daily_stats_df.to_html(classes='table table-striped', index=False)

#     return render_template('visualizations_options.html',
#                          stations=sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique()),
#                          variables=sorted(available_variables),
#                          daily_stats_table=daily_stats_html)

# @app.route('/generate_multi_variable_plot', methods=['POST'])
# def generate_multi_variable_plot_route():
#     """Génère un graphique multi-variables pour une station"""
#     try:
#         station = request.form['station']
#         variables = request.form.getlist('variables[]')
        
#         if not station or not variables:
#             flash('Veuillez sélectionner une station et au moins une variable', 'error')
#             return redirect(url_for('visualizations_options'))

#         fig = generate_multi_variable_station_plot(
#             df=GLOBAL_PROCESSED_DATA_DF,
#             station=station,
#             colors=PALETTE_DEFAUT,
#             metadata=METADATA_VARIABLES
#         )
        
#         # Convertir la figure matplotlib en image
#         img = io.BytesIO()
#         fig.savefig(img, format='png', bbox_inches='tight')
#         img.seek(0)
#         plot_url = base64.b64encode(img.getvalue()).decode('utf8')
#         plt.close(fig)
        
#         return render_template('plot_display.html', 
#                             plot_url=plot_url,
#                             title=f"Graphique multi-variables pour {station}")
    
#     except Exception as e:
#         flash(f"Erreur lors de la génération du graphique: {str(e)}", 'error')
#         return redirect(url_for('visualizations_options'))

# @app.route('/generate_plot', methods=['POST'])
# def generate_plot():
#     """Génère un graphique pour une variable ou une comparaison"""
#     try:
#         # Récupération des données du formulaire
#         station = request.form.get('station')
#         variable = request.form.get('variable')
#         periode = request.form.get('periode')
#         is_comparative = 'comparative' in request.form

#         # Validation des données
#         if not variable or not periode:
#             flash('Veuillez sélectionner une variable et une période', 'error')
#             return redirect(url_for('visualizations_options'))

#         if not is_comparative and not station:
#             flash('Veuillez sélectionner une station', 'error')
#             return redirect(url_for('visualizations_options'))

#         # Génération du graphique
#         if is_comparative:
#             fig = generer_graphique_comparatif(
#                 df=GLOBAL_PROCESSED_DATA_DF,
#                 variable=variable,
#                 periode=periode,
#                 colors=PALETTE_DEFAUT,
#                 metadata=METADATA_VARIABLES
#             )
#             title = f"Comparaison de {METADATA_VARIABLES.get(variable, {}).get('Nom', variable)} ({periode})"
#         else:
#             fig = generer_graphique_par_variable_et_periode(
#                 df=GLOBAL_PROCESSED_DATA_DF,
#                 station=station,
#                 variable=variable,
#                 periode=periode,
#                 colors=PALETTE_DEFAUT,
#                 metadata=METADATA_VARIABLES
#             )
#             title = f"{METADATA_VARIABLES.get(variable, {}).get('Nom', variable)} à {station} ({periode})"

#         # Conversion de la figure Plotly en HTML
#         plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

#         return render_template('plot_display.html', 
#                             plot_html=plot_html,
#                             title=title)

#     except Exception as e:
#         flash(f"Erreur lors de la génération du graphique: {str(e)}", 'error')
#         return redirect(url_for('visualizations_options'))

# @app.route('/preview')
# def data_preview():
#     if GLOBAL_PROCESSED_DATA_DF.empty:
#         flash('Aucune donnée disponible. Veuillez uploader des fichiers d\'abord.', 'error')
#         return redirect(url_for('index'))
    
#     # Préparer l'aperçu
#     stations = GLOBAL_PROCESSED_DATA_DF['Station'].unique()
    
#     if len(stations) == 1:
#         # Si une seule station, afficher 20 lignes
#         preview_df = GLOBAL_PROCESSED_DATA_DF.head(20)
#     else:
#         # Si plusieurs stations, afficher 10 lignes par station
#         preview_dfs = []
#         for station in stations:
#             station_df = GLOBAL_PROCESSED_DATA_DF[GLOBAL_PROCESSED_DATA_DF['Station'] == station].head(10)
#             preview_dfs.append(station_df)
#         preview_df = pd.concat(preview_dfs)
    
#     # Obtenir les dimensions
#     rows, cols = GLOBAL_PROCESSED_DATA_DF.shape
    
#     return render_template('preview.html',
#                          preview_table=preview_df.to_html(classes='table table-striped'),
#                          dataset_shape=f"{rows} lignes × {cols} colonnes",
#                          stations_count=len(stations))

# if __name__ == '__main__':
#     app.run(debug=True)

import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import plotly.graph_objects as go
import plotly.express as px
import json
import io
import base64
import numpy as np
import traceback
from werkzeug.utils import secure_filename

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
    generate_variable_summary_plots_for_web,
    apply_station_specific_preprocessing
)

from config import METADATA_VARIABLES, PALETTE_DEFAUT, DATA_LIMITS, ALLOWED_EXTENSIONS, STATIONS_BY_BASSIN

app = Flask(__name__)
app.secret_key = 'votre_cle_secrete_ici'
app.config['MAX_CONTENT_LENGTH'] = 64 * 1024 * 1024
app.config['UPLOAD_FOLDER'] = 'uploads'

@app.context_processor
def inject_globals():
    return {
        'METADATA_VARIABLES': METADATA_VARIABLES,
        'PALETTE_DEFAUT': PALETTE_DEFAUT,
        'STATIONS_BY_BASSIN': STATIONS_BY_BASSIN
    }

# Variables globales
GLOBAL_PROCESSED_DATA_DF = pd.DataFrame()
GLOBAL_GPS_DATA_DF = pd.DataFrame()

# Chargement des données GPS
with app.app_context():
    GLOBAL_GPS_DATA_DF = _load_and_prepare_gps_data()

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    return render_template('index.html', 
                         bassins=sorted(STATIONS_BY_BASSIN.keys()),
                         stations_by_bassin=STATIONS_BY_BASSIN)

@app.route('/upload', methods=['POST'])
def upload_file():
    global GLOBAL_PROCESSED_DATA_DF

    if not request.files:
        flash('Aucun fichier reçu', 'error')
        return redirect(url_for('index'))

    # Récupération des fichiers et des stations
    uploaded_files = request.files.getlist('file[]')
    stations = [request.form.get(f'station_{i}') for i in range(len(uploaded_files)) 
               if request.form.get(f'station_{i}')]

    if len(uploaded_files) != len(stations):
        flash('Nombre de fichiers et de stations incompatible', 'error')
        return redirect(url_for('index'))

    df_current_batch_raw_merged = pd.DataFrame()

    for file, station in zip(uploaded_files, stations):
        if file and allowed_file(file.filename):
            try:
                # Sauvegarde temporaire du fichier
                filename = secure_filename(file.filename)
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                file.save(filepath)

                # Lecture du fichier
                if filename.endswith('.csv'):
                    df_temp = pd.read_csv(filepath)
                else:
                    df_temp = pd.read_excel(filepath)

                # Prétraitement
                df_temp['Station'] = station
                df_temp = apply_station_specific_preprocessing(df_temp, station)
                df_current_batch_raw_merged = pd.concat([df_current_batch_raw_merged, df_temp], ignore_index=True)

                # Suppression du fichier temporaire
                os.remove(filepath)

            except Exception as e:
                flash(f"Erreur traitement {file.filename}: {str(e)}", 'error')
                continue

    if df_current_batch_raw_merged.empty:
        flash('Aucune donnée valide traitée', 'error')
        return redirect(url_for('index'))

    # Traitement des données
    try:
        df_with_datetime = create_datetime(df_current_batch_raw_merged)
        df_cleaned = gestion_doublons(df_with_datetime)
        df_processed = df_cleaned.set_index('Datetime').sort_index()

        # Mise à jour des données globales
        if GLOBAL_PROCESSED_DATA_DF.empty:
            GLOBAL_PROCESSED_DATA_DF = df_processed
        else:
            stations_to_update = df_processed['Station'].unique()
            GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF[~GLOBAL_PROCESSED_DATA_DF['Station'].isin(stations_to_update)]
            GLOBAL_PROCESSED_DATA_DF = pd.concat([GLOBAL_PROCESSED_DATA_DF, df_processed]).sort_index()

        # Interpolation
        GLOBAL_PROCESSED_DATA_DF = interpolation(GLOBAL_PROCESSED_DATA_DF, DATA_LIMITS, GLOBAL_GPS_DATA_DF)
        flash('Fichiers traités avec succès', 'success')

    except Exception as e:
        flash(f'Erreur traitement données: {str(e)}', 'error')
        return redirect(url_for('index'))

    return redirect(url_for('data_preview'))

@app.route('/visualizations_options')
def visualizations_options():
    if GLOBAL_PROCESSED_DATA_DF.empty:
        flash('Veuillez uploader des fichiers d\'abord', 'error')
        return redirect(url_for('index'))

    excluded_cols = ['Station', 'Is_Daylight', 'Daylight_Duration', 
                   'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
    available_variables = [col for col in GLOBAL_PROCESSED_DATA_DF.columns 
                         if pd.api.types.is_numeric_dtype(GLOBAL_PROCESSED_DATA_DF[col]) 
                         and col not in excluded_cols]

    daily_stats_df = calculate_daily_summary_table(GLOBAL_PROCESSED_DATA_DF)
    daily_stats_html = daily_stats_df.to_html(classes='table table-striped', index=False)

    return render_template('visualizations_options.html',
                         stations=sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique()),
                         variables=sorted(available_variables),
                         daily_stats_table=daily_stats_html)

@app.route('/generate_multi_variable_plot', methods=['POST'])
def generate_multi_variable_plot_route():
    """Génère un graphique multi-variables pour une station"""
    try:
        station = request.form['station']
        variables = request.form.getlist('variables[]')
        
        if not station or not variables:
            flash('Veuillez sélectionner une station et au moins une variable', 'error')
            return redirect(url_for('visualizations_options'))

        fig = generate_multi_variable_station_plot(
            df=GLOBAL_PROCESSED_DATA_DF,
            station=station,
            colors=PALETTE_DEFAUT,
            metadata=METADATA_VARIABLES
        )
        
        # Conversion de la figure Plotly en HTML
        plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')
        
        return render_template('plot_display.html', 
                            plot_html=plot_html,
                            title=f"Graphique multi-variables pour {station}")
    
    except Exception as e:
        flash(f"Erreur lors de la génération du graphique: {str(e)}", 'error')
        return redirect(url_for('visualizations_options'))

@app.route('/generate_plot', methods=['POST'])
def generate_plot():
    """Génère un graphique pour une variable ou une comparaison"""
    try:
        # Récupération des données du formulaire
        station = request.form.get('station')
        variable = request.form.get('variable')
        periode = request.form.get('periode')
        is_comparative = 'comparative' in request.form

        # Validation des données
        if not variable or not periode:
            flash('Veuillez sélectionner une variable et une période', 'error')
            return redirect(url_for('visualizations_options'))

        if not is_comparative and not station:
            flash('Veuillez sélectionner une station', 'error')
            return redirect(url_for('visualizations_options'))

        # Génération du graphique
        if is_comparative:
            fig = generer_graphique_comparatif(
                df=GLOBAL_PROCESSED_DATA_DF,
                variable=variable,
                periode=periode,
                colors=PALETTE_DEFAUT,
                metadata=METADATA_VARIABLES
            )
            title = f"Comparaison de {METADATA_VARIABLES.get(variable, {}).get('Nom', variable)} ({periode})"
        else:
            fig = generer_graphique_par_variable_et_periode(
                df=GLOBAL_PROCESSED_DATA_DF,
                station=station,
                variable=variable,
                periode=periode,
                colors=PALETTE_DEFAUT,
                metadata=METADATA_VARIABLES
            )
            title = f"{METADATA_VARIABLES.get(variable, {}).get('Nom', variable)} à {station} ({periode})"

        # Conversion de la figure Plotly en HTML
        plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

        return render_template('plot_display.html', 
                            plot_html=plot_html,
                            title=title)

    except Exception as e:
        flash(f"Erreur lors de la génération du graphique: {str(e)}", 'error')
        return redirect(url_for('visualizations_options'))


# ... (le reste de vos imports et configurations existants)

@app.route('/preview')
def data_preview():
    """Affiche l'aperçu des données"""
    try:
        if GLOBAL_PROCESSED_DATA_DF.empty:
            flash('Aucune donnée disponible. Veuillez uploader des fichiers d\'abord.', 'error')
            return redirect(url_for('index'))
        
        # Préparation de l'aperçu
        stations = GLOBAL_PROCESSED_DATA_DF['Station'].unique()
        
        if len(stations) == 1:
            preview_df = GLOBAL_PROCESSED_DATA_DF.head(20).reset_index()
            preview_type = "20 premières lignes"
        else:
            preview_dfs = []
            for station in stations:
                station_df = GLOBAL_PROCESSED_DATA_DF[GLOBAL_PROCESSED_DATA_DF['Station'] == station].head(10).reset_index()
                preview_dfs.append(station_df)
            preview_df = pd.concat(preview_dfs)
            preview_type = f"10 lignes × {len(stations)} stations"
        
        # Conversion en HTML - Version corrigée
        preview_html = preview_df.to_html(
            classes='table table-striped table-hover',
            index=False,
            border=0,
            justify='left',
            na_rep='NaN',  # Affichage des valeurs manquantes
            max_rows=None  # Désactive la troncature
        )
        
        return render_template('preview.html',
                            preview_table=preview_html,
                            preview_type=preview_type,
                            dataset_shape=f"{GLOBAL_PROCESSED_DATA_DF.shape[0]} lignes × {GLOBAL_PROCESSED_DATA_DF.shape[1]} colonnes",
                            stations_count=len(stations))
    
    except Exception as e:
        flash(f'Erreur lors de la préparation de l\'aperçu: {str(e)}', 'error')
        return redirect(url_for('index'))
    
# @app.route('/preview')
# def data_preview():
#     if GLOBAL_PROCESSED_DATA_DF.empty:
#         flash('Aucune donnée disponible. Veuillez uploader des fichiers d\'abord.', 'error')
#         return redirect(url_for('index'))
    
#     # Préparer l'aperçu selon le nombre de stations
#     stations = GLOBAL_PROCESSED_DATA_DF['Station'].unique()
    
#     if len(stations) == 1:
#         # Une seule station : afficher 20 lignes
#         preview_df = GLOBAL_PROCESSED_DATA_DF.head(20).reset_index()
#         preview_type = "20 premières lignes de la station unique"
#     else:
#         # Multiple stations : 10 lignes par station
#         preview_dfs = []
#         for station in stations:
#             station_df = GLOBAL_PROCESSED_DATA_DF[GLOBAL_PROCESSED_DATA_DF['Station'] == station].head(10)
#             preview_dfs.append(station_df)
#         preview_df = pd.concat(preview_dfs).reset_index()
#         preview_type = f"10 premières lignes de chacune des {len(stations)} stations"
    
#     # Obtenir les dimensions du dataset complet
#     rows, cols = GLOBAL_PROCESSED_DATA_DF.shape
    
#     # Formater le tableau pour l'affichage HTML
#     preview_table = preview_df.to_html(
#         classes='table table-striped table-bordered',
#         index=False,
#         border=0,
#         justify='left'
#     )
    
#     return render_template('preview.html',
#                          preview_table=preview_table,
#                          preview_type=preview_type,
#                          dataset_shape=f"{rows} lignes × {cols} colonnes",
#                          stations_count=len(stations))


@app.route('/reset_data', methods=['POST'])
def reset_data():
    global GLOBAL_PROCESSED_DATA_DF, GLOBAL_GPS_DATA_DF
    
    try:
        # Réinitialisation des DataFrames
        GLOBAL_PROCESSED_DATA_DF = pd.DataFrame()
        GLOBAL_GPS_DATA_DF = _load_and_prepare_gps_data()  # Recharge les données GPS de base
        
        # Suppression des fichiers uploadés
        upload_folder = app.config['UPLOAD_FOLDER']
        for filename in os.listdir(upload_folder):
            file_path = os.path.join(upload_folder, filename)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                flash(f"Erreur lors de la suppression de {file_path}: {str(e)}", 'warning')
        
        flash('Données réinitialisées avec succès', 'success')
    except Exception as e:
        flash(f'Erreur lors de la réinitialisation: {str(e)}', 'error')
    
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)