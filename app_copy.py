
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, session
import os
import io
import re # Importation pour les expressions régulières

# Importation des fonctions de traitement et de visualisation des données
from data_processing import (
    create_datetime,
    gestion_doublons,
    interpolation,
    traiter_outliers_meteo,
    generer_graphique_par_variable_et_periode,
    generer_graphique_comparatif,
    generate_multi_variable_station_plot,
    daily_stats,
    _load_and_prepare_gps_data
)

# Importation des configurations globales
import config # Importation du module config entier
from config import (
    STATIONS_BY_BASSIN,
    DATA_LIMITS,
    CUSTOM_STATION_COLORS,
    PALETTE_DEFAUT,
    METADATA_VARIABLES
)

# Ajout d'une option pour éviter les avertissements de downcasting de Pandas
pd.set_option('future.no_silent_downcasting', True)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'une_cle_secrete_par_defaut_tres_forte_pour_dev')

# Variable globale pour stocker les données du DataFrame après traitement
GLOBAL_PROCESSED_DATA_DF = None

# Initialisation de GLOBAL_DF_GPS_INFO au démarrage de l'application
try:
    config.GLOBAL_DF_GPS_INFO = _load_and_prepare_gps_data()
except Exception as e:
    print(f"Erreur fatale lors du chargement des données GPS des stations au démarrage: {e}. "
          "L'application pourrait ne pas fonctionner correctement sans ces données.")
    config.GLOBAL_DF_GPS_INFO = pd.DataFrame({
        'Station': [],
        'Lat': [],
        'Long': [],
        'Timezone': []
    })


@app.route('/')
def index():
    """
    Route principale affichant le formulaire d'upload de fichiers.
    """
    return render_template('index.html', stations_by_bassin=STATIONS_BY_BASSIN)

@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    """
    Gère l'upload et le traitement des fichiers CSV/Excel.
    Permet l'upload de multiples fichiers, chacun associé à un bassin et une station,
    en vérifiant leur compatibilité et en les ajoutant au DataFrame global.
    Si la méthode est GET, redirige vers la page d'accueil.
    """
    if request.method == 'GET':
        flash('Accès direct à la page d\'upload non autorisé. Veuillez utiliser le formulaire.', 'warning')
        return redirect(url_for('index'))

    global GLOBAL_PROCESSED_DATA_DF

    upload_groups = []

    # Collecter tous les indices de groupe présents dans le formulaire
    all_input_indices = set()
    for key in request.form.keys():
        match = re.search(r'_((\d+))$', key)
        if match:
            all_input_indices.add(int(match.group(1)))
    for key in request.files.keys():
        match = re.search(r'_(\d+)$', key)
        if match:
            all_input_indices.add(int(match.group(1)))

    sorted_indices = sorted(list(all_input_indices))

    if not sorted_indices:
        flash('Aucun dataset n\'a été soumis. Veuillez ajouter au moins un dataset.', 'error')
        return redirect(url_for('index'))

    print(f"DEBUG (upload_file): Nombre de groupes d'upload détectés: {len(sorted_indices)}")
    for index in sorted_indices:
        bassin_name = request.form.get(f'bassin_{index}')
        station_name = request.form.get(f'station_{index}')
        file_obj = request.files.get(f'file_input_{index}')

        if not bassin_name or not station_name or not file_obj or file_obj.filename == '':
            flash(f'Le dataset pour le groupe {index+1} est incomplet (bassin, station ou fichier manquant). Veuillez compléter toutes les informations.', 'error')
            return redirect(url_for('index'))

        upload_groups.append({
            'bassin': bassin_name,
            'station': station_name,
            'file': file_obj,
            'index': index
        })

    if not upload_groups:
        flash('Aucun dataset valide n\'a été trouvé pour le traitement. Veuillez vérifier vos sélections.', 'error')
        return redirect(url_for('index'))


    processed_dataframes_for_batch = []

    expected_raw_data_columns_for_comparison = None
    expected_raw_time_columns_for_comparison = None


    for group_info in upload_groups:
        file = group_info['file']
        bassin = group_info['bassin']
        station = group_info['station']

        file_extension = os.path.splitext(file.filename)[1].lower()
        df_temp = None

        try:
            if file_extension == '.csv':
                df_temp = pd.read_csv(io.BytesIO(file.read()))
            elif file_extension in ['.xls', '.xlsx']:
                df_temp = pd.read_excel(io.BytesIO(file.read()))
            else:
                flash(f'Extension de fichier non supportée pour "{file.filename}". Seuls les fichiers CSV ou Excel sont acceptés.', 'error')
                return redirect(url_for('index'))

            if df_temp is not None:
                current_file_columns = df_temp.columns.tolist()

                current_raw_time_cols = []
                if 'Date' in current_file_columns:
                    current_raw_time_cols.append('Date')
                if all(col in current_file_columns for col in ['Year', 'Month', 'Day', 'Hour', 'Minute']):
                    current_raw_time_cols.extend(['Year', 'Month', 'Day', 'Hour', 'Minute'])
                current_raw_time_cols_sorted = sorted(list(set(current_raw_time_cols)))

                current_raw_data_cols_sorted = sorted([
                    col for col in current_file_columns
                    if col != 'Station' and col not in ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Date']
                ])

                if expected_raw_data_columns_for_comparison is None:
                    expected_raw_data_columns_for_comparison = current_raw_data_cols_sorted
                    expected_raw_time_columns_for_comparison = current_raw_time_cols_sorted
                else:
                    if current_raw_data_cols_sorted != expected_raw_data_columns_for_comparison:
                        flash(f"Le fichier '{file.filename}' (Station: {station}) a des colonnes de données incompatibles avec les données déjà chargées. "
                              f"Attendues: {expected_raw_data_columns_for_comparison}, Obtenues: {current_raw_data_cols_sorted}. Fusion annulée.", 'error')
                        return redirect(url_for('index'))

                    if current_raw_time_cols_sorted != expected_raw_time_columns_for_comparison:
                        flash(f"Le fichier '{file.filename}' (Station: {station}) a des colonnes temporelles incompatibles avec les données déjà chargées. "
                              f"Attendues: {expected_raw_time_columns_for_comparison}, Obtenues: {current_raw_time_cols_sorted}. Fusion annulée.", 'error')
                        return redirect(url_for('index'))

                df_temp['Station'] = station
                print(f"DEBUG (upload_file): Fichier '{file.filename}' - Station assignée: '{station}'. Premières 5 lignes du DF temporaire:\n{df_temp.head(5)}")
                processed_dataframes_for_batch.append(df_temp)

        except Exception as e:
            flash(f'Erreur lors de la lecture ou du traitement du fichier "{file.filename}": {e}', 'error')
            print(f"DEBUG (upload_file): Erreur lors du traitement du fichier '{file.filename}': {e}")
            return redirect(url_for('index'))

    if not processed_dataframes_for_batch:
        flash('Aucun fichier valide n\'a pu être traité à partir de ce lot après les vérifications de contenu.', 'error')
        return redirect(url_for('index'))

    try:
        # Concaténation de tous les DataFrames du batch
        df_current_batch_raw_merged = pd.concat(processed_dataframes_for_batch, ignore_index=True)
        print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_raw_merged APRÈS CONCATÉNATION DU BATCH:\n{df_current_batch_raw_merged['Station'].unique().tolist()}")

        # Étape 1: Créer la colonne Datetime AVANT la gestion des doublons
        df_current_batch_with_datetime = create_datetime(df_current_batch_raw_merged, bassin=upload_groups[0]['bassin'])
        print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_with_datetime APRÈS CREATE_DATETIME DU BATCH:\n{df_current_batch_with_datetime['Station'].unique().tolist()}")

        # Étape 2: Gérer les doublons maintenant que 'Datetime' est une colonne
        df_current_batch_cleaned = gestion_doublons(df_current_batch_with_datetime)
        print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_cleaned APRÈS GESTION DOUBLONS DU BATCH:\n{df_current_batch_cleaned['Station'].unique().tolist()}")

        if 'Datetime' not in df_current_batch_cleaned.columns or df_current_batch_cleaned['Datetime'].isnull().all():
            flash("Erreur: La colonne 'Datetime' est manquante ou vide après le nettoyage. Impossible de traiter les données.", 'error')
            return redirect(url_for('index'))

        # Assurez-vous que 'Datetime' est bien de type datetime et nettoyez les NaT
        df_current_batch_cleaned['Datetime'] = pd.to_datetime(df_current_batch_cleaned['Datetime'], errors='coerce')
        df_current_batch_cleaned.dropna(subset=['Datetime'], inplace=True)

        if df_current_batch_cleaned.empty:
            flash("Erreur: Toutes les données du lot actuel ont été supprimées en raison de dates invalides après nettoyage. Traitement annulé.", 'error')
            return redirect(url_for('index'))

        # Définir Datetime comme index
        df_current_batch_processed = df_current_batch_cleaned.set_index('Datetime').sort_index()
        print(f"DEBUG (upload_file): Stations uniques dans df_current_batch_processed APRÈS SET_INDEX ET SORT_INDEX DU BATCH:\n{df_current_batch_processed['Station'].unique().tolist()}")


        if GLOBAL_PROCESSED_DATA_DF is None:
            GLOBAL_PROCESSED_DATA_DF = df_current_batch_processed
            flash(f'{len(processed_dataframes_for_batch)} fichier(s) téléchargé(s) et initialisé(s).', 'success')
            print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF initialisé. Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")
        else:
            print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF EXISTANT AVANT FUSION: Stations uniques: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

            new_batch_stations = df_current_batch_processed['Station'].unique().tolist()
            print(f"DEBUG (upload_file): Stations dans le batch actuel à fusionner: {new_batch_stations}")

            # Filtrer les données existantes pour les stations du lot actuel du DataFrame global
            df_global_filtered = GLOBAL_PROCESSED_DATA_DF[~GLOBAL_PROCESSED_DATA_DF['Station'].isin(new_batch_stations)].copy()
            print(f"DEBUG (upload_file): Stations dans GLOBAL_PROCESSED_DATA_DF après filtrage des stations du batch actuel:\n{df_global_filtered['Station'].unique().tolist() if not df_global_filtered.empty else 'DataFrame global vide après filtrage'}")

            # Concaténer le DataFrame global filtré avec les données du nouveau lot
            # Assurez-vous que les deux DataFrames ont des index compatibles (DatetimeIndex)
            combined_df = pd.concat([df_global_filtered, df_current_batch_processed], ignore_index=False) # Important: ignore_index=False pour préserver les DatetimeIndex et les aligner
            print(f"DEBUG (upload_file): Stations uniques dans combined_df APRÈS CONCATINATION GLOBAL_FILTRÉ+BATCH:\n{combined_df['Station'].unique().tolist()}")

            # Pas besoin de to_datetime/dropna/set_index ici si les index sont déjà corrects.
            # Cependant, on peut re-trier par sécurité.
            GLOBAL_PROCESSED_DATA_DF = combined_df.sort_index()

            print(f"DEBUG (upload_file): GLOBAL_PROCESSED_DATA_DF mis à jour. Stations uniques finales: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

            flash(f'{len(processed_dataframes_for_batch)} fichier(s) ajouté(s). Total de stations uniques: {len(GLOBAL_PROCESSED_DATA_DF["Station"].unique())}.', 'success')

        # --- VÉRIFICATION FINALE ET DÉFENSIVE DE L'INDEX DE GLOBAL_PROCESSED_DATA_DF AVANT TOUT APPEL EXTERNE ---
        print(f"DEBUG: Type de l'index de GLOBAL_PROCESSED_DATA_DF avant vérification finale: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
        try:
            # Cette étape est cruciale si l'index pourrait être cassé, mais elle doit être transparente si tout va bien
            if not isinstance(GLOBAL_PROCESSED_DATA_DF.index, pd.DatetimeIndex):
                # Si l'index n'est pas un DatetimeIndex, essayez de le forcer ou de le recréer à partir d'une colonne Datetime
                if 'Datetime' in GLOBAL_PROCESSED_DATA_DF.columns:
                    GLOBAL_PROCESSED_DATA_DF['Datetime'] = pd.to_datetime(GLOBAL_PROCESSED_DATA_DF['Datetime'], errors='coerce')
                    GLOBAL_PROCESSED_DATA_DF.dropna(subset=['Datetime'], inplace=True)
                    GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.set_index('Datetime').sort_index()
                else:
                    raise TypeError("L'index n'est pas DatetimeIndex et aucune colonne 'Datetime' n'est disponible pour la recréation.")

            GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.sort_index() # Re-trier au cas où

            print("DEBUG: Index de GLOBAL_PROCESSED_DATA_DF VALIDÉ et RE-CONVERTI en DatetimeIndex (vérification finale réussie).")

        except Exception as e:
            flash(f"Erreur critique finale: Impossible de garantir un DatetimeIndex pour l'interpolation. Le format des dates dans vos fichiers est incompatible. Détails: {e}", 'error')
            print(f"DEBUG: Erreur critique finale lors de la conversion de l'index: {e}")
            return redirect(url_for('index'))

        if GLOBAL_PROCESSED_DATA_DF.empty:
            flash("Erreur: Le DataFrame global est vide après le traitement des dates. Il n'y a plus de données valides à analyser.", 'error')
            return redirect(url_for('index'))


        print(f"DEBUG: Type de l'index de GLOBAL_PROCESSED_DATA_DF AVANT interpolation: {type(GLOBAL_PROCESSED_DATA_DF.index)}")
        print(f"DEBUG: Dtypes de GLOBAL_PROCESSED_DATA_DF (après vérif index): \n{GLOBAL_PROCESSED_DATA_DF.dtypes}")


        # Traitement d'interpolation et des outliers sur le DataFrame global unifié
        # Ces fonctions attendent un DatetimeIndex
        GLOBAL_PROCESSED_DATA_DF = interpolation(GLOBAL_PROCESSED_DATA_DF, DATA_LIMITS, config.GLOBAL_DF_GPS_INFO)
        GLOBAL_PROCESSED_DATA_DF = traiter_outliers_meteo(GLOBAL_PROCESSED_DATA_DF, DATA_LIMITS)

        # Nettoyage des colonnes temporaires
        cols_to_drop_after_process = [
            'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date',
            'sunrise_time_local', 'sunset_time_local'
        ]
        GLOBAL_PROCESSED_DATA_DF = GLOBAL_PROCESSED_DATA_DF.drop(columns=cols_to_drop_after_process, errors='ignore')

        # Mettre à jour les informations de session pour les options de visualisation
        session['available_stations'] = sorted(GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist())
        session['available_variables'] = sorted([col for col in GLOBAL_PROCESSED_DATA_DF.columns if col in METADATA_VARIABLES.keys()])
        session['can_compare_stations'] = len(session['available_stations']) >= 2

        flash('Données traitées et fusionnées avec succès ! Vous pouvez maintenant visualiser les résultats.', 'success')
        return redirect(url_for('show_visualizations_options'))

    except Exception as e:
        flash(f"Une erreur inattendue s'est produite lors du traitement des données: {e}", 'error')
        print(f"DEBUG: Erreur inattendue dans /upload: {e}")
        return redirect(url_for('index'))

@app.route('/visualizations_options')
def show_visualizations_options():
    """
    Affiche la page des options de visualisation après le traitement des données.
    """
    if GLOBAL_PROCESSED_DATA_DF is None:
        flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
        return redirect(url_for('index'))

    print(f"DEBUG (app.py/visualizations_options): Stations disponibles dans GLOBAL_PROCESSED_DATA_DF: {GLOBAL_PROCESSED_DATA_DF['Station'].unique().tolist()}")

    daily_stats_df = daily_stats(GLOBAL_PROCESSED_DATA_DF)

    print(f"DEBUG (app.py/visualizations_options): Stations présentes dans daily_stats_df: {daily_stats_df['Station'].unique().tolist() if not daily_stats_df.empty else 'DataFrame vide'}")

    initial_data_html = None
    data_summary = f"Lignes: {len(GLOBAL_PROCESSED_DATA_DF)}, Colonnes: {len(GLOBAL_PROCESSED_DATA_DF.columns)}"

    unique_stations_count = len(GLOBAL_PROCESSED_DATA_DF['Station'].unique())

    if unique_stations_count > 1:
        top_n_data = GLOBAL_PROCESSED_DATA_DF.groupby('Station').head(10)
        initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)
    else:
        top_n_data = GLOBAL_PROCESSED_DATA_DF.head(20)
        initial_data_html = top_n_data.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=True, border=0)

    if initial_data_html is None:
        initial_data_html = "<p class='text-center text-red-500'>Aucune donnée à afficher pour la prévisualisation.</p>"


    daily_stats_html = daily_stats_df.to_html(classes='table-auto w-full text-sm text-left text-gray-700 rounded-lg overflow-hidden shadow-md', index=False, border=0) if not daily_stats_df.empty else None

    stations = session.get('available_stations', [])
    variables = session.get('available_variables', [])
    can_compare_stations = session.get('can_compare_stations', False)

    periodes = ['Journalière', 'Hebdomadaire', 'Mensuelle', 'Annuelle']

    return render_template(
        'visualizations_options.html',
        initial_data_html=initial_data_html,
        data_summary=data_summary,
        daily_stats_html=daily_stats_html,
        stations=stations,
        variables=variables,
        periodes=periodes,
        can_compare_stations=can_compare_stations,
        METADATA_VARIABLES=METADATA_VARIABLES
    )

@app.route('/generate_single_plot', methods=['GET', 'POST'])
def generate_single_plot():
    """
    Génère et affiche un graphique pour une seule variable et une seule station.
    Si la méthode est GET, redirige vers les options de visualisation.
    """
    if request.method == 'GET':
        flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
        return redirect(url_for('show_visualizations_options'))

    if GLOBAL_PROCESSED_DATA_DF is None:
        flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
        return redirect(url_for('index'))

    station_select = request.form.get('station_select_single')
    variable_select = request.form.get('variable_select_single')
    periode_select = request.form.get('periode_select_single')

    if not station_select or not variable_select or not periode_select:
        flash('Veuillez sélectionner une station, une variable et une période pour le graphique.', 'error')
        return redirect(url_for('show_visualizations_options'))

    try:
        plot_data = generer_graphique_par_variable_et_periode(
            GLOBAL_PROCESSED_DATA_DF,
            station_select,
            variable_select,
            periode_select,
            CUSTOM_STATION_COLORS,
            METADATA_VARIABLES
        )

        if plot_data:
            meta = METADATA_VARIABLES.get(variable_select, {})
            plot_title = f"Évolution de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) pour la station {station_select} ({periode_select})"
            return render_template('plot_display.html', plot_data=plot_data, plot_title=plot_title)
        else:
            flash("Impossible de générer le graphique. Vérifiez les données ou la sélection.", 'error')
            return redirect(url_for('show_visualizations_options'))
    except TypeError as e: # Capture spécifique de TypeError
        flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
        print(f"DEBUG: Erreur de type dans generate_single_plot: {e}")
        return redirect(url_for('show_visualizations_options'))
    except Exception as e:
        flash(f"Erreur lors de la génération du graphique: {e}", 'error')
        return redirect(url_for('show_visualizations_options'))

@app.route('/generate_comparative_plot', methods=['GET', 'POST'])
def generate_comparative_plot():
    """
    Génère et affiche un graphique comparatif pour une variable à travers toutes les stations.
    Si la méthode est GET, redirige vers les options de visualisation.
    """
    if request.method == 'GET':
        flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
        return redirect(url_for('show_visualizations_options'))

    if GLOBAL_PROCESSED_DATA_DF is None:
        flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
        return redirect(url_for('index'))

    variable_select = request.form.get('variable_select_comparative')
    periode_select = request.form.get('periode_select_comparative')

    if not variable_select or not periode_select:
        flash('Veuillez sélectionner une variable et une période pour le graphique comparatif.', 'error')
        return redirect(url_for('show_visualizations_options'))

    try:
        plot_data = generer_graphique_comparatif(
            GLOBAL_PROCESSED_DATA_DF,
            variable_select,
            periode_select,
            CUSTOM_STATION_COLORS,
            METADATA_VARIABLES
        )

        if plot_data:
            meta = METADATA_VARIABLES.get(variable_select, {})
            plot_title = f"Comparaison de {meta.get('Nom', variable_select)} ({meta.get('Unite', '')}) entre stations ({periode_select})"
            return render_template('plot_display.html', plot_data=plot_data, plot_title=plot_title)
        else:
            flash("Impossible de générer le graphique comparatif. Vérifiez les données ou la sélection.", 'error')
            return redirect(url_for('show_visualizations_options'))
    except TypeError as e: # Capture spécifique de TypeError
        flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
        print(f"DEBUG: Erreur de type dans generate_comparative_plot: {e}")
        return redirect(url_for('show_visualizations_options'))
    except Exception as e:
        flash(f"Erreur lors de la génération du graphique comparatif: {e}", 'error')
        return redirect(url_for('show_visualizations_options'))

@app.route('/generate_multi_variable_plot', methods=['GET', 'POST'])
def generate_multi_variable_plot_route():
    """
    Génère et affiche un graphique d'évolution multi-variables normalisé pour une station.
    Si la méthode est GET, redirige vers les options de visualisation.
    """
    if request.method == 'GET':
        flash('Accès direct au graphique non autorisé. Veuillez sélectionner les options.', 'warning')
        return redirect(url_for('show_visualizations_options'))

    if GLOBAL_PROCESSED_DATA_DF is None:
        flash('Aucune donnée traitée disponible. Veuillez uploader des fichiers.', 'warning')
        return redirect(url_for('index'))

    station_select = request.form.get('station_select_multi_var')

    if not station_select:
        flash('Veuillez sélectionner une station pour le graphique multi-variables.', 'error')
        return redirect(url_for('show_visualizations_options'))

    try:
        plot_data = generate_multi_variable_station_plot(
            GLOBAL_PROCESSED_DATA_DF,
            station_select,
            PALETTE_DEFAUT,
            METADATA_VARIABLES
        )

        if plot_data:
            plot_title = f"Évolution Normalisée des Variables Météorologiques pour la station {station_select}"
            return render_template('plot_display.html', plot_data=plot_data, plot_title=plot_title)
        else:
            flash("Impossible de générer le graphique multi-variables. Vérifiez les données ou la sélection.", 'error')
            return redirect(url_for('show_visualizations_options'))
    except TypeError as e: # Capture spécifique de TypeError
        flash(f"Erreur de configuration des couleurs: Les palettes de couleurs (CUSTOM_STATION_COLORS, CUSTOM_VARIABLE_COLORS) doivent être des dictionnaires (dict) et non des listes (list). Détails: {e}", 'error')
        print(f"DEBUG: Erreur de type dans generate_multi_variable_plot_route: {e}")
        return redirect(url_for('show_visualizations_options'))
    except Exception as e:
        flash(f"Erreur lors de la génération du graphique multi-variables: {e}", 'error')
        return redirect(url_for('show_visualizations_options'))

@app.route('/reset_data', methods=['GET', 'POST'])
def reset_data():
    """
    Réinitialise les données globales traitées et redirige vers la page d'accueil.
    Si la méthode est GET, redirige simplement vers la page d'accueil.
    """
    if request.method == 'GET':
        return redirect(url_for('index'))

    global GLOBAL_PROCESSED_DATA_DF
    GLOBAL_PROCESSED_DATA_DF = None
    session.pop('available_stations', None)
    session.pop('available_variables', None)
    session.pop('can_compare_stations', None)
    flash('Toutes les données chargées ont été réinitialisées. Vous pouvez commencer une nouvelle analyse.', 'info')
    return redirect(url_for('index'))


if __name__ == '__main__':
    app.run(debug=True)
