# # import pandas as pd
# # # Importation de la fonction de chargement et de préparation des données GPS depuis data_processing
# # from data_processing import _load_and_prepare_gps_data

# # # Dictionnaire des stations par bassin
# # STATIONS_BY_BASSIN = {
# #     'DANO': ['Dreyer Foundation', 'Lare', 'Bankandi', 'Wahablé', 'Fafo',
# #              'Yabogane', 'Tambiri 1', 'Tambiri 2'],
# #     'DASSARI': ['Pouri (Fandohoun)', 'Nagasséga', 'Koundri', 'Koupendri',
# #                 'Ouriyori 2', 'Ouriyori 1', 'Wantéhoun', 'Pouri', 'Fandohoun'],
# #     'VEA_SISSILI': ['Kayoro EC', 'Doninga ', 'Bongo Soe', 'Nabugubulle ', 'Gwosi',
# #                     'Bongo Atampisi', 'Manyoro', 'Aniabiisi', 'Nazinga EC', 'Oualem ',
# #                     'Nebou', 'Tabou ', 'Nazinga']
# # }

# # # Dictionnaire des limites pour chaque variable météorologique
# # # Ces limites sont utilisées pour le bornage des valeurs (capping)
# # LIMITS_METEO = {
# #     'Air_Temp_Deg_C': {'min': -20, 'max': 50},
# #     'Rel_H_%': {'min': 0, 'max': 100},
# #     'BP_mbar_Avg': {'min': 950, 'max': 1050},
# #     'Rain_01_mm': {'min': 0, 'max': 500},
# #     'Rain_02_mm': {'min': 0, 'max': 500},
# #     'Rain_mm': {'min': 0, 'max': 500},
# #     'Wind_Sp_m/sec': {'min': 0, 'max': 50},
# #     'Solar_R_W/m^2': {'min': 0, 'max': 2000},
# #     'Wind_Dir_Deg': {'min': 0, 'max': 360}
# # }

# # # La variable GLOBAL_DF_GPS_INFO est maintenant chargée en appelant la fonction
# # # _load_and_prepare_gps_data() depuis data_processing.py.
# # # Cela signifie que les téléchargements et le prétraitement des coordonnées
# # # seront exécutés une seule fois lorsque l'application Flask démarre.
# # try:
# #     GLOBAL_DF_GPS_INFO = _load_and_prepare_gps_data()
# # except Exception as e:
# #     print(f"Erreur fatale lors du chargement des données GPS des stations: {e}. "
# #           "Veuillez vérifier votre connexion Internet et les IDs des fichiers Google Drive.")
# #     # En cas d'échec critique, initialiser avec un DataFrame vide pour éviter un crash complet
# #     GLOBAL_DF_GPS_INFO = pd.DataFrame({
# #         'Station': [],
# #         'Lat': [],
# #         'Long': [],
# #         'Timezone': []
# #     })

# # # Palette de couleurs personnalisée pour les stations (pour les comparaisons)
# # CUSTOM_STATION_COLORS = [
# #     '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
# #     '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
# #     '#aec7e8', '#ffbb78', '#98df8a', '#ff9896', '#c5b0d5',
# #     '#c49c94', '#f7b6d2', '#c7c7c7', '#dbdb8d', '#9edae5'
# # ]

# # # Palette de couleurs personnalisée pour les variables (pour l'évolution normalisée)
# # CUSTOM_VARIABLE_COLORS = [
# #     '#1f77b4',  # Bleu pour Température
# #     '#ff7f0e',  # Orange pour Humidité
# #     '#2ca02c',  # Vert pour Précipitations
# #     '#d62728',  # Rouge pour Radiation
# #     '#9467bd',  # Violet pour Vitesse vent
# #     '#8c564b',  # Marron pour Direction vent
# #     '#e377c2'   # Rose pour Pression
# # ]



# import pandas as pd
# # Importation de la fonction de chargement et de préparation des données GPS depuis data_processing
# from data_processing import _load_and_prepare_gps_data

# # Dictionnaire des stations par bassin
# STATIONS_BY_BASSIN = {
#     'DANO': ['Dreyer Foundation', 'Lare', 'Bankandi', 'Wahablé', 'Fafo',
#              'Yabogane', 'Tambiri 1', 'Tambiri 2'],
#     'DASSARI': ['Pouri (Fandohoun)', 'Nagasséga', 'Koundri', 'Koupendri',
#                 'Ouriyori 2', 'Ouriyori 1', 'Wantéhoun', 'Pouri', 'Fandohoun'],
#     'VEA_SISSILI': ['Kayoro EC', 'Doninga ', 'Bongo Soe', 'Nabugubulle ', 'Gwosi',
#                     'Bongo Atampisi', 'Manyoro', 'Aniabiisi', 'Nazinga EC', 'Oualem ',
#                     'Nebou', 'Tabou ', 'Nazinga']
# }

# # Dictionnaire des limites pour chaque variable météorologique
# # Ces limites sont utilisées pour le bornage des valeurs (capping)
# LIMITS_METEO = {
#     'Air_Temp_Deg_C': {'min': -20, 'max': 50},
#     'Rel_H_%': {'min': 0, 'max': 100},
#     'BP_mbar_Avg': {'min': 950, 'max': 1050},
#     'Rain_01_mm': {'min': 0, 'max': 500},
#     'Rain_02_mm': {'min': 0, 'max': 500},
#     'Rain_mm': {'min': 0, 'max': 500},
#     'Wind_Sp_m/sec': {'min': 0, 'max': 50},
#     'Solar_R_W/m^2': {'min': 0, 'max': 2000},
#     'Wind_Dir_Deg': {'min': 0, 'max': 360}
# }

# # La variable GLOBAL_DF_GPS_INFO est maintenant chargée en appelant la fonction
# # _load_and_prepare_gps_data() depuis data_processing.py.
# # Cela signifie que les téléchargements et le prétraitement des coordonnées
# # seront exécutés une seule fois lorsque l'application Flask démarre.
# try:
#     GLOBAL_DF_GPS_INFO = _load_and_prepare_gps_data()
# except Exception as e:
#     print(f"Erreur fatale lors du chargement des données GPS des stations: {e}. "
#           "Veuillez vérifier votre connexion Internet et les IDs des fichiers Google Drive.")
#     # En cas d'échec critique, initialiser avec un DataFrame vide pour éviter un crash complet
#     GLOBAL_DF_GPS_INFO = pd.DataFrame({
#         'Station': [],
#         'Lat': [],
#         'Long': [],
#         'Timezone': []
#     })

# # Définition des métadonnées pour chaque variable météorologique
# # 'Nom': Nom complet de la variable pour les titres de graphiques.
# # 'Unite': Unité de mesure de la variable.
# # 'agg_type': Type d'agrégation ('cumul' pour somme, 'moyenne' pour moyenne).
# # 'is_rain': Indicateur si c'est une variable de pluie (pour agrégation spéciale).
# METADATA_VARIABLES = {
#     'Rain_mm': {'Nom': "Précipitation", 'Unite': "mm", 'agg_type': 'cumul', 'is_rain': True},
#     'Air_Temp_Deg_C': {'Nom': "Température ", 'Unite': "°C", 'agg_type': 'moyenne', 'is_rain': False},
#     'Rel_H_%': {'Nom': "Humidité Relative", 'Unite': "%", 'agg_type': 'moyenne', 'is_rain': False},
#     'Solar_R_W/m^2': {'Nom': "Radiation Solaire", 'Unite': "W/m²", 'agg_type': 'moyenne', 'is_rain': False},
#     'Wind_Sp_m/sec': {'Nom': "Vitesse du Vent", 'Unite': "m/s", 'agg_type': 'moyenne', 'is_rain': False},
#     'Wind_Dir_Deg': {'Nom': "Direction du Vent", 'Unite': "°", 'agg_type': 'moyenne', 'is_rain': False},
#     'BP_mbar_Avg': {'Nom': "Pression Atmospherique moyenne", 'Unite': "mbar", 'agg_type': 'moyenne', 'is_rain': False}
# }

# # Palette de couleurs personnalisée pour les stations (pour les comparaisons)
# CUSTOM_STATION_COLORS = [
#     '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
#     '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
#     '#aec7e8', '#ffbb78', '#98df8a', '#ff9896', '#c5b0d5',
#     '#c49c94', '#f7b6d2', '#c7c7c7', '#dbdb8d', '#9edae5'
# ]

# # Palette de couleurs personnalisée pour les variables (pour l'évolution normalisée)
# CUSTOM_VARIABLE_COLORS = [
#     '#1f77b4',  # Bleu pour Température
#     '#ff7f0e',  # Orange pour Humidité
#     '#2ca02c',  # Vert pour Précipitations
#     '#d62728',  # Rouge pour Radiation
#     '#9467bd',  # Violet pour Vitesse vent
#     '#8c564b',  # Marron pour Direction vent
#     '#e377c2'   # Rose pour Pression
# ]


# import pandas as pd
# # Suppression de l'importation de _load_and_prepare_gps_data pour éviter l'importation circulaire.
# # Cette fonction sera appelée directement dans app.py.
# # from data_processing import _load_and_prepare_gps_data

# # Dictionnaire des stations par bassin
# STATIONS_BY_BASSIN = {
#     'DANO': ['Dreyer Foundation', 'Lare', 'Bankandi', 'Wahablé', 'Fafo',
#              'Yabogane', 'Tambiri 1', 'Tambiri 2'],
#     'DASSARI': ['Pouri (Fandohoun)', 'Nagasséga', 'Koundri', 'Koupendri',
#                 'Ouriyori 2', 'Ouriyori 1', 'Wantéhoun', 'Pouri', 'Fandohoun'],
#     'VEA_SISSILI': ['Kayoro EC', 'Doninga ', 'Bongo Soe', 'Nabugubulle ', 'Gwosi',
#                     'Bongo Atampisi', 'Manyoro', 'Aniabiisi', 'Nazinga EC', 'Oualem ',
#                     'Nebou', 'Tabou ', 'Nazinga']
# }

# # Dictionnaire des limites pour chaque variable météorologique
# # Ces limites sont utilisées pour le bornage des valeurs (capping)
# DATA_LIMITS = {
#     'Air_Temp_Deg_C': {'min': -20, 'max': 50},
#     'Rel_H_%': {'min': 0, 'max': 100},
#     'BP_mbar_Avg': {'min': 950, 'max': 1050},
#     'Rain_01_mm': {'min': 0, 'max': 500},
#     'Rain_02_mm': {'min': 0, 'max': 500},
#     'Rain_mm': {'min': 0, 'max': 500},
#     'Wind_Sp_m/sec': {'min': 0, 'max': 50},
#     'Solar_R_W/m^2': {'min': 0, 'max': 2000},
#     'Wind_Dir_Deg': {'min': 0, 'max': 360}
# }

# # GLOBAL_DF_GPS_INFO est initialisé comme None ici, il sera peuplé par app.py
# # après l'initialisation de tous les modules.
# GLOBAL_DF_GPS_INFO = None


# # Définition des métadonnées pour chaque variable météorologique
# # 'Nom': Nom complet de la variable pour les titres de graphiques.
# # 'Unite': Unité de mesure de la variable.
# # 'agg_type': Type d'agrégation ('cumul' pour somme, 'moyenne' pour moyenne).
# # 'is_rain': Indicateur si c'est une variable de pluie (pour agrégation spéciale).
# METADATA_VARIABLES = {
#     'Rain_mm': {'Nom': "Précipitation", 'Unite': "mm", 'agg_type': 'cumul', 'is_rain': True},
#     'Air_Temp_Deg_C': {'Nom': "Température ", 'Unite': "°C", 'agg_type': 'moyenne', 'is_rain': False},
#     'Rel_H_%': {'Nom': "Humidité Relative", 'Unite': "%", 'agg_type': 'moyenne', 'is_rain': False},
#     'Solar_R_W/m^2': {'Nom': "Radiation Solaire", 'Unite': "W/m²", 'agg_type': 'moyenne', 'is_rain': False},
#     'Wind_Sp_m/sec': {'Nom': "Vitesse du Vent", 'Unite': "m/s", 'agg_type': 'moyenne', 'is_rain': False},
#     'Wind_Dir_Deg': {'Nom': "Direction du Vent", 'Unite': "°", 'agg_type': 'moyenne', 'is_rain': False},
#     'BP_mbar_Avg': {'Nom': "Pression Atmospherique moyenne", 'Unite': "mbar", 'agg_type': 'moyenne', 'is_rain': False}
# }

# # Palette de couleurs personnalisée pour les stations (DOIT ÊTRE UN DICTIONNAIRE)
# # Assurez-vous que les noms de stations ici correspondent EXACTEMENT à ceux de vos fichiers et de STATIONS_BY_BASSIN
# CUSTOM_STATION_COLORS = {
#     "Dreyer Foundation": "#1f77b4",
#     "Fafo": "#ff7f0e",
#     "Koundri": "#2ca02c",
#     "Wantéhoun": "#d62728",
#     "Lare": "#9467bd",
#     "Bankandi": "#8c564b",
#     "Ouriyori 1": "#e377c2",
#     "Pouri": "#7f7f7f", # Exemple: Pouri
#     "Fandohoun": "#bcbd22", # Exemple: Fandohoun
#     "Nagasséga": "#17becf", # Exemple: Nagasséga
#     "Kayoro EC": "#aec7e8", # Exemple: Kayoro EC
#     # Ajoutez toutes les stations que vous utilisez avec une couleur unique
# }

# # Palette de couleurs personnalisée pour les variables (DOIT ÊTRE UN DICTIONNAIRE)
# PALETTE_DEFAUT = {
#     "Air_Temp_Deg_C": "#1f77b4",  # Bleu
#     "Rel_H_%": "#ff7f0e",       # Orange
#     "Rain_mm": "#2ca02c",       # Vert
#     "Solar_R_W/m^2": "#d62728", # Rouge
#     "Wind_Sp_m/sec": "#9467bd", # Violet
#     "Wind_Dir_Deg": "#8c564b",  # Marron
#     "BP_mbar_Avg": "#e377c2"    # Rose
# }

import pandas as pd
# Suppression de l'importation de _load_and_prepare_gps_data pour éviter l'importation circulaire.
# Cette fonction sera appelée directement dans app.py.
# from data_processing import _load_and_prepare_gps_data

# Dictionnaire des stations par bassin
STATIONS_BY_BASSIN = {
    'DANO': ['Dreyer Foundation', 'Lare', 'Bankandi', 'Wahablé', 'Fafo',
             'Yabogane', 'Tambiri 1', 'Tambiri 2'],
    'DASSARI': ['Pouri (Fandohoun)', 'Nagasséga', 'Koundri', 'Koupendri',
                'Ouriyori 2', 'Ouriyori 1', 'Wantéhoun', 'Pouri', 'Fandohoun'],
    'VEA_SISSILI': ['Kayoro EC', 'Doninga ', 'Bongo Soe', 'Nabugubulle ', 'Gwosi',
                    'Bongo Atampisi', 'Manyoro', 'Aniabiisi', 'Nazinga EC', 'Oualem ',
                    'Nebou', 'Tabou ', 'Nazinga']
}

# Dictionnaire des limites pour chaque variable météorologique
# Ces limites sont utilisées pour le bornage des valeurs (capping)
DATA_LIMITS = {
    'Air_Temp_Deg_C': {'min': -20, 'max': 50},
    'Rel_H_%': {'min': 0, 'max': 100},
    'BP_mbar_Avg': {'min': 950, 'max': 1050},
    'Rain_01_mm': {'min': 0, 'max': 500},
    'Rain_02_mm': {'min': 0, 'max': 500},
    'Rain_mm': {'min': 0, 'max': 500},
    'Wind_Sp_m/sec': {'min': 0, 'max': 50},
    'Solar_R_W/m^2': {'min': 0, 'max': 2000},
    'Wind_Dir_Deg': {'min': 0, 'max': 360}
}

# GLOBAL_DF_GPS_INFO est initialisé comme None ici, il sera peuplé par app.py
# après l'initialisation de tous les modules.
GLOBAL_DF_GPS_INFO = None

# Extensions de fichier autorisées pour l'upload
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'} # Ajouté pour résoudre l'ImportError

# Définition des métadonnées pour chaque variable météorologique
# 'Nom': Nom complet de la variable pour les titres de graphiques.
# 'Unite': Unité de mesure de la variable.
# 'agg_type': Type d'agrégation ('cumul' pour somme, 'moyenne' pour moyenne).
# 'is_rain': Indicateur si c'est une variable de pluie (pour agrégation spéciale).
METADATA_VARIABLES = {
    'Rain_mm': {'Nom': "Précipitation", 'Unite': "mm", 'agg_type': 'cumul', 'is_rain': True},
    'Air_Temp_Deg_C': {'Nom': "Température ", 'Unite': "°C", 'agg_type': 'moyenne', 'is_rain': False},
    'Rel_H_%': {'Nom': "Humidité Relative", 'Unite': "%", 'agg_type': 'moyenne', 'is_rain': False},
    'Solar_R_W/m^2': {'Nom': "Radiation Solaire", 'Unite': "W/m²", 'agg_type': 'moyenne', 'is_rain': False},
    'Wind_Sp_m/sec': {'Nom': "Vitesse du Vent", 'Unite': "m/s", 'agg_type': 'moyenne', 'is_rain': False},
    'Wind_Dir_Deg': {'Nom': "Direction du Vent", 'Unite': "°", 'agg_type': 'moyenne', 'is_rain': False},
    'BP_mbar_Avg': {'Nom': "Pression Atmospherique moyenne", 'Unite': "mbar", 'agg_type': 'moyenne', 'is_rain': False}
}

# Palette de couleurs personnalisée pour les stations (DOIT ÊTRE UN DICTIONNAIRE)
# Assurez-vous que les noms de stations ici correspondent EXACTEMENT à ceux de vos fichiers et de STATIONS_BY_BASSIN
CUSTOM_STATION_COLORS = {
    "Dreyer Foundation": "#1f77b4",
    "Fafo": "#ff7f0e",
    "Koundri": "#2ca02c",
    "Wantéhoun": "#d62728",
    "Lare": "#9467bd",
    "Bankandi": "#8c564b",
    "Ouriyori 1": "#e377c2",
    "Pouri": "#7f7f7f", # Exemple: Pouri
    "Fandohoun": "#bcbd22", # Exemple: Fandohoun
    "Nagasséga": "#17becf", # Exemple: Nagasséga
    "Kayoro EC": "#aec7e8", # Exemple: Kayoro EC
    # Ajoutez toutes les stations que vous utilisez avec une couleur unique
}

# Palette de couleurs personnalisée pour les variables (DOIT ÊTRE UN DICTIONNAIRE)
PALETTE_DEFAUT = {
    "Air_Temp_Deg_C": "#1f77b4",  # Bleu
    "Rel_H_%": "#ff7f0e",       # Orange
    "Rain_mm": "#2ca02c",       # Vert
    "Solar_R_W/m^2": "#d62728", # Rouge
    "Wind_Sp_m/sec": "#9467bd", # Violet
    "Wind_Dir_Deg": "#8c564b",  # Marron
    "BP_mbar_Avg": "#e377c2"    # Rose
}

# Palette de couleurs pour les statistiques
PALETTE_COULEUR = {
    'Maximum': '#d62728', 
    'Minimum': '#1f77b4', 
    'Moyenne': '#2ca02c',
    'Mediane': '#ff7f0e', 
    'Cumul_Annuel': '#8c564b',
    'Moyenne_Jours_Pluvieux': '#9467bd', 
    'Moyenne_Saison_Pluvieuse': '#e377c2',
    'Duree_Saison_Pluvieuse_Jours': '#17becf',
    'Duree_Secheresse_Definie_Jours': '#bcbd22'
}