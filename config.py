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

# # Extensions de fichier autorisées pour l'upload
# ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'} # Ajouté pour résoudre l'ImportError

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


# CUSTOM_STATION_COLORS = {
#     # Bassin DANO (8 stations)
#     'Dreyer Foundation': '#FF0000',  # Rouge vif
#     'Lare': '#00AA00',       # Vert foncé
#     'Bankandi': '#0000FF',   # Bleu pur
#     'Wahablé': '#FF00FF',    # Magenta
#     'Fafo': '#FFA500',       # Orange
#     'Yabogane': '#00FFFF',   # Cyan
#     'Tambiri 1': '#A52A2A',  # Marron
#     'Tambiri 2': '#FFD700',  # Or
    
#     # Bassin DASSARI (9 stations)
#     'Pouri (Fandohoun)': '#9400D3',  # Violet foncé
#     'Nagasséga': '#008000',   # Vert sapin
#     'Koundri': '#4B0082',     # Indigo
#     'Koupendri': '#FF4500',   # Orange-rouge
#     'Ouriyori 2': '#20B2AA',  # Vert mer clair
#     'Ouriyori 1': '#9932CC',  # Orchidée foncée
#     'Wantéhoun': '#8B0000',   # Rouge foncé
#     'Pouri': '#000080',       # Bleu marine
#     'Fandohoun': '#556B2F',   # Vert olive
    
#     # Bassin VEA_SISSILI (13 stations)
#     'Kayoro EC': '#E60000',    # Rouge vif
#     'Doninga ': '#00CED1',     # Bleu turquoise
#     'Bongo Soe': '#32CD32',    # Vert lime
#     'Nabugubulle ': '#8A2BE2', # Bleu violet
#     'Gwosi': '#FF8C00',        # Orange foncé
#     'Bongo Atampisi': '#EE82EE', # Violet clair
#     'Manyoro': '#006400',      # Vert très foncé
#     'Aniabiisi': '#800000',    # Rouge bordeaux
#     'Nazinga EC': '#0000CD',   # Bleu moyen
#     'Oualem ': '#FF69B4',      # Rose vif
#     'Nebou': '#808000',        # Olive
#     'Tabou ': '#7CFC00',       # Vert prairie
#     'Nazinga': '#BA55D3'       # Violet moyen
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

# # Palette de couleurs pour les statistiques
# PALETTE_COULEUR = {
#     'Maximum': '#d62728', 
#     'Minimum': '#1f77b4', 
#     'Moyenne': '#2ca02c',
#     'Mediane': '#ff7f0e', 
#     'Cumul_Annuel': '#8c564b',
#     'Moyenne_Jours_Pluvieux': '#9467bd', 
#     'Moyenne_Saison_Pluvieuse': '#e377c2',
#     'Duree_Saison_Pluvieuse_Jours': '#17becf',
#     'Duree_Secheresse_Definie_Jours': '#bcbd22'
# }

import pandas as pd
from flask_babel import lazy_gettext as _l # Importer lazy_gettext

# Dictionnaire des stations par bassin
STATIONS_BY_BASSIN = {
    'DANO': ['Dreyer Foundation', 'Lare', 'Bankandi', 'Wahablé', 'Fafo',
             'Yabogane', 'Tambiri 1', 'Tambiri 2'],
    'DASSARI': ['Pouri (Fandohoun)', 'Nagasséga', 'Koundri', 'Koupendri',
                'Ouriyori 2', 'Ouriyori 1', 'Wantéhoun', 'Pouri', 'Fandohoun'],
    'VEA_SISSILI': ['Kayoro EC', 'Doninga ', 'Bongo Soe', 'Nabugubulle ', 'Gwosi',
                    'Bongo Atampisi', 'Manyoro', 'Aniabisi', 'Nazinga EC', 'Oualem ',
                    'Nebou', 'Tabou ', 'Nazinga']
}

# Dictionnaire des limites pour chaque variable météorologique
# Ces limites sont utilisées pour le bornage des valeurs (capping)
DATA_LIMITS = {
    'Air_Temp_Deg_C': {'min': 0, 'max': 50},
    'Rel_H_%': {'min': 0, 'max': 100},
    'BP_mbar_Avg': {'min': 850, 'max': 1090},
    'Rain_01_mm': {'min': 0, 'max': 60},
    'Rain_02_mm': {'min': 0, 'max': 60},
    'Rain_mm': {'min': 0, 'max': 60},
    'Wind_Sp_m/sec': {'min': 0, 'max': 60},
    'Solar_R_W/m^2': {'min': 0, 'max': 1300},
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
# Utilisation de _l() pour marquer les chaînes à traduire
# METADATA_VARIABLES = {
#     'Rain_mm': {'Nom': _l("Précipitation"), 'Unite': _l("mm"), 'agg_type': 'cumul', 'is_rain': True},
#     'Air_Temp_Deg_C': {'Nom': _l("Température"), 'Unite': _l("°C"), 'agg_type': 'moyenne', 'is_rain': False},
#     'Rel_H_%': {'Nom': _l("Humidité Relative"), 'Unite': _l("%"), 'agg_type': 'moyenne', 'is_rain': False},
#     'Solar_R_W/m^2': {'Nom': _l("Radiation Solaire"), 'Unite': _l("W/m²"), 'agg_type': 'moyenne', 'is_rain': False},
#     'Wind_Sp_m/sec': {'Nom': _l("Vitesse du Vent"), 'Unite': _l("m/s"), 'agg_type': 'moyenne', 'is_rain': False},
#     'Wind_Dir_Deg': {'Nom': _l("Direction du Vent"), 'Unite': _l("°"), 'agg_type': 'moyenne', 'is_rain': False},
#     'BP_mbar_Avg': {'Nom': _l("Pression Atmospherique moyenne"), 'Unite': _l("mbar"), 'agg_type': 'moyenne', 'is_rain': False}
# }


CUSTOM_STATION_COLORS = {
    # Bassin DANO (8 stations)
    'Dreyer Foundation': '#8B4513',  # Brun Sienne
    'Lare': '#DAA520',       # Jaune doré
    'Bankandi': '#FF6347',   # Rouge Tomate
    'Wahablé': '#40E0D0',    # Turquoise
    'Fafo': '#8A2BE2',       # Bleu Violet
    'Yabogane': '#FF007F',   # Rose Vif
    'Tambiri 1': '#ADFF2F',  # Vert Chartreuse
    'Tambiri 2': '#BA55D3',  # Violet Clair
    
    # Bassin DASSARI (9 stations)
    'Pouri (Fandohoun)': '#F08080',  # Corail clair
    'Nagasséga': '#20B2AA',   # Vert Mer Clair
    'Koundri': '#C71585',     # Rouge Violet
    'Koupendri': '#D2B48C',   # Tan (couleur chamois)
    'Ouriyori 2': '#6B8E23',  # Vert Olive Foncé
    'Ouriyori 1': '#FFDAB9',  # Pêche
    'Wantéhoun': '#AFEEEE',   # Turquoise Pâle
    'Pouri': '#CD853F',       # Peru (couleur terre de Sienne)
    'Fandohoun': '#DA70D6',   # Orchidée
    
    # Bassin VEA_SISSILI (13 stations)
    'Kayoro EC': '#00BFFF',    # Bleu ciel profond
    'Doninga ': '#FF4500',     # Orange Rouge
    'Bongo Soe': '#9ACD32',    # Vert Jaune
    'Nabugubulle ': '#DB7093', # Rose pâle
    'Gwosi': '#BDB76B',        # Olive Foncé
    'Bongo Atampisi': '#8B008B', # Magenta Foncé
    'Manyoro': '#48D1CC',      # Turquoise Moyen
    'Aniabiisi': '#8FBC8F',    # Vert Mer Foncé
    'Nazinga EC': '#C0C0C0',   # Argent
    'Oualem ': '#F0E68C',      # Kaki
    'Nebou': '#4682B4',        # Bleu Acier
    'Tabou ': '#ADD8E6',       # Bleu Clair
    'Nazinga': '#EE6363'       # Rose saumon clair
}


# CUSTOM_STATION_COLORS = {
#     # Bassin DANO (8 stations)
#     'Dreyer Foundation': '#FF6B6B',  # Rouge doux
#     'Lare': '#51A851',              # Vert pomme
#     'Bankandi': '#5E72E4',          # Bleu doux
#     'Wahablé': '#F368E0',           # Rose vif
#     'Fafo': '#FF9F43',              # Orange doux
#     'Yabogane': '#00D2D3',          # Turquoise
#     'Tambiri 1': '#A1785D',         # Marron clair
#     'Tambiri 2': '#FFD166',         # Jaune doré
    
#     # Bassin DASSARI (9 stations)
#     'Pouri (Fandohoun)': '#A55EEA', # Violet moyen
#     'Nagasséga': '#2ECC71',         # Vert émeraude
#     'Koundri': '#6C5CE7',           # Indigo doux
#     'Koupendri': '#FF7F50',         # Corail
#     'Ouriyori 2': '#1DD1A1',        # Vert menthe
#     'Ouriyori 1': '#9B59B6',        # Violet orchidée
#     'Wantéhoun': '#E74C3C',         # Rouge tomate
#     'Pouri': '#3498DB',             # Bleu ciel
#     'Fandohoun': '#27AE60',         # Vert forêt
    
#     # Bassin VEA_SISSILI (13 stations)
#     'Kayoro EC': '#FF5252',         # Rouge clair
#     'Doninga ': '#00CEC9',          # Bleu piscine
#     'Bongo Soe': '#55EFC4',         # Vert menthe clair
#     'Nabugubulle ': '#A29BFE',      # Lavande
#     'Gwosi': '#F39C12',             # Orange foncé
#     'Bongo Atampisi': '#FD79A8',    # Rose bonbon
#     'Manyoro': '#1E8449',           # Vert foncé
#     'Aniabiisi': '#D63031',         # Rouge brique
#     'Nazinga EC': '#0984E3',        # Bleu azur
#     'Oualem ': '#FF9FF3',           # Rose pâle
#     'Nebou': '#B8E994',             # Vert pistache
#     'Tabou ': '#00B894',            # Vert turquoise
#     'Nazinga': '#D6A2E8'            # Lilas
# }

# Palette de couleurs personnalisée pour les variables (DOIT ÊTRE UN DICTIONNAIRE)
# Note: Les clés du dictionnaire (e.g., "Air_Temp_Deg_C") ne sont pas traduites car ce sont des identifiants techniques.
# Seules les valeurs qui apparaissent dans l'interface utilisateur sont traduites.
PALETTE_DEFAUT = {
    "Air_Temp_Deg_C": "#E63946",  # Bleu
    "Rel_H_%": "#00A6FB",       # Orange
    "Rain_mm": "#457B9D",       # Vert
    "Solar_R_W/m^2": "#FFBE0B", # Rouge
    "Wind_Sp_m/sec": "#7209B7", # Violet
    "Wind_Dir_Deg": "#6A4C93",  # Marron
    "BP_mbar_Avg": "#2A9D8F"    # Rose
}

# Palette de couleurs pour les statistiques
# Si ces clés/valeurs sont affichées dans l'interface, elles devraient aussi être balisées avec _l()
PALETTE_COULEUR = {
    _l('Maximum'): '#d62728', 
    _l('Minimum'): '#1f77b4', 
    _l('Moyenne'): '#2ca02c',
    _l('Médiane'): '#ff7f0e', 
    _l('Cumul_Annuel'): '#8c564b', # Peut être laissé non traduit si c'est une clé interne
    _l('Moyenne_Jours_Pluvieux'): '#9467bd', 
    _l('Moyenne_Saison_Pluvieuse'): '#e377c2',
    _l('Duree_Saison_Pluvieuse_Jours'): '#17becf', # Peut être laissé non traduit si c'est une clé interne
    _l('Duree_Secheresse_Definie_Jours'): '#bcbd22' # Peut être laissé non traduit si c'est une clé interne
}

import pandas as pd
from flask_babel import lazy_gettext as _l

METADATA_VARIABLES = {
    'Rain_mm': {
        'Nom': {
            'fr': _l("Précipitation"),
            'en': _l("Precipitation")
        },
        'Unite': {
            'fr': _l("mm"),
            'en': _l("mm")
        },
        'agg_type': 'cumul',
        'is_rain': True
    },
    'Air_Temp_Deg_C': {
        'Nom': {
            'fr': _l("Température"),
            'en': _l("Temperature")
        },
        'Unite': {
            'fr': _l("°C"),
            'en': _l("°C")
        },
        'agg_type': 'moyenne',
        'is_rain': False
    },
    'Rel_H_%': {
        'Nom': {
            'fr': _l("Humidité Relative"),
            'en': _l("Relative Humidity")
        },
        'Unite': {
            'fr': _l("%"),
            'en': _l("%")
        },
        'agg_type': 'moyenne',
        'is_rain': False
    },
    'Solar_R_W/m^2': {
        'Nom': {
            'fr': _l("Radiation Solaire"),
            'en': _l("Solar Radiation")
        },
        'Unite': {
            'fr': _l("W/m²"),
            'en': _l("W/m²")
        },
        'agg_type': 'moyenne',
        'is_rain': False
    },
    'Wind_Sp_m/sec': {
        'Nom': {
            'fr': _l("Vitesse du Vent"),
            'en': _l("Wind Speed")
        },
        'Unite': {
            'fr': _l("m/s"),
            'en': _l("m/s")
        },
        'agg_type': 'moyenne',
        'is_rain': False
    },
    'Wind_Dir_Deg': {
        'Nom': {
            'fr': _l("Direction du Vent"),
            'en': _l("Wind Direction")
        },
        'Unite': {
            'fr': _l("°"),
            'en': _l("°")
        },
        'agg_type': 'moyenne',
        'is_rain': False
    },
    'BP_mbar_Avg': {
        'Nom': {
            'fr': _l("Pression Atmospherique moyenne"),
            'en': _l("Mean Atmospheric Pressure")
        },
        'Unite': {
            'fr': _l("mbar"),
            'en': _l("mbar")
        },
        'agg_type': 'moyenne',
        'is_rain': False
    }
}

# Define metric labels for translation
METRIC_LABELS = {
    'Maximum': {'fr': 'Maximum', 'en': 'Maximum'},
    'Minimum': {'fr': 'Minimum', 'en': 'Minimum'},
    'Mediane': {'fr': 'Médiane', 'en': 'Median'},
    'Moyenne': {'fr': 'Moyenne', 'en': 'Average'},
    'Cumul Annuel': {'fr': 'Cumul Annuel', 'en': 'Annual Accumulation'},
    'Moyenne Jours Pluvieux': {'fr': 'Moyenne des Jours Pluvieux', 'en': 'Average Rainy Days'},
    'Moyenne Saison Pluvieuse': {'fr': 'Moyenne Saison Pluvieuse', 'en': 'Average Rainy Season'},
    'Début Saison Pluvieuse': {'fr': 'Début Saison Pluvieuse', 'en': 'Rainy Season Start'},
    'Fin Saison Pluvieuse': {'fr': 'Fin Saison Pluvieuse', 'en': 'Rainy Season End'},
    'Durée Saison Pluvieuse Jours': {'fr': 'Durée Saison Pluvieuse (Jours)', 'en': 'Rainy Season Duration (Days)'},
    'Début Sécheresse Définie': {'fr': 'Début Sécheresse Définie', 'en': 'Defined Dry Spell Start'},
    'Fin Sécheresse Définie': {'fr': 'Fin Sécheresse Définie', 'en': 'Defined Dry Spell End'},
    'Durée Sécheresse Définie Jours': {'fr': 'Durée Sécheresse Définie (Jours)', 'en': 'Defined Dry Spell Duration (Days)'},
    'Date Max': {'fr': 'Date Max', 'en': 'Max Date'},
    'Date Min': {'fr': 'Date Min', 'en': 'Min Date'},
    'Unknown date': {'fr': 'Date inconnue', 'en': 'Unknown date'},
    'From {} to {}': {'fr': 'Du {} au {}', 'en': 'From {} to {}'},
    'Duration: {} days': {'fr': 'Durée: {} jours', 'en': 'Duration: {} days'},
    'Statistics of {} by Station': {'fr': 'Statistiques de {} par Station', 'en': 'Statistics of {} by Station'},
    'Days': {'fr': 'Jours', 'en': 'Days'},
    # Add any other hardcoded strings that appear as labels or titles in your plot
    # and were previously wrapped in _() but are specific metric labels.
}


# Define period labels for translation
PERIOD_LABELS = {
    'Journalière': {'fr': 'Journalière', 'en': 'Daily'},
    'Hebdomadaire': {'fr': 'Hebdomadaire', 'en': 'Weekly'},
    'Mensuelle': {'fr': 'Mensuelle', 'en': 'Monthly'},
    'Annuelle': {'fr': 'Annuelle', 'en': 'Annual'},
    # Add any other periods you might use
}


# Dictionnaire des types de données pour chaque variable météorologique pour optimiser le stockage
# Utilisation de float32 pour les variables continues et category pour les variables catégorielles
METEO_DTYPES = {
    # Température air (°C)
    'Air_Temp_Deg_C': 'float32',  # -40 à +50°C suffisant en float32
    # Humidité relative (%)
    'Rel_H_%': 'float32',         # 0-100% avec décimale
    # Pression atmosphérique (mbar)
    'BP_mbar_Avg': 'float32',     # 800-1100 mbar
    # Pluviométrie (mm)
    'Rain_01_mm': 'float32',
    'Rain_02_mm': 'float32', 
    'Rain_mm': 'float32',         # Cumuls pluie
    # Vent (m/s)
    'Wind_Sp_m/sec': 'float32',   # 0-60 m/s
    # Radiation solaire (W/m²)
    'Solar_R_W/m^2': 'float32',   # 0-1500 W/m²
    # Direction vent (°)
    'Wind_Dir_Deg': 'float32',    # 0-360°
    # Station (catégorielle)
    'Station': 'category'         # Optimise le stockage des noms
}