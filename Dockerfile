# Étape 1 : image de base
FROM python:3.10-slim

# Étape 2 : définir le dossier de travail
WORKDIR /app

# Étape 3 : Copier le reste des fichiers de l'application
COPY . .


# Étape 4 : installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Étape 5 : exposer le port utilisé par Flask
EXPOSE 8089

# Étape 6 : commande pour démarrer l'application avec Gunicorn
CMD ["python", "app.py"]

# Étape 7 : ajouter un label pour la version de l'application
LABEL version="1.0"