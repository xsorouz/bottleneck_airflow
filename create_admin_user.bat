@echo off
echo ========================================
echo 🚀 Redémarrage complet du projet Airflow
echo ========================================
timeout /t 2

REM 1. Arrêt et suppression des conteneurs existants
echo 🔄 Arrêt des services en cours...
docker compose down --volumes --remove-orphans

REM 2. Reconstruction des images (si Dockerfile/requirements.txt modifiés)
echo 🧱 Reconstruction des images Docker...
docker compose build

REM 3. Initialisation de la base de données Airflow
echo 🧬 Initialisation de la base de données Airflow...
docker compose run --rm airflow-webserver airflow db init

REM 4. Création de l'utilisateur admin
echo 👤 Création de l'utilisateur admin...
docker compose run --rm airflow-webserver airflow users create ^
  --username admin ^
  --firstname Xavier ^
  --lastname Rousseau ^
  --role Admin ^
  --email xavier@example.com ^
  --password admin

REM 5. Lancement des services Airflow
echo 🚀 Lancement d'Airflow en mode détaché...
docker compose up -d

echo ✅ Tout est prêt. Accède à http://localhost:8080 avec admin / admin
