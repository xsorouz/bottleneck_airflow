@echo off
echo --------------------------------------------------------
echo 🧹 Nettoyage complet de l’environnement BottleNeck Airflow
echo --------------------------------------------------------

REM Étape 1 : Arrêt et suppression des conteneurs
echo 🔻 Arrêt des conteneurs...
docker compose down --volumes --remove-orphans

REM Étape 2 : Nettoyage des volumes Docker (optionnel)
echo 🧼 Suppression des volumes non utilisés...
docker volume prune -f

REM Étape 3 : Reconstruction des images personnalisées
echo 🛠️ Reconstruction des images Docker...
docker compose build

REM Étape 4 : Démarrage de l’environnement
echo 🚀 Démarrage des conteneurs Airflow...
docker compose up -d

REM Pause pour laisser les services démarrer proprement
echo ⏳ Attente du démarrage (10 secondes)...
timeout /t 10 /nobreak >nul

REM Étape 5 : Initialisation de la base de données Airflow
echo 🗄️ Initialisation de la base de données Airflow...
docker compose run --rm airflow-webserver airflow db init

REM Étape 6 : Création de l'utilisateur admin
echo 👤 Création de l’utilisateur Airflow admin...
docker compose run --rm airflow-webserver airflow users create ^
  --username admin ^
  --firstname Xavier ^
  --lastname Rousseau ^
  --role Admin ^
  --email xavier@example.com ^
  --password admin

echo ✅ Environnement prêt. Accès Web : http://localhost:8080
echo 📊 Monitoring Flower : http://localhost:5555
