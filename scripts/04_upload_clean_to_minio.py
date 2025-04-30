# === Script 04 - Téléchargement des fichiers CSV depuis MinIO vers data/inputs/ (robuste & logué) ===
# Ce script télécharge les fichiers CSV (erp, web, liaison) du bucket MinIO
# vers le dossier local 'data/inputs/' pour les étapes suivantes du pipeline.

import os
import sys
import warnings
from pathlib import Path
from loguru import logger
import boto3
from botocore.exceptions import ClientError

# ==============================================================================
# 🔧 Initialisation des chemins et logs
# ==============================================================================
warnings.filterwarnings("ignore")

AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "download_from_minio.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# ☁️ Paramètres MinIO
# ==============================================================================
MINIO_ENDPOINT = "http://host.docker.internal:9000"
ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
BUCKET_NAME = "bottleneck"
PREFIX = "data/inputs/"

FILES_TO_DOWNLOAD = ["erp.csv", "web.csv", "liaison.csv"]
LOCAL_PATH = Path("data/inputs")
LOCAL_PATH.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# 📥 Fonction principale de téléchargement
# ==============================================================================
def download_from_minio():
    logger.info("📥 Téléchargement des fichiers depuis MinIO...")

    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name="us-east-1",
        )
        logger.success("✅ Connexion à MinIO réussie.")
    except Exception as e:
        logger.error(f"❌ Connexion échouée : {e}")
        sys.exit(1)

    for filename in FILES_TO_DOWNLOAD:
        s3_key = f"{PREFIX}{filename}"
        local_file = LOCAL_PATH / filename

        try:
            s3_client.download_file(BUCKET_NAME, s3_key, str(local_file))
            logger.success(f"✅ Fichier téléchargé : {filename}")
        except ClientError as e:
            logger.error(f"❌ Erreur lors du téléchargement de {filename} : {e}")
            sys.exit(1)

    logger.success("🎯 Tous les fichiers ont été téléchargés depuis MinIO avec succès.")

# ==============================================================================
# 📌 Lancement direct
# ==============================================================================
if __name__ == "__main__":
    download_from_minio()
