# === Script 07 - Téléchargement des fichiers nettoyés depuis MinIO ===
# Ce script télécharge les fichiers nettoyés ('erp_clean.csv', 'web_clean.csv', 'liaison_clean.csv')
# depuis le bucket MinIO sous 'data/outputs/' et les enregistre localement dans 'data/outputs/'.

import os
import sys
import warnings
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
from loguru import logger

# ============================================================================== 
# 🔧 Initialisation des chemins et logs
# ============================================================================== 
warnings.filterwarnings("ignore")

AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "download_clean_from_minio.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ============================================================================== 
# 💼 Fonction principale : téléchargement depuis MinIO
# ============================================================================== 
def main():
    # 🌍 Configuration MinIO
    MINIO_ENDPOINT = "http://minio:9000"
    ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
    SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    BUCKET_NAME = "bottleneck"
    PREFIX = "data/outputs/"

    # 📥 Dossier local de destination
    LOCAL_OUTPUTS_PATH = Path("data/outputs")
    LOCAL_OUTPUTS_PATH.mkdir(parents=True, exist_ok=True)

    files_to_download = ["erp_clean.csv", "web_clean.csv", "liaison_clean.csv"]

    # 🔌 Connexion MinIO
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name="us-east-1"
        )
        logger.success("✅ Connexion à MinIO établie avec succès.")
    except Exception as e:
        logger.error(f"❌ Échec de connexion à MinIO : {e}")
        return

    # ✅ Vérification du bucket
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        logger.success(f"✅ Bucket '{BUCKET_NAME}' accessible.")
    except ClientError as e:
        logger.error(f"❌ Bucket '{BUCKET_NAME}' inaccessible : {e}")
        return

    # 📥 Téléchargement des fichiers
    logger.info("📥 Début du téléchargement des fichiers nettoyés depuis MinIO...")

    for filename in files_to_download:
        s3_key = f"{PREFIX}{filename}"
        local_file = LOCAL_OUTPUTS_PATH / filename

        try:
            s3_client.download_file(BUCKET_NAME, s3_key, str(local_file))
            logger.success(f"✅ Fichier téléchargé : {filename}")
        except ClientError as e:
            logger.error(f"❌ Erreur lors du téléchargement de {filename} : {e}")
            return

    logger.success("🎯 Tous les fichiers nettoyés ont été récupérés depuis MinIO avec succès dans 'data/outputs/'.")

# ============================================================================== 
# 🚀 Point d'entrée
# ============================================================================== 
if __name__ == "__main__":
    main()
