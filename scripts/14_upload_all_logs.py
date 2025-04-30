# === Script 14 - Upload automatique des fichiers logs vers MinIO ===
# Ce script explore le répertoire "logs" et transfère chaque fichier .log
# dans le bucket MinIO, dans le dossier distant 'logs/'.

import os
import sys
import boto3
from botocore.exceptions import ClientError
from pathlib import Path
from loguru import logger

# ============================================================================== 
# 🔧 Initialisation des chemins et logs
# ============================================================================== 
AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "upload_all_logs.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ============================================================================== 
# 📤 Fonction principale : upload des fichiers logs dans MinIO
# ============================================================================== 
def main():
    BUCKET_NAME = "bottleneck"
    MINIO_ENDPOINT = "http://host.docker.internal:9000"
    ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
    SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

    # Connexion à MinIO
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name="us-east-1",
        )
        logger.success("✅ Connexion à MinIO établie.")
    except Exception as e:
        logger.error(f"❌ Erreur de connexion à MinIO : {e}")
        sys.exit(1)

    # Récupération des fichiers logs
    logs_files = list(LOGS_PATH.glob("*.log"))
    if not logs_files:
        logger.warning("⚠️ Aucun fichier log à uploader.")
        sys.exit(0)

    # Upload un par un
    for log_file in logs_files:
        s3_key = f"logs/{log_file.name}"
        try:
            s3_client.upload_file(str(log_file), BUCKET_NAME, s3_key)
            logger.success(f"📤 Log uploadée : {log_file.name}")
        except ClientError as e:
            logger.error(f"❌ Échec upload {log_file.name} : {e}")
            sys.exit(1)

    logger.success("🎯 Tous les fichiers logs ont été uploadés avec succès dans MinIO.")

# ============================================================================== 
# 🚀 Point d'entrée
# ============================================================================== 
if __name__ == "__main__":
    main()
