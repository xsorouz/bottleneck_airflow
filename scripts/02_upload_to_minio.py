# === Script 02 - Upload CSV vers MinIO (robuste, sécurisé, Airflow-compatible) ===
# Ce script envoie les fichiers CSV nettoyés vers un bucket MinIO compatible S3,
# avec vérification du client, du bucket et du succès des transferts.

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

CSV_PATH = Path("data/inputs")  # 📁 Répertoire des CSV à uploader
LOG_FILE = LOGS_PATH / "upload_minio.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# ☁️ Paramètres MinIO (compatibles avec S3)
# ==============================================================================
MINIO_ENDPOINT = "http://host.docker.internal:9000"
ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
BUCKET_NAME = "bottleneck"
DESTINATION_PREFIX = "data/inputs/"

# ==============================================================================
# ⚙️ Fonction principale d'upload
# ==============================================================================
def upload_to_minio():
    logger.info("🚀 Démarrage de l'upload vers MinIO...")

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
        logger.error(f"❌ Connexion à MinIO échouée : {e}")
        sys.exit(1)

    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        logger.success(f"✅ Bucket '{BUCKET_NAME}' disponible.")
    except ClientError as e:
        logger.error(f"❌ Bucket inexistant ou inaccessible : {e}")
        sys.exit(1)

    files_to_upload = ["erp.csv", "web.csv", "liaison.csv"]

    for filename in files_to_upload:
        local_path = CSV_PATH / filename
        s3_key = f"{DESTINATION_PREFIX}{filename}"

        if not local_path.exists():
            logger.error(f"❌ Fichier local manquant : {filename}")
            sys.exit(1)

        try:
            s3_client.upload_file(str(local_path), BUCKET_NAME, s3_key)
            logger.success(f"✅ Upload réussi : {filename} ➔ {s3_key}")
        except Exception as e:
            logger.error(f"❌ Échec de l'upload pour {filename} : {e}")
            sys.exit(1)

    logger.success("🎉 Tous les fichiers CSV ont été uploadés avec succès.")

# ==============================================================================
# 📌 Lancement direct
# ==============================================================================
if __name__ == "__main__":
    upload_to_minio()
