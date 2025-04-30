# === Script 03 - Vérification présence fichiers dans MinIO (robuste, Airflow-compatible) ===
# Ce script vérifie que tous les fichiers CSV attendus sont bien présents
# dans le bucket MinIO après l’upload initial.

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

LOG_FILE = LOGS_PATH / "verify_upload.log"
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

# ==============================================================================
# ⚙️ Fonction principale de vérification
# ==============================================================================
def verify_minio_upload():
    logger.info("🔍 Vérification des fichiers présents dans MinIO...")

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

    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        logger.success(f"✅ Bucket '{BUCKET_NAME}' accessible.")
    except ClientError as e:
        logger.error(f"❌ Bucket inaccessible : {e}")
        sys.exit(1)

    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
        contents = response.get('Contents', [])

        if not contents:
            logger.error(f"❌ Aucun fichier trouvé dans {BUCKET_NAME}/{PREFIX}.")
            sys.exit(1)

        logger.info(f"📄 Fichiers trouvés : {len(contents)}")
        for obj in contents:
            logger.info(f"   - {obj['Key']} ({obj['Size']} octets)")

        expected_files = {f"{PREFIX}erp.csv", f"{PREFIX}web.csv", f"{PREFIX}liaison.csv"}
        found_files = {obj['Key'] for obj in contents}

        missing = expected_files - found_files
        if missing:
            logger.error(f"❌ Fichiers manquants : {missing}")
            sys.exit(1)

        logger.success("🎯 Tous les fichiers attendus sont présents dans MinIO.")

    except Exception as e:
        logger.error(f"❌ Erreur lors du listing MinIO : {e}")
        sys.exit(1)

# ==============================================================================
# 📌 Lancement direct
# ==============================================================================
if __name__ == "__main__":
    verify_minio_upload()
