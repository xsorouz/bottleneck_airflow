# === Script 12 - Calcul du Z-score et upload dans MinIO ===
# Ce script détecte les vins millésimés selon le Z-score sur les prix
# puis envoie directement les résultats dans MinIO sous 'data/outputs/'.

import os
import sys
import duckdb
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from pathlib import Path
from loguru import logger
import warnings

warnings.filterwarnings("ignore")

# ============================================================================== 
# 🔧 Initialisation des chemins et logs
# ============================================================================== 
AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "calcul_zscore.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ============================================================================== 
# 🍷 Fonction principale : calcul du Z-score et upload vers MinIO
# ============================================================================== 
def main():
    # Connexion à DuckDB
    try:
        con = duckdb.connect("data/bottleneck.duckdb")
        logger.success("✅ Connexion à DuckDB établie.")
    except Exception as e:
        logger.error(f"❌ Échec de connexion à DuckDB : {e}")
        sys.exit(1)

    # Calcul du Z-score
    try:
        df = con.execute("""
            SELECT product_id, post_title, price
            FROM fusion
            WHERE price IS NOT NULL
        """).fetchdf()

        df["z_score"] = (df["price"] - df["price"].mean()) / df["price"].std()
        df["type"] = df["z_score"].apply(lambda z: "millésimé" if z > 2 else "ordinaire")

        nb_millesimes = (df["type"] == "millésimé").sum()
        nb_total = len(df)

        logger.info(f"🍷 Vins millésimés détectés : {nb_millesimes} (attendu : 30)")
        logger.info(f"📦 Vins ordinaires : {nb_total - nb_millesimes}")
    except Exception as e:
        logger.error(f"❌ Erreur durant le calcul du Z-score : {e}")
        sys.exit(1)

    # Export local
    OUTPUTS_PATH = Path("data/outputs")
    OUTPUTS_PATH.mkdir(parents=True, exist_ok=True)

    try:
        vins_millesimes_path = OUTPUTS_PATH / "vins_millesimes.csv"
        vins_ordinaires_path = OUTPUTS_PATH / "vins_ordinaires.csv"

        df[df["type"] == "millésimé"].to_csv(vins_millesimes_path, index=False)
        df[df["type"] == "ordinaire"].to_csv(vins_ordinaires_path, index=False)

        logger.success(f"📄 Export local : {vins_millesimes_path} & {vins_ordinaires_path}")
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'export local : {e}")
        sys.exit(1)

    # Connexion MinIO et upload
    MINIO_ENDPOINT = "http://host.docker.internal:9000"
    ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
    SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    BUCKET_NAME = "bottleneck"
    DESTINATION_PREFIX = "data/outputs/"

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

    try:
        for local_file in [vins_millesimes_path, vins_ordinaires_path]:
            s3_key = f"{DESTINATION_PREFIX}{local_file.name}"
            s3_client.upload_file(str(local_file), BUCKET_NAME, s3_key)
            logger.success(f"🚀 Upload réussi : {local_file.name} vers {s3_key}")
    except ClientError as e:
        logger.error(f"❌ Erreur d'upload MinIO : {e}")
        sys.exit(1)

    # Tests internes de cohérence
    try:
        assert nb_millesimes == 30, f"❌ Nombre de vins millésimés incorrect : {nb_millesimes} (attendu : 30)"
        assert df[["price", "z_score"]].isnull().sum().sum() == 0, "❌ Valeurs nulles détectées"
        assert df["z_score"].isin([float('inf'), float('-inf')]).sum() == 0, "❌ Z-scores infinis détectés"
        logger.success("🧪 Tests de cohérence Z-score validés ✅")
    except Exception as e:
        logger.error(f"❌ Validation finale Z-score échouée : {e}")
        sys.exit(1)

    logger.success("🎯 Tous les fichiers Z-score ont été uploadés avec succès dans MinIO.")

# ============================================================================== 
# 🚀 Point d'entrée
# ============================================================================== 
if __name__ == "__main__":
    main()
