# === Script 13 - Génération du rapport final et upload dans MinIO ===
# Ce script synthétise tout le pipeline et archive le rapport dans MinIO.

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

LOG_FILE = LOGS_PATH / "rapport_final.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ============================================================================== 
# 📊 Fonction principale : génération et upload du rapport final
# ============================================================================== 
def main():
    # Connexion à DuckDB
    try:
        con = duckdb.connect("data/bottleneck.duckdb")
        logger.success("✅ Connexion à DuckDB établie.")
    except Exception as e:
        logger.error(f"❌ Connexion à DuckDB échouée : {e}")
        sys.exit(1)

    # Collecte des métriques
    try:
        logger.info("📋 Récupération des métriques du pipeline...")

        metrics = {}
        metrics["ERP_brut"] = pd.read_csv("data/raw/erp.csv").shape[0]
        metrics["Web_brut"] = pd.read_csv("data/raw/web.csv").shape[0]
        metrics["Liaison_brut"] = pd.read_csv("data/raw/liaison.csv").shape[0]

        metrics["ERP_nettoye"] = con.execute("SELECT COUNT(*) FROM erp_clean").fetchone()[0]
        metrics["Web_nettoye"] = con.execute("SELECT COUNT(*) FROM web_clean").fetchone()[0]
        metrics["Liaison_nettoye"] = con.execute("SELECT COUNT(*) FROM liaison_clean").fetchone()[0]

        metrics["ERP_dedup"] = con.execute("SELECT COUNT(*) FROM erp_dedup").fetchone()[0]
        metrics["Web_dedup"] = con.execute("SELECT COUNT(*) FROM web_dedup").fetchone()[0]
        metrics["Liaison_dedup"] = con.execute("SELECT COUNT(*) FROM liaison_dedup").fetchone()[0]

        metrics["Fusion"] = con.execute("SELECT COUNT(*) FROM fusion").fetchone()[0]
        metrics["CA_total"] = con.execute("SELECT ca_total FROM ca_total").fetchone()[0]
        metrics["Produits_CA"] = con.execute("SELECT COUNT(*) FROM ca_par_produit").fetchone()[0]
        metrics["Vins_millesimes"] = pd.read_csv("data/outputs/vins_millesimes.csv").shape[0]

        logger.success("✅ Données du pipeline récupérées.")
    except Exception as e:
        logger.error(f"❌ Erreur de récupération des données : {e}")
        sys.exit(1)

    # Construction du rapport
    try:
        df_report = pd.DataFrame([
            {"Étape": "Brut - ERP", "Résultat": metrics["ERP_brut"], "Attendu": ""},
            {"Étape": "Brut - Web", "Résultat": metrics["Web_brut"], "Attendu": ""},
            {"Étape": "Brut - Liaison", "Résultat": metrics["Liaison_brut"], "Attendu": ""},
            {"Étape": "Nettoyé - ERP", "Résultat": metrics["ERP_nettoye"], "Attendu": ""},
            {"Étape": "Nettoyé - Web", "Résultat": metrics["Web_nettoye"], "Attendu": ""},
            {"Étape": "Nettoyé - Liaison", "Résultat": metrics["Liaison_nettoye"], "Attendu": ""},
            {"Étape": "Dédoublonné - ERP", "Résultat": metrics["ERP_dedup"], "Attendu": "825"},
            {"Étape": "Dédoublonné - Web", "Résultat": metrics["Web_dedup"], "Attendu": "714"},
            {"Étape": "Dédoublonné - Liaison", "Résultat": metrics["Liaison_dedup"], "Attendu": "825"},
            {"Étape": "Fusion finale", "Résultat": metrics["Fusion"], "Attendu": "714"},
            {"Étape": "Produits CA", "Résultat": metrics["Produits_CA"], "Attendu": "573"},
            {"Étape": "CA Total (€)", "Résultat": round(metrics["CA_total"], 2), "Attendu": "387837.60"},
            {"Étape": "Vins Millésimés", "Résultat": metrics["Vins_millesimes"], "Attendu": "30"},
        ])

        OUTPUTS_PATH = Path("data/outputs")
        OUTPUTS_PATH.mkdir(parents=True, exist_ok=True)

        df_report.to_csv(OUTPUTS_PATH / "rapport_final.csv", index=False)
        df_report.to_excel(OUTPUTS_PATH / "rapport_final.xlsx", index=False)
        logger.success("📄 Rapport final exporté.")
    except Exception as e:
        logger.error(f"❌ Erreur export rapport : {e}")
        sys.exit(1)

    # Upload MinIO
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url="http://host.docker.internal:9000",
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            region_name="us-east-1",
        )
        for filename in ["rapport_final.csv", "rapport_final.xlsx"]:
            local_file = OUTPUTS_PATH / filename
            s3_key = f"data/outputs/{filename}"
            s3_client.upload_file(str(local_file), "bottleneck", s3_key)
            logger.success(f"🚀 Upload réussi : {filename} vers {s3_key}")
    except Exception as e:
        logger.error(f"❌ Erreur upload MinIO : {e}")
        sys.exit(1)

    logger.success("🎯 Rapport final complet archivé avec succès dans MinIO.")

# ============================================================================== 
# 🚀 Point d'entrée
# ============================================================================== 
if __name__ == "__main__":
    main()
