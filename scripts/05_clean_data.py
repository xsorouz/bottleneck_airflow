# === Script 05 - Nettoyage complet et préparation des fichiers (DuckDB + CSV) ===
# Ce script lit les fichiers CSV bruts depuis 'data/inputs/', applique un nettoyage métier,
# enregistre les fichiers propres dans 'data/outputs/' et crée des tables DuckDB persistantes.

import os
import sys
import warnings
from pathlib import Path
import pandas as pd
import duckdb
from loguru import logger

# ============================================================================== 
# 🔧 Initialisation des chemins et logs
# ============================================================================== 
warnings.filterwarnings("ignore")

AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "clean_data.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ============================================================================== 
# 💼 Fonction principale encapsulant toute la logique
# ============================================================================== 
def main():
    # 📁 Chemins des données
    INPUTS_PATH = Path("data/inputs")
    OUTPUTS_PATH = Path("data/outputs")
    DB_PATH = Path("data/bottleneck.duckdb")

    INPUTS_PATH.mkdir(parents=True, exist_ok=True)
    OUTPUTS_PATH.mkdir(parents=True, exist_ok=True)

    # 📂 Chargement des fichiers CSV bruts
    erp_csv = INPUTS_PATH / "erp.csv"
    web_csv = INPUTS_PATH / "web.csv"
    liaison_csv = INPUTS_PATH / "liaison.csv"

    try:
        df_erp = pd.read_csv(erp_csv)
        df_web = pd.read_csv(web_csv)
        df_liaison = pd.read_csv(liaison_csv)

        logger.info(f"ERP     : {len(df_erp)} lignes (lignes vides : {df_erp.isnull().all(axis=1).sum()})")
        logger.info(f"WEB     : {len(df_web)} lignes (lignes vides : {df_web.isnull().all(axis=1).sum()})")
        logger.info(f"LIAISON : {len(df_liaison)} lignes (lignes vides : {df_liaison.isnull().all(axis=1).sum()})")
    except Exception as e:
        logger.error(f"❌ Erreur lors du chargement initial : {e}")
        return

    # 🦆 Nettoyage métier avec DuckDB
    try:
        con = duckdb.connect(str(DB_PATH))
        logger.info("✅ Connexion DuckDB établie.")

        con.execute("""
            CREATE OR REPLACE TABLE erp_clean AS
            SELECT * FROM read_csv_auto('data/inputs/erp.csv')
            WHERE product_id IS NOT NULL
              AND onsale_web IS NOT NULL
              AND price IS NOT NULL AND price > 0
              AND stock_quantity IS NOT NULL
              AND stock_status IS NOT NULL
        """)
        logger.success("✅ Table 'erp_clean' créée.")

        con.execute("""
            CREATE OR REPLACE TABLE web_clean AS
            SELECT * FROM read_csv_auto('data/inputs/web.csv')
            WHERE sku IS NOT NULL
        """)
        logger.success("✅ Table 'web_clean' créée.")

        con.execute("""
            CREATE OR REPLACE TABLE liaison_clean AS
            SELECT * FROM read_csv_auto('data/inputs/liaison.csv')
            WHERE product_id IS NOT NULL
              AND id_web IS NOT NULL
        """)
        logger.success("✅ Table 'liaison_clean' créée.")
    except Exception as e:
        logger.error(f"❌ Erreur lors du nettoyage avec DuckDB : {e}")
        return

    # 💾 Export des données nettoyées
    try:
        con.execute("COPY erp_clean TO 'data/outputs/erp_clean.csv' (HEADER, DELIMITER ',')")
        con.execute("COPY web_clean TO 'data/outputs/web_clean.csv' (HEADER, DELIMITER ',')")
        con.execute("COPY liaison_clean TO 'data/outputs/liaison_clean.csv' (HEADER, DELIMITER ',')")
        logger.success("✅ Données nettoyées exportées dans 'data/outputs/'.")
    except Exception as e:
        logger.error(f"❌ Erreur d'export CSV : {e}")
        return

    # 📊 Résumé statistique
    try:
        resume_df = pd.DataFrame({
            "source": ["erp", "web", "liaison"],
            "nb_lignes_initiales": [len(df_erp), len(df_web), len(df_liaison)],
            "nb_apres_nettoyage": [
                con.execute("SELECT COUNT(*) FROM erp_clean").fetchone()[0],
                con.execute("SELECT COUNT(*) FROM web_clean").fetchone()[0],
                con.execute("SELECT COUNT(*) FROM liaison_clean").fetchone()[0]
            ]
        })
        resume_df["nb_exclues"] = resume_df["nb_lignes_initiales"] - resume_df["nb_apres_nettoyage"]
        resume_df.to_csv(OUTPUTS_PATH / "resume_stats.csv", index=False)
        logger.success("📈 Statistiques exportées : resume_stats.csv")
    except Exception as e:
        logger.error(f"❌ Erreur lors du résumé statistique : {e}")
        return

    logger.success("🎯 Nettoyage terminé avec succès.")

# ============================================================================== 
# 🚀 Exécution principale
# ============================================================================== 
if __name__ == "__main__":
    main()
